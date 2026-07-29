//! Prepared statement and bounded batch support for RMDB Wire Protocol 3.0.

use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};

use super::wire::{
    encode_value, parse_diagnostic, Column, FrameTag, PayloadReader, PreparedDefinition, SqlType,
    WireConnection, WireError, WireResult, WireValue, MAX_DIAGNOSTIC_BYTES, MAX_FRAME_PAYLOAD,
};

const MAX_STATEMENTS: usize = 256;
const MAX_OPERATIONS: usize = 256;
const AUTO_ABORT_FLAG: u8 = 0x01;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatementKind {
    Command,
    Query { columns: Vec<Column> },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Statement {
    pub id: u16,
    pub kind: StatementKind,
    pub param_types: Vec<SqlType>,
    pub sql: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrepareResponse {
    Installed,
    Error { diagnostic: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operation {
    pub statement_id: u16,
    pub parameters: Vec<WireValue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchQueryResult {
    pub operation_index: u16,
    pub rows: Vec<Vec<WireValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchResponse {
    Ok {
        executed_operations: u16,
        results: Vec<BatchQueryResult>,
    },
    TransactionAbort {
        executed_operations: u16,
        failed_operation: u16,
        diagnostic: String,
    },
    Error {
        executed_operations: u16,
        failed_operation: u16,
        diagnostic: String,
    },
    TopLevelError {
        diagnostic: String,
    },
}

impl<S> WireConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Atomically install a connection-local prepared statement dictionary.
    pub async fn prepare_set(&mut self, statements: &[Statement]) -> WireResult<PrepareResponse> {
        self.prepare_set_inner(statements, None).await
    }

    pub(crate) async fn prepare_set_with_timeout(
        &mut self,
        statements: &[Statement],
        timeout: Duration,
    ) -> WireResult<PrepareResponse> {
        self.prepare_set_inner(statements, Some(timeout)).await
    }

    async fn prepare_set_inner(
        &mut self,
        statements: &[Statement],
        timeout: Option<Duration>,
    ) -> WireResult<PrepareResponse> {
        self.ensure_handshaken("PREPARE_SET")?;
        let (payload, replacement) = build_prepare_payload(statements)?;
        let deadline = self
            .write_request_frame(FrameTag::PrepareSet, 0, &payload, "PREPARE_SET", timeout)
            .await?;

        let frame = self
            .read_response_frame_before(deadline, "PREPARE_SET")
            .await?;
        match frame.tag {
            FrameTag::PrepareOk => {
                parse_prepare_ok(&frame.payload, statements)?;
                self.prepared = replacement;
                Ok(PrepareResponse::Installed)
            }
            FrameTag::Error => Ok(PrepareResponse::Error {
                diagnostic: parse_diagnostic(&frame.payload, "ERROR")?,
            }),
            other => Err(WireError::Protocol(format!(
                "unexpected {other:?} frame in PREPARE_SET response"
            ))),
        }
    }

    /// Execute a bounded, ordered operation batch with mandatory AUTO_ABORT.
    pub async fn exec_batch(&mut self, operations: &[Operation]) -> WireResult<BatchResponse> {
        self.exec_batch_inner(operations, None).await
    }

    pub(crate) async fn exec_batch_with_timeout(
        &mut self,
        operations: &[Operation],
        timeout: Duration,
    ) -> WireResult<BatchResponse> {
        self.exec_batch_inner(operations, Some(timeout)).await
    }

    async fn exec_batch_inner(
        &mut self,
        operations: &[Operation],
        timeout: Option<Duration>,
    ) -> WireResult<BatchResponse> {
        self.ensure_handshaken("EXEC_BATCH")?;
        let payload = build_batch_payload(operations, &self.prepared)?;
        let deadline = self
            .write_request_frame(
                FrameTag::ExecBatch,
                AUTO_ABORT_FLAG,
                &payload,
                "EXEC_BATCH",
                timeout,
            )
            .await?;

        let frame = self
            .read_response_frame_before(deadline, "EXEC_BATCH")
            .await?;
        match frame.tag {
            FrameTag::BatchResult => parse_batch_result(&frame.payload, operations, &self.prepared),
            FrameTag::Error => Ok(BatchResponse::TopLevelError {
                diagnostic: parse_diagnostic(&frame.payload, "ERROR")?,
            }),
            other => Err(WireError::Protocol(format!(
                "unexpected {other:?} frame in EXEC_BATCH response"
            ))),
        }
    }
}

fn build_prepare_payload(
    statements: &[Statement],
) -> WireResult<(Vec<u8>, BTreeMap<u16, PreparedDefinition>)> {
    if statements.is_empty() || statements.len() > MAX_STATEMENTS {
        return Err(WireError::Protocol(format!(
            "PREPARE_SET statement_count must be in 1..={MAX_STATEMENTS}"
        )));
    }

    let mut ids = HashSet::with_capacity(statements.len());
    let mut payload_bytes = 2_usize;
    let mut replacement = BTreeMap::new();

    for (index, statement) in statements.iter().enumerate() {
        if statement.id == 0 {
            return Err(WireError::Protocol(format!(
                "PREPARE_SET statement {index} has reserved id 0"
            )));
        }
        if !ids.insert(statement.id) {
            return Err(WireError::Protocol(format!(
                "PREPARE_SET contains duplicate statement id {}",
                statement.id
            )));
        }
        if statement.sql.is_empty() {
            return Err(WireError::Protocol(format!(
                "PREPARE_SET statement {} SQL must not be empty",
                statement.id
            )));
        }
        if statement.sql.as_bytes().contains(&0) {
            return Err(WireError::Protocol(format!(
                "PREPARE_SET statement {} SQL must not contain NUL",
                statement.id
            )));
        }
        if statement.sql.len() > MAX_FRAME_PAYLOAD {
            return Err(WireError::Protocol(format!(
                "PREPARE_SET statement {} SQL exceeds {MAX_FRAME_PAYLOAD} bytes",
                statement.id
            )));
        }
        let _ = u32::try_from(statement.sql.len()).map_err(|_| {
            WireError::Protocol(format!(
                "PREPARE_SET statement {} SQL length does not fit u32",
                statement.id
            ))
        })?;
        let _ = u16::try_from(statement.param_types.len()).map_err(|_| {
            WireError::Protocol(format!(
                "PREPARE_SET statement {} parameter_count does not fit u16",
                statement.id
            ))
        })?;

        let columns = match &statement.kind {
            StatementKind::Command => None,
            StatementKind::Query { columns } => {
                validate_expected_columns(statement.id, columns)?;
                Some(columns.clone())
            }
        };

        payload_bytes = checked_payload_add(
            payload_bytes,
            2 + 1 + 2 + statement.param_types.len() + 4,
            "PREPARE_SET",
        )?;
        payload_bytes = checked_payload_add(payload_bytes, statement.sql.len(), "PREPARE_SET")?;

        replacement.insert(
            statement.id,
            PreparedDefinition {
                parameter_types: statement.param_types.clone(),
                columns,
            },
        );
    }

    let mut payload = Vec::with_capacity(payload_bytes);
    payload.extend_from_slice(&(statements.len() as u16).to_be_bytes());
    for statement in statements {
        payload.extend_from_slice(&statement.id.to_be_bytes());
        payload.push(match statement.kind {
            StatementKind::Command => 0,
            StatementKind::Query { .. } => 1,
        });
        payload.extend_from_slice(&(statement.param_types.len() as u16).to_be_bytes());
        payload.extend(statement.param_types.iter().map(|sql_type| *sql_type as u8));
        payload.extend_from_slice(&(statement.sql.len() as u32).to_be_bytes());
        payload.extend_from_slice(statement.sql.as_bytes());
    }
    debug_assert_eq!(payload.len(), payload_bytes);
    Ok((payload, replacement))
}

fn validate_expected_columns(statement_id: u16, columns: &[Column]) -> WireResult<()> {
    if columns.is_empty() {
        return Err(WireError::Protocol(format!(
            "query statement {statement_id} must declare at least one result column"
        )));
    }
    if columns.len() > u16::MAX as usize {
        return Err(WireError::Protocol(format!(
            "query statement {statement_id} column_count does not fit u16"
        )));
    }
    for (index, column) in columns.iter().enumerate() {
        if column.name.is_empty() {
            return Err(WireError::Protocol(format!(
                "query statement {statement_id} column {index} name must not be empty"
            )));
        }
        if column.name.len() > u16::MAX as usize {
            return Err(WireError::Protocol(format!(
                "query statement {statement_id} column {index} name is too long"
            )));
        }
    }
    Ok(())
}

fn parse_prepare_ok(payload: &[u8], statements: &[Statement]) -> WireResult<()> {
    let mut reader = PayloadReader::new(payload);
    let statement_count = reader.read_u16("PREPARE_OK statement_count")? as usize;
    if statement_count != statements.len() {
        return Err(WireError::Protocol(format!(
            "PREPARE_OK statement_count {statement_count} does not match request {}",
            statements.len()
        )));
    }

    for (index, expected) in statements.iter().enumerate() {
        let statement_id = reader.read_u16("PREPARE_OK statement_id")?;
        if statement_id != expected.id {
            return Err(WireError::Protocol(format!(
                "PREPARE_OK statement {index} id {statement_id} does not match {}",
                expected.id
            )));
        }

        let column_count = reader.read_u16("PREPARE_OK column_count")? as usize;
        let mut columns = Vec::with_capacity(column_count);
        for column_index in 0..column_count {
            columns.push(parse_column_definition(
                &mut reader,
                statement_id,
                column_index,
            )?);
        }

        match &expected.kind {
            StatementKind::Command if !columns.is_empty() => {
                return Err(WireError::Protocol(format!(
                    "PREPARE_OK command statement {statement_id} returned {column_count} columns"
                )));
            }
            StatementKind::Command => {}
            StatementKind::Query {
                columns: expected_columns,
            } => {
                let schema_matches = columns.len() == expected_columns.len()
                    && columns
                        .iter()
                        .zip(expected_columns)
                        .all(|(actual, expected)| actual.sql_type == expected.sql_type);
                if !schema_matches {
                    return Err(WireError::Protocol(format!(
                        "PREPARE_OK query statement {statement_id} schema mismatch: expected \
                         {expected_columns:?}, got {columns:?}"
                    )));
                }
            }
        }
    }

    reader.finish("PREPARE_OK")
}

fn parse_column_definition(
    reader: &mut PayloadReader<'_>,
    statement_id: u16,
    column_index: usize,
) -> WireResult<Column> {
    let name_bytes = reader.read_u16("PREPARE_OK column name_bytes")? as usize;
    if name_bytes == 0 {
        return Err(WireError::Protocol(format!(
            "PREPARE_OK statement {statement_id} column {column_index} name must not be empty"
        )));
    }
    let raw_name = reader.take(name_bytes, "PREPARE_OK column name")?;
    let name = std::str::from_utf8(raw_name)
        .map_err(|_| {
            WireError::Protocol(format!(
                "PREPARE_OK statement {statement_id} column {column_index} name is not UTF-8"
            ))
        })?
        .to_owned();
    let sql_type = SqlType::try_from(reader.read_u8("PREPARE_OK sql_type")?)?;
    Ok(Column { name, sql_type })
}

fn build_batch_payload(
    operations: &[Operation],
    dictionary: &BTreeMap<u16, PreparedDefinition>,
) -> WireResult<Vec<u8>> {
    if operations.is_empty() || operations.len() > MAX_OPERATIONS {
        return Err(WireError::Protocol(format!(
            "EXEC_BATCH operation_count must be in 1..={MAX_OPERATIONS}"
        )));
    }

    let mut payload_bytes = 2_usize;
    for (operation_index, operation) in operations.iter().enumerate() {
        let definition = dictionary.get(&operation.statement_id).ok_or_else(|| {
            WireError::Protocol(format!(
                "EXEC_BATCH operation {operation_index} references unknown statement id {}",
                operation.statement_id
            ))
        })?;
        if operation.parameters.len() != definition.parameter_types.len() {
            return Err(WireError::Protocol(format!(
                "EXEC_BATCH operation {operation_index} statement {} expects {} parameters, got {}",
                operation.statement_id,
                definition.parameter_types.len(),
                operation.parameters.len()
            )));
        }

        payload_bytes = checked_payload_add(payload_bytes, 2, "EXEC_BATCH")?;
        for (parameter_index, (sql_type, value)) in definition
            .parameter_types
            .iter()
            .zip(&operation.parameters)
            .enumerate()
        {
            let encoded_bytes =
                validate_bound_value(operation_index, parameter_index, *sql_type, value)?;
            payload_bytes = checked_payload_add(payload_bytes, encoded_bytes, "EXEC_BATCH")?;
        }
    }

    let mut payload = Vec::with_capacity(payload_bytes);
    payload.extend_from_slice(&(operations.len() as u16).to_be_bytes());
    for operation in operations {
        payload.extend_from_slice(&operation.statement_id.to_be_bytes());
        let definition = dictionary
            .get(&operation.statement_id)
            .expect("operation dictionary membership validated");
        for (sql_type, value) in definition.parameter_types.iter().zip(&operation.parameters) {
            encode_value(*sql_type, value, &mut payload)?;
        }
    }
    debug_assert_eq!(payload.len(), payload_bytes);
    Ok(payload)
}

fn validate_bound_value(
    operation_index: usize,
    parameter_index: usize,
    sql_type: SqlType,
    value: &WireValue,
) -> WireResult<usize> {
    match (sql_type, value) {
        (_, WireValue::Null) => Ok(1),
        (SqlType::Int32, WireValue::Int32(_)) => Ok(5),
        (SqlType::Float32, WireValue::Float32(bits)) => {
            if !f32::from_bits(*bits).is_finite() {
                return Err(WireError::Protocol(format!(
                    "EXEC_BATCH operation {operation_index} parameter {parameter_index} \
                     FLOAT32 must be finite"
                )));
            }
            Ok(5)
        }
        (SqlType::Char, WireValue::Char(bytes)) => {
            let _ = u32::try_from(bytes.len()).map_err(|_| {
                WireError::Protocol(format!(
                    "EXEC_BATCH operation {operation_index} parameter {parameter_index} \
                     CHAR length does not fit u32"
                ))
            })?;
            bytes.len().checked_add(5).ok_or_else(|| {
                WireError::Protocol(format!(
                    "EXEC_BATCH operation {operation_index} parameter {parameter_index} \
                     CHAR encoded length overflows"
                ))
            })
        }
        _ => Err(WireError::Protocol(format!(
            "EXEC_BATCH operation {operation_index} parameter {parameter_index} does not match \
             declared {sql_type:?}"
        ))),
    }
}

fn parse_batch_result(
    payload: &[u8],
    operations: &[Operation],
    dictionary: &BTreeMap<u16, PreparedDefinition>,
) -> WireResult<BatchResponse> {
    let mut reader = PayloadReader::new(payload);
    let executed_operations = reader.read_u16("BATCH_RESULT executed_operations")?;
    let status = reader.read_u8("BATCH_RESULT status")?;
    let failed_operation = reader.read_u16("BATCH_RESULT failed_operation")?;
    let diagnostic_bytes = reader.read_u32("BATCH_RESULT diagnostic_bytes")? as usize;
    if diagnostic_bytes > MAX_DIAGNOSTIC_BYTES {
        return Err(WireError::Protocol(format!(
            "BATCH_RESULT diagnostic exceeds {MAX_DIAGNOSTIC_BYTES} bytes"
        )));
    }
    let raw_diagnostic = reader.take(diagnostic_bytes, "BATCH_RESULT diagnostic")?;
    let diagnostic = std::str::from_utf8(raw_diagnostic)
        .map_err(|_| WireError::Protocol("BATCH_RESULT diagnostic is not valid UTF-8".to_owned()))?
        .to_owned();
    let result_count = reader.read_u16("BATCH_RESULT result_count")? as usize;

    match status {
        0 => parse_success_batch(
            &mut reader,
            operations,
            dictionary,
            executed_operations,
            failed_operation,
            diagnostic,
            result_count,
        ),
        1 | 2 => parse_failed_batch(
            &reader,
            operations.len(),
            status,
            executed_operations,
            failed_operation,
            diagnostic,
            result_count,
        ),
        other => Err(WireError::Protocol(format!(
            "BATCH_RESULT has unknown status {other}"
        ))),
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_success_batch(
    reader: &mut PayloadReader<'_>,
    operations: &[Operation],
    dictionary: &BTreeMap<u16, PreparedDefinition>,
    executed_operations: u16,
    failed_operation: u16,
    diagnostic: String,
    result_count: usize,
) -> WireResult<BatchResponse> {
    if executed_operations as usize != operations.len() {
        return Err(WireError::Protocol(format!(
            "successful BATCH_RESULT executed_operations {executed_operations} does not match {}",
            operations.len()
        )));
    }
    if failed_operation != u16::MAX {
        return Err(WireError::Protocol(format!(
            "successful BATCH_RESULT failed_operation must be 0xffff, got \
             0x{failed_operation:04x}"
        )));
    }
    if !diagnostic.is_empty() {
        return Err(WireError::Protocol(
            "successful BATCH_RESULT diagnostic must be empty".to_owned(),
        ));
    }

    let expected_queries: Vec<(u16, &[Column])> = operations
        .iter()
        .enumerate()
        .filter_map(|(operation_index, operation)| {
            dictionary
                .get(&operation.statement_id)
                .and_then(|definition| {
                    definition
                        .columns
                        .as_deref()
                        .map(|columns| (operation_index as u16, columns))
                })
        })
        .collect();
    if result_count != expected_queries.len() {
        return Err(WireError::Protocol(format!(
            "successful BATCH_RESULT result_count {result_count} does not match {} query \
             operations",
            expected_queries.len()
        )));
    }

    let mut results = Vec::with_capacity(result_count);
    let mut previous_index = None;
    for (result_ordinal, (expected_index, columns)) in expected_queries.into_iter().enumerate() {
        let operation_index = reader.read_u16("BATCH_RESULT operation_index")?;
        if previous_index.is_some_and(|previous| operation_index <= previous) {
            return Err(WireError::Protocol(format!(
                "BATCH_RESULT operation_index {operation_index} is not strictly increasing"
            )));
        }
        if operation_index != expected_index {
            return Err(WireError::Protocol(format!(
                "BATCH_RESULT result {result_ordinal} operation_index {operation_index} does not \
                 match query operation {expected_index}"
            )));
        }
        previous_index = Some(operation_index);

        let row_count = reader.read_u32("BATCH_RESULT row_count")? as usize;
        let minimum_row_bytes = columns.len();
        if row_count > reader.remaining() / minimum_row_bytes {
            return Err(WireError::Protocol(format!(
                "BATCH_RESULT operation {operation_index} row_count {row_count} cannot fit in \
                 remaining payload"
            )));
        }

        let mut rows = Vec::new();
        for row_index in 0..row_count {
            let mut row = Vec::with_capacity(columns.len());
            for (column_index, column) in columns.iter().enumerate() {
                row.push(parse_batch_value(
                    reader,
                    column.sql_type,
                    operation_index,
                    row_index,
                    column_index,
                )?);
            }
            rows.push(row);
        }
        results.push(BatchQueryResult {
            operation_index,
            rows,
        });
    }
    reader.finish("BATCH_RESULT")?;

    Ok(BatchResponse::Ok {
        executed_operations,
        results,
    })
}

fn parse_failed_batch(
    reader: &PayloadReader<'_>,
    operation_count: usize,
    status: u8,
    executed_operations: u16,
    failed_operation: u16,
    diagnostic: String,
    result_count: usize,
) -> WireResult<BatchResponse> {
    if executed_operations as usize >= operation_count {
        return Err(WireError::Protocol(format!(
            "failed BATCH_RESULT executed_operations {executed_operations} must be less than \
             operation_count {operation_count}"
        )));
    }
    if failed_operation != executed_operations {
        return Err(WireError::Protocol(format!(
            "failed BATCH_RESULT failed_operation {failed_operation} does not match \
             executed_operations {executed_operations}"
        )));
    }
    if result_count != 0 {
        return Err(WireError::Protocol(
            "failed BATCH_RESULT must not carry partial query results".to_owned(),
        ));
    }
    reader.finish("BATCH_RESULT")?;

    if status == 1 {
        Ok(BatchResponse::TransactionAbort {
            executed_operations,
            failed_operation,
            diagnostic,
        })
    } else {
        Ok(BatchResponse::Error {
            executed_operations,
            failed_operation,
            diagnostic,
        })
    }
}

fn parse_batch_value(
    reader: &mut PayloadReader<'_>,
    sql_type: SqlType,
    operation_index: u16,
    row_index: usize,
    column_index: usize,
) -> WireResult<WireValue> {
    let present = reader.read_u8("BATCH_RESULT cell present")?;
    match present {
        0 => Ok(WireValue::Null),
        1 => match sql_type {
            SqlType::Int32 => Ok(WireValue::Int32(reader.read_i32("BATCH_RESULT INT32")?)),
            SqlType::Float32 => {
                let bits = reader.read_u32("BATCH_RESULT FLOAT32")?;
                if !f32::from_bits(bits).is_finite() {
                    return Err(WireError::Protocol(format!(
                        "BATCH_RESULT operation {operation_index} row {row_index} column \
                         {column_index} FLOAT32 must be finite"
                    )));
                }
                Ok(WireValue::Float32(bits))
            }
            SqlType::Char => {
                let byte_count = reader.read_u32("BATCH_RESULT CHAR byte_count")? as usize;
                Ok(WireValue::Char(
                    reader.take(byte_count, "BATCH_RESULT CHAR bytes")?.to_vec(),
                ))
            }
        },
        other => Err(WireError::Protocol(format!(
            "BATCH_RESULT operation {operation_index} row {row_index} column {column_index} has \
             invalid present value {other}"
        ))),
    }
}

fn checked_payload_add(total: usize, additional: usize, frame_name: &str) -> WireResult<usize> {
    let total = total
        .checked_add(additional)
        .ok_or_else(|| WireError::Protocol(format!("{frame_name} payload length overflows")))?;
    if total > MAX_FRAME_PAYLOAD {
        return Err(WireError::Protocol(format!(
            "{frame_name} payload exceeds {MAX_FRAME_PAYLOAD} bytes"
        )));
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::cmp;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use tokio::io::{
        duplex, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf,
    };
    use tokio::time::sleep;

    use super::*;
    use crate::connection::wire::HANDSHAKE;

    struct ChunkedIo<T> {
        inner: T,
        max_read: usize,
        max_write: usize,
    }

    impl<T> ChunkedIo<T> {
        fn new(inner: T, max_read: usize, max_write: usize) -> Self {
            Self {
                inner,
                max_read,
                max_write,
            }
        }
    }

    impl<T: AsyncRead + Unpin> AsyncRead for ChunkedIo<T> {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            destination: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            let limit = cmp::min(destination.remaining(), this.max_read);
            let mut scratch = vec![0_u8; limit];
            let mut scratch_buf = ReadBuf::new(&mut scratch);
            match Pin::new(&mut this.inner).poll_read(cx, &mut scratch_buf) {
                Poll::Ready(Ok(())) => {
                    destination.put_slice(scratch_buf.filled());
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl<T: AsyncWrite + Unpin> AsyncWrite for ChunkedIo<T> {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            source: &[u8],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            let limit = cmp::min(source.len(), this.max_write);
            Pin::new(&mut this.inner).poll_write(cx, &source[..limit])
        }

        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_flush(cx)
        }

        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
        }
    }

    fn command(id: u16, param_types: Vec<SqlType>, sql: &str) -> Statement {
        Statement {
            id,
            kind: StatementKind::Command,
            param_types,
            sql: sql.to_owned(),
        }
    }

    fn query(id: u16, columns: Vec<Column>, sql: &str) -> Statement {
        Statement {
            id,
            kind: StatementKind::Query { columns },
            param_types: Vec::new(),
            sql: sql.to_owned(),
        }
    }

    fn column(name: &str, sql_type: SqlType) -> Column {
        Column {
            name: name.to_owned(),
            sql_type,
        }
    }

    fn response_frame(tag: FrameTag, payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(8 + payload.len());
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.push(tag as u8);
        bytes.push(0);
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    async fn read_request(stream: &mut DuplexStream) -> (FrameTag, u8, Vec<u8>) {
        let mut header = [0_u8; 8];
        stream.read_exact(&mut header).await.unwrap();
        assert_eq!(&header[6..], &[0, 0]);
        let payload_bytes = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let mut payload = vec![0_u8; payload_bytes];
        stream.read_exact(&mut payload).await.unwrap();
        (FrameTag::try_from(header[4]).unwrap(), header[5], payload)
    }

    async fn write_fragmented(stream: &mut DuplexStream, bytes: &[u8]) {
        for byte in bytes {
            stream.write_all(&[*byte]).await.unwrap();
            tokio::task::yield_now().await;
        }
    }

    async fn server_handshake(stream: &mut DuplexStream) {
        let mut handshake = [0_u8; 8];
        stream.read_exact(&mut handshake).await.unwrap();
        assert_eq!(handshake, HANDSHAKE);
        write_fragmented(stream, &handshake).await;
    }

    fn prepare_ok(statements: &[(u16, Vec<Column>)]) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(statements.len() as u16).to_be_bytes());
        for (statement_id, columns) in statements {
            payload.extend_from_slice(&statement_id.to_be_bytes());
            payload.extend_from_slice(&(columns.len() as u16).to_be_bytes());
            for column in columns {
                payload.extend_from_slice(&(column.name.len() as u16).to_be_bytes());
                payload.extend_from_slice(column.name.as_bytes());
                payload.push(column.sql_type as u8);
            }
        }
        payload
    }

    fn successful_batch(
        executed: u16,
        operation_index: u16,
        sql_type: SqlType,
        value: &WireValue,
    ) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&executed.to_be_bytes());
        payload.push(0);
        payload.extend_from_slice(&u16::MAX.to_be_bytes());
        payload.extend_from_slice(&0_u32.to_be_bytes());
        payload.extend_from_slice(&1_u16.to_be_bytes());
        payload.extend_from_slice(&operation_index.to_be_bytes());
        payload.extend_from_slice(&1_u32.to_be_bytes());
        encode_value(sql_type, value, &mut payload).unwrap();
        payload
    }

    fn successful_command_batch(executed: u16) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&executed.to_be_bytes());
        payload.push(0);
        payload.extend_from_slice(&u16::MAX.to_be_bytes());
        payload.extend_from_slice(&0_u32.to_be_bytes());
        payload.extend_from_slice(&0_u16.to_be_bytes());
        payload
    }

    #[tokio::test]
    async fn fragmented_prepare_and_batch_preserve_float_bits() {
        let float_bits = 0x3f80_0001_u32;
        let statements = vec![
            command(
                1,
                vec![SqlType::Int32, SqlType::Float32, SqlType::Char],
                "update t set a=$1, f=$2, c=$3;",
            ),
            query(2, vec![column("f", SqlType::Float32)], "select f from t;"),
        ];
        let operations = vec![
            Operation {
                statement_id: 1,
                parameters: vec![
                    WireValue::Int32(7),
                    WireValue::Float32(float_bits),
                    WireValue::Char(b"abc".to_vec()),
                ],
            },
            Operation {
                statement_id: 2,
                parameters: Vec::new(),
            },
        ];

        let (client_io, mut server_io) = duplex(32);
        let server = tokio::spawn(async move {
            server_handshake(&mut server_io).await;

            let (tag, flags, prepare_payload) = read_request(&mut server_io).await;
            assert_eq!(tag, FrameTag::PrepareSet);
            assert_eq!(flags, 0);
            assert_eq!(
                u16::from_be_bytes(prepare_payload[..2].try_into().unwrap()),
                2
            );
            let response = response_frame(
                FrameTag::PrepareOk,
                &prepare_ok(&[(1, Vec::new()), (2, vec![column("f", SqlType::Float32)])]),
            );
            write_fragmented(&mut server_io, &response).await;

            let (tag, flags, batch_payload) = read_request(&mut server_io).await;
            assert_eq!(tag, FrameTag::ExecBatch);
            assert_eq!(flags, AUTO_ABORT_FLAG);
            let mut expected = Vec::new();
            expected.extend_from_slice(&2_u16.to_be_bytes());
            expected.extend_from_slice(&1_u16.to_be_bytes());
            encode_value(SqlType::Int32, &WireValue::Int32(7), &mut expected).unwrap();
            encode_value(
                SqlType::Float32,
                &WireValue::Float32(float_bits),
                &mut expected,
            )
            .unwrap();
            encode_value(
                SqlType::Char,
                &WireValue::Char(b"abc".to_vec()),
                &mut expected,
            )
            .unwrap();
            expected.extend_from_slice(&2_u16.to_be_bytes());
            assert_eq!(batch_payload, expected);

            let response = response_frame(
                FrameTag::BatchResult,
                &successful_batch(2, 1, SqlType::Float32, &WireValue::Float32(float_bits)),
            );
            write_fragmented(&mut server_io, &response).await;
        });

        let mut connection = WireConnection::new(ChunkedIo::new(client_io, 1, 2));
        connection.handshake().await.unwrap();
        assert_eq!(
            connection.prepare_set(&statements).await.unwrap(),
            PrepareResponse::Installed
        );
        assert_eq!(
            connection.exec_batch(&operations).await.unwrap(),
            BatchResponse::Ok {
                executed_operations: 2,
                results: vec![BatchQueryResult {
                    operation_index: 1,
                    rows: vec![vec![WireValue::Float32(float_bits)]],
                }],
            }
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn each_exec_batch_gets_a_fresh_response_deadline() {
        let timeout = Duration::from_millis(400);
        let operation = Operation {
            statement_id: 1,
            parameters: vec![WireValue::Int32(7)],
        };
        let response = response_frame(FrameTag::BatchResult, &successful_command_batch(1));
        let (client_io, mut server_io) = duplex(64);
        let server = tokio::spawn(async move {
            server_handshake(&mut server_io).await;

            for _ in 0..2 {
                let (tag, flags, _) = read_request(&mut server_io).await;
                assert_eq!((tag, flags), (FrameTag::ExecBatch, AUTO_ABORT_FLAG));
                sleep(Duration::from_millis(250)).await;
                server_io.write_all(&response).await.unwrap();
            }
        });

        let mut connection = WireConnection::new(client_io);
        connection.handshake().await.unwrap();
        connection.prepared = test_dictionary();
        for _ in 0..2 {
            assert_eq!(
                connection
                    .exec_batch_with_timeout(std::slice::from_ref(&operation), timeout)
                    .await
                    .unwrap(),
                BatchResponse::Ok {
                    executed_operations: 1,
                    results: Vec::new(),
                }
            );
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn prepare_rejects_schema_mismatch_without_replacing_dictionary() {
        let statements = vec![query(
            7,
            vec![column("amount", SqlType::Float32)],
            "select amount from t;",
        )];
        let (client_io, mut server_io) = duplex(64);
        let server = tokio::spawn(async move {
            server_handshake(&mut server_io).await;
            let (tag, flags, _) = read_request(&mut server_io).await;
            assert_eq!((tag, flags), (FrameTag::PrepareSet, 0));
            let response = response_frame(
                FrameTag::PrepareOk,
                &prepare_ok(&[(7, vec![column("amount", SqlType::Int32)])]),
            );
            write_fragmented(&mut server_io, &response).await;
        });

        let mut connection = WireConnection::new(ChunkedIo::new(client_io, 2, 3));
        connection.handshake().await.unwrap();
        connection.prepared.insert(
            99,
            PreparedDefinition {
                parameter_types: Vec::new(),
                columns: None,
            },
        );
        let error = connection
            .prepare_set(&statements)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("schema mismatch"));
        assert_eq!(connection.prepared.len(), 1);
        assert!(connection.prepared.contains_key(&99));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn prepare_accepts_normalized_column_names_with_matching_types() {
        let statements = vec![query(
            7,
            vec![
                column("w_ytd", SqlType::Float32),
                column("w_id", SqlType::Int32),
            ],
            "select w_ytd, w_id from warehouse;",
        )];
        let (client_io, mut server_io) = duplex(64);
        let server = tokio::spawn(async move {
            server_handshake(&mut server_io).await;
            let (tag, flags, _) = read_request(&mut server_io).await;
            assert_eq!((tag, flags), (FrameTag::PrepareSet, 0));
            let response = response_frame(
                FrameTag::PrepareOk,
                &prepare_ok(&[(
                    7,
                    vec![
                        column("W_YTD", SqlType::Float32),
                        column("warehouse.w_id", SqlType::Int32),
                    ],
                )]),
            );
            write_fragmented(&mut server_io, &response).await;
        });

        let mut connection = WireConnection::new(ChunkedIo::new(client_io, 2, 3));
        connection.handshake().await.unwrap();
        assert_eq!(
            connection.prepare_set(&statements).await.unwrap(),
            PrepareResponse::Installed
        );
        assert_eq!(
            connection.prepared.get(&7).unwrap().columns.as_ref(),
            Some(&vec![
                column("w_ytd", SqlType::Float32),
                column("w_id", SqlType::Int32),
            ])
        );
        server.await.unwrap();
    }

    #[test]
    fn prepare_rejects_empty_response_column_name() {
        let statements = vec![query(
            7,
            vec![column("amount", SqlType::Float32)],
            "select amount from warehouse;",
        )];
        let mut payload = Vec::new();
        payload.extend_from_slice(&1_u16.to_be_bytes());
        payload.extend_from_slice(&7_u16.to_be_bytes());
        payload.extend_from_slice(&1_u16.to_be_bytes());
        payload.extend_from_slice(&0_u16.to_be_bytes());
        payload.push(SqlType::Float32 as u8);

        let error = parse_prepare_ok(&payload, &statements)
            .unwrap_err()
            .to_string();
        assert!(error.contains("column 0 name must not be empty"));
    }

    #[test]
    fn validates_statement_set_bounds_ids_and_sql() {
        assert!(build_prepare_payload(&[]).is_err());

        let zero = command(0, Vec::new(), "begin;");
        assert!(build_prepare_payload(&[zero]).is_err());

        let duplicate = command(1, Vec::new(), "begin;");
        assert!(build_prepare_payload(&[duplicate.clone(), duplicate]).is_err());

        let empty_sql = command(1, Vec::new(), "");
        assert!(build_prepare_payload(&[empty_sql]).is_err());

        let oversized = command(1, Vec::new(), &"x".repeat(MAX_FRAME_PAYLOAD + 1));
        assert!(build_prepare_payload(&[oversized]).is_err());

        let too_many: Vec<_> = (1..=257)
            .map(|id| command(id, Vec::new(), "begin;"))
            .collect();
        assert!(build_prepare_payload(&too_many).is_err());
    }

    fn test_dictionary() -> BTreeMap<u16, PreparedDefinition> {
        BTreeMap::from([
            (
                1,
                PreparedDefinition {
                    parameter_types: vec![SqlType::Int32],
                    columns: None,
                },
            ),
            (
                2,
                PreparedDefinition {
                    parameter_types: Vec::new(),
                    columns: Some(vec![column("f", SqlType::Float32)]),
                },
            ),
        ])
    }

    #[test]
    fn batch_rejects_unknown_statement_type_mismatch_nonfinite_and_oversize() {
        let dictionary = test_dictionary();
        assert!(build_batch_payload(&[], &dictionary).is_err());
        let too_many = vec![
            Operation {
                statement_id: 2,
                parameters: Vec::new(),
            };
            MAX_OPERATIONS + 1
        ];
        assert!(build_batch_payload(&too_many, &dictionary).is_err());

        let unknown = Operation {
            statement_id: 99,
            parameters: Vec::new(),
        };
        assert!(build_batch_payload(&[unknown], &dictionary)
            .unwrap_err()
            .to_string()
            .contains("unknown statement"));

        let mismatch = Operation {
            statement_id: 1,
            parameters: vec![WireValue::Char(b"7".to_vec())],
        };
        assert!(build_batch_payload(&[mismatch], &dictionary)
            .unwrap_err()
            .to_string()
            .contains("does not match"));

        let float_dictionary = BTreeMap::from([(
            3,
            PreparedDefinition {
                parameter_types: vec![SqlType::Float32],
                columns: None,
            },
        )]);
        let infinite = Operation {
            statement_id: 3,
            parameters: vec![WireValue::Float32(f32::INFINITY.to_bits())],
        };
        assert!(build_batch_payload(&[infinite], &float_dictionary)
            .unwrap_err()
            .to_string()
            .contains("must be finite"));

        let char_dictionary = BTreeMap::from([(
            4,
            PreparedDefinition {
                parameter_types: vec![SqlType::Char],
                columns: None,
            },
        )]);
        let oversized = Operation {
            statement_id: 4,
            parameters: vec![WireValue::Char(vec![b'x'; MAX_FRAME_PAYLOAD])],
        };
        assert!(build_batch_payload(&[oversized], &char_dictionary)
            .unwrap_err()
            .to_string()
            .contains("payload exceeds"));
    }

    fn operations_for_result_tests() -> Vec<Operation> {
        vec![
            Operation {
                statement_id: 1,
                parameters: vec![WireValue::Int32(1)],
            },
            Operation {
                statement_id: 2,
                parameters: Vec::new(),
            },
        ]
    }

    fn failed_batch_payload(status: u8, executed: u16, failed: u16, result_count: u16) -> Vec<u8> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&executed.to_be_bytes());
        payload.push(status);
        payload.extend_from_slice(&failed.to_be_bytes());
        payload.extend_from_slice(&0_u32.to_be_bytes());
        payload.extend_from_slice(&result_count.to_be_bytes());
        payload
    }

    #[test]
    fn validates_failed_counts_and_rejects_partial_results() {
        let dictionary = test_dictionary();
        let operations = operations_for_result_tests();

        let wrong_failed = failed_batch_payload(1, 1, 0, 0);
        assert!(parse_batch_result(&wrong_failed, &operations, &dictionary)
            .unwrap_err()
            .to_string()
            .contains("does not match"));

        let partial = failed_batch_payload(1, 1, 1, 1);
        assert!(parse_batch_result(&partial, &operations, &dictionary)
            .unwrap_err()
            .to_string()
            .contains("must not carry partial"));

        let valid_abort = failed_batch_payload(1, 1, 1, 0);
        assert_eq!(
            parse_batch_result(&valid_abort, &operations, &dictionary).unwrap(),
            BatchResponse::TransactionAbort {
                executed_operations: 1,
                failed_operation: 1,
                diagnostic: String::new(),
            }
        );

        let valid_error = failed_batch_payload(2, 1, 1, 0);
        assert_eq!(
            parse_batch_result(&valid_error, &operations, &dictionary).unwrap(),
            BatchResponse::Error {
                executed_operations: 1,
                failed_operation: 1,
                diagnostic: String::new(),
            }
        );
    }

    #[test]
    fn successful_batch_requires_exact_increasing_query_indices() {
        let dictionary = test_dictionary();
        let operations = operations_for_result_tests();
        let wrong_index = successful_batch(
            2,
            0,
            SqlType::Float32,
            &WireValue::Float32(1.0_f32.to_bits()),
        );
        assert!(parse_batch_result(&wrong_index, &operations, &dictionary)
            .unwrap_err()
            .to_string()
            .contains("does not match query operation 1"));
    }
}
