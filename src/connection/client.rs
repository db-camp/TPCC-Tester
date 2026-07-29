use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{debug, trace};

use crate::connection::prepared::{BatchResponse, Operation, PrepareResponse, Statement};
use crate::connection::wire::{StreamResponse, WireConnection, WireError, WireValue};
use crate::error::TpccError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(300);

pub struct RmdbClient {
    connection: WireConnection<TcpStream>,
    response_timeout: Duration,
}

impl RmdbClient {
    pub async fn connect(host: &str, port: u16) -> Result<Self, TpccError> {
        Self::connect_with_timeout(host, port, DEFAULT_RESPONSE_TIMEOUT).await
    }

    pub async fn connect_with_timeout(
        host: &str,
        port: u16,
        response_timeout: Duration,
    ) -> Result<Self, TpccError> {
        let addr = format!("{host}:{port}");
        debug!("正在连接 RMDB: {addr}");

        let connection = tokio::time::timeout(CONNECT_TIMEOUT, WireConnection::connect(&addr))
            .await
            .map_err(|_| TpccError::Timeout {
                context: format!("连接及 Wire v3 握手 {addr} 超时 ({CONNECT_TIMEOUT:?})"),
            })?
            .map_err(map_wire_error)?;

        debug!("已连接到 RMDB 且完成 Wire v3 握手: {addr}");
        Ok(Self {
            connection,
            response_timeout,
        })
    }

    pub async fn exec_stream(&mut self, cmd: &str) -> Result<StreamResponse, TpccError> {
        trace!("发送 SQL: {cmd}");

        tokio::time::timeout(self.response_timeout, self.connection.exec_stream(cmd))
            .await
            .map_err(|_| TpccError::Timeout {
                context: format!(
                    "等待完整 Wire 响应帧超时 ({:?}), 最后发送的 SQL: {cmd}",
                    self.response_timeout
                ),
            })?
            .map_err(map_wire_error)
    }

    pub async fn prepare_set(
        &mut self,
        statements: &[Statement],
    ) -> Result<PrepareResponse, TpccError> {
        tokio::time::timeout(
            self.response_timeout,
            self.connection.prepare_set(statements),
        )
        .await
        .map_err(|_| TpccError::Timeout {
            context: format!(
                "等待 PREPARE_SET 完整响应帧超时 ({:?})",
                self.response_timeout
            ),
        })?
        .map_err(map_wire_error)
    }

    pub async fn exec_batch(
        &mut self,
        operations: &[Operation],
    ) -> Result<BatchResponse, TpccError> {
        tokio::time::timeout(
            self.response_timeout,
            self.connection.exec_batch(operations),
        )
        .await
        .map_err(|_| TpccError::Timeout {
            context: format!(
                "等待 EXEC_BATCH 完整响应帧超时 ({:?})",
                self.response_timeout
            ),
        })?
        .map_err(map_wire_error)
    }

    /// Transitional text adapter for legacy callers.
    ///
    /// New code should use `exec_stream` so FLOAT32 raw bits and terminal
    /// status are never flattened into text.
    pub async fn send_cmd(&mut self, cmd: &str) -> Result<String, TpccError> {
        Ok(response_as_legacy_text(self.exec_stream(cmd).await?))
    }

    pub async fn close(self) {
        let mut stream = self.connection.into_inner();
        let _ = stream.shutdown().await;
        debug!("RMDB 连接已关闭");
    }

    /// Official readiness is a complete framed execution of this exact SQL,
    /// not merely a successful TCP connect.
    pub async fn ping(&mut self) -> Result<(), TpccError> {
        validate_readiness_response(self.exec_stream("show tables;").await?)
    }
}

fn validate_readiness_response(response: StreamResponse) -> Result<(), TpccError> {
    match response {
        StreamResponse::CommandOk | StreamResponse::Query { .. } => Ok(()),
        StreamResponse::TransactionAbort { diagnostic } => Err(TpccError::Abort(diagnostic)),
        StreamResponse::Error { diagnostic } => Err(TpccError::QueryError(diagnostic)),
    }
}

fn map_wire_error(error: WireError) -> TpccError {
    match error {
        WireError::Io(error) => TpccError::Connection(error.to_string()),
        WireError::Protocol(message) => TpccError::Protocol(message),
    }
}

fn response_as_legacy_text(response: StreamResponse) -> String {
    match response {
        StreamResponse::CommandOk => "OK".to_owned(),
        StreamResponse::TransactionAbort { diagnostic } => format!("abort {diagnostic}"),
        StreamResponse::Error { diagnostic } => format!("Error {diagnostic}"),
        StreamResponse::Query { columns, rows } => {
            let mut output = String::new();
            output.push('|');
            for column in columns {
                output.push_str(&column.name);
                output.push('|');
            }
            output.push('\n');
            for row in rows {
                output.push('|');
                for value in row {
                    output.push_str(&wire_value_text(value));
                    output.push('|');
                }
                output.push('\n');
            }
            output
        }
    }
}

fn wire_value_text(value: WireValue) -> String {
    match value {
        WireValue::Null => "NULL".to_owned(),
        WireValue::Int32(value) => value.to_string(),
        WireValue::Float32(bits) => f32::from_bits(bits).to_string(),
        WireValue::Char(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_accepts_both_published_success_terminals() {
        assert!(validate_readiness_response(StreamResponse::CommandOk).is_ok());
        assert!(validate_readiness_response(StreamResponse::Query {
            columns: Vec::new(),
            rows: Vec::new(),
        })
        .is_ok());
    }

    #[test]
    fn readiness_rejects_abort_and_error_terminals() {
        assert!(
            validate_readiness_response(StreamResponse::TransactionAbort {
                diagnostic: "conflict".to_owned(),
            })
            .is_err()
        );
        assert!(validate_readiness_response(StreamResponse::Error {
            diagnostic: "failed".to_owned(),
        })
        .is_err());
    }
}
