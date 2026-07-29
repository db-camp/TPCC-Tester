//! Strict helpers shared by the five public final-2026 transaction runners.
//!
//! The wire decoder already validates the byte-level `BATCH_RESULT` shape.
//! This module adds the transaction-level invariants that runners otherwise
//! tend to implement inconsistently: complete execution, retryable abort
//! classification, operation-indexed query lookup, and typed semantic reads.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::connection::prepared::{BatchResponse, Operation};
use crate::connection::wire::WireValue;

use super::catalog::StatementId;

/// Build one typed prepared operation without exposing numeric statement ids
/// throughout the transaction runners.
pub fn operation(
    statement_id: StatementId,
    parameters: impl IntoIterator<Item = WireValue>,
) -> Operation {
    Operation {
        statement_id: statement_id.wire_id(),
        parameters: parameters.into_iter().collect(),
    }
}

/// The only local-semantic-failure cleanup operation.
pub fn abort_operation() -> Operation {
    operation(StatementId::Abort, [])
}

/// A server-side or batch-envelope failure.
///
/// Only `RetryableAbort` is eligible for a retry with the same frozen
/// transaction input.  Every other variant is fatal for that client.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum BatchExecutionError {
    #[error(
        "retryable transaction abort at operation {failed_operation} after \
         {executed_operations} operations: {diagnostic}"
    )]
    RetryableAbort {
        executed_operations: u16,
        failed_operation: u16,
        diagnostic: String,
    },

    #[error(
        "fatal batch error at operation {failed_operation} after \
         {executed_operations} operations: {diagnostic}"
    )]
    FatalOperation {
        executed_operations: u16,
        failed_operation: u16,
        diagnostic: String,
    },

    #[error("fatal top-level batch error: {diagnostic}")]
    FatalTopLevel { diagnostic: String },

    #[error("invalid BATCH_RESULT semantics: {0}")]
    FatalProtocol(String),
}

impl BatchExecutionError {
    pub fn is_retryable_abort(&self) -> bool {
        matches!(self, Self::RetryableAbort { .. })
    }
}

/// Query results from one completely successful batch, indexed by the
/// zero-based operation index in the submitted batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BatchResults {
    operation_count: usize,
    query_rows: BTreeMap<usize, Vec<Vec<WireValue>>>,
}

impl BatchResults {
    pub fn operation_count(&self) -> usize {
        self.operation_count
    }

    /// Fetch all rows produced by a query operation.
    ///
    /// Missing results are semantic violations: callers must never confuse a
    /// command operation or a missing query result with an empty query result.
    pub fn rows(&self, operation_index: usize) -> SemanticResult<&[Vec<WireValue>]> {
        self.query_rows
            .get(&operation_index)
            .map(Vec::as_slice)
            .ok_or_else(|| {
                SemanticViolation::new(format!("operation {operation_index} has no query result"))
            })
    }

    /// Fetch exactly one row while allowing that row to contain many columns.
    pub fn single_row(&self, operation_index: usize) -> SemanticResult<&[WireValue]> {
        let rows = self.rows(operation_index)?;
        match rows {
            [row] => Ok(row.as_slice()),
            [] => Err(SemanticViolation::new(format!(
                "operation {operation_index} returned no rows; expected exactly one"
            ))),
            _ => Err(SemanticViolation::new(format!(
                "operation {operation_index} returned {} rows; expected exactly one",
                rows.len()
            ))),
        }
    }

    pub fn single_int32(&self, operation_index: usize) -> SemanticResult<i32> {
        let row = self.single_scalar_row(operation_index)?;
        row_int32(row, 0, &format!("operation {operation_index}"))
    }

    /// Return the raw IEEE-754 binary32 bits without a decimal or f64
    /// round-trip.
    pub fn single_f32_bits(&self, operation_index: usize) -> SemanticResult<u32> {
        let row = self.single_scalar_row(operation_index)?;
        row_f32_bits(row, 0, &format!("operation {operation_index}"))
    }

    pub fn single_char(&self, operation_index: usize) -> SemanticResult<&[u8]> {
        let row = self.single_scalar_row(operation_index)?;
        row_char(row, 0, &format!("operation {operation_index}"))
    }

    fn single_scalar_row(&self, operation_index: usize) -> SemanticResult<&[WireValue]> {
        let row = self.single_row(operation_index)?;
        if row.len() != 1 {
            return Err(SemanticViolation::new(format!(
                "operation {operation_index} returned {} columns; expected exactly one",
                row.len()
            )));
        }
        Ok(row)
    }
}

/// Accept only a complete successful batch and preserve retryability exactly.
pub fn accept_batch(
    response: BatchResponse,
    operations: &[Operation],
) -> Result<BatchResults, BatchExecutionError> {
    match response {
        BatchResponse::Ok {
            executed_operations,
            results,
        } => {
            if executed_operations as usize != operations.len() {
                return Err(BatchExecutionError::FatalProtocol(format!(
                    "successful batch executed {executed_operations} operations, expected {}",
                    operations.len()
                )));
            }

            let mut previous_index = None;
            let mut query_rows = BTreeMap::new();
            for result in results {
                let operation_index = result.operation_index as usize;
                if operation_index >= operations.len() {
                    return Err(BatchExecutionError::FatalProtocol(format!(
                        "query result operation index {operation_index} is outside batch length {}",
                        operations.len()
                    )));
                }
                if previous_index.is_some_and(|previous| operation_index <= previous) {
                    return Err(BatchExecutionError::FatalProtocol(format!(
                        "query result operation index {operation_index} is not strictly increasing"
                    )));
                }
                previous_index = Some(operation_index);
                if query_rows.insert(operation_index, result.rows).is_some() {
                    return Err(BatchExecutionError::FatalProtocol(format!(
                        "duplicate query result for operation {operation_index}"
                    )));
                }
            }

            Ok(BatchResults {
                operation_count: operations.len(),
                query_rows,
            })
        }
        BatchResponse::TransactionAbort {
            executed_operations,
            failed_operation,
            diagnostic,
        } => {
            validate_failed_index(executed_operations, failed_operation, operations)?;
            Err(BatchExecutionError::RetryableAbort {
                executed_operations,
                failed_operation,
                diagnostic,
            })
        }
        BatchResponse::Error {
            executed_operations,
            failed_operation,
            diagnostic,
        } => {
            validate_failed_index(executed_operations, failed_operation, operations)?;
            Err(BatchExecutionError::FatalOperation {
                executed_operations,
                failed_operation,
                diagnostic,
            })
        }
        BatchResponse::TopLevelError { diagnostic } => {
            Err(BatchExecutionError::FatalTopLevel { diagnostic })
        }
    }
}

fn validate_failed_index(
    executed_operations: u16,
    failed_operation: u16,
    operations: &[Operation],
) -> Result<(), BatchExecutionError> {
    if failed_operation != executed_operations {
        return Err(BatchExecutionError::FatalProtocol(format!(
            "failed operation {failed_operation} does not equal executed operation count \
             {executed_operations}"
        )));
    }
    if failed_operation as usize >= operations.len() {
        return Err(BatchExecutionError::FatalProtocol(format!(
            "failed operation {failed_operation} is outside batch length {}",
            operations.len()
        )));
    }
    Ok(())
}

pub type SemanticResult<T> = Result<T, SemanticViolation>;

/// A result that was structurally valid on the wire but violates the expected
/// TPC-C transaction semantics.
///
/// If the violation is discovered after `BEGIN` and before a server terminal,
/// the caller must mark it with `require_explicit_abort` and issue
/// `abort_operation()` before returning the failure.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{message}")]
pub struct SemanticViolation {
    message: String,
    explicit_abort_required: bool,
}

impl SemanticViolation {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            explicit_abort_required: false,
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn requires_explicit_abort(&self) -> bool {
        self.explicit_abort_required
    }

    pub fn require_explicit_abort(mut self) -> Self {
        self.explicit_abort_required = true;
        self
    }
}

/// Mark a semantic read performed inside an open transaction.  This makes the
/// cleanup obligation visible in the error type without changing the happy
/// path.
pub trait SemanticResultExt<T> {
    fn require_explicit_abort(self) -> SemanticResult<T>;
}

impl<T> SemanticResultExt<T> for SemanticResult<T> {
    fn require_explicit_abort(self) -> SemanticResult<T> {
        self.map_err(SemanticViolation::require_explicit_abort)
    }
}

pub fn row_int32(row: &[WireValue], column_index: usize, context: &str) -> SemanticResult<i32> {
    match row.get(column_index) {
        Some(WireValue::Int32(value)) => Ok(*value),
        Some(other) => Err(wrong_cell_type(context, column_index, "INT32", other)),
        None => Err(missing_cell(context, column_index)),
    }
}

/// Read raw IEEE-754 binary32 bits and reject non-finite values defensively.
pub fn row_f32_bits(row: &[WireValue], column_index: usize, context: &str) -> SemanticResult<u32> {
    match row.get(column_index) {
        Some(WireValue::Float32(bits)) => {
            finite_f32(*bits, &format!("{context} column {column_index}"))?;
            Ok(*bits)
        }
        Some(other) => Err(wrong_cell_type(context, column_index, "FLOAT32", other)),
        None => Err(missing_cell(context, column_index)),
    }
}

pub fn row_char<'a>(
    row: &'a [WireValue],
    column_index: usize,
    context: &str,
) -> SemanticResult<&'a [u8]> {
    match row.get(column_index) {
        Some(WireValue::Char(value)) => Ok(value.as_slice()),
        Some(other) => Err(wrong_cell_type(context, column_index, "CHAR", other)),
        None => Err(missing_cell(context, column_index)),
    }
}

fn missing_cell(context: &str, column_index: usize) -> SemanticViolation {
    SemanticViolation::new(format!(
        "{context} has no column {column_index}; result row is too short"
    ))
}

fn wrong_cell_type(
    context: &str,
    column_index: usize,
    expected: &str,
    actual: &WireValue,
) -> SemanticViolation {
    SemanticViolation::new(format!(
        "{context} column {column_index} expected {expected}, got {}",
        wire_value_kind(actual)
    ))
}

fn wire_value_kind(value: &WireValue) -> &'static str {
    match value {
        WireValue::Null => "NULL",
        WireValue::Int32(_) => "INT32",
        WireValue::Float32(_) => "FLOAT32",
        WireValue::Char(_) => "CHAR",
    }
}

/// Perform exactly one binary32 addition and return its raw result bits.
pub fn f32_add_bits(left_bits: u32, right_bits: u32) -> SemanticResult<u32> {
    let left = finite_f32(left_bits, "FLOAT32 add left operand")?;
    let right = finite_f32(right_bits, "FLOAT32 add right operand")?;
    let result = left + right;
    if !result.is_finite() {
        return Err(SemanticViolation::new(
            "FLOAT32 addition produced a non-finite result",
        ));
    }
    Ok(result.to_bits())
}

/// Perform exactly one binary32 subtraction and return its raw result bits.
pub fn f32_sub_bits(left_bits: u32, right_bits: u32) -> SemanticResult<u32> {
    let left = finite_f32(left_bits, "FLOAT32 subtract left operand")?;
    let right = finite_f32(right_bits, "FLOAT32 subtract right operand")?;
    let result = left - right;
    if !result.is_finite() {
        return Err(SemanticViolation::new(
            "FLOAT32 subtraction produced a non-finite result",
        ));
    }
    Ok(result.to_bits())
}

pub fn expect_f32_add(
    before_bits: u32,
    delta_bits: u32,
    actual_bits: u32,
    context: &str,
) -> SemanticResult<()> {
    expect_f32_bits(
        f32_add_bits(before_bits, delta_bits)?,
        actual_bits,
        context,
        "addition",
    )
}

pub fn expect_f32_sub(
    before_bits: u32,
    delta_bits: u32,
    actual_bits: u32,
    context: &str,
) -> SemanticResult<()> {
    expect_f32_bits(
        f32_sub_bits(before_bits, delta_bits)?,
        actual_bits,
        context,
        "subtraction",
    )
}

fn expect_f32_bits(
    expected_bits: u32,
    actual_bits: u32,
    context: &str,
    operation_name: &str,
) -> SemanticResult<()> {
    finite_f32(actual_bits, &format!("{context} actual value"))?;
    if actual_bits != expected_bits {
        return Err(SemanticViolation::new(format!(
            "{context} FLOAT32 {operation_name} mismatch: expected bits \
             0x{expected_bits:08x}, got 0x{actual_bits:08x}"
        )));
    }
    Ok(())
}

fn finite_f32(bits: u32, context: &str) -> SemanticResult<f32> {
    let value = f32::from_bits(bits);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(SemanticViolation::new(format!(
            "{context} must be finite, got bits 0x{bits:08x}"
        )))
    }
}

/// Select the stable lower median from rows already ordered by
/// `(c_first, c_id)`: `(n - 1) / 2`.
pub fn customer_lower_median<T>(ordered_rows: &[T]) -> SemanticResult<&T> {
    if ordered_rows.is_empty() {
        return Err(SemanticViolation::new(
            "customer surname lookup returned no rows",
        ));
    }
    Ok(&ordered_rows[(ordered_rows.len() - 1) / 2])
}

#[cfg(test)]
mod tests {
    use crate::connection::prepared::BatchQueryResult;

    use super::*;

    fn op(id: StatementId) -> Operation {
        operation(id, [])
    }

    fn operations(count: usize) -> Vec<Operation> {
        (0..count).map(|_| op(StatementId::Begin)).collect()
    }

    fn successful(executed_operations: u16, results: Vec<BatchQueryResult>) -> BatchResponse {
        BatchResponse::Ok {
            executed_operations,
            results,
        }
    }

    fn query_result(operation_index: u16, rows: Vec<Vec<WireValue>>) -> BatchQueryResult {
        BatchQueryResult {
            operation_index,
            rows,
        }
    }

    #[test]
    fn constructs_typed_and_abort_operations() {
        let amount = WireValue::from_f32(17.25);
        let built = operation(
            StatementId::PaymentUpdateWarehouse,
            [amount.clone(), WireValue::Int32(7)],
        );
        assert_eq!(
            built,
            Operation {
                statement_id: StatementId::PaymentUpdateWarehouse.wire_id(),
                parameters: vec![amount, WireValue::Int32(7)],
            }
        );
        assert_eq!(
            abort_operation(),
            Operation {
                statement_id: StatementId::Abort.wire_id(),
                parameters: Vec::new(),
            }
        );
    }

    #[test]
    fn accepts_only_complete_success_and_indexes_query_rows() {
        let operations = operations(3);
        let response = successful(
            3,
            vec![
                query_result(0, vec![vec![WireValue::Int32(17)]]),
                query_result(2, vec![vec![WireValue::Char(b"ok".to_vec())]]),
            ],
        );
        let results = accept_batch(response, &operations).unwrap();
        assert_eq!(results.operation_count(), 3);
        assert_eq!(results.single_int32(0).unwrap(), 17);
        assert_eq!(results.single_char(2).unwrap(), b"ok");
        assert!(results.rows(1).is_err());

        let partial = successful(2, Vec::new());
        assert!(matches!(
            accept_batch(partial, &operations),
            Err(BatchExecutionError::FatalProtocol(_))
        ));
    }

    #[test]
    fn rejects_invalid_or_reordered_query_indices() {
        let operations = operations(2);
        let outside = successful(2, vec![query_result(2, Vec::new())]);
        assert!(matches!(
            accept_batch(outside, &operations),
            Err(BatchExecutionError::FatalProtocol(_))
        ));

        let reordered = successful(
            2,
            vec![query_result(1, Vec::new()), query_result(0, Vec::new())],
        );
        assert!(matches!(
            accept_batch(reordered, &operations),
            Err(BatchExecutionError::FatalProtocol(_))
        ));
    }

    #[test]
    fn classifies_only_transaction_abort_as_retryable() {
        let operations = operations(2);
        let retryable = accept_batch(
            BatchResponse::TransactionAbort {
                executed_operations: 1,
                failed_operation: 1,
                diagnostic: "write conflict".to_owned(),
            },
            &operations,
        )
        .unwrap_err();
        assert!(retryable.is_retryable_abort());
        assert!(matches!(
            retryable,
            BatchExecutionError::RetryableAbort { .. }
        ));

        let fatal_operation = accept_batch(
            BatchResponse::Error {
                executed_operations: 1,
                failed_operation: 1,
                diagnostic: "constraint".to_owned(),
            },
            &operations,
        )
        .unwrap_err();
        assert!(!fatal_operation.is_retryable_abort());
        assert!(matches!(
            fatal_operation,
            BatchExecutionError::FatalOperation { .. }
        ));

        let top_level = accept_batch(
            BatchResponse::TopLevelError {
                diagnostic: "bad batch".to_owned(),
            },
            &operations,
        )
        .unwrap_err();
        assert!(matches!(
            top_level,
            BatchExecutionError::FatalTopLevel { .. }
        ));
    }

    #[test]
    fn malformed_failure_index_is_fatal_protocol() {
        let error = accept_batch(
            BatchResponse::TransactionAbort {
                executed_operations: 0,
                failed_operation: 1,
                diagnostic: "bad envelope".to_owned(),
            },
            &operations(2),
        )
        .unwrap_err();
        assert!(matches!(error, BatchExecutionError::FatalProtocol(_)));
        assert!(!error.is_retryable_abort());
    }

    #[test]
    fn strict_scalar_reads_reject_empty_multiple_null_and_wrong_type() {
        let operations = operations(5);
        let response = successful(
            5,
            vec![
                query_result(0, Vec::new()),
                query_result(
                    1,
                    vec![vec![WireValue::Int32(1)], vec![WireValue::Int32(2)]],
                ),
                query_result(2, vec![vec![WireValue::Int32(1), WireValue::Int32(2)]]),
                query_result(3, vec![vec![WireValue::Null]]),
                query_result(4, vec![vec![WireValue::Char(b"x".to_vec())]]),
            ],
        );
        let results = accept_batch(response, &operations).unwrap();
        assert!(results.single_int32(0).is_err());
        assert!(results.single_int32(1).is_err());
        assert!(results.single_int32(2).is_err());
        assert!(results.single_int32(3).is_err());
        assert!(results.single_int32(4).is_err());
    }

    #[test]
    fn reads_multi_column_rows_with_strict_types_and_raw_float_bits() {
        let bits = 123.75_f32.to_bits();
        let operations = operations(1);
        let response = successful(
            1,
            vec![query_result(
                0,
                vec![vec![
                    WireValue::Int32(9),
                    WireValue::Float32(bits),
                    WireValue::Char(b"GC".to_vec()),
                ]],
            )],
        );
        let results = accept_batch(response, &operations).unwrap();
        let row = results.single_row(0).unwrap();
        assert_eq!(row_int32(row, 0, "customer").unwrap(), 9);
        assert_eq!(row_f32_bits(row, 1, "customer").unwrap(), bits);
        assert_eq!(row_char(row, 2, "customer").unwrap(), b"GC");
        assert!(row_int32(row, 3, "customer").is_err());
        assert!(row_char(row, 1, "customer").is_err());
    }

    #[test]
    fn binary32_helpers_round_once_and_compare_raw_bits() {
        let large = 16_777_216.0_f32.to_bits();
        let one = 1.0_f32.to_bits();
        assert_eq!(f32_add_bits(large, one).unwrap(), large);

        let before = 10.0_f32.to_bits();
        let delta = 0.1_f32.to_bits();
        let added = (10.0_f32 + 0.1_f32).to_bits();
        let subtracted = (10.0_f32 - 0.1_f32).to_bits();
        assert_eq!(f32_add_bits(before, delta).unwrap(), added);
        assert_eq!(f32_sub_bits(before, delta).unwrap(), subtracted);
        expect_f32_add(before, delta, added, "w_ytd").unwrap();
        expect_f32_sub(before, delta, subtracted, "c_balance").unwrap();

        let mismatch = expect_f32_add(before, delta, added.wrapping_add(1), "w_ytd").unwrap_err();
        assert!(mismatch.message().contains("expected bits"));
    }

    #[test]
    fn binary32_helpers_reject_non_finite_inputs_and_outputs() {
        assert!(f32_add_bits(f32::NAN.to_bits(), 0.0_f32.to_bits()).is_err());
        assert!(f32_sub_bits(f32::INFINITY.to_bits(), 0.0_f32.to_bits()).is_err());
        assert!(f32_add_bits(f32::MAX.to_bits(), f32::MAX.to_bits()).is_err());
        assert!(expect_f32_add(
            1.0_f32.to_bits(),
            1.0_f32.to_bits(),
            f32::INFINITY.to_bits(),
            "w_ytd"
        )
        .is_err());
    }

    #[test]
    fn surname_lookup_uses_stable_lower_median() {
        assert_eq!(*customer_lower_median(&[10]).unwrap(), 10);
        assert_eq!(*customer_lower_median(&[10, 20]).unwrap(), 10);
        assert_eq!(*customer_lower_median(&[10, 20, 30, 40]).unwrap(), 20);
        assert_eq!(*customer_lower_median(&[10, 20, 30, 40, 50]).unwrap(), 30);
        assert!(customer_lower_median::<i32>(&[]).is_err());
    }

    #[test]
    fn open_transaction_semantic_error_carries_abort_obligation() {
        let error = Err::<(), _>(SemanticViolation::new("missing stock"))
            .require_explicit_abort()
            .unwrap_err();
        assert!(error.requires_explicit_abort());
        assert_eq!(error.message(), "missing stock");
        assert_eq!(abort_operation().statement_id, StatementId::Abort.wire_id());
    }
}
