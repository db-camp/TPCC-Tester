//! Shared execution contract for the five ranked transaction runners.

use thiserror::Error;

use crate::connection::client::RmdbClient;
use crate::connection::prepared::Operation;
use crate::error::TpccError;

use super::common::{
    abort_operation, accept_batch, BatchExecutionError, BatchResults, SemanticResult,
    SemanticViolation,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StockVersion {
    pub quantity: i32,
    pub ytd_bits: u32,
    pub order_count: i32,
    pub remote_count: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveryNewOrderLineEvidence {
    pub number: u8,
    pub item_id: u32,
    pub supply_warehouse: u16,
    pub quantity: u8,
    pub amount_bits: u32,
    pub district_info: Vec<u8>,
    pub stock_before: StockVersion,
    pub stock_after: StockVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewOrderEvidence {
    pub warehouse_id: u16,
    pub district_id: u8,
    pub order_id: i32,
    pub line_count: u8,
    pub remote_line_count: u8,
    pub stock_ytd_delta: u32,
    pub line_amount_bits: Vec<u32>,
    pub entry_timestamp: Vec<u8>,
    pub recovery_lines: Vec<RecoveryNewOrderLineEvidence>,
}

/// The two counters jointly identify one logical customer-row predecessor.
///
/// Payment advances only `payment_count`; Delivery advances only
/// `delivery_count`. Recording both values before and after each update makes
/// stale cross-family writers visible even when a FLOAT32 balance update
/// rounds to a self-loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CustomerVersion {
    pub payment_count: i32,
    pub delivery_count: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaymentEvidence {
    pub warehouse_id: u16,
    pub district_id: u8,
    pub customer_warehouse_id: u16,
    pub customer_district_id: u8,
    pub customer_id: i32,
    pub amount_bits: u32,
    pub warehouse_before_bits: u32,
    pub warehouse_after_bits: u32,
    pub district_before_bits: u32,
    pub district_after_bits: u32,
    pub customer_balance_before_bits: u32,
    pub customer_balance_after_bits: u32,
    pub customer_ytd_before_bits: u32,
    pub customer_ytd_after_bits: u32,
    pub customer_version_before: CustomerVersion,
    pub customer_version_after: CustomerVersion,
    pub history_timestamp: Vec<u8>,
    pub history_data: Vec<u8>,
    pub customer_is_bad_credit: bool,
    pub customer_data_before: Vec<u8>,
    pub customer_data_after: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveredOrderEvidence {
    pub warehouse_id: u16,
    pub district_id: u8,
    pub order_id: i32,
    pub customer_id: i32,
    pub line_count: u8,
    pub amount_bits: u32,
    pub customer_balance_before_bits: u32,
    pub customer_balance_after_bits: u32,
    pub customer_version_before: CustomerVersion,
    pub customer_version_after: CustomerVersion,
    pub delivery_timestamp: Vec<u8>,
    pub line_amount_bits: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RankedCommit {
    NewOrder(NewOrderEvidence),
    Payment(PaymentEvidence),
    OrderStatus,
    Delivery(Vec<DeliveredOrderEvidence>),
    StockLevel { low_stock_count: i32 },
}

impl RankedCommit {
    pub fn delivery_processed(&self) -> u64 {
        match self {
            Self::Delivery(orders) => orders.len() as u64,
            _ => 0,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RankedTransactionOutcome {
    Committed(RankedCommit),
    ExpectedRollback,
}

/// Shared, non-configurable policy for a complete retryable transaction abort.
///
/// The first `TRANSACTION_ABORT` retries the exact frozen transaction once. A
/// second abort is the final outcome for that logical selection. Keeping this
/// state outside both executors prevents fast and ranked runs from silently
/// applying different retry limits.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DirectRetryState {
    retry_used: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectRetryDecision {
    RetrySameParameters,
    Abandon,
}

impl DirectRetryState {
    pub fn on_transaction_abort(&mut self) -> DirectRetryDecision {
        if self.retry_used {
            DirectRetryDecision::Abandon
        } else {
            self.retry_used = true;
            DirectRetryDecision::RetrySameParameters
        }
    }
}

#[derive(Debug, Error)]
pub enum RankedTransactionError {
    #[error("ranked transport failed: {0}")]
    Transport(#[from] TpccError),

    #[error("{0}")]
    Batch(#[from] BatchExecutionError),

    #[error("ranked semantic validation failed: {0}")]
    Semantic(SemanticViolation),

    #[error("semantic validation failed ({semantic}); explicit ABORT cleanup failed ({cleanup})")]
    Cleanup {
        semantic: SemanticViolation,
        cleanup: String,
    },
}

impl RankedTransactionError {
    pub fn is_retryable_abort(&self) -> bool {
        matches!(
            self,
            Self::Batch(BatchExecutionError::RetryableAbort { .. })
        )
    }

    /// A response-read deadline exceeded on a ranked request. The official
    /// client abandons such attempts (uniform ~22-27% abandoned across
    /// transaction families) instead of failing the worker, so the local
    /// runner treats them like a retryable abort and rebuilds the session.
    pub fn is_response_timeout(&self) -> bool {
        matches!(self, Self::Transport(TpccError::Timeout { .. }))
    }
}

/// Execute one AUTO_ABORT batch and preserve the server's retry classification.
pub async fn execute_batch(
    client: &mut RmdbClient,
    operations: &[Operation],
) -> Result<BatchResults, RankedTransactionError> {
    let response = client.exec_batch(operations).await?;
    Ok(accept_batch(response, operations)?)
}

/// Resolve a typed semantic read, issuing an explicit ABORT when it failed
/// while a transaction was still open.
pub async fn semantic_or_abort<T>(
    client: &mut RmdbClient,
    result: SemanticResult<T>,
) -> Result<T, RankedTransactionError> {
    match result {
        Ok(value) => Ok(value),
        Err(semantic) if semantic.requires_explicit_abort() => {
            let operations = [abort_operation()];
            match execute_batch(client, &operations).await {
                Ok(_) => Err(RankedTransactionError::Semantic(semantic)),
                Err(cleanup) => Err(RankedTransactionError::Cleanup {
                    semantic,
                    cleanup: cleanup.to_string(),
                }),
            }
        }
        Err(semantic) => Err(RankedTransactionError::Semantic(semantic)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::prepared::BatchResponse;
    use crate::ranking::common::BatchExecutionError;

    #[test]
    fn retry_classification_is_exact() {
        let retryable = RankedTransactionError::Batch(BatchExecutionError::RetryableAbort {
            executed_operations: 0,
            failed_operation: 0,
            diagnostic: "write conflict".to_owned(),
        });
        assert!(retryable.is_retryable_abort());

        let fatal = RankedTransactionError::Batch(BatchExecutionError::FatalTopLevel {
            diagnostic: "bad request".to_owned(),
        });
        assert!(!fatal.is_retryable_abort());

        let _keeps_wire_type_visible = BatchResponse::TopLevelError {
            diagnostic: String::new(),
        };
    }

    #[test]
    fn direct_retry_state_retries_once_then_abandons() {
        let mut state = DirectRetryState::default();

        assert_eq!(
            state.on_transaction_abort(),
            DirectRetryDecision::RetrySameParameters
        );
        assert_eq!(
            state.on_transaction_abort(),
            DirectRetryDecision::Abandon
        );
        assert_eq!(
            state.on_transaction_abort(),
            DirectRetryDecision::Abandon
        );
    }

    #[test]
    fn only_delivery_contributes_processed_queue_count() {
        assert_eq!(RankedCommit::OrderStatus.delivery_processed(), 0);
        assert_eq!(
            RankedCommit::Delivery(vec![DeliveredOrderEvidence {
                warehouse_id: 1,
                district_id: 1,
                order_id: 3001,
                customer_id: 1,
                line_count: 5,
                amount_bits: 1.0_f32.to_bits(),
                customer_balance_before_bits: 0.0_f32.to_bits(),
                customer_balance_after_bits: 1.0_f32.to_bits(),
                customer_version_before: CustomerVersion {
                    payment_count: 1,
                    delivery_count: 0,
                },
                customer_version_after: CustomerVersion {
                    payment_count: 1,
                    delivery_count: 1,
                },
                delivery_timestamp: b"2026-07-29 10:20:30".to_vec(),
                line_amount_bits: vec![1.0_f32.to_bits(); 5],
            }])
            .delivery_processed(),
            1
        );
    }
}
