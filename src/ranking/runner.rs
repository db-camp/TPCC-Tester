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
pub struct NewOrderEvidence {
    pub warehouse_id: u16,
    pub district_id: u8,
    pub order_id: i32,
    pub line_count: u8,
    pub remote_line_count: u8,
    pub stock_ytd_delta: u32,
    pub line_amount_bits: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaymentEvidence {
    pub warehouse_id: u16,
    pub district_id: u8,
    pub customer_warehouse_id: u16,
    pub customer_district_id: u8,
    pub customer_id: i32,
    pub amount_bits: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveredOrderEvidence {
    pub warehouse_id: u16,
    pub district_id: u8,
    pub order_id: i32,
    pub customer_id: i32,
    pub line_count: u8,
    pub amount_bits: u32,
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
            }])
            .delivery_processed(),
            1
        );
    }
}
