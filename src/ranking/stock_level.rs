//! Public final-2026 Stock-Level ranked transaction.
//!
//! The successful path is exactly two `EXEC_BATCH` round trips.  The first
//! opens the transaction and reads the district's next order id.  The second
//! performs the server-side distinct count and commits.

use crate::connection::client::RmdbClient;
use crate::connection::prepared::Operation;
use crate::connection::wire::WireValue;
use crate::routing::RoutedTransaction;
use crate::workload::StockLevelInput;

use super::catalog::StatementId;
use super::common::{
    operation, BatchResults, SemanticResult, SemanticResultExt, SemanticViolation,
};
use super::runner::{
    execute_batch, semantic_or_abort, RankedCommit, RankedTransactionError,
    RankedTransactionOutcome,
};

const MAX_DISTINCT_ITEMS: i32 = 100_000;
const RECENT_ORDER_COUNT: i32 = 20;

pub async fn execute(
    client: &mut RmdbClient,
    route: &RoutedTransaction,
    input: &StockLevelInput,
) -> Result<RankedTransactionOutcome, RankedTransactionError> {
    let stage_one = stage_one_operations(route.home_warehouse, route.home_district);
    let stage_one_results = execute_batch(client, &stage_one).await?;
    let next_order_id = semantic_or_abort(
        client,
        read_next_order_id(&stage_one_results).require_explicit_abort(),
    )
    .await?;

    let stage_two = stage_two_operations(
        route.home_warehouse,
        route.home_district,
        next_order_id,
        input.threshold(),
    );
    let stage_two_results = execute_batch(client, &stage_two).await?;

    // The COMMIT in this batch has already completed, so a malformed result is
    // fatal but must not be followed by a spurious ABORT.
    let low_stock_count =
        read_low_stock_count(&stage_two_results).map_err(RankedTransactionError::Semantic)?;

    Ok(RankedTransactionOutcome::Committed(
        RankedCommit::StockLevel { low_stock_count },
    ))
}

fn stage_one_operations(home_warehouse: u16, home_district: u8) -> [Operation; 2] {
    [
        operation(StatementId::Begin, []),
        operation(
            StatementId::StockLevelNextOrder,
            [
                WireValue::Int32(i32::from(home_warehouse)),
                WireValue::Int32(i32::from(home_district)),
            ],
        ),
    ]
}

fn stage_two_operations(
    home_warehouse: u16,
    home_district: u8,
    next_order_id: i32,
    threshold: u8,
) -> [Operation; 2] {
    [
        operation(
            StatementId::StockLevelCount,
            stock_level_count_parameters(
                i32::from(home_warehouse),
                i32::from(home_district),
                next_order_id,
                i32::from(threshold),
            ),
        ),
        operation(StatementId::Commit, []),
    ]
}

pub(super) fn stock_level_count_parameters(
    home_warehouse: i32,
    home_district: i32,
    next_order_id: i32,
    threshold: i32,
) -> [WireValue; 5] {
    [
        WireValue::Int32(home_warehouse),
        WireValue::Int32(home_district),
        WireValue::Int32(next_order_id - RECENT_ORDER_COUNT),
        WireValue::Int32(next_order_id),
        WireValue::Int32(threshold),
    ]
}

fn read_next_order_id(results: &BatchResults) -> SemanticResult<i32> {
    validate_next_order_id(results.single_int32(1)?)
}

fn validate_next_order_id(next_order_id: i32) -> SemanticResult<i32> {
    if next_order_id > 0 {
        Ok(next_order_id)
    } else {
        Err(SemanticViolation::new(format!(
            "Stock-Level d_next_o_id must be positive, got {next_order_id}"
        )))
    }
}

fn read_low_stock_count(results: &BatchResults) -> SemanticResult<i32> {
    validate_low_stock_count(results.single_int32(0)?)
}

fn validate_low_stock_count(low_stock_count: i32) -> SemanticResult<i32> {
    if (0..=MAX_DISTINCT_ITEMS).contains(&low_stock_count) {
        Ok(low_stock_count)
    } else {
        Err(SemanticViolation::new(format!(
            "Stock-Level distinct item count must be in 0..={MAX_DISTINCT_ITEMS}, \
             got {low_stock_count}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binds_stage_one_in_catalog_order() {
        let operations = stage_one_operations(47, 9);

        assert_eq!(operations[0], operation(StatementId::Begin, []));
        assert_eq!(
            operations[1],
            operation(
                StatementId::StockLevelNextOrder,
                [WireValue::Int32(47), WireValue::Int32(9)]
            )
        );
    }

    #[test]
    fn binds_count_and_commit_in_second_batch() {
        let operations = stage_two_operations(47, 9, 3_017, 14);

        assert_eq!(
            operations[0],
            operation(
                StatementId::StockLevelCount,
                [
                    WireValue::Int32(47),
                    WireValue::Int32(9),
                    WireValue::Int32(2_997),
                    WireValue::Int32(3_017),
                    WireValue::Int32(14),
                ]
            )
        );
        assert_eq!(operations[1], operation(StatementId::Commit, []));
    }

    #[test]
    fn validates_next_order_and_distinct_count_ranges() {
        assert_eq!(validate_next_order_id(1).unwrap(), 1);
        assert!(validate_next_order_id(0).is_err());
        assert!(validate_next_order_id(-1).is_err());

        assert_eq!(validate_low_stock_count(0).unwrap(), 0);
        assert_eq!(
            validate_low_stock_count(MAX_DISTINCT_ITEMS).unwrap(),
            MAX_DISTINCT_ITEMS
        );
        assert!(validate_low_stock_count(-1).is_err());
        assert!(validate_low_stock_count(MAX_DISTINCT_ITEMS + 1).is_err());
    }
}
