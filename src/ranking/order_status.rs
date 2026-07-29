//! Strict public final-2026 OrderStatus transaction runner.

use crate::connection::client::RmdbClient;
use crate::connection::prepared::Operation;
use crate::connection::wire::WireValue;
use crate::profile::{DISTRICTS_PER_WAREHOUSE, ITEM_COUNT, OFFICIAL_WAREHOUSES};
use crate::routing::RoutedTransaction;
use crate::workload::{
    CustomerSelector, OrderStatusInput, CUSTOMERS_PER_DISTRICT, MAX_ITEM_QUANTITY, MAX_ORDER_LINES,
    MIN_ITEM_QUANTITY, MIN_ORDER_LINES,
};

use super::catalog::StatementId;
use super::common::{
    customer_lower_median, operation, row_char, row_f32_bits, row_int32, SemanticResult,
    SemanticResultExt, SemanticViolation,
};
use super::runner::{
    execute_batch, semantic_or_abort, RankedCommit, RankedTransactionError,
    RankedTransactionOutcome,
};

const CUSTOMER_QUERY_OPERATION: usize = 1;
const LATEST_ORDER_QUERY_OPERATION: usize = 0;
const ORDER_QUERY_OPERATION: usize = 0;
const LINES_QUERY_OPERATION: usize = 1;

/// Execute one OrderStatus transaction in exactly three AUTO_ABORT batches.
pub async fn execute(
    client: &mut RmdbClient,
    route: &RoutedTransaction,
    input: &OrderStatusInput,
) -> Result<RankedTransactionOutcome, RankedTransactionError> {
    validate_partition(route).map_err(RankedTransactionError::Semantic)?;
    let customer_lookup =
        customer_lookup_operation(route, input).map_err(RankedTransactionError::Semantic)?;

    let stage_one = [operation(StatementId::Begin, []), customer_lookup];
    let stage_one_results = execute_batch(client, &stage_one).await?;
    let customer_id = semantic_or_abort(
        client,
        select_customer_id(
            input.customer(),
            stage_one_results.rows(CUSTOMER_QUERY_OPERATION),
        )
        .require_explicit_abort(),
    )
    .await?;

    let stage_two = [operation(
        StatementId::OrderStatusLatestOrder,
        partition_customer_parameters(route, customer_id),
    )];
    let stage_two_results = execute_batch(client, &stage_two).await?;
    let order_id = semantic_or_abort(
        client,
        latest_order_id(&stage_two_results, LATEST_ORDER_QUERY_OPERATION).require_explicit_abort(),
    )
    .await?;

    let order_parameters = partition_order_parameters(route, order_id);
    let stage_three = [
        operation(StatementId::OrderStatusOrder, order_parameters.clone()),
        operation(StatementId::OrderStatusLines, order_parameters),
        operation(StatementId::Commit, []),
    ];
    let stage_three_results = execute_batch(client, &stage_three).await?;

    // COMMIT has completed at this point. A semantic mismatch is fatal but
    // must not send a fourth cleanup batch against the next transaction.
    validate_final_results(
        &stage_three_results,
        ORDER_QUERY_OPERATION,
        LINES_QUERY_OPERATION,
        order_id,
    )
    .map_err(RankedTransactionError::Semantic)?;

    Ok(RankedTransactionOutcome::Committed(
        RankedCommit::OrderStatus,
    ))
}

fn validate_partition(route: &RoutedTransaction) -> SemanticResult<()> {
    if !(1..=OFFICIAL_WAREHOUSES).contains(&route.home_warehouse) {
        return Err(SemanticViolation::new(format!(
            "OrderStatus warehouse {} is outside 1..={OFFICIAL_WAREHOUSES}",
            route.home_warehouse
        )));
    }
    if !(1..=DISTRICTS_PER_WAREHOUSE).contains(&route.home_district) {
        return Err(SemanticViolation::new(format!(
            "OrderStatus district {} is outside 1..={DISTRICTS_PER_WAREHOUSE}",
            route.home_district
        )));
    }
    Ok(())
}

fn customer_lookup_operation(
    route: &RoutedTransaction,
    input: &OrderStatusInput,
) -> SemanticResult<Operation> {
    let mut parameters = vec![
        WireValue::Int32(i32::from(route.home_warehouse)),
        WireValue::Int32(i32::from(route.home_district)),
    ];
    let statement_id = match input.customer() {
        CustomerSelector::Id(customer_id) => {
            validate_customer_id(i32::from(*customer_id))?;
            parameters.push(WireValue::Int32(i32::from(*customer_id)));
            StatementId::OrderStatusCustomerById
        }
        CustomerSelector::LastName(last_name) => {
            validate_char(
                last_name.value().as_bytes(),
                1,
                16,
                "OrderStatus selected customer last name",
            )?;
            parameters.push(WireValue::Char(last_name.value().as_bytes().to_vec()));
            StatementId::OrderStatusCustomerByLast
        }
    };
    Ok(operation(statement_id, parameters))
}

fn partition_customer_parameters(route: &RoutedTransaction, customer_id: i32) -> Vec<WireValue> {
    vec![
        WireValue::Int32(i32::from(route.home_warehouse)),
        WireValue::Int32(i32::from(route.home_district)),
        WireValue::Int32(customer_id),
    ]
}

fn partition_order_parameters(route: &RoutedTransaction, order_id: i32) -> Vec<WireValue> {
    vec![
        WireValue::Int32(i32::from(route.home_warehouse)),
        WireValue::Int32(i32::from(route.home_district)),
        WireValue::Int32(order_id),
    ]
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CustomerMatch {
    id: i32,
    first: Vec<u8>,
}

fn select_customer_id(
    selector: &CustomerSelector,
    rows: SemanticResult<&[Vec<WireValue>]>,
) -> SemanticResult<i32> {
    let rows = rows?;
    let mut matches = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        matches.push(parse_customer_row(selector, row, index)?);
    }

    match selector {
        CustomerSelector::Id(expected_id) => {
            if matches.len() != 1 {
                return Err(SemanticViolation::new(format!(
                    "OrderStatus customer-id lookup returned {} rows; expected exactly one",
                    matches.len()
                )));
            }
            let selected = &matches[0];
            if selected.id != i32::from(*expected_id) {
                return Err(SemanticViolation::new(format!(
                    "OrderStatus customer-id lookup returned {}, expected {expected_id}",
                    selected.id
                )));
            }
            Ok(selected.id)
        }
        CustomerSelector::LastName(_) => {
            for pair in matches.windows(2) {
                let left = (&pair[0].first, pair[0].id);
                let right = (&pair[1].first, pair[1].id);
                if left >= right {
                    return Err(SemanticViolation::new(
                        "OrderStatus surname rows are not strictly ordered by (c_first, c_id)",
                    ));
                }
            }
            Ok(customer_lower_median(&matches)?.id)
        }
    }
}

fn parse_customer_row(
    selector: &CustomerSelector,
    row: &[WireValue],
    row_index: usize,
) -> SemanticResult<CustomerMatch> {
    expect_columns(row, 5, &format!("OrderStatus customer row {row_index}"))?;
    let context = format!("OrderStatus customer row {row_index}");
    let customer_id = row_int32(row, 0, &context)?;
    validate_customer_id(customer_id)?;
    let _balance_bits = row_f32_bits(row, 1, &context)?;
    let first = row_char(row, 2, &context)?;
    let middle = row_char(row, 3, &context)?;
    let last = row_char(row, 4, &context)?;
    validate_char(first, 1, 16, &format!("{context} c_first"))?;
    validate_char(middle, 1, 2, &format!("{context} c_middle"))?;
    validate_char(last, 1, 16, &format!("{context} c_last"))?;

    if let CustomerSelector::LastName(expected) = selector {
        if last != expected.value().as_bytes() {
            return Err(SemanticViolation::new(format!(
                "{context} c_last does not match the selected surname"
            )));
        }
    }

    Ok(CustomerMatch {
        id: customer_id,
        first: first.to_vec(),
    })
}

fn validate_customer_id(customer_id: i32) -> SemanticResult<()> {
    if !(1..=i32::from(CUSTOMERS_PER_DISTRICT)).contains(&customer_id) {
        return Err(SemanticViolation::new(format!(
            "OrderStatus customer id {customer_id} is outside 1..={CUSTOMERS_PER_DISTRICT}"
        )));
    }
    Ok(())
}

fn latest_order_id(
    results: &super::common::BatchResults,
    operation_index: usize,
) -> SemanticResult<i32> {
    let order_id = results.single_int32(operation_index)?;
    if order_id <= 0 {
        return Err(SemanticViolation::new(format!(
            "OrderStatus latest order id {order_id} must be positive"
        )));
    }
    Ok(order_id)
}

fn validate_final_results(
    results: &super::common::BatchResults,
    order_operation: usize,
    lines_operation: usize,
    expected_order_id: i32,
) -> SemanticResult<()> {
    validate_order(results.single_row(order_operation)?, expected_order_id)?;
    validate_lines(results.rows(lines_operation)?)
}

fn validate_order(row: &[WireValue], expected_order_id: i32) -> SemanticResult<()> {
    expect_columns(row, 3, "OrderStatus order row")?;
    let order_id = row_int32(row, 0, "OrderStatus order row")?;
    if order_id != expected_order_id {
        return Err(SemanticViolation::new(format!(
            "OrderStatus order row id {order_id} does not match latest order {expected_order_id}"
        )));
    }

    let entry = row_char(row, 1, "OrderStatus order row")?;
    validate_char(entry, 1, 30, "OrderStatus o_entry_d")?;

    let carrier_id = row_int32(row, 2, "OrderStatus order row")?;
    if !(0..=10).contains(&carrier_id) {
        return Err(SemanticViolation::new(format!(
            "OrderStatus carrier id {carrier_id} is outside 0..=10"
        )));
    }
    Ok(())
}

fn validate_lines(rows: &[Vec<WireValue>]) -> SemanticResult<()> {
    if !(usize::from(MIN_ORDER_LINES)..=usize::from(MAX_ORDER_LINES)).contains(&rows.len()) {
        return Err(SemanticViolation::new(format!(
            "OrderStatus returned {} order lines; expected {MIN_ORDER_LINES}..={MAX_ORDER_LINES}",
            rows.len()
        )));
    }

    for (index, row) in rows.iter().enumerate() {
        let context = format!("OrderStatus line row {index}");
        expect_columns(row, 6, &context)?;

        let line_number = row_int32(row, 0, &context)?;
        let expected_number =
            i32::try_from(index + 1).expect("published maximum order line count fits i32");
        if line_number != expected_number {
            return Err(SemanticViolation::new(format!(
                "{context} has ol_number {line_number}, expected {expected_number}"
            )));
        }

        let item_id = row_int32(row, 1, &context)?;
        if !(1..=i32::try_from(ITEM_COUNT).expect("item count fits i32")).contains(&item_id) {
            return Err(SemanticViolation::new(format!(
                "{context} item id {item_id} is outside 1..={ITEM_COUNT}"
            )));
        }

        let supply_warehouse = row_int32(row, 2, &context)?;
        if !(1..=i32::from(OFFICIAL_WAREHOUSES)).contains(&supply_warehouse) {
            return Err(SemanticViolation::new(format!(
                "{context} supply warehouse {supply_warehouse} is outside \
                 1..={OFFICIAL_WAREHOUSES}"
            )));
        }

        let quantity = row_int32(row, 3, &context)?;
        if !(i32::from(MIN_ITEM_QUANTITY)..=i32::from(MAX_ITEM_QUANTITY)).contains(&quantity) {
            return Err(SemanticViolation::new(format!(
                "{context} quantity {quantity} is outside \
                 {MIN_ITEM_QUANTITY}..={MAX_ITEM_QUANTITY}"
            )));
        }

        let _amount_bits = row_f32_bits(row, 4, &context)?;
        let delivery = row_char(row, 5, &context)?;
        if delivery.len() > 30 {
            return Err(SemanticViolation::new(format!(
                "{context} delivery timestamp is {} bytes; maximum is 30",
                delivery.len()
            )));
        }
    }
    Ok(())
}

fn expect_columns(row: &[WireValue], expected: usize, context: &str) -> SemanticResult<()> {
    if row.len() != expected {
        return Err(SemanticViolation::new(format!(
            "{context} returned {} columns; expected {expected}",
            row.len()
        )));
    }
    Ok(())
}

fn validate_char(
    value: &[u8],
    minimum: usize,
    maximum: usize,
    context: &str,
) -> SemanticResult<()> {
    if !(minimum..=maximum).contains(&value.len()) {
        return Err(SemanticViolation::new(format!(
            "{context} is {} bytes; expected {minimum}..={maximum}",
            value.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::prepared::{BatchQueryResult, BatchResponse};
    use crate::profile::TransactionKind;
    use crate::ranking::common::accept_batch;
    use crate::routing::{ClientSequence, OfficialRouter, StageId, WorkloadSeed};
    use crate::workload::{Final2026Workload, TransactionParameters};

    fn sample_order_status(want_last_name: bool) -> (RoutedTransaction, OrderStatusInput) {
        let router = OfficialRouter::new(WorkloadSeed(0x5a17_2026));
        let wheel = router.wheel(StageId::WARMUP);
        let workload = Final2026Workload::new(&router, &wheel);
        let mut sequence = ClientSequence::new(0).unwrap();
        for _ in 0..20_000 {
            let ticket = workload.select(&mut sequence).unwrap();
            let TransactionParameters::OrderStatus(input) = ticket.parameters() else {
                continue;
            };
            let is_last_name = matches!(input.customer(), CustomerSelector::LastName(_));
            if is_last_name == want_last_name {
                assert_eq!(ticket.kind(), TransactionKind::OrderStatus);
                return (ticket.route().clone(), input.clone());
            }
        }
        panic!("deterministic sample did not produce requested OrderStatus selector");
    }

    fn customer_row(id: i32, first: &[u8], last: &[u8]) -> Vec<WireValue> {
        vec![
            WireValue::Int32(id),
            WireValue::from_f32(10.0),
            WireValue::Char(first.to_vec()),
            WireValue::Char(b"OE".to_vec()),
            WireValue::Char(last.to_vec()),
        ]
    }

    fn valid_line(number: i32) -> Vec<WireValue> {
        vec![
            WireValue::Int32(number),
            WireValue::Int32(100 + number),
            WireValue::Int32(1),
            WireValue::Int32(5),
            WireValue::from_f32(number as f32 * 3.25),
            WireValue::Char(b"2026-07-29 12:00:00".to_vec()),
        ]
    }

    #[test]
    fn selector_operation_uses_typed_full_partition_keys() {
        let (id_route, id_input) = sample_order_status(false);
        let id_operation = customer_lookup_operation(&id_route, &id_input).unwrap();
        assert_eq!(
            id_operation.statement_id,
            StatementId::OrderStatusCustomerById.wire_id()
        );
        assert_eq!(
            &id_operation.parameters[..2],
            &[
                WireValue::Int32(i32::from(id_route.home_warehouse)),
                WireValue::Int32(i32::from(id_route.home_district)),
            ]
        );
        assert!(matches!(id_operation.parameters[2], WireValue::Int32(_)));

        let (last_route, last_input) = sample_order_status(true);
        let last_operation = customer_lookup_operation(&last_route, &last_input).unwrap();
        assert_eq!(
            last_operation.statement_id,
            StatementId::OrderStatusCustomerByLast.wire_id()
        );
        assert_eq!(
            &last_operation.parameters[..2],
            &[
                WireValue::Int32(i32::from(last_route.home_warehouse)),
                WireValue::Int32(i32::from(last_route.home_district)),
            ]
        );
        assert!(matches!(last_operation.parameters[2], WireValue::Char(_)));
    }

    #[test]
    fn surname_lookup_selects_stable_lower_median() {
        let (_, input) = sample_order_status(true);
        let CustomerSelector::LastName(last_name) = input.customer() else {
            unreachable!();
        };
        let rows = vec![
            customer_row(11, b"AL", last_name.value().as_bytes()),
            customer_row(12, b"BO", last_name.value().as_bytes()),
            customer_row(13, b"BO", last_name.value().as_bytes()),
            customer_row(14, b"CY", last_name.value().as_bytes()),
        ];
        assert_eq!(select_customer_id(input.customer(), Ok(&rows)).unwrap(), 12);

        let mut unordered = rows;
        unordered.swap(1, 2);
        assert!(select_customer_id(input.customer(), Ok(&unordered)).is_err());
    }

    #[test]
    fn stage_query_operation_indices_are_fixed() {
        let operations = vec![
            operation(
                StatementId::OrderStatusOrder,
                [
                    WireValue::Int32(1),
                    WireValue::Int32(2),
                    WireValue::Int32(3001),
                ],
            ),
            operation(
                StatementId::OrderStatusLines,
                [
                    WireValue::Int32(1),
                    WireValue::Int32(2),
                    WireValue::Int32(3001),
                ],
            ),
            operation(StatementId::Commit, []),
        ];
        let response = BatchResponse::Ok {
            executed_operations: 3,
            results: vec![
                BatchQueryResult {
                    operation_index: ORDER_QUERY_OPERATION as u16,
                    rows: vec![vec![
                        WireValue::Int32(3001),
                        WireValue::Char(b"2026-07-29".to_vec()),
                        WireValue::Int32(0),
                    ]],
                },
                BatchQueryResult {
                    operation_index: LINES_QUERY_OPERATION as u16,
                    rows: (1..=5).map(valid_line).collect(),
                },
            ],
        };
        let results = accept_batch(response, &operations).unwrap();
        validate_final_results(&results, ORDER_QUERY_OPERATION, LINES_QUERY_OPERATION, 3001)
            .unwrap();
    }

    #[test]
    fn line_validation_is_strict_about_shape_ranges_and_finiteness() {
        let valid: Vec<Vec<WireValue>> = (1..=5).map(valid_line).collect();
        validate_lines(&valid).unwrap();

        let mut discontinuous = valid.clone();
        discontinuous[2][0] = WireValue::Int32(4);
        assert!(validate_lines(&discontinuous).is_err());

        let mut bad_quantity = valid.clone();
        bad_quantity[0][3] = WireValue::Int32(0);
        assert!(validate_lines(&bad_quantity).is_err());

        let mut non_finite = valid.clone();
        non_finite[0][4] = WireValue::Float32(f32::NAN.to_bits());
        assert!(validate_lines(&non_finite).is_err());

        let mut long_delivery = valid;
        long_delivery[0][5] = WireValue::Char(vec![b'x'; 31]);
        assert!(validate_lines(&long_delivery).is_err());
    }
}
