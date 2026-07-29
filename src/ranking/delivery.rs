//! Public final-2026 ranked Delivery transaction.
//!
//! The transaction freezes the ten district claims in its first batch, locks
//! and validates every non-empty claim in its second batch, and applies all
//! relative updates in one final batch.  All row identities include the full
//! `(warehouse, district, order)` key.

use crate::connection::client::RmdbClient;
use crate::connection::prepared::Operation;
use crate::connection::wire::WireValue;
use crate::profile::DISTRICTS_PER_WAREHOUSE;
use crate::routing::RoutedTransaction;
use crate::workload::DeliveryInput;

use super::catalog::StatementId;
use super::common::{
    operation, row_f32_bits, row_int32, BatchResults, SemanticResult, SemanticResultExt,
    SemanticViolation,
};
use super::runner::{
    execute_batch, semantic_or_abort, DeliveredOrderEvidence, RankedCommit, RankedTransactionError,
    RankedTransactionOutcome,
};

const MIN_ORDER_LINES: usize = 5;
const MAX_ORDER_LINES: usize = 15;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DistrictClaim {
    district_id: u8,
    order_id: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StageTwoIndices {
    confirm_queue: usize,
    order: usize,
    customer: usize,
    line_rows: usize,
    line_sum: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LockedOrder {
    claim: DistrictClaim,
    customer_id: i32,
    customer_balance_bits: u32,
    customer_payment_count: i32,
    customer_delivery_count: i32,
    line_count: u8,
    amount_bits: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StageThreeIndex {
    customer_after: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CustomerAfter {
    balance_bits: u32,
    payment_count: i32,
    delivery_count: i32,
}

/// Execute one immutable Delivery input.
///
/// A `TRANSACTION_ABORT` returned by any batch remains the only retryable
/// failure through `RankedTransactionError::is_retryable_abort`.
pub async fn execute(
    client: &mut RmdbClient,
    route: &RoutedTransaction,
    input: &DeliveryInput,
    timestamp: &str,
) -> Result<RankedTransactionOutcome, RankedTransactionError> {
    let warehouse_id = route.home_warehouse;

    let stage_one = stage_one_operations(warehouse_id);
    let stage_one_results = execute_batch(client, &stage_one).await?;
    let claims = semantic_or_abort(
        client,
        parse_stage_one(&stage_one_results).require_explicit_abort(),
    )
    .await?;

    if claims.is_empty() {
        let empty_stage = [operation(StatementId::Commit, [])];
        execute_batch(client, &empty_stage).await?;
        return Ok(RankedTransactionOutcome::Committed(RankedCommit::Delivery(
            Vec::new(),
        )));
    }

    let (stage_two, stage_two_indices) = stage_two_operations(warehouse_id, &claims);
    let stage_two_results = execute_batch(client, &stage_two).await?;
    let locked_orders = semantic_or_abort(
        client,
        parse_stage_two(&stage_two_results, &claims, &stage_two_indices).require_explicit_abort(),
    )
    .await?;

    let (stage_three, stage_three_indices) =
        stage_three_operations(warehouse_id, input.carrier_id(), timestamp, &locked_orders);
    let stage_three_results = execute_batch(client, &stage_three).await?;

    // The successful third batch includes COMMIT.  A semantic mismatch found
    // below is fatal evidence about committed state; issuing ABORT now would be
    // both meaningless and a protocol error.
    let customer_after =
        validate_stage_three(&stage_three_results, &locked_orders, &stage_three_indices)
            .map_err(RankedTransactionError::Semantic)?;

    let evidence = locked_orders
        .into_iter()
        .zip(customer_after)
        .map(|(order, after)| DeliveredOrderEvidence {
            warehouse_id,
            district_id: order.claim.district_id,
            order_id: order.claim.order_id,
            customer_id: order.customer_id,
            line_count: order.line_count,
            amount_bits: order.amount_bits,
            customer_balance_before_bits: order.customer_balance_bits,
            customer_balance_after_bits: after.balance_bits,
            customer_delivery_count_before: order.customer_delivery_count,
            customer_delivery_count_after: after.delivery_count,
        })
        .collect();

    Ok(RankedTransactionOutcome::Committed(RankedCommit::Delivery(
        evidence,
    )))
}

fn stage_one_operations(warehouse_id: u16) -> Vec<Operation> {
    let mut operations = Vec::with_capacity(1 + usize::from(DISTRICTS_PER_WAREHOUSE));
    operations.push(operation(StatementId::Begin, []));
    for district_id in 1..=DISTRICTS_PER_WAREHOUSE {
        operations.push(operation(
            StatementId::DeliveryOldestOrder,
            [
                WireValue::Int32(i32::from(warehouse_id)),
                WireValue::Int32(i32::from(district_id)),
            ],
        ));
    }
    operations
}

fn parse_stage_one(results: &BatchResults) -> SemanticResult<Vec<DistrictClaim>> {
    let mut claims = Vec::with_capacity(usize::from(DISTRICTS_PER_WAREHOUSE));
    for district_id in 1..=DISTRICTS_PER_WAREHOUSE {
        let operation_index = usize::from(district_id);
        let row = match results.rows(operation_index)? {
            [] => continue,
            [row] => row.as_slice(),
            rows => {
                return Err(SemanticViolation::new(format!(
                    "Delivery oldest-order district {district_id} returned {} rows; expected \
                     at most one",
                    rows.len()
                )));
            }
        };
        if row.len() != 1 {
            return Err(SemanticViolation::new(format!(
                "Delivery oldest-order district {district_id} returned {} columns; expected one",
                row.len()
            )));
        }
        let order_id = match row.first() {
            Some(WireValue::Null | WireValue::Int32(0)) => continue,
            Some(WireValue::Int32(order_id)) if *order_id > 0 => *order_id,
            Some(WireValue::Int32(order_id)) => {
                return Err(SemanticViolation::new(format!(
                    "Delivery oldest-order district {district_id} returned invalid order id \
                     {order_id}"
                )));
            }
            Some(other) => {
                return Err(SemanticViolation::new(format!(
                    "Delivery oldest-order district {district_id} expected INT32 or NULL, got {}",
                    value_kind(other)
                )));
            }
            None => unreachable!("the one-column check rejected an empty row"),
        };
        claims.push(DistrictClaim {
            district_id,
            order_id,
        });
    }
    Ok(claims)
}

fn stage_two_operations(
    warehouse_id: u16,
    claims: &[DistrictClaim],
) -> (Vec<Operation>, Vec<StageTwoIndices>) {
    let mut operations = Vec::with_capacity(claims.len() * 6);
    let mut indices = Vec::with_capacity(claims.len());

    for claim in claims {
        let key = || {
            [
                WireValue::Int32(i32::from(warehouse_id)),
                WireValue::Int32(i32::from(claim.district_id)),
                WireValue::Int32(claim.order_id),
            ]
        };
        let base = operations.len();
        operations.push(operation(StatementId::DeliveryLockQueue, key()));
        operations.push(operation(StatementId::DeliveryConfirmQueue, key()));
        operations.push(operation(StatementId::DeliveryOrder, key()));
        operations.push(operation(StatementId::DeliveryCustomer, key()));
        operations.push(operation(StatementId::DeliveryLineRows, key()));
        operations.push(operation(StatementId::DeliveryLineSum, key()));
        indices.push(StageTwoIndices {
            confirm_queue: base + 1,
            order: base + 2,
            customer: base + 3,
            line_rows: base + 4,
            line_sum: base + 5,
        });
    }

    (operations, indices)
}

fn parse_stage_two(
    results: &BatchResults,
    claims: &[DistrictClaim],
    indices: &[StageTwoIndices],
) -> SemanticResult<Vec<LockedOrder>> {
    if claims.len() != indices.len() {
        return Err(SemanticViolation::new(format!(
            "Delivery stage-two planner retained {} claims but {} index records",
            claims.len(),
            indices.len()
        )));
    }

    let mut locked_orders = Vec::with_capacity(claims.len());
    for (claim, index) in claims.iter().zip(indices) {
        let context = format!(
            "Delivery warehouse district {} order {}",
            claim.district_id, claim.order_id
        );

        let confirmed_order_id = results.single_int32(index.confirm_queue)?;
        if confirmed_order_id != claim.order_id {
            return Err(SemanticViolation::new(format!(
                "{context} queue confirmation changed oldest order from {} to \
                 {confirmed_order_id}",
                claim.order_id
            )));
        }

        let customer_id = results.single_int32(index.order)?;
        if customer_id <= 0 {
            return Err(SemanticViolation::new(format!(
                "{context} returned invalid customer id {customer_id}"
            )));
        }

        let customer = results.single_row(index.customer)?;
        if customer.len() != 4 {
            return Err(SemanticViolation::new(format!(
                "{context} customer lookup returned {} columns; expected four",
                customer.len()
            )));
        }
        let joined_customer_id = row_int32(customer, 0, &format!("{context} customer"))?;
        if joined_customer_id != customer_id {
            return Err(SemanticViolation::new(format!(
                "{context} order customer {customer_id} disagrees with joined customer \
                 {joined_customer_id}"
            )));
        }
        let customer_balance_bits = row_f32_bits(customer, 1, &format!("{context} customer"))?;
        let customer_payment_count = row_int32(customer, 2, &format!("{context} customer"))?;
        let customer_delivery_count = row_int32(customer, 3, &format!("{context} customer"))?;
        if customer_payment_count < 0 || customer_delivery_count < 0 {
            return Err(SemanticViolation::new(format!(
                "{context} customer has negative logical version \
                 ({customer_payment_count},{customer_delivery_count})"
            )));
        }

        let line_rows = results.rows(index.line_rows)?;
        if !(MIN_ORDER_LINES..=MAX_ORDER_LINES).contains(&line_rows.len()) {
            return Err(SemanticViolation::new(format!(
                "{context} returned {} order lines; expected {MIN_ORDER_LINES}..={MAX_ORDER_LINES}",
                line_rows.len()
            )));
        }
        let mut amount_values = Vec::with_capacity(line_rows.len());
        for (offset, row) in line_rows.iter().enumerate() {
            if row.len() != 2 {
                return Err(SemanticViolation::new(format!(
                    "{context} line row {} returned {} columns; expected two",
                    offset + 1,
                    row.len()
                )));
            }
            let line_number = row_int32(row, 0, &format!("{context} line {}", offset + 1))?;
            let expected_line_number =
                i32::try_from(offset + 1).expect("Delivery line count is bounded by fifteen");
            if line_number != expected_line_number {
                return Err(SemanticViolation::new(format!(
                    "{context} line sequence expected {expected_line_number}, got {line_number}"
                )));
            }
            amount_values.push(row_f32_bits(
                row,
                1,
                &format!("{context} line {line_number}"),
            )?);
        }

        let amount_bits = exact_f64_sum_to_f32_bits(&amount_values)?;
        let server_amount_bits = results.single_f32_bits(index.line_sum)?;
        if server_amount_bits != amount_bits {
            return Err(SemanticViolation::new(format!(
                "{context} SUM(ol_amount) mismatch: expected bits 0x{amount_bits:08x}, \
                 got 0x{server_amount_bits:08x}"
            )));
        }

        locked_orders.push(LockedOrder {
            claim: *claim,
            customer_id,
            customer_balance_bits,
            customer_payment_count,
            customer_delivery_count,
            line_count: line_rows.len() as u8,
            amount_bits,
        });
    }

    Ok(locked_orders)
}

fn stage_three_operations(
    warehouse_id: u16,
    carrier_id: u8,
    timestamp: &str,
    orders: &[LockedOrder],
) -> (Vec<Operation>, Vec<StageThreeIndex>) {
    let mut operations = Vec::with_capacity(orders.len() * 5 + 1);
    let mut indices = Vec::with_capacity(orders.len());

    for order in orders {
        let warehouse = WireValue::Int32(i32::from(warehouse_id));
        let district = WireValue::Int32(i32::from(order.claim.district_id));
        let order_id = WireValue::Int32(order.claim.order_id);
        operations.push(operation(
            StatementId::DeliveryDeleteQueue,
            [warehouse.clone(), district.clone(), order_id.clone()],
        ));
        operations.push(operation(
            StatementId::DeliveryUpdateOrder,
            [
                WireValue::Int32(i32::from(carrier_id)),
                warehouse.clone(),
                district.clone(),
                order_id.clone(),
            ],
        ));
        operations.push(operation(
            StatementId::DeliveryUpdateLines,
            [
                WireValue::Char(timestamp.as_bytes().to_vec()),
                warehouse.clone(),
                district.clone(),
                order_id,
            ],
        ));
        operations.push(operation(
            StatementId::DeliveryUpdateCustomer,
            [
                WireValue::Float32(order.amount_bits),
                warehouse.clone(),
                district.clone(),
                WireValue::Int32(order.customer_id),
            ],
        ));
        let customer_after = operations.len();
        operations.push(operation(
            StatementId::DeliveryCustomerAfter,
            [warehouse, district, WireValue::Int32(order.customer_id)],
        ));
        indices.push(StageThreeIndex { customer_after });
    }

    operations.push(operation(StatementId::Commit, []));
    (operations, indices)
}

fn validate_stage_three(
    results: &BatchResults,
    orders: &[LockedOrder],
    indices: &[StageThreeIndex],
) -> SemanticResult<Vec<CustomerAfter>> {
    if orders.len() != indices.len() {
        return Err(SemanticViolation::new(format!(
            "Delivery stage-three planner retained {} orders but {} index records",
            orders.len(),
            indices.len()
        )));
    }

    let mut after = Vec::with_capacity(orders.len());
    for (order, index) in orders.iter().zip(indices) {
        let context = format!(
            "Delivery customer after district {} order {}",
            order.claim.district_id, order.claim.order_id
        );
        let row = results.single_row(index.customer_after)?;
        if row.len() != 3 {
            return Err(SemanticViolation::new(format!(
                "{context} returned {} columns; expected three",
                row.len()
            )));
        }
        let actual_balance_bits = row_f32_bits(row, 0, &context)?;
        let actual_payment_count = row_int32(row, 1, &context)?;
        let actual_delivery_count = row_int32(row, 2, &context)?;
        validate_customer_after(
            order.customer_balance_bits,
            order.customer_payment_count,
            order.customer_delivery_count,
            order.amount_bits,
            actual_balance_bits,
            actual_payment_count,
            actual_delivery_count,
            &context,
        )?;
        after.push(CustomerAfter {
            balance_bits: actual_balance_bits,
            payment_count: actual_payment_count,
            delivery_count: actual_delivery_count,
        });
    }
    Ok(after)
}

fn validate_customer_after(
    before_balance_bits: u32,
    before_payment_count: i32,
    before_delivery_count: i32,
    amount_bits: u32,
    actual_balance_bits: u32,
    actual_payment_count: i32,
    actual_delivery_count: i32,
    context: &str,
) -> SemanticResult<()> {
    let before_balance = finite_f32(before_balance_bits, &format!("{context} before balance"))?;
    let amount = finite_f32(amount_bits, &format!("{context} amount"))?;
    let expected_balance = before_balance + amount;
    if !expected_balance.is_finite() {
        return Err(SemanticViolation::new(format!(
            "{context} relative balance update produced a non-finite FLOAT32"
        )));
    }
    finite_f32(actual_balance_bits, &format!("{context} actual balance"))?;
    if actual_balance_bits != expected_balance.to_bits() {
        return Err(SemanticViolation::new(format!(
            "{context} relative balance update mismatch: expected bits 0x{:08x}, got \
             0x{actual_balance_bits:08x}",
            expected_balance.to_bits()
        )));
    }

    if actual_payment_count != before_payment_count {
        return Err(SemanticViolation::new(format!(
            "{context} payment count changed from {before_payment_count} to \
             {actual_payment_count}"
        )));
    }
    let expected_delivery_count = before_delivery_count.checked_add(1).ok_or_else(|| {
        SemanticViolation::new(format!("{context} delivery count overflowed INT32"))
    })?;
    if actual_delivery_count != expected_delivery_count {
        return Err(SemanticViolation::new(format!(
            "{context} delivery count mismatch: expected {expected_delivery_count}, got \
             {actual_delivery_count}"
        )));
    }
    Ok(())
}

fn exact_f64_sum_to_f32_bits(values: &[u32]) -> SemanticResult<u32> {
    let mut sum = 0.0_f64;
    for (index, bits) in values.iter().enumerate() {
        let value = finite_f32(*bits, &format!("Delivery line amount {}", index + 1))?;
        sum += f64::from(value);
        if !sum.is_finite() {
            return Err(SemanticViolation::new(format!(
                "Delivery line amount accumulation became non-finite at line {}",
                index + 1
            )));
        }
    }
    let rounded = sum as f32;
    if !rounded.is_finite() {
        return Err(SemanticViolation::new(
            "Delivery line amount sum cannot be represented as finite FLOAT32",
        ));
    }
    Ok(rounded.to_bits())
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

fn value_kind(value: &WireValue) -> &'static str {
    match value {
        WireValue::Null => "NULL",
        WireValue::Int32(_) => "INT32",
        WireValue::Float32(_) => "FLOAT32",
        WireValue::Char(_) => "CHAR",
    }
}

#[cfg(test)]
mod tests {
    use crate::connection::prepared::{BatchQueryResult, BatchResponse};
    use crate::ranking::common::accept_batch;

    use super::*;

    fn query(operation_index: usize, rows: Vec<Vec<WireValue>>) -> BatchQueryResult {
        BatchQueryResult {
            operation_index: operation_index as u16,
            rows,
        }
    }

    #[test]
    fn empty_delivery_has_two_round_trips() {
        let operations = stage_one_operations(7);
        let response = BatchResponse::Ok {
            executed_operations: operations.len() as u16,
            results: (1..=usize::from(DISTRICTS_PER_WAREHOUSE))
                .map(|index| match index {
                    3 => query(index, Vec::new()),
                    7 => query(index, vec![vec![WireValue::Int32(0)]]),
                    _ => query(index, vec![vec![WireValue::Null]]),
                })
                .collect(),
        };
        let results = accept_batch(response, &operations).unwrap();
        let claims = parse_stage_one(&results).unwrap();
        assert!(claims.is_empty());

        let terminal = [operation(StatementId::Commit, [])];
        let batches = [&operations[..], &terminal[..]];
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[1][0].statement_id, StatementId::Commit.wire_id());
    }

    #[test]
    fn stage_two_records_each_query_operation_index() {
        let claims = [
            DistrictClaim {
                district_id: 2,
                order_id: 3001,
            },
            DistrictClaim {
                district_id: 9,
                order_id: 3017,
            },
        ];
        let (operations, indices) = stage_two_operations(17, &claims);
        assert_eq!(operations.len(), 12);
        assert_eq!(
            indices,
            vec![
                StageTwoIndices {
                    confirm_queue: 1,
                    order: 2,
                    customer: 3,
                    line_rows: 4,
                    line_sum: 5,
                },
                StageTwoIndices {
                    confirm_queue: 7,
                    order: 8,
                    customer: 9,
                    line_rows: 10,
                    line_sum: 11,
                },
            ]
        );
        assert_eq!(
            operations[6].statement_id,
            StatementId::DeliveryLockQueue.wire_id()
        );
        assert_eq!(
            operations[6].parameters,
            vec![
                WireValue::Int32(17),
                WireValue::Int32(9),
                WireValue::Int32(3017),
            ]
        );
    }

    #[test]
    fn line_sum_expands_f32_to_f64_then_rounds_once() {
        let values = [
            16_777_216.0_f32.to_bits(),
            1.0_f32.to_bits(),
            1.0_f32.to_bits(),
        ];
        let bits = exact_f64_sum_to_f32_bits(&values).unwrap();
        assert_eq!(f32::from_bits(bits), 16_777_218.0_f32);

        let sequential_f32 = values
            .into_iter()
            .fold(0.0_f32, |sum, bits| sum + f32::from_bits(bits));
        assert_ne!(bits, sequential_f32.to_bits());
    }

    #[test]
    fn customer_after_requires_zero_ulp_and_exact_count() {
        let before = 16_777_216.0_f32.to_bits();
        let amount = 1.0_f32.to_bits();
        let expected = (f32::from_bits(before) + f32::from_bits(amount)).to_bits();
        validate_customer_after(before, 4, 9, amount, expected, 4, 10, "customer").unwrap();

        assert!(validate_customer_after(
            before,
            4,
            9,
            amount,
            expected.wrapping_add(1),
            4,
            10,
            "customer"
        )
        .unwrap_err()
        .message()
        .contains("expected bits"));
        assert!(validate_customer_after(before, 4, 9, amount, expected, 4, 9, "customer").is_err());
        assert!(
            validate_customer_after(before, 4, 9, amount, expected, 5, 10, "customer").is_err()
        );
    }
}
