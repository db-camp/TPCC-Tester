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

use super::catalog::{StatementId, UNDELIVERED_CARRIER_ID};
use super::common::{
    operation, row_f32_bits, row_int32, BatchResults, SemanticResult, SemanticResultExt,
    SemanticViolation,
};
use super::runner::{
    abort_retryable_contention, execute_batch, semantic_or_abort, CustomerVersion,
    DeliveredOrderEvidence, RankedCommit, RankedTransactionError, RankedTransactionOutcome,
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
    earlier_queue_count: usize,
    exact_queue_count: usize,
    order: usize,
    line_rows: usize,
    line_sum: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LockedOrder {
    claim: DistrictClaim,
    customer_id: i32,
    line_count: u8,
    amount_bits: u32,
    line_amount_bits: Vec<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum StageTwoOutcome {
    Locked(Vec<LockedOrder>),
    ClaimLost(DistrictClaim),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StageThreeIndex {
    customer_before: usize,
    customer_after: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CustomerSnapshot {
    balance_bits: u32,
    payment_count: i32,
    delivery_count: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CustomerTransition {
    before: CustomerSnapshot,
    after: CustomerSnapshot,
}

/// Execute one immutable Delivery input.
///
/// A server-side `TRANSACTION_ABORT`, or a claim that disappears before the
/// second-stage confirmation and is explicitly aborted here, is retryable
/// through `RankedTransactionError::is_retryable_abort`.
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
    let stage_two_outcome = semantic_or_abort(
        client,
        parse_stage_two(&stage_two_results, &claims, &stage_two_indices).require_explicit_abort(),
    )
    .await?;
    let locked_orders = match stage_two_outcome {
        StageTwoOutcome::Locked(orders) => orders,
        StageTwoOutcome::ClaimLost(claim) => {
            let diagnostic = format!(
                "Delivery warehouse district {} order {} disappeared before queue confirmation",
                claim.district_id, claim.order_id
            );
            return Err(abort_retryable_contention(client, diagnostic).await);
        }
    };

    let (stage_three, stage_three_indices) =
        stage_three_operations(warehouse_id, input.carrier_id(), timestamp, &locked_orders);
    let stage_three_results = execute_batch(client, &stage_three).await?;

    // The successful third batch includes COMMIT.  A semantic mismatch found
    // below is fatal evidence about committed state; issuing ABORT now would be
    // both meaningless and a protocol error.
    let customer_transitions =
        validate_stage_three(&stage_three_results, &locked_orders, &stage_three_indices)
            .map_err(RankedTransactionError::Semantic)?;

    let evidence = locked_orders
        .into_iter()
        .zip(customer_transitions)
        .map(|(order, transition)| DeliveredOrderEvidence {
            warehouse_id,
            district_id: order.claim.district_id,
            order_id: order.claim.order_id,
            customer_id: order.customer_id,
            line_count: order.line_count,
            amount_bits: order.amount_bits,
            customer_balance_before_bits: transition.before.balance_bits,
            customer_balance_after_bits: transition.after.balance_bits,
            customer_version_before: CustomerVersion {
                payment_count: transition.before.payment_count,
                delivery_count: transition.before.delivery_count,
            },
            customer_version_after: CustomerVersion {
                payment_count: transition.after.payment_count,
                delivery_count: transition.after.delivery_count,
            },
            delivery_timestamp: timestamp.as_bytes().to_vec(),
            line_amount_bits: order.line_amount_bits,
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
        operations.push(operation(StatementId::DeliveryEarlierQueueCount, key()));
        operations.push(operation(StatementId::DeliveryExactQueueCount, key()));
        operations.push(operation(StatementId::DeliveryOrder, key()));
        operations.push(operation(StatementId::DeliveryLineRows, key()));
        operations.push(operation(StatementId::DeliveryLineSum, key()));
        indices.push(StageTwoIndices {
            earlier_queue_count: base + 1,
            exact_queue_count: base + 2,
            order: base + 3,
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
) -> SemanticResult<StageTwoOutcome> {
    if claims.len() != indices.len() {
        return Err(SemanticViolation::new(format!(
            "Delivery stage-two planner retained {} claims but {} index records",
            claims.len(),
            indices.len()
        )));
    }

    let mut locked_orders = Vec::with_capacity(claims.len());
    let mut lost_claim = None;
    for (claim, index) in claims.iter().zip(indices) {
        let context = format!(
            "Delivery warehouse district {} order {}",
            claim.district_id, claim.order_id
        );

        let earlier_queue_count = results.single_int32(index.earlier_queue_count)?;
        if earlier_queue_count != 0 {
            return Err(SemanticViolation::new(format!(
                "{context} has {earlier_queue_count} earlier queue rows; expected zero"
            )));
        }

        let exact_queue_count = results.single_int32(index.exact_queue_count)?;
        if exact_queue_count == 0 {
            lost_claim.get_or_insert(*claim);
            continue;
        }
        if exact_queue_count != 1 {
            return Err(SemanticViolation::new(format!(
                "{context} has {exact_queue_count} exact queue rows; expected one"
            )));
        }

        let order = results.single_row(index.order)?;
        if order.len() != 3 {
            return Err(SemanticViolation::new(format!(
                "{context} order lookup returned {} columns; expected three",
                order.len()
            )));
        }
        let customer_id = row_int32(order, 0, &format!("{context} order"))?;
        if customer_id <= 0 {
            return Err(SemanticViolation::new(format!(
                "{context} returned invalid customer id {customer_id}"
            )));
        }
        let carrier_id = row_int32(order, 1, &format!("{context} order"))?;
        if carrier_id != UNDELIVERED_CARRIER_ID {
            return Err(SemanticViolation::new(format!(
                "{context} has carrier id {carrier_id}; expected undelivered carrier id \
                 {UNDELIVERED_CARRIER_ID}"
            )));
        }
        let declared_line_count = row_int32(order, 2, &format!("{context} order"))?;
        let declared_line_count = usize::try_from(declared_line_count).map_err(|_| {
            SemanticViolation::new(format!(
                "{context} declared negative order-line count {declared_line_count}"
            ))
        })?;
        if !(MIN_ORDER_LINES..=MAX_ORDER_LINES).contains(&declared_line_count) {
            return Err(SemanticViolation::new(format!(
                "{context} declared {declared_line_count} order lines; expected \
                 {MIN_ORDER_LINES}..={MAX_ORDER_LINES}"
            )));
        }

        let line_rows = results.rows(index.line_rows)?;
        if !(MIN_ORDER_LINES..=MAX_ORDER_LINES).contains(&line_rows.len()) {
            return Err(SemanticViolation::new(format!(
                "{context} returned {} order lines; expected {MIN_ORDER_LINES}..={MAX_ORDER_LINES}",
                line_rows.len()
            )));
        }
        if line_rows.len() != declared_line_count {
            return Err(SemanticViolation::new(format!(
                "{context} declared {declared_line_count} order lines but returned {}",
                line_rows.len()
            )));
        }
        let mut amount_values = Vec::with_capacity(line_rows.len());
        for (offset, row) in line_rows.iter().enumerate() {
            if row.len() != 3 {
                return Err(SemanticViolation::new(format!(
                    "{context} line row {} returned {} columns; expected three",
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
            let line_order_id =
                row_int32(row, 2, &format!("{context} line {line_number} order id"))?;
            if line_order_id != claim.order_id {
                return Err(SemanticViolation::new(format!(
                    "{context} line {line_number} belongs to order {line_order_id}"
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
            line_count: line_rows.len() as u8,
            amount_bits,
            line_amount_bits: amount_values,
        });
    }

    Ok(match lost_claim {
        Some(claim) => StageTwoOutcome::ClaimLost(claim),
        None => StageTwoOutcome::Locked(locked_orders),
    })
}

fn stage_three_operations(
    warehouse_id: u16,
    carrier_id: u8,
    timestamp: &str,
    orders: &[LockedOrder],
) -> (Vec<Operation>, Vec<StageThreeIndex>) {
    let mut operations = Vec::with_capacity(orders.len() * 6 + 1);
    let mut indices = Vec::with_capacity(orders.len());

    for order in orders {
        let warehouse = WireValue::Int32(i32::from(warehouse_id));
        let district = WireValue::Int32(i32::from(order.claim.district_id));
        let order_id = WireValue::Int32(order.claim.order_id);
        let customer_id = WireValue::Int32(order.customer_id);
        let customer_before = operations.len();
        operations.push(operation(
            StatementId::DeliveryCustomer,
            [warehouse.clone(), district.clone(), customer_id.clone()],
        ));
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
                customer_id.clone(),
            ],
        ));
        let customer_after = operations.len();
        operations.push(operation(
            StatementId::DeliveryCustomerAfter,
            [warehouse, district, customer_id],
        ));
        indices.push(StageThreeIndex {
            customer_before,
            customer_after,
        });
    }

    operations.push(operation(StatementId::Commit, []));
    (operations, indices)
}

fn validate_stage_three(
    results: &BatchResults,
    orders: &[LockedOrder],
    indices: &[StageThreeIndex],
) -> SemanticResult<Vec<CustomerTransition>> {
    if orders.len() != indices.len() {
        return Err(SemanticViolation::new(format!(
            "Delivery stage-three planner retained {} orders but {} index records",
            orders.len(),
            indices.len()
        )));
    }

    let mut transitions = Vec::with_capacity(orders.len());
    for (order, index) in orders.iter().zip(indices) {
        let order_context = format!(
            "Delivery district {} order {} customer {}",
            order.claim.district_id, order.claim.order_id, order.customer_id
        );
        let before = parse_customer_snapshot(
            results.single_row(index.customer_before)?,
            &format!("{order_context} before update"),
        )?;
        let after = parse_customer_snapshot(
            results.single_row(index.customer_after)?,
            &format!("{order_context} after update"),
        )?;
        validate_customer_after(
            before.balance_bits,
            before.payment_count,
            before.delivery_count,
            order.amount_bits,
            after.balance_bits,
            after.payment_count,
            after.delivery_count,
            &order_context,
        )?;
        transitions.push(CustomerTransition { before, after });
    }
    Ok(transitions)
}

fn parse_customer_snapshot(row: &[WireValue], context: &str) -> SemanticResult<CustomerSnapshot> {
    if row.len() != 3 {
        return Err(SemanticViolation::new(format!(
            "{context} returned {} columns; expected three",
            row.len()
        )));
    }
    let balance_bits = row_f32_bits(row, 0, context)?;
    finite_f32(balance_bits, &format!("{context} balance"))?;
    let payment_count = row_int32(row, 1, context)?;
    let delivery_count = row_int32(row, 2, context)?;
    if payment_count < 0 || delivery_count < 0 {
        return Err(SemanticViolation::new(format!(
            "{context} has negative logical version ({payment_count},{delivery_count})"
        )));
    }
    Ok(CustomerSnapshot {
        balance_bits,
        payment_count,
        delivery_count,
    })
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
                    earlier_queue_count: 1,
                    exact_queue_count: 2,
                    order: 3,
                    line_rows: 4,
                    line_sum: 5,
                },
                StageTwoIndices {
                    earlier_queue_count: 7,
                    exact_queue_count: 8,
                    order: 9,
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
        assert_eq!(
            operations[1].statement_id,
            StatementId::DeliveryEarlierQueueCount.wire_id()
        );
        assert_eq!(
            operations[2].statement_id,
            StatementId::DeliveryExactQueueCount.wire_id()
        );
        assert_eq!(
            operations[3].statement_id,
            StatementId::DeliveryOrder.wire_id()
        );
    }

    #[test]
    fn stage_two_retains_each_raw_line_for_recovery_evidence() {
        let claims = [DistrictClaim {
            district_id: 2,
            order_id: 3001,
        }];
        let (operations, indices) = stage_two_operations(17, &claims);
        let amounts = [
            1.25_f32.to_bits(),
            2.5_f32.to_bits(),
            3.75_f32.to_bits(),
            4.0_f32.to_bits(),
            5.5_f32.to_bits(),
        ];
        let sum_bits = exact_f64_sum_to_f32_bits(&amounts).unwrap();
        let response = BatchResponse::Ok {
            executed_operations: operations.len() as u16,
            results: vec![
                query(1, vec![vec![WireValue::Int32(0)]]),
                query(2, vec![vec![WireValue::Int32(1)]]),
                query(
                    3,
                    vec![vec![
                        WireValue::Int32(42),
                        WireValue::Int32(UNDELIVERED_CARRIER_ID),
                        WireValue::Int32(amounts.len() as i32),
                    ]],
                ),
                query(
                    4,
                    amounts
                        .iter()
                        .enumerate()
                        .map(|(index, bits)| {
                            vec![
                                WireValue::Int32((index + 1) as i32),
                                WireValue::Float32(*bits),
                                WireValue::Int32(3001),
                            ]
                        })
                        .collect(),
                ),
                query(5, vec![vec![WireValue::Float32(sum_bits)]]),
            ],
        };
        let results = accept_batch(response, &operations).unwrap();
        let StageTwoOutcome::Locked(locked) =
            parse_stage_two(&results, &claims, &indices).unwrap()
        else {
            panic!("complete queue evidence must retain the locked order");
        };
        assert_eq!(locked.len(), 1);
        assert_eq!(locked[0].line_amount_bits, amounts);
        assert_eq!(locked[0].amount_bits, sum_bits);
    }

    #[test]
    fn one_lost_claim_discards_other_locked_districts() {
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
        let amounts = [1.0_f32.to_bits(); MIN_ORDER_LINES];
        let sum_bits = exact_f64_sum_to_f32_bits(&amounts).unwrap();
        let response = BatchResponse::Ok {
            executed_operations: operations.len() as u16,
            results: vec![
                query(1, vec![vec![WireValue::Int32(0)]]),
                query(2, vec![vec![WireValue::Int32(1)]]),
                query(
                    3,
                    vec![vec![
                        WireValue::Int32(42),
                        WireValue::Int32(UNDELIVERED_CARRIER_ID),
                        WireValue::Int32(MIN_ORDER_LINES as i32),
                    ]],
                ),
                query(
                    4,
                    amounts
                        .iter()
                        .enumerate()
                        .map(|(offset, bits)| {
                            vec![
                                WireValue::Int32((offset + 1) as i32),
                                WireValue::Float32(*bits),
                                WireValue::Int32(3001),
                            ]
                        })
                        .collect(),
                ),
                query(5, vec![vec![WireValue::Float32(sum_bits)]]),
                query(7, vec![vec![WireValue::Int32(0)]]),
                query(8, vec![vec![WireValue::Int32(0)]]),
                query(9, Vec::new()),
                query(10, Vec::new()),
                query(11, Vec::new()),
            ],
        };
        let results = accept_batch(response, &operations).unwrap();

        assert_eq!(
            parse_stage_two(&results, &claims, &indices).unwrap(),
            StageTwoOutcome::ClaimLost(claims[1])
        );
    }

    #[test]
    fn stage_two_rejects_non_oldest_or_mismatched_line_order() {
        let claims = [DistrictClaim {
            district_id: 2,
            order_id: 3001,
        }];
        let (operations, indices) = stage_two_operations(17, &claims);
        let amounts = [1.0_f32.to_bits(); MIN_ORDER_LINES];

        let response = |earlier_count, exact_count, carrier_id, declared_lines, line_order_id| {
            BatchResponse::Ok {
                executed_operations: operations.len() as u16,
                results: vec![
                    query(1, vec![vec![WireValue::Int32(earlier_count)]]),
                    query(2, vec![vec![WireValue::Int32(exact_count)]]),
                    query(
                        3,
                        vec![vec![
                            WireValue::Int32(42),
                            WireValue::Int32(carrier_id),
                            WireValue::Int32(declared_lines),
                        ]],
                    ),
                    query(
                        4,
                        amounts
                            .iter()
                            .enumerate()
                            .map(|(index, bits)| {
                                vec![
                                    WireValue::Int32((index + 1) as i32),
                                    WireValue::Float32(*bits),
                                    WireValue::Int32(line_order_id),
                                ]
                            })
                            .collect(),
                    ),
                    query(
                        5,
                        vec![vec![WireValue::Float32(
                            exact_f64_sum_to_f32_bits(&amounts).unwrap(),
                        )]],
                    ),
                ],
            }
        };

        let earlier = accept_batch(response(1, 0, 0, 5, 3001), &operations).unwrap();
        assert!(parse_stage_two(&earlier, &claims, &indices)
            .unwrap_err()
            .message()
            .contains("earlier queue rows"));

        let lost_exact = accept_batch(response(0, 0, 0, 5, 3001), &operations).unwrap();
        assert_eq!(
            parse_stage_two(&lost_exact, &claims, &indices).unwrap(),
            StageTwoOutcome::ClaimLost(claims[0])
        );

        let mut missing_exact_result = response(0, 1, 0, 5, 3001);
        let BatchResponse::Ok { results, .. } = &mut missing_exact_result else {
            unreachable!("test response is successful");
        };
        results.retain(|result| result.operation_index != 2);
        let missing_exact_result = accept_batch(missing_exact_result, &operations).unwrap();
        assert!(parse_stage_two(&missing_exact_result, &claims, &indices)
            .unwrap_err()
            .message()
            .contains("operation 2 has no query result"));

        let mut wrong_exact_type = response(0, 1, 0, 5, 3001);
        let BatchResponse::Ok { results, .. } = &mut wrong_exact_type else {
            unreachable!("test response is successful");
        };
        results
            .iter_mut()
            .find(|result| result.operation_index == 2)
            .unwrap()
            .rows[0][0] = WireValue::Char(b"1".to_vec());
        let wrong_exact_type = accept_batch(wrong_exact_type, &operations).unwrap();
        assert!(parse_stage_two(&wrong_exact_type, &claims, &indices)
            .unwrap_err()
            .message()
            .contains("expected INT32"));

        let duplicate_exact = accept_batch(response(0, 2, 0, 5, 3001), &operations).unwrap();
        assert!(parse_stage_two(&duplicate_exact, &claims, &indices)
            .unwrap_err()
            .message()
            .contains("exact queue rows"));

        let delivered = accept_batch(response(0, 1, 7, 5, 3001), &operations).unwrap();
        assert!(parse_stage_two(&delivered, &claims, &indices)
            .unwrap_err()
            .message()
            .contains("carrier id 7"));

        let wrong_declared_count = accept_batch(response(0, 1, 0, 6, 3001), &operations).unwrap();
        assert!(parse_stage_two(&wrong_declared_count, &claims, &indices)
            .unwrap_err()
            .message()
            .contains("declared 6 order lines but returned 5"));

        let wrong_line_order = accept_batch(response(0, 1, 0, 5, 3002), &operations).unwrap();
        assert!(parse_stage_two(&wrong_line_order, &claims, &indices)
            .unwrap_err()
            .message()
            .contains("belongs to order 3002"));
    }

    #[test]
    fn stage_three_reads_customer_before_mutation_and_checks_after_commit() {
        let order = LockedOrder {
            claim: DistrictClaim {
                district_id: 2,
                order_id: 3001,
            },
            customer_id: 42,
            line_count: 5,
            amount_bits: 2.5_f32.to_bits(),
            line_amount_bits: vec![0.5_f32.to_bits(); 5],
        };
        let (operations, indices) = stage_three_operations(17, 7, "2026-08-11", &[order.clone()]);
        assert_eq!(operations.len(), 7);
        assert_eq!(indices[0].customer_before, 0);
        assert_eq!(indices[0].customer_after, 5);
        assert_eq!(
            operations
                .iter()
                .map(|operation| operation.statement_id)
                .collect::<Vec<_>>(),
            vec![
                StatementId::DeliveryCustomer.wire_id(),
                StatementId::DeliveryDeleteQueue.wire_id(),
                StatementId::DeliveryUpdateOrder.wire_id(),
                StatementId::DeliveryUpdateLines.wire_id(),
                StatementId::DeliveryUpdateCustomer.wire_id(),
                StatementId::DeliveryCustomerAfter.wire_id(),
                StatementId::Commit.wire_id(),
            ]
        );

        let response = BatchResponse::Ok {
            executed_operations: operations.len() as u16,
            results: vec![
                query(
                    0,
                    vec![vec![
                        WireValue::Float32(10.0_f32.to_bits()),
                        WireValue::Int32(3),
                        WireValue::Int32(4),
                    ]],
                ),
                query(
                    5,
                    vec![vec![
                        WireValue::Float32(12.5_f32.to_bits()),
                        WireValue::Int32(3),
                        WireValue::Int32(5),
                    ]],
                ),
            ],
        };
        let results = accept_batch(response, &operations).unwrap();
        let transitions = validate_stage_three(&results, &[order], &indices).unwrap();
        assert_eq!(transitions[0].before.balance_bits, 10.0_f32.to_bits());
        assert_eq!(transitions[0].after.balance_bits, 12.5_f32.to_bits());
        assert_eq!(transitions[0].before.delivery_count, 4);
        assert_eq!(transitions[0].after.delivery_count, 5);
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
