//! Typed Wire-v3 executor for public-spec consistency plans.

use std::collections::BTreeMap;

use tracing::{info, warn};

use crate::connection::client::RmdbClient;
use crate::connection::wire::{StreamResponse, WireValue};
use crate::consistency::{
    float32_matches, float_aggregate_plan, public_online_integer_plan, recovery_partition_audits,
    recovery_plan, setup_plan, validate_crash_float_baseline, validate_increment_chain,
    validate_public_float_ledger, validate_relative_update_chain_from_initial, CheckQuery,
    CheckScope, ConsistencyPlan, FloatAggregateId, NonNegativeF32Accumulator, PartitionExpectation,
    PartitionKey, PublicFloatLedgerEvidence, RecoveryExpectations, RelativeUpdateEvidence,
    SetupExpectations, TypedResult, TypedValue, DISTRICTS_PER_WAREHOUSE, FINAL_WAREHOUSES,
    FLOAT_AGGREGATES, NEW_ORDERS_PER_DISTRICT, ORDERS_PER_DISTRICT, PUBLIC_SPEC_NOTICE,
};
use crate::error::TpccError;
use crate::ranking::ledger::{LedgerEvent, RunLedger};
use crate::run_state::DatasetState;

pub type FloatBaseline = BTreeMap<FloatAggregateId, u32>;

const PUBLIC_CUSTOMER_ENDPOINT_SAMPLE_LIMIT: usize = 64;

pub async fn run_setup(client: &mut RmdbClient, dataset: &DatasetState) -> Result<(), TpccError> {
    let plan = setup_plan(SetupExpectations {
        warehouses: dataset.warehouses,
        order_line_rows: dataset.order_line_rows,
        undelivered_order_line_rows: dataset.undelivered_order_line_rows,
    })
    .map_err(|error| TpccError::Protocol(format!("invalid setup plan: {error}")))?;
    run_plan(client, &plan).await
}

/// Execute the public final-2026 online gate with typed Wire values.
///
/// `ledger` must be the full physical projection (warmup, ranked commits, and
/// grace-tail commits), not only the ranked projection. The exact initial
/// order-line accumulator is produced while loading and is a separate input so
/// the runtime never uses the database query itself as its expected answer.
pub async fn run_final_online(
    client: &mut RmdbClient,
    dataset: &DatasetState,
    ledger: &RunLedger,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
) -> Result<FloatBaseline, TpccError> {
    warn!("{PUBLIC_SPEC_NOTICE}");
    let expectations = final_expectations(dataset, ledger, initial_order_line_amounts)?;
    let plan = public_online_integer_plan(expectations)
        .map_err(|error| protocol_error("invalid public online plan", error))?;
    run_plan(client, &plan).await?;

    validate_transaction_evidence(ledger)?;
    let values = read_float_aggregates(client, CheckScope::Online).await?;
    validate_online_float_ledger(dataset, ledger, initial_order_line_amounts, &values)?;
    info!(
        "public online consistency PASS; hidden official 6/37 SQL, keys, seed, and answers were not inferred"
    );
    Ok(values)
}

/// Execute the public final-2026 post-crash gate.
///
/// This reruns full public integer checks, compares all seven raw FLOAT32
/// aggregate values to the online baseline with the per-category ULP limits,
/// verifies Payment warehouse/district endpoints at 0 ULP, and audits all 500
/// warehouse/district partitions in six grouped round trips.
pub async fn run_final_recovery(
    client: &mut RmdbClient,
    dataset: &DatasetState,
    ledger: &RunLedger,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
    online_baseline: &FloatBaseline,
) -> Result<(), TpccError> {
    warn!("{PUBLIC_SPEC_NOTICE}");
    let expectations = final_expectations(dataset, ledger, initial_order_line_amounts)?;
    let plan = recovery_plan(expectations)
        .map_err(|error| protocol_error("invalid public recovery plan", error))?;
    run_plan(client, &plan).await?;

    let endpoints = validate_transaction_evidence(ledger)?;
    let recovered = read_float_aggregates(client, CheckScope::Recovery).await?;
    validate_float_baseline(online_baseline, &recovered)?;
    validate_payment_endpoints(client, &endpoints).await?;
    validate_customer_endpoint_sample(client, &endpoints).await?;

    let partitions = partition_expectations(dataset, ledger)?;
    // Reuse the transport-neutral generator as a strict completeness and
    // invariant validator, but execute the equivalent audit in six grouped
    // queries rather than 3,000 scalar requests.
    recovery_partition_audits(partitions.clone())
        .map_err(|error| protocol_error("invalid 500-partition ledger", error))?;
    run_grouped_partition_audit(client, &partitions).await?;
    info!(
        "public recovery consistency PASS; hidden official 37 SQL, generated keys, seed, and answers remain unavailable"
    );
    Ok(())
}

pub async fn run_plan(client: &mut RmdbClient, plan: &ConsistencyPlan) -> Result<(), TpccError> {
    warn!("{PUBLIC_SPEC_NOTICE}");
    for query in &plan.queries {
        let result = execute_query(client, query).await?;
        query.validate(&result).map_err(|error| {
            TpccError::QueryError(format!(
                "consistency check {} ({}) failed: {error}",
                query.id, query.description
            ))
        })?;
        info!("consistency PASS: {} — {}", query.id, query.description);
    }
    Ok(())
}

pub async fn read_float_aggregates(
    client: &mut RmdbClient,
    scope: CheckScope,
) -> Result<FloatBaseline, TpccError> {
    let plan = float_aggregate_plan(scope);
    let mut values = BTreeMap::new();
    for (spec, query) in FLOAT_AGGREGATES.iter().zip(&plan.queries) {
        let result = execute_query(client, query).await?;
        query.validate(&result).map_err(|error| {
            TpccError::QueryError(format!(
                "FLOAT32 consistency check {} failed: {error}",
                query.id
            ))
        })?;
        let bits = match result.rows.as_slice() {
            [row] => match row.as_slice() {
                [TypedValue::Float32(bits)] => *bits,
                _ => unreachable!("validated FLOAT32 scalar shape"),
            },
            _ => unreachable!("validated FLOAT32 scalar shape"),
        };
        values.insert(spec.id, bits);
    }
    Ok(values)
}

fn final_expectations(
    dataset: &DatasetState,
    ledger: &RunLedger,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
) -> Result<RecoveryExpectations, TpccError> {
    if dataset.warehouses != FINAL_WAREHOUSES {
        return Err(TpccError::Protocol(format!(
            "final-2026 consistency requires {FINAL_WAREHOUSES} warehouses, state has {}",
            dataset.warehouses
        )));
    }
    let initial_terms = u64::try_from(dataset.order_line_rows).map_err(|_| {
        TpccError::Protocol("dataset order-line count is negative or too large".to_owned())
    })?;
    if initial_order_line_amounts.term_count() != initial_terms {
        return Err(TpccError::Protocol(format!(
            "initial order-line FLOAT accumulator has {} terms, dataset records {initial_terms}",
            initial_order_line_amounts.term_count()
        )));
    }
    Ok(RecoveryExpectations {
        setup: SetupExpectations {
            warehouses: dataset.warehouses,
            order_line_rows: dataset.order_line_rows,
            undelivered_order_line_rows: dataset.undelivered_order_line_rows,
        },
        committed: ledger.to_committed_ledger(),
    })
}

#[derive(Debug, Default)]
struct RelativeEndpoints {
    warehouses: BTreeMap<i32, u32>,
    districts: BTreeMap<(i32, i32), u32>,
    customers: BTreeMap<CustomerKey, CustomerEndpoint>,
}

type CustomerKey = (i32, i32, i32);

#[derive(Clone, Copy, Debug)]
struct CustomerEndpoint {
    balance_bits: u32,
    ytd_payment_bits: u32,
    payment_count: i32,
    delivery_count: i32,
}

impl Default for CustomerEndpoint {
    fn default() -> Self {
        Self {
            balance_bits: (-10.0_f32).to_bits(),
            ytd_payment_bits: 10.0_f32.to_bits(),
            payment_count: 1,
            delivery_count: 0,
        }
    }
}

fn validate_transaction_evidence(ledger: &RunLedger) -> Result<RelativeEndpoints, TpccError> {
    let mut warehouse_updates: BTreeMap<i32, Vec<RelativeUpdateEvidence>> = BTreeMap::new();
    let mut district_updates: BTreeMap<(i32, i32), Vec<RelativeUpdateEvidence>> = BTreeMap::new();
    let mut customer_balance_updates: BTreeMap<CustomerKey, Vec<RelativeUpdateEvidence>> =
        BTreeMap::new();
    let mut customer_ytd_updates: BTreeMap<CustomerKey, Vec<RelativeUpdateEvidence>> =
        BTreeMap::new();
    let mut customer_payment_counts: BTreeMap<CustomerKey, Vec<(i32, i32)>> = BTreeMap::new();
    let mut customer_delivery_counts: BTreeMap<CustomerKey, Vec<(i32, i32)>> = BTreeMap::new();

    for event in ledger.events() {
        match event {
            LedgerEvent::Payment(delta) => {
                let amount = delta.amount_bits;
                warehouse_updates
                    .entry(i32::from(delta.warehouse_id))
                    .or_default()
                    .push(RelativeUpdateEvidence {
                        before_bits: delta.warehouse_before_bits,
                        bound_amount_bits: amount,
                        after_bits: delta.warehouse_after_bits,
                    });
                district_updates
                    .entry((i32::from(delta.warehouse_id), i32::from(delta.district_id)))
                    .or_default()
                    .push(RelativeUpdateEvidence {
                        before_bits: delta.district_before_bits,
                        bound_amount_bits: amount,
                        after_bits: delta.district_after_bits,
                    });
                let customer = (
                    i32::from(delta.customer_warehouse_id),
                    i32::from(delta.customer_district_id),
                    delta.customer_id,
                );
                customer_balance_updates.entry(customer).or_default().push(
                    RelativeUpdateEvidence {
                        before_bits: delta.customer_balance_before_bits,
                        bound_amount_bits: amount ^ 0x8000_0000,
                        after_bits: delta.customer_balance_after_bits,
                    },
                );
                customer_ytd_updates
                    .entry(customer)
                    .or_default()
                    .push(RelativeUpdateEvidence {
                        before_bits: delta.customer_ytd_before_bits,
                        bound_amount_bits: amount,
                        after_bits: delta.customer_ytd_after_bits,
                    });
                customer_payment_counts.entry(customer).or_default().push((
                    delta.customer_payment_count_before,
                    delta.customer_payment_count_after,
                ));
            }
            LedgerEvent::Delivery(delta) => {
                for order in &delta.orders {
                    let customer = (
                        i32::from(delta.warehouse_id),
                        i32::from(order.district_id),
                        order.customer_id,
                    );
                    customer_balance_updates.entry(customer).or_default().push(
                        RelativeUpdateEvidence {
                            before_bits: order.customer_balance_before_bits,
                            bound_amount_bits: order.customer_amount_bits,
                            after_bits: order.customer_balance_after_bits,
                        },
                    );
                    customer_delivery_counts.entry(customer).or_default().push((
                        order.customer_delivery_count_before,
                        order.customer_delivery_count_after,
                    ));
                }
            }
            LedgerEvent::NewOrder(_)
            | LedgerEvent::OrderStatus { .. }
            | LedgerEvent::StockLevel { .. }
            | LedgerEvent::ExpectedRollback { .. } => {}
        }
    }

    let mut endpoints = RelativeEndpoints::default();
    for (warehouse, updates) in warehouse_updates {
        let endpoint =
            validate_relative_update_chain_from_initial(300_000.0_f32.to_bits(), &updates)
                .map_err(|error| {
                    TpccError::QueryError(format!(
                        "Payment w_ytd chain for warehouse {warehouse} failed: {error}"
                    ))
                })?;
        endpoints.warehouses.insert(warehouse, endpoint);
    }
    for ((warehouse, district), updates) in district_updates {
        let endpoint =
            validate_relative_update_chain_from_initial(30_000.0_f32.to_bits(), &updates)
                .map_err(|error| {
                    TpccError::QueryError(format!(
                        "Payment d_ytd chain for warehouse {warehouse}, district {district} failed: {error}"
                    ))
                })?;
        endpoints.districts.insert((warehouse, district), endpoint);
    }
    for ((warehouse, district, customer), updates) in customer_balance_updates {
        let endpoint =
            validate_relative_update_chain_from_initial((-10.0_f32).to_bits(), &updates).map_err(
                |error| {
                TpccError::QueryError(format!(
                    "Payment/Delivery c_balance chain for ({warehouse},{district},{customer}) failed: {error}"
                ))
                },
            )?;
        endpoints
            .customers
            .entry((warehouse, district, customer))
            .or_default()
            .balance_bits = endpoint;
    }
    for ((warehouse, district, customer), updates) in customer_ytd_updates {
        let endpoint =
            validate_relative_update_chain_from_initial(10.0_f32.to_bits(), &updates).map_err(
                |error| {
                TpccError::QueryError(format!(
                    "Payment c_ytd_payment chain for ({warehouse},{district},{customer}) failed: {error}"
                ))
                },
            )?;
        endpoints
            .customers
            .entry((warehouse, district, customer))
            .or_default()
            .ytd_payment_bits = endpoint;
    }
    for ((warehouse, district, customer), updates) in customer_payment_counts {
        let endpoint = validate_increment_chain(1, &updates).map_err(|error| {
            TpccError::QueryError(format!(
                "Payment c_payment_cnt chain for ({warehouse},{district},{customer}) failed: {error}"
            ))
        })?;
        endpoints
            .customers
            .entry((warehouse, district, customer))
            .or_default()
            .payment_count = endpoint;
    }
    for ((warehouse, district, customer), updates) in customer_delivery_counts {
        let endpoint = validate_increment_chain(0, &updates).map_err(|error| {
            TpccError::QueryError(format!(
                "Delivery c_delivery_cnt chain for ({warehouse},{district},{customer}) failed: {error}"
            ))
        })?;
        endpoints
            .customers
            .entry((warehouse, district, customer))
            .or_default()
            .delivery_count = endpoint;
    }
    Ok(endpoints)
}

fn validate_online_float_ledger(
    dataset: &DatasetState,
    ledger: &RunLedger,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
    values: &FloatBaseline,
) -> Result<(), TpccError> {
    let initial_history_rows = i64::from(dataset.warehouses)
        .checked_mul(i64::from(DISTRICTS_PER_WAREHOUSE))
        .and_then(|count| count.checked_mul(3_000))
        .ok_or_else(|| TpccError::Protocol("initial history row count overflowed".to_owned()))?;
    let mut history = NonNegativeF32Accumulator::default();
    history
        .add_repeated_bits(
            10.0_f32.to_bits(),
            u64::try_from(initial_history_rows)
                .map_err(|_| TpccError::Protocol("negative history row count".to_owned()))?,
        )
        .map_err(|error| protocol_error("initial history accumulator failed", error))?;
    history
        .extend_bits(ledger.payment_amount_bits().iter().copied())
        .map_err(|error| protocol_error("Payment history accumulator failed", error))?;

    let mut order_line = initial_order_line_amounts.clone();
    order_line
        .extend_bits(ledger.new_order_line_amount_bits().iter().copied())
        .map_err(|error| protocol_error("order-line accumulator failed", error))?;

    let stock_ytd = ledger.stock_ytd_delta() as f32;
    if !stock_ytd.is_finite() {
        return Err(TpccError::Protocol(
            "stock YTD ledger cannot be represented as finite FLOAT32".to_owned(),
        ));
    }
    validate_public_float_ledger(
        aggregate_bits(values, FloatAggregateId::HistoryAmount)?,
        aggregate_bits(values, FloatAggregateId::StockYtd)?,
        aggregate_bits(values, FloatAggregateId::OrderLineAmount)?,
        PublicFloatLedgerEvidence {
            history_amount: history
                .boundary()
                .map_err(|error| protocol_error("history boundary failed", error))?,
            stock_ytd_bits: stock_ytd.to_bits(),
            order_line_amount: order_line
                .boundary()
                .map_err(|error| protocol_error("order-line boundary failed", error))?,
        },
    )
    .map_err(|error| TpccError::QueryError(format!("public FLOAT ledger gate failed: {error}")))
}

fn validate_float_baseline(before: &FloatBaseline, after: &FloatBaseline) -> Result<(), TpccError> {
    if before.len() != FLOAT_AGGREGATES.len() || after.len() != FLOAT_AGGREGATES.len() {
        return Err(TpccError::Protocol(format!(
            "FLOAT baseline must contain exactly {} categories (before={}, after={})",
            FLOAT_AGGREGATES.len(),
            before.len(),
            after.len()
        )));
    }
    for spec in FLOAT_AGGREGATES {
        let before_bits = aggregate_bits(before, spec.id)?;
        let after_bits = aggregate_bits(after, spec.id)?;
        validate_crash_float_baseline(spec, before_bits, after_bits).map_err(|error| {
            TpccError::QueryError(format!(
                "post-crash {} comparison failed: {error}",
                spec.description
            ))
        })?;
    }
    Ok(())
}

fn aggregate_bits(values: &FloatBaseline, id: FloatAggregateId) -> Result<u32, TpccError> {
    values
        .get(&id)
        .copied()
        .ok_or_else(|| TpccError::Protocol(format!("FLOAT baseline is missing aggregate {id:?}")))
}

fn require_zero_ulp(name: &str, expected_bits: u32, actual_bits: u32) -> Result<(), TpccError> {
    if float32_matches(expected_bits, actual_bits, 0) {
        Ok(())
    } else {
        Err(TpccError::QueryError(format!(
            "{name} expected 0x{expected_bits:08x}, got 0x{actual_bits:08x} (0 ULP)"
        )))
    }
}

async fn validate_payment_endpoints(
    client: &mut RmdbClient,
    endpoints: &RelativeEndpoints,
) -> Result<(), TpccError> {
    let warehouses = execute_typed_sql(
        client,
        "recovery.payment.warehouse_endpoints",
        "SELECT w_id, w_ytd FROM warehouse",
    )
    .await?;
    validate_float_endpoint_rows(
        "warehouse w_ytd",
        warehouses,
        (1..=FINAL_WAREHOUSES).map(|warehouse| {
            (
                PartitionKey {
                    warehouse_id: warehouse,
                    district_id: 0,
                },
                endpoints
                    .warehouses
                    .get(&warehouse)
                    .copied()
                    .unwrap_or(300_000.0_f32.to_bits()),
            )
        }),
        false,
    )?;

    let districts = execute_typed_sql(
        client,
        "recovery.payment.district_endpoints",
        "SELECT d_w_id, d_id, d_ytd FROM district",
    )
    .await?;
    validate_float_endpoint_rows(
        "district d_ytd",
        districts,
        (1..=FINAL_WAREHOUSES).flat_map(|warehouse| {
            (1..=DISTRICTS_PER_WAREHOUSE).map(move |district| {
                (
                    PartitionKey {
                        warehouse_id: warehouse,
                        district_id: district,
                    },
                    endpoints
                        .districts
                        .get(&(warehouse, district))
                        .copied()
                        .unwrap_or(30_000.0_f32.to_bits()),
                )
            })
        }),
        true,
    )?;
    info!("recovery Payment warehouse/district endpoints PASS (0 ULP)");
    Ok(())
}

async fn validate_customer_endpoint_sample(
    client: &mut RmdbClient,
    endpoints: &RelativeEndpoints,
) -> Result<(), TpccError> {
    let sample = evenly_spaced_customer_sample(&endpoints.customers);
    for (ordinal, (key, endpoint)) in sample.iter().enumerate() {
        let result = execute_typed_sql(
            client,
            &format!("recovery.customer.sample.{}", ordinal + 1),
            &format!(
                "SELECT c_w_id, c_d_id, c_id, c_balance, c_ytd_payment, c_payment_cnt, \
                 c_delivery_cnt FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                key.0, key.1, key.2
            ),
        )
        .await?;
        let expected = BTreeMap::from([(*key, *endpoint)]);
        validate_customer_endpoint_rows("recovery", result, &expected)?;
    }
    info!(
        "recovery customer ledger sample PASS ({} composite-index keys, raw FLOAT32 and counters)",
        sample.len()
    );
    Ok(())
}

fn evenly_spaced_customer_sample(
    expected: &BTreeMap<CustomerKey, CustomerEndpoint>,
) -> Vec<(CustomerKey, CustomerEndpoint)> {
    let count = expected.len().min(PUBLIC_CUSTOMER_ENDPOINT_SAMPLE_LIMIT);
    if count == 0 {
        return Vec::new();
    }
    if count == expected.len() {
        return expected.iter().map(|(key, value)| (*key, *value)).collect();
    }

    let last_index = expected.len() - 1;
    let denominator = count - 1;
    let selected_indexes = (0..count)
        .map(|ordinal| ordinal * last_index / denominator)
        .collect::<Vec<_>>();
    let mut selected = Vec::with_capacity(count);
    let mut next = selected_indexes.into_iter();
    let mut selected_index = next.next();
    for (index, (key, value)) in expected.iter().enumerate() {
        if selected_index == Some(index) {
            selected.push((*key, *value));
            selected_index = next.next();
        }
    }
    selected
}

fn validate_customer_endpoint_rows(
    scope: &str,
    result: TypedResult,
    expected: &BTreeMap<CustomerKey, CustomerEndpoint>,
) -> Result<(), TpccError> {
    let mut actual = BTreeMap::new();
    for row in result.rows {
        let endpoint = match row.as_slice() {
            [TypedValue::Int32(warehouse), TypedValue::Int32(district), TypedValue::Int32(customer), TypedValue::Float32(balance_bits), TypedValue::Float32(ytd_payment_bits), TypedValue::Int32(payment_count), TypedValue::Int32(delivery_count)] => {
                (
                    (*warehouse, *district, *customer),
                    CustomerEndpoint {
                        balance_bits: *balance_bits,
                        ytd_payment_bits: *ytd_payment_bits,
                        payment_count: *payment_count,
                        delivery_count: *delivery_count,
                    },
                )
            }
            _ => {
                return Err(TpccError::Protocol(format!(
                    "{scope} changed-customer endpoint query returned an invalid typed row"
                )));
            }
        };
        let (key, value) = endpoint;
        if !expected.contains_key(&key) {
            return Err(TpccError::QueryError(format!(
                "{scope} changed-customer endpoint query returned unexpected key ({},{},{})",
                key.0, key.1, key.2
            )));
        }
        if actual.insert(key, value).is_some() {
            return Err(TpccError::Protocol(format!(
                "{scope} changed-customer endpoint query returned duplicate key ({},{},{})",
                key.0, key.1, key.2
            )));
        }
    }

    for (key, expected_endpoint) in expected {
        let actual_endpoint = actual.get(key).ok_or_else(|| {
            TpccError::QueryError(format!(
                "{scope} changed-customer endpoint query omitted key ({},{},{})",
                key.0, key.1, key.2
            ))
        })?;
        require_zero_ulp(
            &format!("{scope} customer ({},{},{}) c_balance", key.0, key.1, key.2),
            expected_endpoint.balance_bits,
            actual_endpoint.balance_bits,
        )?;
        require_zero_ulp(
            &format!(
                "{scope} customer ({},{},{}) c_ytd_payment",
                key.0, key.1, key.2
            ),
            expected_endpoint.ytd_payment_bits,
            actual_endpoint.ytd_payment_bits,
        )?;
        if actual_endpoint.payment_count != expected_endpoint.payment_count
            || actual_endpoint.delivery_count != expected_endpoint.delivery_count
        {
            return Err(TpccError::QueryError(format!(
                "{scope} customer ({},{},{}) counters expected payment/delivery={}/{}, got {}/{}",
                key.0,
                key.1,
                key.2,
                expected_endpoint.payment_count,
                expected_endpoint.delivery_count,
                actual_endpoint.payment_count,
                actual_endpoint.delivery_count
            )));
        }
    }
    Ok(())
}

fn validate_float_endpoint_rows<I>(
    name: &str,
    result: TypedResult,
    expected: I,
    has_district_column: bool,
) -> Result<(), TpccError>
where
    I: IntoIterator<Item = (PartitionKey, u32)>,
{
    let expected = expected.into_iter().collect::<BTreeMap<_, _>>();
    let mut actual = BTreeMap::new();
    for row in result.rows {
        let (key, bits) = match (has_district_column, row.as_slice()) {
            (false, [TypedValue::Int32(warehouse), TypedValue::Float32(bits)]) => (
                PartitionKey {
                    warehouse_id: *warehouse,
                    district_id: 0,
                },
                *bits,
            ),
            (
                true,
                [TypedValue::Int32(warehouse), TypedValue::Int32(district), TypedValue::Float32(bits)],
            ) => (
                PartitionKey {
                    warehouse_id: *warehouse,
                    district_id: *district,
                },
                *bits,
            ),
            _ => {
                return Err(TpccError::Protocol(format!(
                    "{name} endpoint query returned an invalid typed row"
                )));
            }
        };
        if !expected.contains_key(&key) {
            return Err(TpccError::QueryError(format!(
                "{name} endpoint query returned unexpected key ({},{})",
                key.warehouse_id, key.district_id
            )));
        }
        if actual.insert(key, bits).is_some() {
            return Err(TpccError::Protocol(format!(
                "{name} endpoint query returned duplicate key ({},{})",
                key.warehouse_id, key.district_id
            )));
        }
    }
    for (key, expected_bits) in expected {
        let actual_bits = actual.get(&key).copied().ok_or_else(|| {
            TpccError::QueryError(format!(
                "{name} endpoint query omitted key ({},{})",
                key.warehouse_id, key.district_id
            ))
        })?;
        require_zero_ulp(name, expected_bits, actual_bits)?;
    }
    Ok(())
}

fn partition_expectations(
    dataset: &DatasetState,
    ledger: &RunLedger,
) -> Result<Vec<PartitionExpectation>, TpccError> {
    if dataset.partitions.len() != (FINAL_WAREHOUSES * DISTRICTS_PER_WAREHOUSE) as usize {
        return Err(TpccError::Protocol(format!(
            "final recovery requires 500 load partitions, state has {}",
            dataset.partitions.len()
        )));
    }
    dataset
        .partitions
        .iter()
        .map(|initial| {
            let key = PartitionKey {
                warehouse_id: initial.warehouse_id,
                district_id: initial.district_id,
            };
            let delta = ledger
                .partition_delta(key.warehouse_id, key.district_id)
                .map_err(|error| protocol_error("invalid ledger partition", error))?;
            let order_count = checked_partition_add(
                i64::from(ORDERS_PER_DISTRICT),
                delta.new_orders,
                "partition order count",
            )?;
            let order_line_count = checked_partition_add(
                initial.order_line_rows,
                delta.new_order_lines,
                "partition order-line count",
            )?;
            let new_order_count = checked_partition_add(
                checked_partition_add(
                    i64::from(NEW_ORDERS_PER_DISTRICT),
                    delta.new_orders,
                    "partition new-order count",
                )?,
                -delta.delivered_orders,
                "partition new-order count",
            )?;
            let empty_delivery_time_count = checked_partition_add(
                checked_partition_add(
                    initial.undelivered_order_line_rows,
                    delta.new_order_lines,
                    "partition empty delivery count",
                )?,
                -delta.delivered_order_lines,
                "partition empty delivery count",
            )?;
            let next_order_id = checked_partition_add(order_count, 1, "partition next order id")?;
            Ok(PartitionExpectation {
                key,
                order_count,
                order_line_count,
                new_order_count,
                empty_delivery_time_count,
                carrier_zero_count: new_order_count,
                next_order_id,
            })
        })
        .collect()
}

fn checked_partition_add(left: i64, right: i64, name: &str) -> Result<i64, TpccError> {
    left.checked_add(right)
        .filter(|value| *value >= 0)
        .ok_or_else(|| TpccError::Protocol(format!("{name} overflowed or became negative")))
}

#[derive(Clone, Copy)]
enum PartitionMetric {
    Orders,
    OrderLines,
    NewOrders,
    EmptyDeliveryTimes,
    CarrierZero,
}

impl PartitionMetric {
    fn expected(self, partition: PartitionExpectation) -> i64 {
        match self {
            Self::Orders => partition.order_count,
            Self::OrderLines => partition.order_line_count,
            Self::NewOrders => partition.new_order_count,
            Self::EmptyDeliveryTimes => partition.empty_delivery_time_count,
            Self::CarrierZero => partition.carrier_zero_count,
        }
    }
}

async fn run_grouped_partition_audit(
    client: &mut RmdbClient,
    partitions: &[PartitionExpectation],
) -> Result<(), TpccError> {
    for (id, sql, metric) in [
        (
            "recovery.partition.grouped.orders",
            "SELECT o_w_id, o_d_id, COUNT(*) FROM orders GROUP BY o_w_id, o_d_id",
            PartitionMetric::Orders,
        ),
        (
            "recovery.partition.grouped.order_lines",
            "SELECT ol_w_id, ol_d_id, COUNT(*) FROM order_line GROUP BY ol_w_id, ol_d_id",
            PartitionMetric::OrderLines,
        ),
        (
            "recovery.partition.grouped.new_orders",
            "SELECT no_w_id, no_d_id, COUNT(*) FROM new_orders GROUP BY no_w_id, no_d_id",
            PartitionMetric::NewOrders,
        ),
        (
            "recovery.partition.grouped.empty_delivery_times",
            "SELECT ol_w_id, ol_d_id, COUNT(*) FROM order_line WHERE ol_delivery_d = '' GROUP BY ol_w_id, ol_d_id",
            PartitionMetric::EmptyDeliveryTimes,
        ),
        (
            "recovery.partition.grouped.carrier_zero",
            "SELECT o_w_id, o_d_id, COUNT(*) FROM orders WHERE o_carrier_id = 0 GROUP BY o_w_id, o_d_id",
            PartitionMetric::CarrierZero,
        ),
    ] {
        let result = execute_typed_sql(client, id, sql).await?;
        validate_grouped_partition_counts(id, result, partitions, metric)?;
        info!("consistency PASS: {id} (500 partitions in one typed response)");
    }

    let result = execute_typed_sql(
        client,
        "recovery.partition.grouped.next_order_id",
        "SELECT d_w_id, d_id, d_next_o_id FROM district",
    )
    .await?;
    validate_partition_next_order_ids(result, partitions)?;
    info!(
        "consistency PASS: recovery.partition.grouped.next_order_id (500 partitions in one typed response)"
    );
    Ok(())
}

fn validate_grouped_partition_counts(
    id: &str,
    result: TypedResult,
    partitions: &[PartitionExpectation],
    metric: PartitionMetric,
) -> Result<(), TpccError> {
    let expected = partitions
        .iter()
        .copied()
        .map(|partition| (partition.key, metric.expected(partition)))
        .collect::<BTreeMap<_, _>>();
    let mut actual = BTreeMap::new();
    for row in result.rows {
        let (warehouse, district, count) = match row.as_slice() {
            [TypedValue::Int32(warehouse), TypedValue::Int32(district), TypedValue::Int32(count)] => {
                (*warehouse, *district, i64::from(*count))
            }
            _ => {
                return Err(TpccError::Protocol(format!(
                    "{id} returned a row other than INT32,INT32,INT32"
                )));
            }
        };
        let key = PartitionKey {
            warehouse_id: warehouse,
            district_id: district,
        };
        if !expected.contains_key(&key) {
            return Err(TpccError::QueryError(format!(
                "{id} returned unexpected partition ({warehouse},{district})"
            )));
        }
        if count < 0 || actual.insert(key, count).is_some() {
            return Err(TpccError::Protocol(format!(
                "{id} returned a negative count or duplicate partition ({warehouse},{district})"
            )));
        }
    }
    for (key, expected_count) in expected {
        // SQL GROUP BY legitimately omits a zero-row group.
        let actual_count = actual.get(&key).copied().unwrap_or(0);
        if actual_count != expected_count {
            return Err(TpccError::QueryError(format!(
                "{id} partition ({},{}) expected {expected_count}, got {actual_count}",
                key.warehouse_id, key.district_id
            )));
        }
    }
    Ok(())
}

fn validate_partition_next_order_ids(
    result: TypedResult,
    partitions: &[PartitionExpectation],
) -> Result<(), TpccError> {
    let expected = partitions
        .iter()
        .map(|partition| (partition.key, partition.next_order_id))
        .collect::<BTreeMap<_, _>>();
    let mut actual = BTreeMap::new();
    for row in result.rows {
        let (warehouse, district, next_order_id) = match row.as_slice() {
            [TypedValue::Int32(warehouse), TypedValue::Int32(district), TypedValue::Int32(next_order_id)] => {
                (*warehouse, *district, i64::from(*next_order_id))
            }
            _ => {
                return Err(TpccError::Protocol(
                    "district endpoint query returned a row other than INT32,INT32,INT32"
                        .to_owned(),
                ));
            }
        };
        let key = PartitionKey {
            warehouse_id: warehouse,
            district_id: district,
        };
        if !expected.contains_key(&key) || actual.insert(key, next_order_id).is_some() {
            return Err(TpccError::Protocol(format!(
                "district endpoint query returned unexpected/duplicate partition ({warehouse},{district})"
            )));
        }
    }
    for (key, expected_value) in expected {
        let actual_value = actual.get(&key).copied().ok_or_else(|| {
            TpccError::QueryError(format!(
                "district endpoint query omitted partition ({},{})",
                key.warehouse_id, key.district_id
            ))
        })?;
        if actual_value != expected_value {
            return Err(TpccError::QueryError(format!(
                "district ({},{}) d_next_o_id expected {expected_value}, got {actual_value}",
                key.warehouse_id, key.district_id
            )));
        }
    }
    Ok(())
}

async fn execute_typed_sql(
    client: &mut RmdbClient,
    id: &str,
    sql: &str,
) -> Result<TypedResult, TpccError> {
    match client.exec_stream(&terminated_sql(sql)).await? {
        StreamResponse::Query { rows, .. } => Ok(TypedResult {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(typed_value).collect())
                .collect(),
        }),
        StreamResponse::CommandOk => Err(TpccError::Protocol(format!(
            "consistency query {id} returned COMMAND_OK"
        ))),
        StreamResponse::TransactionAbort { diagnostic } => Err(TpccError::Abort(format!(
            "consistency query {id} aborted: {diagnostic}"
        ))),
        StreamResponse::Error { diagnostic } => Err(TpccError::QueryError(format!(
            "consistency query {id} failed: {diagnostic}"
        ))),
    }
}

async fn execute_query(
    client: &mut RmdbClient,
    query: &CheckQuery,
) -> Result<TypedResult, TpccError> {
    execute_typed_sql(client, &query.id, &query.sql).await
}

fn protocol_error(context: &str, error: impl std::fmt::Display) -> TpccError {
    TpccError::Protocol(format!("{context}: {error}"))
}

fn typed_value(value: WireValue) -> TypedValue {
    match value {
        WireValue::Null => TypedValue::Null,
        WireValue::Int32(value) => TypedValue::Int32(value),
        WireValue::Float32(bits) => TypedValue::Float32(bits),
        WireValue::Char(bytes) => TypedValue::Char(bytes),
    }
}

fn terminated_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.ends_with(';') {
        trimmed.to_owned()
    } else {
        format!("{trimmed};")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_is_terminated_once_and_float_bits_are_not_formatted() {
        assert_eq!(
            terminated_sql(" SELECT COUNT(*) FROM item "),
            "SELECT COUNT(*) FROM item;"
        );
        assert_eq!(terminated_sql("show tables;"), "show tables;");
        assert_eq!(
            typed_value(WireValue::Float32(0x7f7f_ffff)),
            TypedValue::Float32(0x7f7f_ffff)
        );
    }

    fn partition(
        warehouse_id: i32,
        district_id: i32,
        new_order_count: i64,
    ) -> PartitionExpectation {
        PartitionExpectation {
            key: PartitionKey {
                warehouse_id,
                district_id,
            },
            order_count: 3_001,
            order_line_count: 30_010,
            new_order_count,
            empty_delivery_time_count: new_order_count * 10,
            carrier_zero_count: new_order_count,
            next_order_id: 3_002,
        }
    }

    #[test]
    fn grouped_partition_counts_accept_zero_group_omission() {
        let expected = [partition(1, 1, 4), partition(1, 2, 0)];
        let result = TypedResult {
            rows: vec![vec![
                TypedValue::Int32(1),
                TypedValue::Int32(1),
                TypedValue::Int32(4),
            ]],
        };
        assert!(validate_grouped_partition_counts(
            "test",
            result,
            &expected,
            PartitionMetric::NewOrders
        )
        .is_ok());
    }

    #[test]
    fn grouped_partition_counts_reject_duplicates_and_mismatches() {
        let expected = [partition(1, 1, 4)];
        let duplicate = TypedResult {
            rows: vec![
                vec![
                    TypedValue::Int32(1),
                    TypedValue::Int32(1),
                    TypedValue::Int32(4),
                ],
                vec![
                    TypedValue::Int32(1),
                    TypedValue::Int32(1),
                    TypedValue::Int32(4),
                ],
            ],
        };
        assert!(validate_grouped_partition_counts(
            "test",
            duplicate,
            &expected,
            PartitionMetric::NewOrders
        )
        .is_err());

        let mismatch = TypedResult {
            rows: vec![vec![
                TypedValue::Int32(1),
                TypedValue::Int32(1),
                TypedValue::Int32(3),
            ]],
        };
        assert!(validate_grouped_partition_counts(
            "test",
            mismatch,
            &expected,
            PartitionMetric::NewOrders
        )
        .is_err());
    }

    #[test]
    fn endpoint_rows_compare_raw_float_bits() {
        let result = TypedResult {
            rows: vec![vec![
                TypedValue::Int32(1),
                TypedValue::Float32(300_001.0_f32.to_bits()),
            ]],
        };
        assert!(validate_float_endpoint_rows(
            "warehouse",
            result,
            [(
                PartitionKey {
                    warehouse_id: 1,
                    district_id: 0
                },
                300_001.0_f32.to_bits()
            )],
            false
        )
        .is_ok());
    }

    #[test]
    fn customer_sample_rows_require_exact_bits_counters_and_key_set() {
        let key = (1, 2, 3);
        let endpoint = CustomerEndpoint {
            balance_bits: 7.25_f32.to_bits(),
            ytd_payment_bits: 22.5_f32.to_bits(),
            payment_count: 4,
            delivery_count: 2,
        };
        let expected = [(key, endpoint)].into_iter().collect();
        let row = || {
            vec![
                TypedValue::Int32(key.0),
                TypedValue::Int32(key.1),
                TypedValue::Int32(key.2),
                TypedValue::Float32(endpoint.balance_bits),
                TypedValue::Float32(endpoint.ytd_payment_bits),
                TypedValue::Int32(endpoint.payment_count),
                TypedValue::Int32(endpoint.delivery_count),
            ]
        };
        assert!(validate_customer_endpoint_rows(
            "recovery",
            TypedResult { rows: vec![row()] },
            &expected
        )
        .is_ok());

        let mut wrong_bits = row();
        wrong_bits[3] = TypedValue::Float32((7.25_f32.to_bits()) + 1);
        assert!(validate_customer_endpoint_rows(
            "recovery",
            TypedResult {
                rows: vec![wrong_bits]
            },
            &expected
        )
        .is_err());
        assert!(validate_customer_endpoint_rows(
            "recovery",
            TypedResult { rows: Vec::new() },
            &expected
        )
        .is_err());
    }

    #[test]
    fn customer_sample_is_bounded_and_spans_sorted_keys() {
        let expected = (1..=100)
            .map(|customer| ((1, 1, customer), CustomerEndpoint::default()))
            .collect::<BTreeMap<_, _>>();
        let sample = evenly_spaced_customer_sample(&expected);
        assert_eq!(sample.len(), PUBLIC_CUSTOMER_ENDPOINT_SAMPLE_LIMIT);
        assert_eq!(sample.first().map(|item| item.0), Some((1, 1, 1)));
        assert_eq!(sample.last().map(|item| item.0), Some((1, 1, 100)));
    }
}
