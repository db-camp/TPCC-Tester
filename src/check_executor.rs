//! Typed Wire-v3 executor for public-spec consistency plans.

use std::collections::BTreeMap;

use tracing::{info, warn};

use crate::connection::client::RmdbClient;
use crate::connection::wire::{StreamResponse, WireValue};
use crate::consistency::{
    float32_matches, float_aggregate_plan, public_online_integer_plan,
    recovery_partition_audits_for_warehouses, recovery_plan, setup_plan, sum_f32_as_f64_once,
    validate_crash_float_baseline, validate_customer_update_chain, validate_public_float_ledger,
    validate_relative_update_chain_from_initial, CheckQuery, CheckScope, ConsistencyPlan,
    CustomerLogicalVersion, CustomerUpdateEvidence, CustomerUpdateKind, FloatAggregateId,
    NonNegativeF32Accumulator, PartitionExpectation, PartitionKey, PublicFloatLedgerEvidence,
    RecoveryExpectations, RelativeUpdateEvidence, SetupExpectations, TypedResult, TypedValue,
    DISTRICTS_PER_WAREHOUSE, FINAL_WAREHOUSES, FLOAT_AGGREGATES, NEW_ORDERS_PER_DISTRICT,
    ORDERS_PER_DISTRICT, PUBLIC_SPEC_NOTICE,
};
use crate::error::TpccError;
use crate::ranking::ledger::{LedgerEvent, RunLedger};
use crate::run_state::DatasetState;
use crate::runtime_schema::RuntimeSchema;
use crate::sample_evidence::{
    DistrictSample, HistorySample, ItemSample, OrderLineSample, SetupEvidence, StockSample,
};

pub type FloatBaseline = BTreeMap<FloatAggregateId, u32>;

const PUBLIC_CUSTOMER_ENDPOINT_SAMPLE_LIMIT: usize = 64;

pub async fn run_setup(client: &mut RmdbClient, dataset: &DatasetState) -> Result<(), TpccError> {
    let plan = scheduled_setup_plan(dataset)?;
    run_plan(client, &plan, &dataset.runtime_schema).await?;
    run_setup_sample_checks(client, dataset.setup_evidence(), &dataset.runtime_schema).await
}

fn scheduled_setup_plan(dataset: &DatasetState) -> Result<ConsistencyPlan, TpccError> {
    let generated = setup_plan(SetupExpectations {
        warehouses: dataset.warehouses,
        order_line_rows: dataset.order_line_rows,
        undelivered_order_line_rows: dataset.undelivered_order_line_rows,
    })
    .map_err(|error| TpccError::Protocol(format!("invalid setup plan: {error}")))?;
    let count_queries = generated
        .queries
        .iter()
        .filter(|query| query.id.starts_with("setup.count."))
        .count();
    if count_queries != 9 {
        return Err(TpccError::Protocol(format!(
            "setup plan generated {count_queries} COUNT phase queries, expected 9"
        )));
    }
    let mut post_count = generated
        .queries
        .into_iter()
        .filter(|query| !query.id.starts_with("setup.count."))
        .map(|query| (query.id.clone(), query))
        .collect::<BTreeMap<_, _>>();
    let mut plan = ConsistencyPlan::default();
    for id in dataset.runtime_schema.schedule().setup_checks() {
        plan.queries.push(post_count.remove(*id).ok_or_else(|| {
            TpccError::Protocol(format!("setup schedule references unknown check {id:?}"))
        })?);
    }
    if !post_count.is_empty() {
        return Err(TpccError::Protocol(format!(
            "setup schedule omitted checks: {}",
            post_count.keys().cloned().collect::<Vec<_>>().join(",")
        )));
    }
    Ok(plan)
}

#[derive(Debug)]
struct ExactSetupQuery {
    id: &'static str,
    sql: String,
    expected_rows: Vec<Vec<TypedValue>>,
}

async fn run_setup_sample_checks(
    client: &mut RmdbClient,
    evidence: &SetupEvidence,
    schema: &RuntimeSchema,
) -> Result<(), TpccError> {
    for query in setup_sample_queries(evidence)? {
        let result = execute_typed_sql(client, schema, query.id, &query.sql).await?;
        validate_exact_setup_rows(query.id, result, query.expected_rows)?;
        info!(
            "consistency PASS: {} — bounded relationship/content evidence",
            query.id
        );
    }
    Ok(())
}

fn setup_sample_queries(evidence: &SetupEvidence) -> Result<Vec<ExactSetupQuery>, TpccError> {
    let mut warehouse_rows = BTreeMap::new();
    for anchor in &evidence.anchors {
        warehouse_rows
            .entry(anchor.warehouse.id)
            .or_insert(&anchor.warehouse);
    }
    let warehouse_terms = warehouse_rows
        .keys()
        .map(|warehouse| format!("warehouse.w_id = {warehouse}"))
        .collect();
    let warehouse_expected = warehouse_rows
        .into_values()
        .map(|row| {
            vec![
                TypedValue::Int32(row.id),
                TypedValue::Char(row.name.clone()),
                TypedValue::Char(row.state.clone()),
                TypedValue::Char(row.zip.clone()),
                TypedValue::Float32(row.tax_bits),
                TypedValue::Float32(row.ytd_bits),
            ]
        })
        .collect();

    let district_terms = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.district;
            format!(
                "(district.d_w_id = {} AND district.d_id = {})",
                row.warehouse_id, row.id
            )
        })
        .collect();
    let district_expected = evidence
        .anchors
        .iter()
        .map(|anchor| district_typed_row(&anchor.district))
        .collect();

    let customer_terms = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.customer;
            format!(
                "(customer.c_w_id = {} AND customer.c_d_id = {} AND customer.c_id = {})",
                row.warehouse_id, row.district_id, row.id
            )
        })
        .collect();
    let customer_expected = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.customer;
            vec![
                TypedValue::Int32(row.warehouse_id),
                TypedValue::Int32(row.district_id),
                TypedValue::Int32(row.id),
                TypedValue::Char(row.first.clone()),
                TypedValue::Char(row.middle.clone()),
                TypedValue::Char(row.last.clone()),
                TypedValue::Char(row.since.clone()),
                TypedValue::Char(row.credit.clone()),
                TypedValue::Float32(row.discount_bits),
                TypedValue::Float32(row.balance_bits),
                TypedValue::Float32(row.ytd_payment_bits),
                TypedValue::Int32(row.payment_count),
                TypedValue::Int32(row.delivery_count),
                TypedValue::Char(row.data.clone()),
            ]
        })
        .collect();

    let order_terms = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.order;
            format!(
                "(orders.o_w_id = {} AND orders.o_d_id = {} AND orders.o_id = {})",
                row.warehouse_id, row.district_id, row.id
            )
        })
        .collect::<Vec<_>>();
    let order_expected = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.order;
            vec![
                TypedValue::Int32(row.warehouse_id),
                TypedValue::Int32(row.district_id),
                TypedValue::Int32(row.id),
                TypedValue::Int32(row.customer_id),
                TypedValue::Char(row.entry_date.clone()),
                TypedValue::Int32(row.carrier_id),
                TypedValue::Int32(row.line_count),
                TypedValue::Int32(row.all_local),
            ]
        })
        .collect();

    let new_order_terms = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.new_order;
            format!(
                "(new_orders.no_w_id = {} AND new_orders.no_d_id = {} AND new_orders.no_o_id = {})",
                row.warehouse_id, row.district_id, row.order_id
            )
        })
        .collect();
    let new_order_expected = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.new_order;
            vec![
                TypedValue::Int32(row.warehouse_id),
                TypedValue::Int32(row.district_id),
                TypedValue::Int32(row.order_id),
            ]
        })
        .collect();

    let history_terms = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.history;
            format!(
                "(history.h_c_w_id = {} AND history.h_c_d_id = {} AND history.h_c_id = {} AND history.h_w_id = {} AND history.h_d_id = {})",
                row.customer_warehouse_id,
                row.customer_district_id,
                row.customer_id,
                row.warehouse_id,
                row.district_id,
            )
        })
        .collect();
    let history_expected = evidence
        .anchors
        .iter()
        .map(|anchor| history_typed_row(&anchor.history))
        .collect();

    let line_terms = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let row = &anchor.order;
            format!(
                "(order_line.ol_w_id = {} AND order_line.ol_d_id = {} AND order_line.ol_o_id = {})",
                row.warehouse_id, row.district_id, row.id
            )
        })
        .collect();
    let line_expected = evidence
        .anchors
        .iter()
        .flat_map(|anchor| anchor.lines.iter().map(order_line_typed_row))
        .collect();

    let item_terms = evidence
        .items
        .iter()
        .map(|row| format!("item.i_id = {}", row.id))
        .collect();
    let item_expected = evidence.items.iter().map(item_typed_row).collect();

    let stock_terms = evidence
        .stocks
        .iter()
        .map(|row| {
            format!(
                "(stock.s_w_id = {} AND stock.s_i_id = {})",
                row.warehouse_id, row.item_id
            )
        })
        .collect();
    let stock_expected = evidence.stocks.iter().map(stock_typed_row).collect();

    let sum_expected = evidence
        .anchors
        .iter()
        .map(|anchor| {
            let bits = sum_f32_as_f64_once(anchor.lines.iter().map(|line| line.amount_bits))
                .map_err(|error| {
                    TpccError::Protocol(format!(
                        "invalid persisted setup order-line amount evidence: {error}"
                    ))
                })?;
            Ok(vec![
                TypedValue::Int32(anchor.order.warehouse_id),
                TypedValue::Int32(anchor.order.district_id),
                TypedValue::Int32(anchor.order.id),
                TypedValue::Float32(bits),
            ])
        })
        .collect::<Result<Vec<_>, TpccError>>()?;
    let sum_terms = evidence
        .anchors
        .iter()
        .map(|anchor| {
            format!(
                "(order_line.ol_w_id = {} AND order_line.ol_d_id = {} AND order_line.ol_o_id = {})",
                anchor.order.warehouse_id, anchor.order.district_id, anchor.order.id
            )
        })
        .collect();

    let order_filter = or_terms(order_terms)?;
    let sum_filter = or_terms(sum_terms)?;
    Ok(vec![
        ExactSetupQuery {
            id: "setup.sample.warehouse_content",
            sql: format!(
                "SELECT warehouse.w_id, warehouse.w_name, warehouse.w_state, warehouse.w_zip, warehouse.w_tax, warehouse.w_ytd FROM warehouse WHERE {}",
                or_terms(warehouse_terms)?
            ),
            expected_rows: warehouse_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.district_to_warehouse",
            sql: format!(
                "SELECT district.d_w_id, district.d_id, district.d_name, district.d_state, district.d_zip, district.d_tax, district.d_ytd, district.d_next_o_id FROM district, warehouse WHERE ({}) AND warehouse.w_id = district.d_w_id",
                or_terms(district_terms)?
            ),
            expected_rows: district_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.customer_to_district",
            sql: format!(
                "SELECT customer.c_w_id, customer.c_d_id, customer.c_id, customer.c_first, customer.c_middle, customer.c_last, customer.c_since, customer.c_credit, customer.c_discount, customer.c_balance, customer.c_ytd_payment, customer.c_payment_cnt, customer.c_delivery_cnt, customer.c_data FROM customer, district WHERE ({}) AND district.d_w_id = customer.c_w_id AND district.d_id = customer.c_d_id",
                or_terms(customer_terms)?
            ),
            expected_rows: customer_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.orders_to_customer",
            sql: format!(
                "SELECT orders.o_w_id, orders.o_d_id, orders.o_id, orders.o_c_id, orders.o_entry_d, orders.o_carrier_id, orders.o_ol_cnt, orders.o_all_local FROM orders, customer WHERE ({order_filter}) AND customer.c_w_id = orders.o_w_id AND customer.c_d_id = orders.o_d_id AND customer.c_id = orders.o_c_id"
            ),
            expected_rows: order_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.new_orders_to_orders",
            sql: format!(
                "SELECT new_orders.no_w_id, new_orders.no_d_id, new_orders.no_o_id FROM new_orders, orders WHERE ({}) AND orders.o_w_id = new_orders.no_w_id AND orders.o_d_id = new_orders.no_d_id AND orders.o_id = new_orders.no_o_id",
                or_terms(new_order_terms)?
            ),
            expected_rows: new_order_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.history_to_customer",
            sql: format!(
                "SELECT history.h_c_w_id, history.h_c_d_id, history.h_c_id, history.h_w_id, history.h_d_id, history.h_date, history.h_amount, history.h_data FROM history, customer WHERE ({}) AND customer.c_w_id = history.h_c_w_id AND customer.c_d_id = history.h_c_d_id AND customer.c_id = history.h_c_id",
                or_terms(history_terms)?
            ),
            expected_rows: history_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.order_line_relationships",
            sql: format!(
                "SELECT order_line.ol_w_id, order_line.ol_d_id, order_line.ol_o_id, order_line.ol_number, order_line.ol_i_id, order_line.ol_supply_w_id, order_line.ol_delivery_d, order_line.ol_quantity, order_line.ol_amount, order_line.ol_dist_info FROM order_line, orders, item, stock WHERE ({}) AND orders.o_w_id = order_line.ol_w_id AND orders.o_d_id = order_line.ol_d_id AND orders.o_id = order_line.ol_o_id AND item.i_id = order_line.ol_i_id AND stock.s_w_id = order_line.ol_supply_w_id AND stock.s_i_id = order_line.ol_i_id",
                or_terms(line_terms)?
            ),
            expected_rows: line_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.item_content",
            sql: format!(
                "SELECT item.i_id, item.i_name, item.i_price, item.i_data FROM item WHERE {}",
                or_terms(item_terms)?
            ),
            expected_rows: item_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.stock_content",
            sql: format!(
                "SELECT stock.s_w_id, stock.s_i_id, stock.s_quantity, stock.s_ytd, stock.s_order_cnt, stock.s_remote_cnt, stock.s_data FROM stock WHERE {}",
                or_terms(stock_terms)?
            ),
            expected_rows: stock_expected,
        },
        ExactSetupQuery {
            id: "setup.sample.undelivered_order_sum",
            sql: format!(
                "SELECT order_line.ol_w_id, order_line.ol_d_id, order_line.ol_o_id, SUM(order_line.ol_amount) FROM order_line WHERE {} GROUP BY order_line.ol_w_id, order_line.ol_d_id, order_line.ol_o_id",
                sum_filter
            ),
            expected_rows: sum_expected,
        },
    ])
}

fn or_terms(terms: Vec<String>) -> Result<String, TpccError> {
    if terms.is_empty() {
        return Err(TpccError::Protocol(
            "setup evidence produced an empty bounded predicate".to_owned(),
        ));
    }
    Ok(terms.join(" OR "))
}

fn district_typed_row(row: &DistrictSample) -> Vec<TypedValue> {
    vec![
        TypedValue::Int32(row.warehouse_id),
        TypedValue::Int32(row.id),
        TypedValue::Char(row.name.clone()),
        TypedValue::Char(row.state.clone()),
        TypedValue::Char(row.zip.clone()),
        TypedValue::Float32(row.tax_bits),
        TypedValue::Float32(row.ytd_bits),
        TypedValue::Int32(row.next_order_id),
    ]
}

fn history_typed_row(row: &HistorySample) -> Vec<TypedValue> {
    vec![
        TypedValue::Int32(row.customer_warehouse_id),
        TypedValue::Int32(row.customer_district_id),
        TypedValue::Int32(row.customer_id),
        TypedValue::Int32(row.warehouse_id),
        TypedValue::Int32(row.district_id),
        TypedValue::Char(row.date.clone()),
        TypedValue::Float32(row.amount_bits),
        TypedValue::Char(row.data.clone()),
    ]
}

fn order_line_typed_row(row: &OrderLineSample) -> Vec<TypedValue> {
    vec![
        TypedValue::Int32(row.warehouse_id),
        TypedValue::Int32(row.district_id),
        TypedValue::Int32(row.order_id),
        TypedValue::Int32(row.number),
        TypedValue::Int32(row.item_id),
        TypedValue::Int32(row.supply_warehouse_id),
        TypedValue::Char(row.delivery_date.clone()),
        TypedValue::Int32(row.quantity),
        TypedValue::Float32(row.amount_bits),
        TypedValue::Char(row.dist_info.clone()),
    ]
}

fn item_typed_row(row: &ItemSample) -> Vec<TypedValue> {
    vec![
        TypedValue::Int32(row.id),
        TypedValue::Char(row.name.clone()),
        TypedValue::Float32(row.price_bits),
        TypedValue::Char(row.data.clone()),
    ]
}

fn stock_typed_row(row: &StockSample) -> Vec<TypedValue> {
    vec![
        TypedValue::Int32(row.warehouse_id),
        TypedValue::Int32(row.item_id),
        TypedValue::Int32(row.quantity),
        TypedValue::Float32(row.ytd_bits),
        TypedValue::Int32(row.order_count),
        TypedValue::Int32(row.remote_count),
        TypedValue::Char(row.data.clone()),
    ]
}

fn validate_exact_setup_rows(
    id: &str,
    result: TypedResult,
    expected_rows: Vec<Vec<TypedValue>>,
) -> Result<(), TpccError> {
    if result.rows.len() != expected_rows.len() {
        return Err(TpccError::QueryError(format!(
            "LOAD content mismatch: {id} expected {} rows, got {}",
            expected_rows.len(),
            result.rows.len()
        )));
    }
    let expected_shape = expected_rows
        .first()
        .ok_or_else(|| TpccError::Protocol(format!("{id} has no persisted expected setup rows")))?;
    for row in &result.rows {
        if row.len() != expected_shape.len() {
            return Err(TpccError::Protocol(format!(
                "{id} returned {} columns, expected {}",
                row.len(),
                expected_shape.len()
            )));
        }
        for (column, (expected, actual)) in expected_shape.iter().zip(row).enumerate() {
            if !same_typed_kind(expected, actual) {
                return Err(TpccError::Protocol(format!(
                    "{id} column {column} returned {}, expected {}",
                    typed_kind(actual),
                    typed_kind(expected)
                )));
            }
        }
    }
    let mut matched = vec![false; expected_rows.len()];
    for (row_index, actual) in result.rows.iter().enumerate() {
        let Some(expected_index) =
            expected_rows
                .iter()
                .enumerate()
                .position(|(index, expected)| {
                    !matched[index] && exact_setup_row_matches(expected, actual)
                })
        else {
            return Err(TpccError::QueryError(format!(
                "LOAD content mismatch: {id} returned unexpected row {row_index}"
            )));
        };
        matched[expected_index] = true;
    }
    if matched.iter().all(|value| *value) {
        Ok(())
    } else {
        Err(TpccError::QueryError(format!(
            "LOAD content mismatch: {id} omitted a persisted setup row"
        )))
    }
}

fn exact_setup_row_matches(expected: &[TypedValue], actual: &[TypedValue]) -> bool {
    expected.len() == actual.len()
        && expected
            .iter()
            .zip(actual)
            .all(|(expected, actual)| match (expected, actual) {
                (TypedValue::Float32(expected), TypedValue::Float32(actual)) => {
                    float32_matches(*expected, *actual, 0)
                }
                _ => expected == actual,
            })
}

fn same_typed_kind(left: &TypedValue, right: &TypedValue) -> bool {
    matches!(
        (left, right),
        (TypedValue::Null, TypedValue::Null)
            | (TypedValue::Int32(_), TypedValue::Int32(_))
            | (TypedValue::Float32(_), TypedValue::Float32(_))
            | (TypedValue::Char(_), TypedValue::Char(_))
    )
}

fn typed_kind(value: &TypedValue) -> &'static str {
    match value {
        TypedValue::Null => "NULL",
        TypedValue::Int32(_) => "INT32",
        TypedValue::Float32(_) => "FLOAT32",
        TypedValue::Char(_) => "CHAR",
    }
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
    run_plan(client, &plan, &dataset.runtime_schema).await?;

    validate_transaction_evidence(ledger)?;
    let values = read_float_aggregates(client, CheckScope::Online, &dataset.runtime_schema).await?;
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
/// verifies Payment warehouse/district endpoints at 0 ULP, and audits every
/// warehouse/district partition (500 at the official scale) in six grouped
/// round trips.
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
    run_plan(client, &plan, &dataset.runtime_schema).await?;

    let endpoints = validate_transaction_evidence(ledger)?;
    let recovered =
        read_float_aggregates(client, CheckScope::Recovery, &dataset.runtime_schema).await?;
    validate_float_baseline(online_baseline, &recovered)?;
    validate_payment_endpoints(
        client,
        &dataset.runtime_schema,
        dataset.warehouses,
        &endpoints,
    )
    .await?;
    validate_customer_endpoint_sample(client, &dataset.runtime_schema, &endpoints).await?;

    let partitions = partition_expectations(dataset, ledger)?;
    // Reuse the transport-neutral generator as a strict completeness and
    // invariant validator, but execute the equivalent audit in six grouped
    // queries rather than 3,000 scalar requests.
    recovery_partition_audits_for_warehouses(dataset.warehouses, partitions.clone())
        .map_err(|error| protocol_error("invalid recovery partition ledger", error))?;
    run_grouped_partition_audit(client, &dataset.runtime_schema, &partitions).await?;
    info!(
        "public recovery consistency PASS; hidden official 37 SQL, generated keys, seed, and answers remain unavailable"
    );
    Ok(())
}

pub async fn run_plan(
    client: &mut RmdbClient,
    plan: &ConsistencyPlan,
    schema: &RuntimeSchema,
) -> Result<(), TpccError> {
    warn!("{PUBLIC_SPEC_NOTICE}");
    for query in &plan.queries {
        let result = execute_query(client, schema, query).await?;
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
    schema: &RuntimeSchema,
) -> Result<FloatBaseline, TpccError> {
    let plan = float_aggregate_plan(scope);
    let mut values = BTreeMap::new();
    for (spec, query) in FLOAT_AGGREGATES.iter().zip(&plan.queries) {
        let result = execute_query(client, schema, query).await?;
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
    validate_consistency_warehouse_count(dataset.warehouses)?;
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

fn validate_consistency_warehouse_count(warehouses: i32) -> Result<usize, TpccError> {
    if !(1..=FINAL_WAREHOUSES).contains(&warehouses) {
        return Err(TpccError::Protocol(format!(
            "public consistency requires 1..={FINAL_WAREHOUSES} warehouses, state has {warehouses}"
        )));
    }
    Ok((warehouses * DISTRICTS_PER_WAREHOUSE) as usize)
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
    let mut customer_updates: BTreeMap<CustomerKey, Vec<CustomerUpdateEvidence>> = BTreeMap::new();

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
                customer_updates
                    .entry(customer)
                    .or_default()
                    .push(CustomerUpdateEvidence {
                        kind: CustomerUpdateKind::Payment,
                        before_version: CustomerLogicalVersion {
                            payment_count: delta.customer_payment_count_before,
                            delivery_count: delta.customer_delivery_count_before,
                        },
                        after_version: CustomerLogicalVersion {
                            payment_count: delta.customer_payment_count_after,
                            delivery_count: delta.customer_delivery_count_after,
                        },
                        amount_bits: amount,
                        balance_before_bits: delta.customer_balance_before_bits,
                        balance_after_bits: delta.customer_balance_after_bits,
                        ytd_payment_before_bits: Some(delta.customer_ytd_before_bits),
                        ytd_payment_after_bits: Some(delta.customer_ytd_after_bits),
                    });
            }
            LedgerEvent::Delivery(delta) => {
                for order in &delta.orders {
                    let customer = (
                        i32::from(delta.warehouse_id),
                        i32::from(order.district_id),
                        order.customer_id,
                    );
                    customer_updates
                        .entry(customer)
                        .or_default()
                        .push(CustomerUpdateEvidence {
                            kind: CustomerUpdateKind::Delivery,
                            before_version: CustomerLogicalVersion {
                                payment_count: order.customer_payment_count_before,
                                delivery_count: order.customer_delivery_count_before,
                            },
                            after_version: CustomerLogicalVersion {
                                payment_count: order.customer_payment_count_after,
                                delivery_count: order.customer_delivery_count_after,
                            },
                            amount_bits: order.customer_amount_bits,
                            balance_before_bits: order.customer_balance_before_bits,
                            balance_after_bits: order.customer_balance_after_bits,
                            ytd_payment_before_bits: None,
                            ytd_payment_after_bits: None,
                        });
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
    for ((warehouse, district, customer), updates) in customer_updates {
        let endpoint = validate_customer_update_chain(
            (-10.0_f32).to_bits(),
            10.0_f32.to_bits(),
            CustomerLogicalVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            &updates,
        )
        .map_err(|error| {
            TpccError::QueryError(format!(
                "Payment/Delivery customer version chain for \
                 ({warehouse},{district},{customer}) failed: {error}"
            ))
        })?;
        endpoints.customers.insert(
            (warehouse, district, customer),
            CustomerEndpoint {
                balance_bits: endpoint.balance_bits,
                ytd_payment_bits: endpoint.ytd_payment_bits,
                payment_count: endpoint.version.payment_count,
                delivery_count: endpoint.version.delivery_count,
            },
        );
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
    schema: &RuntimeSchema,
    warehouse_count: i32,
    endpoints: &RelativeEndpoints,
) -> Result<(), TpccError> {
    let warehouses = execute_typed_sql(
        client,
        schema,
        "recovery.payment.warehouse_endpoints",
        "SELECT w_id, w_ytd FROM warehouse",
    )
    .await?;
    validate_float_endpoint_rows(
        "warehouse w_ytd",
        warehouses,
        (1..=warehouse_count).map(|warehouse| {
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
        schema,
        "recovery.payment.district_endpoints",
        "SELECT d_w_id, d_id, d_ytd FROM district",
    )
    .await?;
    validate_float_endpoint_rows(
        "district d_ytd",
        districts,
        (1..=warehouse_count).flat_map(|warehouse| {
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
    schema: &RuntimeSchema,
    endpoints: &RelativeEndpoints,
) -> Result<(), TpccError> {
    let sample = evenly_spaced_customer_sample(&endpoints.customers);
    for (ordinal, (key, endpoint)) in sample.iter().enumerate() {
        let result = execute_typed_sql(
            client,
            schema,
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
    let expected_partitions = validate_consistency_warehouse_count(dataset.warehouses)?;
    if dataset.partitions.len() != expected_partitions {
        return Err(TpccError::Protocol(format!(
            "recovery requires {expected_partitions} load partitions for {} warehouses, state has {}",
            dataset.warehouses,
            dataset.partitions.len(),
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
    schema: &RuntimeSchema,
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
        let result = execute_typed_sql(client, schema, id, sql).await?;
        validate_grouped_partition_counts(id, result, partitions, metric)?;
        info!(
            "consistency PASS: {id} ({} partitions in one typed response)",
            partitions.len()
        );
    }

    let result = execute_typed_sql(
        client,
        schema,
        "recovery.partition.grouped.next_order_id",
        "SELECT d_w_id, d_id, d_next_o_id FROM district",
    )
    .await?;
    validate_partition_next_order_ids(result, partitions)?;
    info!(
        "consistency PASS: recovery.partition.grouped.next_order_id ({} partitions in one typed response)",
        partitions.len()
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
    schema: &RuntimeSchema,
    id: &str,
    sql: &str,
) -> Result<TypedResult, TpccError> {
    let rendered = schema.render_sql(sql);
    match client.exec_stream(&terminated_sql(&rendered)).await? {
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
    schema: &RuntimeSchema,
    query: &CheckQuery,
) -> Result<TypedResult, TpccError> {
    execute_typed_sql(client, schema, &query.id, &query.sql).await
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
    use crate::loader::{LoadSummary, PartitionLoadSummary};
    use crate::sample_evidence::setup_evidence_fixture;

    fn smoke_dataset(warehouses: i32) -> DatasetState {
        let partitions = (1..=warehouses)
            .flat_map(|warehouse_id| {
                (1..=DISTRICTS_PER_WAREHOUSE).map(move |district_id| PartitionLoadSummary {
                    warehouse_id,
                    district_id,
                    order_line_rows: 15_000,
                    undelivered_order_line_rows: 4_500,
                })
            })
            .collect::<Vec<_>>();
        let order_line_rows = i64::from(warehouses) * 10 * 15_000;
        let undelivered_order_line_rows = i64::from(warehouses) * 10 * 4_500;
        let mut order_line_amounts = NonNegativeF32Accumulator::default();
        order_line_amounts
            .add_repeated_bits(1.0_f32.to_bits(), order_line_rows as u64)
            .unwrap();
        DatasetState::from_load(
            format!("smoke-sf{warehouses}"),
            1,
            warehouses,
            LoadSummary {
                order_line_rows,
                undelivered_order_line_rows,
                order_line_amounts,
                partitions,
                setup_evidence: setup_evidence_fixture(warehouses, 1),
            },
        )
        .unwrap()
    }

    #[test]
    fn sf1_smoke_uses_its_complete_dataset_keyspace() {
        let dataset = smoke_dataset(1);
        let ledger = RunLedger::default();
        let expectations =
            final_expectations(&dataset, &ledger, dataset.initial_order_line_amounts()).unwrap();
        assert_eq!(expectations.setup.warehouses, 1);

        let partitions = partition_expectations(&dataset, &ledger).unwrap();
        assert_eq!(partitions.len(), DISTRICTS_PER_WAREHOUSE as usize);
        assert_eq!(
            recovery_partition_audits_for_warehouses(dataset.warehouses, partitions)
                .unwrap()
                .len(),
            DISTRICTS_PER_WAREHOUSE as usize
        );
        assert!(validate_consistency_warehouse_count(FINAL_WAREHOUSES + 1).is_err());
    }

    #[test]
    fn setup_executes_only_the_scheduled_post_count_checks() {
        let dataset = smoke_dataset(1);
        let plan = scheduled_setup_plan(&dataset).unwrap();
        assert_eq!(plan.queries.len(), 18);
        assert!(plan
            .queries
            .iter()
            .all(|query| !query.id.starts_with("setup.count.")));
        assert_eq!(
            plan.queries
                .iter()
                .map(|query| query.id.as_str())
                .collect::<Vec<_>>(),
            dataset.runtime_schema.schedule().setup_checks()
        );
    }

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

    #[test]
    fn setup_sample_queries_are_bounded_and_cover_every_published_relationship() {
        let evidence = setup_evidence_fixture(50, 2026);
        let queries = setup_sample_queries(&evidence).unwrap();
        assert_eq!(queries.len(), 10);
        assert!(queries.iter().all(|query| query.sql.len() < 64 * 1024));

        let district = queries
            .iter()
            .find(|query| query.id == "setup.sample.district_to_warehouse")
            .unwrap();
        assert!(district.sql.contains("warehouse.w_id = district.d_w_id"));
        let customer = queries
            .iter()
            .find(|query| query.id == "setup.sample.customer_to_district")
            .unwrap();
        assert!(customer.sql.contains("district.d_w_id = customer.c_w_id"));
        let orders = queries
            .iter()
            .find(|query| query.id == "setup.sample.orders_to_customer")
            .unwrap();
        assert!(orders.sql.contains("customer.c_id = orders.o_c_id"));
        let new_orders = queries
            .iter()
            .find(|query| query.id == "setup.sample.new_orders_to_orders")
            .unwrap();
        assert!(new_orders.sql.contains("orders.o_id = new_orders.no_o_id"));
        let lines = queries
            .iter()
            .find(|query| query.id == "setup.sample.order_line_relationships")
            .unwrap();
        for relation in [
            "orders.o_id = order_line.ol_o_id",
            "item.i_id = order_line.ol_i_id",
            "stock.s_w_id = order_line.ol_supply_w_id",
            "stock.s_i_id = order_line.ol_i_id",
        ] {
            assert!(lines.sql.contains(relation));
        }
        let history = queries
            .iter()
            .find(|query| query.id == "setup.sample.history_to_customer")
            .unwrap();
        assert!(history.sql.contains("customer.c_id = history.h_c_id"));
        assert_eq!(
            history.expected_rows.len(),
            evidence.anchors.len(),
            "history is checked in one bounded scan, not one full scan per anchor"
        );
    }

    #[test]
    fn exact_setup_rows_use_zero_ulp_and_exact_char_bytes() {
        let expected = vec![
            vec![
                TypedValue::Int32(1),
                TypedValue::Float32(0.0_f32.to_bits()),
                TypedValue::Char(b"one".to_vec()),
            ],
            vec![
                TypedValue::Int32(2),
                TypedValue::Float32(1.0_f32.to_bits()),
                TypedValue::Char(b"two".to_vec()),
            ],
        ];
        let reordered_with_negative_zero = TypedResult {
            rows: vec![
                expected[1].clone(),
                vec![
                    TypedValue::Int32(1),
                    TypedValue::Float32((-0.0_f32).to_bits()),
                    TypedValue::Char(b"one".to_vec()),
                ],
            ],
        };
        assert!(validate_exact_setup_rows(
            "setup.test",
            reordered_with_negative_zero,
            expected.clone()
        )
        .is_ok());

        let mut one_ulp = expected.clone();
        one_ulp[1][1] = TypedValue::Float32(1.0_f32.to_bits() + 1);
        assert!(validate_exact_setup_rows(
            "setup.test",
            TypedResult { rows: one_ulp },
            expected.clone()
        )
        .is_err());

        let mut wrong_char = expected.clone();
        wrong_char[0][2] = TypedValue::Char(b"One".to_vec());
        assert!(validate_exact_setup_rows(
            "setup.test",
            TypedResult { rows: wrong_char },
            expected
        )
        .is_err());
    }
}
