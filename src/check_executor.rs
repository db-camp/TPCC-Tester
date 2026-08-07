//! Typed Wire-v3 executor for public-spec consistency plans.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tracing::{info, warn};

use crate::connection::client::RmdbClient;
use crate::connection::wire::{FoldStreamResponse, StreamResponse, WireValue};
use crate::consistency::{
    float32_matches, float_aggregate_plan, public_online_integer_plan,
    recovery_partition_audits_for_warehouses, recovery_plan, setup_plan, sum_f32_as_f64_once,
    validate_crash_float_baseline, validate_public_float_ledger, AbandonedWrites, CheckQuery,
    CheckScope, ConsistencyPlan, Float32RangeBits, FloatAggregateId, NonNegativeF32Accumulator,
    PartitionExpectation, PartitionKey, PublicFloatLedgerEvidence, RecoveryExpectations,
    ScalarExpectation, SetupExpectations, TypedResult, TypedValue, DISTRICTS_PER_WAREHOUSE,
    FINAL_WAREHOUSES, FLOAT_AGGREGATES, MAX_NEW_ORDER_LINE_AMOUNT, MAX_NEW_ORDER_OL_COUNT,
    MAX_NEW_ORDER_STOCK_YTD, MAX_PAYMENT_H_AMOUNT, NEW_ORDERS_PER_DISTRICT, ORDERS_PER_DISTRICT,
    PUBLIC_SPEC_NOTICE,
};
use crate::error::TpccError;
use crate::ranking::bounded_stats::BoundedPhysicalStats;
use crate::ranking::payment_endpoints::PaymentEndpointView;
use crate::ranking::terminal_evidence::{validate_terminal_evidence, TerminalEvidenceView};
use crate::recovery_sample_checker;
use crate::run_state::DatasetState;
use crate::runtime_schema::RuntimeSchema;
use crate::sample_evidence::{
    DistrictSample, HistorySample, ItemSample, OrderLineSample, SetupEvidence, StockSample,
};

pub type FloatBaseline = BTreeMap<FloatAggregateId, u32>;

pub async fn run_setup(client: &mut RmdbClient, dataset: &DatasetState) -> Result<(), TpccError> {
    dataset
        .validate_setup_evidence_binding()
        .map_err(|error| protocol_error("invalid persisted setup evidence", error))?;
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
    if evidence.anchors.is_empty() || evidence.items.is_empty() || evidence.stocks.is_empty() {
        return Err(TpccError::Protocol(
            "setup evidence produced an empty bounded sample".to_owned(),
        ));
    }

    // RMDB's public grammar accepts a conjunction of simple conditions but no
    // boolean OR or parenthesized predicate. Keep every probe a bounded,
    // index-friendly point lookup instead of combining unrelated sample keys.
    let mut queries = Vec::new();
    let mut seen_warehouses = BTreeSet::new();
    for (anchor_index, anchor) in evidence.anchors.iter().enumerate() {
        let warehouse = &anchor.warehouse;
        if seen_warehouses.insert(warehouse.id) {
            queries.push(ExactSetupQuery {
                id: "setup.sample.warehouse_content",
                sql: format!(
                    "SELECT warehouse.w_id, warehouse.w_name, warehouse.w_state, warehouse.w_zip, warehouse.w_tax, warehouse.w_ytd FROM warehouse WHERE warehouse.w_id = {}",
                    warehouse.id
                ),
                expected_rows: vec![warehouse_typed_row(warehouse)],
            });
        }

        let district = &anchor.district;
        queries.push(ExactSetupQuery {
            id: "setup.sample.district_to_warehouse",
            sql: format!(
                "SELECT district.d_w_id, district.d_id, district.d_name, district.d_state, district.d_zip, district.d_tax, district.d_ytd, district.d_next_o_id FROM district, warehouse WHERE district.d_w_id = {} AND district.d_id = {} AND warehouse.w_id = district.d_w_id",
                district.warehouse_id, district.id
            ),
            expected_rows: vec![district_typed_row(district)],
        });

        let customer = &anchor.customer;
        queries.push(ExactSetupQuery {
            id: "setup.sample.customer_to_district",
            sql: format!(
                "SELECT customer.c_w_id, customer.c_d_id, customer.c_id, customer.c_first, customer.c_middle, customer.c_last, customer.c_since, customer.c_credit, customer.c_discount, customer.c_balance, customer.c_ytd_payment, customer.c_payment_cnt, customer.c_delivery_cnt, customer.c_data FROM customer, district WHERE customer.c_w_id = {} AND customer.c_d_id = {} AND customer.c_id = {} AND district.d_w_id = customer.c_w_id AND district.d_id = customer.c_d_id",
                customer.warehouse_id, customer.district_id, customer.id
            ),
            expected_rows: vec![customer_typed_row(customer)],
        });

        let order = &anchor.order;
        queries.push(ExactSetupQuery {
            id: "setup.sample.orders_to_customer",
            sql: format!(
                "SELECT orders.o_w_id, orders.o_d_id, orders.o_id, orders.o_c_id, orders.o_entry_d, orders.o_carrier_id, orders.o_ol_cnt, orders.o_all_local FROM orders, customer WHERE orders.o_w_id = {} AND orders.o_d_id = {} AND orders.o_id = {} AND customer.c_w_id = orders.o_w_id AND customer.c_d_id = orders.o_d_id AND customer.c_id = orders.o_c_id",
                order.warehouse_id, order.district_id, order.id
            ),
            expected_rows: vec![order_typed_row(order)],
        });

        let new_order = &anchor.new_order;
        queries.push(ExactSetupQuery {
            id: "setup.sample.new_orders_to_orders",
            sql: format!(
                "SELECT new_orders.no_w_id, new_orders.no_d_id, new_orders.no_o_id FROM new_orders, orders WHERE new_orders.no_w_id = {} AND new_orders.no_d_id = {} AND new_orders.no_o_id = {} AND orders.o_w_id = new_orders.no_w_id AND orders.o_d_id = new_orders.no_d_id AND orders.o_id = new_orders.no_o_id",
                new_order.warehouse_id, new_order.district_id, new_order.order_id
            ),
            expected_rows: vec![new_order_typed_row(new_order)],
        });

        // The published ten-index schema has no history index. Probe the
        // deterministic first/last anchors so the relationship still crosses
        // partitions without repeating a large sequential scan 16 times.
        if anchor_index == 0 || anchor_index + 1 == evidence.anchors.len() {
            let history = &anchor.history;
            queries.push(ExactSetupQuery {
                id: "setup.sample.history_to_customer",
                sql: format!(
                    "SELECT history.h_c_w_id, history.h_c_d_id, history.h_c_id, history.h_w_id, history.h_d_id, history.h_date, history.h_amount, history.h_data FROM history, customer WHERE history.h_c_w_id = {} AND history.h_c_d_id = {} AND history.h_c_id = {} AND history.h_w_id = {} AND history.h_d_id = {} AND customer.c_w_id = history.h_c_w_id AND customer.c_d_id = history.h_c_d_id AND customer.c_id = history.h_c_id",
                    history.customer_warehouse_id,
                    history.customer_district_id,
                    history.customer_id,
                    history.warehouse_id,
                    history.district_id,
                ),
                expected_rows: vec![history_typed_row(history)],
            });
        }

        queries.push(ExactSetupQuery {
            id: "setup.sample.order_line_relationships",
            sql: format!(
                "SELECT order_line.ol_w_id, order_line.ol_d_id, order_line.ol_o_id, order_line.ol_number, order_line.ol_i_id, order_line.ol_supply_w_id, order_line.ol_delivery_d, order_line.ol_quantity, order_line.ol_amount, order_line.ol_dist_info FROM order_line, orders, item, stock WHERE order_line.ol_w_id = {} AND order_line.ol_d_id = {} AND order_line.ol_o_id = {} AND orders.o_w_id = order_line.ol_w_id AND orders.o_d_id = order_line.ol_d_id AND orders.o_id = order_line.ol_o_id AND item.i_id = order_line.ol_i_id AND stock.s_w_id = order_line.ol_supply_w_id AND stock.s_i_id = order_line.ol_i_id",
                order.warehouse_id, order.district_id, order.id
            ),
            expected_rows: anchor.lines.iter().map(order_line_typed_row).collect(),
        });

        let sum_bits = sum_f32_as_f64_once(anchor.lines.iter().map(|line| line.amount_bits))
            .map_err(|error| {
                TpccError::Protocol(format!(
                    "invalid persisted setup order-line amount evidence: {error}"
                ))
            })?;
        queries.push(ExactSetupQuery {
            id: "setup.sample.undelivered_order_sum",
            sql: format!(
                "SELECT SUM(order_line.ol_amount) FROM order_line WHERE order_line.ol_w_id = {} AND order_line.ol_d_id = {} AND order_line.ol_o_id = {}",
                order.warehouse_id, order.district_id, order.id
            ),
            expected_rows: vec![vec![TypedValue::Float32(sum_bits)]],
        });
    }

    for item in &evidence.items {
        queries.push(ExactSetupQuery {
            id: "setup.sample.item_content",
            sql: format!(
                "SELECT item.i_id, item.i_name, item.i_price, item.i_data FROM item WHERE item.i_id = {}",
                item.id
            ),
            expected_rows: vec![item_typed_row(item)],
        });
    }
    for stock in &evidence.stocks {
        queries.push(ExactSetupQuery {
            id: "setup.sample.stock_content",
            sql: format!(
                "SELECT stock.s_w_id, stock.s_i_id, stock.s_quantity, stock.s_ytd, stock.s_order_cnt, stock.s_remote_cnt, stock.s_data FROM stock WHERE stock.s_w_id = {} AND stock.s_i_id = {}",
                stock.warehouse_id, stock.item_id
            ),
            expected_rows: vec![stock_typed_row(stock)],
        });
    }
    Ok(queries)
}

fn warehouse_typed_row(row: &crate::sample_evidence::WarehouseSample) -> Vec<TypedValue> {
    vec![
        TypedValue::Int32(row.id),
        TypedValue::Char(row.name.clone()),
        TypedValue::Char(row.state.clone()),
        TypedValue::Char(row.zip.clone()),
        TypedValue::Float32(row.tax_bits),
        TypedValue::Float32(row.ytd_bits),
    ]
}

fn customer_typed_row(row: &crate::sample_evidence::CustomerSample) -> Vec<TypedValue> {
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
}

fn order_typed_row(row: &crate::sample_evidence::OrderSample) -> Vec<TypedValue> {
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
}

fn new_order_typed_row(row: &crate::sample_evidence::NewOrderSample) -> Vec<TypedValue> {
    vec![
        TypedValue::Int32(row.warehouse_id),
        TypedValue::Int32(row.district_id),
        TypedValue::Int32(row.order_id),
    ]
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

/// Execute the public online gate from the bounded terminal oracle.
///
/// Every artifact, dataset, Payment-domain, integer, and FLOAT oracle check is
/// completed before the first Wire request. The online network surface remains
/// exactly the six public-semantic integer queries plus seven FLOAT aggregates;
/// recovery endpoints, retained samples, and partition audits are not queried.
pub async fn run_final_online_from_terminal_evidence(
    client: &mut RmdbClient,
    dataset: &DatasetState,
    evidence: &dyn TerminalEvidenceView,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
    abandoned: AbandonedWrites,
) -> Result<FloatBaseline, TpccError> {
    warn!("{PUBLIC_SPEC_NOTICE}");
    let prepared =
        prepare_bounded_online(dataset, evidence, initial_order_line_amounts, abandoned)?;
    run_plan(client, &prepared.integer_plan, &dataset.runtime_schema).await?;

    let values = read_float_aggregates(client, CheckScope::Online, &dataset.runtime_schema).await?;
    validate_public_float_ledger(
        aggregate_bits(&values, FloatAggregateId::HistoryAmount)?,
        aggregate_bits(&values, FloatAggregateId::StockYtd)?,
        aggregate_bits(&values, FloatAggregateId::OrderLineAmount)?,
        prepared.float_oracle,
    )
    .map_err(|error| {
        TpccError::QueryError(format!("public bounded FLOAT ledger gate failed: {error}"))
    })?;
    info!(
        "public bounded online consistency PASS; hidden official 6 SQL, keys, seed, and answers were not inferred"
    );
    Ok(values)
}

struct PreparedBoundedOnline {
    integer_plan: ConsistencyPlan,
    float_oracle: PublicFloatLedgerEvidence,
}

fn prepare_bounded_online(
    dataset: &DatasetState,
    evidence: &dyn TerminalEvidenceView,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
    abandoned: AbandonedWrites,
) -> Result<PreparedBoundedOnline, TpccError> {
    validate_terminal_evidence(evidence).map_err(|_| {
        TpccError::Protocol("online terminal evidence failed validation".to_owned())
    })?;
    validate_terminal_dataset_binding(
        dataset,
        evidence.intervals().warehouses(),
        evidence.intervals().sample_seed(),
    )?;
    bounded_payment_endpoint_expectations(dataset.warehouses, evidence.payment())?;
    let expectations = bounded_recovery_expectations(
        dataset,
        evidence.stats(),
        initial_order_line_amounts,
        abandoned,
    )?;
    let float_oracle = bounded_online_float_oracle(
        dataset,
        evidence.stats(),
        initial_order_line_amounts,
        abandoned,
    )?;
    let sample = dataset
        .online_key_sample()
        .map_err(|error| protocol_error("invalid online setup-evidence binding", error))?;
    let integer_plan = public_online_integer_plan(expectations, sample)
        .map_err(|error| protocol_error("invalid public online plan", error))?;
    if integer_plan.queries.len() != 6 {
        return Err(TpccError::Protocol(format!(
            "bounded online plan generated {} integer queries, expected 6",
            integer_plan.queries.len()
        )));
    }
    Ok(PreparedBoundedOnline {
        integer_plan,
        float_oracle,
    })
}

/// Execute post-crash validation from the bounded, canonically restorable
/// terminal oracle.
///
/// Public counts and partitions come from bounded statistics, exact Warehouse
/// and District values come from the Payment endpoint certificate, and
/// retained per-key mutations are checked by the bounded recovery sample
/// executor.
pub async fn run_final_recovery_from_terminal_evidence(
    client: &mut RmdbClient,
    dataset: &DatasetState,
    evidence: &dyn TerminalEvidenceView,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
    online_baseline: &FloatBaseline,
    abandoned: AbandonedWrites,
) -> Result<(), TpccError> {
    warn!("{PUBLIC_SPEC_NOTICE}");
    validate_terminal_evidence(evidence).map_err(|_| {
        TpccError::Protocol("recovery terminal evidence failed validation".to_owned())
    })?;
    validate_terminal_dataset_binding(
        dataset,
        evidence.intervals().warehouses(),
        evidence.intervals().sample_seed(),
    )?;
    let expectations = bounded_recovery_expectations(
        dataset,
        evidence.stats(),
        initial_order_line_amounts,
        abandoned,
    )?;
    // Construct every bounded oracle before issuing the first query so a
    // malformed or cross-dataset artifact cannot partially execute recovery.
    bounded_payment_endpoint_expectations(dataset.warehouses, evidence.payment())?;
    let partitions = bounded_partition_expectations(dataset, evidence.stats())?;

    let plan = recovery_plan(expectations)
        .map_err(|error| protocol_error("invalid public recovery plan", error))?;
    run_plan(client, &plan, &dataset.runtime_schema).await?;

    let recovered =
        read_float_aggregates(client, CheckScope::Recovery, &dataset.runtime_schema).await?;
    validate_float_baseline(online_baseline, &recovered)?;
    validate_bounded_payment_endpoints(
        client,
        &dataset.runtime_schema,
        dataset.warehouses,
        evidence.payment(),
    )
    .await?;

    recovery_partition_audits_for_warehouses(dataset.warehouses, partitions.clone())
        .map_err(|error| protocol_error("invalid bounded recovery partitions", error))?;
    run_grouped_partition_audit(client, &dataset.runtime_schema, &partitions).await?;
    recovery_sample_checker::check_recovery_samples(client, &dataset.runtime_schema, evidence)
        .await?;
    info!(
        "public bounded recovery consistency PASS; hidden official 37 SQL, generated keys, seed, and answers remain unavailable"
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

/// Execute the public 37 integer and 7 FLOAT32 recovery query shapes without
/// comparing values or publishing any formal-state receipt.
///
/// This deliberately non-scoring probe exists only to reproduce response
/// framing/deadline failures after a local crash when ranking may have ended
/// before terminal evidence could be sealed. The official SQL and answers are
/// unpublished; this is the same public-spec approximation used elsewhere.
pub async fn probe_public_post_crash_responses(
    client: &mut RmdbClient,
    dataset: &DatasetState,
) -> Result<(), TpccError> {
    warn!(
        "running non-scoring public post-crash response probe; values are not compared and no \
         recovery receipt will be written"
    );
    let plan = recovery_plan(RecoveryExpectations {
        setup: SetupExpectations {
            warehouses: dataset.warehouses,
            order_line_rows: dataset.order_line_rows,
            undelivered_order_line_rows: dataset.undelivered_order_line_rows,
        },
        committed: Default::default(),
        abandoned: Default::default(),
    })
    .map_err(|error| protocol_error("invalid local recovery response plan", error))?;
    let mut ordinal = 0_usize;
    for query in &plan.queries {
        ordinal += 1;
        probe_response(
            client,
            &dataset.runtime_schema,
            ordinal,
            &query.id,
            &query.sql,
        )
        .await?;
    }
    for query in &float_aggregate_plan(CheckScope::Recovery).queries {
        ordinal += 1;
        probe_response(
            client,
            &dataset.runtime_schema,
            ordinal,
            &query.id,
            &query.sql,
        )
        .await?;
    }
    for query in setup_sample_queries(dataset.setup_evidence())? {
        ordinal += 1;
        probe_response(
            client,
            &dataset.runtime_schema,
            ordinal,
            query.id,
            &query.sql,
        )
        .await?;
    }
    for (id, sql) in grouped_partition_query_specs() {
        ordinal += 1;
        probe_response(client, &dataset.runtime_schema, ordinal, id, sql).await?;
    }
    info!(
        "non-scoring public post-crash response probe completed {ordinal} public-spec request shapes"
    );
    Ok(())
}

async fn probe_response(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    ordinal: usize,
    shape: &str,
    sql: &str,
) -> Result<(), TpccError> {
    let started = Instant::now();
    let first_frame_seen = Arc::new(AtomicBool::new(false));
    let meta_seen = Arc::clone(&first_frame_seen);
    let row_seen = Arc::clone(&first_frame_seen);
    info!("post-crash probe SEND ordinal={ordinal} shape={shape}");

    let rendered = schema.render_sql(sql);
    let response = client
        .exec_stream_fold(
            &terminated_sql(&rendered),
            0_u64,
            move |_, _| {
                if !meta_seen.swap(true, Ordering::Relaxed) {
                    info!(
                        "post-crash probe FIRST_FRAME ordinal={ordinal} shape={shape} elapsed_ms={}",
                        started.elapsed().as_millis()
                    );
                }
                Ok(())
            },
            move |_, _, rows| {
                if !row_seen.swap(true, Ordering::Relaxed) {
                    info!(
                        "post-crash probe FIRST_FRAME ordinal={ordinal} shape={shape} elapsed_ms={}",
                        started.elapsed().as_millis()
                    );
                }
                *rows = rows.checked_add(1).ok_or_else(|| {
                    crate::connection::wire::WireError::Protocol(
                        "post-crash probe row count overflow".to_owned(),
                    )
                })?;
                Ok(())
            },
        )
        .await
        .map_err(|error| {
            TpccError::Protocol(format!(
                "post-crash probe ordinal={ordinal} shape={shape} failed after {} ms: {error}",
                started.elapsed().as_millis()
            ))
        })?;

    let (terminal, rows) = match response {
        FoldStreamResponse::Query {
            row_count, state, ..
        } => {
            if row_count != state {
                return Err(TpccError::Protocol(format!(
                    "post-crash probe ordinal={ordinal} shape={shape} folded {state} rows but terminal declared {row_count}"
                )));
            }
            ("RESULT_END", row_count)
        }
        FoldStreamResponse::CommandOk => ("COMMAND_OK", 0),
        FoldStreamResponse::TransactionAbort { .. } => ("TRANSACTION_ABORT", 0),
        FoldStreamResponse::Error { .. } => ("ERROR", 0),
    };
    info!(
        "post-crash probe TERMINAL ordinal={ordinal} shape={shape} terminal={terminal} rows={rows} elapsed_ms={}",
        started.elapsed().as_millis()
    );
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

fn bounded_recovery_expectations(
    dataset: &DatasetState,
    stats: &BoundedPhysicalStats,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
    abandoned: AbandonedWrites,
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
        committed: stats
            .to_committed_ledger()
            .map_err(|error| protocol_error("invalid bounded recovery statistics", error))?,
        abandoned,
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

fn validate_terminal_dataset_binding(
    dataset: &DatasetState,
    evidence_warehouses: u16,
    evidence_seed: u64,
) -> Result<(), TpccError> {
    validate_consistency_warehouse_count(dataset.warehouses)?;
    let dataset_warehouses = u16::try_from(dataset.warehouses)
        .map_err(|_| TpccError::Protocol("recovery warehouse count exceeds UINT16".to_owned()))?;
    if evidence_warehouses != dataset_warehouses || evidence_seed != dataset.seed {
        return Err(TpccError::Protocol(
            "terminal evidence is bound to a different recovery dataset".to_owned(),
        ));
    }
    Ok(())
}

fn bounded_online_float_oracle(
    dataset: &DatasetState,
    stats: &BoundedPhysicalStats,
    initial_order_line_amounts: &NonNegativeF32Accumulator,
    abandoned: AbandonedWrites,
) -> Result<PublicFloatLedgerEvidence, TpccError> {
    validate_consistency_warehouse_count(dataset.warehouses)?;
    let initial_order_line_terms = u64::try_from(dataset.order_line_rows).map_err(|_| {
        TpccError::Protocol("dataset order-line count is negative or too large".to_owned())
    })?;
    if initial_order_line_amounts.term_count() != initial_order_line_terms {
        return Err(TpccError::Protocol(format!(
            "initial order-line FLOAT accumulator has {} terms, dataset records {initial_order_line_terms}",
            initial_order_line_amounts.term_count()
        )));
    }
    let committed = stats
        .to_committed_ledger()
        .map_err(|error| protocol_error("invalid bounded online statistics", error))?;

    let initial_history_rows = u64::try_from(dataset.warehouses)
        .ok()
        .and_then(|warehouses| warehouses.checked_mul(DISTRICTS_PER_WAREHOUSE as u64))
        .and_then(|partitions| partitions.checked_mul(3_000))
        .ok_or_else(|| TpccError::Protocol("initial history row count overflowed".to_owned()))?;
    let mut history = NonNegativeF32Accumulator::default();
    history
        .add_repeated_bits(10.0_f32.to_bits(), initial_history_rows)
        .map_err(|error| protocol_error("initial history accumulator failed", error))?;
    let payment_history = stats
        .payment_history_amounts()
        .map_err(|error| protocol_error("bounded Payment history accumulator failed", error))?;
    history
        .merge(&payment_history)
        .map_err(|error| protocol_error("Payment history accumulator failed", error))?;

    let mut order_line = initial_order_line_amounts.clone();
    let committed_order_lines = stats
        .new_order_line_amounts()
        .map_err(|error| protocol_error("bounded NewOrder line accumulator failed", error))?;
    order_line
        .merge(&committed_order_lines)
        .map_err(|error| protocol_error("order-line accumulator failed", error))?;

    let stock_ytd = committed.stock_ytd_delta as f32;
    if !stock_ytd.is_finite() {
        return Err(TpccError::Protocol(
            "bounded stock YTD total cannot be represented as finite FLOAT32".to_owned(),
        ));
    }
    let max_stock_ytd = committed
        .stock_ytd_delta
        .checked_add(
            abandoned
                .new_orders
                .checked_mul(MAX_NEW_ORDER_STOCK_YTD)
                .ok_or_else(|| {
                    TpccError::Protocol("abandoned stock YTD upper bound overflowed".to_owned())
                })?,
        )
        .ok_or_else(|| TpccError::Protocol("stock YTD upper bound overflowed".to_owned()))? as f32;
    let max_order_lines = abandoned
        .new_orders
        .checked_mul(MAX_NEW_ORDER_OL_COUNT as i64)
        .ok_or_else(|| {
            TpccError::Protocol("abandoned order-line count upper bound overflowed".to_owned())
        })? as u64;
    Ok(PublicFloatLedgerEvidence {
        history_amount: history
            .boundary_with_abandoned(
                abandoned.payments as u64,
                MAX_PAYMENT_H_AMOUNT.to_bits(),
            )
            .map_err(|error| protocol_error("history boundary failed", error))?,
        stock_ytd: Float32RangeBits {
            expected_bits: stock_ytd.to_bits(),
            lower_bits: stock_ytd.to_bits(),
            upper_bits: max_stock_ytd.to_bits(),
        },
        order_line_amount: order_line
            .boundary_with_abandoned(max_order_lines, MAX_NEW_ORDER_LINE_AMOUNT.to_bits())
            .map_err(|error| protocol_error("order-line boundary failed", error))?,
    })
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

async fn validate_bounded_payment_endpoints(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    warehouse_count: i32,
    endpoints: &dyn PaymentEndpointView,
) -> Result<(), TpccError> {
    let (expected_warehouses, expected_districts) =
        bounded_payment_endpoint_expectations(warehouse_count, endpoints)?;
    let warehouses = execute_typed_sql(
        client,
        schema,
        "recovery.payment.warehouse_endpoints",
        "SELECT w_id, w_ytd FROM warehouse",
    )
    .await?;
    validate_float_endpoint_rows("warehouse w_ytd", warehouses, expected_warehouses, false)?;

    let districts = execute_typed_sql(
        client,
        schema,
        "recovery.payment.district_endpoints",
        "SELECT d_w_id, d_id, d_ytd FROM district",
    )
    .await?;
    validate_float_endpoint_rows("district d_ytd", districts, expected_districts, true)?;
    info!("recovery bounded Payment warehouse/district endpoints PASS (0 ULP)");
    Ok(())
}

type FloatEndpointExpectations = (Vec<(PartitionKey, u32)>, Vec<(PartitionKey, u32)>);

fn bounded_payment_endpoint_expectations(
    warehouse_count: i32,
    endpoints: &dyn PaymentEndpointView,
) -> Result<FloatEndpointExpectations, TpccError> {
    validate_consistency_warehouse_count(warehouse_count)?;
    let expected_warehouses = u16::try_from(warehouse_count)
        .map_err(|_| TpccError::Protocol("recovery warehouse count exceeds UINT16".to_owned()))?;
    if endpoints.warehouses() != expected_warehouses
        || endpoints.warehouse_edge_count() != endpoints.terminal_count()
        || endpoints.district_edge_count() != endpoints.terminal_count()
    {
        return Err(TpccError::Protocol(
            "bounded Payment evidence disagrees with the recovery dataset".to_owned(),
        ));
    }

    let mut warehouse_rows = Vec::with_capacity(usize::from(expected_warehouses));
    let mut district_rows =
        Vec::with_capacity(usize::from(expected_warehouses) * DISTRICTS_PER_WAREHOUSE as usize);
    let mut warehouse_updates = 0_u64;
    let mut district_updates = 0_u64;
    for warehouse_id in 1..=expected_warehouses {
        let endpoint_bits = endpoints
            .warehouse_endpoint_bits(warehouse_id)
            .ok_or_else(|| {
                TpccError::Protocol(
                    "bounded Payment evidence omitted a warehouse endpoint".to_owned(),
                )
            })?;
        require_finite_endpoint_bits(endpoint_bits)?;
        warehouse_updates = warehouse_updates
            .checked_add(
                endpoints
                    .warehouse_update_count(warehouse_id)
                    .ok_or_else(|| {
                        TpccError::Protocol(
                            "bounded Payment evidence omitted a warehouse update count".to_owned(),
                        )
                    })?,
            )
            .ok_or_else(|| {
                TpccError::Protocol("bounded Payment warehouse count overflowed".to_owned())
            })?;
        warehouse_rows.push((
            PartitionKey {
                warehouse_id: i32::from(warehouse_id),
                district_id: 0,
            },
            endpoint_bits,
        ));

        for district_id in 1..=DISTRICTS_PER_WAREHOUSE as u8 {
            let endpoint_bits = endpoints
                .district_endpoint_bits(warehouse_id, district_id)
                .ok_or_else(|| {
                    TpccError::Protocol(
                        "bounded Payment evidence omitted a district endpoint".to_owned(),
                    )
                })?;
            require_finite_endpoint_bits(endpoint_bits)?;
            district_updates = district_updates
                .checked_add(
                    endpoints
                        .district_update_count(warehouse_id, district_id)
                        .ok_or_else(|| {
                            TpccError::Protocol(
                                "bounded Payment evidence omitted a district update count"
                                    .to_owned(),
                            )
                        })?,
                )
                .ok_or_else(|| {
                    TpccError::Protocol("bounded Payment district count overflowed".to_owned())
                })?;
            district_rows.push((
                PartitionKey {
                    warehouse_id: i32::from(warehouse_id),
                    district_id: i32::from(district_id),
                },
                endpoint_bits,
            ));
        }
    }
    if warehouse_updates != endpoints.terminal_count()
        || district_updates != endpoints.terminal_count()
    {
        return Err(TpccError::Protocol(
            "bounded Payment per-key counts disagree with the terminal total".to_owned(),
        ));
    }
    Ok((warehouse_rows, district_rows))
}

fn require_finite_endpoint_bits(bits: u32) -> Result<(), TpccError> {
    if f32::from_bits(bits).is_finite() {
        Ok(())
    } else {
        Err(TpccError::Protocol(
            "bounded Payment endpoint is not finite binary32".to_owned(),
        ))
    }
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

fn bounded_partition_expectations(
    dataset: &DatasetState,
    stats: &BoundedPhysicalStats,
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
            let delta = stats
                .partition_totals(key.warehouse_id, key.district_id)
                .map_err(|error| protocol_error("invalid bounded recovery partition", error))?;
            let new_orders = checked_partition_u64(delta.new_orders, "partition new orders")?;
            let new_order_lines =
                checked_partition_u64(delta.new_order_lines, "partition new-order lines")?;
            let delivered_orders =
                checked_partition_u64(delta.delivered_orders, "partition delivered orders")?;
            let delivered_order_lines = checked_partition_u64(
                delta.delivered_order_lines,
                "partition delivered order lines",
            )?;
            let order_count = checked_partition_add(
                i64::from(ORDERS_PER_DISTRICT),
                new_orders,
                "partition order count",
            )?;
            let order_line_count = checked_partition_add(
                initial.order_line_rows,
                new_order_lines,
                "partition order-line count",
            )?;
            let new_order_count = checked_partition_add(
                checked_partition_add(
                    i64::from(NEW_ORDERS_PER_DISTRICT),
                    new_orders,
                    "partition new-order count",
                )?,
                -delivered_orders,
                "partition new-order count",
            )?;
            let empty_delivery_time_count = checked_partition_add(
                checked_partition_add(
                    initial.undelivered_order_line_rows,
                    new_order_lines,
                    "partition empty delivery count",
                )?,
                -delivered_order_lines,
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

fn checked_partition_u64(value: u64, name: &str) -> Result<i64, TpccError> {
    i64::try_from(value).map_err(|_| TpccError::Protocol(format!("{name} exceeds INT64")))
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

const GROUPED_PARTITION_QUERIES: [(&str, &str); 6] = [
    (
        "recovery.partition.grouped.orders",
        "SELECT o_w_id, o_d_id, COUNT(*) FROM orders GROUP BY o_w_id, o_d_id",
    ),
    (
        "recovery.partition.grouped.order_lines",
        "SELECT ol_w_id, ol_d_id, COUNT(*) FROM order_line GROUP BY ol_w_id, ol_d_id",
    ),
    (
        "recovery.partition.grouped.new_orders",
        "SELECT no_w_id, no_d_id, COUNT(*) FROM new_orders GROUP BY no_w_id, no_d_id",
    ),
    (
        "recovery.partition.grouped.empty_delivery_times",
        "SELECT ol_w_id, ol_d_id, COUNT(*) FROM order_line WHERE ol_delivery_d = '' GROUP BY ol_w_id, ol_d_id",
    ),
    (
        "recovery.partition.grouped.carrier_zero",
        "SELECT o_w_id, o_d_id, COUNT(*) FROM orders WHERE o_carrier_id = 0 GROUP BY o_w_id, o_d_id",
    ),
    (
        "recovery.partition.grouped.next_order_id",
        "SELECT d_w_id, d_id, d_next_o_id FROM district",
    ),
];

fn grouped_partition_query_specs() -> impl Iterator<Item = (&'static str, &'static str)> {
    GROUPED_PARTITION_QUERIES.into_iter()
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
    for ((id, sql), metric) in GROUPED_PARTITION_QUERIES[..5].iter().copied().zip([
        PartitionMetric::Orders,
        PartitionMetric::OrderLines,
        PartitionMetric::NewOrders,
        PartitionMetric::EmptyDeliveryTimes,
        PartitionMetric::CarrierZero,
    ]) {
        let result = execute_typed_sql(client, schema, id, sql).await?;
        validate_grouped_partition_counts(id, result, partitions, metric)?;
        info!(
            "consistency PASS: {id} ({} partitions in one typed response)",
            partitions.len()
        );
    }

    let (id, sql) = GROUPED_PARTITION_QUERIES[5];
    let result = execute_typed_sql(client, schema, id, sql).await?;
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
    match client
        .exec_consistency_stream(&terminated_sql(&rendered), id)
        .await?
    {
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
    use std::collections::BTreeSet;
    use std::time::Duration;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    use super::*;
    use crate::connection::wire::HANDSHAKE;
    use crate::consistency::PUBLIC_RECOVERY_INTEGER_CHECK_COUNT;
    use crate::data_gen::TpccDataGen;
    use crate::loader::{LoadSummary, PartitionLoadSummary};
    use crate::ranking::bounded_stats::{
        ClassTotals, PartitionTotals, LEDGER_CLASS_COUNT, PHYSICAL_PARTITION_COUNT,
    };
    use crate::ranking::evidence_collector::{
        CustomerKey as EvidenceCustomerKey, SealedIntervalEvidence, StockKey,
    };
    use crate::ranking::preflight::PreparedPathPreflightProof;
    use crate::ranking::rich_recovery_samples::{
        InitialCustomerData, InitialHistoryRow, SealedRichRecoverySamples,
    };
    use crate::ranking::runner::StockVersion;
    use crate::ranking::terminal_evidence::{SealedTerminalEvidence, TerminalEvidenceCollector};
    use crate::runtime_schema::{LogicalTable, SchemaMode};
    use crate::sample_evidence::setup_evidence_fixture;

    struct EmptyPaymentEndpoints {
        warehouses: u16,
        omit_last_warehouse: bool,
        nonfinite_last_warehouse: bool,
    }

    impl PaymentEndpointView for EmptyPaymentEndpoints {
        fn warehouses(&self) -> u16 {
            self.warehouses
        }

        fn terminal_count(&self) -> u64 {
            0
        }

        fn warehouse_edge_count(&self) -> u64 {
            0
        }

        fn district_edge_count(&self) -> u64 {
            0
        }

        fn warehouse_endpoint_bits(&self, warehouse_id: u16) -> Option<u32> {
            if self.omit_last_warehouse && warehouse_id == self.warehouses {
                None
            } else if self.nonfinite_last_warehouse && warehouse_id == self.warehouses {
                Some(f32::INFINITY.to_bits())
            } else {
                Some(300_000.0_f32.to_bits())
            }
        }

        fn warehouse_update_count(&self, warehouse_id: u16) -> Option<u64> {
            self.warehouse_endpoint_bits(warehouse_id).map(|_| 0)
        }

        fn district_endpoint_bits(&self, warehouse_id: u16, district_id: u8) -> Option<u32> {
            (warehouse_id > 0
                && warehouse_id <= self.warehouses
                && district_id > 0
                && district_id <= DISTRICTS_PER_WAREHOUSE as u8)
                .then(|| 30_000.0_f32.to_bits())
        }

        fn district_update_count(&self, warehouse_id: u16, district_id: u8) -> Option<u64> {
            self.district_endpoint_bits(warehouse_id, district_id)
                .map(|_| 0)
        }
    }

    struct PaymentOverrideEvidence<'a> {
        inner: &'a SealedTerminalEvidence,
        payment: &'a dyn PaymentEndpointView,
    }

    impl TerminalEvidenceView for PaymentOverrideEvidence<'_> {
        fn policy_version(&self) -> u32 {
            self.inner.policy_version()
        }

        fn stats(&self) -> &BoundedPhysicalStats {
            self.inner.stats()
        }

        fn intervals(&self) -> &SealedIntervalEvidence {
            self.inner.intervals()
        }

        fn payment(&self) -> &dyn PaymentEndpointView {
            self.payment
        }

        fn rich(&self) -> &SealedRichRecoverySamples {
            self.inner.rich()
        }
    }

    async fn empty_terminal_evidence(dataset: &DatasetState) -> SealedTerminalEvidence {
        let warehouses = u16::try_from(dataset.warehouses).unwrap();
        let setup = dataset.setup_evidence();
        let timestamp = String::from_utf8(setup.load_timestamp.clone()).unwrap();
        let history_generator = TpccDataGen::with_seed_and_timestamp(
            dataset.warehouses,
            setup.load_seed,
            timestamp.clone(),
        );
        let customer_generator =
            TpccDataGen::with_seed_and_timestamp(dataset.warehouses, setup.load_seed, timestamp);
        let initial_history = move |customer: EvidenceCustomerKey| {
            history_generator
                .initial_history(
                    customer.warehouse_id,
                    customer.district_id,
                    customer.customer_id,
                )
                .map(|history| {
                    InitialHistoryRow::new(
                        history.h_date.into_bytes(),
                        (history.h_amount as f32).to_bits(),
                        history.h_data.into_bytes(),
                    )
                    .unwrap()
                })
        };
        let initial_customer = move |customer: EvidenceCustomerKey| {
            customer_generator
                .initial_customer_profile(
                    customer.warehouse_id,
                    customer.district_id,
                    customer.customer_id,
                )
                .map(|profile| {
                    InitialCustomerData::new(*profile.credit(), profile.data().to_vec()).unwrap()
                })
        };
        let stock_roots = |_stock: StockKey| {
            Some(StockVersion {
                quantity: 50,
                ytd_bits: 0.0_f32.to_bits(),
                order_count: 0,
                remote_count: 0,
            })
        };
        let collector = TerminalEvidenceCollector::new(
            warehouses,
            1,
            dataset.seed,
            stock_roots,
            initial_history,
            initial_customer,
            PreparedPathPreflightProof::verified_for_test(dataset.seed, warehouses),
        )
        .unwrap();
        collector.worker_finished(0).await.unwrap();
        let sealed = collector.seal().await.unwrap();
        validate_terminal_evidence(&sealed).unwrap();
        sealed
    }

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
    fn bounded_sf1_recovery_uses_its_complete_dataset_keyspace() {
        let dataset = smoke_dataset(1);
        let stats = BoundedPhysicalStats::default();
        let expectations = bounded_recovery_expectations(
            &dataset,
            &stats,
            dataset.initial_order_line_amounts(),
            AbandonedWrites::default(),
        )
        .unwrap();
        assert_eq!(expectations.setup.warehouses, 1);

        let partitions = bounded_partition_expectations(&dataset, &stats).unwrap();
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
    fn terminal_recovery_binding_rejects_wrong_seed_or_scale() {
        let dataset = smoke_dataset(1);
        assert!(validate_terminal_dataset_binding(&dataset, 1, dataset.seed).is_ok());
        assert!(validate_terminal_dataset_binding(&dataset, 1, dataset.seed ^ 1).is_err());
        assert!(validate_terminal_dataset_binding(&dataset, 2, dataset.seed).is_err());
    }

    #[test]
    fn bounded_payment_endpoints_are_total_and_dataset_bound() {
        let complete = EmptyPaymentEndpoints {
            warehouses: 1,
            omit_last_warehouse: false,
            nonfinite_last_warehouse: false,
        };
        let (warehouses, districts) = bounded_payment_endpoint_expectations(1, &complete).unwrap();
        assert_eq!(warehouses.len(), 1);
        assert_eq!(districts.len(), DISTRICTS_PER_WAREHOUSE as usize);

        let wrong_scale = EmptyPaymentEndpoints {
            warehouses: 2,
            omit_last_warehouse: false,
            nonfinite_last_warehouse: false,
        };
        assert!(bounded_payment_endpoint_expectations(1, &wrong_scale).is_err());

        let missing = EmptyPaymentEndpoints {
            warehouses: 1,
            omit_last_warehouse: true,
            nonfinite_last_warehouse: false,
        };
        assert!(bounded_payment_endpoint_expectations(1, &missing).is_err());

        let nonfinite = EmptyPaymentEndpoints {
            warehouses: 1,
            omit_last_warehouse: false,
            nonfinite_last_warehouse: true,
        };
        assert!(bounded_payment_endpoint_expectations(1, &nonfinite).is_err());
    }

    #[tokio::test]
    async fn bounded_online_prepares_exact_public_query_surface() {
        let dataset = smoke_dataset(1);
        let evidence = empty_terminal_evidence(&dataset).await;
        let prepared = prepare_bounded_online(
            &dataset,
            &evidence,
            dataset.initial_order_line_amounts(),
            AbandonedWrites::default(),
        )
        .unwrap();

        assert_eq!(prepared.integer_plan.queries.len(), 6);
        assert!(prepared
            .integer_plan
            .queries
            .iter()
            .all(|query| query.id.starts_with("online.public.")));
        // No abandoned writes keep the online next-order gate exact.
        let next_sum = prepared
            .integer_plan
            .queries
            .iter()
            .find(|query| query.id == "online.public.district_next_sum")
            .unwrap();
        assert!(matches!(next_sum.expectation, ScalarExpectation::ExactInt(_)));
        assert_eq!(
            float_aggregate_plan(CheckScope::Online).queries.len(),
            FLOAT_AGGREGATES.len()
        );
        assert_eq!(FLOAT_AGGREGATES.len(), 7);

        assert!(
            prepare_bounded_online(
                &dataset,
                &evidence,
                &NonNegativeF32Accumulator::default(),
                AbandonedWrites::default(),
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn malformed_bounded_online_evidence_fails_before_first_wire_request() {
        let dataset = smoke_dataset(1);
        let evidence = empty_terminal_evidence(&dataset).await;
        let incomplete_payment = EmptyPaymentEndpoints {
            warehouses: 1,
            omit_last_warehouse: true,
            nonfinite_last_warehouse: false,
        };
        let incomplete_evidence = PaymentOverrideEvidence {
            inner: &evidence,
            payment: &incomplete_payment,
        };

        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0_u8; HANDSHAKE.len()];
            socket.read_exact(&mut handshake).await.unwrap();
            assert_eq!(handshake, HANDSHAKE);
            socket.write_all(&HANDSHAKE).await.unwrap();
            matches!(
                tokio::time::timeout(Duration::from_millis(200), socket.read_u8()).await,
                Ok(Ok(_))
            )
        });
        let mut client =
            RmdbClient::connect_with_timeout("127.0.0.1", port, Duration::from_secs(1))
                .await
                .unwrap();

        assert!(run_final_online_from_terminal_evidence(
            &mut client,
            &dataset,
            &incomplete_evidence,
            dataset.initial_order_line_amounts(),
            AbandonedWrites::default(),
        )
        .await
        .is_err());
        assert!(
            !server.await.unwrap(),
            "bounded evidence validation emitted a Wire request"
        );
    }

    #[test]
    fn bounded_online_float_oracle_merges_all_nine_physical_classes() {
        let dataset = smoke_dataset(1);
        let mut classes = [ClassTotals::default(); LEDGER_CLASS_COUNT];
        let mut partitions = [PartitionTotals::default(); PHYSICAL_PARTITION_COUNT];
        let mut payment_amounts = std::array::from_fn(|_| NonNegativeF32Accumulator::default());
        let mut order_line_amounts = std::array::from_fn(|_| NonNegativeF32Accumulator::default());
        let delivery_amounts = std::array::from_fn(|_| NonNegativeF32Accumulator::default());
        let mut class_bits = Vec::with_capacity(LEDGER_CLASS_COUNT);
        for index in 0..LEDGER_CLASS_COUNT {
            let bits = ((index + 1) as f32).to_bits();
            class_bits.push(bits);
            classes[index] = ClassTotals {
                new_order_commits: 1,
                payment_commits: 1,
                new_orders: 1,
                new_order_lines: 5,
                stock_quantity_delta: 5,
                ..ClassTotals::default()
            };
            payment_amounts[index].add_bits(bits).unwrap();
            order_line_amounts[index]
                .add_repeated_bits(bits, 5)
                .unwrap();
        }
        partitions[0] = PartitionTotals {
            new_orders: LEDGER_CLASS_COUNT as u64,
            new_order_lines: (LEDGER_CLASS_COUNT * 5) as u64,
            ..PartitionTotals::default()
        };
        let stats = BoundedPhysicalStats::from_canonical_parts(
            classes,
            partitions,
            payment_amounts,
            order_line_amounts,
            delivery_amounts,
        )
        .unwrap();

        let oracle = bounded_online_float_oracle(
            &dataset,
            &stats,
            dataset.initial_order_line_amounts(),
            AbandonedWrites::default(),
        )
        .unwrap();
        let mut expected_history = NonNegativeF32Accumulator::default();
        expected_history
            .add_repeated_bits(10.0_f32.to_bits(), (DISTRICTS_PER_WAREHOUSE * 3_000) as u64)
            .unwrap();
        let mut expected_order_lines = dataset.initial_order_line_amounts().clone();
        for bits in class_bits {
            expected_history.add_bits(bits).unwrap();
            expected_order_lines.add_repeated_bits(bits, 5).unwrap();
        }

        assert_eq!(oracle.history_amount, expected_history.boundary().unwrap());
        assert_eq!(
            oracle.order_line_amount,
            expected_order_lines.boundary().unwrap()
        );
        assert_eq!(
            oracle.stock_ytd,
            Float32RangeBits::exact(45.0_f32.to_bits())
        );
    }

    #[test]
    fn recovery_integer_gate_uses_production_opaque_renderer() {
        let dataset = smoke_dataset(1);
        assert_eq!(dataset.runtime_schema.mode(), SchemaMode::LocalSeedOpaqueV1);

        let expectations = bounded_recovery_expectations(
            &dataset,
            &BoundedPhysicalStats::default(),
            dataset.initial_order_line_amounts(),
            AbandonedWrites::default(),
        )
        .unwrap();
        let plan = recovery_plan(expectations).unwrap();
        assert_eq!(plan.queries.len(), PUBLIC_RECOVERY_INTEGER_CHECK_COUNT);

        for query in &plan.queries {
            let rendered = dataset.runtime_schema.render_sql(&query.sql);
            assert_ne!(rendered, query.sql, "{} was not rendered", query.id);

            let logical_tokens = query
                .sql
                .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .collect::<Vec<_>>();
            let rendered_tokens = rendered
                .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .collect::<Vec<_>>();
            let mut rewritten_identifiers = 0;
            for table in LogicalTable::ALL {
                for identifier in
                    std::iter::once(table.canonical()).chain(table.columns().iter().copied())
                {
                    if logical_tokens.contains(&identifier) {
                        rewritten_identifiers += 1;
                        assert!(
                            !rendered_tokens.contains(&identifier),
                            "{} leaked canonical identifier {identifier}",
                            query.id
                        );
                    }
                }
            }
            assert!(
                rewritten_identifiers > 0,
                "{} exercised no runtime identifier",
                query.id
            );
        }
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
    fn response_probe_covers_every_grouped_partition_shape_once() {
        let specs = grouped_partition_query_specs().collect::<Vec<_>>();
        assert_eq!(specs.len(), 6);
        assert_eq!(
            specs
                .iter()
                .map(|(id, _)| *id)
                .collect::<BTreeSet<_>>()
                .len(),
            specs.len()
        );
        assert!(specs.iter().all(
            |(id, sql)| id.starts_with("recovery.partition.grouped.") && sql.contains("SELECT")
        ));
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
    fn setup_sample_queries_are_bounded_and_cover_every_published_relationship() {
        let evidence = setup_evidence_fixture(50, 2026);
        let queries = setup_sample_queries(&evidence).unwrap();
        let warehouse_count = evidence
            .anchors
            .iter()
            .map(|anchor| anchor.warehouse.id)
            .collect::<BTreeSet<_>>()
            .len();
        assert_eq!(
            queries.len(),
            warehouse_count
                + 6 * evidence.anchors.len()
                + evidence.anchors.len().min(2)
                + evidence.items.len()
                + evidence.stocks.len()
        );
        assert!(queries.iter().all(|query| query.sql.len() < 4 * 1024));
        assert_eq!(
            queries
                .iter()
                .filter(|query| query.id == "setup.sample.history_to_customer")
                .count(),
            evidence.anchors.len().min(2)
        );
        assert_eq!(
            queries
                .iter()
                .filter(|query| query.id == "setup.sample.item_content")
                .count(),
            evidence.items.len()
        );
        assert_eq!(
            queries
                .iter()
                .filter(|query| query.id == "setup.sample.stock_content")
                .count(),
            evidence.stocks.len()
        );

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
            .filter(|query| query.id == "setup.sample.history_to_customer")
            .collect::<Vec<_>>();
        assert!(history
            .iter()
            .all(|query| query.sql.contains("customer.c_id = history.h_c_id")));
        assert!(history.iter().all(|query| query.expected_rows.len() == 1));
        assert_eq!(
            history.first().unwrap().expected_rows[0][..2],
            [
                TypedValue::Int32(evidence.anchors[0].history.customer_warehouse_id),
                TypedValue::Int32(evidence.anchors[0].history.customer_district_id),
            ]
        );
        let last = evidence.anchors.last().unwrap();
        assert_eq!(
            history.last().unwrap().expected_rows[0][..2],
            [
                TypedValue::Int32(last.history.customer_warehouse_id),
                TypedValue::Int32(last.history.customer_district_id),
            ]
        );
    }

    #[test]
    fn setup_sample_queries_use_parser_supported_conjunctive_point_predicates() {
        let evidence = setup_evidence_fixture(50, 2026);
        for query in setup_sample_queries(&evidence).unwrap() {
            assert!(
                !query.sql.contains(" OR "),
                "{} contains unsupported OR",
                query.id
            );
            let (_, predicate) = query.sql.split_once(" WHERE ").unwrap();
            assert!(
                !predicate.contains('(') && !predicate.contains(')'),
                "{} contains an unsupported parenthesized predicate",
                query.id
            );
        }
    }

    #[test]
    fn every_setup_sample_query_renders_only_opaque_runtime_identifiers() {
        let evidence = setup_evidence_fixture(50, 2026);
        let schema = RuntimeSchema::opaque(2026).unwrap();
        let canonical_identifiers = LogicalTable::ALL
            .iter()
            .flat_map(|table| {
                std::iter::once(table.canonical()).chain(table.columns().iter().copied())
            })
            .collect::<BTreeSet<_>>();

        for query in setup_sample_queries(&evidence).unwrap() {
            let rendered = schema.render_sql(&query.sql);
            assert_ne!(rendered, query.sql, "{} was not rendered", query.id);
            let rendered_tokens = rendered
                .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .filter(|token| !token.is_empty())
                .collect::<BTreeSet<_>>();
            assert!(
                canonical_identifiers.is_disjoint(&rendered_tokens),
                "{} leaked a canonical identifier after rendering",
                query.id
            );
        }
    }

    #[test]
    fn setup_order_sum_is_derived_once_from_persisted_raw_float_bits() {
        let evidence = setup_evidence_fixture(50, 2026);
        let first = &evidence.anchors[0];
        let expected_bits =
            sum_f32_as_f64_once(first.lines.iter().map(|line| line.amount_bits)).unwrap();
        let query = setup_sample_queries(&evidence)
            .unwrap()
            .into_iter()
            .find(|query| query.id == "setup.sample.undelivered_order_sum")
            .unwrap();
        assert_eq!(
            query.expected_rows,
            vec![vec![TypedValue::Float32(expected_bits)]]
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
