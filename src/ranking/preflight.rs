//! Untimed semantic probes for the prepared ranked path.
//!
//! These probes run only after every ranked connection has installed the
//! prepared catalogue and before the timing barrier is released.  They use
//! the same typed statement ids as the ranked transactions.  The Stock-Level
//! cross-check deliberately reconstructs an answer only as an independent
//! preflight oracle; the measured transaction continues to execute the single
//! prepared `COUNT(DISTINCT ...)` statement.

use std::collections::{BTreeMap, BTreeSet};

use crate::connection::client::RmdbClient;
use crate::connection::prepared::Operation;
use crate::connection::wire::WireValue;
use crate::error::TpccError;
use crate::profile::{DISTRICTS_PER_WAREHOUSE, ITEM_COUNT};
use crate::workload::{CUSTOMERS_PER_DISTRICT, INVALID_ITEM_ID};

use super::catalog::{StatementId, UNDELIVERED_CARRIER_ID, UNDELIVERED_DATE};
use super::common::{operation, row_char, row_f32_bits, row_int32, BatchResults};
use super::runner::{execute_batch, StockVersion};

const PREFLIGHT_VALID_LINES: usize = 5;
const STOCK_LEVEL_RECENT_ORDERS: i32 = 20;
const STOCK_LEVEL_MIN_THRESHOLD: i32 = 10;
const STOCK_LEVEL_THRESHOLD_SPAN: u64 = 11;
const MAX_DETAIL_BATCH_OPERATIONS: usize = 200;

/// Run the deterministic, non-measured semantic preflight on one already
/// configured and prepared ranked connection.
pub async fn run(client: &mut RmdbClient, seed: u64, warehouses: u16) -> Result<(), TpccError> {
    let selection = PreflightSelection::derive(seed, warehouses)?;
    verify_stock_level(client, &selection).await?;
    verify_new_order_rollback(client, &selection).await
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreflightLine {
    number: i32,
    item_id: i32,
    quantity: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreflightSelection {
    warehouse_id: i32,
    district_id: i32,
    customer_id: i32,
    stock_threshold: i32,
    timestamp: Vec<u8>,
    valid_lines: Vec<PreflightLine>,
}

impl PreflightSelection {
    fn derive(seed: u64, warehouses: u16) -> Result<Self, TpccError> {
        if warehouses == 0 {
            return Err(preflight_protocol(
                "warehouse count must be positive for semantic preflight",
            ));
        }

        let warehouse_id =
            1 + domain_sample(seed, "preflight/warehouse", 0, u64::from(warehouses)) as i32;
        let district_id = 1 + domain_sample(
            seed,
            "preflight/district",
            0,
            u64::from(DISTRICTS_PER_WAREHOUSE),
        ) as i32;
        let customer_id = 1 + domain_sample(
            seed,
            "preflight/customer",
            0,
            u64::from(CUSTOMERS_PER_DISTRICT),
        ) as i32;
        let stock_threshold = STOCK_LEVEL_MIN_THRESHOLD
            + domain_sample(
                seed,
                "preflight/stock-level/threshold",
                0,
                STOCK_LEVEL_THRESHOLD_SPAN,
            ) as i32;
        let timestamp = format!(
            "PF{:016x}",
            domain_sample(seed, "preflight/new-order/timestamp", 0, u64::MAX)
        )
        .into_bytes();

        let mut selected_items = BTreeSet::new();
        let mut valid_lines = Vec::with_capacity(PREFLIGHT_VALID_LINES);
        for index in 0..PREFLIGHT_VALID_LINES {
            let item_id = select_unique_item(seed, index as u64, &selected_items);
            selected_items.insert(item_id);
            valid_lines.push(PreflightLine {
                number: (index + 1) as i32,
                item_id,
                quantity: 1 + domain_sample(seed, "preflight/new-order/quantity", index as u64, 10)
                    as i32,
            });
        }

        Ok(Self {
            warehouse_id,
            district_id,
            customer_id,
            stock_threshold,
            timestamp,
            valid_lines,
        })
    }

    fn invalid_line_number(&self) -> i32 {
        self.valid_lines.len() as i32 + 1
    }

    fn all_item_ids(&self) -> impl Iterator<Item = i32> + '_ {
        self.valid_lines
            .iter()
            .map(|line| line.item_id)
            .chain(std::iter::once(INVALID_ITEM_ID as i32))
    }
}

fn select_unique_item(seed: u64, ordinal: u64, selected: &BTreeSet<i32>) -> i32 {
    for attempt in 0..u64::from(ITEM_COUNT) {
        let sample_ordinal = ordinal
            .checked_mul(u64::from(ITEM_COUNT))
            .and_then(|base| base.checked_add(attempt))
            .expect("published item domain fits u64");
        let candidate = 1 + domain_sample(
            seed,
            "preflight/new-order/item",
            sample_ordinal,
            ITEM_COUNT as u64,
        ) as i32;
        if !selected.contains(&candidate) {
            return candidate;
        }
    }
    unreachable!("five selections cannot exhaust the published item domain")
}

/// Stateless and domain-separated selection.  This is intentionally local to
/// preflight so adding a probe cannot perturb the ranked routing sequence.
fn domain_sample(seed: u64, domain: &str, ordinal: u64, upper: u64) -> u64 {
    assert!(upper > 0, "preflight sample upper bound must be positive");
    let mut state = 0xcbf2_9ce4_8422_2325_u64;
    for byte in seed
        .to_be_bytes()
        .into_iter()
        .chain(domain.as_bytes().iter().copied())
        .chain(ordinal.to_be_bytes())
    {
        state ^= u64::from(byte);
        state = state.wrapping_mul(0x0000_0100_0000_01b3);
    }
    splitmix64(state) % upper
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

async fn verify_stock_level(
    client: &mut RmdbClient,
    selection: &PreflightSelection,
) -> Result<(), TpccError> {
    let begin = [
        operation(StatementId::Begin, []),
        operation(
            StatementId::StockLevelNextOrder,
            [
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(selection.district_id),
            ],
        ),
    ];
    let begin_results =
        execute_preflight_batch(client, "StockLevel begin/next-order", &begin).await?;
    let next_order_id =
        match parse_positive_scalar(&begin_results, 1, "StockLevel preflight d_next_o_id") {
            Ok(value) => value,
            Err(error) => return semantic_abort(client, error).await,
        };

    // This is the exact ranked aggregate path: one prepared
    // COUNT(DISTINCT ...) operation, with no client-side substitute.
    let aggregate = [operation(
        StatementId::StockLevelCount,
        [
            WireValue::Int32(selection.warehouse_id),
            WireValue::Int32(selection.district_id),
            WireValue::Int32(next_order_id),
            WireValue::Int32(selection.stock_threshold),
        ],
    )];
    debug_assert_eq!(aggregate.len(), 1);
    let aggregate_results =
        execute_preflight_batch(client, "StockLevel single COUNT(DISTINCT)", &aggregate).await?;
    let prepared_count =
        match parse_nonnegative_scalar(&aggregate_results, 0, "StockLevel prepared count") {
            Ok(value) => value,
            Err(error) => return semantic_abort(client, error).await,
        };

    let first_order_id = (next_order_id - STOCK_LEVEL_RECENT_ORDERS).max(1);
    let order_ids: Vec<i32> = (first_order_id..next_order_id).collect();
    let line_operations: Vec<_> = order_ids
        .iter()
        .map(|order_id| {
            operation(
                StatementId::OrderStatusLines,
                [
                    WireValue::Int32(selection.warehouse_id),
                    WireValue::Int32(selection.district_id),
                    WireValue::Int32(*order_id),
                ],
            )
        })
        .collect();
    let line_results = execute_preflight_batch(
        client,
        "StockLevel recent-order detail probes",
        &line_operations,
    )
    .await?;
    let distinct_items = match collect_distinct_line_items(&line_results, &order_ids) {
        Ok(items) => items,
        Err(error) => return semantic_abort(client, error).await,
    };

    let reconstructed_count = reconstruct_low_stock_count(
        client,
        selection.warehouse_id,
        selection.stock_threshold,
        &distinct_items,
    )
    .await?;

    abort_open_transaction(client, "StockLevel preflight read-only rollback").await?;
    if reconstructed_count != prepared_count {
        return Err(preflight_semantic(format!(
            "StockLevel prepared COUNT(DISTINCT) returned {prepared_count}, \
             independent prepared detail probes reconstructed {reconstructed_count}"
        )));
    }
    Ok(())
}

fn collect_distinct_line_items(
    results: &BatchResults,
    order_ids: &[i32],
) -> Result<BTreeSet<i32>, String> {
    if results.operation_count() != order_ids.len() {
        return Err(format!(
            "StockLevel detail batch reported {} operations, expected {}",
            results.operation_count(),
            order_ids.len()
        ));
    }
    let mut items = BTreeSet::new();
    for (operation_index, order_id) in order_ids.iter().enumerate() {
        let rows = results
            .rows(operation_index)
            .map_err(|error| error.to_string())?;
        for (row_index, row) in rows.iter().enumerate() {
            if row.len() != 6 {
                return Err(format!(
                    "StockLevel order {order_id} line {row_index} returned {} columns, expected 6",
                    row.len()
                ));
            }
            let item_id = row_int32(
                row,
                1,
                &format!("StockLevel order {order_id} line {row_index}"),
            )
            .map_err(|error| error.to_string())?;
            if !(1..=ITEM_COUNT as i32).contains(&item_id) {
                return Err(format!(
                    "StockLevel order {order_id} returned invalid item id {item_id}"
                ));
            }
            items.insert(item_id);
        }
    }
    Ok(items)
}

async fn reconstruct_low_stock_count(
    client: &mut RmdbClient,
    warehouse_id: i32,
    threshold: i32,
    distinct_items: &BTreeSet<i32>,
) -> Result<i32, TpccError> {
    let items: Vec<i32> = distinct_items.iter().copied().collect();
    let mut low_stock_count = 0_i32;
    for chunk in items.chunks(MAX_DETAIL_BATCH_OPERATIONS) {
        let operations: Vec<_> = chunk
            .iter()
            .map(|item_id| {
                operation(
                    StatementId::NewOrderStock,
                    [WireValue::Int32(warehouse_id), WireValue::Int32(*item_id)],
                )
            })
            .collect();
        let results =
            execute_preflight_batch(client, "StockLevel stock detail probes", &operations).await?;
        for (operation_index, item_id) in chunk.iter().enumerate() {
            let row = match results.rows(operation_index) {
                Ok(rows) => match exactly_one_row(
                    rows,
                    &format!("StockLevel stock ({warehouse_id}, {item_id})"),
                ) {
                    Ok(row) => row,
                    Err(error) => {
                        return abort_after_error(client, preflight_semantic(error)).await
                    }
                },
                Err(error) => {
                    return abort_after_error(client, preflight_semantic(error.to_string())).await
                }
            };
            let stock = match parse_stock_version(row, warehouse_id, *item_id) {
                Ok(stock) => stock,
                Err(error) => return abort_after_error(client, preflight_semantic(error)).await,
            };
            if stock.quantity < threshold {
                low_stock_count = low_stock_count
                    .checked_add(1)
                    .ok_or_else(|| preflight_protocol("StockLevel reconstructed count overflow"))?;
            }
        }
    }
    Ok(low_stock_count)
}

async fn verify_new_order_rollback(
    client: &mut RmdbClient,
    selection: &PreflightSelection,
) -> Result<(), TpccError> {
    let prospective_order_id = read_prospective_order_id(client, selection).await?;
    let before = read_rollback_snapshot(client, selection, prospective_order_id).await?;
    require_pristine_order_slot(&before, "before NewOrder rollback probe")?;

    let stage_one_plan = build_new_order_stage_one(selection);
    let stage_one_results = execute_preflight_batch(
        client,
        "NewOrder rollback stage one",
        &stage_one_plan.operations,
    )
    .await?;
    let materialized =
        match parse_new_order_stage_one(selection, &stage_one_plan, &stage_one_results) {
            Ok(materialized) => materialized,
            Err(error) => return semantic_abort(client, error).await,
        };
    if materialized.order_id != before.district_next_order_id {
        return semantic_abort(
            client,
            format!(
                "NewOrder rollback order id changed between untimed probes: before {}, stage one {}",
                before.district_next_order_id, materialized.order_id
            ),
        )
        .await;
    }

    let stage_two = build_new_order_abort_stage(selection, &materialized)?;
    execute_preflight_batch(
        client,
        "NewOrder valid write prefix followed by ABORT",
        &stage_two,
    )
    .await?;

    let after = read_rollback_snapshot(client, selection, materialized.order_id).await?;
    require_pristine_order_slot(&after, "after NewOrder rollback probe")?;
    if after != before {
        return Err(preflight_semantic(format!(
            "NewOrder ABORT left a visible change: before {before:?}, after {after:?}"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NewOrderStageOnePlan {
    operations: Vec<Operation>,
    home_result: usize,
    line_results: Vec<(usize, usize)>,
}

fn build_new_order_stage_one(selection: &PreflightSelection) -> NewOrderStageOnePlan {
    let mut operations = vec![operation(StatementId::Begin, [])];
    let home_result = operations.len();
    operations.push(new_order_home_operation(selection));

    let stock_keys: BTreeSet<_> = selection.all_item_ids().collect();
    for item_id in stock_keys {
        operations.push(operation(
            StatementId::NewOrderLockStock,
            [
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(item_id),
            ],
        ));
    }

    let mut line_results = Vec::with_capacity(PREFLIGHT_VALID_LINES + 1);
    for item_id in selection.all_item_ids() {
        let item_result = operations.len();
        operations.push(operation(
            StatementId::NewOrderItem,
            [WireValue::Int32(item_id)],
        ));
        let stock_result = operations.len();
        operations.push(operation(
            StatementId::NewOrderStock,
            [
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(item_id),
            ],
        ));
        line_results.push((item_result, stock_result));
    }

    NewOrderStageOnePlan {
        operations,
        home_result,
        line_results,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MaterializedLine {
    plan: PreflightLine,
    stock: StockVersion,
    amount_bits: u32,
    district_info: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MaterializedNewOrder {
    order_id: i32,
    lines: Vec<MaterializedLine>,
}

fn parse_new_order_stage_one(
    selection: &PreflightSelection,
    plan: &NewOrderStageOnePlan,
    results: &BatchResults,
) -> Result<MaterializedNewOrder, String> {
    let home = exactly_one_row(
        results
            .rows(plan.home_result)
            .map_err(|error| error.to_string())?,
        "NewOrder preflight home",
    )?;
    if home.len() != 6 {
        return Err(format!(
            "NewOrder preflight home returned {} columns, expected 6",
            home.len()
        ));
    }
    let order_id =
        row_int32(home, 4, "NewOrder preflight home").map_err(|error| error.to_string())?;
    if order_id <= 0 {
        return Err(format!(
            "NewOrder preflight d_next_o_id must be positive, got {order_id}"
        ));
    }

    let all_items: Vec<_> = selection.all_item_ids().collect();
    if plan.line_results.len() != all_items.len() {
        return Err("NewOrder preflight result map does not match its lines".to_owned());
    }
    let mut lines = Vec::with_capacity(selection.valid_lines.len());
    for (index, ((item_result, stock_result), item_id)) in
        plan.line_results.iter().zip(all_items.iter()).enumerate()
    {
        let item_rows = results
            .rows(*item_result)
            .map_err(|error| error.to_string())?;
        let stock_rows = results
            .rows(*stock_result)
            .map_err(|error| error.to_string())?;
        if index == selection.valid_lines.len() {
            if *item_id != INVALID_ITEM_ID as i32 {
                return Err("NewOrder preflight invalid item is not final".to_owned());
            }
            if !item_rows.is_empty() || !stock_rows.is_empty() {
                return Err(format!(
                    "NewOrder preflight invalid final item {item_id} unexpectedly resolved"
                ));
            }
            continue;
        }

        let item = exactly_one_row(item_rows, &format!("NewOrder preflight item {item_id}"))?;
        if item.len() != 3 {
            return Err(format!(
                "NewOrder preflight item {item_id} returned {} columns, expected 3",
                item.len()
            ));
        }
        let price_bits = row_f32_bits(item, 0, &format!("NewOrder preflight item {item_id}"))
            .map_err(|error| error.to_string())?;
        let price = f32::from_bits(price_bits);
        if !(1.0..=100.0).contains(&price) {
            return Err(format!(
                "NewOrder preflight item {item_id} price {price} is outside 1..=100"
            ));
        }

        let stock_row = exactly_one_row(
            stock_rows,
            &format!(
                "NewOrder preflight stock ({warehouse}, {item_id})",
                warehouse = selection.warehouse_id
            ),
        )?;
        let stock = parse_stock_version(stock_row, selection.warehouse_id, *item_id)?;
        if stock_row.len() != 15 {
            return Err(format!(
                "NewOrder preflight stock ({}, {item_id}) returned {} columns, expected 15",
                selection.warehouse_id,
                stock_row.len()
            ));
        }
        let district_info = row_char(
            stock_row,
            selection.district_id as usize + 4,
            &format!(
                "NewOrder preflight stock ({}, {item_id})",
                selection.warehouse_id
            ),
        )
        .map_err(|error| error.to_string())?
        .to_vec();
        if district_info.len() != 24 {
            return Err(format!(
                "NewOrder preflight stock ({}, {item_id}) district data has {} bytes, expected 24",
                selection.warehouse_id,
                district_info.len()
            ));
        }

        let plan = selection.valid_lines[index].clone();
        let amount = price * plan.quantity as f32;
        if !amount.is_finite() {
            return Err(format!(
                "NewOrder preflight line {} amount is non-finite",
                plan.number
            ));
        }
        lines.push(MaterializedLine {
            plan,
            stock,
            amount_bits: amount.to_bits(),
            district_info,
        });
    }

    Ok(MaterializedNewOrder { order_id, lines })
}

fn build_new_order_abort_stage(
    selection: &PreflightSelection,
    materialized: &MaterializedNewOrder,
) -> Result<Vec<Operation>, TpccError> {
    if materialized.lines.len() != PREFLIGHT_VALID_LINES {
        return Err(preflight_protocol(format!(
            "NewOrder preflight materialized {} valid lines, expected {PREFLIGHT_VALID_LINES}",
            materialized.lines.len()
        )));
    }

    let total_line_count = selection.invalid_line_number();
    let mut operations = vec![
        operation(
            StatementId::NewOrderAdvanceDistrict,
            [
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(selection.district_id),
            ],
        ),
        operation(
            StatementId::NewOrderInsertOrder,
            [
                WireValue::Int32(materialized.order_id),
                WireValue::Int32(selection.district_id),
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(selection.customer_id),
                WireValue::Char(selection.timestamp.clone()),
                WireValue::Int32(UNDELIVERED_CARRIER_ID),
                WireValue::Int32(total_line_count),
                WireValue::Int32(1),
            ],
        ),
        operation(
            StatementId::NewOrderInsertQueue,
            [
                WireValue::Int32(materialized.order_id),
                WireValue::Int32(selection.district_id),
                WireValue::Int32(selection.warehouse_id),
            ],
        ),
    ];

    for line in &materialized.lines {
        let normal_update = line.stock.quantity >= line.plan.quantity + 10;
        operations.push(operation(
            if normal_update {
                StatementId::NewOrderUpdateStockNormal
            } else {
                StatementId::NewOrderUpdateStockWrapped
            },
            [
                WireValue::Int32(line.plan.quantity),
                WireValue::Float32((line.plan.quantity as f32).to_bits()),
                WireValue::Int32(0),
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(line.plan.item_id),
            ],
        ));
        operations.push(operation(
            StatementId::NewOrderInsertLine,
            [
                WireValue::Int32(materialized.order_id),
                WireValue::Int32(selection.district_id),
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(line.plan.number),
                WireValue::Int32(line.plan.item_id),
                WireValue::Int32(selection.warehouse_id),
                WireValue::Char(UNDELIVERED_DATE.as_bytes().to_vec()),
                WireValue::Int32(line.plan.quantity),
                WireValue::Float32(line.amount_bits),
                WireValue::Char(line.district_info.clone()),
            ],
        ));
    }
    operations.push(operation(StatementId::Abort, []));
    Ok(operations)
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RollbackSnapshot {
    district_next_order_id: i32,
    stocks: BTreeMap<i32, StockVersion>,
    order_rows: Vec<Vec<WireValue>>,
    order_line_rows: Vec<Vec<WireValue>>,
    queue_rows: Vec<Vec<WireValue>>,
}

async fn read_rollback_snapshot(
    client: &mut RmdbClient,
    selection: &PreflightSelection,
    order_id: i32,
) -> Result<RollbackSnapshot, TpccError> {
    let mut operations = vec![operation(StatementId::Begin, [])];
    let home_result = operations.len();
    operations.push(new_order_home_operation(selection));

    let mut stock_results = Vec::with_capacity(selection.valid_lines.len());
    for line in &selection.valid_lines {
        stock_results.push((line.item_id, operations.len()));
        operations.push(operation(
            StatementId::NewOrderStock,
            [
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(line.item_id),
            ],
        ));
    }

    let order_result = operations.len();
    operations.push(operation(
        StatementId::OrderStatusOrder,
        order_key_parameters(selection, order_id),
    ));
    let line_result = operations.len();
    operations.push(operation(
        StatementId::OrderStatusLines,
        order_key_parameters(selection, order_id),
    ));
    let queue_result = operations.len();
    operations.push(operation(
        StatementId::DeliveryConfirmQueue,
        order_key_parameters(selection, order_id),
    ));
    operations.push(operation(StatementId::Abort, []));

    let results =
        execute_preflight_batch(client, "NewOrder rollback residue probes", &operations).await?;
    let district_next_order_id =
        parse_positive_scalar(&results, home_result, "NewOrder rollback d_next_o_id")
            .map_err(preflight_semantic)?;

    let mut stocks = BTreeMap::new();
    for (item_id, operation_index) in stock_results {
        let row = exactly_one_row(
            results
                .rows(operation_index)
                .map_err(|error| preflight_semantic(error.to_string()))?,
            &format!(
                "NewOrder rollback stock ({}, {item_id})",
                selection.warehouse_id
            ),
        )
        .map_err(preflight_semantic)?;
        let stock = parse_stock_version(row, selection.warehouse_id, item_id)
            .map_err(preflight_semantic)?;
        if stocks.insert(item_id, stock).is_some() {
            return Err(preflight_protocol(format!(
                "duplicate NewOrder preflight stock key {item_id}"
            )));
        }
    }

    Ok(RollbackSnapshot {
        district_next_order_id,
        stocks,
        order_rows: results
            .rows(order_result)
            .map_err(|error| preflight_semantic(error.to_string()))?
            .to_vec(),
        order_line_rows: results
            .rows(line_result)
            .map_err(|error| preflight_semantic(error.to_string()))?
            .to_vec(),
        queue_rows: results
            .rows(queue_result)
            .map_err(|error| preflight_semantic(error.to_string()))?
            .to_vec(),
    })
}

async fn read_prospective_order_id(
    client: &mut RmdbClient,
    selection: &PreflightSelection,
) -> Result<i32, TpccError> {
    let operations = [
        operation(StatementId::Begin, []),
        new_order_home_operation(selection),
        operation(StatementId::Abort, []),
    ];
    let results =
        execute_preflight_batch(client, "NewOrder rollback prospective order", &operations).await?;
    parse_positive_scalar(&results, 1, "NewOrder rollback d_next_o_id").map_err(preflight_semantic)
}

fn require_pristine_order_slot(
    snapshot: &RollbackSnapshot,
    context: &str,
) -> Result<(), TpccError> {
    if !snapshot.order_rows.is_empty()
        || !snapshot.order_line_rows.is_empty()
        || !snapshot.queue_rows.is_empty()
    {
        return Err(preflight_semantic(format!(
            "{context}: prospective order slot is not empty \
             (orders={}, order_line={}, new_orders={})",
            snapshot.order_rows.len(),
            snapshot.order_line_rows.len(),
            snapshot.queue_rows.len()
        )));
    }
    Ok(())
}

fn new_order_home_operation(selection: &PreflightSelection) -> Operation {
    operation(
        StatementId::NewOrderHome,
        [
            WireValue::Int32(selection.warehouse_id),
            WireValue::Int32(selection.district_id),
            WireValue::Int32(selection.customer_id),
        ],
    )
}

fn order_key_parameters(selection: &PreflightSelection, order_id: i32) -> Vec<WireValue> {
    vec![
        WireValue::Int32(selection.warehouse_id),
        WireValue::Int32(selection.district_id),
        WireValue::Int32(order_id),
    ]
}

fn parse_stock_version(
    row: &[WireValue],
    warehouse_id: i32,
    item_id: i32,
) -> Result<StockVersion, String> {
    if row.len() != 15 {
        return Err(format!(
            "stock ({warehouse_id}, {item_id}) returned {} columns, expected 15",
            row.len()
        ));
    }
    let context = format!("stock ({warehouse_id}, {item_id})");
    let quantity = row_int32(row, 0, &context).map_err(|error| error.to_string())?;
    let ytd_bits = row_f32_bits(row, 1, &context).map_err(|error| error.to_string())?;
    let order_count = row_int32(row, 2, &context).map_err(|error| error.to_string())?;
    let remote_count = row_int32(row, 3, &context).map_err(|error| error.to_string())?;
    if !(10..=100).contains(&quantity) {
        return Err(format!("{context} quantity {quantity} is outside 10..=100"));
    }
    if f32::from_bits(ytd_bits) < 0.0 {
        return Err(format!("{context} ytd is negative"));
    }
    if order_count < 0 || !(0..=order_count).contains(&remote_count) {
        return Err(format!(
            "{context} has invalid counters ({order_count}, {remote_count})"
        ));
    }
    Ok(StockVersion {
        quantity,
        ytd_bits,
        order_count,
        remote_count,
    })
}

fn exactly_one_row<'a>(
    rows: &'a [Vec<WireValue>],
    context: &str,
) -> Result<&'a [WireValue], String> {
    match rows {
        [row] => Ok(row),
        [] => Err(format!("{context} returned no rows, expected exactly one")),
        _ => Err(format!(
            "{context} returned {} rows, expected exactly one",
            rows.len()
        )),
    }
}

fn parse_positive_scalar(
    results: &BatchResults,
    operation_index: usize,
    context: &str,
) -> Result<i32, String> {
    let value = results
        .single_int32(operation_index)
        .map_err(|error| error.to_string())?;
    if value <= 0 {
        return Err(format!("{context} must be positive, got {value}"));
    }
    Ok(value)
}

fn parse_nonnegative_scalar(
    results: &BatchResults,
    operation_index: usize,
    context: &str,
) -> Result<i32, String> {
    let value = results
        .single_int32(operation_index)
        .map_err(|error| error.to_string())?;
    if value < 0 {
        return Err(format!("{context} must be non-negative, got {value}"));
    }
    Ok(value)
}

async fn execute_preflight_batch(
    client: &mut RmdbClient,
    stage: &str,
    operations: &[Operation],
) -> Result<BatchResults, TpccError> {
    if operations.is_empty() {
        return Err(preflight_protocol(format!(
            "{stage} attempted an empty EXEC_BATCH"
        )));
    }
    execute_batch(client, operations)
        .await
        .map_err(|error| preflight_semantic(format!("{stage} failed: {error}")))
}

async fn semantic_abort<T>(client: &mut RmdbClient, error: String) -> Result<T, TpccError> {
    match abort_open_transaction(client, "semantic preflight cleanup").await {
        Ok(()) => Err(preflight_semantic(error)),
        Err(cleanup) => Err(preflight_semantic(format!(
            "{error}; explicit ABORT cleanup also failed: {cleanup}"
        ))),
    }
}

async fn abort_after_error<T>(client: &mut RmdbClient, error: TpccError) -> Result<T, TpccError> {
    match abort_open_transaction(client, "semantic preflight cleanup").await {
        Ok(()) => Err(error),
        Err(cleanup) => Err(preflight_semantic(format!(
            "{error}; explicit ABORT cleanup also failed: {cleanup}"
        ))),
    }
}

async fn abort_open_transaction(client: &mut RmdbClient, stage: &str) -> Result<(), TpccError> {
    let operations = [operation(StatementId::Abort, [])];
    let results = execute_preflight_batch(client, stage, &operations).await?;
    if results.operation_count() != 1 {
        return Err(preflight_protocol(format!(
            "{stage} executed {} operations, expected 1",
            results.operation_count()
        )));
    }
    Ok(())
}

fn preflight_semantic(message: impl Into<String>) -> TpccError {
    TpccError::QueryError(format!(
        "ranked semantic preflight failed: {}",
        message.into()
    ))
}

fn preflight_protocol(message: impl Into<String>) -> TpccError {
    TpccError::Protocol(format!("ranked semantic preflight: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use crate::connection::prepared::{BatchQueryResult, BatchResponse};
    use crate::ranking::common::accept_batch;

    use super::*;

    fn test_selection() -> PreflightSelection {
        PreflightSelection::derive(0x0123_4567_89ab_cdef, 50).unwrap()
    }

    fn test_stock(quantity: i32, ytd: f32, order_count: i32, remote_count: i32) -> StockVersion {
        StockVersion {
            quantity,
            ytd_bits: ytd.to_bits(),
            order_count,
            remote_count,
        }
    }

    fn stock_row(stock: &StockVersion) -> Vec<WireValue> {
        let mut row = vec![
            WireValue::Int32(stock.quantity),
            WireValue::Float32(stock.ytd_bits),
            WireValue::Int32(stock.order_count),
            WireValue::Int32(stock.remote_count),
            WireValue::Char(vec![b'x'; 26]),
        ];
        row.extend((0..10).map(|_| WireValue::Char(vec![b'd'; 24])));
        row
    }

    #[test]
    fn selection_is_deterministic_domain_separated_and_in_range() {
        let left = test_selection();
        let right = test_selection();
        assert_eq!(left, right);
        assert!((1..=50).contains(&left.warehouse_id));
        assert!((1..=10).contains(&left.district_id));
        assert!((1..=3_000).contains(&left.customer_id));
        assert!((10..=20).contains(&left.stock_threshold));
        assert_eq!(left.valid_lines.len(), PREFLIGHT_VALID_LINES);
        assert_eq!(
            left.valid_lines
                .iter()
                .map(|line| line.item_id)
                .collect::<BTreeSet<_>>()
                .len(),
            PREFLIGHT_VALID_LINES
        );
        assert_ne!(
            domain_sample(7, "preflight/warehouse", 0, 1_000_000),
            domain_sample(7, "preflight/district", 0, 1_000_000)
        );
    }

    #[test]
    fn stock_level_formal_probe_is_one_count_distinct_operation() {
        let selection = test_selection();
        let aggregate = [operation(
            StatementId::StockLevelCount,
            [
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(selection.district_id),
                WireValue::Int32(3_001),
                WireValue::Int32(selection.stock_threshold),
            ],
        )];
        assert_eq!(aggregate.len(), 1);
        assert_eq!(
            aggregate[0].statement_id,
            StatementId::StockLevelCount.wire_id()
        );
    }

    #[test]
    fn detail_oracle_deduplicates_items_before_stock_comparison() {
        let operations = vec![
            operation(StatementId::OrderStatusLines, []),
            operation(StatementId::OrderStatusLines, []),
        ];
        let response = BatchResponse::Ok {
            executed_operations: 2,
            results: vec![
                BatchQueryResult {
                    operation_index: 0,
                    rows: vec![
                        vec![
                            WireValue::Int32(1),
                            WireValue::Int32(17),
                            WireValue::Int32(1),
                            WireValue::Int32(5),
                            WireValue::Float32(1.0_f32.to_bits()),
                            WireValue::Char(Vec::new()),
                        ],
                        vec![
                            WireValue::Int32(2),
                            WireValue::Int32(17),
                            WireValue::Int32(1),
                            WireValue::Int32(5),
                            WireValue::Float32(1.0_f32.to_bits()),
                            WireValue::Char(Vec::new()),
                        ],
                    ],
                },
                BatchQueryResult {
                    operation_index: 1,
                    rows: vec![vec![
                        WireValue::Int32(1),
                        WireValue::Int32(23),
                        WireValue::Int32(1),
                        WireValue::Int32(5),
                        WireValue::Float32(1.0_f32.to_bits()),
                        WireValue::Char(Vec::new()),
                    ]],
                },
            ],
        };
        let results = accept_batch(response, &operations).unwrap();
        let items = collect_distinct_line_items(&results, &[2_999, 3_000]).unwrap();
        assert_eq!(items, BTreeSet::from([17, 23]));
    }

    #[test]
    fn new_order_abort_batch_writes_every_valid_prefix_before_abort() {
        let selection = test_selection();
        let materialized = MaterializedNewOrder {
            order_id: 3_001,
            lines: selection
                .valid_lines
                .iter()
                .cloned()
                .map(|plan| MaterializedLine {
                    plan,
                    stock: test_stock(50, 0.0, 0, 0),
                    amount_bits: 10.0_f32.to_bits(),
                    district_info: vec![b'd'; 24],
                })
                .collect(),
        };
        let operations = build_new_order_abort_stage(&selection, &materialized).unwrap();
        let ids: Vec<_> = operations
            .iter()
            .map(|operation| operation.statement_id)
            .collect();
        assert_eq!(ids[0], StatementId::NewOrderAdvanceDistrict.wire_id());
        assert_eq!(ids[1], StatementId::NewOrderInsertOrder.wire_id());
        assert_eq!(ids[2], StatementId::NewOrderInsertQueue.wire_id());
        assert_eq!(
            ids.iter()
                .filter(|id| **id == StatementId::NewOrderInsertLine.wire_id())
                .count(),
            PREFLIGHT_VALID_LINES
        );
        assert_eq!(ids.last(), Some(&StatementId::Abort.wire_id()));
    }

    #[test]
    fn rollback_snapshot_comparison_covers_all_mutated_stock_fields_and_residue() {
        let selection = test_selection();
        let stocks = selection
            .valid_lines
            .iter()
            .map(|line| (line.item_id, test_stock(50, 0.0, 0, 0)))
            .collect();
        let before = RollbackSnapshot {
            district_next_order_id: 3_001,
            stocks,
            order_rows: Vec::new(),
            order_line_rows: Vec::new(),
            queue_rows: Vec::new(),
        };
        let mut leaked = before.clone();
        leaked.stocks.values_mut().next().unwrap().ytd_bits = 1.0_f32.to_bits();
        assert_ne!(before, leaked);

        let mut indexed_residue = before.clone();
        indexed_residue
            .order_rows
            .push(vec![WireValue::Int32(3_001)]);
        assert!(require_pristine_order_slot(&indexed_residue, "test").is_err());
    }

    #[test]
    fn stock_parser_requires_full_mutable_projection() {
        let stock = test_stock(42, 7.0, 9, 2);
        assert_eq!(
            parse_stock_version(&stock_row(&stock), 1, 2).unwrap(),
            stock
        );
        assert!(parse_stock_version(&stock_row(&stock)[..14], 1, 2).is_err());
    }
}
