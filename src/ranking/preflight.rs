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
use crate::connection::prepared::{BatchResponse, Operation};
use crate::connection::wire::WireValue;
use crate::error::TpccError;
use crate::profile::{DISTRICTS_PER_WAREHOUSE, ITEM_COUNT};
use crate::workload::{CUSTOMERS_PER_DISTRICT, INVALID_ITEM_ID};

use super::catalog::{StatementId, UNDELIVERED_CARRIER_ID, UNDELIVERED_DATE};
use super::common::{
    accept_batch, f32_add_bits, operation, row_char, row_f32_bits, row_int32, BatchExecutionError,
    BatchResults,
};
use super::runner::StockVersion;

const PREFLIGHT_VALID_LINES: usize = 5;
const STOCK_LEVEL_RECENT_ORDERS: i32 = 20;
const STOCK_LEVEL_MIN_THRESHOLD: i32 = 10;
const STOCK_LEVEL_THRESHOLD_SPAN: u64 = 11;
const MAX_DETAIL_BATCH_OPERATIONS: usize = 200;
const PAYMENT_PROBE_AMOUNT_BITS: u32 = 1.0_f32.to_bits();
const PAYMENT_PROBE_RESTORE_BITS: u32 = (-1.0_f32).to_bits();
const INITIAL_WAREHOUSE_YTD_BITS: u32 = 300_000.0_f32.to_bits();
const STALE_PAYMENT_QUERY_INDEX: u16 = 0;
const STALE_PAYMENT_UPDATE_INDEX: u16 = 1;
const STALE_PAYMENT_COMMIT_INDEX: u16 = 2;

/// Run the deterministic, non-measured semantic preflight on two already
/// configured and prepared ranked connections.
pub async fn run(
    primary: &mut RmdbClient,
    contender: &mut RmdbClient,
    seed: u64,
    warehouses: u16,
) -> Result<(), TpccError> {
    let selection = PreflightSelection::derive(seed, warehouses)?;
    verify_stock_level(primary, &selection).await?;
    verify_new_order_rollback(primary, &selection).await?;
    verify_new_order_auto_abort(primary, &selection).await?;
    // Reinsert the exact same prospective keys after AUTO_ABORT.  Read-only
    // residue checks cannot expose an invisible heap/index ghost that still
    // rejects a duplicate key; a second full write prefix does.
    verify_new_order_rollback(primary, &selection).await?;
    verify_payment_stale_write(primary, contender, selection.warehouse_id).await
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
    let before = read_new_order_state(client, selection, prospective_order_id).await?;
    require_pristine_order_slot(
        &before,
        prospective_order_id,
        "before NewOrder rollback probe",
    )?;

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

    let stage_two = build_new_order_write_stage(selection, &materialized)?;
    execute_preflight_batch(client, "NewOrder valid write prefix", &stage_two).await?;

    let visible = read_open_new_order_state(client, selection, materialized.order_id).await?;
    if let Err(error) = validate_visible_new_order_state(selection, &materialized, &visible) {
        return semantic_abort(client, error).await;
    }
    abort_open_transaction(client, "NewOrder explicit rollback").await?;

    let after = read_new_order_state(client, selection, materialized.order_id).await?;
    require_pristine_order_slot(
        &after,
        materialized.order_id,
        "after NewOrder rollback probe",
    )?;
    if after != before {
        return Err(preflight_semantic(format!(
            "NewOrder ABORT left a visible change: before {before:?}, after {after:?}"
        )));
    }
    Ok(())
}

async fn verify_new_order_auto_abort(
    client: &mut RmdbClient,
    selection: &PreflightSelection,
) -> Result<(), TpccError> {
    let prospective_order_id = read_prospective_order_id(client, selection).await?;
    let before = read_new_order_state(client, selection, prospective_order_id).await?;
    require_pristine_order_slot(
        &before,
        prospective_order_id,
        "before NewOrder AUTO_ABORT probe",
    )?;

    let stage_one_plan = build_new_order_stage_one(selection);
    let stage_one_results = execute_preflight_batch(
        client,
        "NewOrder AUTO_ABORT stage one",
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
                "NewOrder AUTO_ABORT order id changed between untimed probes: before {}, \
                 stage one {}",
                before.district_next_order_id, materialized.order_id
            ),
        )
        .await;
    }

    let write_prefix = build_new_order_write_stage(selection, &materialized)?;
    execute_preflight_batch(
        client,
        "NewOrder AUTO_ABORT valid write prefix",
        &write_prefix,
    )
    .await?;
    let visible = read_open_new_order_state(client, selection, materialized.order_id).await?;
    if let Err(error) = validate_visible_new_order_state(selection, &materialized, &visible) {
        return semantic_abort(client, error).await;
    }

    // The first operation deliberately produces a query result.  A failure
    // terminal cannot carry results in the typed BatchResponse, so accepting
    // the exact terminal below also proves that this partial result was
    // discarded.  The duplicate prospective PK is the final operation and
    // AUTO_ABORT must end the transaction; no explicit ABORT follows it.
    let failure_operations = [
        new_order_home_operation(selection),
        new_order_insert_order_operation(selection, &materialized),
    ];
    let response = client.exec_batch(&failure_operations).await?;
    validate_expected_auto_abort_error(response, failure_operations.len() - 1)?;

    // BEGIN on the very next prepared batch proves that the failed batch
    // automatically ended the transaction and left the session reusable.
    let after = read_new_order_state(client, selection, materialized.order_id).await?;
    require_pristine_order_slot(
        &after,
        materialized.order_id,
        "after NewOrder AUTO_ABORT probe",
    )?;
    if after != before {
        return Err(preflight_semantic(format!(
            "NewOrder AUTO_ABORT left a visible change: before {before:?}, after {after:?}"
        )));
    }
    Ok(())
}

fn validate_expected_auto_abort_error(
    response: BatchResponse,
    expected_failed_operation: usize,
) -> Result<(), TpccError> {
    let expected_failed_operation = u16::try_from(expected_failed_operation)
        .map_err(|_| preflight_protocol("AUTO_ABORT failed-operation index exceeds u16"))?;
    match response {
        BatchResponse::Error {
            executed_operations,
            failed_operation,
            ..
        } if executed_operations == expected_failed_operation
            && failed_operation == expected_failed_operation =>
        {
            Ok(())
        }
        BatchResponse::Error {
            executed_operations,
            failed_operation,
            diagnostic,
        } => Err(preflight_semantic(format!(
            "NewOrder AUTO_ABORT returned ERROR at ({executed_operations}, \
             {failed_operation}), expected ({expected_failed_operation}, \
             {expected_failed_operation}): {diagnostic}"
        ))),
        BatchResponse::TransactionAbort {
            executed_operations,
            failed_operation,
            diagnostic,
        } => Err(preflight_semantic(format!(
            "NewOrder duplicate primary key returned TRANSACTION_ABORT at \
             ({executed_operations}, {failed_operation}), expected ERROR at \
             ({expected_failed_operation}, {expected_failed_operation}): {diagnostic}"
        ))),
        BatchResponse::Ok {
            executed_operations,
            results,
        } => Err(preflight_semantic(format!(
            "NewOrder duplicate primary key unexpectedly succeeded after \
             {executed_operations} operations with {} query result(s)",
            results.len()
        ))),
        BatchResponse::TopLevelError { diagnostic } => Err(preflight_semantic(format!(
            "NewOrder duplicate primary key returned top-level ERROR instead of operation \
             ERROR: {diagnostic}"
        ))),
    }
}

async fn verify_payment_stale_write(
    primary: &mut RmdbClient,
    contender: &mut RmdbClient,
    warehouse_id: i32,
) -> Result<(), TpccError> {
    let original_bits =
        begin_payment_warehouse_snapshot(primary, warehouse_id, "Payment primary snapshot").await?;
    if original_bits != INITIAL_WAREHOUSE_YTD_BITS {
        return semantic_abort(
            primary,
            format!(
                "Payment warehouse {warehouse_id} initial w_ytd was {} (0x{original_bits:08x}), \
                 expected bit-exact binary32 300000 (0x{INITIAL_WAREHOUSE_YTD_BITS:08x})",
                f32::from_bits(original_bits)
            ),
        )
        .await;
    }
    let incremented_bits = match f32_add_bits(original_bits, PAYMENT_PROBE_AMOUNT_BITS) {
        Ok(bits) => bits,
        Err(error) => return semantic_abort(primary, error.to_string()).await,
    };
    let reversible_bits = match f32_add_bits(incremented_bits, PAYMENT_PROBE_RESTORE_BITS) {
        Ok(bits) => bits,
        Err(error) => return semantic_abort(primary, error.to_string()).await,
    };
    if reversible_bits != original_bits {
        return semantic_abort(
            primary,
            format!(
                "Payment warehouse {warehouse_id} probe is not binary32 reversible: \
                 0x{original_bits:08x} + 1.0 - 1.0 = 0x{reversible_bits:08x}"
            ),
        )
        .await;
    }

    let contender_bits =
        match begin_payment_warehouse_snapshot(contender, warehouse_id, "Payment stale snapshot")
            .await
        {
            Ok(bits) => bits,
            Err(error) => return abort_after_error(primary, error).await,
        };
    if contender_bits != original_bits {
        return semantic_abort_pair(
            primary,
            contender,
            format!(
                "Payment dual snapshots disagree for warehouse {warehouse_id}: \
                 primary 0x{original_bits:08x}, contender 0x{contender_bits:08x}"
            ),
        )
        .await;
    }

    if let Err(error) = update_open_payment_warehouse(
        primary,
        warehouse_id,
        PAYMENT_PROBE_AMOUNT_BITS,
        incremented_bits,
        "Payment primary +1.0",
    )
    .await
    {
        return abort_after_error(contender, error).await;
    }
    if let Err(error) = commit_open_transaction(primary, "Payment primary +1.0 COMMIT").await {
        return abort_after_error(contender, error).await;
    }

    // Operation zero would produce a stale-snapshot query result.  The typed
    // TRANSACTION_ABORT terminal carries no results, proving that it is
    // discarded whether the conflict surfaces at UPDATE or COMMIT.
    let stale_operations = [
        payment_warehouse_operation(warehouse_id),
        payment_update_warehouse_operation(PAYMENT_PROBE_AMOUNT_BITS, warehouse_id),
        operation(StatementId::Commit, []),
    ];
    debug_assert_eq!(STALE_PAYMENT_QUERY_INDEX, 0);
    let response = contender.exec_batch(&stale_operations).await?;
    validate_expected_stale_payment_abort(response)?;

    // AUTO_ABORT must have ended the stale transaction.  The very next
    // prepared batch on the same connection starts and completes normally.
    let reusable_bits = read_payment_warehouse_value(
        contender,
        warehouse_id,
        "Payment contender reuse after stale abort",
    )
    .await?;
    require_exact_f32(
        reusable_bits,
        incremented_bits,
        "Payment contender reuse observed committed +1.0",
    )?;

    let restore_before =
        begin_payment_warehouse_snapshot(primary, warehouse_id, "Payment restore snapshot").await?;
    if restore_before != incremented_bits {
        return semantic_abort(
            primary,
            exact_f32_mismatch(
                restore_before,
                incremented_bits,
                "Payment restore predecessor",
            ),
        )
        .await;
    }
    update_open_payment_warehouse(
        primary,
        warehouse_id,
        PAYMENT_PROBE_RESTORE_BITS,
        original_bits,
        "Payment primary -1.0 restore",
    )
    .await?;
    commit_open_transaction(primary, "Payment primary restore COMMIT").await?;

    let final_bits =
        read_payment_warehouse_value(contender, warehouse_id, "Payment final restored value")
            .await?;
    require_exact_f32(
        final_bits,
        original_bits,
        "Payment warehouse final 0-ULP restoration",
    )
}

async fn begin_payment_warehouse_snapshot(
    client: &mut RmdbClient,
    warehouse_id: i32,
    stage: &str,
) -> Result<u32, TpccError> {
    let operations = [
        operation(StatementId::Begin, []),
        payment_warehouse_operation(warehouse_id),
    ];
    let results = execute_preflight_batch(client, stage, &operations).await?;
    match parse_payment_warehouse_bits(&results, 1, stage) {
        Ok(bits) => Ok(bits),
        Err(error) => semantic_abort(client, error).await,
    }
}

async fn update_open_payment_warehouse(
    client: &mut RmdbClient,
    warehouse_id: i32,
    amount_bits: u32,
    expected_bits: u32,
    stage: &str,
) -> Result<(), TpccError> {
    let operations = [
        payment_update_warehouse_operation(amount_bits, warehouse_id),
        payment_warehouse_operation(warehouse_id),
    ];
    let results = execute_preflight_batch(client, stage, &operations).await?;
    let actual_bits = match parse_payment_warehouse_bits(&results, 1, stage) {
        Ok(bits) => bits,
        Err(error) => return semantic_abort(client, error).await,
    };
    if actual_bits != expected_bits {
        return semantic_abort(
            client,
            exact_f32_mismatch(actual_bits, expected_bits, stage),
        )
        .await;
    }
    Ok(())
}

async fn read_payment_warehouse_value(
    client: &mut RmdbClient,
    warehouse_id: i32,
    stage: &str,
) -> Result<u32, TpccError> {
    let operations = [
        operation(StatementId::Begin, []),
        payment_warehouse_operation(warehouse_id),
        operation(StatementId::Abort, []),
    ];
    let results = execute_preflight_batch(client, stage, &operations).await?;
    parse_payment_warehouse_bits(&results, 1, stage).map_err(preflight_semantic)
}

fn parse_payment_warehouse_bits(
    results: &BatchResults,
    operation_index: usize,
    context: &str,
) -> Result<u32, String> {
    let row = exactly_one_row(
        results
            .rows(operation_index)
            .map_err(|error| error.to_string())?,
        context,
    )?;
    if row.len() != 7 {
        return Err(format!(
            "{context} returned {} warehouse columns, expected 7",
            row.len()
        ));
    }
    row_f32_bits(row, 0, context).map_err(|error| error.to_string())
}

fn payment_warehouse_operation(warehouse_id: i32) -> Operation {
    operation(
        StatementId::PaymentWarehouse,
        [WireValue::Int32(warehouse_id)],
    )
}

fn payment_update_warehouse_operation(amount_bits: u32, warehouse_id: i32) -> Operation {
    operation(
        StatementId::PaymentUpdateWarehouse,
        [
            WireValue::Float32(amount_bits),
            WireValue::Int32(warehouse_id),
        ],
    )
}

fn validate_expected_stale_payment_abort(response: BatchResponse) -> Result<(), TpccError> {
    match response {
        BatchResponse::TransactionAbort {
            executed_operations,
            failed_operation,
            ..
        } if executed_operations == failed_operation
            && matches!(
                failed_operation,
                STALE_PAYMENT_UPDATE_INDEX | STALE_PAYMENT_COMMIT_INDEX
            ) =>
        {
            Ok(())
        }
        BatchResponse::TransactionAbort {
            executed_operations,
            failed_operation,
            diagnostic,
        } => Err(preflight_semantic(format!(
            "Payment stale write returned TRANSACTION_ABORT at \
             ({executed_operations}, {failed_operation}), expected conflict at UPDATE index \
             {STALE_PAYMENT_UPDATE_INDEX} or COMMIT index {STALE_PAYMENT_COMMIT_INDEX}: \
             {diagnostic}"
        ))),
        BatchResponse::Error {
            executed_operations,
            failed_operation,
            diagnostic,
        } => Err(preflight_semantic(format!(
            "Payment stale write returned ERROR at ({executed_operations}, \
             {failed_operation}), expected TRANSACTION_ABORT: {diagnostic}"
        ))),
        BatchResponse::Ok {
            executed_operations,
            results,
        } => Err(preflight_semantic(format!(
            "Payment stale write unexpectedly succeeded after {executed_operations} operations \
             with {} query result(s)",
            results.len()
        ))),
        BatchResponse::TopLevelError { diagnostic } => Err(preflight_semantic(format!(
            "Payment stale write returned top-level ERROR instead of TRANSACTION_ABORT: \
             {diagnostic}"
        ))),
    }
}

async fn commit_open_transaction(client: &mut RmdbClient, stage: &str) -> Result<(), TpccError> {
    let operations = [operation(StatementId::Commit, [])];
    let results = execute_preflight_batch(client, stage, &operations).await?;
    if results.operation_count() != 1 {
        return Err(preflight_protocol(format!(
            "{stage} executed {} operations, expected 1",
            results.operation_count()
        )));
    }
    Ok(())
}

async fn semantic_abort_pair<T>(
    primary: &mut RmdbClient,
    contender: &mut RmdbClient,
    error: String,
) -> Result<T, TpccError> {
    let contender_cleanup =
        abort_open_transaction(contender, "dual-session semantic preflight cleanup").await;
    let primary_cleanup =
        abort_open_transaction(primary, "dual-session semantic preflight cleanup").await;
    let mut failure = preflight_semantic(error);
    if let Err(cleanup) = primary_cleanup {
        failure = combine_cleanup_failure(
            failure,
            attach_error_context(cleanup, "primary-session cleanup"),
        );
    }
    if let Err(cleanup) = contender_cleanup {
        failure = combine_cleanup_failure(
            failure,
            attach_error_context(cleanup, "contender-session cleanup"),
        );
    }
    Err(failure)
}

fn require_exact_f32(actual_bits: u32, expected_bits: u32, context: &str) -> Result<(), TpccError> {
    if actual_bits == expected_bits {
        Ok(())
    } else {
        Err(preflight_semantic(exact_f32_mismatch(
            actual_bits,
            expected_bits,
            context,
        )))
    }
}

fn exact_f32_mismatch(actual_bits: u32, expected_bits: u32, context: &str) -> String {
    format!(
        "{context}: observed {} (0x{actual_bits:08x}), expected {} \
         (0x{expected_bits:08x}, 0 ULP tolerance)",
        f32::from_bits(actual_bits),
        f32::from_bits(expected_bits)
    )
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

fn build_new_order_write_stage(
    selection: &PreflightSelection,
    materialized: &MaterializedNewOrder,
) -> Result<Vec<Operation>, TpccError> {
    if materialized.lines.len() != PREFLIGHT_VALID_LINES {
        return Err(preflight_protocol(format!(
            "NewOrder preflight materialized {} valid lines, expected {PREFLIGHT_VALID_LINES}",
            materialized.lines.len()
        )));
    }

    let mut operations = vec![
        operation(
            StatementId::NewOrderAdvanceDistrict,
            [
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(selection.district_id),
            ],
        ),
        new_order_insert_order_operation(selection, materialized),
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
    Ok(operations)
}

fn new_order_insert_order_operation(
    selection: &PreflightSelection,
    materialized: &MaterializedNewOrder,
) -> Operation {
    operation(
        StatementId::NewOrderInsertOrder,
        [
            WireValue::Int32(materialized.order_id),
            WireValue::Int32(selection.district_id),
            WireValue::Int32(selection.warehouse_id),
            WireValue::Int32(selection.customer_id),
            WireValue::Char(selection.timestamp.clone()),
            WireValue::Int32(UNDELIVERED_CARRIER_ID),
            WireValue::Int32(selection.invalid_line_number()),
            WireValue::Int32(1),
        ],
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NewOrderState {
    district_next_order_id: i32,
    stocks: BTreeMap<i32, StockVersion>,
    order_rows: Vec<Vec<WireValue>>,
    delivery_order_rows: Vec<Vec<WireValue>>,
    latest_order_rows: Vec<Vec<WireValue>>,
    order_line_rows: Vec<Vec<WireValue>>,
    queue_rows: Vec<Vec<WireValue>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct NewOrderStateProbe {
    home_result: usize,
    stock_results: Vec<(i32, usize)>,
    order_result: usize,
    delivery_order_result: usize,
    latest_order_result: usize,
    line_result: usize,
    queue_result: usize,
}

fn append_new_order_state_probe(
    operations: &mut Vec<Operation>,
    selection: &PreflightSelection,
    order_id: i32,
) -> NewOrderStateProbe {
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
    let delivery_order_result = operations.len();
    operations.push(operation(
        StatementId::DeliveryOrder,
        order_key_parameters(selection, order_id),
    ));
    let latest_order_result = operations.len();
    operations.push(operation(
        StatementId::OrderStatusLatestOrder,
        [
            WireValue::Int32(selection.warehouse_id),
            WireValue::Int32(selection.district_id),
            WireValue::Int32(selection.customer_id),
        ],
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

    NewOrderStateProbe {
        home_result,
        stock_results,
        order_result,
        delivery_order_result,
        latest_order_result,
        line_result,
        queue_result,
    }
}

fn parse_new_order_state(
    results: &BatchResults,
    probe: &NewOrderStateProbe,
    selection: &PreflightSelection,
) -> Result<NewOrderState, String> {
    let district_next_order_id =
        parse_positive_scalar(results, probe.home_result, "NewOrder rollback d_next_o_id")?;

    let mut stocks = BTreeMap::new();
    for (item_id, operation_index) in &probe.stock_results {
        let row = exactly_one_row(
            results
                .rows(*operation_index)
                .map_err(|error| error.to_string())?,
            &format!(
                "NewOrder rollback stock ({}, {item_id})",
                selection.warehouse_id
            ),
        )?;
        let stock = parse_stock_version(row, selection.warehouse_id, *item_id)?;
        if stocks.insert(*item_id, stock).is_some() {
            return Err(format!("duplicate NewOrder preflight stock key {item_id}"));
        }
    }

    Ok(NewOrderState {
        district_next_order_id,
        stocks,
        order_rows: results
            .rows(probe.order_result)
            .map_err(|error| error.to_string())?
            .to_vec(),
        delivery_order_rows: results
            .rows(probe.delivery_order_result)
            .map_err(|error| error.to_string())?
            .to_vec(),
        latest_order_rows: results
            .rows(probe.latest_order_result)
            .map_err(|error| error.to_string())?
            .to_vec(),
        order_line_rows: results
            .rows(probe.line_result)
            .map_err(|error| error.to_string())?
            .to_vec(),
        queue_rows: results
            .rows(probe.queue_result)
            .map_err(|error| error.to_string())?
            .to_vec(),
    })
}

async fn read_new_order_state(
    client: &mut RmdbClient,
    selection: &PreflightSelection,
    order_id: i32,
) -> Result<NewOrderState, TpccError> {
    let mut operations = vec![operation(StatementId::Begin, [])];
    let probe = append_new_order_state_probe(&mut operations, selection, order_id);
    operations.push(operation(StatementId::Abort, []));

    let results =
        execute_preflight_batch(client, "NewOrder rollback residue probes", &operations).await?;
    parse_new_order_state(&results, &probe, selection).map_err(preflight_semantic)
}

async fn read_open_new_order_state(
    client: &mut RmdbClient,
    selection: &PreflightSelection,
    order_id: i32,
) -> Result<NewOrderState, TpccError> {
    let mut operations = Vec::new();
    let probe = append_new_order_state_probe(&mut operations, selection, order_id);
    let results =
        execute_preflight_batch(client, "NewOrder visible write probes", &operations).await?;
    match parse_new_order_state(&results, &probe, selection) {
        Ok(state) => Ok(state),
        Err(error) => semantic_abort(client, error).await,
    }
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
    snapshot: &NewOrderState,
    prospective_order_id: i32,
    context: &str,
) -> Result<(), TpccError> {
    if !snapshot.order_rows.is_empty()
        || !snapshot.delivery_order_rows.is_empty()
        || !snapshot.order_line_rows.is_empty()
        || !snapshot.queue_rows.is_empty()
    {
        return Err(preflight_semantic(format!(
            "{context}: prospective order slot is not empty \
             (orders={}, delivery_order={}, order_line={}, new_orders={})",
            snapshot.order_rows.len(),
            snapshot.delivery_order_rows.len(),
            snapshot.order_line_rows.len(),
            snapshot.queue_rows.len()
        )));
    }
    if snapshot.latest_order_rows.iter().any(
        |row| matches!(row.as_slice(), [WireValue::Int32(value)] if *value == prospective_order_id),
    ) {
        return Err(preflight_semantic(format!(
            "{context}: prospective order {prospective_order_id} is reachable through \
             OrderStatusLatestOrder"
        )));
    }
    Ok(())
}

fn validate_visible_new_order_state(
    selection: &PreflightSelection,
    materialized: &MaterializedNewOrder,
    state: &NewOrderState,
) -> Result<(), String> {
    let expected_next_order_id = materialized
        .order_id
        .checked_add(1)
        .ok_or_else(|| "NewOrder visible d_next_o_id overflow".to_owned())?;
    if state.district_next_order_id != expected_next_order_id {
        return Err(format!(
            "NewOrder write prefix did not advance district: observed {}, expected {}",
            state.district_next_order_id, expected_next_order_id
        ));
    }

    let expected_order_rows = vec![vec![
        WireValue::Int32(materialized.order_id),
        WireValue::Char(selection.timestamp.clone()),
        WireValue::Int32(UNDELIVERED_CARRIER_ID),
    ]];
    if state.order_rows != expected_order_rows {
        return Err(format!(
            "NewOrder write prefix exact order point probe mismatch: {:?}",
            state.order_rows
        ));
    }
    let expected_delivery_rows = vec![vec![WireValue::Int32(selection.customer_id)]];
    if state.delivery_order_rows != expected_delivery_rows {
        return Err(format!(
            "NewOrder write prefix heap/order lookup mismatch: {:?}",
            state.delivery_order_rows
        ));
    }
    let expected_latest_rows = vec![vec![WireValue::Int32(materialized.order_id)]];
    if state.latest_order_rows != expected_latest_rows {
        return Err(format!(
            "NewOrder write prefix secondary order index mismatch: {:?}",
            state.latest_order_rows
        ));
    }
    let expected_queue_rows = vec![vec![WireValue::Int32(materialized.order_id)]];
    if state.queue_rows != expected_queue_rows {
        return Err(format!(
            "NewOrder write prefix queue mismatch: {:?}",
            state.queue_rows
        ));
    }

    let expected_line_rows: Vec<_> = materialized
        .lines
        .iter()
        .map(|line| {
            vec![
                WireValue::Int32(line.plan.number),
                WireValue::Int32(line.plan.item_id),
                WireValue::Int32(selection.warehouse_id),
                WireValue::Int32(line.plan.quantity),
                WireValue::Float32(line.amount_bits),
                WireValue::Char(UNDELIVERED_DATE.as_bytes().to_vec()),
            ]
        })
        .collect();
    if state.order_line_rows != expected_line_rows {
        return Err(format!(
            "NewOrder write prefix line rows mismatch: {:?}",
            state.order_line_rows
        ));
    }

    let mut expected_stocks = BTreeMap::new();
    for line in &materialized.lines {
        let quantity = if line.stock.quantity >= line.plan.quantity + 10 {
            line.stock.quantity - line.plan.quantity
        } else {
            line.stock.quantity + 91 - line.plan.quantity
        };
        let ytd_bits = f32_add_bits(line.stock.ytd_bits, (line.plan.quantity as f32).to_bits())
            .map_err(|error| error.to_string())?;
        let order_count =
            line.stock.order_count.checked_add(1).ok_or_else(|| {
                format!("NewOrder stock {} order count overflow", line.plan.item_id)
            })?;
        expected_stocks.insert(
            line.plan.item_id,
            StockVersion {
                quantity,
                ytd_bits,
                order_count,
                remote_count: line.stock.remote_count,
            },
        );
    }
    if state.stocks != expected_stocks {
        return Err(format!(
            "NewOrder write prefix stock versions mismatch: observed {:?}, expected {:?}",
            state.stocks, expected_stocks
        ));
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
    let response = client.exec_batch(operations).await?;
    accept_batch(response, operations).map_err(|error| map_preflight_batch_error(stage, error))
}

fn map_preflight_batch_error(stage: &str, error: BatchExecutionError) -> TpccError {
    match error {
        error @ BatchExecutionError::RetryableAbort { .. } => {
            TpccError::Abort(format!("{stage} failed: {error}"))
        }
        BatchExecutionError::FatalProtocol(message) => {
            preflight_protocol(format!("{stage} failed: {message}"))
        }
        error @ (BatchExecutionError::FatalOperation { .. }
        | BatchExecutionError::FatalTopLevel { .. }) => {
            preflight_semantic(format!("{stage} failed: {error}"))
        }
    }
}

async fn semantic_abort<T>(client: &mut RmdbClient, error: String) -> Result<T, TpccError> {
    match abort_open_transaction(client, "semantic preflight cleanup").await {
        Ok(()) => Err(preflight_semantic(error)),
        Err(cleanup) => Err(combine_cleanup_failure(preflight_semantic(error), cleanup)),
    }
}

async fn abort_after_error<T>(client: &mut RmdbClient, error: TpccError) -> Result<T, TpccError> {
    match abort_open_transaction(client, "semantic preflight cleanup").await {
        Ok(()) => Err(error),
        Err(cleanup) => Err(combine_cleanup_failure(error, cleanup)),
    }
}

fn combine_cleanup_failure(original: TpccError, cleanup: TpccError) -> TpccError {
    let original_text = original.to_string();
    let cleanup_text = cleanup.to_string();
    if is_transport_or_protocol_error(&original) {
        attach_error_context(
            original,
            &format!("explicit ABORT cleanup also failed: {cleanup_text}"),
        )
    } else if is_transport_or_protocol_error(&cleanup) {
        attach_error_context(
            cleanup,
            &format!("while cleaning up prior failure: {original_text}"),
        )
    } else {
        attach_error_context(
            original,
            &format!("explicit ABORT cleanup also failed: {cleanup_text}"),
        )
    }
}

fn is_transport_or_protocol_error(error: &TpccError) -> bool {
    matches!(
        error,
        TpccError::Connection(_)
            | TpccError::ParseError(_)
            | TpccError::Protocol(_)
            | TpccError::Io(_)
            | TpccError::Timeout { .. }
    )
}

fn attach_error_context(error: TpccError, context: &str) -> TpccError {
    match error {
        TpccError::Connection(message) => TpccError::Connection(format!("{message}; {context}")),
        TpccError::Abort(message) => TpccError::Abort(format!("{message}; {context}")),
        TpccError::QueryError(message) => TpccError::QueryError(format!("{message}; {context}")),
        TpccError::ParseError(message) => TpccError::ParseError(format!("{message}; {context}")),
        TpccError::Protocol(message) => TpccError::Protocol(format!("{message}; {context}")),
        TpccError::Io(error) => TpccError::Io(std::io::Error::new(
            error.kind(),
            format!("{error}; {context}"),
        )),
        TpccError::Timeout {
            context: timeout_context,
        } => TpccError::Timeout {
            context: format!("{timeout_context}; {context}"),
        },
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
    fn new_order_write_batch_contains_every_valid_prefix_mutation() {
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
        let operations = build_new_order_write_stage(&selection, &materialized).unwrap();
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
        assert!(!ids.contains(&StatementId::Abort.wire_id()));
    }

    #[test]
    fn new_order_state_comparison_covers_both_order_indexes_and_all_mutable_fields() {
        let selection = test_selection();
        let stocks = selection
            .valid_lines
            .iter()
            .map(|line| (line.item_id, test_stock(50, 0.0, 0, 0)))
            .collect();
        let before = NewOrderState {
            district_next_order_id: 3_001,
            stocks,
            order_rows: Vec::new(),
            delivery_order_rows: Vec::new(),
            latest_order_rows: vec![vec![WireValue::Int32(3_000)]],
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
        assert!(require_pristine_order_slot(&indexed_residue, 3_001, "test").is_err());

        let mut secondary_residue = before.clone();
        secondary_residue.latest_order_rows = vec![vec![WireValue::Int32(3_001)]];
        assert!(require_pristine_order_slot(&secondary_residue, 3_001, "test").is_err());
    }

    #[test]
    fn visible_new_order_state_requires_exact_heap_index_line_queue_and_stock_values() {
        let selection = test_selection();
        let materialized = MaterializedNewOrder {
            order_id: 3_001,
            lines: selection
                .valid_lines
                .iter()
                .cloned()
                .map(|plan| MaterializedLine {
                    plan,
                    stock: test_stock(50, 2.0, 7, 3),
                    amount_bits: 10.0_f32.to_bits(),
                    district_info: vec![b'd'; 24],
                })
                .collect(),
        };
        let stocks = materialized
            .lines
            .iter()
            .map(|line| {
                (
                    line.plan.item_id,
                    StockVersion {
                        quantity: 50 - line.plan.quantity,
                        ytd_bits: (2.0_f32 + line.plan.quantity as f32).to_bits(),
                        order_count: 8,
                        remote_count: 3,
                    },
                )
            })
            .collect();
        let state = NewOrderState {
            district_next_order_id: 3_002,
            stocks,
            order_rows: vec![vec![
                WireValue::Int32(3_001),
                WireValue::Char(selection.timestamp.clone()),
                WireValue::Int32(UNDELIVERED_CARRIER_ID),
            ]],
            delivery_order_rows: vec![vec![WireValue::Int32(selection.customer_id)]],
            latest_order_rows: vec![vec![WireValue::Int32(3_001)]],
            order_line_rows: materialized
                .lines
                .iter()
                .map(|line| {
                    vec![
                        WireValue::Int32(line.plan.number),
                        WireValue::Int32(line.plan.item_id),
                        WireValue::Int32(selection.warehouse_id),
                        WireValue::Int32(line.plan.quantity),
                        WireValue::Float32(line.amount_bits),
                        WireValue::Char(Vec::new()),
                    ]
                })
                .collect(),
            queue_rows: vec![vec![WireValue::Int32(3_001)]],
        };
        assert!(validate_visible_new_order_state(&selection, &materialized, &state).is_ok());
        let mut wrong = state;
        wrong.stocks.values_mut().next().unwrap().remote_count += 1;
        assert!(validate_visible_new_order_state(&selection, &materialized, &wrong).is_err());
    }

    #[test]
    fn preflight_batch_error_mapping_preserves_abort_protocol_and_semantic_classes() {
        assert!(matches!(
            map_preflight_batch_error(
                "stage",
                BatchExecutionError::RetryableAbort {
                    executed_operations: 0,
                    failed_operation: 0,
                    diagnostic: "conflict".to_owned(),
                },
            ),
            TpccError::Abort(_)
        ));
        assert!(matches!(
            map_preflight_batch_error(
                "stage",
                BatchExecutionError::FatalProtocol("bad frame".to_owned()),
            ),
            TpccError::Protocol(_)
        ));
        assert!(matches!(
            map_preflight_batch_error(
                "stage",
                BatchExecutionError::FatalTopLevel {
                    diagnostic: "server error".to_owned(),
                },
            ),
            TpccError::QueryError(_)
        ));
    }

    #[test]
    fn cleanup_failure_preserves_transport_timeout_and_protocol_classes() {
        assert!(matches!(
            combine_cleanup_failure(
                TpccError::Connection("socket closed".to_owned()),
                TpccError::QueryError("ABORT failed".to_owned()),
            ),
            TpccError::Connection(_)
        ));
        assert!(matches!(
            combine_cleanup_failure(
                TpccError::QueryError("semantic mismatch".to_owned()),
                TpccError::Timeout {
                    context: "ABORT response".to_owned(),
                },
            ),
            TpccError::Timeout { .. }
        ));
        assert!(matches!(
            combine_cleanup_failure(
                TpccError::Protocol("bad batch".to_owned()),
                TpccError::Timeout {
                    context: "ABORT response".to_owned(),
                },
            ),
            TpccError::Protocol(_)
        ));
        assert!(matches!(
            combine_cleanup_failure(
                TpccError::QueryError("semantic mismatch".to_owned()),
                TpccError::Protocol("bad ABORT frame".to_owned()),
            ),
            TpccError::Protocol(_)
        ));
    }

    #[test]
    fn auto_abort_probe_accepts_only_exact_operation_error_terminal() {
        assert!(validate_expected_auto_abort_error(
            BatchResponse::Error {
                executed_operations: 1,
                failed_operation: 1,
                diagnostic: "duplicate key".to_owned(),
            },
            1,
        )
        .is_ok());
        assert!(validate_expected_auto_abort_error(
            BatchResponse::TransactionAbort {
                executed_operations: 1,
                failed_operation: 1,
                diagnostic: "wrong terminal".to_owned(),
            },
            1,
        )
        .is_err());
        assert!(validate_expected_auto_abort_error(
            BatchResponse::Error {
                executed_operations: 0,
                failed_operation: 0,
                diagnostic: "wrong index".to_owned(),
            },
            1,
        )
        .is_err());
        assert!(validate_expected_auto_abort_error(
            BatchResponse::Ok {
                executed_operations: 2,
                results: vec![BatchQueryResult {
                    operation_index: 0,
                    rows: Vec::new(),
                }],
            },
            1,
        )
        .is_err());
    }

    #[test]
    fn stale_payment_probe_accepts_only_update_or_commit_transaction_abort() {
        for failed_operation in [STALE_PAYMENT_UPDATE_INDEX, STALE_PAYMENT_COMMIT_INDEX] {
            assert!(
                validate_expected_stale_payment_abort(BatchResponse::TransactionAbort {
                    executed_operations: failed_operation,
                    failed_operation,
                    diagnostic: "write-write conflict".to_owned(),
                })
                .is_ok()
            );
        }
        assert!(
            validate_expected_stale_payment_abort(BatchResponse::TransactionAbort {
                executed_operations: STALE_PAYMENT_QUERY_INDEX,
                failed_operation: STALE_PAYMENT_QUERY_INDEX,
                diagnostic: "wrong operation".to_owned(),
            })
            .is_err()
        );
        assert!(validate_expected_stale_payment_abort(BatchResponse::Error {
            executed_operations: STALE_PAYMENT_UPDATE_INDEX,
            failed_operation: STALE_PAYMENT_UPDATE_INDEX,
            diagnostic: "wrong terminal".to_owned(),
        })
        .is_err());
        assert!(validate_expected_stale_payment_abort(BatchResponse::Ok {
            executed_operations: 3,
            results: Vec::new(),
        })
        .is_err());
    }

    #[test]
    fn payment_probe_uses_reversible_binary32_transition_and_full_projection() {
        let incremented =
            f32_add_bits(INITIAL_WAREHOUSE_YTD_BITS, PAYMENT_PROBE_AMOUNT_BITS).unwrap();
        assert_eq!(incremented, 300_001.0_f32.to_bits());
        assert_eq!(
            f32_add_bits(incremented, PAYMENT_PROBE_RESTORE_BITS).unwrap(),
            INITIAL_WAREHOUSE_YTD_BITS
        );

        let operations = [payment_warehouse_operation(1)];
        let mut row = vec![WireValue::Float32(INITIAL_WAREHOUSE_YTD_BITS)];
        row.extend((0..6).map(|_| WireValue::Char(Vec::new())));
        let results = accept_batch(
            BatchResponse::Ok {
                executed_operations: 1,
                results: vec![BatchQueryResult {
                    operation_index: 0,
                    rows: vec![row],
                }],
            },
            &operations,
        )
        .unwrap();
        assert_eq!(
            parse_payment_warehouse_bits(&results, 0, "test").unwrap(),
            INITIAL_WAREHOUSE_YTD_BITS
        );
        assert_eq!(StatementId::ALL.len(), 42);
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
