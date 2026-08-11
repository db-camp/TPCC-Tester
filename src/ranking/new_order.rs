//! Public final-2026 New-Order transaction runner.
//!
//! A successful transaction uses exactly two `EXEC_BATCH` round trips.  The
//! first starts the transaction, acquires stock rows in a deterministic order,
//! and reads every item and stock row.  The second performs only relative
//! writes and reaches either `COMMIT` or the specified invalid-item `ABORT`.

use std::collections::{BTreeMap, BTreeSet};

use crate::connection::client::RmdbClient;
use crate::connection::prepared::Operation;
use crate::connection::wire::WireValue;
use crate::profile::{TransactionKind, DISTRICTS_PER_WAREHOUSE, ITEM_COUNT, OFFICIAL_WAREHOUSES};
use crate::routing::RoutedTransaction;
use crate::workload::{
    NewOrderInput, CUSTOMERS_PER_DISTRICT, INVALID_ITEM_ID, MAX_ITEM_QUANTITY, MAX_ORDER_LINES,
    MIN_ITEM_QUANTITY, MIN_ORDER_LINES,
};

use super::catalog::{StatementId, UNDELIVERED_CARRIER_ID, UNDELIVERED_DATE};
use super::common::{
    operation, row_char, row_f32_bits, row_int32, BatchResults, SemanticResult, SemanticResultExt,
    SemanticViolation,
};
use super::runner::{
    execute_batch, semantic_or_abort, NewOrderEvidence, RankedCommit, RankedTransactionError,
    RankedTransactionOutcome, RecoveryNewOrderLineEvidence, StockVersion,
};

const MIN_STOCK_QUANTITY: i32 = 10;
const MAX_STOCK_QUANTITY: i32 = 100;
const MIN_ITEM_PRICE: f32 = 1.0;
const MAX_ITEM_PRICE: f32 = 100.0;

/// Execute one immutable New-Order selection.
///
/// `route`, `input`, and `timestamp` must be reused unchanged after a
/// retryable server abort.  No retry is performed here: the phase scheduler is
/// responsible for retrying only `RankedTransactionError::is_retryable_abort`.
pub async fn execute(
    client: &mut RmdbClient,
    route: &RoutedTransaction,
    input: &NewOrderInput,
    timestamp: &str,
) -> Result<RankedTransactionOutcome, RankedTransactionError> {
    let validated =
        validate_input(route, input, timestamp).map_err(RankedTransactionError::Semantic)?;
    let stage_one_plan = build_stage_one(&validated);

    let stage_one_results = execute_batch(client, &stage_one_plan.operations).await?;
    let materialized = semantic_or_abort(
        client,
        parse_stage_one(&validated, &stage_one_plan, &stage_one_results).require_explicit_abort(),
    )
    .await?;

    let stage_two = semantic_or_abort(
        client,
        build_stage_two(&validated, &materialized).require_explicit_abort(),
    )
    .await?;
    let stage_two_results = execute_batch(client, &stage_two.operations).await?;

    // The terminal is the final operation in this batch, so a mismatch is a
    // fatal post-terminal semantic failure. A stale writer must instead be
    // rejected by the server as TRANSACTION_ABORT while the batch executes.
    validate_stage_two_stock_readbacks(&stage_two_results, &stage_two)
        .map_err(RankedTransactionError::Semantic)?;

    if validated.expected_rollback {
        return Ok(RankedTransactionOutcome::ExpectedRollback);
    }

    Ok(RankedTransactionOutcome::Committed(RankedCommit::NewOrder(
        NewOrderEvidence {
            warehouse_id: validated.warehouse_id as u16,
            district_id: validated.district_id as u8,
            order_id: materialized.order_id,
            line_count: validated.lines.len() as u8,
            remote_line_count: stage_two.remote_line_count,
            stock_ytd_delta: stage_two.stock_ytd_delta,
            line_amount_bits: materialized
                .lines
                .iter()
                .map(|line| line.amount_bits)
                .collect(),
            entry_timestamp: validated.timestamp,
            recovery_lines: stage_two.recovery_lines,
        },
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LinePlan {
    number: u8,
    item_id: i32,
    supply_warehouse: i32,
    quantity: i32,
    invalid_item: bool,
}

impl LinePlan {
    fn stock_key(self) -> (i32, i32) {
        (self.supply_warehouse, self.item_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ValidatedInput {
    warehouse_id: i32,
    district_id: i32,
    customer_id: i32,
    timestamp: Vec<u8>,
    all_local: i32,
    expected_rollback: bool,
    lines: Vec<LinePlan>,
}

fn validate_input(
    route: &RoutedTransaction,
    input: &NewOrderInput,
    timestamp: &str,
) -> SemanticResult<ValidatedInput> {
    if route.kind != TransactionKind::NewOrder {
        return Err(SemanticViolation::new(format!(
            "New-Order runner received {:?} route",
            route.kind
        )));
    }
    if !(1..=OFFICIAL_WAREHOUSES).contains(&route.home_warehouse) {
        return Err(SemanticViolation::new(format!(
            "New-Order warehouse {} is outside 1..={OFFICIAL_WAREHOUSES}",
            route.home_warehouse
        )));
    }
    if !(1..=DISTRICTS_PER_WAREHOUSE).contains(&route.home_district) {
        return Err(SemanticViolation::new(format!(
            "New-Order district {} is outside 1..={DISTRICTS_PER_WAREHOUSE}",
            route.home_district
        )));
    }
    if !(1..=CUSTOMERS_PER_DISTRICT).contains(&input.customer_id()) {
        return Err(SemanticViolation::new(format!(
            "New-Order customer {} is outside 1..={CUSTOMERS_PER_DISTRICT}",
            input.customer_id()
        )));
    }
    if timestamp.is_empty() || timestamp.len() > 30 || timestamp.as_bytes().contains(&0) {
        return Err(SemanticViolation::new(
            "New-Order timestamp must be a nonempty, NUL-free CHAR(30) value",
        ));
    }

    let line_count = input.lines().len();
    if !(usize::from(MIN_ORDER_LINES)..=usize::from(MAX_ORDER_LINES)).contains(&line_count) {
        return Err(SemanticViolation::new(format!(
            "New-Order line count {line_count} is outside {MIN_ORDER_LINES}..={MAX_ORDER_LINES}"
        )));
    }

    let mut lines = Vec::with_capacity(line_count);
    let mut invalid_positions = Vec::new();
    for (index, line) in input.lines().iter().enumerate() {
        let expected_number = (index + 1) as u8;
        if line.number() != expected_number {
            return Err(SemanticViolation::new(format!(
                "New-Order line {} has number {}, expected {expected_number}",
                index + 1,
                line.number()
            )));
        }
        if !(1..=OFFICIAL_WAREHOUSES).contains(&line.supply_warehouse()) {
            return Err(SemanticViolation::new(format!(
                "New-Order line {} supply warehouse {} is outside 1..={OFFICIAL_WAREHOUSES}",
                line.number(),
                line.supply_warehouse()
            )));
        }
        if !(MIN_ITEM_QUANTITY..=MAX_ITEM_QUANTITY).contains(&line.quantity()) {
            return Err(SemanticViolation::new(format!(
                "New-Order line {} quantity {} is outside {MIN_ITEM_QUANTITY}..={MAX_ITEM_QUANTITY}",
                line.number(),
                line.quantity()
            )));
        }

        let invalid_item = line.is_invalid_item();
        if invalid_item {
            invalid_positions.push(index);
        } else if line.item_id() == 0 || line.item_id() > ITEM_COUNT {
            return Err(SemanticViolation::new(format!(
                "New-Order line {} item {} is outside 1..={ITEM_COUNT}",
                line.number(),
                line.item_id()
            )));
        }

        lines.push(LinePlan {
            number: line.number(),
            item_id: i32::try_from(line.item_id())
                .map_err(|_| SemanticViolation::new("New-Order item id does not fit INT32"))?,
            supply_warehouse: i32::from(line.supply_warehouse()),
            quantity: i32::from(line.quantity()),
            invalid_item,
        });
    }

    let expected_invalid_positions = if input.expected_rollback() {
        vec![line_count - 1]
    } else {
        Vec::new()
    };
    if invalid_positions != expected_invalid_positions {
        return Err(SemanticViolation::new(format!(
            "New-Order invalid item positions {invalid_positions:?} do not match expected \
             rollback {}",
            input.expected_rollback()
        )));
    }
    if input.expected_rollback()
        && lines
            .last()
            .is_none_or(|line| line.item_id != INVALID_ITEM_ID as i32)
    {
        return Err(SemanticViolation::new(
            "New-Order expected rollback item must be the final invalid item",
        ));
    }

    let computed_all_local = lines
        .iter()
        .all(|line| line.supply_warehouse == i32::from(route.home_warehouse));
    if input.all_local() != computed_all_local {
        return Err(SemanticViolation::new(format!(
            "New-Order all_local={} does not match line routing ({computed_all_local})",
            input.all_local()
        )));
    }

    Ok(ValidatedInput {
        warehouse_id: i32::from(route.home_warehouse),
        district_id: i32::from(route.home_district),
        customer_id: i32::from(input.customer_id()),
        timestamp: timestamp.as_bytes().to_vec(),
        all_local: i32::from(computed_all_local),
        expected_rollback: input.expected_rollback(),
        lines,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct LineQueryIndices {
    item: usize,
    stock: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StageOnePlan {
    operations: Vec<Operation>,
    home_result: usize,
    line_results: Vec<LineQueryIndices>,
}

fn build_stage_one(input: &ValidatedInput) -> StageOnePlan {
    let mut operations = vec![operation(StatementId::Begin, [])];
    let home_result = operations.len();
    operations.push(operation(
        StatementId::NewOrderHome,
        [
            WireValue::Int32(input.warehouse_id),
            WireValue::Int32(input.district_id),
            WireValue::Int32(input.customer_id),
        ],
    ));

    let stock_keys: BTreeSet<_> = input.lines.iter().map(|line| line.stock_key()).collect();
    for (warehouse_id, item_id) in stock_keys {
        operations.push(operation(
            StatementId::NewOrderLockStock,
            [WireValue::Int32(warehouse_id), WireValue::Int32(item_id)],
        ));
    }

    let mut line_results = Vec::with_capacity(input.lines.len());
    for line in &input.lines {
        let item = operations.len();
        operations.push(operation(
            StatementId::NewOrderItem,
            [WireValue::Int32(line.item_id)],
        ));
        let stock = operations.len();
        operations.push(operation(
            StatementId::NewOrderStock,
            [
                WireValue::Int32(line.supply_warehouse),
                WireValue::Int32(line.item_id),
            ],
        ));
        line_results.push(LineQueryIndices { item, stock });
    }

    StageOnePlan {
        operations,
        home_result,
        line_results,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MaterializedLine {
    plan: LinePlan,
    initial_stock: StockVersion,
    amount_bits: u32,
    district_info: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MaterializedOrder {
    order_id: i32,
    lines: Vec<MaterializedLine>,
}

fn parse_stage_one(
    input: &ValidatedInput,
    plan: &StageOnePlan,
    results: &BatchResults,
) -> SemanticResult<MaterializedOrder> {
    if plan.line_results.len() != input.lines.len() {
        return Err(SemanticViolation::new(
            "New-Order stage-one result map does not match input lines",
        ));
    }

    let home = results.single_row(plan.home_result)?;
    require_columns(home, 6, "New-Order home")?;
    let discount_bits = row_f32_bits(home, 0, "New-Order home")?;
    require_f32_range(discount_bits, 0.0, 0.5, "customer.c_discount")?;
    require_char_range(
        row_char(home, 1, "New-Order home")?,
        1,
        16,
        "customer.c_last",
    )?;
    let credit = row_char(home, 2, "New-Order home")?;
    if credit != b"GC" && credit != b"BC" {
        return Err(SemanticViolation::new(format!(
            "customer.c_credit must be GC or BC, got {:?}",
            String::from_utf8_lossy(credit)
        )));
    }
    let warehouse_tax_bits = row_f32_bits(home, 3, "New-Order home")?;
    require_f32_range(warehouse_tax_bits, 0.0, 0.2, "warehouse.w_tax")?;
    let order_id = row_int32(home, 4, "New-Order home")?;
    if order_id <= 0 {
        return Err(SemanticViolation::new(format!(
            "district.d_next_o_id must be positive, got {order_id}"
        )));
    }
    let district_tax_bits = row_f32_bits(home, 5, "New-Order home")?;
    require_f32_range(district_tax_bits, 0.0, 0.2, "district.d_tax")?;

    let mut item_prices = BTreeMap::<i32, u32>::new();
    let mut stock_snapshots = BTreeMap::<(i32, i32), (StockVersion, Vec<u8>)>::new();
    let mut lines = Vec::with_capacity(input.lines.len());

    for (line, indices) in input.lines.iter().zip(&plan.line_results) {
        let item_rows = results.rows(indices.item)?;
        let stock_rows = results.rows(indices.stock)?;
        if line.invalid_item {
            if !item_rows.is_empty() || !stock_rows.is_empty() {
                return Err(SemanticViolation::new(format!(
                    "invalid final item {} unexpectedly resolved to item or stock rows",
                    line.item_id
                )));
            }
            continue;
        }

        let item = exactly_one_row(item_rows, &format!("New-Order item {}", line.item_id))?;
        require_columns(item, 3, &format!("New-Order item {}", line.item_id))?;
        let price_bits = row_f32_bits(item, 0, &format!("New-Order item {}", line.item_id))?;
        require_f32_range(
            price_bits,
            MIN_ITEM_PRICE,
            MAX_ITEM_PRICE,
            &format!("item {} i_price", line.item_id),
        )?;
        require_char_range(
            row_char(item, 1, &format!("New-Order item {}", line.item_id))?,
            14,
            24,
            &format!("item {} i_name", line.item_id),
        )?;
        require_char_range(
            row_char(item, 2, &format!("New-Order item {}", line.item_id))?,
            26,
            50,
            &format!("item {} i_data", line.item_id),
        )?;
        if let Some(previous) = item_prices.insert(line.item_id, price_bits) {
            if previous != price_bits {
                return Err(SemanticViolation::new(format!(
                    "item {} returned inconsistent prices inside one snapshot",
                    line.item_id
                )));
            }
        }

        let stock = exactly_one_row(
            stock_rows,
            &format!(
                "New-Order stock ({}, {})",
                line.supply_warehouse, line.item_id
            ),
        )?;
        require_columns(
            stock,
            15,
            &format!(
                "New-Order stock ({}, {})",
                line.supply_warehouse, line.item_id
            ),
        )?;
        let stock_quantity = row_int32(
            stock,
            0,
            &format!(
                "New-Order stock ({}, {})",
                line.supply_warehouse, line.item_id
            ),
        )?;
        if !(MIN_STOCK_QUANTITY..=MAX_STOCK_QUANTITY).contains(&stock_quantity) {
            return Err(SemanticViolation::new(format!(
                "stock ({}, {}) quantity {stock_quantity} is outside \
                 {MIN_STOCK_QUANTITY}..={MAX_STOCK_QUANTITY}",
                line.supply_warehouse, line.item_id
            )));
        }
        let stock_ytd_bits = row_f32_bits(
            stock,
            1,
            &format!(
                "New-Order stock ({}, {})",
                line.supply_warehouse, line.item_id
            ),
        )?;
        let stock_ytd = f32::from_bits(stock_ytd_bits);
        if !stock_ytd.is_finite() || stock_ytd < 0.0 {
            return Err(SemanticViolation::new(format!(
                "stock ({}, {}) s_ytd must be finite and non-negative",
                line.supply_warehouse, line.item_id
            )));
        }
        let stock_order_count = row_int32(
            stock,
            2,
            &format!(
                "New-Order stock ({}, {})",
                line.supply_warehouse, line.item_id
            ),
        )?;
        let stock_remote_count = row_int32(
            stock,
            3,
            &format!(
                "New-Order stock ({}, {})",
                line.supply_warehouse, line.item_id
            ),
        )?;
        if stock_order_count < 0 || stock_remote_count < 0 || stock_remote_count > stock_order_count
        {
            return Err(SemanticViolation::new(format!(
                "stock ({}, {}) has invalid counters ({stock_order_count},{stock_remote_count})",
                line.supply_warehouse, line.item_id
            )));
        }
        let stock_version = StockVersion {
            quantity: stock_quantity,
            ytd_bits: stock_ytd_bits,
            order_count: stock_order_count,
            remote_count: stock_remote_count,
        };
        require_char_range(
            row_char(
                stock,
                4,
                &format!(
                    "New-Order stock ({}, {})",
                    line.supply_warehouse, line.item_id
                ),
            )?,
            26,
            50,
            "stock.s_data",
        )?;
        for column in 5..15 {
            require_char_range(
                row_char(
                    stock,
                    column,
                    &format!(
                        "New-Order stock ({}, {})",
                        line.supply_warehouse, line.item_id
                    ),
                )?,
                24,
                24,
                &format!("stock.s_dist_{:02}", column - 4),
            )?;
        }
        let district_info = row_char(
            stock,
            input.district_id as usize + 4,
            &format!(
                "New-Order stock ({}, {})",
                line.supply_warehouse, line.item_id
            ),
        )?
        .to_vec();
        require_char_range(&district_info, 24, 24, "stock district information")?;

        let key = line.stock_key();
        if let Some((previous_version, previous_info)) =
            stock_snapshots.insert(key, (stock_version.clone(), district_info.clone()))
        {
            if previous_version != stock_version || previous_info != district_info {
                return Err(SemanticViolation::new(format!(
                    "stock ({}, {}) returned inconsistent rows inside one snapshot",
                    line.supply_warehouse, line.item_id
                )));
            }
        }

        lines.push(MaterializedLine {
            plan: *line,
            initial_stock: stock_version,
            amount_bits: multiply_f32_bits(price_bits, line.quantity)?,
            district_info,
        });
    }

    let expected_materialized = input.lines.len() - usize::from(input.expected_rollback);
    if lines.len() != expected_materialized {
        return Err(SemanticViolation::new(format!(
            "New-Order materialized {} valid lines, expected {expected_materialized}",
            lines.len()
        )));
    }

    Ok(MaterializedOrder { order_id, lines })
}

fn exactly_one_row<'a>(
    rows: &'a [Vec<WireValue>],
    context: &str,
) -> SemanticResult<&'a [WireValue]> {
    match rows {
        [row] => Ok(row),
        [] => Err(SemanticViolation::new(format!(
            "{context} returned no rows; expected exactly one"
        ))),
        _ => Err(SemanticViolation::new(format!(
            "{context} returned {} rows; expected exactly one",
            rows.len()
        ))),
    }
}

fn require_columns(row: &[WireValue], expected: usize, context: &str) -> SemanticResult<()> {
    if row.len() != expected {
        return Err(SemanticViolation::new(format!(
            "{context} returned {} columns; expected {expected}",
            row.len()
        )));
    }
    Ok(())
}

fn require_char_range(
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

fn require_f32_range(bits: u32, minimum: f32, maximum: f32, context: &str) -> SemanticResult<()> {
    let value = f32::from_bits(bits);
    if !(minimum..=maximum).contains(&value) {
        return Err(SemanticViolation::new(format!(
            "{context} {value} is outside {minimum}..={maximum}"
        )));
    }
    Ok(())
}

/// Multiply already-bound binary32 operands with exactly one binary32
/// round-to-nearest-even operation.
fn multiply_f32_bits(price_bits: u32, quantity: i32) -> SemanticResult<u32> {
    let price = f32::from_bits(price_bits);
    if !price.is_finite() {
        return Err(SemanticViolation::new(format!(
            "item price must be finite, got bits 0x{price_bits:08x}"
        )));
    }
    let quantity = quantity as f32;
    let amount = price * quantity;
    if !amount.is_finite() {
        return Err(SemanticViolation::new(
            "order-line amount multiplication produced a non-finite FLOAT32",
        ));
    }
    Ok(amount.to_bits())
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StageTwoPlan {
    operations: Vec<Operation>,
    stock_after_results: Vec<usize>,
    remote_line_count: u8,
    stock_ytd_delta: u32,
    recovery_lines: Vec<RecoveryNewOrderLineEvidence>,
}

fn build_stage_two(
    input: &ValidatedInput,
    materialized: &MaterializedOrder,
) -> SemanticResult<StageTwoPlan> {
    let total_line_count = i32::try_from(input.lines.len())
        .map_err(|_| SemanticViolation::new("New-Order line count does not fit INT32"))?;
    let mut operations = vec![
        operation(
            StatementId::NewOrderAdvanceDistrict,
            [
                WireValue::Int32(input.warehouse_id),
                WireValue::Int32(input.district_id),
            ],
        ),
        operation(
            StatementId::NewOrderInsertOrder,
            [
                WireValue::Int32(materialized.order_id),
                WireValue::Int32(input.district_id),
                WireValue::Int32(input.warehouse_id),
                WireValue::Int32(input.customer_id),
                WireValue::Char(input.timestamp.clone()),
                WireValue::Int32(UNDELIVERED_CARRIER_ID),
                WireValue::Int32(total_line_count),
                WireValue::Int32(input.all_local),
            ],
        ),
        operation(
            StatementId::NewOrderInsertQueue,
            [
                WireValue::Int32(materialized.order_id),
                WireValue::Int32(input.district_id),
                WireValue::Int32(input.warehouse_id),
            ],
        ),
    ];

    let mut current_stocks = BTreeMap::<(i32, i32), StockVersion>::new();
    let mut remote_line_count = 0_u8;
    let mut stock_ytd_delta = 0_u32;
    let mut stock_after_results = Vec::with_capacity(materialized.lines.len());
    let mut recovery_lines = Vec::with_capacity(materialized.lines.len());
    for line in &materialized.lines {
        let key = line.plan.stock_key();
        let current = current_stocks
            .entry(key)
            .or_insert_with(|| line.initial_stock.clone());
        let stock_before = current.clone();
        let normal_update = current.quantity >= line.plan.quantity + 10;
        current.quantity = if normal_update {
            current.quantity - line.plan.quantity
        } else {
            current.quantity - line.plan.quantity + 91
        };
        if !(MIN_STOCK_QUANTITY..=MAX_STOCK_QUANTITY).contains(&current.quantity) {
            return Err(SemanticViolation::new(format!(
                "stock ({}, {}) relative update produced out-of-range quantity {}",
                line.plan.supply_warehouse, line.plan.item_id, current.quantity
            )));
        }

        let remote = i32::from(line.plan.supply_warehouse != input.warehouse_id);
        let stock_ytd = f32::from_bits(current.ytd_bits);
        current.ytd_bits = (stock_ytd + line.plan.quantity as f32).to_bits();
        current.order_count = current
            .order_count
            .checked_add(1)
            .ok_or_else(|| SemanticViolation::new("New-Order stock order count overflow"))?;
        current.remote_count = current
            .remote_count
            .checked_add(remote)
            .ok_or_else(|| SemanticViolation::new("New-Order stock remote count overflow"))?;
        let stock_after = current.clone();
        remote_line_count = remote_line_count
            .checked_add(remote as u8)
            .ok_or_else(|| SemanticViolation::new("New-Order remote line count overflow"))?;
        stock_ytd_delta = stock_ytd_delta
            .checked_add(line.plan.quantity as u32)
            .ok_or_else(|| SemanticViolation::new("New-Order stock YTD delta overflow"))?;

        operations.push(operation(
            if normal_update {
                StatementId::NewOrderUpdateStockNormal
            } else {
                StatementId::NewOrderUpdateStockWrapped
            },
            [
                WireValue::Int32(line.plan.quantity),
                WireValue::Float32((line.plan.quantity as f32).to_bits()),
                WireValue::Int32(remote),
                WireValue::Int32(line.plan.supply_warehouse),
                WireValue::Int32(line.plan.item_id),
                WireValue::Int32(line.plan.quantity + 10),
            ],
        ));
        // Aligned with official appendix A §7: the second NewOrder dependency
        // stage is a pure write batch (update stocks, insert order_line rows,
        // commit/abort). No per-line stock read-back is issued, matching the
        // official SQL shape and avoiding N extra read ops per NewOrder.
        operations.push(operation(
            StatementId::NewOrderInsertLine,
            [
                WireValue::Int32(materialized.order_id),
                WireValue::Int32(input.district_id),
                WireValue::Int32(input.warehouse_id),
                WireValue::Int32(i32::from(line.plan.number)),
                WireValue::Int32(line.plan.item_id),
                WireValue::Int32(line.plan.supply_warehouse),
                WireValue::Char(UNDELIVERED_DATE.as_bytes().to_vec()),
                WireValue::Int32(line.plan.quantity),
                WireValue::Float32(line.amount_bits),
                WireValue::Char(line.district_info.clone()),
            ],
        ));
        recovery_lines.push(RecoveryNewOrderLineEvidence {
            number: line.plan.number,
            item_id: u32::try_from(line.plan.item_id)
                .map_err(|_| SemanticViolation::new("New-Order item id does not fit UINT32"))?,
            supply_warehouse: u16::try_from(line.plan.supply_warehouse).map_err(|_| {
                SemanticViolation::new("New-Order supply warehouse does not fit UINT16")
            })?,
            quantity: u8::try_from(line.plan.quantity)
                .map_err(|_| SemanticViolation::new("New-Order quantity does not fit UINT8"))?,
            amount_bits: line.amount_bits,
            district_info: line.district_info.clone(),
            stock_before,
            stock_after,
        });
    }

    operations.push(operation(
        if input.expected_rollback {
            StatementId::Abort
        } else {
            StatementId::Commit
        },
        [],
    ));

    Ok(StageTwoPlan {
        operations,
        stock_after_results,
        remote_line_count,
        stock_ytd_delta,
        recovery_lines,
    })
}

fn validate_stage_two_stock_readbacks(
    results: &BatchResults,
    plan: &StageTwoPlan,
) -> SemanticResult<()> {
    // No per-line stock read-back is issued (official stage-2 is a pure write
    // batch), so there is nothing to verify when the readback map is empty.
    if plan.stock_after_results.is_empty() {
        return Ok(());
    }
    if plan.stock_after_results.len() != plan.recovery_lines.len() {
        return Err(SemanticViolation::new(
            "New-Order stock readback map differs from recovery line count",
        ));
    }

    for (operation_index, line) in plan
        .stock_after_results
        .iter()
        .copied()
        .zip(&plan.recovery_lines)
    {
        let context = format!(
            "New-Order stock readback ({}, {}) after line {}",
            line.supply_warehouse, line.item_id, line.number
        );
        let row = results.single_row(operation_index)?;
        require_columns(row, 15, &context)?;
        let actual = StockVersion {
            quantity: row_int32(row, 0, &context)?,
            ytd_bits: row_f32_bits(row, 1, &context)?,
            order_count: row_int32(row, 2, &context)?,
            remote_count: row_int32(row, 3, &context)?,
        };
        if actual != line.stock_after {
            return Err(SemanticViolation::new(format!(
                "{context} was {actual:?}, expected exact relative-update endpoint {:?}",
                line.stock_after
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::prepared::{BatchQueryResult, BatchResponse};
    use crate::ranking::common::accept_batch;

    fn line(
        number: u8,
        supply_warehouse: i32,
        item_id: i32,
        quantity: i32,
        invalid_item: bool,
    ) -> LinePlan {
        LinePlan {
            number,
            item_id,
            supply_warehouse,
            quantity,
            invalid_item,
        }
    }

    fn input(lines: Vec<LinePlan>, expected_rollback: bool) -> ValidatedInput {
        ValidatedInput {
            warehouse_id: 2,
            district_id: 3,
            customer_id: 17,
            timestamp: b"2026-07-29 10:20:30".to_vec(),
            all_local: 0,
            expected_rollback,
            lines,
        }
    }

    fn materialized(plan: LinePlan, stock: i32, amount: f32) -> MaterializedLine {
        MaterializedLine {
            plan,
            initial_stock: StockVersion {
                quantity: stock,
                ytd_bits: 0.0_f32.to_bits(),
                order_count: 0,
                remote_count: 0,
            },
            amount_bits: amount.to_bits(),
            district_info: b"district".to_vec(),
        }
    }

    fn ids(operations: &[Operation]) -> Vec<u16> {
        operations
            .iter()
            .map(|operation| operation.statement_id)
            .collect()
    }

    #[test]
    fn stage_one_locks_sorted_unique_before_ordered_line_reads() {
        let input = input(
            vec![
                line(1, 9, 80, 3, false),
                line(2, 1, 70, 4, false),
                line(3, 9, 80, 5, false),
            ],
            false,
        );
        let plan = build_stage_one(&input);

        assert_eq!(
            ids(&plan.operations),
            vec![
                StatementId::Begin.wire_id(),
                StatementId::NewOrderHome.wire_id(),
                StatementId::NewOrderLockStock.wire_id(),
                StatementId::NewOrderLockStock.wire_id(),
                StatementId::NewOrderItem.wire_id(),
                StatementId::NewOrderStock.wire_id(),
                StatementId::NewOrderItem.wire_id(),
                StatementId::NewOrderStock.wire_id(),
                StatementId::NewOrderItem.wire_id(),
                StatementId::NewOrderStock.wire_id(),
            ]
        );
        assert_eq!(
            plan.operations[2].parameters,
            vec![WireValue::Int32(1), WireValue::Int32(70)]
        );
        assert_eq!(
            plan.operations[3].parameters,
            vec![WireValue::Int32(9), WireValue::Int32(80)]
        );
        assert_eq!(plan.operations[4].parameters, vec![WireValue::Int32(80)]);
        assert_eq!(plan.operations[6].parameters, vec![WireValue::Int32(70)]);
        assert_eq!(plan.operations[8].parameters, vec![WireValue::Int32(80)]);
    }

    #[test]
    fn invalid_item_batch_writes_valid_prefix_then_aborts() {
        let first = line(1, 2, 10, 4, false);
        let second = line(2, 7, 20, 6, false);
        let invalid = line(3, 2, INVALID_ITEM_ID as i32, 2, true);
        let input = input(vec![first, second, invalid], true);
        let materialized = MaterializedOrder {
            order_id: 3001,
            lines: vec![materialized(first, 90, 8.0), materialized(second, 12, 15.0)],
        };

        let stage = build_stage_two(&input, &materialized).unwrap();
        assert_eq!(
            ids(&stage.operations),
            vec![
                StatementId::NewOrderAdvanceDistrict.wire_id(),
                StatementId::NewOrderInsertOrder.wire_id(),
                StatementId::NewOrderInsertQueue.wire_id(),
                StatementId::NewOrderUpdateStockNormal.wire_id(),
                StatementId::NewOrderInsertLine.wire_id(),
                StatementId::NewOrderUpdateStockWrapped.wire_id(),
                StatementId::NewOrderInsertLine.wire_id(),
                StatementId::Abort.wire_id(),
            ]
        );
        assert!(!stage
            .operations
            .iter()
            .any(|operation| operation.statement_id == StatementId::Commit.wire_id()));
        assert_eq!(stage.stock_ytd_delta, 10);
        assert_eq!(stage.remote_line_count, 1);
        assert_eq!(
            stage.operations[1].parameters[6],
            WireValue::Int32(3),
            "rolled-back header still describes the complete attempted order"
        );
    }

    #[test]
    fn successful_batch_ends_in_commit_and_uses_relative_float_delta() {
        let only = line(1, 2, 10, 7, false);
        let input = input(vec![only], false);
        let materialized = MaterializedOrder {
            order_id: 3001,
            lines: vec![materialized(only, 100, 14.0)],
        };

        let stage = build_stage_two(&input, &materialized).unwrap();
        assert_eq!(
            stage.operations.last().unwrap().statement_id,
            StatementId::Commit.wire_id()
        );
        assert_eq!(
            stage.operations[3].parameters,
            vec![
                WireValue::Int32(7),
                WireValue::Float32(7.0_f32.to_bits()),
                WireValue::Int32(0),
                WireValue::Int32(2),
                WireValue::Int32(10),
                WireValue::Int32(17),
            ]
        );
    }

    #[test]
    fn repeated_stock_keys_choose_updates_from_simulated_relative_state() {
        let first = line(1, 2, 10, 8, false);
        let second = line(2, 2, 10, 8, false);
        let input = input(vec![first, second], false);
        let materialized = MaterializedOrder {
            order_id: 3001,
            lines: vec![materialized(first, 25, 8.0), materialized(second, 25, 8.0)],
        };

        let stage = build_stage_two(&input, &materialized).unwrap();
        assert_eq!(
            stage.operations[3].statement_id,
            StatementId::NewOrderUpdateStockNormal.wire_id()
        );
        assert_eq!(
            stage.operations[5].statement_id,
            StatementId::NewOrderUpdateStockWrapped.wire_id()
        );
        assert_eq!(
            stage.operations[5].parameters,
            vec![
                WireValue::Int32(8),
                WireValue::Float32(8.0_f32.to_bits()),
                WireValue::Int32(0),
                WireValue::Int32(2),
                WireValue::Int32(10),
                WireValue::Int32(18),
            ],
            "wrapped stock binds quantity and the public branch threshold"
        );
        assert_eq!(stage.recovery_lines.len(), 2);
        assert_eq!(stage.recovery_lines[0].stock_before.quantity, 25);
        assert_eq!(stage.recovery_lines[0].stock_after.quantity, 17);
        assert_eq!(
            stage.recovery_lines[0].stock_after.ytd_bits,
            8.0_f32.to_bits()
        );
        assert_eq!(stage.recovery_lines[0].stock_after.order_count, 1);
        assert_eq!(stage.recovery_lines[1].stock_before.quantity, 17);
        assert_eq!(stage.recovery_lines[1].stock_after.quantity, 100);
        assert_eq!(
            stage.recovery_lines[1].stock_after.ytd_bits,
            16.0_f32.to_bits()
        );
        assert_eq!(stage.recovery_lines[1].stock_after.order_count, 2);
    }

    fn readback_results(stage: &StageTwoPlan, _versions: &[StockVersion]) -> BatchResults {
        assert!(
            stage.stock_after_results.is_empty(),
            "no per-line stock read-backs are issued"
        );
        let results = Vec::new();
        accept_batch(
            BatchResponse::Ok {
                executed_operations: stage.operations.len() as u16,
                results,
            },
            &stage.operations,
        )
        .unwrap()
    }

    fn one_line_stage() -> StageTwoPlan {
        let only = line(1, 2, 10, 7, false);
        let input = input(vec![only], false);
        let materialized = MaterializedOrder {
            order_id: 3001,
            lines: vec![materialized(only, 100, 14.0)],
        };
        build_stage_two(&input, &materialized).unwrap()
    }

    #[test]
    fn stock_readback_skipped_when_none_issued() {
        let stage = one_line_stage();
        assert!(stage.stock_after_results.is_empty());
        assert!(
            validate_stage_two_stock_readbacks(&readback_results(&stage, &[]), &stage).is_ok()
        );
    }

    #[test]
    fn stock_readback_skipped_even_with_pending_versions() {
        let stage = one_line_stage();
        assert!(stage.stock_after_results.is_empty());
        let stale = stage.recovery_lines[0].stock_before.clone();
        // No read-backs are issued, so any supplied read-back set is ignored
        // and validation trivially passes.
        assert!(
            validate_stage_two_stock_readbacks(&readback_results(&stage, &[stale]), &stage).is_ok()
        );
    }

    #[test]
    fn amount_uses_one_finite_binary32_multiplication() {
        let price = f32::from_bits(1.0_f32.to_bits() + 1);
        assert_eq!(
            multiply_f32_bits(price.to_bits(), 7).unwrap(),
            (price * 7.0_f32).to_bits()
        );
        assert!(multiply_f32_bits(f32::INFINITY.to_bits(), 7).is_err());
        assert!(multiply_f32_bits(f32::MAX.to_bits(), 10).is_err());
    }
}
