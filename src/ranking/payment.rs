//! Public final-2026 Payment transaction runner.
//!
//! A successful execution uses exactly two `EXEC_BATCH` round trips.  The
//! immutable routed/input keys and the caller-supplied timestamp are reused by
//! the outer retry loop; this runner never redraws transaction parameters.

use std::cmp::Ordering;

use crate::connection::client::RmdbClient;
use crate::connection::prepared::Operation;
use crate::connection::wire::WireValue;
use crate::profile::{TransactionKind, DISTRICTS_PER_WAREHOUSE, OFFICIAL_WAREHOUSES};
use crate::routing::RoutedTransaction;
use crate::workload::{
    CustomerSelector, PaymentInput, CUSTOMERS_PER_DISTRICT, MAX_PAYMENT_CENTS, MIN_PAYMENT_CENTS,
};

use super::catalog::StatementId;
use super::common::{
    customer_lower_median, expect_f32_add, expect_f32_sub, operation, row_char, row_f32_bits,
    row_int32, BatchResults, SemanticResult, SemanticResultExt, SemanticViolation,
};
use super::runner::{
    execute_batch, semantic_or_abort, CustomerVersion, PaymentEvidence, RankedCommit,
    RankedTransactionError, RankedTransactionOutcome,
};

const STAGE_ONE_WAREHOUSE_BEFORE: usize = 1;
const STAGE_ONE_WAREHOUSE_AFTER: usize = 3;
const STAGE_ONE_DISTRICT_BEFORE: usize = 4;
const STAGE_ONE_DISTRICT_AFTER: usize = 6;
const STAGE_ONE_CUSTOMER: usize = 7;
const STAGE_TWO_CUSTOMER_AFTER: usize = 2;

const WAREHOUSE_COLUMNS: usize = 2;
const WAREHOUSE_AFTER_COLUMNS: usize = 1;
const DISTRICT_COLUMNS: usize = 2;
const DISTRICT_AFTER_COLUMNS: usize = 1;
const CUSTOMER_COLUMNS: usize = 9;
const CUSTOMER_AFTER_COLUMNS: usize = 5;
const MAX_NAME_BYTES: usize = 10;
const MAX_HISTORY_DATA_BYTES: usize = 24;
const MAX_CUSTOMER_DATA_BYTES: usize = 50;
const MAX_TIMESTAMP_BYTES: usize = 30;

/// Execute one Payment transaction using the public final-2026 two-batch
/// dependency boundary.
pub async fn execute(
    client: &mut RmdbClient,
    route: &RoutedTransaction,
    input: &PaymentInput,
    timestamp: &str,
) -> Result<RankedTransactionOutcome, RankedTransactionError> {
    validate_request(route, input, timestamp).map_err(RankedTransactionError::Semantic)?;

    let home_warehouse = route.home_warehouse;
    let home_district = route.home_district;
    let amount_bits = input.amount_bits();
    let stage_one_operations = build_stage_one(
        home_warehouse,
        home_district,
        input.customer_warehouse(),
        input.customer_district(),
        input.customer(),
        amount_bits,
    );
    let stage_one_results = execute_batch(client, &stage_one_operations).await?;

    // The first successful batch deliberately leaves the transaction active.
    // Any local semantic failure at this boundary therefore requires ABORT.
    let snapshot = semantic_or_abort(
        client,
        validate_stage_one(&stage_one_results, input.customer(), amount_bits)
            .require_explicit_abort(),
    )
    .await?;

    let stage_two = semantic_or_abort(
        client,
        build_stage_two(
            home_warehouse,
            home_district,
            input.customer_warehouse(),
            input.customer_district(),
            input.amount_cents(),
            amount_bits,
            timestamp,
            &snapshot,
        )
        .require_explicit_abort(),
    )
    .await?;

    let stage_two_results = execute_batch(client, &stage_two.operations).await?;

    // COMMIT is the last operation in stage two.  A semantic mismatch found
    // while inspecting its already-returned results must not send ABORT.
    let customer_after = validate_stage_two(
        &stage_two_results,
        amount_bits,
        &snapshot,
        &stage_two.expected_customer_data,
    )
    .map_err(RankedTransactionError::Semantic)?;

    Ok(RankedTransactionOutcome::Committed(RankedCommit::Payment(
        PaymentEvidence {
            warehouse_id: home_warehouse,
            district_id: home_district,
            customer_warehouse_id: input.customer_warehouse(),
            customer_district_id: input.customer_district(),
            customer_id: snapshot.customer.id,
            amount_bits,
            warehouse_before_bits: snapshot.warehouse_before_bits,
            warehouse_after_bits: snapshot.warehouse_after_bits,
            district_before_bits: snapshot.district_before_bits,
            district_after_bits: snapshot.district_after_bits,
            customer_balance_before_bits: snapshot.customer.balance_bits,
            customer_balance_after_bits: customer_after.balance_bits,
            customer_ytd_before_bits: snapshot.customer.ytd_payment_bits,
            customer_ytd_after_bits: customer_after.ytd_payment_bits,
            customer_version_before: CustomerVersion {
                payment_count: snapshot.customer.payment_count,
                delivery_count: snapshot.customer.delivery_count,
            },
            customer_version_after: CustomerVersion {
                payment_count: customer_after.payment_count,
                delivery_count: customer_after.delivery_count,
            },
            history_timestamp: timestamp.as_bytes().to_vec(),
            history_data: stage_two.history_data,
            customer_is_bad_credit: stage_two.customer_is_bad_credit,
            customer_data_before: snapshot.customer.data,
            customer_data_after: stage_two.expected_customer_data,
        },
    )))
}

fn validate_request(
    route: &RoutedTransaction,
    input: &PaymentInput,
    timestamp: &str,
) -> SemanticResult<()> {
    if route.kind != TransactionKind::Payment {
        return Err(SemanticViolation::new(format!(
            "Payment runner received {:?} route",
            route.kind
        )));
    }
    if !(1..=OFFICIAL_WAREHOUSES).contains(&route.home_warehouse) {
        return Err(SemanticViolation::new(format!(
            "Payment home warehouse {} is out of range",
            route.home_warehouse
        )));
    }
    if !(1..=DISTRICTS_PER_WAREHOUSE).contains(&route.home_district) {
        return Err(SemanticViolation::new(format!(
            "Payment home district {} is out of range",
            route.home_district
        )));
    }
    if route.payment_customer_warehouse != input.customer_warehouse() {
        return Err(SemanticViolation::new(format!(
            "routed Payment customer warehouse {} does not match frozen input {}",
            route.payment_customer_warehouse,
            input.customer_warehouse()
        )));
    }
    if !(1..=OFFICIAL_WAREHOUSES).contains(&input.customer_warehouse()) {
        return Err(SemanticViolation::new(format!(
            "Payment customer warehouse {} is out of range",
            input.customer_warehouse()
        )));
    }
    if !(1..=DISTRICTS_PER_WAREHOUSE).contains(&input.customer_district()) {
        return Err(SemanticViolation::new(format!(
            "Payment customer district {} is out of range",
            input.customer_district()
        )));
    }
    match input.customer() {
        CustomerSelector::Id(customer_id)
            if !(1..=CUSTOMERS_PER_DISTRICT).contains(customer_id) =>
        {
            return Err(SemanticViolation::new(format!(
                "Payment customer id {customer_id} is out of range"
            )));
        }
        CustomerSelector::LastName(last_name)
            if last_name.value().is_empty() || last_name.value().len() > 16 =>
        {
            return Err(SemanticViolation::new(
                "Payment customer last name must contain 1..=16 bytes",
            ));
        }
        _ => {}
    }
    if !(MIN_PAYMENT_CENTS..=MAX_PAYMENT_CENTS).contains(&input.amount_cents()) {
        return Err(SemanticViolation::new(format!(
            "Payment amount {} cents is out of range",
            input.amount_cents()
        )));
    }
    let expected_amount_bits = (input.amount_cents() as f32 / 100.0_f32).to_bits();
    if input.amount_bits() != expected_amount_bits {
        return Err(SemanticViolation::new(format!(
            "Payment bound amount bits 0x{:08x} do not match {} cents (0x{expected_amount_bits:08x})",
            input.amount_bits(),
            input.amount_cents()
        )));
    }
    if timestamp.is_empty() || timestamp.len() > MAX_TIMESTAMP_BYTES {
        return Err(SemanticViolation::new(format!(
            "Payment timestamp length {} is outside 1..={MAX_TIMESTAMP_BYTES}",
            timestamp.len()
        )));
    }
    Ok(())
}

fn build_stage_one(
    home_warehouse: u16,
    home_district: u8,
    customer_warehouse: u16,
    customer_district: u8,
    customer: &CustomerSelector,
    amount_bits: u32,
) -> Vec<Operation> {
    let amount = WireValue::Float32(amount_bits);
    let home_w = WireValue::Int32(i32::from(home_warehouse));
    let home_d = WireValue::Int32(i32::from(home_district));
    let customer_w = WireValue::Int32(i32::from(customer_warehouse));
    let customer_d = WireValue::Int32(i32::from(customer_district));
    let (customer_statement, customer_key) = match customer {
        CustomerSelector::Id(customer_id) => (
            StatementId::PaymentCustomerById,
            WireValue::Int32(i32::from(*customer_id)),
        ),
        CustomerSelector::LastName(last_name) => (
            StatementId::PaymentCustomerByLast,
            WireValue::Char(last_name.value().as_bytes().to_vec()),
        ),
    };

    vec![
        operation(StatementId::Begin, []),
        operation(StatementId::PaymentWarehouse, [home_w.clone()]),
        operation(
            StatementId::PaymentUpdateWarehouse,
            [amount.clone(), home_w.clone()],
        ),
        operation(StatementId::PaymentWarehouseAfter, [home_w.clone()]),
        operation(
            StatementId::PaymentDistrict,
            [home_w.clone(), home_d.clone()],
        ),
        operation(
            StatementId::PaymentUpdateDistrict,
            [amount, home_w.clone(), home_d.clone()],
        ),
        operation(StatementId::PaymentDistrictAfter, [home_w, home_d]),
        operation(customer_statement, [customer_w, customer_d, customer_key]),
    ]
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CustomerCredit {
    Good,
    Bad,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CustomerSnapshot {
    id: i32,
    first: Vec<u8>,
    last: Vec<u8>,
    credit: CustomerCredit,
    balance_bits: u32,
    ytd_payment_bits: u32,
    payment_count: i32,
    delivery_count: i32,
    data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PaymentSnapshot {
    warehouse_before_bits: u32,
    warehouse_after_bits: u32,
    district_before_bits: u32,
    district_after_bits: u32,
    warehouse_name: Vec<u8>,
    district_name: Vec<u8>,
    customer: CustomerSnapshot,
}

fn validate_stage_one(
    results: &BatchResults,
    selector: &CustomerSelector,
    amount_bits: u32,
) -> SemanticResult<PaymentSnapshot> {
    let warehouse_before = results.single_row(STAGE_ONE_WAREHOUSE_BEFORE)?;
    expect_width(
        warehouse_before,
        WAREHOUSE_COLUMNS,
        "Payment warehouse before",
    )?;
    let warehouse_after = results.single_row(STAGE_ONE_WAREHOUSE_AFTER)?;
    expect_width(
        warehouse_after,
        WAREHOUSE_AFTER_COLUMNS,
        "Payment warehouse after",
    )?;
    let warehouse_before_bits = row_f32_bits(warehouse_before, 0, "Payment warehouse before")?;
    let warehouse_after_bits = row_f32_bits(warehouse_after, 0, "Payment warehouse after")?;
    expect_f32_add(
        warehouse_before_bits,
        amount_bits,
        warehouse_after_bits,
        "Payment warehouse.w_ytd",
    )?;
    let warehouse_name = row_char(warehouse_before, 1, "Payment warehouse before")?.to_vec();

    let district_before = results.single_row(STAGE_ONE_DISTRICT_BEFORE)?;
    expect_width(district_before, DISTRICT_COLUMNS, "Payment district before")?;
    let district_after = results.single_row(STAGE_ONE_DISTRICT_AFTER)?;
    expect_width(
        district_after,
        DISTRICT_AFTER_COLUMNS,
        "Payment district after",
    )?;
    let district_before_bits = row_f32_bits(district_before, 0, "Payment district before")?;
    let district_after_bits = row_f32_bits(district_after, 0, "Payment district after")?;
    expect_f32_add(
        district_before_bits,
        amount_bits,
        district_after_bits,
        "Payment district.d_ytd",
    )?;
    let district_name = row_char(district_before, 1, "Payment district before")?.to_vec();

    let customer_rows = results.rows(STAGE_ONE_CUSTOMER)?;
    let customer = select_customer(customer_rows, selector)?;

    Ok(PaymentSnapshot {
        warehouse_before_bits,
        warehouse_after_bits,
        district_before_bits,
        district_after_bits,
        warehouse_name,
        district_name,
        customer,
    })
}

fn select_customer(
    rows: &[Vec<WireValue>],
    selector: &CustomerSelector,
) -> SemanticResult<CustomerSnapshot> {
    match selector {
        CustomerSelector::Id(expected_id) => {
            let [row] = rows else {
                return Err(SemanticViolation::new(format!(
                    "Payment customer id lookup returned {} rows; expected exactly one",
                    rows.len()
                )));
            };
            let customer = parse_customer(row)?;
            if customer.id != i32::from(*expected_id) {
                return Err(SemanticViolation::new(format!(
                    "Payment customer id lookup requested {expected_id}, returned {}",
                    customer.id
                )));
            }
            Ok(customer)
        }
        CustomerSelector::LastName(last_name) => {
            select_last_name_customer(rows, last_name.value().as_bytes())
        }
    }
}

fn select_last_name_customer(
    rows: &[Vec<WireValue>],
    expected_last: &[u8],
) -> SemanticResult<CustomerSnapshot> {
    let mut customers = Vec::with_capacity(rows.len());
    for row in rows {
        let customer = parse_customer(row)?;
        if customer.last != expected_last {
            return Err(SemanticViolation::new(format!(
                "Payment surname lookup returned customer {} with a different last name",
                customer.id
            )));
        }
        customers.push(customer);
    }

    customers.sort_by(compare_customer_order);
    for pair in customers.windows(2) {
        if compare_customer_order(&pair[0], &pair[1]) == Ordering::Equal {
            return Err(SemanticViolation::new(format!(
                "Payment surname lookup returned duplicate (c_first, c_id) for customer {}",
                pair[1].id
            )));
        }
    }

    Ok(customer_lower_median(&customers)?.clone())
}

fn compare_customer_order(left: &CustomerSnapshot, right: &CustomerSnapshot) -> Ordering {
    left.first
        .cmp(&right.first)
        .then_with(|| left.id.cmp(&right.id))
}

fn parse_customer(row: &[WireValue]) -> SemanticResult<CustomerSnapshot> {
    expect_width(row, CUSTOMER_COLUMNS, "Payment customer")?;
    let id = row_int32(row, 0, "Payment customer")?;
    if !(1..=i32::from(CUSTOMERS_PER_DISTRICT)).contains(&id) {
        return Err(SemanticViolation::new(format!(
            "Payment customer result id {id} is out of range"
        )));
    }
    let first = row_char(row, 1, "Payment customer")?.to_vec();
    let last = row_char(row, 2, "Payment customer")?.to_vec();
    let credit = match row_char(row, 3, "Payment customer")? {
        b"GC" => CustomerCredit::Good,
        b"BC" => CustomerCredit::Bad,
        value => {
            return Err(SemanticViolation::new(format!(
                "Payment customer {id} has invalid c_credit {:?}",
                String::from_utf8_lossy(value)
            )));
        }
    };
    let payment_count = row_int32(row, 6, "Payment customer")?;
    let delivery_count = row_int32(row, 7, "Payment customer")?;
    if payment_count < 0 || delivery_count < 0 {
        return Err(SemanticViolation::new(format!(
            "Payment customer {id} has negative logical version ({payment_count},{delivery_count})"
        )));
    }
    let data = row_char(row, 8, "Payment customer")?.to_vec();
    if data.len() > MAX_CUSTOMER_DATA_BYTES {
        return Err(SemanticViolation::new(format!(
            "Payment customer {id} c_data has {} bytes; maximum is {MAX_CUSTOMER_DATA_BYTES}",
            data.len()
        )));
    }

    Ok(CustomerSnapshot {
        id,
        first,
        last,
        credit,
        balance_bits: row_f32_bits(row, 4, "Payment customer")?,
        ytd_payment_bits: row_f32_bits(row, 5, "Payment customer")?,
        payment_count,
        delivery_count,
        data,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StageTwo {
    operations: Vec<Operation>,
    expected_customer_data: Vec<u8>,
    history_data: Vec<u8>,
    customer_is_bad_credit: bool,
}

#[allow(clippy::too_many_arguments)]
fn build_stage_two(
    home_warehouse: u16,
    home_district: u8,
    customer_warehouse: u16,
    customer_district: u8,
    amount_cents: u32,
    amount_bits: u32,
    timestamp: &str,
    snapshot: &PaymentSnapshot,
) -> SemanticResult<StageTwo> {
    let h_data = history_data(&snapshot.warehouse_name, &snapshot.district_name)?;
    let expected_customer_data = match snapshot.customer.credit {
        CustomerCredit::Good => snapshot.customer.data.clone(),
        CustomerCredit::Bad => bad_credit_data(
            snapshot.customer.id,
            customer_district,
            customer_warehouse,
            home_district,
            home_warehouse,
            amount_cents,
            &snapshot.customer.data,
        ),
    };

    let amount = WireValue::Float32(amount_bits);
    let customer_w = WireValue::Int32(i32::from(customer_warehouse));
    let customer_d = WireValue::Int32(i32::from(customer_district));
    let customer_id = WireValue::Int32(snapshot.customer.id);
    let customer_update = match snapshot.customer.credit {
        CustomerCredit::Good => operation(
            StatementId::PaymentUpdateGoodCustomer,
            [
                amount.clone(),
                customer_w.clone(),
                customer_d.clone(),
                customer_id.clone(),
            ],
        ),
        CustomerCredit::Bad => operation(
            StatementId::PaymentUpdateBadCustomer,
            [
                amount.clone(),
                WireValue::Char(expected_customer_data.clone()),
                customer_w.clone(),
                customer_d.clone(),
                customer_id.clone(),
            ],
        ),
    };

    let operations = vec![
        customer_update,
        operation(
            StatementId::PaymentInsertHistory,
            [
                customer_id.clone(),
                customer_d.clone(),
                customer_w.clone(),
                WireValue::Int32(i32::from(home_district)),
                WireValue::Int32(i32::from(home_warehouse)),
                WireValue::Char(timestamp.as_bytes().to_vec()),
                amount,
                WireValue::Char(h_data.clone()),
            ],
        ),
        operation(
            StatementId::PaymentCustomerAfter,
            [customer_w, customer_d, customer_id],
        ),
        operation(StatementId::Commit, []),
    ];

    Ok(StageTwo {
        operations,
        expected_customer_data,
        history_data: h_data,
        customer_is_bad_credit: snapshot.customer.credit == CustomerCredit::Bad,
    })
}

fn history_data(warehouse_name: &[u8], district_name: &[u8]) -> SemanticResult<Vec<u8>> {
    if warehouse_name.len() > MAX_NAME_BYTES || district_name.len() > MAX_NAME_BYTES {
        return Err(SemanticViolation::new(format!(
            "Payment history names exceed CHAR(10): warehouse={}, district={}",
            warehouse_name.len(),
            district_name.len()
        )));
    }
    let mut value = Vec::with_capacity(warehouse_name.len() + 4 + district_name.len());
    value.extend_from_slice(warehouse_name);
    value.extend_from_slice(b"    ");
    value.extend_from_slice(district_name);
    if value.len() > MAX_HISTORY_DATA_BYTES {
        return Err(SemanticViolation::new(format!(
            "Payment h_data has {} bytes; maximum is {MAX_HISTORY_DATA_BYTES}",
            value.len()
        )));
    }
    Ok(value)
}

/// TPC-C 2.5.2.2 bad-credit data: prepend the six Payment fields, separated
/// by spaces, to the old value and retain the leading CHAR(50) bytes.
fn bad_credit_data(
    customer_id: i32,
    customer_district: u8,
    customer_warehouse: u16,
    home_district: u8,
    home_warehouse: u16,
    amount_cents: u32,
    old_data: &[u8],
) -> Vec<u8> {
    let prefix = format!(
        "{customer_id} {customer_district} {customer_warehouse} \
         {home_district} {home_warehouse} {}.{:02} ",
        amount_cents / 100,
        amount_cents % 100
    );
    let mut value = Vec::with_capacity(MAX_CUSTOMER_DATA_BYTES);
    value.extend_from_slice(prefix.as_bytes());
    value.extend_from_slice(old_data);
    value.truncate(MAX_CUSTOMER_DATA_BYTES);
    value
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CustomerAfter {
    balance_bits: u32,
    ytd_payment_bits: u32,
    payment_count: i32,
    delivery_count: i32,
}

fn validate_stage_two(
    results: &BatchResults,
    amount_bits: u32,
    snapshot: &PaymentSnapshot,
    expected_customer_data: &[u8],
) -> SemanticResult<CustomerAfter> {
    let row = results.single_row(STAGE_TWO_CUSTOMER_AFTER)?;
    expect_width(row, CUSTOMER_AFTER_COLUMNS, "Payment customer after")?;
    let balance_bits = row_f32_bits(row, 0, "Payment customer after")?;
    let ytd_payment_bits = row_f32_bits(row, 1, "Payment customer after")?;
    expect_f32_sub(
        snapshot.customer.balance_bits,
        amount_bits,
        balance_bits,
        "Payment customer.c_balance",
    )?;
    expect_f32_add(
        snapshot.customer.ytd_payment_bits,
        amount_bits,
        ytd_payment_bits,
        "Payment customer.c_ytd_payment",
    )?;
    let expected_count = snapshot
        .customer
        .payment_count
        .checked_add(1)
        .ok_or_else(|| SemanticViolation::new("Payment c_payment_cnt overflow"))?;
    let actual_count = row_int32(row, 2, "Payment customer after")?;
    if actual_count != expected_count {
        return Err(SemanticViolation::new(format!(
            "Payment customer.c_payment_cnt mismatch: expected {expected_count}, got \
             {actual_count}"
        )));
    }
    let actual_delivery_count = row_int32(row, 3, "Payment customer after")?;
    if actual_delivery_count != snapshot.customer.delivery_count {
        return Err(SemanticViolation::new(format!(
            "Payment customer.c_delivery_cnt changed from {} to {actual_delivery_count}",
            snapshot.customer.delivery_count
        )));
    }
    let actual_data = row_char(row, 4, "Payment customer after")?;
    if actual_data != expected_customer_data {
        return Err(SemanticViolation::new(format!(
            "Payment customer.c_data mismatch: expected {} bytes, got {} bytes",
            expected_customer_data.len(),
            actual_data.len()
        )));
    }
    Ok(CustomerAfter {
        balance_bits,
        ytd_payment_bits,
        payment_count: actual_count,
        delivery_count: actual_delivery_count,
    })
}

fn expect_width(row: &[WireValue], expected: usize, context: &str) -> SemanticResult<()> {
    if row.len() != expected {
        return Err(SemanticViolation::new(format!(
            "{context} returned {} columns; expected {expected}",
            row.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::prepared::{BatchQueryResult, BatchResponse};
    use crate::ranking::common::accept_batch;

    fn result(
        operations: &[Operation],
        rows: impl IntoIterator<Item = (u16, Vec<Vec<WireValue>>)>,
    ) -> BatchResults {
        accept_batch(
            BatchResponse::Ok {
                executed_operations: operations.len() as u16,
                results: rows
                    .into_iter()
                    .map(|(operation_index, rows)| BatchQueryResult {
                        operation_index,
                        rows,
                    })
                    .collect(),
            },
            operations,
        )
        .unwrap()
    }

    fn warehouse_row(ytd_bits: u32, name: &[u8]) -> Vec<WireValue> {
        vec![
            WireValue::Float32(ytd_bits),
            WireValue::Char(name.to_vec()),
        ]
    }

    fn district_row(ytd_bits: u32, name: &[u8]) -> Vec<WireValue> {
        warehouse_row(ytd_bits, name)
    }

    fn ytd_row(ytd_bits: u32) -> Vec<WireValue> {
        vec![WireValue::Float32(ytd_bits)]
    }

    fn customer_row(
        id: i32,
        first: &[u8],
        last: &[u8],
        credit: &[u8],
        balance_bits: u32,
        ytd_bits: u32,
        payment_count: i32,
        delivery_count: i32,
        data: &[u8],
    ) -> Vec<WireValue> {
        vec![
            WireValue::Int32(id),
            WireValue::Char(first.to_vec()),
            WireValue::Char(last.to_vec()),
            WireValue::Char(credit.to_vec()),
            WireValue::Float32(balance_bits),
            WireValue::Float32(ytd_bits),
            WireValue::Int32(payment_count),
            WireValue::Int32(delivery_count),
            WireValue::Char(data.to_vec()),
        ]
    }

    fn snapshot(credit: CustomerCredit) -> PaymentSnapshot {
        PaymentSnapshot {
            warehouse_before_bits: 100.0_f32.to_bits(),
            warehouse_after_bits: 101.0_f32.to_bits(),
            district_before_bits: 200.0_f32.to_bits(),
            district_after_bits: 201.0_f32.to_bits(),
            warehouse_name: b"WAREHOUSE".to_vec(),
            district_name: b"DISTRICT".to_vec(),
            customer: CustomerSnapshot {
                id: 321,
                first: b"ALICE".to_vec(),
                last: b"BARBARBAR".to_vec(),
                credit,
                balance_bits: (-10.0_f32).to_bits(),
                ytd_payment_bits: 10.0_f32.to_bits(),
                payment_count: 1,
                delivery_count: 0,
                data: b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwx".to_vec(),
            },
        }
    }

    #[test]
    fn stage_one_uses_exact_indices_and_separates_remote_keys() {
        let amount_bits = 17.25_f32.to_bits();
        let operations = build_stage_one(3, 4, 50, 7, &CustomerSelector::Id(321), amount_bits);
        assert_eq!(operations.len(), 8);
        assert_eq!(
            operations
                .iter()
                .map(|operation| operation.statement_id)
                .collect::<Vec<_>>(),
            vec![
                StatementId::Begin.wire_id(),
                StatementId::PaymentWarehouse.wire_id(),
                StatementId::PaymentUpdateWarehouse.wire_id(),
                StatementId::PaymentWarehouseAfter.wire_id(),
                StatementId::PaymentDistrict.wire_id(),
                StatementId::PaymentUpdateDistrict.wire_id(),
                StatementId::PaymentDistrictAfter.wire_id(),
                StatementId::PaymentCustomerById.wire_id(),
            ]
        );
        assert_eq!(
            operations[STAGE_ONE_WAREHOUSE_BEFORE].parameters,
            vec![WireValue::Int32(3)]
        );
        assert_eq!(
            operations[2].parameters,
            vec![WireValue::Float32(amount_bits), WireValue::Int32(3)]
        );
        assert_eq!(
            operations[STAGE_ONE_DISTRICT_BEFORE].parameters,
            vec![WireValue::Int32(3), WireValue::Int32(4)]
        );
        assert_eq!(
            operations[5].parameters,
            vec![
                WireValue::Float32(amount_bits),
                WireValue::Int32(3),
                WireValue::Int32(4),
            ]
        );
        assert_eq!(
            operations[STAGE_ONE_CUSTOMER].parameters,
            vec![
                WireValue::Int32(50),
                WireValue::Int32(7),
                WireValue::Int32(321),
            ]
        );
    }

    #[test]
    fn surname_selection_uses_stable_lower_median() {
        let rows = vec![
            customer_row(
                8,
                b"ALICE",
                b"BARBARBAR",
                b"GC",
                0.0_f32.to_bits(),
                10.0_f32.to_bits(),
                1,
                0,
                b"a",
            ),
            customer_row(
                2,
                b"BOB",
                b"BARBARBAR",
                b"GC",
                0.0_f32.to_bits(),
                10.0_f32.to_bits(),
                1,
                0,
                b"b",
            ),
            customer_row(
                9,
                b"BOB",
                b"BARBARBAR",
                b"GC",
                0.0_f32.to_bits(),
                10.0_f32.to_bits(),
                1,
                0,
                b"c",
            ),
            customer_row(
                4,
                b"CAROL",
                b"BARBARBAR",
                b"BC",
                0.0_f32.to_bits(),
                10.0_f32.to_bits(),
                1,
                0,
                b"d",
            ),
        ];
        let selected = select_last_name_customer(&rows, b"BARBARBAR").unwrap();
        assert_eq!(selected.id, 2);

        let mut reordered = rows.clone();
        reordered.swap(1, 2);
        let selected = select_last_name_customer(&reordered, b"BARBARBAR").unwrap();
        assert_eq!(selected.id, 2);

        let duplicate_row = rows[0].clone();
        let mut duplicate = rows;
        duplicate.push(duplicate_row);
        assert!(select_last_name_customer(&duplicate, b"BARBARBAR").is_err());
    }

    #[test]
    fn large_float_update_requires_zero_ulp() {
        let selector = CustomerSelector::Id(321);
        let amount_bits = 1.0_f32.to_bits();
        let operations = build_stage_one(3, 4, 3, 4, &selector, amount_bits);
        let large = 16_777_216.0_f32.to_bits();
        let district_before = 30_000.0_f32.to_bits();
        let district_after = (30_000.0_f32 + 1.0_f32).to_bits();
        let customer = customer_row(
            321,
            b"ALICE",
            b"LAST",
            b"GC",
            (-10.0_f32).to_bits(),
            10.0_f32.to_bits(),
            1,
            0,
            b"data",
        );
        let good = result(
            &operations,
            [
                (1, vec![warehouse_row(large, b"WAREHOUSE")]),
                (3, vec![ytd_row(large)]),
                (4, vec![district_row(district_before, b"DISTRICT")]),
                (6, vec![ytd_row(district_after)]),
                (7, vec![customer.clone()]),
            ],
        );
        validate_stage_one(&good, &selector, amount_bits).unwrap();

        let one_ulp_wrong = result(
            &operations,
            [
                (1, vec![warehouse_row(large, b"WAREHOUSE")]),
                (3, vec![ytd_row(large.wrapping_add(1))]),
                (4, vec![district_row(district_before, b"DISTRICT")]),
                (6, vec![ytd_row(district_after)]),
                (7, vec![customer]),
            ],
        );
        assert!(validate_stage_one(&one_ulp_wrong, &selector, amount_bits).is_err());
    }

    #[test]
    fn bad_credit_stage_truncates_data_and_preserves_all_keys() {
        let snapshot = snapshot(CustomerCredit::Bad);
        let amount_bits = 5_000.0_f32.to_bits();
        let stage = build_stage_two(
            3,
            4,
            50,
            7,
            500_000,
            amount_bits,
            "2026-07-29 12:34:56",
            &snapshot,
        )
        .unwrap();
        assert_eq!(stage.operations.len(), 4);
        assert_eq!(
            stage
                .operations
                .iter()
                .map(|operation| operation.statement_id)
                .collect::<Vec<_>>(),
            vec![
                StatementId::PaymentUpdateBadCustomer.wire_id(),
                StatementId::PaymentInsertHistory.wire_id(),
                StatementId::PaymentCustomerAfter.wire_id(),
                StatementId::Commit.wire_id(),
            ]
        );
        assert_eq!(stage.expected_customer_data.len(), 50);
        assert!(stage.customer_is_bad_credit);
        assert_eq!(stage.history_data, b"WAREHOUSE    DISTRICT");
        assert!(stage
            .expected_customer_data
            .starts_with(b"321 7 50 4 3 5000.00 "));
        assert_eq!(
            stage.operations[0].parameters,
            vec![
                WireValue::Float32(amount_bits),
                WireValue::Char(stage.expected_customer_data.clone()),
                WireValue::Int32(50),
                WireValue::Int32(7),
                WireValue::Int32(321),
            ]
        );
        assert_eq!(
            stage.operations[1].parameters,
            vec![
                WireValue::Int32(321),
                WireValue::Int32(7),
                WireValue::Int32(50),
                WireValue::Int32(4),
                WireValue::Int32(3),
                WireValue::Char(b"2026-07-29 12:34:56".to_vec()),
                WireValue::Float32(amount_bits),
                WireValue::Char(b"WAREHOUSE    DISTRICT".to_vec()),
            ]
        );
        assert_eq!(
            stage.operations[STAGE_TWO_CUSTOMER_AFTER].parameters,
            vec![
                WireValue::Int32(50),
                WireValue::Int32(7),
                WireValue::Int32(321),
            ]
        );
    }

    #[test]
    fn post_commit_validation_keeps_good_credit_data_and_never_requests_abort() {
        let snapshot = snapshot(CustomerCredit::Good);
        let amount_bits = 1.0_f32.to_bits();
        let stage = build_stage_two(
            3,
            4,
            3,
            4,
            100,
            amount_bits,
            "2026-07-29 12:34:56",
            &snapshot,
        )
        .unwrap();
        assert_eq!(stage.expected_customer_data, snapshot.customer.data);
        assert!(!stage.customer_is_bad_credit);
        let results = result(
            &stage.operations,
            [(
                2,
                vec![vec![
                    WireValue::Float32((-11.0_f32).to_bits()),
                    WireValue::Float32(11.0_f32.to_bits()),
                    WireValue::Int32(2),
                    WireValue::Int32(0),
                    WireValue::Char(stage.expected_customer_data.clone()),
                ]],
            )],
        );
        validate_stage_two(
            &results,
            amount_bits,
            &snapshot,
            &stage.expected_customer_data,
        )
        .unwrap();

        let wrong = result(
            &stage.operations,
            [(
                2,
                vec![vec![
                    WireValue::Float32((-11.0_f32).to_bits().wrapping_add(1)),
                    WireValue::Float32(11.0_f32.to_bits()),
                    WireValue::Int32(2),
                    WireValue::Int32(0),
                    WireValue::Char(stage.expected_customer_data.clone()),
                ]],
            )],
        );
        let error = validate_stage_two(
            &wrong,
            amount_bits,
            &snapshot,
            &stage.expected_customer_data,
        )
        .unwrap_err();
        assert!(!error.requires_explicit_abort());

        let changed_delivery_count = result(
            &stage.operations,
            [(
                2,
                vec![vec![
                    WireValue::Float32((-11.0_f32).to_bits()),
                    WireValue::Float32(11.0_f32.to_bits()),
                    WireValue::Int32(2),
                    WireValue::Int32(1),
                    WireValue::Char(stage.expected_customer_data.clone()),
                ]],
            )],
        );
        assert!(validate_stage_two(
            &changed_delivery_count,
            amount_bits,
            &snapshot,
            &stage.expected_customer_data,
        )
        .is_err());
    }
}
