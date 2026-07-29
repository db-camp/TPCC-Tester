//! Public-spec TPC-C consistency plans and pure result validation.
//!
//! This module deliberately does not contain a database connection.  Callers
//! execute [`CheckQuery::sql`] through any transport, translate the result to
//! [`TypedResult`], and then call [`CheckQuery::validate`].
//!
//! The final-2026 statement publishes consistency *semantics*, but withholds
//! the official 37 integer SQL statements, their generated identifiers, keys,
//! seed, and answers.  The plans below exercise every published invariant and
//! are therefore suitable for local regression; they are not represented as a
//! clone of that hidden checker.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const FINAL_WAREHOUSES: i32 = 50;
pub const DISTRICTS_PER_WAREHOUSE: i32 = 10;
pub const CUSTOMERS_PER_DISTRICT: i64 = 3_000;
pub const ORDERS_PER_DISTRICT: i64 = 3_000;
pub const NEW_ORDERS_PER_DISTRICT: i64 = 900;
pub const ITEMS: i64 = 100_000;
pub const PUBLIC_RECOVERY_INTEGER_CHECK_COUNT: usize = 37;

/// Makes the public/hidden boundary available to reports and command help.
pub const PUBLIC_SPEC_NOTICE: &str = "public-spec consistency plan; the official 37 integer \
    SQL statements, generated identifiers, sampled keys, seed, and answers are not public";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckScope {
    Setup,
    Online,
    Recovery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypedValue {
    Null,
    Int32(i32),
    Float32(u32),
    Char(Vec<u8>),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TypedResult {
    pub rows: Vec<Vec<TypedValue>>,
}

impl TypedResult {
    pub fn scalar(value: TypedValue) -> Self {
        Self {
            rows: vec![vec![value]],
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum ScalarExpectation {
    ExactInt(i64),
    ExactFloat32 { bits: u32, max_ulps: u32 },
    FiniteFloat32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CheckQuery {
    pub id: String,
    pub scope: CheckScope,
    pub description: String,
    pub sql: String,
    pub expectation: ScalarExpectation,
}

impl CheckQuery {
    pub fn validate(&self, result: &TypedResult) -> Result<(), ValidationError> {
        if result.rows.len() != 1 || result.rows[0].len() != 1 {
            return Err(ValidationError::Shape {
                check_id: self.id.clone(),
                rows: result.rows.len(),
                first_row_columns: result.rows.first().map_or(0, Vec::len),
            });
        }

        let actual = &result.rows[0][0];
        match (&self.expectation, actual) {
            (ScalarExpectation::ExactInt(expected), TypedValue::Int32(actual)) => {
                if i64::from(*actual) == *expected {
                    Ok(())
                } else {
                    Err(ValidationError::IntegerMismatch {
                        check_id: self.id.clone(),
                        expected: *expected,
                        actual: i64::from(*actual),
                    })
                }
            }
            (
                ScalarExpectation::ExactFloat32 {
                    bits: expected,
                    max_ulps,
                },
                TypedValue::Float32(actual),
            ) => {
                if float32_matches(*expected, *actual, *max_ulps) {
                    Ok(())
                } else {
                    Err(ValidationError::FloatMismatch {
                        check_id: self.id.clone(),
                        expected_bits: *expected,
                        actual_bits: *actual,
                        max_ulps: *max_ulps,
                    })
                }
            }
            (ScalarExpectation::FiniteFloat32, TypedValue::Float32(bits)) => {
                if f32::from_bits(*bits).is_finite() {
                    Ok(())
                } else {
                    Err(ValidationError::NonFiniteFloat {
                        check_id: self.id.clone(),
                        bits: *bits,
                    })
                }
            }
            (expected, actual) => Err(ValidationError::Type {
                check_id: self.id.clone(),
                expected: expected.type_name(),
                actual: typed_value_name(actual),
            }),
        }
    }
}

impl ScalarExpectation {
    fn type_name(&self) -> &'static str {
        match self {
            Self::ExactInt(_) => "INT32",
            Self::ExactFloat32 { .. } | Self::FiniteFloat32 => "FLOAT32",
        }
    }
}

fn typed_value_name(value: &TypedValue) -> &'static str {
    match value {
        TypedValue::Null => "NULL",
        TypedValue::Int32(_) => "INT32",
        TypedValue::Float32(_) => "FLOAT32",
        TypedValue::Char(_) => "CHAR",
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConsistencyPlan {
    pub queries: Vec<CheckQuery>,
}

impl ConsistencyPlan {
    pub fn extend(&mut self, other: Self) {
        self.queries.extend(other.queries);
    }
}

/// Logical-to-runtime identifier mapping used by opaque-schema local runs.
///
/// Plans are authored against the public logical schema. Rendering performs
/// token-aware replacement outside CHAR literals, so opaque runtime names do
/// not accidentally rewrite substrings or string values.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IdentifierMap {
    names: BTreeMap<String, String>,
}

impl IdentifierMap {
    pub fn insert(
        &mut self,
        logical: impl Into<String>,
        runtime: impl Into<String>,
    ) -> Result<(), IdentifierError> {
        let logical = logical.into();
        let runtime = runtime.into();
        if !is_sql_identifier(&logical) {
            return Err(IdentifierError::InvalidLogical(logical));
        }
        if !is_sql_identifier(&runtime) {
            return Err(IdentifierError::InvalidRuntime(runtime));
        }
        self.names.insert(logical, runtime);
        Ok(())
    }

    pub fn render_query(&self, query: &CheckQuery) -> CheckQuery {
        let mut rendered = query.clone();
        rendered.sql = rewrite_identifiers(&query.sql, &self.names);
        rendered
    }

    pub fn render_plan(&self, plan: &ConsistencyPlan) -> ConsistencyPlan {
        ConsistencyPlan {
            queries: plan
                .queries
                .iter()
                .map(|query| self.render_query(query))
                .collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentifierError {
    InvalidLogical(String),
    InvalidRuntime(String),
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLogical(value) => write!(f, "invalid logical SQL identifier {value:?}"),
            Self::InvalidRuntime(value) => write!(f, "invalid runtime SQL identifier {value:?}"),
        }
    }
}

impl std::error::Error for IdentifierError {}

fn is_sql_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn rewrite_identifiers(sql: &str, names: &BTreeMap<String, String>) -> String {
    let bytes = sql.as_bytes();
    let mut rendered = String::with_capacity(sql.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\'' {
            let start = index;
            index += 1;
            while index < bytes.len() {
                if bytes[index] == b'\'' {
                    index += 1;
                    if index < bytes.len() && bytes[index] == b'\'' {
                        index += 1;
                        continue;
                    }
                    break;
                }
                index += 1;
            }
            rendered.push_str(&sql[start..index]);
            continue;
        }

        if bytes[index].is_ascii_alphabetic() || bytes[index] == b'_' {
            let start = index;
            index += 1;
            while index < bytes.len()
                && (bytes[index].is_ascii_alphanumeric() || bytes[index] == b'_')
            {
                index += 1;
            }
            let token = &sql[start..index];
            rendered.push_str(names.get(token).map_or(token, String::as_str));
            continue;
        }

        rendered.push(bytes[index] as char);
        index += 1;
    }
    rendered
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    Shape {
        check_id: String,
        rows: usize,
        first_row_columns: usize,
    },
    Type {
        check_id: String,
        expected: &'static str,
        actual: &'static str,
    },
    IntegerMismatch {
        check_id: String,
        expected: i64,
        actual: i64,
    },
    FloatMismatch {
        check_id: String,
        expected_bits: u32,
        actual_bits: u32,
        max_ulps: u32,
    },
    NonFiniteFloat {
        check_id: String,
        bits: u32,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Shape {
                check_id,
                rows,
                first_row_columns,
            } => write!(
                f,
                "{check_id}: expected one scalar row, got {rows} rows and \
                 {first_row_columns} columns in the first row"
            ),
            Self::Type {
                check_id,
                expected,
                actual,
            } => write!(f, "{check_id}: expected {expected}, got {actual}"),
            Self::IntegerMismatch {
                check_id,
                expected,
                actual,
            } => write!(f, "{check_id}: expected {expected}, got {actual}"),
            Self::FloatMismatch {
                check_id,
                expected_bits,
                actual_bits,
                max_ulps,
            } => write!(
                f,
                "{check_id}: expected 0x{expected_bits:08x}, got \
                 0x{actual_bits:08x}, tolerance {max_ulps} ULP"
            ),
            Self::NonFiniteFloat { check_id, bits } => {
                write!(f, "{check_id}: non-finite FLOAT32 0x{bits:08x}")
            }
        }
    }
}

impl std::error::Error for ValidationError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlanError {
    NonPositiveWarehouseCount(i32),
    WarehouseCountExceedsPublicMaximum { actual: i32, maximum: i32 },
    NegativeCount(&'static str),
    ArithmeticOverflow(&'static str),
    InvalidOnlineSample(&'static str),
    InvalidPartitionCount { expected: usize, actual: usize },
    InvalidPartitionKey(PartitionKey),
    DuplicatePartition(PartitionKey),
    MissingPartition(PartitionKey),
    InconsistentLedger(&'static str),
    InvalidRecoveryIntegerCheckCount { expected: usize, actual: usize },
    DuplicateRecoveryCheckId(String),
    InvalidRecoveryIntegerCheck(String),
    IntegerExpectationOutOfRange { check_id: String, expected: i64 },
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonPositiveWarehouseCount(value) => {
                write!(f, "warehouse count must be positive, got {value}")
            }
            Self::WarehouseCountExceedsPublicMaximum { actual, maximum } => write!(
                f,
                "warehouse count {actual} exceeds public maximum {maximum}"
            ),
            Self::NegativeCount(name) => write!(f, "{name} must not be negative"),
            Self::ArithmeticOverflow(name) => write!(f, "{name} overflowed i64"),
            Self::InvalidOnlineSample(message) => {
                write!(f, "invalid online setup-evidence sample: {message}")
            }
            Self::InvalidPartitionCount { expected, actual } => write!(
                f,
                "expected {expected} partition expectations, got {actual}"
            ),
            Self::InvalidPartitionKey(key) => {
                write!(
                    f,
                    "invalid partition ({}, {})",
                    key.warehouse_id, key.district_id
                )
            }
            Self::DuplicatePartition(key) => write!(
                f,
                "duplicate partition ({}, {})",
                key.warehouse_id, key.district_id
            ),
            Self::MissingPartition(key) => write!(
                f,
                "missing partition ({}, {})",
                key.warehouse_id, key.district_id
            ),
            Self::InconsistentLedger(message) => write!(f, "inconsistent ledger: {message}"),
            Self::InvalidRecoveryIntegerCheckCount { expected, actual } => write!(
                f,
                "public recovery integer gate requires exactly {expected} checks, got {actual}"
            ),
            Self::DuplicateRecoveryCheckId(check_id) => {
                write!(f, "duplicate public recovery integer check id {check_id:?}")
            }
            Self::InvalidRecoveryIntegerCheck(check_id) => write!(
                f,
                "public recovery check {check_id:?} is not a Recovery ExactInt scalar"
            ),
            Self::IntegerExpectationOutOfRange { check_id, expected } => write!(
                f,
                "public recovery check {check_id:?} expectation {expected} is outside INT32"
            ),
        }
    }
}

impl std::error::Error for PlanError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetupExpectations {
    pub warehouses: i32,
    pub order_line_rows: i64,
    pub undelivered_order_line_rows: i64,
}

impl SetupExpectations {
    pub fn final_2026(order_line_rows: i64, undelivered_order_line_rows: i64) -> Self {
        Self {
            warehouses: FINAL_WAREHOUSES,
            order_line_rows,
            undelivered_order_line_rows,
        }
    }
}

/// Generate all published initial-state checks.
///
/// `order_line_rows` and `undelivered_order_line_rows` are run-specific:
/// final-2026 generates 5..=15 lines independently for every initial order.
pub fn setup_plan(input: SetupExpectations) -> Result<ConsistencyPlan, PlanError> {
    let counts = validate_setup_expectations(input)?;
    let initial_new_orders = counts["new_orders"];
    let initial_orders = counts["orders"];
    let initial_stock = counts["stock"];

    let mut plan = ConsistencyPlan::default();
    for (table, expected) in &counts {
        plan.queries.push(int_query(
            CheckScope::Setup,
            format!("setup.count.{table}"),
            format!("initial {table} row count"),
            format!("SELECT COUNT(*) FROM {table}"),
            *expected,
        ));
    }

    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.orders.sum_o_ol_cnt",
        "orders SUM(o_ol_cnt) equals the generated order_line row count",
        "SELECT SUM(o_ol_cnt) FROM orders",
        input.order_line_rows,
    ));
    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.stock.quantity_range",
        "every initial stock quantity is in 10..=100",
        "SELECT COUNT(*) FROM stock WHERE s_quantity >= 10 AND s_quantity <= 100",
        initial_stock,
    ));
    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.orders.line_count_range",
        "every initial order has 5..=15 lines",
        "SELECT COUNT(*) FROM orders WHERE o_ol_cnt >= 5 AND o_ol_cnt <= 15",
        initial_orders,
    ));
    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.order_line.quantity",
        "every initial order line has quantity 5",
        "SELECT COUNT(*) FROM order_line WHERE ol_quantity = 5",
        input.order_line_rows,
    ));
    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.orders.carrier_range",
        "every carrier id is in 0..=10",
        "SELECT COUNT(*) FROM orders WHERE o_carrier_id >= 0 AND o_carrier_id <= 10",
        initial_orders,
    ));
    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.orders.open_carrier_count",
        "carrier-id zero orders equal the initial new_orders queue",
        "SELECT COUNT(*) FROM orders WHERE o_carrier_id = 0",
        initial_new_orders,
    ));
    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.order_line.undelivered_count",
        "empty delivery timestamps equal the generated undelivered-line count",
        "SELECT COUNT(*) FROM order_line WHERE ol_delivery_d = ''",
        input.undelivered_order_line_rows,
    ));
    plan.queries.push(float_query(
        CheckScope::Setup,
        "setup.stock.sum_ytd",
        "initial SUM(s_ytd) is positive zero",
        "SELECT SUM(s_ytd) FROM stock",
        0.0_f32.to_bits(),
        0,
    ));
    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.stock.sum_order_cnt",
        "initial SUM(s_order_cnt) is zero",
        "SELECT SUM(s_order_cnt) FROM stock",
        0,
    ));
    plan.queries.push(int_query(
        CheckScope::Setup,
        "setup.stock.sum_remote_cnt",
        "initial SUM(s_remote_cnt) is zero",
        "SELECT SUM(s_remote_cnt) FROM stock",
        0,
    ));
    for (suffix, column) in [
        ("ytd", "s_ytd"),
        ("order_cnt", "s_order_cnt"),
        ("remote_cnt", "s_remote_cnt"),
    ] {
        plan.queries.push(int_query(
            CheckScope::Setup,
            format!("setup.stock.nonzero_{suffix}"),
            format!("every initial stock.{column} value is zero"),
            format!("SELECT COUNT(*) FROM stock WHERE {column} = 0"),
            initial_stock,
        ));
    }
    add_key_range_checks(&mut plan, CheckScope::Setup, input.warehouses, &counts);
    Ok(plan)
}

fn validate_setup_expectations(
    input: SetupExpectations,
) -> Result<BTreeMap<&'static str, i64>, PlanError> {
    validate_non_negative("order_line_rows", input.order_line_rows)?;
    validate_non_negative(
        "undelivered_order_line_rows",
        input.undelivered_order_line_rows,
    )?;
    let counts = initial_counts(input.warehouses, input.order_line_rows)?;
    let orders = counts["orders"];
    let initial_new_orders = counts["new_orders"];
    if !(checked_mul(orders, 5, "minimum initial order-line count")?
        ..=checked_mul(orders, 15, "maximum initial order-line count")?)
        .contains(&input.order_line_rows)
    {
        return Err(PlanError::InconsistentLedger(
            "initial order_line rows must be 5..=15 per order",
        ));
    }
    if !(checked_mul(
        initial_new_orders,
        5,
        "minimum undelivered order-line count",
    )?
        ..=checked_mul(
            initial_new_orders,
            15,
            "maximum undelivered order-line count",
        )?)
        .contains(&input.undelivered_order_line_rows)
    {
        return Err(PlanError::InconsistentLedger(
            "undelivered order_line rows must be 5..=15 per initial new_order",
        ));
    }
    Ok(counts)
}

fn initial_counts(
    warehouses: i32,
    order_line_rows: i64,
) -> Result<BTreeMap<&'static str, i64>, PlanError> {
    if warehouses <= 0 {
        return Err(PlanError::NonPositiveWarehouseCount(warehouses));
    }
    validate_non_negative("order_line_rows", order_line_rows)?;

    let w = i64::from(warehouses);
    let districts = checked_mul(w, i64::from(DISTRICTS_PER_WAREHOUSE), "district count")?;
    let customers = checked_mul(
        districts,
        CUSTOMERS_PER_DISTRICT,
        "customer/history/order count",
    )?;
    let new_orders = checked_mul(districts, NEW_ORDERS_PER_DISTRICT, "new_orders count")?;
    let stock = checked_mul(w, ITEMS, "stock count")?;

    Ok(BTreeMap::from([
        ("warehouse", w),
        ("district", districts),
        ("customer", customers),
        ("history", customers),
        ("orders", customers),
        ("new_orders", new_orders),
        ("order_line", order_line_rows),
        ("item", ITEMS),
        ("stock", stock),
    ]))
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CommittedLedger {
    pub new_orders: i64,
    pub new_order_lines: i64,
    pub remote_new_order_lines: i64,
    pub stock_ytd_delta: i64,
    pub payments: i64,
    pub delivered_orders: i64,
    pub delivered_order_lines: i64,
}

/// Public recovery expectations derived only from committed transaction counts.
///
/// The official checker retains additional hidden per-transaction evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryExpectations {
    pub setup: SetupExpectations,
    pub committed: CommittedLedger,
}

/// One coherent set of logical keys selected from persisted setup evidence.
///
/// This is a local public-spec approximation only. The official six online
/// statements and their generated keys remain hidden.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OnlineKeySample {
    pub item_id: i32,
    pub customer_warehouse_id: i32,
    pub customer_district_id: i32,
    pub customer_id: i32,
    pub stock_warehouse_id: i32,
    pub stock_item_id: i32,
}

impl OnlineKeySample {
    fn validate(self, warehouses: i32) -> Result<(), PlanError> {
        if !(1..=ITEMS as i32).contains(&self.item_id) {
            return Err(PlanError::InvalidOnlineSample(
                "item id is outside the public key range",
            ));
        }
        if !(1..=warehouses).contains(&self.customer_warehouse_id)
            || !(1..=DISTRICTS_PER_WAREHOUSE).contains(&self.customer_district_id)
            || !(1..=CUSTOMERS_PER_DISTRICT as i32).contains(&self.customer_id)
        {
            return Err(PlanError::InvalidOnlineSample(
                "customer key is outside the dataset keyspace",
            ));
        }
        if !(1..=warehouses).contains(&self.stock_warehouse_id)
            || !(1..=ITEMS as i32).contains(&self.stock_item_id)
        {
            return Err(PlanError::InvalidOnlineSample(
                "stock key is outside the dataset keyspace",
            ));
        }
        if self.item_id != self.stock_item_id
            || self.customer_warehouse_id != self.stock_warehouse_id
        {
            return Err(PlanError::InvalidOnlineSample(
                "item, customer, and stock keys are not one setup-evidence relationship",
            ));
        }
        Ok(())
    }
}

impl RecoveryExpectations {
    fn expected_counts(self) -> Result<BTreeMap<&'static str, i64>, PlanError> {
        for (name, value) in [
            ("committed.new_orders", self.committed.new_orders),
            ("committed.new_order_lines", self.committed.new_order_lines),
            (
                "committed.remote_new_order_lines",
                self.committed.remote_new_order_lines,
            ),
            ("committed.stock_ytd_delta", self.committed.stock_ytd_delta),
            ("committed.payments", self.committed.payments),
            (
                "committed.delivered_orders",
                self.committed.delivered_orders,
            ),
            (
                "committed.delivered_order_lines",
                self.committed.delivered_order_lines,
            ),
        ] {
            validate_non_negative(name, value)?;
        }
        if self.committed.remote_new_order_lines > self.committed.new_order_lines {
            return Err(PlanError::InconsistentLedger(
                "remote NewOrder lines exceed all committed NewOrder lines",
            ));
        }
        let minimum_lines = checked_mul(
            self.committed.new_orders,
            5,
            "minimum committed NewOrder lines",
        )?;
        let maximum_lines = checked_mul(
            self.committed.new_orders,
            15,
            "maximum committed NewOrder lines",
        )?;
        if !(minimum_lines..=maximum_lines).contains(&self.committed.new_order_lines) {
            return Err(PlanError::InconsistentLedger(
                "committed NewOrder lines must be 5..=15 per committed NewOrder",
            ));
        }
        let maximum_stock_ytd_delta = checked_mul(
            self.committed.new_order_lines,
            10,
            "maximum committed stock YTD delta",
        )?;
        if !(self.committed.new_order_lines..=maximum_stock_ytd_delta)
            .contains(&self.committed.stock_ytd_delta)
        {
            return Err(PlanError::InconsistentLedger(
                "committed stock YTD delta must equal 1..=10 per committed NewOrder line",
            ));
        }
        let minimum_delivered_lines = checked_mul(
            self.committed.delivered_orders,
            5,
            "minimum delivered order lines",
        )?;
        let maximum_delivered_lines = checked_mul(
            self.committed.delivered_orders,
            15,
            "maximum delivered order lines",
        )?;
        if !(minimum_delivered_lines..=maximum_delivered_lines)
            .contains(&self.committed.delivered_order_lines)
        {
            return Err(PlanError::InconsistentLedger(
                "delivered order lines must be 5..=15 per processed order",
            ));
        }
        let available_undelivered_lines = checked_add(
            self.setup.undelivered_order_line_rows,
            self.committed.new_order_lines,
            "available undelivered order lines",
        )?;
        if self.committed.delivered_order_lines > available_undelivered_lines {
            return Err(PlanError::InconsistentLedger(
                "delivered order lines exceed the visible undelivered-line pool",
            ));
        }

        let mut counts = validate_setup_expectations(self.setup)?;
        add_to_count(&mut counts, "orders", self.committed.new_orders)?;
        add_to_count(&mut counts, "order_line", self.committed.new_order_lines)?;
        add_to_count(&mut counts, "history", self.committed.payments)?;
        add_to_count(&mut counts, "new_orders", self.committed.new_orders)?;
        add_to_count(&mut counts, "new_orders", -self.committed.delivered_orders)?;
        if counts["new_orders"] < 0 {
            return Err(PlanError::NegativeCount("recovery new_orders count"));
        }
        Ok(counts)
    }
}

/// Generate exactly 37 public-spec integer recovery checks.
///
/// This is an independently derived, replayable local gate. The official SQL,
/// generated identifiers, keys, seed, and answers are withheld, so this plan
/// must not be represented as a clone of the official checker. The separate
/// 500-partition audit and seven FLOAT32 checks are not counted here.
pub fn recovery_plan(input: RecoveryExpectations) -> Result<ConsistencyPlan, PlanError> {
    let counts = input.expected_counts()?;
    let mut plan = ConsistencyPlan::default();

    for (table, expected) in &counts {
        plan.queries.push(int_query(
            CheckScope::Recovery,
            format!("recovery.count.{table}"),
            format!("post-recovery {table} row count from committed ledger"),
            format!("SELECT COUNT(*) FROM {table}"),
            *expected,
        ));
    }

    let order_line_rows = counts["order_line"];
    let queued_orders = counts["new_orders"];
    let empty_delivery_rows = checked_add(
        input.setup.undelivered_order_line_rows,
        input.committed.new_order_lines,
        "recovery undelivered order-line count",
    )?
    .checked_sub(input.committed.delivered_order_lines)
    .ok_or(PlanError::ArithmeticOverflow(
        "recovery undelivered order-line count",
    ))?;
    let partition_count = i64::from(input.setup.warehouses) * i64::from(DISTRICTS_PER_WAREHOUSE);
    let initial_customers = checked_mul(
        partition_count,
        CUSTOMERS_PER_DISTRICT,
        "initial customer count",
    )?;
    let initial_next_order_sum = checked_mul(
        partition_count,
        ORDERS_PER_DISTRICT + 1,
        "initial d_next_o_id sum",
    )?;
    let initial_line_quantity = checked_mul(
        input.setup.order_line_rows,
        5,
        "initial order-line quantity sum",
    )?;
    let expected_line_quantity = checked_add(
        initial_line_quantity,
        input.committed.stock_ytd_delta,
        "recovery order-line quantity sum",
    )?;
    let maximum_order_id = checked_add(
        ORDERS_PER_DISTRICT,
        input.committed.new_orders,
        "maximum recovery order id",
    )?;
    let maximum_next_order_id = checked_add(
        maximum_order_id,
        1,
        "maximum recovery district next order id",
    )?;
    let maximum_customer_payment_count = checked_add(
        1,
        input.committed.payments,
        "maximum customer payment count",
    )?;
    for (name, value) in [
        ("recovery.bound.maximum_order_id", maximum_order_id),
        (
            "recovery.bound.maximum_next_order_id",
            maximum_next_order_id,
        ),
        (
            "recovery.bound.maximum_customer_payment_count",
            maximum_customer_payment_count,
        ),
        (
            "recovery.bound.maximum_customer_delivery_count",
            input.committed.delivered_orders,
        ),
        (
            "recovery.bound.maximum_stock_order_count",
            input.committed.new_order_lines,
        ),
        (
            "recovery.bound.maximum_stock_remote_count",
            input.committed.remote_new_order_lines,
        ),
    ] {
        require_int32(name, value)?;
    }

    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.orders.sum_o_ol_cnt",
        "SUM(o_ol_cnt) equals all visible order_line rows",
        "SELECT SUM(o_ol_cnt) FROM orders",
        order_line_rows,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.order_line.sum_quantity",
        "SUM(ol_quantity) equals initial quantity plus committed NewOrder quantities",
        "SELECT SUM(ol_quantity) FROM order_line",
        expected_line_quantity,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.orders.open_carrier_count",
        "carrier-id zero orders equal the visible new_orders queue",
        "SELECT COUNT(*) FROM orders WHERE o_carrier_id = 0",
        queued_orders,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.order_line.empty_delivery_time_count",
        "empty delivery timestamps match committed NewOrder and Delivery line counts",
        "SELECT COUNT(*) FROM order_line WHERE ol_delivery_d = ''",
        empty_delivery_rows,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.district.sum_next_order_id",
        "district next-order totals include every committed NewOrder",
        "SELECT SUM(d_next_o_id) FROM district",
        checked_add(
            initial_next_order_sum,
            input.committed.new_orders,
            "recovery d_next_o_id sum",
        )?,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.customer.sum_payment_cnt",
        "customer payment counts include every committed Payment",
        "SELECT SUM(c_payment_cnt) FROM customer",
        checked_add(
            initial_customers,
            input.committed.payments,
            "recovery customer payment count",
        )?,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.customer.sum_delivery_cnt",
        "customer delivery counts include every processed queued order",
        "SELECT SUM(c_delivery_cnt) FROM customer",
        input.committed.delivered_orders,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.stock.sum_order_cnt",
        "stock order counts include every committed NewOrder line",
        "SELECT SUM(s_order_cnt) FROM stock",
        input.committed.new_order_lines,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.stock.sum_remote_cnt",
        "stock remote counts include every committed remote-supply line",
        "SELECT SUM(s_remote_cnt) FROM stock",
        input.committed.remote_new_order_lines,
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.stock.quantity_range",
        "post-load stock quantity remains in the TPC-C update range 10..=100",
        "SELECT COUNT(*) FROM stock WHERE s_quantity >= 10 AND s_quantity <= 100",
        counts["stock"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.orders.line_count_range",
        "every order still has 5..=15 lines",
        "SELECT COUNT(*) FROM orders WHERE o_ol_cnt >= 5 AND o_ol_cnt <= 15",
        counts["orders"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.order_line.quantity_range",
        "every order-line quantity remains in 1..=10",
        "SELECT COUNT(*) FROM order_line WHERE ol_quantity >= 1 AND ol_quantity <= 10",
        counts["order_line"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.orders.carrier_range",
        "every carrier id remains in 0..=10",
        "SELECT COUNT(*) FROM orders WHERE o_carrier_id >= 0 AND o_carrier_id <= 10",
        counts["orders"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.orders.all_local_range",
        "every order all-local flag remains Boolean",
        "SELECT COUNT(*) FROM orders WHERE o_all_local >= 0 AND o_all_local <= 1",
        counts["orders"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.stock.counter_range",
        "stock counters are non-negative, bounded by the ledger, and remote never exceeds order",
        format!(
            "SELECT COUNT(*) FROM stock WHERE s_order_cnt >= 0 \
             AND s_order_cnt <= {} AND s_remote_cnt >= 0 \
             AND s_remote_cnt <= {} AND s_remote_cnt <= s_order_cnt",
            input.committed.new_order_lines, input.committed.remote_new_order_lines
        ),
        counts["stock"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.customer.counter_range",
        "customer counters retain their initial lower bounds and ledger-derived upper bounds",
        format!(
            "SELECT COUNT(*) FROM customer WHERE c_payment_cnt >= 1 \
             AND c_payment_cnt <= {maximum_customer_payment_count} \
             AND c_delivery_cnt >= 0 AND c_delivery_cnt <= {}",
            input.committed.delivered_orders
        ),
        counts["customer"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.district.next_order_id_range",
        "district next-order ids stay within the initial and committed allocation domain",
        format!(
            "SELECT COUNT(*) FROM district WHERE d_next_o_id >= {} \
             AND d_next_o_id <= {}",
            ORDERS_PER_DISTRICT + 1,
            maximum_next_order_id
        ),
        counts["district"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.warehouse.key_range",
        "warehouse identifiers remain in the dataset keyspace",
        format!(
            "SELECT COUNT(*) FROM warehouse WHERE w_id >= 1 AND w_id <= {}",
            input.setup.warehouses
        ),
        counts["warehouse"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.item.key_range",
        "item identifiers remain in the public keyspace",
        format!("SELECT COUNT(*) FROM item WHERE i_id >= 1 AND i_id <= {ITEMS}"),
        counts["item"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.history.key_range",
        "history customer and owning partition identifiers remain in range",
        format!(
            "SELECT COUNT(*) FROM history WHERE h_c_id >= 1 \
             AND h_c_id <= {CUSTOMERS_PER_DISTRICT} AND h_c_d_id >= 1 \
             AND h_c_d_id <= {DISTRICTS_PER_WAREHOUSE} AND h_c_w_id >= 1 \
             AND h_c_w_id <= {} AND h_d_id >= 1 \
             AND h_d_id <= {DISTRICTS_PER_WAREHOUSE} AND h_w_id >= 1 \
             AND h_w_id <= {}",
            input.setup.warehouses, input.setup.warehouses
        ),
        counts["history"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.new_orders.key_range",
        "new-order queue keys remain in the dataset and allocated-order domain",
        format!(
            "SELECT COUNT(*) FROM new_orders WHERE no_w_id >= 1 \
             AND no_w_id <= {} AND no_d_id >= 1 \
             AND no_d_id <= {DISTRICTS_PER_WAREHOUSE} AND no_o_id >= 1 \
             AND no_o_id <= {maximum_order_id}",
            input.setup.warehouses
        ),
        counts["new_orders"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.orders.order_id_range",
        "order identifiers remain in the allocated-order domain",
        format!("SELECT COUNT(*) FROM orders WHERE o_id >= 1 AND o_id <= {maximum_order_id}"),
        counts["orders"],
    ));
    plan.queries.push(int_query(
        CheckScope::Recovery,
        "recovery.order_line.order_key_range",
        "order-line order and line identifiers remain in their legal domains",
        format!(
            "SELECT COUNT(*) FROM order_line WHERE ol_o_id >= 1 \
             AND ol_o_id <= {maximum_order_id} AND ol_number >= 1 \
             AND ol_number <= 15"
        ),
        counts["order_line"],
    ));
    add_key_range_checks(
        &mut plan,
        CheckScope::Recovery,
        input.setup.warehouses,
        &counts,
    );
    validate_public_recovery_integer_gate(&plan)?;
    Ok(plan)
}

pub fn validate_public_recovery_integer_gate(plan: &ConsistencyPlan) -> Result<(), PlanError> {
    if plan.queries.len() != PUBLIC_RECOVERY_INTEGER_CHECK_COUNT {
        return Err(PlanError::InvalidRecoveryIntegerCheckCount {
            expected: PUBLIC_RECOVERY_INTEGER_CHECK_COUNT,
            actual: plan.queries.len(),
        });
    }

    let mut ids = BTreeSet::new();
    for query in &plan.queries {
        if query.scope != CheckScope::Recovery
            || !query.id.starts_with("recovery.")
            || query.id.starts_with("recovery.partition.")
            || query.sql.contains(" OR ")
            || query.sql.contains(';')
            || !(query.sql.starts_with("SELECT COUNT(*) ") || query.sql.starts_with("SELECT SUM("))
        {
            return Err(PlanError::InvalidRecoveryIntegerCheck(query.id.clone()));
        }
        if !ids.insert(query.id.clone()) {
            return Err(PlanError::DuplicateRecoveryCheckId(query.id.clone()));
        }
        match query.expectation {
            ScalarExpectation::ExactInt(expected) => {
                require_int32(&query.id, expected)?;
            }
            _ => return Err(PlanError::InvalidRecoveryIntegerCheck(query.id.clone())),
        }
    }
    Ok(())
}

/// A six-query, public-semantic online plan.
///
/// The official six integer SQL statements are hidden.  This local fast plan
/// confines itself to small relations or exact index-key probes and should not
/// be labelled as the official query set.
pub fn public_online_integer_plan(
    input: RecoveryExpectations,
    sample: OnlineKeySample,
) -> Result<ConsistencyPlan, PlanError> {
    let counts = input.expected_counts()?;
    sample.validate(input.setup.warehouses)?;
    let partition_count = i64::from(input.setup.warehouses) * i64::from(DISTRICTS_PER_WAREHOUSE);
    let initial_next_order_sum = checked_mul(
        partition_count,
        ORDERS_PER_DISTRICT + 1,
        "initial d_next_o_id sum",
    )?;
    let expected_next_sum = checked_add(
        initial_next_order_sum,
        input.committed.new_orders,
        "online d_next_o_id sum",
    )?;

    Ok(ConsistencyPlan {
        queries: vec![
            int_query(
                CheckScope::Online,
                "online.public.warehouse_count",
                "warehouse cardinality",
                "SELECT COUNT(*) FROM warehouse",
                counts["warehouse"],
            ),
            int_query(
                CheckScope::Online,
                "online.public.district_count",
                "district cardinality",
                "SELECT COUNT(*) FROM district",
                counts["district"],
            ),
            int_query(
                CheckScope::Online,
                "online.public.district_next_sum",
                "district next-order total",
                "SELECT SUM(d_next_o_id) FROM district",
                expected_next_sum,
            ),
            int_query(
                CheckScope::Online,
                "online.public.item_key",
                "one persisted setup-evidence item index key remains visible",
                format!("SELECT COUNT(*) FROM item WHERE i_id = {}", sample.item_id),
                1,
            ),
            int_query(
                CheckScope::Online,
                "online.public.customer_key",
                "one persisted setup-evidence customer composite-index key remains visible",
                format!(
                    "SELECT COUNT(*) FROM customer WHERE c_w_id = {} AND c_d_id = {} AND c_id = {}",
                    sample.customer_warehouse_id, sample.customer_district_id, sample.customer_id
                ),
                1,
            ),
            int_query(
                CheckScope::Online,
                "online.public.stock_key",
                "one persisted setup-evidence stock composite-index key remains visible",
                format!(
                    "SELECT COUNT(*) FROM stock WHERE s_w_id = {} AND s_i_id = {}",
                    sample.stock_warehouse_id, sample.stock_item_id
                ),
                1,
            ),
        ],
    })
}

fn add_key_range_checks(
    plan: &mut ConsistencyPlan,
    scope: CheckScope,
    warehouses: i32,
    counts: &BTreeMap<&'static str, i64>,
) {
    let prefix = match scope {
        CheckScope::Setup => "setup",
        CheckScope::Online => "online",
        CheckScope::Recovery => "recovery",
    };
    for (table, suffix, description, sql) in [
        (
            "district",
            "district.key_range",
            "district warehouse and district identifiers are in range",
            format!(
                "SELECT COUNT(*) FROM district WHERE d_w_id >= 1 AND d_w_id <= {warehouses} \
                 AND d_id >= 1 AND d_id <= {DISTRICTS_PER_WAREHOUSE}"
            ),
        ),
        (
            "customer",
            "customer.key_range",
            "customer warehouse and district identifiers are in range",
            format!(
                "SELECT COUNT(*) FROM customer WHERE c_w_id >= 1 AND c_w_id <= {warehouses} \
                 AND c_d_id >= 1 AND c_d_id <= {DISTRICTS_PER_WAREHOUSE} \
                 AND c_id >= 1 AND c_id <= {CUSTOMERS_PER_DISTRICT}"
            ),
        ),
        (
            "orders",
            "orders.key_range",
            "order warehouse, district, and customer identifiers are in range",
            format!(
                "SELECT COUNT(*) FROM orders WHERE o_w_id >= 1 AND o_w_id <= {warehouses} \
                 AND o_d_id >= 1 AND o_d_id <= {DISTRICTS_PER_WAREHOUSE} \
                 AND o_c_id >= 1 AND o_c_id <= {CUSTOMERS_PER_DISTRICT}"
            ),
        ),
        (
            "order_line",
            "order_line.key_range",
            "order-line warehouse, district, item, and supply-warehouse identifiers are in range",
            format!(
                "SELECT COUNT(*) FROM order_line WHERE ol_w_id >= 1 AND ol_w_id <= {warehouses} \
                 AND ol_d_id >= 1 AND ol_d_id <= {DISTRICTS_PER_WAREHOUSE} \
                 AND ol_i_id >= 1 AND ol_i_id <= {ITEMS} \
                 AND ol_supply_w_id >= 1 AND ol_supply_w_id <= {warehouses}"
            ),
        ),
        (
            "stock",
            "stock.key_range",
            "stock warehouse and item identifiers are in range",
            format!(
                "SELECT COUNT(*) FROM stock WHERE s_w_id >= 1 AND s_w_id <= {warehouses} \
                 AND s_i_id >= 1 AND s_i_id <= {ITEMS}"
            ),
        ),
    ] {
        plan.queries.push(int_query(
            scope,
            format!("{prefix}.{suffix}"),
            description,
            sql,
            counts[table],
        ));
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PartitionKey {
    pub warehouse_id: i32,
    pub district_id: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PartitionExpectation {
    pub key: PartitionKey,
    pub order_count: i64,
    pub order_line_count: i64,
    pub new_order_count: i64,
    pub empty_delivery_time_count: i64,
    pub carrier_zero_count: i64,
    pub next_order_id: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PartitionAudit {
    pub key: PartitionKey,
    pub checks: Vec<CheckQuery>,
}

/// Generate the mandatory final-2026 local audit shape for all 50*10 partitions.
///
/// The expected values must come from the caller's committed transaction
/// ledger.  Every predicate carries both warehouse and district keys, and the
/// order-line/new-order relationship is never joined without warehouse.
pub fn recovery_partition_audits(
    expected: Vec<PartitionExpectation>,
) -> Result<Vec<PartitionAudit>, PlanError> {
    recovery_partition_audits_for_warehouses(FINAL_WAREHOUSES, expected)
}

/// Generate the same recovery audit for a public-scale non-ranked smoke run.
///
/// The official final-2026 wrapper remains fixed at 50 warehouses. This
/// scale-aware form exists only so smaller local smoke datasets validate their
/// complete keyspace instead of being rejected by the official fixed scale.
pub fn recovery_partition_audits_for_warehouses(
    warehouses: i32,
    expected: Vec<PartitionExpectation>,
) -> Result<Vec<PartitionAudit>, PlanError> {
    if warehouses <= 0 {
        return Err(PlanError::NonPositiveWarehouseCount(warehouses));
    }
    if warehouses > FINAL_WAREHOUSES {
        return Err(PlanError::WarehouseCountExceedsPublicMaximum {
            actual: warehouses,
            maximum: FINAL_WAREHOUSES,
        });
    }
    let required = (warehouses * DISTRICTS_PER_WAREHOUSE) as usize;
    if expected.len() != required {
        return Err(PlanError::InvalidPartitionCount {
            expected: required,
            actual: expected.len(),
        });
    }

    let mut by_key = BTreeMap::new();
    for item in expected {
        if !(1..=warehouses).contains(&item.key.warehouse_id)
            || !(1..=DISTRICTS_PER_WAREHOUSE).contains(&item.key.district_id)
        {
            return Err(PlanError::InvalidPartitionKey(item.key));
        }
        for (name, value) in [
            ("partition.order_count", item.order_count),
            ("partition.order_line_count", item.order_line_count),
            ("partition.new_order_count", item.new_order_count),
            (
                "partition.empty_delivery_time_count",
                item.empty_delivery_time_count,
            ),
            ("partition.carrier_zero_count", item.carrier_zero_count),
            ("partition.next_order_id", item.next_order_id),
        ] {
            validate_non_negative(name, value)?;
        }
        if item.next_order_id
            != checked_add(item.order_count, 1, "partition expected next order id")?
        {
            return Err(PlanError::InconsistentLedger(
                "partition d_next_o_id must equal order_count + 1",
            ));
        }
        if item.new_order_count != item.carrier_zero_count {
            return Err(PlanError::InconsistentLedger(
                "partition new_orders count must equal carrier-zero order count",
            ));
        }
        if !(checked_mul(item.order_count, 5, "minimum partition order-line count")?
            ..=checked_mul(item.order_count, 15, "maximum partition order-line count")?)
            .contains(&item.order_line_count)
        {
            return Err(PlanError::InconsistentLedger(
                "partition order-line count must be 5..=15 per order",
            ));
        }
        if !(checked_mul(
            item.new_order_count,
            5,
            "minimum partition empty delivery-time count",
        )?
            ..=checked_mul(
                item.new_order_count,
                15,
                "maximum partition empty delivery-time count",
            )?)
            .contains(&item.empty_delivery_time_count)
        {
            return Err(PlanError::InconsistentLedger(
                "partition empty delivery-time rows must be 5..=15 per queued order",
            ));
        }
        if by_key.insert(item.key, item).is_some() {
            return Err(PlanError::DuplicatePartition(item.key));
        }
    }

    let mut audits = Vec::with_capacity(required);
    for warehouse_id in 1..=warehouses {
        for district_id in 1..=DISTRICTS_PER_WAREHOUSE {
            let key = PartitionKey {
                warehouse_id,
                district_id,
            };
            let item = by_key
                .remove(&key)
                .ok_or(PlanError::MissingPartition(key))?;
            let base = format!("recovery.partition.w{warehouse_id}.d{district_id}");
            audits.push(PartitionAudit {
                key,
                checks: vec![
                    int_query(
                        CheckScope::Recovery,
                        format!("{base}.orders"),
                        "partition order count",
                        format!(
                            "SELECT COUNT(*) FROM orders WHERE o_w_id = {warehouse_id} \
                             AND o_d_id = {district_id}"
                        ),
                        item.order_count,
                    ),
                    int_query(
                        CheckScope::Recovery,
                        format!("{base}.order_lines"),
                        "partition order-line count",
                        format!(
                            "SELECT COUNT(*) FROM order_line WHERE ol_w_id = {warehouse_id} \
                             AND ol_d_id = {district_id}"
                        ),
                        item.order_line_count,
                    ),
                    int_query(
                        CheckScope::Recovery,
                        format!("{base}.new_orders"),
                        "partition undelivered queue count",
                        format!(
                            "SELECT COUNT(*) FROM new_orders WHERE no_w_id = {warehouse_id} \
                             AND no_d_id = {district_id}"
                        ),
                        item.new_order_count,
                    ),
                    int_query(
                        CheckScope::Recovery,
                        format!("{base}.empty_delivery_times"),
                        "partition empty order-line delivery timestamp count",
                        format!(
                            "SELECT COUNT(*) FROM order_line WHERE ol_w_id = {warehouse_id} \
                             AND ol_d_id = {district_id} AND ol_delivery_d = ''"
                        ),
                        item.empty_delivery_time_count,
                    ),
                    int_query(
                        CheckScope::Recovery,
                        format!("{base}.carrier_zero"),
                        "partition carrier-id zero order count",
                        format!(
                            "SELECT COUNT(*) FROM orders WHERE o_w_id = {warehouse_id} \
                             AND o_d_id = {district_id} AND o_carrier_id = 0"
                        ),
                        item.carrier_zero_count,
                    ),
                    int_query(
                        CheckScope::Recovery,
                        format!("{base}.next_order_id"),
                        "partition district next order id",
                        format!(
                            "SELECT d_next_o_id FROM district WHERE d_w_id = {warehouse_id} \
                             AND d_id = {district_id}"
                        ),
                        item.next_order_id,
                    ),
                ],
            });
        }
    }
    Ok(audits)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FloatAggregateId {
    WarehouseYtd,
    DistrictYtd,
    CustomerBalance,
    CustomerYtdPayment,
    HistoryAmount,
    StockYtd,
    OrderLineAmount,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerFloatRule {
    /// Official Payment evidence verifies each per-row update chain at 0 ULP.
    PaymentUpdateChain,
    /// Stock YTD is a sum of integral quantities and is checked at 0 ULP.
    ExactZeroUlp,
    /// Rank-scale non-negative ledgers use the published binary64 boundary.
    LargeSetBoundary,
    /// Published online/recovery comparison exists, but no public ledger
    /// formula alone proves the value.
    CrashBaselineOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FloatAggregateSpec {
    pub id: FloatAggregateId,
    pub check_name: &'static str,
    pub description: &'static str,
    pub sql: &'static str,
    pub crash_max_ulps: u32,
    pub ledger_rule: LedgerFloatRule,
}

/// The seven published FLOAT32 aggregate categories.
///
/// The first, second, and sixth are compared crash-before/crash-after at
/// 0 ULP.  The other four permit at most 1 ULP for that baseline comparison,
/// because SQL does not prescribe the binary64 scan accumulation order.
pub const FLOAT_AGGREGATES: [FloatAggregateSpec; 7] = [
    FloatAggregateSpec {
        id: FloatAggregateId::WarehouseYtd,
        check_name: "float.sum_warehouse_ytd",
        description: "SUM(warehouse.w_ytd)",
        sql: "SELECT SUM(w_ytd) FROM warehouse",
        crash_max_ulps: 0,
        ledger_rule: LedgerFloatRule::PaymentUpdateChain,
    },
    FloatAggregateSpec {
        id: FloatAggregateId::DistrictYtd,
        check_name: "float.sum_district_ytd",
        description: "SUM(district.d_ytd)",
        sql: "SELECT SUM(d_ytd) FROM district",
        crash_max_ulps: 0,
        ledger_rule: LedgerFloatRule::PaymentUpdateChain,
    },
    FloatAggregateSpec {
        id: FloatAggregateId::CustomerBalance,
        check_name: "float.sum_customer_balance",
        description: "SUM(customer.c_balance)",
        sql: "SELECT SUM(c_balance) FROM customer",
        crash_max_ulps: 1,
        ledger_rule: LedgerFloatRule::CrashBaselineOnly,
    },
    FloatAggregateSpec {
        id: FloatAggregateId::CustomerYtdPayment,
        check_name: "float.sum_customer_ytd_payment",
        description: "SUM(customer.c_ytd_payment)",
        sql: "SELECT SUM(c_ytd_payment) FROM customer",
        crash_max_ulps: 1,
        ledger_rule: LedgerFloatRule::CrashBaselineOnly,
    },
    FloatAggregateSpec {
        id: FloatAggregateId::HistoryAmount,
        check_name: "float.sum_history_amount",
        description: "SUM(history.h_amount)",
        sql: "SELECT SUM(h_amount) FROM history",
        crash_max_ulps: 1,
        ledger_rule: LedgerFloatRule::LargeSetBoundary,
    },
    FloatAggregateSpec {
        id: FloatAggregateId::StockYtd,
        check_name: "float.sum_stock_ytd",
        description: "SUM(stock.s_ytd)",
        sql: "SELECT SUM(s_ytd) FROM stock",
        crash_max_ulps: 0,
        ledger_rule: LedgerFloatRule::ExactZeroUlp,
    },
    FloatAggregateSpec {
        id: FloatAggregateId::OrderLineAmount,
        check_name: "float.sum_new_order_line_amount",
        description: "SUM(order_line.ol_amount), including initial and committed NewOrder lines",
        sql: "SELECT SUM(ol_amount) FROM order_line",
        crash_max_ulps: 1,
        ledger_rule: LedgerFloatRule::LargeSetBoundary,
    },
];

pub fn float_aggregate_plan(scope: CheckScope) -> ConsistencyPlan {
    ConsistencyPlan {
        queries: FLOAT_AGGREGATES
            .iter()
            .map(|spec| CheckQuery {
                id: format!("{:?}.{}", scope, spec.check_name).to_lowercase(),
                scope,
                description: spec.description.to_owned(),
                sql: spec.sql.to_owned(),
                expectation: ScalarExpectation::FiniteFloat32,
            })
            .collect(),
    }
}

pub fn validate_crash_float_baseline(
    spec: FloatAggregateSpec,
    before_bits: u32,
    after_bits: u32,
) -> Result<(), FloatError> {
    require_finite(before_bits)?;
    require_finite(after_bits)?;
    if float32_matches(before_bits, after_bits, spec.crash_max_ulps) {
        Ok(())
    } else {
        Err(FloatError::UlpMismatch {
            expected_bits: before_bits,
            actual_bits: after_bits,
            max_ulps: spec.crash_max_ulps,
        })
    }
}

/// The three aggregate values for which the public statement provides a
/// transaction-ledger comparison before the crash.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PublicFloatLedgerEvidence {
    pub history_amount: LargeSetBoundary,
    pub stock_ytd_bits: u32,
    pub order_line_amount: LargeSetBoundary,
}

/// Validate the public online ledger gate: history and order-line totals use
/// the rank-scale endpoint rule; stock YTD is an integral-quantity ledger and
/// is exact at 0 ULP.
pub fn validate_public_float_ledger(
    history_amount_bits: u32,
    stock_ytd_bits: u32,
    order_line_amount_bits: u32,
    evidence: PublicFloatLedgerEvidence,
) -> Result<(), FloatError> {
    require_finite(history_amount_bits)?;
    require_finite(stock_ytd_bits)?;
    require_finite(order_line_amount_bits)?;
    if !evidence.history_amount.accepts(history_amount_bits) {
        return Err(FloatError::BoundaryMismatch {
            aggregate: FloatAggregateId::HistoryAmount,
            actual_bits: history_amount_bits,
            lower_bits: evidence.history_amount.lower_bits,
            upper_bits: evidence.history_amount.upper_bits,
        });
    }
    if !float32_matches(evidence.stock_ytd_bits, stock_ytd_bits, 0) {
        return Err(FloatError::UlpMismatch {
            expected_bits: evidence.stock_ytd_bits,
            actual_bits: stock_ytd_bits,
            max_ulps: 0,
        });
    }
    if !evidence.order_line_amount.accepts(order_line_amount_bits) {
        return Err(FloatError::BoundaryMismatch {
            aggregate: FloatAggregateId::OrderLineAmount,
            actual_bits: order_line_amount_bits,
            lower_bits: evidence.order_line_amount.lower_bits,
            upper_bits: evidence.order_line_amount.upper_bits,
        });
    }
    Ok(())
}

/// Validate a single Payment/Delivery relative update at the required 0 ULP.
pub fn validate_relative_add(
    before_bits: u32,
    bound_amount_bits: u32,
    after_bits: u32,
) -> Result<(), FloatError> {
    require_finite(after_bits)?;
    let expected_bits = add_f32_once(before_bits, bound_amount_bits)?;
    if float32_matches(expected_bits, after_bits, 0) {
        Ok(())
    } else {
        Err(FloatError::UlpMismatch {
            expected_bits,
            actual_bits: after_bits,
            max_ulps: 0,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelativeUpdateEvidence {
    pub before_bits: u32,
    pub bound_amount_bits: u32,
    pub after_bits: u32,
}

/// Validate all committed Payment updates for one warehouse or district key.
///
/// Evidence may arrive in any client-response order. Each edge is checked at
/// 0 ULP, then the directed multigraph must form one Euler trail from the
/// load/checkpoint value to the typed recovery value. Repeated self-loops are
/// valid when a small amount rounds away at binary32 precision.
pub fn validate_relative_update_chain(
    initial_bits: u32,
    recovery_bits: u32,
    updates: &[RelativeUpdateEvidence],
) -> Result<(), FloatError> {
    require_finite(initial_bits)?;
    require_finite(recovery_bits)?;
    let start = canonical_zero(initial_bits);
    let expected_end = canonical_zero(recovery_bits);
    let mut outgoing: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    let mut in_degree: BTreeMap<u32, usize> = BTreeMap::new();
    let mut out_degree: BTreeMap<u32, usize> = BTreeMap::new();

    for update in updates {
        validate_relative_add(
            update.before_bits,
            update.bound_amount_bits,
            update.after_bits,
        )?;
        let before = canonical_zero(update.before_bits);
        let after = canonical_zero(update.after_bits);
        outgoing.entry(before).or_default().push(after);
        *out_degree.entry(before).or_default() += 1;
        *in_degree.entry(after).or_default() += 1;
        in_degree.entry(before).or_default();
        out_degree.entry(after).or_default();
    }

    for node in in_degree.keys().chain(out_degree.keys()) {
        let incoming = *in_degree.get(node).unwrap_or(&0);
        let outgoing_count = *out_degree.get(node).unwrap_or(&0);
        let valid = if start == expected_end {
            incoming == outgoing_count
        } else if *node == start {
            outgoing_count == incoming + 1
        } else if *node == expected_end {
            incoming == outgoing_count + 1
        } else {
            incoming == outgoing_count
        };
        if !valid {
            return Err(FloatError::BrokenUpdateChain {
                expected_final_bits: recovery_bits,
                observed_final_bits: *node,
                used_updates: 0,
                total_updates: updates.len(),
            });
        }
    }

    let mut stack = vec![start];
    let mut reverse_path = Vec::with_capacity(updates.len() + 1);
    while let Some(current) = stack.last().copied() {
        if let Some(next) = outgoing.get_mut(&current).and_then(Vec::pop) {
            stack.push(next);
        } else {
            reverse_path.push(current);
            stack.pop();
        }
    }
    reverse_path.reverse();
    let used_updates = reverse_path.len().saturating_sub(1);
    let observed_end = reverse_path.last().copied().unwrap_or(start);
    if used_updates != updates.len() || observed_end != expected_end {
        return Err(FloatError::BrokenUpdateChain {
            expected_final_bits: recovery_bits,
            observed_final_bits: observed_end,
            used_updates,
            total_updates: updates.len(),
        });
    }
    Ok(())
}

/// Infer and validate the terminal value of an unordered relative-update chain.
///
/// The graph must contain every supplied edge in one trail rooted at the
/// caller's known load/checkpoint value. A stale lost update normally creates
/// two outgoing edges from the same committed value; without a matching
/// serialized predecessor/successor, that fork cannot form this complete
/// trail and is rejected. Every edge is also checked as one binary32 RNE add.
pub fn validate_relative_update_chain_from_initial(
    initial_bits: u32,
    updates: &[RelativeUpdateEvidence],
) -> Result<u32, FloatError> {
    require_finite(initial_bits)?;
    if updates.is_empty() {
        return Ok(canonical_zero(initial_bits));
    }

    let start = canonical_zero(initial_bits);
    let mut balance: BTreeMap<u32, i64> = BTreeMap::new();
    for update in updates {
        validate_relative_add(
            update.before_bits,
            update.bound_amount_bits,
            update.after_bits,
        )?;
        let before = canonical_zero(update.before_bits);
        let after = canonical_zero(update.after_bits);
        *balance.entry(before).or_default() += 1;
        *balance.entry(after).or_default() -= 1;
    }

    let mut end = None;
    for (node, degree) in &balance {
        if *node == start {
            if *degree == 0 {
                continue;
            }
            if *degree != 1 {
                return Err(FloatError::BrokenUpdateChain {
                    expected_final_bits: start,
                    observed_final_bits: *node,
                    used_updates: 0,
                    total_updates: updates.len(),
                });
            }
        } else if *degree == -1 && end.is_none() {
            end = Some(*node);
        } else if *degree != 0 {
            return Err(FloatError::BrokenUpdateChain {
                expected_final_bits: start,
                observed_final_bits: *node,
                used_updates: 0,
                total_updates: updates.len(),
            });
        }
    }

    let endpoint = end.unwrap_or(start);
    validate_relative_update_chain(initial_bits, endpoint, updates)?;
    Ok(endpoint)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CounterChainError {
    Overflow,
    InvalidEdge { before: i32, after: i32 },
    DuplicatePredecessor(i32),
    MissingPredecessor(i32),
}

impl fmt::Display for CounterChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => write!(f, "counter chain overflowed INT32"),
            Self::InvalidEdge { before, after } => {
                write!(f, "counter edge {before}->{after} is not exactly +1")
            }
            Self::DuplicatePredecessor(value) => {
                write!(f, "counter chain forks from duplicate predecessor {value}")
            }
            Self::MissingPredecessor(value) => {
                write!(f, "counter chain is disconnected at predecessor {value}")
            }
        }
    }
}

impl std::error::Error for CounterChainError {}

/// Validate an unordered set of `before -> before + 1` counter edges.
///
/// Counts make stale forks unambiguous: two committed writers reporting the
/// same predecessor are rejected even if their associated FLOAT32 value did
/// not change because of rounding.
pub fn validate_increment_chain(
    initial: i32,
    updates: &[(i32, i32)],
) -> Result<i32, CounterChainError> {
    let mut by_before = BTreeMap::new();
    for (before, after) in updates {
        let expected = before.checked_add(1).ok_or(CounterChainError::Overflow)?;
        if *after != expected {
            return Err(CounterChainError::InvalidEdge {
                before: *before,
                after: *after,
            });
        }
        if by_before.insert(*before, *after).is_some() {
            return Err(CounterChainError::DuplicatePredecessor(*before));
        }
    }

    let mut current = initial;
    for _ in 0..updates.len() {
        current = by_before
            .remove(&current)
            .ok_or(CounterChainError::MissingPredecessor(current))?;
    }
    if let Some((&before, _)) = by_before.first_key_value() {
        return Err(CounterChainError::MissingPredecessor(before));
    }
    Ok(current)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CustomerLogicalVersion {
    pub payment_count: i32,
    pub delivery_count: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CustomerUpdateKind {
    Payment,
    Delivery,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CustomerUpdateEvidence {
    pub kind: CustomerUpdateKind,
    pub before_version: CustomerLogicalVersion,
    pub after_version: CustomerLogicalVersion,
    pub amount_bits: u32,
    pub balance_before_bits: u32,
    pub balance_after_bits: u32,
    pub ytd_payment_before_bits: Option<u32>,
    pub ytd_payment_after_bits: Option<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CustomerUpdateEndpoint {
    pub version: CustomerLogicalVersion,
    pub balance_bits: u32,
    pub ytd_payment_bits: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CustomerChainError {
    NegativeVersion(CustomerLogicalVersion),
    VersionOverflow(CustomerLogicalVersion),
    InvalidVersionEdge {
        kind: CustomerUpdateKind,
        before: CustomerLogicalVersion,
        after: CustomerLogicalVersion,
    },
    DuplicatePredecessor(CustomerLogicalVersion),
    MissingPredecessor(CustomerLogicalVersion),
    BalancePredecessorMismatch {
        version: CustomerLogicalVersion,
        expected_bits: u32,
        actual_bits: u32,
    },
    YtdPredecessorMismatch {
        version: CustomerLogicalVersion,
        expected_bits: u32,
        actual_bits: u32,
    },
    MissingPaymentYtd(CustomerLogicalVersion),
    UnexpectedDeliveryYtd(CustomerLogicalVersion),
    Float(FloatError),
}

impl fmt::Display for CustomerChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeVersion(version) => {
                write!(f, "customer logical version is negative: {version:?}")
            }
            Self::VersionOverflow(version) => {
                write!(f, "customer logical version overflowed after {version:?}")
            }
            Self::InvalidVersionEdge {
                kind,
                before,
                after,
            } => write!(
                f,
                "{kind:?} customer version edge {before:?}->{after:?} does not advance exactly \
                 one family counter"
            ),
            Self::DuplicatePredecessor(version) => {
                write!(
                    f,
                    "customer update chain forks from predecessor {version:?}"
                )
            }
            Self::MissingPredecessor(version) => {
                write!(
                    f,
                    "customer update chain is disconnected at predecessor {version:?}"
                )
            }
            Self::BalancePredecessorMismatch {
                version,
                expected_bits,
                actual_bits,
            } => write!(
                f,
                "customer balance predecessor at {version:?} expected 0x{expected_bits:08x}, \
                 got 0x{actual_bits:08x}"
            ),
            Self::YtdPredecessorMismatch {
                version,
                expected_bits,
                actual_bits,
            } => write!(
                f,
                "customer ytd predecessor at {version:?} expected 0x{expected_bits:08x}, \
                 got 0x{actual_bits:08x}"
            ),
            Self::MissingPaymentYtd(version) => {
                write!(f, "Payment at {version:?} omitted c_ytd_payment evidence")
            }
            Self::UnexpectedDeliveryYtd(version) => {
                write!(
                    f,
                    "Delivery at {version:?} supplied Payment-only ytd evidence"
                )
            }
            Self::Float(error) => write!(f, "customer FLOAT32 chain failed: {error}"),
        }
    }
}

impl std::error::Error for CustomerChainError {}

impl From<FloatError> for CustomerChainError {
    fn from(error: FloatError) -> Self {
        Self::Float(error)
    }
}

/// Replay all Payment and Delivery writes to one customer through their shared
/// `(c_payment_cnt, c_delivery_cnt)` predecessor.
///
/// The pair is a monotonic logical row version: a Payment advances only the
/// first component and a Delivery advances only the second. Consequently each
/// committed update must own one unique predecessor. This rejects stale
/// cross-family forks even when the balance edge is a binary32 self-loop, and
/// rejects disconnected or compensating evidence before it can be used to
/// derive recovery endpoints.
pub fn validate_customer_update_chain(
    initial_balance_bits: u32,
    initial_ytd_payment_bits: u32,
    initial_version: CustomerLogicalVersion,
    updates: &[CustomerUpdateEvidence],
) -> Result<CustomerUpdateEndpoint, CustomerChainError> {
    require_finite(initial_balance_bits)?;
    require_finite(initial_ytd_payment_bits)?;
    validate_customer_version(initial_version)?;

    let mut by_predecessor = BTreeMap::new();
    for update in updates {
        validate_customer_version(update.before_version)?;
        validate_customer_version(update.after_version)?;
        let expected_after = next_customer_version(update.kind, update.before_version)?;
        if update.after_version != expected_after {
            return Err(CustomerChainError::InvalidVersionEdge {
                kind: update.kind,
                before: update.before_version,
                after: update.after_version,
            });
        }
        if by_predecessor
            .insert(update.before_version, *update)
            .is_some()
        {
            return Err(CustomerChainError::DuplicatePredecessor(
                update.before_version,
            ));
        }
    }

    let mut version = initial_version;
    let mut balance_bits = canonical_zero(initial_balance_bits);
    let mut ytd_payment_bits = canonical_zero(initial_ytd_payment_bits);
    while !by_predecessor.is_empty() {
        let update = by_predecessor
            .remove(&version)
            .ok_or(CustomerChainError::MissingPredecessor(version))?;
        if !float32_matches(balance_bits, update.balance_before_bits, 0) {
            return Err(CustomerChainError::BalancePredecessorMismatch {
                version,
                expected_bits: balance_bits,
                actual_bits: update.balance_before_bits,
            });
        }

        let balance_delta_bits = match update.kind {
            CustomerUpdateKind::Payment => update.amount_bits ^ 0x8000_0000,
            CustomerUpdateKind::Delivery => update.amount_bits,
        };
        let expected_balance_bits = add_f32_once(balance_bits, balance_delta_bits)?;
        if !float32_matches(expected_balance_bits, update.balance_after_bits, 0) {
            return Err(CustomerChainError::Float(FloatError::UlpMismatch {
                expected_bits: expected_balance_bits,
                actual_bits: update.balance_after_bits,
                max_ulps: 0,
            }));
        }
        balance_bits = canonical_zero(update.balance_after_bits);

        match (
            update.kind,
            update.ytd_payment_before_bits,
            update.ytd_payment_after_bits,
        ) {
            (CustomerUpdateKind::Payment, Some(before_bits), Some(after_bits)) => {
                if !float32_matches(ytd_payment_bits, before_bits, 0) {
                    return Err(CustomerChainError::YtdPredecessorMismatch {
                        version,
                        expected_bits: ytd_payment_bits,
                        actual_bits: before_bits,
                    });
                }
                let expected_ytd_bits = add_f32_once(ytd_payment_bits, update.amount_bits)?;
                if !float32_matches(expected_ytd_bits, after_bits, 0) {
                    return Err(CustomerChainError::Float(FloatError::UlpMismatch {
                        expected_bits: expected_ytd_bits,
                        actual_bits: after_bits,
                        max_ulps: 0,
                    }));
                }
                ytd_payment_bits = canonical_zero(after_bits);
            }
            (CustomerUpdateKind::Payment, _, _) => {
                return Err(CustomerChainError::MissingPaymentYtd(version));
            }
            (CustomerUpdateKind::Delivery, None, None) => {}
            (CustomerUpdateKind::Delivery, _, _) => {
                return Err(CustomerChainError::UnexpectedDeliveryYtd(version));
            }
        }
        version = update.after_version;
    }

    Ok(CustomerUpdateEndpoint {
        version,
        balance_bits,
        ytd_payment_bits,
    })
}

fn validate_customer_version(version: CustomerLogicalVersion) -> Result<(), CustomerChainError> {
    if version.payment_count < 0 || version.delivery_count < 0 {
        Err(CustomerChainError::NegativeVersion(version))
    } else {
        Ok(())
    }
}

fn next_customer_version(
    kind: CustomerUpdateKind,
    before: CustomerLogicalVersion,
) -> Result<CustomerLogicalVersion, CustomerChainError> {
    let mut after = before;
    match kind {
        CustomerUpdateKind::Payment => {
            after.payment_count = after
                .payment_count
                .checked_add(1)
                .ok_or(CustomerChainError::VersionOverflow(before))?;
        }
        CustomerUpdateKind::Delivery => {
            after.delivery_count = after
                .delivery_count
                .checked_add(1)
                .ok_or(CustomerChainError::VersionOverflow(before))?;
        }
    }
    Ok(after)
}

#[derive(Clone, Debug, PartialEq)]
pub enum FloatError {
    NonFinite(u32),
    NegativeInput(u32),
    NonFiniteExactSum(f64),
    NegativeExactSum(f64),
    TooManyTerms(u64),
    ArithmeticOverflow,
    InvalidAccumulator(&'static str),
    BoundaryMismatch {
        aggregate: FloatAggregateId,
        actual_bits: u32,
        lower_bits: u32,
        upper_bits: u32,
    },
    BrokenUpdateChain {
        expected_final_bits: u32,
        observed_final_bits: u32,
        used_updates: usize,
        total_updates: usize,
    },
    UlpMismatch {
        expected_bits: u32,
        actual_bits: u32,
        max_ulps: u32,
    },
}

impl fmt::Display for FloatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonFinite(bits) => write!(f, "non-finite FLOAT32 0x{bits:08x}"),
            Self::NegativeInput(bits) => {
                write!(f, "large-set boundary input is negative: 0x{bits:08x}")
            }
            Self::NonFiniteExactSum(value) => write!(f, "non-finite exact sum {value}"),
            Self::NegativeExactSum(value) => {
                write!(
                    f,
                    "large-set boundary requires a non-negative sum, got {value}"
                )
            }
            Self::TooManyTerms(count) => write!(
                f,
                "large-set boundary supports at most 2^53 terms, got {count}"
            ),
            Self::ArithmeticOverflow => write!(f, "FLOAT32 operation overflowed"),
            Self::InvalidAccumulator(reason) => {
                write!(f, "invalid non-negative FLOAT32 accumulator: {reason}")
            }
            Self::BoundaryMismatch {
                aggregate,
                actual_bits,
                lower_bits,
                upper_bits,
            } => write!(
                f,
                "{aggregate:?}: 0x{actual_bits:08x} is outside \
                 [0x{lower_bits:08x}, 0x{upper_bits:08x}]"
            ),
            Self::BrokenUpdateChain {
                expected_final_bits,
                observed_final_bits,
                used_updates,
                total_updates,
            } => write!(
                f,
                "relative-update evidence does not form one complete chain: used \
                 {used_updates}/{total_updates}, expected recovery 0x{expected_final_bits:08x}, \
                 observed endpoint 0x{observed_final_bits:08x}"
            ),
            Self::UlpMismatch {
                expected_bits,
                actual_bits,
                max_ulps,
            } => write!(
                f,
                "expected 0x{expected_bits:08x}, got 0x{actual_bits:08x}, \
                 tolerance {max_ulps} ULP"
            ),
        }
    }
}

impl std::error::Error for FloatError {}

pub fn require_finite(bits: u32) -> Result<f32, FloatError> {
    let value = f32::from_bits(bits);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(FloatError::NonFinite(bits))
    }
}

pub fn float32_matches(expected_bits: u32, actual_bits: u32, max_ulps: u32) -> bool {
    match ulp_distance(expected_bits, actual_bits) {
        Some(distance) => distance <= max_ulps,
        None => false,
    }
}

/// ULP distance for finite binary32 values, treating +0 and -0 as equal.
pub fn ulp_distance(left_bits: u32, right_bits: u32) -> Option<u32> {
    let left = f32::from_bits(left_bits);
    let right = f32::from_bits(right_bits);
    if !left.is_finite() || !right.is_finite() {
        return None;
    }
    if left == 0.0 && right == 0.0 {
        return Some(0);
    }

    Some(ordered_f32_bits(left_bits).abs_diff(ordered_f32_bits(right_bits)))
}

fn ordered_f32_bits(bits: u32) -> u32 {
    let magnitude = bits & 0x7fff_ffff;
    if bits & 0x8000_0000 == 0 {
        0x8000_0000 + magnitude
    } else {
        0x8000_0000 - magnitude
    }
}

/// One binary32 round-to-nearest/ties-to-even relative addition.
pub fn add_f32_once(left_bits: u32, right_bits: u32) -> Result<u32, FloatError> {
    let left = require_finite(left_bits)?;
    let right = require_finite(right_bits)?;
    let result = left + right;
    if result.is_finite() {
        Ok(result.to_bits())
    } else {
        Err(FloatError::ArithmeticOverflow)
    }
}

/// RMDB `SUM(FLOAT)` semantics for a specified scan order.
///
/// Every stored binary32 value is converted exactly to binary64, accumulation
/// occurs in binary64, and the result is rounded once to binary32.
pub fn sum_f32_as_f64_once<I>(values: I) -> Result<u32, FloatError>
where
    I: IntoIterator<Item = u32>,
{
    let mut sum = 0.0_f64;
    for bits in values {
        sum += f64::from(require_finite(bits)?);
    }
    let rounded = sum as f32;
    if rounded.is_finite() {
        Ok(rounded.to_bits())
    } else {
        Err(FloatError::ArithmeticOverflow)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct LargeSetBoundary {
    /// Human-readable approximation only; endpoint bits are authoritative.
    pub sum_for_diagnostics: f64,
    pub term_count: u64,
    /// Human-readable approximation only; endpoint bits are authoritative.
    pub error_bound_for_diagnostics: f64,
    pub lower_bits: u32,
    pub upper_bits: u32,
}

impl LargeSetBoundary {
    pub fn accepts(self, actual_bits: u32) -> bool {
        if require_finite(actual_bits).is_err() {
            return false;
        }
        let actual = ordered_f32_bits(canonical_zero(actual_bits));
        let lower = ordered_f32_bits(canonical_zero(self.lower_bits));
        let upper = ordered_f32_bits(canonical_zero(self.upper_bits));
        (lower..=upper).contains(&actual)
    }
}

const MAX_ACCUMULATOR_TERMS: u64 = 1_u64 << 53;
const MAX_ACCUMULATOR_WORDS: usize = 6;

/// Mergeable exact sum of non-negative binary32 inputs.
///
/// The little-endian words encode an integer in units of 2^-149. Together
/// with `term_count`, this is sufficient to persist and later reproduce the
/// published rank-scale boundary without decimal formatting or an
/// order-dependent binary64 sum.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct NonNegativeF32Accumulator {
    term_count: u64,
    exact: PositiveF32Sum,
}

impl NonNegativeF32Accumulator {
    pub fn term_count(&self) -> u64 {
        self.term_count
    }

    pub fn add_bits(&mut self, bits: u32) -> Result<(), FloatError> {
        let value = require_finite(bits)?;
        if value < 0.0 {
            return Err(FloatError::NegativeInput(bits));
        }
        let next = self
            .term_count
            .checked_add(1)
            .ok_or(FloatError::ArithmeticOverflow)?;
        if next > MAX_ACCUMULATOR_TERMS {
            return Err(FloatError::TooManyTerms(next));
        }
        self.exact.add_bits(bits);
        self.term_count = next;
        Ok(())
    }

    pub fn extend_bits<I>(&mut self, values: I) -> Result<(), FloatError>
    where
        I: IntoIterator<Item = u32>,
    {
        for bits in values {
            self.add_bits(bits)?;
        }
        Ok(())
    }

    pub fn add_repeated_bits(&mut self, bits: u32, count: u64) -> Result<(), FloatError> {
        let value = require_finite(bits)?;
        if value < 0.0 {
            return Err(FloatError::NegativeInput(bits));
        }
        let next = self
            .term_count
            .checked_add(count)
            .ok_or(FloatError::ArithmeticOverflow)?;
        if next > MAX_ACCUMULATOR_TERMS {
            return Err(FloatError::TooManyTerms(next));
        }
        let mut term = PositiveF32Sum::default();
        term.add_bits(bits);
        self.exact.add_sum(&term.mul_u64(count));
        self.term_count = next;
        Ok(())
    }

    pub fn merge(&mut self, other: &Self) -> Result<(), FloatError> {
        let next = self
            .term_count
            .checked_add(other.term_count)
            .ok_or(FloatError::ArithmeticOverflow)?;
        if next > MAX_ACCUMULATOR_TERMS {
            return Err(FloatError::TooManyTerms(next));
        }
        self.exact.add_sum(&other.exact);
        self.term_count = next;
        Ok(())
    }

    pub fn boundary(&self) -> Result<LargeSetBoundary, FloatError> {
        boundary_from_exact(&self.exact, self.term_count)
    }

    /// Return `(term_count, exact little-endian 64-bit words)`.
    pub fn to_words(&self) -> (u64, Vec<u64>) {
        (self.term_count, self.exact.limbs.clone())
    }

    /// Restore a canonical accumulator representation.
    ///
    /// Trailing zero words, impossible sums for the supplied term count, and
    /// counts above the public 2^53 bound are rejected.
    pub fn from_words(term_count: u64, words: &[u64]) -> Result<Self, FloatError> {
        if term_count > MAX_ACCUMULATOR_TERMS {
            return Err(FloatError::TooManyTerms(term_count));
        }
        if words.len() > MAX_ACCUMULATOR_WORDS {
            return Err(FloatError::InvalidAccumulator(
                "exact sum contains too many words",
            ));
        }
        if words.last() == Some(&0) {
            return Err(FloatError::InvalidAccumulator(
                "words must not contain a trailing zero",
            ));
        }
        if term_count == 0 && !words.is_empty() {
            return Err(FloatError::InvalidAccumulator(
                "zero terms must have a zero exact sum",
            ));
        }

        let exact = PositiveF32Sum {
            limbs: words.to_vec(),
        };
        let mut maximum_term = PositiveF32Sum::default();
        maximum_term.add_bits(f32::MAX.to_bits());
        let maximum = maximum_term.mul_u64(term_count);
        if exact.greater_than(&maximum) {
            return Err(FloatError::InvalidAccumulator(
                "exact sum exceeds term_count * FLOAT32_MAX",
            ));
        }
        Ok(Self { term_count, exact })
    }
}

/// Published rank-scale non-negative ledger boundary.
///
/// `exact_sum` is the caller ledger's exact-real sum represented as binary64.
/// The official checker owns a higher-precision ledger; this API keeps that
/// responsibility outside the transport-neutral validator.  It applies
/// epsilon = n * 2^-53 * S, rounds both endpoints once to binary32, and accepts
/// the representable interval between them.
pub fn large_set_boundary(exact_sum: f64, term_count: u64) -> Result<LargeSetBoundary, FloatError> {
    if term_count > 1_u64 << 53 {
        return Err(FloatError::TooManyTerms(term_count));
    }
    if !exact_sum.is_finite() {
        return Err(FloatError::NonFiniteExactSum(exact_sum));
    }
    if exact_sum < 0.0 {
        return Err(FloatError::NegativeExactSum(exact_sum));
    }

    let unit_roundoff = 2.0_f64.powi(-53);
    let error_bound = (term_count as f64) * unit_roundoff * exact_sum;
    if !error_bound.is_finite() {
        return Err(FloatError::ArithmeticOverflow);
    }
    let lower = (exact_sum - error_bound).max(0.0) as f32;
    let upper = (exact_sum + error_bound) as f32;
    if !lower.is_finite() || !upper.is_finite() {
        return Err(FloatError::ArithmeticOverflow);
    }

    Ok(LargeSetBoundary {
        sum_for_diagnostics: exact_sum,
        term_count,
        error_bound_for_diagnostics: error_bound,
        lower_bits: lower.to_bits(),
        upper_bits: upper.to_bits(),
    })
}

/// Build the rank-scale boundary from the exact dyadic sum of non-negative
/// binary32 inputs.
///
/// Unlike ordinary `f64` accumulation, this path first accumulates every f32
/// significand into a base-2 superaccumulator.  The two rational endpoints are
/// then rounded directly to binary32 with ties-to-even.  This preserves the
/// one-bit ambiguity at a binary32 midpoint instead of making it depend on a
/// host-side `f64` summation order.
pub fn large_set_boundary_from_f32<I>(values: I) -> Result<LargeSetBoundary, FloatError>
where
    I: IntoIterator<Item = u32>,
{
    let mut accumulator = NonNegativeF32Accumulator::default();
    accumulator.extend_bits(values)?;
    accumulator.boundary()
}

fn boundary_from_exact(
    exact: &PositiveF32Sum,
    term_count: u64,
) -> Result<LargeSetBoundary, FloatError> {
    let sum_for_diagnostics = exact.to_f64();
    if !sum_for_diagnostics.is_finite() {
        return Err(FloatError::ArithmeticOverflow);
    }
    let error_bound_for_diagnostics = (term_count as f64) * 2.0_f64.powi(-53) * sum_for_diagnostics;
    let lower = exact.mul_u64(MAX_ACCUMULATOR_TERMS - term_count);
    let upper = exact.mul_u64(MAX_ACCUMULATOR_TERMS + term_count);
    // exact uses units of 2^-149 and the endpoint factor has denominator 2^53.
    let lower_bits = round_positive_binary_to_f32(&lower, -202)?;
    let upper_bits = round_positive_binary_to_f32(&upper, -202)?;

    Ok(LargeSetBoundary {
        sum_for_diagnostics,
        term_count,
        error_bound_for_diagnostics,
        lower_bits,
        upper_bits,
    })
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct PositiveF32Sum {
    /// Little-endian limbs in units of 2^-149.
    limbs: Vec<u64>,
}

impl PositiveF32Sum {
    fn add_bits(&mut self, bits: u32) {
        let exponent = (bits >> 23) & 0xff;
        let fraction = bits & 0x007f_ffff;
        if exponent == 0 && fraction == 0 {
            return;
        }

        let (significand, shift) = if exponent == 0 {
            (u64::from(fraction), 0_usize)
        } else {
            (u64::from(fraction | 0x0080_0000), (exponent - 1) as usize)
        };
        self.add_shifted(significand, shift);
    }

    fn add_shifted(&mut self, value: u64, shift: usize) {
        let limb_index = shift / 64;
        let bit_offset = shift % 64;
        let required = limb_index + if bit_offset == 0 { 1 } else { 2 };
        self.limbs.resize(self.limbs.len().max(required), 0);

        let low = value << bit_offset;
        let mut carry = low;
        let mut index = limb_index;
        loop {
            let (sum, overflow) = self.limbs[index].overflowing_add(carry);
            self.limbs[index] = sum;
            if !overflow {
                break;
            }
            index += 1;
            if index == self.limbs.len() {
                self.limbs.push(0);
            }
            carry = 1;
        }

        if bit_offset != 0 {
            let high = value >> (64 - bit_offset);
            if high != 0 {
                let mut carry = high;
                let mut index = limb_index + 1;
                loop {
                    let (sum, overflow) = self.limbs[index].overflowing_add(carry);
                    self.limbs[index] = sum;
                    if !overflow {
                        break;
                    }
                    index += 1;
                    if index == self.limbs.len() {
                        self.limbs.push(0);
                    }
                    carry = 1;
                }
            }
        }
        self.trim();
    }

    fn add_sum(&mut self, other: &Self) {
        let required = self.limbs.len().max(other.limbs.len());
        self.limbs.resize(required, 0);
        let mut carry = 0_u128;
        for index in 0..required {
            let sum = u128::from(self.limbs[index])
                + u128::from(other.limbs.get(index).copied().unwrap_or(0))
                + carry;
            self.limbs[index] = sum as u64;
            carry = sum >> 64;
        }
        if carry != 0 {
            self.limbs.push(carry as u64);
        }
        self.trim();
    }

    fn mul_u64(&self, multiplier: u64) -> Self {
        if multiplier == 0 || self.is_zero() {
            return Self::default();
        }
        let mut result = Vec::with_capacity(self.limbs.len() + 1);
        let mut carry = 0_u128;
        for limb in &self.limbs {
            let product = u128::from(*limb) * u128::from(multiplier) + carry;
            result.push(product as u64);
            carry = product >> 64;
        }
        if carry != 0 {
            result.push(carry as u64);
        }
        Self { limbs: result }
    }

    fn greater_than(&self, other: &Self) -> bool {
        if self.limbs.len() != other.limbs.len() {
            return self.limbs.len() > other.limbs.len();
        }
        self.limbs
            .iter()
            .rev()
            .zip(other.limbs.iter().rev())
            .find_map(|(left, right)| (left != right).then_some(left > right))
            .unwrap_or(false)
    }

    fn is_zero(&self) -> bool {
        self.limbs.is_empty()
    }

    fn trim(&mut self) {
        while self.limbs.last() == Some(&0) {
            self.limbs.pop();
        }
    }

    fn bit_len(&self) -> usize {
        self.limbs.last().map_or(0, |top| {
            (self.limbs.len() - 1) * 64 + (64 - top.leading_zeros() as usize)
        })
    }

    fn bit(&self, index: usize) -> bool {
        self.limbs
            .get(index / 64)
            .is_some_and(|limb| limb & (1_u64 << (index % 64)) != 0)
    }

    fn any_bits_below(&self, exclusive: usize) -> bool {
        if exclusive == 0 {
            return false;
        }
        let full_limbs = exclusive / 64;
        if self.limbs.iter().take(full_limbs).any(|limb| *limb != 0) {
            return true;
        }
        let partial = exclusive % 64;
        partial != 0
            && self
                .limbs
                .get(full_limbs)
                .is_some_and(|limb| *limb & ((1_u64 << partial) - 1) != 0)
    }

    fn shifted_low_u64(&self, shift: usize) -> u64 {
        let limb = shift / 64;
        let offset = shift % 64;
        let low = self.limbs.get(limb).copied().unwrap_or(0) >> offset;
        if offset == 0 {
            low
        } else {
            low | (self.limbs.get(limb + 1).copied().unwrap_or(0) << (64 - offset))
        }
    }

    fn rounded_shift_to_u64(&self, shift: usize) -> u64 {
        let mut kept = self.shifted_low_u64(shift);
        if shift == 0 {
            return kept;
        }
        let guard = self.bit(shift - 1);
        let sticky = self.any_bits_below(shift - 1);
        if guard && (sticky || kept & 1 != 0) {
            kept += 1;
        }
        kept
    }

    fn to_f64(&self) -> f64 {
        let mut value = 0.0_f64;
        for limb in self.limbs.iter().rev() {
            value = value * 2.0_f64.powi(64) + (*limb as f64);
        }
        value * 2.0_f64.powi(-149)
    }
}

fn round_positive_binary_to_f32(
    integer: &PositiveF32Sum,
    binary_scale: i32,
) -> Result<u32, FloatError> {
    if integer.is_zero() {
        return Ok(0.0_f32.to_bits());
    }

    let top_bit = integer.bit_len() as i32 - 1;
    let mut exponent = top_bit + binary_scale;
    if exponent > 127 {
        return Err(FloatError::ArithmeticOverflow);
    }

    if exponent >= -126 {
        let mut significand = if top_bit >= 23 {
            integer.rounded_shift_to_u64((top_bit - 23) as usize)
        } else {
            integer.shifted_low_u64(0) << (23 - top_bit) as usize
        };
        if significand == 1_u64 << 24 {
            significand >>= 1;
            exponent += 1;
            if exponent > 127 {
                return Err(FloatError::ArithmeticOverflow);
            }
        }
        let exponent_bits = (exponent + 127) as u32;
        let fraction = significand as u32 & 0x007f_ffff;
        return Ok((exponent_bits << 23) | fraction);
    }

    // Subnormal binary32 values are integral multiples of 2^-149.
    let unit_shift = binary_scale + 149;
    let significand = if unit_shift >= 0 {
        let left = unit_shift as usize;
        let bit_len = integer.bit_len() + left;
        if bit_len > 24 {
            return Err(FloatError::ArithmeticOverflow);
        }
        integer.shifted_low_u64(0) << left
    } else {
        integer.rounded_shift_to_u64((-unit_shift) as usize)
    };
    if significand >= 1_u64 << 23 {
        // Rounding the largest subnormal can produce the smallest normal.
        Ok(0x0080_0000)
    } else {
        Ok(significand as u32)
    }
}

fn canonical_zero(bits: u32) -> u32 {
    if f32::from_bits(bits) == 0.0 {
        0
    } else {
        bits
    }
}

fn int_query(
    scope: CheckScope,
    id: impl Into<String>,
    description: impl Into<String>,
    sql: impl Into<String>,
    expected: i64,
) -> CheckQuery {
    CheckQuery {
        id: id.into(),
        scope,
        description: description.into(),
        sql: sql.into(),
        expectation: ScalarExpectation::ExactInt(expected),
    }
}

fn float_query(
    scope: CheckScope,
    id: impl Into<String>,
    description: impl Into<String>,
    sql: impl Into<String>,
    bits: u32,
    max_ulps: u32,
) -> CheckQuery {
    CheckQuery {
        id: id.into(),
        scope,
        description: description.into(),
        sql: sql.into(),
        expectation: ScalarExpectation::ExactFloat32 { bits, max_ulps },
    }
}

fn validate_non_negative(name: &'static str, value: i64) -> Result<(), PlanError> {
    if value < 0 {
        Err(PlanError::NegativeCount(name))
    } else {
        Ok(())
    }
}

fn require_int32(check_id: &str, value: i64) -> Result<i32, PlanError> {
    i32::try_from(value).map_err(|_| PlanError::IntegerExpectationOutOfRange {
        check_id: check_id.to_owned(),
        expected: value,
    })
}

fn checked_mul(left: i64, right: i64, name: &'static str) -> Result<i64, PlanError> {
    left.checked_mul(right)
        .ok_or(PlanError::ArithmeticOverflow(name))
}

fn checked_add(left: i64, right: i64, name: &'static str) -> Result<i64, PlanError> {
    left.checked_add(right)
        .ok_or(PlanError::ArithmeticOverflow(name))
}

fn add_to_count(
    counts: &mut BTreeMap<&'static str, i64>,
    table: &'static str,
    delta: i64,
) -> Result<(), PlanError> {
    let current = counts
        .get_mut(table)
        .expect("internal table count must be present");
    *current = checked_add(*current, delta, table)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition_expectations(warehouses: i32) -> Vec<PartitionExpectation> {
        (1..=warehouses)
            .flat_map(|warehouse_id| {
                (1..=DISTRICTS_PER_WAREHOUSE).map(move |district_id| PartitionExpectation {
                    key: PartitionKey {
                        warehouse_id,
                        district_id,
                    },
                    order_count: 3_000,
                    order_line_count: 15_000,
                    new_order_count: 900,
                    empty_delivery_time_count: 4_500,
                    carrier_zero_count: 900,
                    next_order_id: 3_001,
                })
            })
            .collect()
    }

    #[test]
    fn setup_valid_row_checks_use_and_only_predicates_and_exact_counts() {
        let plan = setup_plan(SetupExpectations::final_2026(15_123_456, 4_571_234)).unwrap();
        for (id, expected) in [
            ("setup.stock.quantity_range", 5_000_000),
            ("setup.orders.line_count_range", 1_500_000),
            ("setup.order_line.quantity", 15_123_456),
            ("setup.orders.carrier_range", 1_500_000),
            ("setup.stock.nonzero_ytd", 5_000_000),
            ("setup.stock.nonzero_order_cnt", 5_000_000),
            ("setup.stock.nonzero_remote_cnt", 5_000_000),
        ] {
            let query = plan.queries.iter().find(|query| query.id == id).unwrap();
            assert!(!query.sql.contains(" OR "), "{id} still uses boolean OR");
            assert_eq!(query.expectation, ScalarExpectation::ExactInt(expected));
        }
    }

    #[test]
    fn scaled_recovery_audit_covers_smoke_keyspace_and_caps_public_scale() {
        let audits =
            recovery_partition_audits_for_warehouses(1, partition_expectations(1)).unwrap();
        assert_eq!(audits.len(), DISTRICTS_PER_WAREHOUSE as usize);
        assert_eq!(audits.first().unwrap().key.warehouse_id, 1);
        assert_eq!(
            audits.last().unwrap().key,
            PartitionKey {
                warehouse_id: 1,
                district_id: DISTRICTS_PER_WAREHOUSE,
            }
        );

        assert!(matches!(
            recovery_partition_audits_for_warehouses(0, Vec::new()),
            Err(PlanError::NonPositiveWarehouseCount(0))
        ));
        assert!(matches!(
            recovery_partition_audits_for_warehouses(
                FINAL_WAREHOUSES + 1,
                partition_expectations(FINAL_WAREHOUSES + 1)
            ),
            Err(PlanError::WarehouseCountExceedsPublicMaximum { .. })
        ));
    }

    #[test]
    fn mergeable_accumulator_round_trips_canonical_words() {
        let mut left = NonNegativeF32Accumulator::default();
        left.add_repeated_bits(10.0_f32.to_bits(), 1_500_000)
            .unwrap();
        let mut right = NonNegativeF32Accumulator::default();
        right
            .extend_bits([1.25_f32.to_bits(), 2.5_f32.to_bits()])
            .unwrap();
        left.merge(&right).unwrap();

        let (terms, words) = left.to_words();
        let restored = NonNegativeF32Accumulator::from_words(terms, &words).unwrap();
        assert_eq!(restored, left);
        assert_eq!(restored.boundary().unwrap(), left.boundary().unwrap());

        let mut non_canonical = words.clone();
        non_canonical.push(0);
        assert!(NonNegativeF32Accumulator::from_words(terms, &non_canonical).is_err());
        assert!(NonNegativeF32Accumulator::from_words(0, &[1]).is_err());
    }

    #[test]
    fn accumulator_merge_matches_streaming_exact_sum() {
        let values = [
            f32::from_bits(1).to_bits(),
            1.0_f32.to_bits(),
            1234.5_f32.to_bits(),
            0.0_f32.to_bits(),
        ];
        let mut first = NonNegativeF32Accumulator::default();
        first.extend_bits(values[..2].iter().copied()).unwrap();
        let mut second = NonNegativeF32Accumulator::default();
        second.extend_bits(values[2..].iter().copied()).unwrap();
        first.merge(&second).unwrap();
        assert_eq!(
            first.boundary().unwrap(),
            large_set_boundary_from_f32(values).unwrap()
        );
    }

    #[test]
    fn inferred_relative_chain_rejects_stale_fork() {
        let edge = |before: f32, delta: f32, after: f32| RelativeUpdateEvidence {
            before_bits: before.to_bits(),
            bound_amount_bits: delta.to_bits(),
            after_bits: after.to_bits(),
        };
        let serialized = [edge(100.0, 1.0, 101.0), edge(101.0, 2.0, 103.0)];
        assert_eq!(
            validate_relative_update_chain_from_initial(100.0_f32.to_bits(), &serialized).unwrap(),
            103.0_f32.to_bits()
        );

        let stale_fork = [edge(100.0, 1.0, 101.0), edge(100.0, 2.0, 102.0)];
        assert!(
            validate_relative_update_chain_from_initial(100.0_f32.to_bits(), &stale_fork).is_err()
        );
    }

    fn customer_version(payment_count: i32, delivery_count: i32) -> CustomerLogicalVersion {
        CustomerLogicalVersion {
            payment_count,
            delivery_count,
        }
    }

    fn payment_customer_update(
        before_version: CustomerLogicalVersion,
        before_balance: f32,
        before_ytd: f32,
        amount: f32,
    ) -> CustomerUpdateEvidence {
        CustomerUpdateEvidence {
            kind: CustomerUpdateKind::Payment,
            before_version,
            after_version: customer_version(
                before_version.payment_count + 1,
                before_version.delivery_count,
            ),
            amount_bits: amount.to_bits(),
            balance_before_bits: before_balance.to_bits(),
            balance_after_bits: (before_balance - amount).to_bits(),
            ytd_payment_before_bits: Some(before_ytd.to_bits()),
            ytd_payment_after_bits: Some((before_ytd + amount).to_bits()),
        }
    }

    fn delivery_customer_update(
        before_version: CustomerLogicalVersion,
        before_balance: f32,
        amount: f32,
    ) -> CustomerUpdateEvidence {
        CustomerUpdateEvidence {
            kind: CustomerUpdateKind::Delivery,
            before_version,
            after_version: customer_version(
                before_version.payment_count,
                before_version.delivery_count + 1,
            ),
            amount_bits: amount.to_bits(),
            balance_before_bits: before_balance.to_bits(),
            balance_after_bits: (before_balance + amount).to_bits(),
            ytd_payment_before_bits: None,
            ytd_payment_after_bits: None,
        }
    }

    #[test]
    fn customer_version_pair_replays_mixed_families_in_predecessor_order() {
        let payment_one = payment_customer_update(customer_version(1, 0), -10.0, 10.0, 2.0);
        let delivery = delivery_customer_update(customer_version(2, 0), -12.0, 5.0);
        let payment_two = payment_customer_update(customer_version(2, 1), -7.0, 12.0, 1.0);
        let unordered = [payment_two, payment_one, delivery];

        let endpoint = validate_customer_update_chain(
            (-10.0_f32).to_bits(),
            10.0_f32.to_bits(),
            customer_version(1, 0),
            &unordered,
        )
        .unwrap();
        assert_eq!(endpoint.version, customer_version(3, 1));
        assert_eq!(endpoint.balance_bits, (-8.0_f32).to_bits());
        assert_eq!(endpoint.ytd_payment_bits, 13.0_f32.to_bits());
    }

    #[test]
    fn customer_version_pair_rejects_cross_family_stale_fork_and_broken_chain() {
        let payment = payment_customer_update(customer_version(1, 0), -10.0, 10.0, 2.0);
        let delivery = delivery_customer_update(customer_version(1, 0), -10.0, 5.0);
        assert!(matches!(
            validate_customer_update_chain(
                (-10.0_f32).to_bits(),
                10.0_f32.to_bits(),
                customer_version(1, 0),
                &[payment, delivery],
            ),
            Err(CustomerChainError::DuplicatePredecessor(version))
                if version == customer_version(1, 0)
        ));

        let disconnected = payment_customer_update(customer_version(2, 0), -10.0, 10.0, 2.0);
        assert!(matches!(
            validate_customer_update_chain(
                (-10.0_f32).to_bits(),
                10.0_f32.to_bits(),
                customer_version(1, 0),
                &[disconnected],
            ),
            Err(CustomerChainError::MissingPredecessor(version))
                if version == customer_version(1, 0)
        ));
    }

    #[test]
    fn customer_version_pair_rejects_compensation_and_self_loop_stale_evidence() {
        let mut compensation = payment_customer_update(customer_version(1, 0), -10.0, 10.0, 2.0);
        compensation.after_version = customer_version(1, 0);
        assert!(matches!(
            validate_customer_update_chain(
                (-10.0_f32).to_bits(),
                10.0_f32.to_bits(),
                customer_version(1, 0),
                &[compensation],
            ),
            Err(CustomerChainError::InvalidVersionEdge { .. })
        ));

        let large = 16_777_216.0_f32;
        let self_loop = delivery_customer_update(customer_version(1, 0), large, 1.0);
        assert_eq!(self_loop.balance_before_bits, self_loop.balance_after_bits);
        assert!(matches!(
            validate_customer_update_chain(
                large.to_bits(),
                10.0_f32.to_bits(),
                customer_version(1, 0),
                &[self_loop, self_loop],
            ),
            Err(CustomerChainError::DuplicatePredecessor(version))
                if version == customer_version(1, 0)
        ));
    }

    #[test]
    fn customer_version_pair_rejects_stale_ytd_on_later_payment() {
        let payment = payment_customer_update(customer_version(1, 0), -10.0, 10.0, 2.0);
        let delivery = delivery_customer_update(customer_version(2, 0), -12.0, 5.0);
        let stale_ytd = payment_customer_update(customer_version(2, 1), -7.0, 10.0, 1.0);
        assert!(matches!(
            validate_customer_update_chain(
                (-10.0_f32).to_bits(),
                10.0_f32.to_bits(),
                customer_version(1, 0),
                &[stale_ytd, delivery, payment],
            ),
            Err(CustomerChainError::YtdPredecessorMismatch { .. })
        ));
    }

    #[test]
    fn counter_chain_rejects_duplicate_predecessor() {
        assert_eq!(validate_increment_chain(1, &[(2, 3), (1, 2)]), Ok(3));
        assert_eq!(
            validate_increment_chain(1, &[(1, 2), (1, 2)]),
            Err(CounterChainError::DuplicatePredecessor(1))
        );
        assert!(validate_increment_chain(1, &[(2, 3)]).is_err());
    }
}
