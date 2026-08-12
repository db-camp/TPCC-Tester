//! Seed-derived local approximation of the final-2026 opaque runtime layout.
//!
//! The official seed, identifiers, statement ids, and schedules are private.
//! This module deliberately defines a versioned local domain
//! (`local_seed_opaque_v2`) instead of claiming to reproduce hidden assets.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const RUNTIME_SCHEMA_VERSION: u32 = 1;
pub const OPAQUE_SCHEMA_ALGORITHM_V1: &str = "local_seed_opaque_v1";
pub const OPAQUE_SCHEMA_ALGORITHM: &str = "local_seed_opaque_v2";
pub const CANONICAL_SCHEMA_ALGORITHM: &str = "canonical";

pub const ENCODED_BEGIN_MARKER: &str = "runtime_schema_begin";
pub const ENCODED_END_MARKER: &str = "runtime_schema_end";

const DOMAIN_TABLE_NAMES: &str = "final2026/runtime/table-name/v1";
const DOMAIN_COLUMN_NAMES: &str = "final2026/runtime/column-name/v1";
const DOMAIN_CSV_NAMES: &str = "final2026/runtime/csv-basename/v1";
const DOMAIN_STATEMENT_IDS: &str = "final2026/runtime/statement-id/v1";
const DOMAIN_SUPPLEMENTAL_STATEMENT_IDS: &str =
    "final2026/runtime/supplemental-statement-id/v1";
const DOMAIN_CREATE_ORDER: &str = "final2026/setup/create-order/v1";
const DOMAIN_INDEX_ORDER: &str = "final2026/setup/index-order/v1";
const DOMAIN_LOAD_ORDER: &str = "final2026/setup/load-order/v1";
const DOMAIN_COUNT_ORDER: &str = "final2026/setup/count-order/v1";
const DOMAIN_CHECK_ORDER: &str = "final2026/setup/check-order/v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchemaMode {
    LocalSeedOpaqueV1,
    LocalSeedOpaqueV2,
    Canonical,
}

impl SchemaMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LocalSeedOpaqueV1 => OPAQUE_SCHEMA_ALGORITHM_V1,
            Self::LocalSeedOpaqueV2 => OPAQUE_SCHEMA_ALGORITHM,
            Self::Canonical => CANONICAL_SCHEMA_ALGORITHM,
        }
    }

    const fn is_opaque(self) -> bool {
        matches!(self, Self::LocalSeedOpaqueV1 | Self::LocalSeedOpaqueV2)
    }

    fn parse(value: &str) -> Result<Self, RuntimeSchemaError> {
        match value {
            OPAQUE_SCHEMA_ALGORITHM_V1 => Ok(Self::LocalSeedOpaqueV1),
            OPAQUE_SCHEMA_ALGORITHM => Ok(Self::LocalSeedOpaqueV2),
            CANONICAL_SCHEMA_ALGORITHM => Ok(Self::Canonical),
            _ => Err(RuntimeSchemaError::Invalid(format!(
                "unknown runtime schema mode {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LogicalTable {
    Warehouse,
    Item,
    Stock,
    District,
    Customer,
    History,
    Orders,
    NewOrders,
    OrderLine,
}

impl LogicalTable {
    pub const ALL: [Self; 9] = [
        Self::Warehouse,
        Self::Item,
        Self::Stock,
        Self::District,
        Self::Customer,
        Self::History,
        Self::Orders,
        Self::NewOrders,
        Self::OrderLine,
    ];

    pub const fn canonical(self) -> &'static str {
        match self {
            Self::Warehouse => "warehouse",
            Self::Item => "item",
            Self::Stock => "stock",
            Self::District => "district",
            Self::Customer => "customer",
            Self::History => "history",
            Self::Orders => "orders",
            Self::NewOrders => "new_orders",
            Self::OrderLine => "order_line",
        }
    }

    pub const fn columns(self) -> &'static [&'static str] {
        match self {
            Self::Warehouse => &[
                "w_id",
                "w_name",
                "w_street_1",
                "w_street_2",
                "w_city",
                "w_state",
                "w_zip",
                "w_tax",
                "w_ytd",
            ],
            Self::Item => &["i_id", "i_im_id", "i_name", "i_price", "i_data"],
            Self::Stock => &[
                "s_i_id",
                "s_w_id",
                "s_quantity",
                "s_dist_01",
                "s_dist_02",
                "s_dist_03",
                "s_dist_04",
                "s_dist_05",
                "s_dist_06",
                "s_dist_07",
                "s_dist_08",
                "s_dist_09",
                "s_dist_10",
                "s_ytd",
                "s_order_cnt",
                "s_remote_cnt",
                "s_data",
            ],
            Self::District => &[
                "d_id",
                "d_w_id",
                "d_name",
                "d_street_1",
                "d_street_2",
                "d_city",
                "d_state",
                "d_zip",
                "d_tax",
                "d_ytd",
                "d_next_o_id",
            ],
            Self::Customer => &[
                "c_id",
                "c_d_id",
                "c_w_id",
                "c_first",
                "c_middle",
                "c_last",
                "c_street_1",
                "c_street_2",
                "c_city",
                "c_state",
                "c_zip",
                "c_phone",
                "c_since",
                "c_credit",
                "c_credit_lim",
                "c_discount",
                "c_balance",
                "c_ytd_payment",
                "c_payment_cnt",
                "c_delivery_cnt",
                "c_data",
            ],
            Self::History => &[
                "h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount",
                "h_data",
            ],
            Self::Orders => &[
                "o_id",
                "o_d_id",
                "o_w_id",
                "o_c_id",
                "o_entry_d",
                "o_carrier_id",
                "o_ol_cnt",
                "o_all_local",
            ],
            Self::NewOrders => &["no_o_id", "no_d_id", "no_w_id"],
            Self::OrderLine => &[
                "ol_o_id",
                "ol_d_id",
                "ol_w_id",
                "ol_number",
                "ol_i_id",
                "ol_supply_w_id",
                "ol_delivery_d",
                "ol_quantity",
                "ol_amount",
                "ol_dist_info",
            ],
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|table| table.canonical() == value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LogicalIndex {
    WarehousePrimary,
    DistrictPrimary,
    CustomerPrimary,
    CustomerLastName,
    NewOrdersPrimary,
    OrdersPrimary,
    OrdersCustomer,
    OrderLinePrimary,
    ItemPrimary,
    StockPrimary,
}

impl LogicalIndex {
    pub const ALL: [Self; 10] = [
        Self::WarehousePrimary,
        Self::DistrictPrimary,
        Self::CustomerPrimary,
        Self::CustomerLastName,
        Self::NewOrdersPrimary,
        Self::OrdersPrimary,
        Self::OrdersCustomer,
        Self::OrderLinePrimary,
        Self::ItemPrimary,
        Self::StockPrimary,
    ];

    pub const fn ordinal(self) -> u8 {
        match self {
            Self::WarehousePrimary => 0,
            Self::DistrictPrimary => 1,
            Self::CustomerPrimary => 2,
            Self::CustomerLastName => 3,
            Self::NewOrdersPrimary => 4,
            Self::OrdersPrimary => 5,
            Self::OrdersCustomer => 6,
            Self::OrderLinePrimary => 7,
            Self::ItemPrimary => 8,
            Self::StockPrimary => 9,
        }
    }

    fn from_ordinal(value: u8) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|index| index.ordinal() == value)
    }
}

/// Stable logical keys for the 42 prepared statements in the public workload.
///
/// Numeric ids are intentionally absent: the opaque layout assigns them.
pub const FINAL2026_STATEMENT_KEYS: [&str; 42] = [
    "begin",
    "commit",
    "abort",
    "new_order.home",
    "new_order.lock_stock",
    "new_order.item",
    "new_order.stock",
    "new_order.advance_district",
    "new_order.insert_order",
    "new_order.insert_queue",
    "new_order.update_stock_normal",
    "new_order.update_stock_wrapped",
    "new_order.insert_line",
    "payment.warehouse",
    "payment.update_warehouse",
    "payment.district",
    "payment.update_district",
    "payment.customer_by_id",
    "payment.customer_by_last",
    "payment.update_good_customer",
    "payment.update_bad_customer",
    "payment.insert_history",
    "payment.customer_after",
    "order_status.customer_by_id",
    "order_status.customer_by_last",
    "order_status.latest_order",
    "order_status.order",
    "order_status.lines",
    "delivery.oldest_order",
    "delivery.lock_queue",
    "delivery.confirm_queue",
    "delivery.order",
    "delivery.customer",
    "delivery.line_rows",
    "delivery.line_sum",
    "delivery.delete_queue",
    "delivery.update_order",
    "delivery.update_lines",
    "delivery.update_customer",
    "delivery.customer_after",
    "stock_level.next_order",
    "stock_level.count",
];

const CANONICAL_STATEMENT_IDS: [u16; 42] = [
    1, 2, 3, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 50,
    51, 52, 53, 54, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 89, 90,
];

/// Runtime-only prepared statements layered over the persisted 42-key layout.
///
/// These keys are deliberately excluded from `RuntimeSchema::encode` and its
/// fingerprint. Their ids are reconstructed deterministically from the
/// persisted base mapping so existing schema caches remain byte-for-byte
/// compatible.
pub const FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS: [&str; 14] = [
    "delivery.earlier_queue_count",
    "delivery.exact_queue_count",
    "new_order.stock_d02",
    "new_order.stock_d03",
    "new_order.stock_d04",
    "new_order.stock_d05",
    "new_order.stock_d06",
    "new_order.stock_d07",
    "new_order.stock_d08",
    "new_order.stock_d09",
    "new_order.stock_d10",
    "preflight.new_order_stock_version",
    "payment.warehouse_after",
    "payment.district_after",
];

const CANONICAL_SUPPLEMENTAL_STATEMENT_IDS: [u16; 14] =
    [82, 83, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102];

pub const SETUP_CHECK_KEYS: [&str; 18] = [
    "setup.orders.sum_o_ol_cnt",
    "setup.stock.quantity_range",
    "setup.orders.line_count_range",
    "setup.order_line.quantity",
    "setup.orders.carrier_range",
    "setup.orders.open_carrier_count",
    "setup.order_line.undelivered_count",
    "setup.stock.sum_ytd",
    "setup.stock.sum_order_cnt",
    "setup.stock.sum_remote_cnt",
    "setup.stock.nonzero_ytd",
    "setup.stock.nonzero_order_cnt",
    "setup.stock.nonzero_remote_cnt",
    "setup.district.key_range",
    "setup.customer.key_range",
    "setup.orders.key_range",
    "setup.order_line.key_range",
    "setup.stock.key_range",
];

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetupSchedule {
    create_tables: Vec<LogicalTable>,
    create_indexes: Vec<LogicalIndex>,
    load_tables: Vec<LogicalTable>,
    count_tables: Vec<LogicalTable>,
    setup_checks: Vec<&'static str>,
}

impl SetupSchedule {
    pub fn create_tables(&self) -> &[LogicalTable] {
        &self.create_tables
    }

    pub fn create_indexes(&self) -> &[LogicalIndex] {
        &self.create_indexes
    }

    pub fn load_tables(&self) -> &[LogicalTable] {
        &self.load_tables
    }

    pub fn count_tables(&self) -> &[LogicalTable] {
        &self.count_tables
    }

    pub fn setup_checks(&self) -> &[&'static str] {
        &self.setup_checks
    }

    fn derive(seed: u64, mode: SchemaMode) -> Self {
        let mut schedule = Self {
            create_tables: LogicalTable::ALL.to_vec(),
            create_indexes: LogicalIndex::ALL.to_vec(),
            load_tables: LogicalTable::ALL.to_vec(),
            count_tables: LogicalTable::ALL.to_vec(),
            setup_checks: SETUP_CHECK_KEYS.to_vec(),
        };
        if mode.is_opaque() {
            deterministic_shuffle(
                &mut schedule.create_tables,
                domain_seed(seed, DOMAIN_CREATE_ORDER),
            );
            deterministic_shuffle(
                &mut schedule.create_indexes,
                domain_seed(seed, DOMAIN_INDEX_ORDER),
            );
            deterministic_shuffle(
                &mut schedule.load_tables,
                domain_seed(seed, DOMAIN_LOAD_ORDER),
            );
            deterministic_shuffle(
                &mut schedule.count_tables,
                domain_seed(seed, DOMAIN_COUNT_ORDER),
            );
            deterministic_shuffle(
                &mut schedule.setup_checks,
                domain_seed(seed, DOMAIN_CHECK_ORDER),
            );
        }
        schedule
    }

    fn validate(&self) -> Result<(), RuntimeSchemaError> {
        validate_permutation(
            "CREATE TABLE schedule",
            &self.create_tables,
            &LogicalTable::ALL,
        )?;
        validate_permutation(
            "CREATE INDEX schedule",
            &self.create_indexes,
            &LogicalIndex::ALL,
        )?;
        validate_permutation("LOAD schedule", &self.load_tables, &LogicalTable::ALL)?;
        validate_permutation("COUNT schedule", &self.count_tables, &LogicalTable::ALL)?;
        validate_permutation(
            "setup check schedule",
            &self.setup_checks,
            &SETUP_CHECK_KEYS,
        )?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatementLayout {
    ids: BTreeMap<String, u16>,
    supplemental_ids: BTreeMap<String, u16>,
}

impl StatementLayout {
    fn derive(seed: u64, mode: SchemaMode) -> Result<Self, RuntimeSchemaError> {
        let mut ids = BTreeMap::new();
        let mut used = BTreeSet::new();
        for (ordinal, key) in FINAL2026_STATEMENT_KEYS.iter().enumerate() {
            let id = match mode {
                SchemaMode::Canonical => CANONICAL_STATEMENT_IDS[ordinal],
                SchemaMode::LocalSeedOpaqueV1 | SchemaMode::LocalSeedOpaqueV2 => {
                    derive_unique_statement_id(seed, key, &used)
                }
            };
            if id == 0 || !used.insert(id) || ids.insert((*key).to_owned(), id).is_some() {
                return Err(RuntimeSchemaError::Invalid(
                    "statement ids must be non-zero and unique".to_owned(),
                ));
            }
        }
        Self::from_base(ids, mode)
    }

    fn from_base(
        ids: BTreeMap<String, u16>,
        mode: SchemaMode,
    ) -> Result<Self, RuntimeSchemaError> {
        let supplemental_ids = derive_supplemental_statement_ids(&ids, mode)?;
        let layout = Self {
            ids,
            supplemental_ids,
        };
        layout.validate(mode)?;
        Ok(layout)
    }

    pub fn id(&self, key: &str) -> Result<u16, RuntimeSchemaError> {
        self.ids
            .get(key)
            .or_else(|| self.supplemental_ids.get(key))
            .copied()
            .ok_or_else(|| {
                RuntimeSchemaError::Invalid(format!("unknown logical statement key {key:?}"))
            })
    }

    pub fn entries(&self) -> impl Iterator<Item = (&str, u16)> {
        self.ids.iter().map(|(key, id)| (key.as_str(), *id))
    }

    fn validate(&self, mode: SchemaMode) -> Result<(), RuntimeSchemaError> {
        if self.ids.len() != FINAL2026_STATEMENT_KEYS.len() {
            return Err(RuntimeSchemaError::Invalid(format!(
                "statement layout has {} entries, expected {}",
                self.ids.len(),
                FINAL2026_STATEMENT_KEYS.len()
            )));
        }
        let mut ids = BTreeSet::new();
        for key in FINAL2026_STATEMENT_KEYS {
            let id = self.id(key)?;
            if id == 0 || !ids.insert(id) {
                return Err(RuntimeSchemaError::Invalid(
                    "statement ids must be non-zero and unique".to_owned(),
                ));
            }
        }
        if self.supplemental_ids.len() != FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS.len() {
            return Err(RuntimeSchemaError::Invalid(format!(
                "supplemental statement layout has {} entries, expected {}",
                self.supplemental_ids.len(),
                FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS.len()
            )));
        }
        for key in FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS {
            let id = self.id(key)?;
            if id == 0 || !ids.insert(id) {
                return Err(RuntimeSchemaError::Invalid(
                    "base and supplemental statement ids must be non-zero and unique".to_owned(),
                ));
            }
        }
        if self.supplemental_ids != derive_supplemental_statement_ids(&self.ids, mode)? {
            return Err(RuntimeSchemaError::Invalid(
                "supplemental statement ids do not match the persisted base mapping".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeSchema {
    version: u32,
    mode: SchemaMode,
    seed: u64,
    tables: BTreeMap<String, String>,
    columns: BTreeMap<String, String>,
    csv_basenames: BTreeMap<String, String>,
    statements: StatementLayout,
    schedule: SetupSchedule,
    fingerprint: u64,
}

impl RuntimeSchema {
    pub fn derive(seed: u64, mode: SchemaMode) -> Result<Self, RuntimeSchemaError> {
        let mut used_identifiers = BTreeSet::new();
        let mut used_csv_stems = BTreeSet::new();
        let mut tables = BTreeMap::new();
        let mut columns = BTreeMap::new();
        let mut csv_basenames = BTreeMap::new();

        for table in LogicalTable::ALL {
            let logical = table.canonical();
            let runtime = match mode {
                SchemaMode::Canonical => logical.to_owned(),
                SchemaMode::LocalSeedOpaqueV1 | SchemaMode::LocalSeedOpaqueV2 => {
                    derive_unique_identifier(
                        seed,
                        DOMAIN_TABLE_NAMES,
                        logical,
                        't',
                        mode,
                        &used_identifiers,
                    )
                }
            };
            used_identifiers.insert(runtime.clone());
            tables.insert(logical.to_owned(), runtime);

            let csv = match mode {
                SchemaMode::Canonical => format!("{logical}.csv"),
                SchemaMode::LocalSeedOpaqueV1 | SchemaMode::LocalSeedOpaqueV2 => {
                    format!(
                        "{}.csv",
                        derive_unique_identifier(
                            seed,
                            DOMAIN_CSV_NAMES,
                            logical,
                            'f',
                            mode,
                            &used_csv_stems,
                        )
                    )
                }
            };
            used_csv_stems.insert(
                csv.strip_suffix(".csv")
                    .expect("derived CSV basename lost its suffix")
                    .to_owned(),
            );
            csv_basenames.insert(logical.to_owned(), csv);

            for column in table.columns() {
                let runtime = match mode {
                    SchemaMode::Canonical => (*column).to_owned(),
                    SchemaMode::LocalSeedOpaqueV1 | SchemaMode::LocalSeedOpaqueV2 => {
                        derive_unique_identifier(
                            seed,
                            DOMAIN_COLUMN_NAMES,
                            &format!("{logical}.{column}"),
                            'c',
                            mode,
                            &used_identifiers,
                        )
                    }
                };
                used_identifiers.insert(runtime.clone());
                columns.insert((*column).to_owned(), runtime);
            }
        }

        let mut schema = Self {
            version: RUNTIME_SCHEMA_VERSION,
            mode,
            seed,
            tables,
            columns,
            csv_basenames,
            statements: StatementLayout::derive(seed, mode)?,
            schedule: SetupSchedule::derive(seed, mode),
            fingerprint: 0,
        };
        schema.validate_without_fingerprint()?;
        schema.fingerprint = schema.compute_fingerprint();
        schema.validate()?;
        Ok(schema)
    }

    pub fn opaque(seed: u64) -> Result<Self, RuntimeSchemaError> {
        Self::derive(seed, SchemaMode::LocalSeedOpaqueV2)
    }

    pub fn canonical(seed: u64) -> Result<Self, RuntimeSchemaError> {
        Self::derive(seed, SchemaMode::Canonical)
    }

    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn mode(&self) -> SchemaMode {
        self.mode
    }

    pub const fn seed(&self) -> u64 {
        self.seed
    }

    pub const fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    pub fn schedule(&self) -> &SetupSchedule {
        &self.schedule
    }

    pub fn statements(&self) -> &StatementLayout {
        &self.statements
    }

    pub fn table(&self, table: LogicalTable) -> &str {
        self.tables
            .get(table.canonical())
            .expect("validated runtime schema lost a table")
    }

    pub fn column(
        &self,
        table: LogicalTable,
        logical_column: &str,
    ) -> Result<&str, RuntimeSchemaError> {
        if !table.columns().contains(&logical_column) {
            return Err(RuntimeSchemaError::Invalid(format!(
                "{logical_column:?} is not a column of {}",
                table.canonical()
            )));
        }
        self.columns
            .get(logical_column)
            .map(String::as_str)
            .ok_or_else(|| {
                RuntimeSchemaError::Invalid(format!(
                    "runtime schema is missing column {logical_column:?}"
                ))
            })
    }

    pub fn columns(&self, table: LogicalTable) -> Result<Vec<&str>, RuntimeSchemaError> {
        table
            .columns()
            .iter()
            .map(|column| self.column(table, column))
            .collect()
    }

    pub fn csv_basename(&self, table: LogicalTable) -> &str {
        self.csv_basenames
            .get(table.canonical())
            .expect("validated runtime schema lost a CSV basename")
    }

    /// Rewrite complete SQL identifier tokens while preserving quoted text.
    pub fn render_sql(&self, logical_sql: &str) -> String {
        let mut output = String::with_capacity(logical_sql.len());
        let bytes = logical_sql.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'\'' | b'"' => {
                    let quote = bytes[index];
                    let start = index;
                    index += 1;
                    while index < bytes.len() {
                        if bytes[index] == quote {
                            index += 1;
                            if index < bytes.len() && bytes[index] == quote {
                                index += 1;
                                continue;
                            }
                            break;
                        }
                        index += 1;
                    }
                    output.push_str(&logical_sql[start..index]);
                }
                b'-' if bytes.get(index + 1) == Some(&b'-') => {
                    let start = index;
                    index += 2;
                    while index < bytes.len() && bytes[index] != b'\n' {
                        index += 1;
                    }
                    output.push_str(&logical_sql[start..index]);
                }
                b'/' if bytes.get(index + 1) == Some(&b'*') => {
                    let start = index;
                    index += 2;
                    while index + 1 < bytes.len()
                        && !(bytes[index] == b'*' && bytes[index + 1] == b'/')
                    {
                        index += 1;
                    }
                    index = (index + 2).min(bytes.len());
                    output.push_str(&logical_sql[start..index]);
                }
                byte if is_identifier_start(byte) => {
                    let start = index;
                    index += 1;
                    while index < bytes.len() && is_identifier_continue(bytes[index]) {
                        index += 1;
                    }
                    let token = &logical_sql[start..index];
                    let logical = token.to_ascii_lowercase();
                    if let Some(runtime) = self
                        .tables
                        .get(&logical)
                        .or_else(|| self.columns.get(&logical))
                    {
                        output.push_str(runtime);
                    } else {
                        output.push_str(token);
                    }
                }
                _ => {
                    output.push(bytes[index] as char);
                    index += 1;
                }
            }
        }
        output
    }

    pub fn encode(&self) -> String {
        let mut output = String::new();
        output.push_str(ENCODED_BEGIN_MARKER);
        output.push('\n');
        output.push_str(&format!("version={}\n", self.version));
        output.push_str(&format!("mode={}\n", self.mode.as_str()));
        output.push_str(&format!("seed={}\n", self.seed));
        output.push_str(&format!("fingerprint={:016x}\n", self.fingerprint));
        for table in LogicalTable::ALL {
            output.push_str(&format!(
                "table={},{},{}\n",
                table.canonical(),
                self.table(table),
                self.csv_basename(table)
            ));
        }
        for table in LogicalTable::ALL {
            for column in table.columns() {
                output.push_str(&format!(
                    "column={},{},{}\n",
                    table.canonical(),
                    column,
                    self.column(table, column)
                        .expect("validated runtime schema lost a column")
                ));
            }
        }
        for key in FINAL2026_STATEMENT_KEYS {
            output.push_str(&format!(
                "statement={},{}\n",
                key,
                self.statements
                    .id(key)
                    .expect("validated runtime schema lost a statement")
            ));
        }
        output.push_str(&format!(
            "create_order={}\n",
            encode_tables(self.schedule.create_tables())
        ));
        output.push_str(&format!(
            "index_order={}\n",
            self.schedule
                .create_indexes()
                .iter()
                .map(|index| index.ordinal().to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
        output.push_str(&format!(
            "load_order={}\n",
            encode_tables(self.schedule.load_tables())
        ));
        output.push_str(&format!(
            "count_order={}\n",
            encode_tables(self.schedule.count_tables())
        ));
        output.push_str(&format!(
            "check_order={}\n",
            self.schedule.setup_checks().join(",")
        ));
        output.push_str(ENCODED_END_MARKER);
        output.push('\n');
        output
    }

    pub fn decode(input: &str) -> Result<Self, RuntimeSchemaError> {
        let mut lines = input.lines();
        expect_line(&mut lines, ENCODED_BEGIN_MARKER)?;
        let version = parse_field::<u32>(&mut lines, "version")?;
        if version != RUNTIME_SCHEMA_VERSION {
            return Err(RuntimeSchemaError::Invalid(format!(
                "runtime schema version mismatch: expected {RUNTIME_SCHEMA_VERSION}, got {version}"
            )));
        }
        let mode = SchemaMode::parse(field(&mut lines, "mode")?)?;
        let seed = parse_field::<u64>(&mut lines, "seed")?;
        let fingerprint = parse_lower_hex_u64(field(&mut lines, "fingerprint")?)?;

        let mut tables = BTreeMap::new();
        let mut csv_basenames = BTreeMap::new();
        for expected in LogicalTable::ALL {
            let parts = record(&mut lines, "table", 3)?;
            if parts[0] != expected.canonical() {
                return Err(RuntimeSchemaError::Invalid(format!(
                    "expected table {}, got {:?}",
                    expected.canonical(),
                    parts[0]
                )));
            }
            tables.insert(parts[0].to_owned(), parts[1].to_owned());
            csv_basenames.insert(parts[0].to_owned(), parts[2].to_owned());
        }

        let mut columns = BTreeMap::new();
        for table in LogicalTable::ALL {
            for expected_column in table.columns() {
                let parts = record(&mut lines, "column", 3)?;
                if parts[0] != table.canonical() || parts[1] != *expected_column {
                    return Err(RuntimeSchemaError::Invalid(format!(
                        "expected column {}.{}, got {}.{}",
                        table.canonical(),
                        expected_column,
                        parts[0],
                        parts[1]
                    )));
                }
                columns.insert(parts[1].to_owned(), parts[2].to_owned());
            }
        }

        let mut statement_ids = BTreeMap::new();
        for expected_key in FINAL2026_STATEMENT_KEYS {
            let parts = record(&mut lines, "statement", 2)?;
            if parts[0] != expected_key {
                return Err(RuntimeSchemaError::Invalid(format!(
                    "expected statement key {expected_key:?}, got {:?}",
                    parts[0]
                )));
            }
            let id = parts[1].parse::<u16>().map_err(|_| {
                RuntimeSchemaError::Invalid(format!("statement id {:?} is not a u16", parts[1]))
            })?;
            statement_ids.insert(parts[0].to_owned(), id);
        }

        let schedule = SetupSchedule {
            create_tables: decode_tables(field(&mut lines, "create_order")?)?,
            create_indexes: decode_indexes(field(&mut lines, "index_order")?)?,
            load_tables: decode_tables(field(&mut lines, "load_order")?)?,
            count_tables: decode_tables(field(&mut lines, "count_order")?)?,
            setup_checks: decode_checks(field(&mut lines, "check_order")?)?,
        };
        expect_line(&mut lines, ENCODED_END_MARKER)?;
        if lines.next().is_some() {
            return Err(RuntimeSchemaError::Invalid(
                "runtime schema contains trailing fields".to_owned(),
            ));
        }

        let schema = Self {
            version,
            mode,
            seed,
            tables,
            columns,
            csv_basenames,
            statements: StatementLayout::from_base(statement_ids, mode)?,
            schedule,
            fingerprint,
        };
        schema.validate()?;
        if schema.mode == SchemaMode::LocalSeedOpaqueV2 {
            let derived = Self::derive(schema.seed, SchemaMode::LocalSeedOpaqueV2)?;
            if schema != derived {
                return Err(RuntimeSchemaError::Invalid(
                    "local_seed_opaque_v2 mapping is not the deterministic seed-derived layout"
                        .to_owned(),
                ));
            }
        }
        Ok(schema)
    }

    pub fn validate(&self) -> Result<(), RuntimeSchemaError> {
        self.validate_without_fingerprint()?;
        let expected = self.compute_fingerprint();
        if self.fingerprint != expected {
            return Err(RuntimeSchemaError::Invalid(format!(
                "runtime schema fingerprint mismatch: encoded={:016x}, computed={expected:016x}",
                self.fingerprint
            )));
        }
        Ok(())
    }

    fn validate_without_fingerprint(&self) -> Result<(), RuntimeSchemaError> {
        if self.version != RUNTIME_SCHEMA_VERSION {
            return Err(RuntimeSchemaError::Invalid(
                "unsupported runtime schema version".to_owned(),
            ));
        }
        if self.tables.len() != LogicalTable::ALL.len()
            || self.csv_basenames.len() != LogicalTable::ALL.len()
            || self.columns.len() != 92
        {
            return Err(RuntimeSchemaError::Invalid(format!(
                "runtime schema coverage is {}/{}/{}, expected 9/9/92",
                self.tables.len(),
                self.csv_basenames.len(),
                self.columns.len()
            )));
        }

        let mut identifiers = BTreeSet::new();
        let mut csv_names = BTreeSet::new();
        for table in LogicalTable::ALL {
            let runtime = self.tables.get(table.canonical()).ok_or_else(|| {
                RuntimeSchemaError::Invalid(format!("missing table {}", table.canonical()))
            })?;
            validate_identifier(runtime)?;
            if !identifiers.insert(runtime.clone()) {
                return Err(RuntimeSchemaError::Invalid(format!(
                    "duplicate runtime identifier {runtime:?}"
                )));
            }
            let csv = self.csv_basenames.get(table.canonical()).ok_or_else(|| {
                RuntimeSchemaError::Invalid(format!(
                    "missing CSV basename for {}",
                    table.canonical()
                ))
            })?;
            validate_csv_basename(csv)?;
            if !csv_names.insert(csv.clone()) {
                return Err(RuntimeSchemaError::Invalid(format!(
                    "duplicate CSV basename {csv:?}"
                )));
            }
            for column in table.columns() {
                let runtime = self.columns.get(*column).ok_or_else(|| {
                    RuntimeSchemaError::Invalid(format!("missing column {column}"))
                })?;
                validate_identifier(runtime)?;
                if !identifiers.insert(runtime.clone()) {
                    return Err(RuntimeSchemaError::Invalid(format!(
                        "duplicate runtime identifier {runtime:?}"
                    )));
                }
            }
        }
        if self.mode.is_opaque() {
            let logical_identifiers = LogicalTable::ALL
                .iter()
                .flat_map(|table| {
                    std::iter::once(table.canonical()).chain(table.columns().iter().copied())
                })
                .collect::<BTreeSet<_>>();
            if identifiers
                .iter()
                .any(|runtime| logical_identifiers.contains(runtime.as_str()))
            {
                return Err(RuntimeSchemaError::Invalid(
                    "opaque runtime schema exposes a canonical identifier".to_owned(),
                ));
            }
        }
        self.statements.validate(self.mode)?;
        self.schedule.validate()?;
        Ok(())
    }

    fn compute_fingerprint(&self) -> u64 {
        let mut hash = FNV_OFFSET_BASIS;
        hash = hash_bytes(hash, &self.version.to_be_bytes());
        hash = hash_bytes(hash, self.mode.as_str().as_bytes());
        hash = hash_bytes(hash, &self.seed.to_be_bytes());
        for table in LogicalTable::ALL {
            hash = hash_bytes(hash, table.canonical().as_bytes());
            hash = hash_bytes(hash, self.table(table).as_bytes());
            hash = hash_bytes(hash, self.csv_basename(table).as_bytes());
            for column in table.columns() {
                hash = hash_bytes(hash, column.as_bytes());
                hash = hash_bytes(
                    hash,
                    self.columns
                        .get(*column)
                        .expect("validated runtime schema lost a column")
                        .as_bytes(),
                );
            }
        }
        for key in FINAL2026_STATEMENT_KEYS {
            hash = hash_bytes(hash, key.as_bytes());
            hash = hash_bytes(
                hash,
                &self
                    .statements
                    .id(key)
                    .expect("validated runtime schema lost a statement")
                    .to_be_bytes(),
            );
        }
        hash = hash_bytes(
            hash,
            encode_tables(self.schedule.create_tables()).as_bytes(),
        );
        for index in self.schedule.create_indexes() {
            hash = hash_bytes(hash, &[index.ordinal()]);
        }
        hash = hash_bytes(hash, encode_tables(self.schedule.load_tables()).as_bytes());
        hash = hash_bytes(hash, encode_tables(self.schedule.count_tables()).as_bytes());
        hash = hash_bytes(hash, self.schedule.setup_checks().join(",").as_bytes());
        hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeSchemaError {
    Invalid(String),
}

impl fmt::Display for RuntimeSchemaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for RuntimeSchemaError {}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn hash_bytes(hash: u64, bytes: &[u8]) -> u64 {
    bytes.iter().fold(hash, |state, byte| {
        (state ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

fn domain_seed(seed: u64, domain: &str) -> u64 {
    let hash = hash_bytes(
        hash_bytes(FNV_OFFSET_BASIS, &seed.to_be_bytes()),
        domain.as_bytes(),
    );
    splitmix64(hash)
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn derive_unique_identifier(
    seed: u64,
    domain: &str,
    logical: &str,
    prefix: char,
    mode: SchemaMode,
    used: &BTreeSet<String>,
) -> String {
    for probe in 0_u64.. {
        let mut value = domain_seed(seed, domain);
        value = hash_bytes(value, logical.as_bytes());
        value = hash_bytes(value, &probe.to_be_bytes());
        let digest = splitmix64(value);
        let candidate = match mode {
            SchemaMode::LocalSeedOpaqueV1 => format!("{prefix}{digest:016x}"),
            SchemaMode::LocalSeedOpaqueV2 => {
                format!("{prefix}{:014x}", digest & 0x00ff_ffff_ffff_ffff)
            }
            SchemaMode::Canonical => unreachable!("canonical identifiers are not derived"),
        };
        if !used.contains(&candidate) && !is_reserved(&candidate) {
            return candidate;
        }
    }
    unreachable!("u64 identifier namespace exhausted")
}

fn derive_unique_statement_id(seed: u64, key: &str, used: &BTreeSet<u16>) -> u16 {
    let mut value = domain_seed(seed, DOMAIN_STATEMENT_IDS);
    value = hash_bytes(value, key.as_bytes());
    for probe in 0_u64.. {
        let candidate = (splitmix64(value ^ probe) & u64::from(u16::MAX)) as u16;
        if candidate != 0 && !used.contains(&candidate) {
            return candidate;
        }
    }
    unreachable!("u16 statement-id namespace exhausted")
}

fn derive_supplemental_statement_ids(
    base_ids: &BTreeMap<String, u16>,
    mode: SchemaMode,
) -> Result<BTreeMap<String, u16>, RuntimeSchemaError> {
    if base_ids.len() != FINAL2026_STATEMENT_KEYS.len() {
        return Err(RuntimeSchemaError::Invalid(format!(
            "cannot derive supplemental statement ids from {} base entries, expected {}",
            base_ids.len(),
            FINAL2026_STATEMENT_KEYS.len()
        )));
    }

    let mut mapping_seed = FNV_OFFSET_BASIS;
    let mut used = BTreeSet::new();
    for key in FINAL2026_STATEMENT_KEYS {
        let id = base_ids.get(key).copied().ok_or_else(|| {
            RuntimeSchemaError::Invalid(format!(
                "cannot derive supplemental statement ids without base key {key:?}"
            ))
        })?;
        if id == 0 || !used.insert(id) {
            return Err(RuntimeSchemaError::Invalid(
                "base statement ids must be non-zero and unique".to_owned(),
            ));
        }
        mapping_seed = hash_bytes(mapping_seed, key.as_bytes());
        mapping_seed = hash_bytes(mapping_seed, &id.to_be_bytes());
    }
    mapping_seed = domain_seed(mapping_seed, DOMAIN_SUPPLEMENTAL_STATEMENT_IDS);

    let mut supplemental_ids = BTreeMap::new();
    for (ordinal, key) in FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS
        .iter()
        .enumerate()
    {
        let id = match mode {
            SchemaMode::Canonical => CANONICAL_SUPPLEMENTAL_STATEMENT_IDS[ordinal],
            SchemaMode::LocalSeedOpaqueV1 | SchemaMode::LocalSeedOpaqueV2 => {
                let key_hash = hash_bytes(mapping_seed, key.as_bytes());
                let start = (splitmix64(key_hash) % u64::from(u16::MAX) + 1) as u16;
                first_free_statement_id(start, &used)?
            }
        };
        if id == 0
            || !used.insert(id)
            || supplemental_ids.insert((*key).to_owned(), id).is_some()
        {
            return Err(RuntimeSchemaError::Invalid(
                "base and supplemental statement ids must be non-zero and unique".to_owned(),
            ));
        }
    }
    Ok(supplemental_ids)
}

fn first_free_statement_id(
    start: u16,
    used: &BTreeSet<u16>,
) -> Result<u16, RuntimeSchemaError> {
    if start == 0 {
        return Err(RuntimeSchemaError::Invalid(
            "statement-id probe must start in 1..=65535".to_owned(),
        ));
    }
    for offset in 0..u32::from(u16::MAX) {
        let candidate = ((u32::from(start) - 1 + offset) % u32::from(u16::MAX) + 1) as u16;
        if !used.contains(&candidate) {
            return Ok(candidate);
        }
    }
    Err(RuntimeSchemaError::Invalid(
        "u16 statement-id namespace exhausted".to_owned(),
    ))
}

fn deterministic_shuffle<T>(values: &mut [T], seed: u64) {
    let mut state = seed;
    for upper in (1..values.len()).rev() {
        state = splitmix64(state);
        let selected = (state % (upper as u64 + 1)) as usize;
        values.swap(upper, selected);
    }
}

fn validate_permutation<T>(
    name: &str,
    actual: &[T],
    expected: &[T],
) -> Result<(), RuntimeSchemaError>
where
    T: Ord + Clone,
{
    let actual_set = actual.iter().cloned().collect::<BTreeSet<_>>();
    let expected_set = expected.iter().cloned().collect::<BTreeSet<_>>();
    if actual.len() != expected.len()
        || actual_set.len() != actual.len()
        || actual_set != expected_set
    {
        return Err(RuntimeSchemaError::Invalid(format!(
            "{name} is not a complete permutation"
        )));
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), RuntimeSchemaError> {
    if value.len() > 63
        || !value
            .as_bytes()
            .first()
            .is_some_and(|byte| is_identifier_start(*byte))
        || !value.as_bytes().iter().copied().all(is_identifier_continue)
        || is_reserved(value)
    {
        return Err(RuntimeSchemaError::Invalid(format!(
            "unsafe SQL identifier {value:?}"
        )));
    }
    Ok(())
}

fn validate_csv_basename(value: &str) -> Result<(), RuntimeSchemaError> {
    let stem = value.strip_suffix(".csv").ok_or_else(|| {
        RuntimeSchemaError::Invalid(format!("CSV basename must end in .csv: {value:?}"))
    })?;
    if value.len() > 68
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.as_bytes().contains(&0)
    {
        return Err(RuntimeSchemaError::Invalid(format!(
            "unsafe CSV basename {value:?}"
        )));
    }
    validate_identifier(stem)
}

fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

fn is_reserved(value: &str) -> bool {
    const RESERVED: &[&str] = &[
        "abort",
        "and",
        "as",
        "begin",
        "by",
        "char",
        "commit",
        "count",
        "create",
        "delete",
        "distinct",
        "float",
        "from",
        "group",
        "index",
        "insert",
        "int",
        "into",
        "limit",
        "load",
        "max",
        "min",
        "or",
        "order",
        "select",
        "set",
        "show",
        "sum",
        "table",
        "transaction",
        "update",
        "values",
        "where",
    ];
    RESERVED
        .binary_search(&value.to_ascii_lowercase().as_str())
        .is_ok()
}

fn encode_tables(tables: &[LogicalTable]) -> String {
    tables
        .iter()
        .map(|table| table.canonical())
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_tables(value: &str) -> Result<Vec<LogicalTable>, RuntimeSchemaError> {
    value
        .split(',')
        .map(|logical| {
            LogicalTable::parse(logical).ok_or_else(|| {
                RuntimeSchemaError::Invalid(format!("unknown logical table {logical:?}"))
            })
        })
        .collect()
}

fn decode_indexes(value: &str) -> Result<Vec<LogicalIndex>, RuntimeSchemaError> {
    value
        .split(',')
        .map(|ordinal| {
            let ordinal = ordinal.parse::<u8>().map_err(|_| {
                RuntimeSchemaError::Invalid(format!("invalid logical index ordinal {ordinal:?}"))
            })?;
            LogicalIndex::from_ordinal(ordinal).ok_or_else(|| {
                RuntimeSchemaError::Invalid(format!("unknown logical index ordinal {ordinal}"))
            })
        })
        .collect()
}

fn decode_checks(value: &str) -> Result<Vec<&'static str>, RuntimeSchemaError> {
    value
        .split(',')
        .map(|key| {
            SETUP_CHECK_KEYS
                .iter()
                .copied()
                .find(|expected| *expected == key)
                .ok_or_else(|| {
                    RuntimeSchemaError::Invalid(format!("unknown setup check key {key:?}"))
                })
        })
        .collect()
}

fn expect_line<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    expected: &str,
) -> Result<(), RuntimeSchemaError> {
    let actual = lines.next().ok_or_else(|| {
        RuntimeSchemaError::Invalid(format!("missing runtime schema marker {expected:?}"))
    })?;
    if actual != expected {
        return Err(RuntimeSchemaError::Invalid(format!(
            "expected runtime schema marker {expected:?}, got {actual:?}"
        )));
    }
    Ok(())
}

fn field<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    key: &str,
) -> Result<&'a str, RuntimeSchemaError> {
    let line = lines
        .next()
        .ok_or_else(|| RuntimeSchemaError::Invalid(format!("missing runtime schema {key}")))?;
    line.strip_prefix(&format!("{key}=")).ok_or_else(|| {
        RuntimeSchemaError::Invalid(format!("expected runtime schema {key}, got {line:?}"))
    })
}

fn parse_field<'a, T: std::str::FromStr>(
    lines: &mut impl Iterator<Item = &'a str>,
    key: &str,
) -> Result<T, RuntimeSchemaError> {
    field(lines, key)?.parse().map_err(|_| {
        RuntimeSchemaError::Invalid(format!("runtime schema {key} is not a valid number"))
    })
}

fn record<'a>(
    lines: &mut impl Iterator<Item = &'a str>,
    key: &str,
    fields: usize,
) -> Result<Vec<&'a str>, RuntimeSchemaError> {
    let values = field(lines, key)?.split(',').collect::<Vec<_>>();
    if values.len() != fields || values.iter().any(|value| value.is_empty()) {
        return Err(RuntimeSchemaError::Invalid(format!(
            "runtime schema {key} must contain {fields} non-empty fields"
        )));
    }
    Ok(values)
}

fn parse_lower_hex_u64(value: &str) -> Result<u64, RuntimeSchemaError> {
    if value.len() != 16
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RuntimeSchemaError::Invalid(
            "runtime schema fingerprint must be 16 lower-case hex digits".to_owned(),
        ));
    }
    u64::from_str_radix(value, 16)
        .map_err(|_| RuntimeSchemaError::Invalid("invalid runtime schema fingerprint".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_schema_is_complete_safe_and_reproducible() {
        let left = RuntimeSchema::opaque(73).unwrap();
        let right = RuntimeSchema::opaque(73).unwrap();
        assert_eq!(left, right);
        assert_eq!(left.tables.len(), 9);
        assert_eq!(left.columns.len(), 92);
        assert_eq!(left.csv_basenames.len(), 9);
        assert_eq!(left.statements.ids.len(), 42);
        assert_eq!(left.statements.supplemental_ids.len(), 14);
        assert_eq!(left.mode(), SchemaMode::LocalSeedOpaqueV2);
        assert!(left.tables.values().all(|identifier| identifier.len() == 15));
        assert!(left.columns.values().all(|identifier| identifier.len() == 15));
        assert!(left.csv_basenames.values().all(|name| {
            name.strip_suffix(".csv")
                .is_some_and(|stem| stem.len() == 15)
        }));
        assert_eq!(left, RuntimeSchema::decode(&left.encode()).unwrap());
        left.validate().unwrap();
    }

    #[test]
    fn opaque_v1_remains_stable_and_decodable() {
        let schema = RuntimeSchema::derive(2026, SchemaMode::LocalSeedOpaqueV1).unwrap();
        assert_eq!(schema.fingerprint(), 0x7167_a66c_d8d9_bac0);
        assert!(schema.tables.values().all(|identifier| identifier.len() == 17));
        assert!(schema.columns.values().all(|identifier| identifier.len() == 17));
        assert_eq!(RuntimeSchema::decode(&schema.encode()).unwrap(), schema);
    }

    #[test]
    fn different_seeds_change_every_runtime_domain() {
        let left = RuntimeSchema::opaque(73).unwrap();
        let right = RuntimeSchema::opaque(74).unwrap();
        assert_ne!(left.tables, right.tables);
        assert_ne!(left.columns, right.columns);
        assert_ne!(left.csv_basenames, right.csv_basenames);
        assert_ne!(left.statements, right.statements);
        assert_ne!(left.schedule.create_tables, right.schedule.create_tables);
        assert_ne!(left.schedule.create_indexes, right.schedule.create_indexes);
        assert_ne!(left.schedule.load_tables, right.schedule.load_tables);
        assert_ne!(left.schedule.count_tables, right.schedule.count_tables);
        assert_ne!(left.schedule.setup_checks, right.schedule.setup_checks);
        assert_ne!(left.fingerprint(), right.fingerprint());
    }

    #[test]
    fn setup_orders_are_independent_complete_permutations() {
        let schema = RuntimeSchema::opaque(2026).unwrap();
        schema.schedule.validate().unwrap();
        let domain_seeds = [
            DOMAIN_CREATE_ORDER,
            DOMAIN_INDEX_ORDER,
            DOMAIN_LOAD_ORDER,
            DOMAIN_COUNT_ORDER,
            DOMAIN_CHECK_ORDER,
        ]
        .map(|domain| domain_seed(schema.seed(), domain));
        assert_eq!(domain_seeds.into_iter().collect::<BTreeSet<_>>().len(), 5);
    }

    #[test]
    fn statement_ids_are_nonzero_unique_and_session_stable() {
        for seed in [73, 74] {
            let schema = RuntimeSchema::opaque(seed).unwrap();
            let expected = schema.statements.entries().collect::<Vec<_>>();
            let supplemental = FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS
                .map(|key| (key, schema.statements.id(key).unwrap()));
            let ids = expected
                .iter()
                .map(|(_, id)| *id)
                .chain(supplemental.iter().map(|(_, id)| *id))
                .collect::<BTreeSet<_>>();
            assert_eq!(
                ids.len(),
                FINAL2026_STATEMENT_KEYS.len() + FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS.len()
            );
            assert!(!ids.contains(&0));
            for _session in 0..32 {
                assert_eq!(
                    schema.statements.entries().collect::<Vec<_>>(),
                    expected,
                    "all 32 sessions must share one run layout"
                );
            }
        }
    }

    #[test]
    fn supplemental_ids_probe_across_wraparound_without_collisions() {
        let used = BTreeSet::from([u16::MAX, 1, 2]);
        assert_eq!(first_free_statement_id(u16::MAX, &used).unwrap(), 3);
        assert!(first_free_statement_id(0, &used).is_err());
    }

    #[test]
    fn supplemental_overlay_preserves_the_seed_2026_cache_contract() {
        let schema = RuntimeSchema::opaque(2026).unwrap();
        assert_eq!(schema.fingerprint(), 0x45b1_0a2a_a625_dea4);

        let encoded = schema.encode();
        assert_eq!(
            encoded
                .lines()
                .filter(|line| line.starts_with("statement="))
                .count(),
            FINAL2026_STATEMENT_KEYS.len()
        );
        assert!(FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS
            .iter()
            .all(|key| !encoded.contains(key)));

        let decoded = RuntimeSchema::decode(&encoded).unwrap();
        assert_eq!(decoded.encode(), encoded);
        for key in FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS {
            assert_eq!(decoded.statements.id(key), schema.statements.id(key));
        }
    }

    #[test]
    fn sql_renderer_preserves_literals_comments_markers_and_substrings() {
        let schema = RuntimeSchema::opaque(73).unwrap();
        let rendered = schema.render_sql(
            "SELECT warehouse.w_ytd AS w_ytd, 'warehouse.w_ytd''x', $1 \
             FROM warehouse -- warehouse w_ytd\n\
             WHERE warehouse.w_id = $2 AND warehouse_name = 'w_id'; \
             /* warehouse w_ytd */",
        );
        assert!(rendered.contains(&format!(
            "{}.{} AS {}",
            schema.table(LogicalTable::Warehouse),
            schema.column(LogicalTable::Warehouse, "w_ytd").unwrap(),
            schema.column(LogicalTable::Warehouse, "w_ytd").unwrap()
        )));
        assert!(rendered.contains("'warehouse.w_ytd''x'"));
        assert!(rendered.contains("-- warehouse w_ytd"));
        assert!(rendered.contains("/* warehouse w_ytd */"));
        assert!(rendered.contains("$1"));
        assert!(rendered.contains("warehouse_name"));
        assert!(rendered.contains("'w_id'"));
    }

    #[test]
    fn encoded_fingerprint_rejects_tampering() {
        let schema = RuntimeSchema::opaque(73).unwrap();
        let encoded = schema.encode();
        let table = schema.table(LogicalTable::Warehouse);
        let tampered = encoded.replacen(table, "t0000000000000000", 1);
        assert!(RuntimeSchema::decode(&tampered).is_err());
    }

    #[test]
    fn canonical_layout_remains_an_explicit_compatibility_mode() {
        let schema = RuntimeSchema::canonical(73).unwrap();
        assert_eq!(schema.table(LogicalTable::Warehouse), "warehouse");
        assert_eq!(
            schema.column(LogicalTable::Warehouse, "w_ytd").unwrap(),
            "w_ytd"
        );
        assert_eq!(
            schema.csv_basename(LogicalTable::Warehouse),
            "warehouse.csv"
        );
        assert_eq!(
            schema.statements.id("payment.update_warehouse").unwrap(),
            31
        );
        assert_eq!(
            FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS.map(|key| schema.statements.id(key).unwrap()),
            CANONICAL_SUPPLEMENTAL_STATEMENT_IDS
        );
    }
}
