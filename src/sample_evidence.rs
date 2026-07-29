//! Deterministic, bounded evidence captured from the generated setup CSV rows.
//!
//! The official setup sample keys and answers are intentionally hidden. This
//! module implements an independent public-spec-aligned sample: keys are
//! selected from the configured local load seed, while every expected value is
//! captured before CSV serialization. No expected value is read back from the
//! database under test.

use std::collections::{BTreeMap, BTreeSet};

use crate::connection::cursor::SqlParam;
use crate::data_gen::{
    TpccDataGen, CUSTOMERS_PER_DISTRICT, DISTRICTS_PER_WAREHOUSE, ITEMS_TOTAL,
    NEW_ORDERS_PER_DISTRICT, ORDERS_PER_DISTRICT,
};
use crate::error::TpccError;

pub const SETUP_SAMPLE_LIMIT: usize = 16;
pub const MAX_SETUP_SAMPLE_LINES: usize = SETUP_SAMPLE_LIMIT * 15;
const FIRST_UNDELIVERED_ORDER_ID: i32 = ORDERS_PER_DISTRICT - NEW_ORDERS_PER_DISTRICT + 1;
const SAMPLE_ORDER_DOMAIN: u64 = 0xa954_6f3c_7d1e_b820;
const EVIDENCE_VERSION: u32 = 1;
const MAX_EVIDENCE_HEX_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WarehouseSample {
    pub id: i32,
    pub name: Vec<u8>,
    pub state: Vec<u8>,
    pub zip: Vec<u8>,
    pub tax_bits: u32,
    pub ytd_bits: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DistrictSample {
    pub warehouse_id: i32,
    pub id: i32,
    pub name: Vec<u8>,
    pub state: Vec<u8>,
    pub zip: Vec<u8>,
    pub tax_bits: u32,
    pub ytd_bits: u32,
    pub next_order_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomerSample {
    pub warehouse_id: i32,
    pub district_id: i32,
    pub id: i32,
    pub first: Vec<u8>,
    pub middle: Vec<u8>,
    pub last: Vec<u8>,
    pub since: Vec<u8>,
    pub credit: Vec<u8>,
    pub discount_bits: u32,
    pub balance_bits: u32,
    pub ytd_payment_bits: u32,
    pub payment_count: i32,
    pub delivery_count: i32,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderSample {
    pub warehouse_id: i32,
    pub district_id: i32,
    pub id: i32,
    pub customer_id: i32,
    pub entry_date: Vec<u8>,
    pub carrier_id: i32,
    pub line_count: i32,
    pub all_local: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewOrderSample {
    pub warehouse_id: i32,
    pub district_id: i32,
    pub order_id: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistorySample {
    pub customer_warehouse_id: i32,
    pub customer_district_id: i32,
    pub customer_id: i32,
    pub warehouse_id: i32,
    pub district_id: i32,
    pub date: Vec<u8>,
    pub amount_bits: u32,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderLineSample {
    pub warehouse_id: i32,
    pub district_id: i32,
    pub order_id: i32,
    pub number: i32,
    pub item_id: i32,
    pub supply_warehouse_id: i32,
    pub delivery_date: Vec<u8>,
    pub quantity: i32,
    pub amount_bits: u32,
    pub dist_info: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ItemSample {
    pub id: i32,
    pub name: Vec<u8>,
    pub price_bits: u32,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StockSample {
    pub warehouse_id: i32,
    pub item_id: i32,
    pub quantity: i32,
    pub ytd_bits: u32,
    pub order_count: i32,
    pub remote_count: i32,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetupAnchorSample {
    pub warehouse: WarehouseSample,
    pub district: DistrictSample,
    pub customer: CustomerSample,
    pub order: OrderSample,
    pub new_order: NewOrderSample,
    pub history: HistorySample,
    pub lines: Vec<OrderLineSample>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SetupEvidence {
    pub load_seed: u64,
    pub load_timestamp: Vec<u8>,
    pub anchors: Vec<SetupAnchorSample>,
    pub items: Vec<ItemSample>,
    pub stocks: Vec<StockSample>,
}

impl SetupEvidence {
    pub fn validate(&self, warehouses: i32) -> Result<(), String> {
        if warehouses <= 0 {
            return Err("setup evidence warehouse count must be positive".to_owned());
        }
        validate_bytes("load timestamp", &self.load_timestamp, 1, 30)?;
        let timestamp = String::from_utf8(self.load_timestamp.clone())
            .map_err(|_| "setup load timestamp is not UTF-8".to_owned())?;
        let expected_partitions = selected_partitions(warehouses)?;
        if self.anchors.len() != expected_partitions.len() {
            return Err(format!(
                "setup evidence has {} anchors, expected {}",
                self.anchors.len(),
                expected_partitions.len()
            ));
        }
        let generator = TpccDataGen::with_seed_and_timestamp(warehouses, self.load_seed, timestamp);
        let mut referenced_items = BTreeSet::new();
        let mut referenced_stocks = BTreeSet::new();
        let mut warehouse_rows = BTreeMap::new();

        for (anchor, &(warehouse_id, district_id)) in self.anchors.iter().zip(&expected_partitions)
        {
            let order_id = selected_order_id(self.load_seed, warehouse_id, district_id);
            let customer_id =
                generator.initial_order_customer_id(warehouse_id, district_id, order_id);
            let line_count =
                generator.initial_order_line_count(warehouse_id, district_id, order_id);

            if anchor.warehouse.id != warehouse_id {
                return Err("setup anchor warehouse key is not canonical".to_owned());
            }
            validate_warehouse(&anchor.warehouse, warehouses)?;
            if let Some(previous) =
                warehouse_rows.insert(anchor.warehouse.id, anchor.warehouse.clone())
            {
                if previous != anchor.warehouse {
                    return Err(format!(
                        "setup evidence disagrees about warehouse {}",
                        anchor.warehouse.id
                    ));
                }
            }

            if (anchor.district.warehouse_id, anchor.district.id) != (warehouse_id, district_id) {
                return Err("district->warehouse setup reference is inconsistent".to_owned());
            }
            validate_district(&anchor.district, warehouses)?;

            if (
                anchor.customer.warehouse_id,
                anchor.customer.district_id,
                anchor.customer.id,
            ) != (warehouse_id, district_id, customer_id)
            {
                return Err("customer->district setup reference is inconsistent".to_owned());
            }
            validate_customer(&anchor.customer, warehouses, &self.load_timestamp)?;

            if (
                anchor.order.warehouse_id,
                anchor.order.district_id,
                anchor.order.id,
                anchor.order.customer_id,
            ) != (warehouse_id, district_id, order_id, customer_id)
            {
                return Err("orders->customer setup reference is inconsistent".to_owned());
            }
            validate_order(&anchor.order, warehouses, &self.load_timestamp)?;
            if anchor.order.line_count != line_count {
                return Err(format!(
                    "sampled order ({warehouse_id},{district_id},{order_id}) has line_count {}, expected generated shape {line_count}",
                    anchor.order.line_count
                ));
            }

            if (
                anchor.new_order.warehouse_id,
                anchor.new_order.district_id,
                anchor.new_order.order_id,
            ) != (warehouse_id, district_id, order_id)
            {
                return Err("new_orders->orders setup reference is inconsistent".to_owned());
            }

            if (
                anchor.history.customer_warehouse_id,
                anchor.history.customer_district_id,
                anchor.history.customer_id,
                anchor.history.warehouse_id,
                anchor.history.district_id,
            ) != (
                warehouse_id,
                district_id,
                customer_id,
                warehouse_id,
                district_id,
            ) {
                return Err("history->customer setup reference is inconsistent".to_owned());
            }
            validate_history(&anchor.history, warehouses, &self.load_timestamp)?;

            if anchor.lines.len() != line_count as usize {
                return Err(format!(
                    "sampled order ({warehouse_id},{district_id},{order_id}) has {} lines, expected {line_count}",
                    anchor.lines.len()
                ));
            }
            for (index, line) in anchor.lines.iter().enumerate() {
                let number = index as i32 + 1;
                let item_id = generator.initial_order_line_item_id(
                    warehouse_id,
                    district_id,
                    order_id,
                    number,
                );
                if (
                    line.warehouse_id,
                    line.district_id,
                    line.order_id,
                    line.number,
                    line.item_id,
                    line.supply_warehouse_id,
                ) != (
                    warehouse_id,
                    district_id,
                    order_id,
                    number,
                    item_id,
                    warehouse_id,
                ) {
                    return Err(format!(
                        "order_line->orders/item/stock reference is inconsistent at ({warehouse_id},{district_id},{order_id},{number})"
                    ));
                }
                validate_order_line(line, warehouses)?;
                referenced_items.insert(item_id);
                referenced_stocks.insert((warehouse_id, item_id));
            }
        }

        let mut previous_item = None;
        let mut actual_items = BTreeSet::new();
        for item in &self.items {
            if previous_item.is_some_and(|previous| previous >= item.id) {
                return Err("setup item samples are duplicate or unordered".to_owned());
            }
            previous_item = Some(item.id);
            validate_item(item)?;
            actual_items.insert(item.id);
        }
        if actual_items != referenced_items {
            return Err("setup item samples do not exactly cover line references".to_owned());
        }

        let mut previous_stock = None;
        let mut actual_stocks = BTreeSet::new();
        for stock in &self.stocks {
            let key = (stock.warehouse_id, stock.item_id);
            if previous_stock.is_some_and(|previous| previous >= key) {
                return Err("setup stock samples are duplicate or unordered".to_owned());
            }
            previous_stock = Some(key);
            validate_stock(stock, warehouses)?;
            actual_stocks.insert(key);
        }
        if actual_stocks != referenced_stocks {
            return Err("setup stock samples do not exactly cover line references".to_owned());
        }
        if self.items.len() > MAX_SETUP_SAMPLE_LINES || self.stocks.len() > MAX_SETUP_SAMPLE_LINES {
            return Err("setup evidence exceeds its bounded row limit".to_owned());
        }
        Ok(())
    }

    pub fn encode_hex(&self) -> String {
        let mut encoder = Encoder::default();
        encoder.u32(EVIDENCE_VERSION);
        encoder.u64(self.load_seed);
        encoder.bytes(&self.load_timestamp);
        encoder.count(self.anchors.len());
        for anchor in &self.anchors {
            encode_warehouse(&mut encoder, &anchor.warehouse);
            encode_district(&mut encoder, &anchor.district);
            encode_customer(&mut encoder, &anchor.customer);
            encode_order(&mut encoder, &anchor.order);
            encode_new_order(&mut encoder, &anchor.new_order);
            encode_history(&mut encoder, &anchor.history);
            encoder.count(anchor.lines.len());
            for line in &anchor.lines {
                encode_order_line(&mut encoder, line);
            }
        }
        encoder.count(self.items.len());
        for item in &self.items {
            encode_item(&mut encoder, item);
        }
        encoder.count(self.stocks.len());
        for stock in &self.stocks {
            encode_stock(&mut encoder, stock);
        }
        hex_encode(&encoder.bytes)
    }

    pub fn decode_hex(input: &str, warehouses: i32) -> Result<Self, String> {
        let raw = hex_decode(input)?;
        let mut decoder = Decoder::new(&raw);
        let version = decoder.u32("setup evidence version")?;
        if version != EVIDENCE_VERSION {
            return Err(format!(
                "unsupported setup evidence version {version}, expected {EVIDENCE_VERSION}"
            ));
        }
        let load_seed = decoder.u64("setup load seed")?;
        let load_timestamp = decoder.bytes("setup load timestamp", 30)?;
        let anchor_count = decoder.count("setup anchor count", SETUP_SAMPLE_LIMIT)?;
        let mut anchors = Vec::with_capacity(anchor_count);
        for _ in 0..anchor_count {
            let warehouse = decode_warehouse(&mut decoder)?;
            let district = decode_district(&mut decoder)?;
            let customer = decode_customer(&mut decoder)?;
            let order = decode_order(&mut decoder)?;
            let new_order = decode_new_order(&mut decoder)?;
            let history = decode_history(&mut decoder)?;
            let line_count = decoder.count("setup order-line count", 15)?;
            let mut lines = Vec::with_capacity(line_count);
            for _ in 0..line_count {
                lines.push(decode_order_line(&mut decoder)?);
            }
            anchors.push(SetupAnchorSample {
                warehouse,
                district,
                customer,
                order,
                new_order,
                history,
                lines,
            });
        }
        let item_count = decoder.count("setup item count", MAX_SETUP_SAMPLE_LINES)?;
        let mut items = Vec::with_capacity(item_count);
        for _ in 0..item_count {
            items.push(decode_item(&mut decoder)?);
        }
        let stock_count = decoder.count("setup stock count", MAX_SETUP_SAMPLE_LINES)?;
        let mut stocks = Vec::with_capacity(stock_count);
        for _ in 0..stock_count {
            stocks.push(decode_stock(&mut decoder)?);
        }
        decoder.finish()?;
        let evidence = Self {
            load_seed,
            load_timestamp,
            anchors,
            items,
            stocks,
        };
        evidence.validate(warehouses)?;
        if evidence.encode_hex() != input {
            return Err("setup evidence encoding is not canonical".to_owned());
        }
        Ok(evidence)
    }
}

#[derive(Clone, Debug)]
struct TargetAnchor {
    warehouse_id: i32,
    district_id: i32,
    order_id: i32,
    customer_id: i32,
}

pub struct SetupEvidenceCollector {
    warehouses: i32,
    load_seed: u64,
    load_timestamp: Vec<u8>,
    targets: Vec<TargetAnchor>,
    target_warehouses: BTreeSet<i32>,
    target_districts: BTreeSet<(i32, i32)>,
    target_customers: BTreeSet<(i32, i32, i32)>,
    target_orders: BTreeSet<(i32, i32, i32)>,
    target_items: BTreeSet<i32>,
    target_stocks: BTreeSet<(i32, i32)>,
    warehouses_seen: BTreeMap<i32, WarehouseSample>,
    districts_seen: BTreeMap<(i32, i32), DistrictSample>,
    customers_seen: BTreeMap<(i32, i32, i32), CustomerSample>,
    orders_seen: BTreeMap<(i32, i32, i32), OrderSample>,
    new_orders_seen: BTreeMap<(i32, i32, i32), NewOrderSample>,
    histories_seen: BTreeMap<(i32, i32, i32), HistorySample>,
    lines_seen: BTreeMap<(i32, i32, i32, i32), OrderLineSample>,
    items_seen: BTreeMap<i32, ItemSample>,
    stocks_seen: BTreeMap<(i32, i32), StockSample>,
}

impl SetupEvidenceCollector {
    pub fn new(generator: &TpccDataGen, warehouses: i32) -> Result<Self, TpccError> {
        if warehouses <= 0 || generator.scale_factor != warehouses {
            return Err(TpccError::Protocol(
                "setup evidence generator/warehouse mismatch".to_owned(),
            ));
        }
        let mut targets = Vec::new();
        let mut target_warehouses = BTreeSet::new();
        let mut target_districts = BTreeSet::new();
        let mut target_customers = BTreeSet::new();
        let mut target_orders = BTreeSet::new();
        let mut target_items = BTreeSet::new();
        let mut target_stocks = BTreeSet::new();
        for (warehouse_id, district_id) in
            selected_partitions(warehouses).map_err(TpccError::Protocol)?
        {
            let order_id = selected_order_id(generator.load_seed(), warehouse_id, district_id);
            let customer_id =
                generator.initial_order_customer_id(warehouse_id, district_id, order_id);
            let line_count =
                generator.initial_order_line_count(warehouse_id, district_id, order_id);
            for number in 1..=line_count {
                let item_id = generator.initial_order_line_item_id(
                    warehouse_id,
                    district_id,
                    order_id,
                    number,
                );
                target_items.insert(item_id);
                target_stocks.insert((warehouse_id, item_id));
            }
            targets.push(TargetAnchor {
                warehouse_id,
                district_id,
                order_id,
                customer_id,
            });
            target_warehouses.insert(warehouse_id);
            target_districts.insert((warehouse_id, district_id));
            target_customers.insert((warehouse_id, district_id, customer_id));
            target_orders.insert((warehouse_id, district_id, order_id));
        }
        Ok(Self {
            warehouses,
            load_seed: generator.load_seed(),
            load_timestamp: generator.load_timestamp().as_bytes().to_vec(),
            targets,
            target_warehouses,
            target_districts,
            target_customers,
            target_orders,
            target_items,
            target_stocks,
            warehouses_seen: BTreeMap::new(),
            districts_seen: BTreeMap::new(),
            customers_seen: BTreeMap::new(),
            orders_seen: BTreeMap::new(),
            new_orders_seen: BTreeMap::new(),
            histories_seen: BTreeMap::new(),
            lines_seen: BTreeMap::new(),
            items_seen: BTreeMap::new(),
            stocks_seen: BTreeMap::new(),
        })
    }

    pub fn observe_warehouse(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 9, "warehouse")?;
        let id = int(row, 0, "warehouse.w_id")?;
        if !self.target_warehouses.contains(&id) {
            return Ok(());
        }
        insert_unique(
            &mut self.warehouses_seen,
            id,
            WarehouseSample {
                id,
                name: chars(row, 1, "warehouse.w_name")?,
                state: chars(row, 5, "warehouse.w_state")?,
                zip: chars(row, 6, "warehouse.w_zip")?,
                tax_bits: float_bits(row, 7, "warehouse.w_tax")?,
                ytd_bits: float_bits(row, 8, "warehouse.w_ytd")?,
            },
            "warehouse",
        )
    }

    pub fn observe_district(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 11, "district")?;
        let key = (
            int(row, 1, "district.d_w_id")?,
            int(row, 0, "district.d_id")?,
        );
        if !self.target_districts.contains(&key) {
            return Ok(());
        }
        insert_unique(
            &mut self.districts_seen,
            key,
            DistrictSample {
                warehouse_id: key.0,
                id: key.1,
                name: chars(row, 2, "district.d_name")?,
                state: chars(row, 6, "district.d_state")?,
                zip: chars(row, 7, "district.d_zip")?,
                tax_bits: float_bits(row, 8, "district.d_tax")?,
                ytd_bits: float_bits(row, 9, "district.d_ytd")?,
                next_order_id: int(row, 10, "district.d_next_o_id")?,
            },
            "district",
        )
    }

    pub fn observe_item(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 5, "item")?;
        let id = int(row, 0, "item.i_id")?;
        if !self.target_items.contains(&id) {
            return Ok(());
        }
        insert_unique(
            &mut self.items_seen,
            id,
            ItemSample {
                id,
                name: chars(row, 2, "item.i_name")?,
                price_bits: float_bits(row, 3, "item.i_price")?,
                data: chars(row, 4, "item.i_data")?,
            },
            "item",
        )
    }

    pub fn observe_customer(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 21, "customer")?;
        let key = (
            int(row, 2, "customer.c_w_id")?,
            int(row, 1, "customer.c_d_id")?,
            int(row, 0, "customer.c_id")?,
        );
        if !self.target_customers.contains(&key) {
            return Ok(());
        }
        insert_unique(
            &mut self.customers_seen,
            key,
            CustomerSample {
                warehouse_id: key.0,
                district_id: key.1,
                id: key.2,
                first: chars(row, 3, "customer.c_first")?,
                middle: chars(row, 4, "customer.c_middle")?,
                last: chars(row, 5, "customer.c_last")?,
                since: chars(row, 12, "customer.c_since")?,
                credit: chars(row, 13, "customer.c_credit")?,
                discount_bits: float_bits(row, 15, "customer.c_discount")?,
                balance_bits: float_bits(row, 16, "customer.c_balance")?,
                ytd_payment_bits: float_bits(row, 17, "customer.c_ytd_payment")?,
                payment_count: int(row, 18, "customer.c_payment_cnt")?,
                delivery_count: int(row, 19, "customer.c_delivery_cnt")?,
                data: chars(row, 20, "customer.c_data")?,
            },
            "customer",
        )
    }

    pub fn observe_stock(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 17, "stock")?;
        let key = (int(row, 1, "stock.s_w_id")?, int(row, 0, "stock.s_i_id")?);
        if !self.target_stocks.contains(&key) {
            return Ok(());
        }
        insert_unique(
            &mut self.stocks_seen,
            key,
            StockSample {
                warehouse_id: key.0,
                item_id: key.1,
                quantity: int(row, 2, "stock.s_quantity")?,
                ytd_bits: float_bits(row, 13, "stock.s_ytd")?,
                order_count: int(row, 14, "stock.s_order_cnt")?,
                remote_count: int(row, 15, "stock.s_remote_cnt")?,
                data: chars(row, 16, "stock.s_data")?,
            },
            "stock",
        )
    }

    pub fn observe_order(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 8, "orders")?;
        let key = (
            int(row, 2, "orders.o_w_id")?,
            int(row, 1, "orders.o_d_id")?,
            int(row, 0, "orders.o_id")?,
        );
        if !self.target_orders.contains(&key) {
            return Ok(());
        }
        insert_unique(
            &mut self.orders_seen,
            key,
            OrderSample {
                warehouse_id: key.0,
                district_id: key.1,
                id: key.2,
                customer_id: int(row, 3, "orders.o_c_id")?,
                entry_date: chars(row, 4, "orders.o_entry_d")?,
                carrier_id: int(row, 5, "orders.o_carrier_id")?,
                line_count: int(row, 6, "orders.o_ol_cnt")?,
                all_local: int(row, 7, "orders.o_all_local")?,
            },
            "orders",
        )
    }

    pub fn observe_new_order(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 3, "new_orders")?;
        let key = (
            int(row, 2, "new_orders.no_w_id")?,
            int(row, 1, "new_orders.no_d_id")?,
            int(row, 0, "new_orders.no_o_id")?,
        );
        if !self.target_orders.contains(&key) {
            return Ok(());
        }
        insert_unique(
            &mut self.new_orders_seen,
            key,
            NewOrderSample {
                warehouse_id: key.0,
                district_id: key.1,
                order_id: key.2,
            },
            "new_orders",
        )
    }

    pub fn observe_history(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 8, "history")?;
        let key = (
            int(row, 2, "history.h_c_w_id")?,
            int(row, 1, "history.h_c_d_id")?,
            int(row, 0, "history.h_c_id")?,
        );
        if !self.target_customers.contains(&key) {
            return Ok(());
        }
        insert_unique(
            &mut self.histories_seen,
            key,
            HistorySample {
                customer_warehouse_id: key.0,
                customer_district_id: key.1,
                customer_id: key.2,
                district_id: int(row, 3, "history.h_d_id")?,
                warehouse_id: int(row, 4, "history.h_w_id")?,
                date: chars(row, 5, "history.h_date")?,
                amount_bits: float_bits(row, 6, "history.h_amount")?,
                data: chars(row, 7, "history.h_data")?,
            },
            "history",
        )
    }

    pub fn observe_order_line(&mut self, row: &[SqlParam]) -> Result<(), TpccError> {
        require_arity(row, 10, "order_line")?;
        let order_key = (
            int(row, 2, "order_line.ol_w_id")?,
            int(row, 1, "order_line.ol_d_id")?,
            int(row, 0, "order_line.ol_o_id")?,
        );
        if !self.target_orders.contains(&order_key) {
            return Ok(());
        }
        let number = int(row, 3, "order_line.ol_number")?;
        let key = (order_key.0, order_key.1, order_key.2, number);
        insert_unique(
            &mut self.lines_seen,
            key,
            OrderLineSample {
                warehouse_id: key.0,
                district_id: key.1,
                order_id: key.2,
                number,
                item_id: int(row, 4, "order_line.ol_i_id")?,
                supply_warehouse_id: int(row, 5, "order_line.ol_supply_w_id")?,
                delivery_date: chars(row, 6, "order_line.ol_delivery_d")?,
                quantity: int(row, 7, "order_line.ol_quantity")?,
                amount_bits: float_bits(row, 8, "order_line.ol_amount")?,
                dist_info: chars(row, 9, "order_line.ol_dist_info")?,
            },
            "order_line",
        )
    }

    pub fn finish(mut self) -> Result<SetupEvidence, TpccError> {
        let mut anchors = Vec::with_capacity(self.targets.len());
        for target in self.targets {
            let partition = (target.warehouse_id, target.district_id);
            let customer_key = (target.warehouse_id, target.district_id, target.customer_id);
            let order_key = (target.warehouse_id, target.district_id, target.order_id);
            let order = take_required(&mut self.orders_seen, &order_key, "orders")?;
            let mut lines = Vec::with_capacity(order.line_count.max(0) as usize);
            for number in 1..=order.line_count {
                lines.push(take_required(
                    &mut self.lines_seen,
                    &(order_key.0, order_key.1, order_key.2, number),
                    "order_line",
                )?);
            }
            anchors.push(SetupAnchorSample {
                warehouse: self
                    .warehouses_seen
                    .get(&target.warehouse_id)
                    .cloned()
                    .ok_or_else(|| missing("warehouse", target.warehouse_id))?,
                district: take_required(&mut self.districts_seen, &partition, "district")?,
                customer: take_required(&mut self.customers_seen, &customer_key, "customer")?,
                order,
                new_order: take_required(&mut self.new_orders_seen, &order_key, "new_orders")?,
                history: take_required(&mut self.histories_seen, &customer_key, "history")?,
                lines,
            });
        }
        if !self.districts_seen.is_empty()
            || !self.customers_seen.is_empty()
            || !self.orders_seen.is_empty()
            || !self.new_orders_seen.is_empty()
            || !self.histories_seen.is_empty()
            || !self.lines_seen.is_empty()
        {
            return Err(TpccError::Protocol(
                "setup evidence retained unexpected sampled rows".to_owned(),
            ));
        }
        let evidence = SetupEvidence {
            load_seed: self.load_seed,
            load_timestamp: self.load_timestamp,
            anchors,
            items: self.items_seen.into_values().collect(),
            stocks: self.stocks_seen.into_values().collect(),
        };
        evidence.validate(self.warehouses).map_err(|error| {
            TpccError::Protocol(format!("invalid captured setup evidence: {error}"))
        })?;
        Ok(evidence)
    }
}

pub fn selected_partitions(warehouses: i32) -> Result<Vec<(i32, i32)>, String> {
    if warehouses <= 0 {
        return Err("warehouse count must be positive".to_owned());
    }
    let total = usize::try_from(warehouses)
        .ok()
        .and_then(|value| value.checked_mul(DISTRICTS_PER_WAREHOUSE as usize))
        .ok_or_else(|| "warehouse partition count overflow".to_owned())?;
    let count = total.min(SETUP_SAMPLE_LIMIT);
    let mut result = Vec::with_capacity(count);
    for ordinal in 0..count {
        let index = if count == 1 {
            0
        } else {
            ordinal * (total - 1) / (count - 1)
        };
        result.push((
            (index / DISTRICTS_PER_WAREHOUSE as usize) as i32 + 1,
            (index % DISTRICTS_PER_WAREHOUSE as usize) as i32 + 1,
        ));
    }
    if result.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err("setup partition selector produced duplicate/unordered keys".to_owned());
    }
    Ok(result)
}

pub fn selected_order_id(seed: u64, warehouse_id: i32, district_id: i32) -> i32 {
    let mut value = splitmix64(seed ^ SAMPLE_ORDER_DOMAIN);
    value = splitmix64(value ^ warehouse_id as u32 as u64);
    value = splitmix64(value ^ district_id as u32 as u64);
    FIRST_UNDELIVERED_ORDER_ID + (value % NEW_ORDERS_PER_DISTRICT as u64) as i32
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn validate_warehouse(row: &WarehouseSample, warehouses: i32) -> Result<(), String> {
    validate_id("warehouse", row.id, 1, warehouses)?;
    validate_bytes("warehouse name", &row.name, 1, 10)?;
    validate_bytes("warehouse state", &row.state, 2, 2)?;
    validate_bytes("warehouse zip", &row.zip, 9, 9)?;
    validate_finite("warehouse tax", row.tax_bits)?;
    validate_finite("warehouse ytd", row.ytd_bits)?;
    if row.ytd_bits != 300_000.0_f32.to_bits() {
        return Err("sampled warehouse initial ytd is not exact".to_owned());
    }
    Ok(())
}

fn validate_district(row: &DistrictSample, warehouses: i32) -> Result<(), String> {
    validate_id("district warehouse", row.warehouse_id, 1, warehouses)?;
    validate_id("district", row.id, 1, DISTRICTS_PER_WAREHOUSE)?;
    validate_bytes("district name", &row.name, 1, 10)?;
    validate_bytes("district state", &row.state, 2, 2)?;
    validate_bytes("district zip", &row.zip, 9, 9)?;
    validate_finite("district tax", row.tax_bits)?;
    validate_finite("district ytd", row.ytd_bits)?;
    if row.ytd_bits != 30_000.0_f32.to_bits() || row.next_order_id != ORDERS_PER_DISTRICT + 1 {
        return Err("sampled district initial state is not exact".to_owned());
    }
    Ok(())
}

fn validate_customer(
    row: &CustomerSample,
    warehouses: i32,
    timestamp: &[u8],
) -> Result<(), String> {
    validate_id("customer warehouse", row.warehouse_id, 1, warehouses)?;
    validate_id(
        "customer district",
        row.district_id,
        1,
        DISTRICTS_PER_WAREHOUSE,
    )?;
    validate_id("customer", row.id, 1, CUSTOMERS_PER_DISTRICT)?;
    validate_bytes("customer first", &row.first, 1, 16)?;
    validate_bytes("customer middle", &row.middle, 2, 2)?;
    validate_bytes("customer last", &row.last, 1, 16)?;
    validate_bytes("customer since", &row.since, 1, 30)?;
    validate_bytes("customer credit", &row.credit, 2, 2)?;
    validate_bytes("customer data", &row.data, 1, 50)?;
    for (name, bits) in [
        ("customer discount", row.discount_bits),
        ("customer balance", row.balance_bits),
        ("customer ytd payment", row.ytd_payment_bits),
    ] {
        validate_finite(name, bits)?;
    }
    if row.middle != b"OE"
        || !matches!(row.credit.as_slice(), b"GC" | b"BC")
        || row.since != timestamp
        || row.balance_bits != (-10.0_f32).to_bits()
        || row.ytd_payment_bits != 10.0_f32.to_bits()
        || row.payment_count != 1
        || row.delivery_count != 0
    {
        return Err("sampled customer initial state is not exact".to_owned());
    }
    Ok(())
}

fn validate_order(row: &OrderSample, warehouses: i32, timestamp: &[u8]) -> Result<(), String> {
    validate_id("order warehouse", row.warehouse_id, 1, warehouses)?;
    validate_id(
        "order district",
        row.district_id,
        1,
        DISTRICTS_PER_WAREHOUSE,
    )?;
    validate_id("order", row.id, 1, ORDERS_PER_DISTRICT)?;
    validate_id("order customer", row.customer_id, 1, CUSTOMERS_PER_DISTRICT)?;
    validate_bytes("order entry date", &row.entry_date, 1, 30)?;
    if row.entry_date != timestamp
        || row.carrier_id != 0
        || !(5..=15).contains(&row.line_count)
        || row.all_local != 1
        || row.id < FIRST_UNDELIVERED_ORDER_ID
    {
        return Err("sampled undelivered order initial state is not exact".to_owned());
    }
    Ok(())
}

fn validate_history(row: &HistorySample, warehouses: i32, timestamp: &[u8]) -> Result<(), String> {
    validate_id(
        "history customer warehouse",
        row.customer_warehouse_id,
        1,
        warehouses,
    )?;
    validate_id(
        "history customer district",
        row.customer_district_id,
        1,
        DISTRICTS_PER_WAREHOUSE,
    )?;
    validate_id(
        "history customer",
        row.customer_id,
        1,
        CUSTOMERS_PER_DISTRICT,
    )?;
    validate_id("history warehouse", row.warehouse_id, 1, warehouses)?;
    validate_id(
        "history district",
        row.district_id,
        1,
        DISTRICTS_PER_WAREHOUSE,
    )?;
    validate_bytes("history date", &row.date, 1, 30)?;
    validate_bytes("history data", &row.data, 1, 24)?;
    validate_finite("history amount", row.amount_bits)?;
    if row.date != timestamp || row.amount_bits != 10.0_f32.to_bits() {
        return Err("sampled history initial state is not exact".to_owned());
    }
    Ok(())
}

fn validate_order_line(row: &OrderLineSample, warehouses: i32) -> Result<(), String> {
    validate_id("order-line warehouse", row.warehouse_id, 1, warehouses)?;
    validate_id(
        "order-line district",
        row.district_id,
        1,
        DISTRICTS_PER_WAREHOUSE,
    )?;
    validate_id("order-line order", row.order_id, 1, ORDERS_PER_DISTRICT)?;
    validate_id("order-line number", row.number, 1, 15)?;
    validate_id("order-line item", row.item_id, 1, ITEMS_TOTAL)?;
    validate_id(
        "order-line supply warehouse",
        row.supply_warehouse_id,
        1,
        warehouses,
    )?;
    validate_bytes("order-line delivery date", &row.delivery_date, 0, 30)?;
    validate_bytes("order-line dist info", &row.dist_info, 24, 24)?;
    validate_finite("order-line amount", row.amount_bits)?;
    if !row.delivery_date.is_empty() || row.quantity != 5 || f32::from_bits(row.amount_bits) <= 0.0
    {
        return Err("sampled undelivered order-line state is not exact".to_owned());
    }
    Ok(())
}

fn validate_item(row: &ItemSample) -> Result<(), String> {
    validate_id("item", row.id, 1, ITEMS_TOTAL)?;
    validate_bytes("item name", &row.name, 1, 24)?;
    validate_bytes("item data", &row.data, 1, 50)?;
    validate_finite("item price", row.price_bits)?;
    let price = f32::from_bits(row.price_bits);
    if !(1.0..=100.0).contains(&price) {
        return Err("sampled item price is outside the initial range".to_owned());
    }
    Ok(())
}

fn validate_stock(row: &StockSample, warehouses: i32) -> Result<(), String> {
    validate_id("stock warehouse", row.warehouse_id, 1, warehouses)?;
    validate_id("stock item", row.item_id, 1, ITEMS_TOTAL)?;
    validate_bytes("stock data", &row.data, 1, 50)?;
    validate_finite("stock ytd", row.ytd_bits)?;
    if !(10..=100).contains(&row.quantity)
        || row.ytd_bits != 0.0_f32.to_bits()
        || row.order_count != 0
        || row.remote_count != 0
    {
        return Err("sampled stock initial state is not exact".to_owned());
    }
    Ok(())
}

fn validate_id(name: &str, value: i32, min: i32, max: i32) -> Result<(), String> {
    if (min..=max).contains(&value) {
        Ok(())
    } else {
        Err(format!("{name} id {value} is outside {min}..={max}"))
    }
}

fn validate_bytes(name: &str, value: &[u8], min: usize, max: usize) -> Result<(), String> {
    if value.len() < min || value.len() > max || value.contains(&0) {
        Err(format!(
            "{name} byte length {} is outside {min}..={max} or contains NUL",
            value.len()
        ))
    } else {
        Ok(())
    }
}

fn validate_finite(name: &str, bits: u32) -> Result<(), String> {
    if f32::from_bits(bits).is_finite() {
        Ok(())
    } else {
        Err(format!("{name} is non-finite 0x{bits:08x}"))
    }
}

fn require_arity(row: &[SqlParam], expected: usize, table: &str) -> Result<(), TpccError> {
    if row.len() == expected {
        Ok(())
    } else {
        Err(TpccError::Protocol(format!(
            "generated {table} row has {} columns, expected {expected}",
            row.len()
        )))
    }
}

fn int(row: &[SqlParam], index: usize, name: &str) -> Result<i32, TpccError> {
    match row.get(index) {
        Some(SqlParam::Int(value)) => i32::try_from(*value)
            .map_err(|_| TpccError::Protocol(format!("generated {name} does not fit INT32"))),
        _ => Err(TpccError::Protocol(format!("generated {name} is not INT"))),
    }
}

fn float_bits(row: &[SqlParam], index: usize, name: &str) -> Result<u32, TpccError> {
    match row.get(index) {
        Some(SqlParam::Float(value)) => {
            let narrowed = *value as f32;
            if narrowed.is_finite() {
                Ok(narrowed.to_bits())
            } else {
                Err(TpccError::Protocol(format!(
                    "generated {name} is not finite FLOAT32"
                )))
            }
        }
        _ => Err(TpccError::Protocol(format!(
            "generated {name} is not FLOAT"
        ))),
    }
}

fn chars(row: &[SqlParam], index: usize, name: &str) -> Result<Vec<u8>, TpccError> {
    match row.get(index) {
        Some(SqlParam::Str(value)) if !value.as_bytes().contains(&0) => {
            Ok(value.as_bytes().to_vec())
        }
        _ => Err(TpccError::Protocol(format!(
            "generated {name} is not NUL-free CHAR"
        ))),
    }
}

fn insert_unique<K: Ord + std::fmt::Debug, V>(
    rows: &mut BTreeMap<K, V>,
    key: K,
    value: V,
    table: &str,
) -> Result<(), TpccError> {
    if rows.insert(key, value).is_some() {
        Err(TpccError::Protocol(format!(
            "generated {table} contains a duplicate sampled key"
        )))
    } else {
        Ok(())
    }
}

fn take_required<K: Ord + std::fmt::Debug, V>(
    rows: &mut BTreeMap<K, V>,
    key: &K,
    table: &str,
) -> Result<V, TpccError> {
    rows.remove(key)
        .ok_or_else(|| missing(table, format!("{key:?}")))
}

fn missing(table: &str, key: impl std::fmt::Display) -> TpccError {
    TpccError::Protocol(format!(
        "generated {table} omitted required setup sample {key}"
    ))
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

impl Encoder {
    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn i32(&mut self, value: i32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn count(&mut self, value: usize) {
        self.u32(value as u32);
    }

    fn bytes(&mut self, value: &[u8]) {
        self.count(value.len());
        self.bytes.extend_from_slice(value);
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize, name: &str) -> Result<&'a [u8], String> {
        let end = self
            .offset
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| format!("truncated {name}"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u32(&mut self, name: &str) -> Result<u32, String> {
        Ok(u32::from_be_bytes(
            self.take(4, name)?.try_into().expect("four bytes"),
        ))
    }

    fn i32(&mut self, name: &str) -> Result<i32, String> {
        Ok(i32::from_be_bytes(
            self.take(4, name)?.try_into().expect("four bytes"),
        ))
    }

    fn u64(&mut self, name: &str) -> Result<u64, String> {
        Ok(u64::from_be_bytes(
            self.take(8, name)?.try_into().expect("eight bytes"),
        ))
    }

    fn count(&mut self, name: &str, maximum: usize) -> Result<usize, String> {
        let value = self.u32(name)? as usize;
        if value > maximum {
            Err(format!("{name} {value} exceeds maximum {maximum}"))
        } else {
            Ok(value)
        }
    }

    fn bytes(&mut self, name: &str, maximum: usize) -> Result<Vec<u8>, String> {
        let count = self.count(&format!("{name} length"), maximum)?;
        Ok(self.take(count, name)?.to_vec())
    }

    fn finish(&self) -> Result<(), String> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err("setup evidence has trailing bytes".to_owned())
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn hex_decode(input: &str) -> Result<Vec<u8>, String> {
    if input.len() > MAX_EVIDENCE_HEX_BYTES || input.len() % 2 != 0 {
        return Err("setup evidence hex is oversized or odd-length".to_owned());
    }
    let mut output = Vec::with_capacity(input.len() / 2);
    for pair in input.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        output.push((high << 4) | low);
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Result<u8, String> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err("setup evidence hex must be lowercase canonical ASCII".to_owned()),
    }
}

macro_rules! encode_ints {
    ($encoder:expr, $row:expr, $( $field:ident ),+ $(,)?) => {
        $( $encoder.i32($row.$field); )+
    };
}

fn encode_warehouse(encoder: &mut Encoder, row: &WarehouseSample) {
    encoder.i32(row.id);
    encoder.bytes(&row.name);
    encoder.bytes(&row.state);
    encoder.bytes(&row.zip);
    encoder.u32(row.tax_bits);
    encoder.u32(row.ytd_bits);
}

fn decode_warehouse(decoder: &mut Decoder<'_>) -> Result<WarehouseSample, String> {
    Ok(WarehouseSample {
        id: decoder.i32("warehouse id")?,
        name: decoder.bytes("warehouse name", 10)?,
        state: decoder.bytes("warehouse state", 2)?,
        zip: decoder.bytes("warehouse zip", 9)?,
        tax_bits: decoder.u32("warehouse tax")?,
        ytd_bits: decoder.u32("warehouse ytd")?,
    })
}

fn encode_district(encoder: &mut Encoder, row: &DistrictSample) {
    encode_ints!(encoder, row, warehouse_id, id);
    encoder.bytes(&row.name);
    encoder.bytes(&row.state);
    encoder.bytes(&row.zip);
    encoder.u32(row.tax_bits);
    encoder.u32(row.ytd_bits);
    encoder.i32(row.next_order_id);
}

fn decode_district(decoder: &mut Decoder<'_>) -> Result<DistrictSample, String> {
    Ok(DistrictSample {
        warehouse_id: decoder.i32("district warehouse")?,
        id: decoder.i32("district id")?,
        name: decoder.bytes("district name", 10)?,
        state: decoder.bytes("district state", 2)?,
        zip: decoder.bytes("district zip", 9)?,
        tax_bits: decoder.u32("district tax")?,
        ytd_bits: decoder.u32("district ytd")?,
        next_order_id: decoder.i32("district next order")?,
    })
}

fn encode_customer(encoder: &mut Encoder, row: &CustomerSample) {
    encode_ints!(encoder, row, warehouse_id, district_id, id);
    encoder.bytes(&row.first);
    encoder.bytes(&row.middle);
    encoder.bytes(&row.last);
    encoder.bytes(&row.since);
    encoder.bytes(&row.credit);
    encoder.u32(row.discount_bits);
    encoder.u32(row.balance_bits);
    encoder.u32(row.ytd_payment_bits);
    encoder.i32(row.payment_count);
    encoder.i32(row.delivery_count);
    encoder.bytes(&row.data);
}

fn decode_customer(decoder: &mut Decoder<'_>) -> Result<CustomerSample, String> {
    Ok(CustomerSample {
        warehouse_id: decoder.i32("customer warehouse")?,
        district_id: decoder.i32("customer district")?,
        id: decoder.i32("customer id")?,
        first: decoder.bytes("customer first", 16)?,
        middle: decoder.bytes("customer middle", 2)?,
        last: decoder.bytes("customer last", 16)?,
        since: decoder.bytes("customer since", 30)?,
        credit: decoder.bytes("customer credit", 2)?,
        discount_bits: decoder.u32("customer discount")?,
        balance_bits: decoder.u32("customer balance")?,
        ytd_payment_bits: decoder.u32("customer ytd payment")?,
        payment_count: decoder.i32("customer payment count")?,
        delivery_count: decoder.i32("customer delivery count")?,
        data: decoder.bytes("customer data", 50)?,
    })
}

fn encode_order(encoder: &mut Encoder, row: &OrderSample) {
    encode_ints!(encoder, row, warehouse_id, district_id, id, customer_id);
    encoder.bytes(&row.entry_date);
    encode_ints!(encoder, row, carrier_id, line_count, all_local);
}

fn decode_order(decoder: &mut Decoder<'_>) -> Result<OrderSample, String> {
    Ok(OrderSample {
        warehouse_id: decoder.i32("order warehouse")?,
        district_id: decoder.i32("order district")?,
        id: decoder.i32("order id")?,
        customer_id: decoder.i32("order customer")?,
        entry_date: decoder.bytes("order entry date", 30)?,
        carrier_id: decoder.i32("order carrier")?,
        line_count: decoder.i32("order line count")?,
        all_local: decoder.i32("order all-local")?,
    })
}

fn encode_new_order(encoder: &mut Encoder, row: &NewOrderSample) {
    encode_ints!(encoder, row, warehouse_id, district_id, order_id);
}

fn decode_new_order(decoder: &mut Decoder<'_>) -> Result<NewOrderSample, String> {
    Ok(NewOrderSample {
        warehouse_id: decoder.i32("new-order warehouse")?,
        district_id: decoder.i32("new-order district")?,
        order_id: decoder.i32("new-order id")?,
    })
}

fn encode_history(encoder: &mut Encoder, row: &HistorySample) {
    encode_ints!(
        encoder,
        row,
        customer_warehouse_id,
        customer_district_id,
        customer_id,
        warehouse_id,
        district_id
    );
    encoder.bytes(&row.date);
    encoder.u32(row.amount_bits);
    encoder.bytes(&row.data);
}

fn decode_history(decoder: &mut Decoder<'_>) -> Result<HistorySample, String> {
    Ok(HistorySample {
        customer_warehouse_id: decoder.i32("history customer warehouse")?,
        customer_district_id: decoder.i32("history customer district")?,
        customer_id: decoder.i32("history customer")?,
        warehouse_id: decoder.i32("history warehouse")?,
        district_id: decoder.i32("history district")?,
        date: decoder.bytes("history date", 30)?,
        amount_bits: decoder.u32("history amount")?,
        data: decoder.bytes("history data", 24)?,
    })
}

fn encode_order_line(encoder: &mut Encoder, row: &OrderLineSample) {
    encode_ints!(
        encoder,
        row,
        warehouse_id,
        district_id,
        order_id,
        number,
        item_id,
        supply_warehouse_id
    );
    encoder.bytes(&row.delivery_date);
    encoder.i32(row.quantity);
    encoder.u32(row.amount_bits);
    encoder.bytes(&row.dist_info);
}

fn decode_order_line(decoder: &mut Decoder<'_>) -> Result<OrderLineSample, String> {
    Ok(OrderLineSample {
        warehouse_id: decoder.i32("order-line warehouse")?,
        district_id: decoder.i32("order-line district")?,
        order_id: decoder.i32("order-line order")?,
        number: decoder.i32("order-line number")?,
        item_id: decoder.i32("order-line item")?,
        supply_warehouse_id: decoder.i32("order-line supply warehouse")?,
        delivery_date: decoder.bytes("order-line delivery date", 30)?,
        quantity: decoder.i32("order-line quantity")?,
        amount_bits: decoder.u32("order-line amount")?,
        dist_info: decoder.bytes("order-line dist info", 24)?,
    })
}

fn encode_item(encoder: &mut Encoder, row: &ItemSample) {
    encoder.i32(row.id);
    encoder.bytes(&row.name);
    encoder.u32(row.price_bits);
    encoder.bytes(&row.data);
}

fn decode_item(decoder: &mut Decoder<'_>) -> Result<ItemSample, String> {
    Ok(ItemSample {
        id: decoder.i32("item id")?,
        name: decoder.bytes("item name", 24)?,
        price_bits: decoder.u32("item price")?,
        data: decoder.bytes("item data", 50)?,
    })
}

fn encode_stock(encoder: &mut Encoder, row: &StockSample) {
    encode_ints!(encoder, row, warehouse_id, item_id, quantity);
    encoder.u32(row.ytd_bits);
    encode_ints!(encoder, row, order_count, remote_count);
    encoder.bytes(&row.data);
}

fn decode_stock(decoder: &mut Decoder<'_>) -> Result<StockSample, String> {
    Ok(StockSample {
        warehouse_id: decoder.i32("stock warehouse")?,
        item_id: decoder.i32("stock item")?,
        quantity: decoder.i32("stock quantity")?,
        ytd_bits: decoder.u32("stock ytd")?,
        order_count: decoder.i32("stock order count")?,
        remote_count: decoder.i32("stock remote count")?,
        data: decoder.bytes("stock data", 50)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_selector_is_bounded_deterministic_and_distributed() {
        assert_eq!(
            selected_partitions(1).unwrap(),
            (1..=10).map(|district| (1, district)).collect::<Vec<_>>()
        );
        let sf50 = selected_partitions(50).unwrap();
        assert_eq!(sf50.len(), SETUP_SAMPLE_LIMIT);
        assert_eq!(sf50.first(), Some(&(1, 1)));
        assert_eq!(sf50.last(), Some(&(50, 10)));
        assert!(sf50.iter().map(|key| key.0).collect::<BTreeSet<_>>().len() > 1);
        assert_eq!(sf50, selected_partitions(50).unwrap());
    }

    #[test]
    fn order_selector_stays_in_undelivered_range_and_uses_seed() {
        let first = selected_order_id(1, 1, 1);
        let second = selected_order_id(2, 1, 1);
        assert!((FIRST_UNDELIVERED_ORDER_ID..=ORDERS_PER_DISTRICT).contains(&first));
        assert!((FIRST_UNDELIVERED_ORDER_ID..=ORDERS_PER_DISTRICT).contains(&second));
        assert_ne!(first, second);
    }

    #[test]
    fn order_identity_helpers_match_streamed_rows() {
        let generator = TpccDataGen::with_seed(1, 2026);
        let order_id = selected_order_id(2026, 1, 3);
        let expected_customer = generator.initial_order_customer_id(1, 3, order_id);
        let order = generator
            .generate_orders()
            .find(|row| row.o_w_id == 1 && row.o_d_id == 3 && row.o_id == order_id)
            .unwrap();
        assert_eq!(order.o_c_id, expected_customer);
        let expected_item = generator.initial_order_line_item_id(1, 3, order_id, 1);
        let line = generator
            .generate_order_lines()
            .find(|row| {
                row.ol_w_id == 1
                    && row.ol_d_id == 3
                    && row.ol_o_id == order_id
                    && row.ol_number == 1
            })
            .unwrap();
        assert_eq!(line.ol_i_id, expected_item);
    }

    #[test]
    fn decoder_rejects_noncanonical_and_truncated_hex_before_allocating() {
        assert!(SetupEvidence::decode_hex("AA", 1).is_err());
        assert!(SetupEvidence::decode_hex("0", 1).is_err());
        assert!(SetupEvidence::decode_hex("00000001", 1).is_err());
        assert!(
            SetupEvidence::decode_hex(&"00".repeat(MAX_EVIDENCE_HEX_BYTES / 2 + 1), 1).is_err()
        );
    }
}
