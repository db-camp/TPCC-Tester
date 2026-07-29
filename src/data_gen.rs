use chrono::{Duration, TimeZone, Utc};
use rand::Rng;

use crate::model::*;

const SYLLABLES: &[&str] = &[
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

const CITIES: &[&str] = &[
    "Springfield",
    "Rivertown",
    "Oakland",
    "Madison",
    "Lincoln",
    "Franklin",
];

const STATES: &[&str] = &["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"];

const FIRST_NAMES: &[&str] = &[
    "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Edward", "Fiona",
];

const ITEM_PREFIXES: &[&str] = &[
    "Red", "Blue", "Green", "Large", "Small", "Premium", "Standard",
];

const ITEM_NOUNS: &[&str] = &["Widget", "Gadget", "Tool", "Device", "Product", "Item"];

/// Public local default only. The official grader derives an independent hidden seed.
pub const DEFAULT_LOAD_SEED: u64 = 20_260_729;
pub const DISTRICTS_PER_WAREHOUSE: i32 = 10;
pub const CUSTOMERS_PER_DISTRICT: i32 = 3000;
pub const ITEMS_TOTAL: i32 = 100_000;
pub const ORDERS_PER_DISTRICT: i32 = 3000;
pub const NEW_ORDERS_PER_DISTRICT: i32 = 900;

const DELIVERED_ORDER_MAX: i32 = ORDERS_PER_DISTRICT - NEW_ORDERS_PER_DISTRICT;

// Independent deterministic domains keep changes to one relation from shifting all others.
const DOMAIN_WAREHOUSE: u64 = 0x19d3_08da_454d_4a71;
const DOMAIN_DISTRICT: u64 = 0xca4f_4d4b_f13a_6ac1;
const DOMAIN_ITEM: u64 = 0x989e_6e8c_b019_a93b;
const DOMAIN_CUSTOMER: u64 = 0xb9bb_ef18_e67b_d5f2;
const DOMAIN_STOCK: u64 = 0x92f2_cfa4_7d71_a9d0;
const DOMAIN_ORDER: u64 = 0x6837_5c90_4340_a4fb;
const DOMAIN_ORDER_SHAPE: u64 = 0x355d_9272_a4a6_274e;
const DOMAIN_ORDER_LINE: u64 = 0xf675_f643_d32c_d4dc;
const DOMAIN_HISTORY: u64 = 0xd6e8_feb8_6659_fd93;

/// SplitMix64 with explicit rejection sampling. Unlike `StdRng` and
/// `rand::distributions`, this load-data stream is stable across crate upgrades.
struct StableRng {
    state: u64,
}

impl StableRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn below(&mut self, upper_exclusive: u64) -> u64 {
        assert!(upper_exclusive > 0);
        let threshold = upper_exclusive.wrapping_neg() % upper_exclusive;
        loop {
            let value = self.next_u64();
            if value >= threshold {
                return value % upper_exclusive;
            }
        }
    }

    fn i32_inclusive(&mut self, min: i32, max: i32) -> i32 {
        assert!(min <= max);
        min + self.below((i64::from(max) - i64::from(min) + 1) as u64) as i32
    }

    fn i64_inclusive(&mut self, min: i64, max: i64) -> i64 {
        assert!(min <= max);
        min + self.below((max - min + 1) as u64) as i64
    }

    fn usize_exclusive(&mut self, min: usize, max: usize) -> usize {
        assert!(min < max);
        min + self.below((max - min) as u64) as usize
    }

    fn usize_inclusive(&mut self, min: usize, max: usize) -> usize {
        assert!(min <= max);
        min + self.below((max - min + 1) as u64) as usize
    }
}

pub struct TpccDataGen {
    pub scale_factor: i32,
    load_seed: u64,
}

impl TpccDataGen {
    /// Uses `RMDB_TPCC_SEED` when set, otherwise a documented local seed.
    ///
    /// The value is intentionally configurable and is not the official hidden seed.
    pub fn new(scale_factor: i32) -> Self {
        let load_seed = std::env::var("RMDB_TPCC_SEED")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(DEFAULT_LOAD_SEED);
        Self::with_seed(scale_factor, load_seed)
    }

    pub fn with_seed(scale_factor: i32, load_seed: u64) -> Self {
        Self {
            scale_factor: scale_factor.max(1),
            load_seed,
        }
    }

    pub fn load_seed(&self) -> u64 {
        self.load_seed
    }

    // ─── Initial-data deterministic helpers ─────────────

    fn splitmix64(mut value: u64) -> u64 {
        value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^ (value >> 31)
    }

    fn row_rng(&self, domain: u64, keys: &[i32]) -> StableRng {
        let mut state = Self::splitmix64(self.load_seed ^ domain);
        for key in keys {
            state = Self::splitmix64(state ^ (*key as u32 as u64));
        }
        StableRng::new(state)
    }

    fn rng_int(rng: &mut StableRng, min: i32, max: i32) -> i32 {
        rng.i32_inclusive(min, max)
    }

    fn rng_string(rng: &mut StableRng, len: usize) -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        (0..len)
            .map(|_| CHARSET[rng.usize_exclusive(0, CHARSET.len())] as char)
            .collect()
    }

    fn rng_alpha_num(rng: &mut StableRng, len: usize) -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        (0..len)
            .map(|_| CHARSET[rng.usize_exclusive(0, CHARSET.len())] as char)
            .collect()
    }

    fn gen_street(rng: &mut StableRng) -> String {
        format!(
            "{} {} St",
            Self::rng_int(rng, 1, 9999),
            Self::rng_string(rng, 5).to_uppercase()
        )
    }

    fn gen_city(rng: &mut StableRng) -> String {
        CITIES[rng.usize_exclusive(0, CITIES.len())].to_string()
    }

    fn gen_state(rng: &mut StableRng) -> String {
        STATES[rng.usize_exclusive(0, STATES.len())].to_string()
    }

    fn gen_zip(rng: &mut StableRng) -> String {
        format!(
            "{}{}",
            Self::rng_int(rng, 10000, 99999),
            Self::rng_int(rng, 1000, 9999)
        )
    }

    fn gen_phone(rng: &mut StableRng) -> String {
        (0..16)
            .map(|_| char::from(b'0' + rng.i32_inclusive(0, 9) as u8))
            .collect()
    }

    fn gen_tax(rng: &mut StableRng) -> f64 {
        f64::from(Self::rng_int(rng, 0, 2000) as f32 / 10_000.0_f32)
    }

    fn gen_discount(rng: &mut StableRng) -> f64 {
        f64::from(Self::rng_int(rng, 0, 5000) as f32 / 10_000.0_f32)
    }

    fn gen_price(rng: &mut StableRng) -> f64 {
        f64::from(Self::rng_int(rng, 100, 10_000) as f32 / 100.0_f32)
    }

    fn gen_item_name(rng: &mut StableRng) -> String {
        format!(
            "{} {}",
            ITEM_PREFIXES[rng.usize_exclusive(0, ITEM_PREFIXES.len())],
            ITEM_NOUNS[rng.usize_exclusive(0, ITEM_NOUNS.len())]
        )
    }

    fn gen_data(rng: &mut StableRng, min_len: usize, max_len: usize) -> String {
        let len = rng.usize_inclusive(min_len, max_len);
        Self::rng_string(rng, len)
    }

    fn gen_dist_info(rng: &mut StableRng) -> String {
        Self::rng_alpha_num(rng, 24)
    }

    fn gen_first_name(rng: &mut StableRng) -> String {
        FIRST_NAMES[rng.usize_exclusive(0, FIRST_NAMES.len())].to_string()
    }

    fn gen_timestamp(rng: &mut StableRng) -> String {
        // A fixed epoch makes the public seed reproduce complete CSV contents.
        let base = Utc
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("valid fixed timestamp");
        let seconds_ago = rng.i64_inclusive(0, 730_i64 * 24 * 60 * 60);
        (base - Duration::seconds(seconds_ago))
            .format("%Y-%m-%d %H:%M:%S")
            .to_string()
    }

    fn maybe_mark_original(rng: &mut StableRng, mut data: String) -> String {
        if Self::rng_int(rng, 1, 100) <= 10 {
            let pos = Self::rng_int(rng, 0, (data.len() as i32 - 8).max(0)) as usize;
            let end = (pos + 8).min(data.len());
            data = format!("{}ORIGINAL{}", &data[..pos], &data[end..]);
        }
        data
    }

    fn initial_order_timestamp(&self, w_id: i32, d_id: i32, o_id: i32) -> String {
        let mut rng = self.row_rng(DOMAIN_ORDER, &[w_id, d_id, o_id, 1]);
        Self::gen_timestamp(&mut rng)
    }

    fn initial_customer_id(&self, w_id: i32, d_id: i32, o_id: i32) -> i32 {
        // An affine permutation gives every district customer exactly one initial order.
        let mut rng = self.row_rng(DOMAIN_ORDER, &[w_id, d_id, 2]);
        let offset = rng.i32_inclusive(0, CUSTOMERS_PER_DISTRICT - 1);
        ((1009 * (o_id - 1) + offset) % CUSTOMERS_PER_DISTRICT) + 1
    }

    /// The one source of truth used by both `orders.o_ol_cnt` and `order_line`.
    pub fn initial_order_line_count(&self, w_id: i32, d_id: i32, o_id: i32) -> i32 {
        let mut rng = self.row_rng(DOMAIN_ORDER_SHAPE, &[w_id, d_id, o_id]);
        rng.i32_inclusive(5, 15)
    }

    fn initial_order_line_amount_cents(
        &self,
        w_id: i32,
        d_id: i32,
        o_id: i32,
        ol_number: i32,
    ) -> i32 {
        if o_id <= DELIVERED_ORDER_MAX {
            return 0;
        }
        let mut rng = self.row_rng(DOMAIN_ORDER_LINE, &[w_id, d_id, o_id, ol_number, 2]);
        rng.i32_inclusive(1, 999_999)
    }

    pub fn expected_order_line_count(&self) -> i64 {
        (1..=self.scale_factor)
            .flat_map(|w_id| {
                (1..=DISTRICTS_PER_WAREHOUSE).flat_map(move |d_id| {
                    (1..=ORDERS_PER_DISTRICT)
                        .map(move |o_id| self.initial_order_line_count(w_id, d_id, o_id) as i64)
                })
            })
            .sum()
    }

    pub fn generate_last_name(customer_id: i32) -> String {
        let number = (customer_id - 1).rem_euclid(1000) as usize;
        format!(
            "{}{}{}",
            SYLLABLES[number / 100],
            SYLLABLES[(number / 10) % 10],
            SYLLABLES[number % 10]
        )
    }

    // ─── Streaming initial-data generators ──────────────

    pub fn generate_warehouses(&self) -> impl Iterator<Item = Warehouse> + '_ {
        (1..=self.scale_factor).map(move |w_id| {
            let mut rng = self.row_rng(DOMAIN_WAREHOUSE, &[w_id]);
            Warehouse {
                w_id,
                w_name: format!("W{w_id:02}"),
                w_street_1: Self::gen_street(&mut rng),
                w_street_2: Self::gen_street(&mut rng),
                w_city: Self::gen_city(&mut rng),
                w_state: Self::gen_state(&mut rng),
                w_zip: Self::gen_zip(&mut rng),
                w_tax: Self::gen_tax(&mut rng),
                w_ytd: 300_000.0,
            }
        })
    }

    pub fn generate_districts(&self) -> impl Iterator<Item = District> + '_ {
        (1..=self.scale_factor).flat_map(move |w_id| {
            (1..=DISTRICTS_PER_WAREHOUSE).map(move |d_id| {
                let mut rng = self.row_rng(DOMAIN_DISTRICT, &[w_id, d_id]);
                District {
                    d_id,
                    d_w_id: w_id,
                    d_name: format!("D{d_id:02}"),
                    d_street_1: Self::gen_street(&mut rng),
                    d_street_2: Self::gen_street(&mut rng),
                    d_city: Self::gen_city(&mut rng),
                    d_state: Self::gen_state(&mut rng),
                    d_zip: Self::gen_zip(&mut rng),
                    d_tax: Self::gen_tax(&mut rng),
                    d_ytd: 30_000.0,
                    d_next_o_id: ORDERS_PER_DISTRICT + 1,
                }
            })
        })
    }

    pub fn generate_items(&self) -> impl Iterator<Item = Item> + '_ {
        (1..=ITEMS_TOTAL).map(move |i_id| {
            let mut rng = self.row_rng(DOMAIN_ITEM, &[i_id]);
            let data = Self::gen_data(&mut rng, 26, 50);
            Item {
                i_id,
                i_im_id: Self::rng_int(&mut rng, 1, 10_000),
                i_name: Self::gen_item_name(&mut rng),
                i_price: Self::gen_price(&mut rng),
                i_data: Self::maybe_mark_original(&mut rng, data),
            }
        })
    }

    pub fn generate_customers(&self) -> impl Iterator<Item = Customer> + '_ {
        (1..=self.scale_factor).flat_map(move |w_id| {
            (1..=DISTRICTS_PER_WAREHOUSE).flat_map(move |d_id| {
                (1..=CUSTOMERS_PER_DISTRICT).map(move |c_id| {
                    let mut rng = self.row_rng(DOMAIN_CUSTOMER, &[w_id, d_id, c_id]);
                    let credit = if Self::rng_int(&mut rng, 1, 100) <= 90 {
                        "GC"
                    } else {
                        "BC"
                    };
                    let c_last = if c_id <= 1000 {
                        Self::generate_last_name(c_id)
                    } else {
                        Self::generate_last_name(Self::rng_int(&mut rng, 1, 1000))
                    };
                    Customer {
                        c_id,
                        c_d_id: d_id,
                        c_w_id: w_id,
                        c_first: Self::gen_first_name(&mut rng),
                        c_middle: "OE".to_string(),
                        c_last,
                        c_street_1: Self::gen_street(&mut rng),
                        c_street_2: Self::gen_street(&mut rng),
                        c_city: Self::gen_city(&mut rng),
                        c_state: Self::gen_state(&mut rng),
                        c_zip: Self::gen_zip(&mut rng),
                        c_phone: Self::gen_phone(&mut rng),
                        c_since: Self::gen_timestamp(&mut rng),
                        c_credit: credit.to_string(),
                        c_credit_lim: 50_000,
                        c_discount: Self::gen_discount(&mut rng),
                        c_balance: -10.0,
                        c_ytd_payment: 10.0,
                        c_payment_cnt: 1,
                        c_delivery_cnt: 0,
                        c_data: Self::gen_data(&mut rng, 50, 50),
                    }
                })
            })
        })
    }

    pub fn generate_stock(&self) -> impl Iterator<Item = Stock> + '_ {
        (1..=self.scale_factor).flat_map(move |w_id| {
            (1..=ITEMS_TOTAL).map(move |i_id| {
                let mut rng = self.row_rng(DOMAIN_STOCK, &[w_id, i_id]);
                let data = Self::gen_data(&mut rng, 26, 50);
                Stock {
                    s_i_id: i_id,
                    s_w_id: w_id,
                    s_quantity: Self::rng_int(&mut rng, 10, 100),
                    s_dist_01: Self::gen_dist_info(&mut rng),
                    s_dist_02: Self::gen_dist_info(&mut rng),
                    s_dist_03: Self::gen_dist_info(&mut rng),
                    s_dist_04: Self::gen_dist_info(&mut rng),
                    s_dist_05: Self::gen_dist_info(&mut rng),
                    s_dist_06: Self::gen_dist_info(&mut rng),
                    s_dist_07: Self::gen_dist_info(&mut rng),
                    s_dist_08: Self::gen_dist_info(&mut rng),
                    s_dist_09: Self::gen_dist_info(&mut rng),
                    s_dist_10: Self::gen_dist_info(&mut rng),
                    s_ytd: 0.0,
                    s_order_cnt: 0,
                    s_remote_cnt: 0,
                    s_data: Self::maybe_mark_original(&mut rng, data),
                }
            })
        })
    }

    pub fn generate_orders(&self) -> impl Iterator<Item = Orders> + '_ {
        (1..=self.scale_factor).flat_map(move |w_id| {
            (1..=DISTRICTS_PER_WAREHOUSE).flat_map(move |d_id| {
                (1..=ORDERS_PER_DISTRICT).map(move |o_id| {
                    let mut rng = self.row_rng(DOMAIN_ORDER, &[w_id, d_id, o_id, 3]);
                    let carrier_id = if o_id > DELIVERED_ORDER_MAX {
                        0
                    } else {
                        Self::rng_int(&mut rng, 1, 10)
                    };
                    Orders {
                        o_id,
                        o_d_id: d_id,
                        o_w_id: w_id,
                        o_c_id: self.initial_customer_id(w_id, d_id, o_id),
                        o_entry_d: self.initial_order_timestamp(w_id, d_id, o_id),
                        o_carrier_id: carrier_id,
                        o_ol_cnt: self.initial_order_line_count(w_id, d_id, o_id),
                        o_all_local: 1,
                    }
                })
            })
        })
    }

    pub fn generate_new_orders(&self) -> impl Iterator<Item = NewOrder> + '_ {
        (1..=self.scale_factor).flat_map(move |w_id| {
            (1..=DISTRICTS_PER_WAREHOUSE).flat_map(move |d_id| {
                (DELIVERED_ORDER_MAX + 1..=ORDERS_PER_DISTRICT).map(move |no_o_id| NewOrder {
                    no_o_id,
                    no_d_id: d_id,
                    no_w_id: w_id,
                })
            })
        })
    }

    pub fn generate_history(&self) -> impl Iterator<Item = History> + '_ {
        (1..=self.scale_factor).flat_map(move |w_id| {
            (1..=DISTRICTS_PER_WAREHOUSE).flat_map(move |d_id| {
                (1..=CUSTOMERS_PER_DISTRICT).map(move |c_id| {
                    let mut rng = self.row_rng(DOMAIN_HISTORY, &[w_id, d_id, c_id]);
                    History {
                        h_c_id: c_id,
                        h_c_d_id: d_id,
                        h_c_w_id: w_id,
                        h_d_id: d_id,
                        h_w_id: w_id,
                        h_date: Self::gen_timestamp(&mut rng),
                        h_amount: 10.0,
                        h_data: "Initial deposit".to_string(),
                    }
                })
            })
        })
    }

    pub fn generate_order_lines(&self) -> impl Iterator<Item = OrderLine> + '_ {
        (1..=self.scale_factor).flat_map(move |w_id| {
            (1..=DISTRICTS_PER_WAREHOUSE).flat_map(move |d_id| {
                (1..=ORDERS_PER_DISTRICT).flat_map(move |o_id| {
                    let ol_count = self.initial_order_line_count(w_id, d_id, o_id);
                    let delivery_d = if o_id <= DELIVERED_ORDER_MAX {
                        self.initial_order_timestamp(w_id, d_id, o_id)
                    } else {
                        String::new()
                    };
                    (1..=ol_count).map(move |ol_number| {
                        let mut rng =
                            self.row_rng(DOMAIN_ORDER_LINE, &[w_id, d_id, o_id, ol_number, 1]);
                        let amount_cents =
                            self.initial_order_line_amount_cents(w_id, d_id, o_id, ol_number);
                        // amount_cents is exactly representable in f32; the division is the one
                        // required round-to-nearest-even conversion to binary32.
                        let amount = amount_cents as f32 / 100.0_f32;
                        OrderLine {
                            ol_o_id: o_id,
                            ol_d_id: d_id,
                            ol_w_id: w_id,
                            ol_number,
                            ol_i_id: Self::rng_int(&mut rng, 1, ITEMS_TOTAL),
                            ol_supply_w_id: w_id,
                            ol_delivery_d: delivery_d.clone(),
                            ol_quantity: 5,
                            ol_amount: f64::from(amount),
                            ol_dist_info: Self::gen_dist_info(&mut rng),
                        }
                    })
                })
            })
        })
    }

    // ─── Transaction parameter generation ───────────────

    fn rand_int(min: i32, max: i32) -> i32 {
        rand::rng().random_range(min..=max)
    }

    fn rand_float(min: f64, max: f64) -> f64 {
        rand::rng().random_range(min..=max)
    }

    pub fn get_random_warehouse_id(&self) -> i32 {
        Self::rand_int(1, self.scale_factor)
    }

    pub fn get_random_district_id(&self) -> i32 {
        Self::rand_int(1, DISTRICTS_PER_WAREHOUSE)
    }

    pub fn get_random_customer_id(&self) -> i32 {
        Self::rand_int(1, CUSTOMERS_PER_DISTRICT)
    }

    pub fn get_random_item_id(&self) -> i32 {
        Self::rand_int(1, ITEMS_TOTAL)
    }

    pub fn get_random_order_line_count(&self) -> i32 {
        Self::rand_int(5, 15)
    }

    pub fn get_random_quantity(&self) -> i32 {
        Self::rand_int(1, 10)
    }

    pub fn get_payment_customer_warehouse(&self, w_id: i32, d_id: i32) -> (i32, i32) {
        if self.scale_factor == 1 || Self::rand_int(1, 100) <= 85 {
            let c_w_id = w_id;
            let c_d_id = if Self::rand_int(1, 100) <= 15 {
                Self::rand_int(1, DISTRICTS_PER_WAREHOUSE)
            } else {
                d_id
            };
            (c_w_id, c_d_id)
        } else {
            let mut c_w_id = Self::rand_int(1, self.scale_factor - 1);
            if c_w_id >= w_id {
                c_w_id += 1;
            }
            (c_w_id, Self::rand_int(1, DISTRICTS_PER_WAREHOUSE))
        }
    }

    pub fn get_random_payment_amount(&self) -> f64 {
        Self::rand_float(1.0, 5000.0)
    }

    pub fn get_random_carrier_id(&self) -> i32 {
        Self::rand_int(1, 10)
    }

    pub fn get_random_stock_threshold(&self) -> i32 {
        Self::rand_int(10, 20)
    }

    pub fn get_random_customer_last_name(&self) -> String {
        Self::generate_last_name(self.get_random_customer_id())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_header_and_lines_share_the_same_dynamic_count() {
        let gen = TpccDataGen::with_seed(1, 0x5eed_2026);
        let headers: Vec<_> = gen.generate_orders().take(64).collect();
        let lines: Vec<_> = gen
            .generate_order_lines()
            .take_while(|line| line.ol_d_id == 1 && line.ol_o_id <= 64)
            .collect();

        for order in headers {
            let line_count = lines
                .iter()
                .filter(|line| line.ol_o_id == order.o_id)
                .count() as i32;
            assert_eq!(order.o_ol_cnt, line_count);
            assert!((5..=15).contains(&order.o_ol_cnt));
        }
    }

    #[test]
    fn initial_order_line_amounts_follow_final_2026_binary32_rules() {
        let gen = TpccDataGen::with_seed(1, 123_456);
        let delivered = gen
            .generate_order_lines()
            .find(|line| line.ol_o_id == DELIVERED_ORDER_MAX)
            .unwrap();
        assert_eq!((delivered.ol_amount as f32).to_bits(), 0.0_f32.to_bits());
        assert_eq!(delivered.ol_quantity, 5);
        assert!(!delivered.ol_delivery_d.is_empty());

        let undelivered = gen
            .generate_order_lines()
            .find(|line| line.ol_o_id == DELIVERED_ORDER_MAX + 1)
            .unwrap();
        let cents = gen.initial_order_line_amount_cents(
            undelivered.ol_w_id,
            undelivered.ol_d_id,
            undelivered.ol_o_id,
            undelivered.ol_number,
        );
        let expected = cents as f32 / 100.0_f32;
        let actual = undelivered.ol_amount as f32;
        assert_eq!(actual.to_bits(), expected.to_bits());
        assert!((1..=999_999).contains(&cents));
        assert_eq!(undelivered.ol_quantity, 5);
        assert!(undelivered.ol_delivery_d.is_empty());

        let csv_round_trip: f32 = actual.to_string().parse().unwrap();
        assert_eq!(csv_round_trip.to_bits(), actual.to_bits());
    }

    #[test]
    fn load_seed_reproduces_rows_and_changes_order_shapes() {
        let left = TpccDataGen::with_seed(1, 7);
        let same = TpccDataGen::with_seed(1, 7);
        let other = TpccDataGen::with_seed(1, 8);

        let left_counts: Vec<_> = (1..=128)
            .map(|o_id| left.initial_order_line_count(1, 1, o_id))
            .collect();
        let same_counts: Vec<_> = (1..=128)
            .map(|o_id| same.initial_order_line_count(1, 1, o_id))
            .collect();
        let other_counts: Vec<_> = (1..=128)
            .map(|o_id| other.initial_order_line_count(1, 1, o_id))
            .collect();

        assert_eq!(left_counts, same_counts);
        assert_ne!(left_counts, other_counts);
    }
}
