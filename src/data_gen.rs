use chrono::{Local, Duration};
use rand::Rng;

use crate::model::*;

const SYLLABLES: &[&str] = &[
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

const CITIES: &[&str] = &[
    "Springfield", "Rivertown", "Oakland", "Madison", "Lincoln", "Franklin",
];

const STATES: &[&str] = &["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"];

const FIRST_NAMES: &[&str] = &[
    "John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Edward", "Fiona",
];

const ITEM_PREFIXES: &[&str] = &[
    "Red", "Blue", "Green", "Large", "Small", "Premium", "Standard",
];

const ITEM_NOUNS: &[&str] = &["Widget", "Gadget", "Tool", "Device", "Product", "Item"];

pub const DISTRICTS_PER_WAREHOUSE: i32 = 10;
pub const CUSTOMERS_PER_DISTRICT: i32 = 3000;
pub const ITEMS_TOTAL: i32 = 100_000;
pub const ORDERS_PER_DISTRICT: i32 = 3000;
pub const NEW_ORDERS_PER_DISTRICT: i32 = 900;

pub struct TpccDataGen {
    pub scale_factor: i32,
}

impl TpccDataGen {
    pub fn new(scale_factor: i32) -> Self {
        Self {
            scale_factor: scale_factor.max(1),
        }
    }

    // ─── Random helpers ─────────────────────────────────

    fn rand_int(min: i32, max: i32) -> i32 {
        rand::rng().random_range(min..=max)
    }

    fn rand_float(min: f64, max: f64) -> f64 {
        rand::rng().random_range(min..=max)
    }

    fn rand_string(len: usize) -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        let mut rng = rand::rng();
        (0..len)
            .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
            .collect()
    }

    fn rand_alpha_num(len: usize) -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        let mut rng = rand::rng();
        (0..len)
            .map(|_| CHARSET[rng.random_range(0..CHARSET.len())] as char)
            .collect()
    }

    fn gen_street() -> String {
        format!(
            "{} {} St",
            Self::rand_int(1, 9999),
            Self::rand_string(5).to_uppercase()
        )
    }

    fn gen_city() -> String {
        CITIES[rand::rng().random_range(0..CITIES.len())].to_string()
    }

    fn gen_state() -> String {
        STATES[rand::rng().random_range(0..STATES.len())].to_string()
    }

    fn gen_zip() -> String {
        format!("{}{}", Self::rand_int(10000, 99999), Self::rand_int(1000, 9999))
    }

    fn gen_phone() -> String {
        format!(
            "({}) {}-{}",
            Self::rand_int(100, 999),
            Self::rand_int(100, 999),
            Self::rand_int(1000, 9999)
        )
    }

    fn gen_tax() -> f64 {
        (Self::rand_float(0.0, 2000.0) / 10000.0 * 10000.0).round() / 10000.0
    }

    fn gen_discount() -> f64 {
        (Self::rand_float(0.0, 5000.0) / 10000.0 * 10000.0).round() / 10000.0
    }

    fn gen_price() -> f64 {
        (Self::rand_float(100.0, 10000.0) / 100.0 * 100.0).round() / 100.0
    }

    fn gen_item_name() -> String {
        let mut rng = rand::rng();
        format!(
            "{} {}",
            ITEM_PREFIXES[rng.random_range(0..ITEM_PREFIXES.len())],
            ITEM_NOUNS[rng.random_range(0..ITEM_NOUNS.len())]
        )
    }

    fn gen_data(min_len: usize, max_len: usize) -> String {
        let len = Self::rand_int(min_len as i32, max_len as i32) as usize;
        Self::rand_string(len)
    }

    fn gen_dist_info() -> String {
        Self::rand_alpha_num(24)
    }

    fn gen_first_name() -> String {
        FIRST_NAMES[rand::rng().random_range(0..FIRST_NAMES.len())].to_string()
    }

    fn gen_timestamp() -> String {
        let days_ago = Self::rand_int(0, 730);
        let ts = Local::now() - Duration::days(days_ago as i64);
        ts.format("%Y-%m-%d %H:%M:%S").to_string()
    }

    pub fn generate_last_name(customer_id: i32) -> String {
        if customer_id < 1000 {
            SYLLABLES[(customer_id / 100) as usize].to_string()
        } else {
            format!(
                "{}{}",
                SYLLABLES[(customer_id % 1000 / 100) as usize],
                SYLLABLES[(customer_id / 1000) as usize]
            )
        }
    }

    // ─── Data generators (iterators) ────────────────────

    pub fn generate_warehouses(&self) -> Vec<Warehouse> {
        (1..=self.scale_factor)
            .map(|w_id| Warehouse {
                w_id,
                w_name: format!("W{w_id:02}"),
                w_street_1: Self::gen_street(),
                w_street_2: Self::gen_street(),
                w_city: Self::gen_city(),
                w_state: Self::gen_state(),
                w_zip: Self::gen_zip(),
                w_tax: Self::gen_tax(),
                w_ytd: 300000.0,
            })
            .collect()
    }

    pub fn generate_districts(&self) -> Vec<District> {
        let mut result = Vec::new();
        for w_id in 1..=self.scale_factor {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
                result.push(District {
                    d_id,
                    d_w_id: w_id,
                    d_name: format!("D{d_id:02}"),
                    d_street_1: Self::gen_street(),
                    d_street_2: Self::gen_street(),
                    d_city: Self::gen_city(),
                    d_state: Self::gen_state(),
                    d_zip: Self::gen_zip(),
                    d_tax: Self::gen_tax(),
                    d_ytd: 30000.0,
                    d_next_o_id: 3001,
                });
            }
        }
        result
    }

    pub fn generate_items(&self) -> Vec<Item> {
        (1..=ITEMS_TOTAL)
            .map(|i_id| {
                let mut data = Self::gen_data(26, 50);
                if Self::rand_int(0, 100) < 10 {
                    let pos = Self::rand_int(0, (data.len() as i32 - 8).max(0)) as usize;
                    let end = (pos + 8).min(data.len());
                    data = format!("{}{}{}", &data[..pos], "ORIGINAL", &data[end..]);
                }
                Item {
                    i_id,
                    i_im_id: Self::rand_int(1, 10000),
                    i_name: Self::gen_item_name(),
                    i_price: Self::gen_price(),
                    i_data: data,
                }
            })
            .collect()
    }

    pub fn generate_customers(&self) -> Vec<Customer> {
        let mut result = Vec::new();
        for w_id in 1..=self.scale_factor {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
                for c_id in 1..=CUSTOMERS_PER_DISTRICT {
                    let credit = if Self::rand_int(0, 100) < 90 {
                        "GC"
                    } else {
                        "BC"
                    };
                    result.push(Customer {
                        c_id,
                        c_d_id: d_id,
                        c_w_id: w_id,
                        c_first: Self::gen_first_name(),
                        c_middle: "OE".to_string(),
                        c_last: Self::generate_last_name(c_id),
                        c_street_1: Self::gen_street(),
                        c_street_2: Self::gen_street(),
                        c_city: Self::gen_city(),
                        c_state: Self::gen_state(),
                        c_zip: Self::gen_zip(),
                        c_phone: Self::gen_phone(),
                        c_since: Self::gen_timestamp(),
                        c_credit: credit.to_string(),
                        c_credit_lim: 50000.0,
                        c_discount: Self::gen_discount(),
                        c_balance: 10.5,
                        c_ytd_payment: 10.5,
                        c_payment_cnt: 1,
                        c_delivery_cnt: 0,
                        c_data: Self::gen_data(50, 300),
                    });
                }
            }
        }
        result
    }

    pub fn generate_stock(&self) -> Vec<Stock> {
        let mut result = Vec::new();
        for w_id in 1..=self.scale_factor {
            for i_id in 1..=ITEMS_TOTAL {
                let mut data = Self::gen_data(26, 50);
                if Self::rand_int(0, 100) < 10 {
                    let pos = Self::rand_int(0, (data.len() as i32 - 8).max(0)) as usize;
                    let end = (pos + 8).min(data.len());
                    data = format!("{}{}{}", &data[..pos], "ORIGINAL", &data[end..]);
                }
                result.push(Stock {
                    s_i_id: i_id,
                    s_w_id: w_id,
                    s_quantity: Self::rand_int(10, 100),
                    s_dist_01: Self::gen_dist_info(),
                    s_dist_02: Self::gen_dist_info(),
                    s_dist_03: Self::gen_dist_info(),
                    s_dist_04: Self::gen_dist_info(),
                    s_dist_05: Self::gen_dist_info(),
                    s_dist_06: Self::gen_dist_info(),
                    s_dist_07: Self::gen_dist_info(),
                    s_dist_08: Self::gen_dist_info(),
                    s_dist_09: Self::gen_dist_info(),
                    s_dist_10: Self::gen_dist_info(),
                    s_ytd: 0,
                    s_order_cnt: 0,
                    s_remote_cnt: 0,
                    s_data: data,
                });
            }
        }
        result
    }

    pub fn generate_orders(&self) -> Vec<Orders> {
        let mut result = Vec::new();
        for w_id in 1..=self.scale_factor {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
                for o_id in 1..=ORDERS_PER_DISTRICT {
                    let carrier_id = if o_id > 2100 {
                        0
                    } else {
                        Self::rand_int(1, 10)
                    };
                    result.push(Orders {
                        o_id,
                        o_d_id: d_id,
                        o_w_id: w_id,
                        o_c_id: Self::rand_int(1, 3000),
                        o_entry_d: Self::gen_timestamp(),
                        o_carrier_id: carrier_id,
                        o_ol_cnt: 10,
                        o_all_local: 1,
                    });
                }
            }
        }
        result
    }

    pub fn generate_new_orders(&self) -> Vec<NewOrder> {
        let mut result = Vec::new();
        for w_id in 1..=self.scale_factor {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
                for no_id in 2101..(2101 + NEW_ORDERS_PER_DISTRICT) {
                    result.push(NewOrder {
                        no_o_id: no_id,
                        no_d_id: d_id,
                        no_w_id: w_id,
                    });
                }
            }
        }
        result
    }

    pub fn generate_history(&self) -> Vec<History> {
        let mut result = Vec::new();
        for w_id in 1..=self.scale_factor {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
                for c_id in 1..=CUSTOMERS_PER_DISTRICT {
                    result.push(History {
                        h_c_id: c_id,
                        h_c_d_id: d_id,
                        h_c_w_id: w_id,
                        h_d_id: d_id,
                        h_w_id: w_id,
                        h_datetime: Self::gen_timestamp(),
                        h_amount: 10.0,
                        h_data: "Initial deposit".to_string(),
                    });
                }
            }
        }
        result
    }

    pub fn generate_order_lines(&self) -> Vec<OrderLine> {
        let mut result = Vec::new();
        for w_id in 1..=self.scale_factor {
            for d_id in 1..=DISTRICTS_PER_WAREHOUSE {
                for o_id in 1..=ORDERS_PER_DISTRICT {
                    let ol_cnt = 10;
                    for ol_number in 1..=ol_cnt {
                        let i_id = Self::rand_int(1, ITEMS_TOTAL);
                        let quantity = Self::rand_int(1, 10);
                        let amount = quantity as f64 * Self::gen_price();
                        let delivery_d = if o_id <= 2100 {
                            Self::gen_timestamp()
                        } else {
                            "1970-01-01 00:00:00".to_string()
                        };
                        result.push(OrderLine {
                            ol_o_id: o_id,
                            ol_d_id: d_id,
                            ol_w_id: w_id,
                            ol_number,
                            ol_i_id: i_id,
                            ol_supply_w_id: w_id,
                            ol_delivery_d: delivery_d,
                            ol_quantity: quantity,
                            ol_amount: amount,
                            ol_dist_info: Self::gen_dist_info(),
                        });
                    }
                }
            }
        }
        result
    }

    // ─── Transaction parameter generation ───────────────

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
            let available: Vec<i32> = (1..=self.scale_factor).filter(|&id| id != w_id).collect();
            let c_w_id = available[rand::rng().random_range(0..available.len())];
            let c_d_id = Self::rand_int(1, DISTRICTS_PER_WAREHOUSE);
            (c_w_id, c_d_id)
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
        let c_id = self.get_random_customer_id();
        Self::generate_last_name(c_id)
    }
}
