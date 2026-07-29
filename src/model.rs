use crate::connection::cursor::SqlParam;

// ─── Warehouse ──────────────────────────────────────────

pub struct Warehouse {
    pub w_id: i32,
    pub w_name: String,
    pub w_street_1: String,
    pub w_street_2: String,
    pub w_city: String,
    pub w_state: String,
    pub w_zip: String,
    pub w_tax: f64,
    pub w_ytd: f64,
}

impl Warehouse {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.w_id as i64),
            SqlParam::Str(self.w_name.clone()),
            SqlParam::Str(self.w_street_1.clone()),
            SqlParam::Str(self.w_street_2.clone()),
            SqlParam::Str(self.w_city.clone()),
            SqlParam::Str(self.w_state.clone()),
            SqlParam::Str(self.w_zip.clone()),
            SqlParam::Float(self.w_tax),
            SqlParam::Float(self.w_ytd),
        ]
    }
}

// ─── District ───────────────────────────────────────────

pub struct District {
    pub d_id: i32,
    pub d_w_id: i32,
    pub d_name: String,
    pub d_street_1: String,
    pub d_street_2: String,
    pub d_city: String,
    pub d_state: String,
    pub d_zip: String,
    pub d_tax: f64,
    pub d_ytd: f64,
    pub d_next_o_id: i32,
}

impl District {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.d_id as i64),
            SqlParam::Int(self.d_w_id as i64),
            SqlParam::Str(self.d_name.clone()),
            SqlParam::Str(self.d_street_1.clone()),
            SqlParam::Str(self.d_street_2.clone()),
            SqlParam::Str(self.d_city.clone()),
            SqlParam::Str(self.d_state.clone()),
            SqlParam::Str(self.d_zip.clone()),
            SqlParam::Float(self.d_tax),
            SqlParam::Float(self.d_ytd),
            SqlParam::Int(self.d_next_o_id as i64),
        ]
    }
}

// ─── Customer ───────────────────────────────────────────

pub struct Customer {
    pub c_id: i32,
    pub c_d_id: i32,
    pub c_w_id: i32,
    pub c_first: String,
    pub c_middle: String,
    pub c_last: String,
    pub c_street_1: String,
    pub c_street_2: String,
    pub c_city: String,
    pub c_state: String,
    pub c_zip: String,
    pub c_phone: String,
    pub c_since: String,
    pub c_credit: String,
    pub c_credit_lim: i32,
    pub c_discount: f64,
    pub c_balance: f64,
    pub c_ytd_payment: f64,
    pub c_payment_cnt: i32,
    pub c_delivery_cnt: i32,
    pub c_data: String,
}

impl Customer {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.c_id as i64),
            SqlParam::Int(self.c_d_id as i64),
            SqlParam::Int(self.c_w_id as i64),
            SqlParam::Str(self.c_first.clone()),
            SqlParam::Str(self.c_middle.clone()),
            SqlParam::Str(self.c_last.clone()),
            SqlParam::Str(self.c_street_1.clone()),
            SqlParam::Str(self.c_street_2.clone()),
            SqlParam::Str(self.c_city.clone()),
            SqlParam::Str(self.c_state.clone()),
            SqlParam::Str(self.c_zip.clone()),
            SqlParam::Str(self.c_phone.clone()),
            SqlParam::Str(self.c_since.clone()),
            SqlParam::Str(self.c_credit.clone()),
            SqlParam::Int(self.c_credit_lim as i64),
            SqlParam::Float(self.c_discount),
            SqlParam::Float(self.c_balance),
            SqlParam::Float(self.c_ytd_payment),
            SqlParam::Int(self.c_payment_cnt as i64),
            SqlParam::Int(self.c_delivery_cnt as i64),
            SqlParam::Str(self.c_data.clone()),
        ]
    }
}

// ─── Item ───────────────────────────────────────────────

pub struct Item {
    pub i_id: i32,
    pub i_im_id: i32,
    pub i_name: String,
    pub i_price: f64,
    pub i_data: String,
}

impl Item {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.i_id as i64),
            SqlParam::Int(self.i_im_id as i64),
            SqlParam::Str(self.i_name.clone()),
            SqlParam::Float(self.i_price),
            SqlParam::Str(self.i_data.clone()),
        ]
    }
}

// ─── Stock ──────────────────────────────────────────────

pub struct Stock {
    pub s_i_id: i32,
    pub s_w_id: i32,
    pub s_quantity: i32,
    pub s_dist_01: String,
    pub s_dist_02: String,
    pub s_dist_03: String,
    pub s_dist_04: String,
    pub s_dist_05: String,
    pub s_dist_06: String,
    pub s_dist_07: String,
    pub s_dist_08: String,
    pub s_dist_09: String,
    pub s_dist_10: String,
    pub s_ytd: f64,
    pub s_order_cnt: i32,
    pub s_remote_cnt: i32,
    pub s_data: String,
}

impl Stock {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.s_i_id as i64),
            SqlParam::Int(self.s_w_id as i64),
            SqlParam::Int(self.s_quantity as i64),
            SqlParam::Str(self.s_dist_01.clone()),
            SqlParam::Str(self.s_dist_02.clone()),
            SqlParam::Str(self.s_dist_03.clone()),
            SqlParam::Str(self.s_dist_04.clone()),
            SqlParam::Str(self.s_dist_05.clone()),
            SqlParam::Str(self.s_dist_06.clone()),
            SqlParam::Str(self.s_dist_07.clone()),
            SqlParam::Str(self.s_dist_08.clone()),
            SqlParam::Str(self.s_dist_09.clone()),
            SqlParam::Str(self.s_dist_10.clone()),
            SqlParam::Float(self.s_ytd),
            SqlParam::Int(self.s_order_cnt as i64),
            SqlParam::Int(self.s_remote_cnt as i64),
            SqlParam::Str(self.s_data.clone()),
        ]
    }
}

// ─── Orders ─────────────────────────────────────────────

pub struct Orders {
    pub o_id: i32,
    pub o_d_id: i32,
    pub o_w_id: i32,
    pub o_c_id: i32,
    pub o_entry_d: String,
    pub o_carrier_id: i32,
    pub o_ol_cnt: i32,
    pub o_all_local: i32,
}

impl Orders {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.o_id as i64),
            SqlParam::Int(self.o_d_id as i64),
            SqlParam::Int(self.o_w_id as i64),
            SqlParam::Int(self.o_c_id as i64),
            SqlParam::Str(self.o_entry_d.clone()),
            SqlParam::Int(self.o_carrier_id as i64),
            SqlParam::Int(self.o_ol_cnt as i64),
            SqlParam::Int(self.o_all_local as i64),
        ]
    }
}

// ─── NewOrder ───────────────────────────────────────────

pub struct NewOrder {
    pub no_o_id: i32,
    pub no_d_id: i32,
    pub no_w_id: i32,
}

impl NewOrder {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.no_o_id as i64),
            SqlParam::Int(self.no_d_id as i64),
            SqlParam::Int(self.no_w_id as i64),
        ]
    }
}

// ─── History ────────────────────────────────────────────

pub struct History {
    pub h_c_id: i32,
    pub h_c_d_id: i32,
    pub h_c_w_id: i32,
    pub h_d_id: i32,
    pub h_w_id: i32,
    pub h_date: String,
    pub h_amount: f64,
    pub h_data: String,
}

impl History {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.h_c_id as i64),
            SqlParam::Int(self.h_c_d_id as i64),
            SqlParam::Int(self.h_c_w_id as i64),
            SqlParam::Int(self.h_d_id as i64),
            SqlParam::Int(self.h_w_id as i64),
            SqlParam::Str(self.h_date.clone()),
            SqlParam::Float(self.h_amount),
            SqlParam::Str(self.h_data.clone()),
        ]
    }
}

// ─── OrderLine ──────────────────────────────────────────

pub struct OrderLine {
    pub ol_o_id: i32,
    pub ol_d_id: i32,
    pub ol_w_id: i32,
    pub ol_number: i32,
    pub ol_i_id: i32,
    pub ol_supply_w_id: i32,
    pub ol_delivery_d: String,
    pub ol_quantity: i32,
    pub ol_amount: f64,
    pub ol_dist_info: String,
}

impl OrderLine {
    pub fn to_sql_params(&self) -> Vec<SqlParam> {
        vec![
            SqlParam::Int(self.ol_o_id as i64),
            SqlParam::Int(self.ol_d_id as i64),
            SqlParam::Int(self.ol_w_id as i64),
            SqlParam::Int(self.ol_number as i64),
            SqlParam::Int(self.ol_i_id as i64),
            SqlParam::Int(self.ol_supply_w_id as i64),
            SqlParam::Str(self.ol_delivery_d.clone()),
            SqlParam::Int(self.ol_quantity as i64),
            SqlParam::Float(self.ol_amount),
            SqlParam::Str(self.ol_dist_info.clone()),
        ]
    }
}
