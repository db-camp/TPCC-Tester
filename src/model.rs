use std::fmt::Write as FmtWrite;

pub trait ToCsvRow {
    fn csv_header() -> &'static str;
    fn to_csv_row(&self, buf: &mut String);
}

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

impl ToCsvRow for Warehouse {
    fn csv_header() -> &'static str {
        "w_id,w_name,w_street_1,w_street_2,w_city,w_state,w_zip,w_tax,w_ytd"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{},{},{},{},{},{},{}",
            self.w_id, self.w_name, self.w_street_1, self.w_street_2,
            self.w_city, self.w_state, self.w_zip, self.w_tax, self.w_ytd);
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

impl ToCsvRow for District {
    fn csv_header() -> &'static str {
        "d_id,d_w_id,d_name,d_street_1,d_street_2,d_city,d_state,d_zip,d_tax,d_ytd,d_next_o_id"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{},{},{},{},{},{},{},{},{}",
            self.d_id, self.d_w_id, self.d_name, self.d_street_1, self.d_street_2,
            self.d_city, self.d_state, self.d_zip, self.d_tax, self.d_ytd, self.d_next_o_id);
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
    pub c_credit_lim: f64,
    pub c_discount: f64,
    pub c_balance: f64,
    pub c_ytd_payment: f64,
    pub c_payment_cnt: i32,
    pub c_delivery_cnt: i32,
    pub c_data: String,
}

impl ToCsvRow for Customer {
    fn csv_header() -> &'static str {
        "c_id,c_d_id,c_w_id,c_first,c_middle,c_last,c_street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_since,c_credit,c_credit_lim,c_discount,c_balance,c_ytd_payment,c_payment_cnt,c_delivery_cnt,c_data"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.c_id, self.c_d_id, self.c_w_id, self.c_first, self.c_middle, self.c_last,
            self.c_street_1, self.c_street_2, self.c_city, self.c_state, self.c_zip,
            self.c_phone, self.c_since, self.c_credit, self.c_credit_lim, self.c_discount,
            self.c_balance, self.c_ytd_payment, self.c_payment_cnt, self.c_delivery_cnt,
            self.c_data);
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

impl ToCsvRow for Item {
    fn csv_header() -> &'static str {
        "i_id,i_im_id,i_name,i_price,i_data"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{},{},{}",
            self.i_id, self.i_im_id, self.i_name, self.i_price, self.i_data);
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
    pub s_ytd: i32,
    pub s_order_cnt: i32,
    pub s_remote_cnt: i32,
    pub s_data: String,
}

impl ToCsvRow for Stock {
    fn csv_header() -> &'static str {
        "s_i_id,s_w_id,s_quantity,s_dist_01,s_dist_02,s_dist_03,s_dist_04,s_dist_05,s_dist_06,s_dist_07,s_dist_08,s_dist_09,s_dist_10,s_ytd,s_order_cnt,s_remote_cnt,s_data"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.s_i_id, self.s_w_id, self.s_quantity,
            self.s_dist_01, self.s_dist_02, self.s_dist_03, self.s_dist_04, self.s_dist_05,
            self.s_dist_06, self.s_dist_07, self.s_dist_08, self.s_dist_09, self.s_dist_10,
            self.s_ytd, self.s_order_cnt, self.s_remote_cnt, self.s_data);
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

impl ToCsvRow for Orders {
    fn csv_header() -> &'static str {
        "o_id,o_d_id,o_w_id,o_c_id,o_entry_d,o_carrier_id,o_ol_cnt,o_all_local"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{},{},{},{},{},{}",
            self.o_id, self.o_d_id, self.o_w_id, self.o_c_id,
            self.o_entry_d, self.o_carrier_id, self.o_ol_cnt, self.o_all_local);
    }
}

// ─── NewOrder ───────────────────────────────────────────

pub struct NewOrder {
    pub no_o_id: i32,
    pub no_d_id: i32,
    pub no_w_id: i32,
}

impl ToCsvRow for NewOrder {
    fn csv_header() -> &'static str {
        "no_o_id,no_d_id,no_w_id"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{}", self.no_o_id, self.no_d_id, self.no_w_id);
    }
}

// ─── History ────────────────────────────────────────────

pub struct History {
    pub h_c_id: i32,
    pub h_c_d_id: i32,
    pub h_c_w_id: i32,
    pub h_d_id: i32,
    pub h_w_id: i32,
    pub h_datetime: String,
    pub h_amount: f64,
    pub h_data: String,
}

impl ToCsvRow for History {
    fn csv_header() -> &'static str {
        "h_c_id,h_c_d_id,h_c_w_id,h_d_id,h_w_id,h_datetime,h_amount,h_data"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{},{},{},{},{},{}",
            self.h_c_id, self.h_c_d_id, self.h_c_w_id, self.h_d_id, self.h_w_id,
            self.h_datetime, self.h_amount, self.h_data);
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

impl ToCsvRow for OrderLine {
    fn csv_header() -> &'static str {
        "ol_o_id,ol_d_id,ol_w_id,ol_number,ol_i_id,ol_supply_w_id,ol_delivery_d,ol_quantity,ol_amount,ol_dist_info"
    }
    fn to_csv_row(&self, buf: &mut String) {
        let _ = writeln!(buf, "{},{},{},{},{},{},{},{},{},{}",
            self.ol_o_id, self.ol_d_id, self.ol_w_id, self.ol_number, self.ol_i_id,
            self.ol_supply_w_id, self.ol_delivery_d, self.ol_quantity, self.ol_amount,
            self.ol_dist_info);
    }
}
