use chrono::Local;
use tracing::{debug, error, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

pub async fn execute(cursor: &mut RmdbCursor, gen: &TpccDataGen) -> Result<bool, TpccError> {
    let w_id = gen.get_random_warehouse_id();
    let d_id = gen.get_random_district_id();
    let (c_w_id, c_d_id) = gen.get_payment_customer_warehouse(w_id, d_id);
    let amount = gen.get_random_payment_amount();

    let by_id = rand::random::<f64>() < 0.6;
    let c_id_or_last: Either = if by_id {
        Either::Id(gen.get_random_customer_id())
    } else {
        Either::Name(gen.get_random_customer_last_name())
    };

    // BEGIN
    if let Err(e) = cursor.execute_update("BEGIN", &[]).await {
        error!("[Payment] BEGIN 失败: {e}");
        return Err(e);
    }

    // Step 1: Get warehouse info
    let wh_result = cursor
        .execute(
            "SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd FROM warehouse WHERE w_id = ?",
            &[SqlParam::Int(w_id as i64)],
        )
        .await;

    let wh_result = match wh_result {
        Ok(r) => r,
        Err(e) => {
            warn!("[Payment Step 1] SELECT warehouse 失败: {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }
    };

    if wh_result.is_empty() {
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    let w_name = wh_result.rows[0][0].as_str().to_string();

    // Update warehouse YTD
    if let Err(e) = cursor
        .execute_update(
            "UPDATE warehouse SET w_ytd = w_ytd+? WHERE w_id = ?",
            &[SqlParam::Float(amount), SqlParam::Int(w_id as i64)],
        )
        .await
    {
        warn!("[Payment Step 1] UPDATE warehouse 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    // Step 2: Get district info
    let dist_result = cursor
        .execute(
            "SELECT d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_ytd FROM district WHERE d_w_id = ? AND d_id = ?",
            &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
        )
        .await;

    let dist_result = match dist_result {
        Ok(r) => r,
        Err(e) => {
            warn!("[Payment Step 2] SELECT district 失败: {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }
    };

    if dist_result.is_empty() {
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    let d_name = dist_result.rows[0][0].as_str().to_string();

    // Update district YTD
    if let Err(e) = cursor
        .execute_update(
            "UPDATE district SET d_ytd = d_ytd+? WHERE d_w_id = ? AND d_id = ?",
            &[
                SqlParam::Float(amount),
                SqlParam::Int(w_id as i64),
                SqlParam::Int(d_id as i64),
            ],
        )
        .await
    {
        warn!("[Payment Step 2] UPDATE district 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    // Step 3: Get customer info
    let cust_result = if by_id {
        let c_id = match &c_id_or_last {
            Either::Id(id) => *id,
            _ => unreachable!(),
        };
        cursor
            .execute(
                "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                &[
                    SqlParam::Int(c_w_id as i64),
                    SqlParam::Int(c_d_id as i64),
                    SqlParam::Int(c_id as i64),
                ],
            )
            .await
    } else {
        let c_last = match &c_id_or_last {
            Either::Name(name) => name.clone(),
            _ => unreachable!(),
        };
        cursor
            .execute(
                "SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_data FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                &[
                    SqlParam::Int(c_w_id as i64),
                    SqlParam::Int(c_d_id as i64),
                    SqlParam::Str(c_last),
                ],
            )
            .await
    };

    let cust_result = match cust_result {
        Ok(r) => r,
        Err(e) => {
            warn!("[Payment Step 3] SELECT customer 失败: {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }
    };

    if cust_result.is_empty() {
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    // Select middle customer for last-name lookup
    let customer_row = if by_id {
        &cust_result.rows[0]
    } else {
        let mid = cust_result.rows.len() / 2;
        &cust_result.rows[mid]
    };

    let c_id: i32 = customer_row[0].as_i32().unwrap_or(0);
    let c_credit = customer_row[11].as_str();
    let c_balance: f64 = customer_row[14].as_f64().unwrap_or(0.0);
    let c_ytd_payment: f64 = customer_row[15].as_f64().unwrap_or(0.0);
    let c_payment_cnt: i32 = customer_row[16].as_i32().unwrap_or(0);
    let c_data = customer_row[17].as_str();

    // Step 4: Update customer
    let new_balance = c_balance - amount;
    let new_ytd_payment = c_ytd_payment + amount;
    let new_payment_cnt = c_payment_cnt + 1;

    if c_credit == "BC" {
        let payment_info = format!("{c_id}{c_d_id}{c_w_id}{d_id}{w_id}{amount:.2}");
        let mut new_data = format!("{payment_info}{c_data}");
        new_data.truncate(300);

        if let Err(e) = cursor
            .execute_update(
                "UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ?, c_data = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                &[
                    SqlParam::Float(new_balance),
                    SqlParam::Float(new_ytd_payment),
                    SqlParam::Int(new_payment_cnt as i64),
                    SqlParam::Str(new_data),
                    SqlParam::Int(c_w_id as i64),
                    SqlParam::Int(c_d_id as i64),
                    SqlParam::Int(c_id as i64),
                ],
            )
            .await
        {
            warn!("[Payment Step 4] UPDATE customer (BC) 失败: {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }
    } else {
        if let Err(e) = cursor
            .execute_update(
                "UPDATE customer SET c_balance = ?, c_ytd_payment = ?, c_payment_cnt = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                &[
                    SqlParam::Float(new_balance),
                    SqlParam::Float(new_ytd_payment),
                    SqlParam::Int(new_payment_cnt as i64),
                    SqlParam::Int(c_w_id as i64),
                    SqlParam::Int(c_d_id as i64),
                    SqlParam::Int(c_id as i64),
                ],
            )
            .await
        {
            warn!("[Payment Step 4] UPDATE customer (GC) 失败: {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }
    }

    // Step 5: Insert history record
    let w_name_trunc: String = w_name.chars().take(10).collect();
    let d_name_trunc: String = d_name.chars().take(10).collect();
    let h_data = format!("{w_name_trunc}    {d_name_trunc}");
    let h_date = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

    if let Err(e) = cursor
        .execute_update(
            "INSERT INTO history VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            &[
                SqlParam::Int(c_id as i64),
                SqlParam::Int(c_d_id as i64),
                SqlParam::Int(c_w_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(w_id as i64),
                SqlParam::Str(h_date),
                SqlParam::Float(amount),
                SqlParam::Str(h_data),
            ],
        )
        .await
    {
        warn!("[Payment Step 5] INSERT history 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    // COMMIT
    if let Err(e) = cursor.execute_update("COMMIT", &[]).await {
        error!("[Payment] COMMIT 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    debug!("[Payment] 事务成功: w_id={w_id}, d_id={d_id}, c_id={c_id}, amount={amount:.2}");
    Ok(true)
}

enum Either {
    Id(i32),
    Name(String),
}
