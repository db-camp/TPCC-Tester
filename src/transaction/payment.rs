use chrono::Local;
use tracing::{debug, error, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

pub async fn execute(cursor: &mut RmdbCursor, gen: &TpccDataGen) -> Result<bool, TpccError> {
    let w_id = gen.get_random_warehouse_id();
    let d_id = gen.get_random_district_id();
    let c_id = gen.get_random_customer_id();
    let amount = gen.get_random_payment_amount();

    // BEGIN
    if let Err(e) = cursor.execute_update("BEGIN", &[]).await {
        error!("[Payment] BEGIN 失败: {e}");
        return Err(e);
    }

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

    // Step 1: Get warehouse info
    let wh_result = cursor
        .execute(
            "SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = ?",
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

    let w_name = wh_result.rows[0][5].clone();

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

    // Step 2: Get district info
    let dist_result = cursor
        .execute(
            "SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = ? AND d_id = ?",
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

    let d_name = dist_result.rows[0][5].clone();

    // Step 3: Get customer info
    let cust_result = cursor
        .execute(
            "SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            &[
                SqlParam::Int(w_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(c_id as i64),
            ],
        )
        .await;

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

    let c_balance: f64 = cust_result.rows[0][12].parse().unwrap_or(0.0);

    // Step 4: Update customer
    let new_balance = c_balance - amount;

    if let Err(e) = cursor
        .execute_update(
            "UPDATE customer SET c_balance = ? WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
            &[
                SqlParam::Float(new_balance),
                SqlParam::Int(w_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(c_id as i64),
            ],
        )
        .await
    {
        warn!("[Payment Step 4] UPDATE customer 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
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
                SqlParam::Int(d_id as i64),
                SqlParam::Int(w_id as i64),
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
