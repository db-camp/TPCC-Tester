use chrono::Local;
use tracing::{debug, error, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

pub async fn execute(cursor: &mut RmdbCursor, gen: &TpccDataGen) -> Result<bool, TpccError> {
    let w_id = gen.get_random_warehouse_id();
    let d_id = gen.get_random_district_id();
    let c_id = gen.get_random_customer_id();

    // Generate order line items
    let ol_cnt = gen.get_random_order_line_count();

    let ol_i_ids: Vec<i32> = (0..ol_cnt).map(|_| gen.get_random_item_id()).collect();
    let ol_supply_w_ids: Vec<i32> = (0..ol_cnt)
        .map(|_| {
            if rand::random::<f64>() < 0.99 {
                w_id
            } else {
                gen.get_random_warehouse_id()
            }
        })
        .collect();
    let ol_quantities: Vec<i32> = (0..ol_cnt).map(|_| gen.get_random_quantity()).collect();

    // BEGIN
    if let Err(e) = cursor.execute_update("BEGIN", &[]).await {
        error!("[NewOrder] BEGIN 失败: {e}");
        return Err(e);
    }

    // Phase 1: Get district info
    let district_result = cursor
        .execute(
            "SELECT d_tax, d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?",
            &[SqlParam::Int(d_id as i64), SqlParam::Int(w_id as i64)],
        )
        .await;

    let district_result = match district_result {
        Ok(r) => r,
        Err(e) => {
            warn!("[NewOrder Phase 1] SELECT district 失败: {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }
    };

    if district_result.is_empty() {
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    let d_tax: f64 = district_result.rows[0][0].as_f64().unwrap_or(0.0);
    let o_id: i32 = district_result.rows[0][1].as_i32().unwrap_or(0);

    // Update next order ID
    if let Err(e) = cursor
        .execute_update(
            "UPDATE district SET d_next_o_id = d_next_o_id+1 WHERE d_id = ? AND d_w_id = ?",
            &[SqlParam::Int(d_id as i64), SqlParam::Int(w_id as i64)],
        )
        .await
    {
        warn!("[NewOrder Phase 1] UPDATE district 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    // Get customer and warehouse info
    let customer_result = cursor
        .execute(
            "SELECT c_discount, c_last, c_credit, w_tax FROM customer, warehouse WHERE c_w_id = w_id AND c_d_id = ? AND c_id = ? AND w_id = ?",
            &[
                SqlParam::Int(d_id as i64),
                SqlParam::Int(c_id as i64),
                SqlParam::Int(w_id as i64),
            ],
        )
        .await;

    let customer_result = match customer_result {
        Ok(r) => r,
        Err(e) => {
            warn!("[NewOrder Phase 1] SELECT customer,warehouse 失败: {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }
    };

    if customer_result.is_empty() {
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    let c_discount: f64 = customer_result.rows[0][0].as_f64().unwrap_or(0.0);
    let w_tax: f64 = customer_result.rows[0][3].as_f64().unwrap_or(0.0);

    // Phase 2: Create order and new order
    let o_entry_d = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let o_all_local: i32 = if ol_supply_w_ids.iter().all(|&sw| sw == w_id) {
        1
    } else {
        0
    };

    // Insert order
    if let Err(e) = cursor
        .execute_update(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            &[
                SqlParam::Int(o_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(w_id as i64),
                SqlParam::Int(c_id as i64),
                SqlParam::Str(o_entry_d),
                SqlParam::Int(-1),
                SqlParam::Int(ol_cnt as i64),
                SqlParam::Int(o_all_local as i64),
            ],
        )
        .await
    {
        warn!("[NewOrder Phase 2] INSERT orders 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    // Insert new_orders
    if let Err(e) = cursor
        .execute_update(
            "INSERT INTO new_orders VALUES (?, ?, ?)",
            &[
                SqlParam::Int(o_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(w_id as i64),
            ],
        )
        .await
    {
        warn!("[NewOrder Phase 2] INSERT new_orders 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    // Phase 3: Process order lines
    let mut total_amount: f64 = 0.0;
    for ol_number in 1..=ol_cnt {
        let idx = (ol_number - 1) as usize;

        // Get item info
        let item_result = cursor
            .execute(
                "SELECT i_price, i_name, i_data FROM item WHERE i_id = ?",
                &[SqlParam::Int(ol_i_ids[idx] as i64)],
            )
            .await;

        let item_result = match item_result {
            Ok(r) => r,
            Err(e) => {
                warn!("[NewOrder Phase 3] SELECT item 失败 (ol_number={ol_number}): {e}");
                let _ = cursor.execute_update("ROLLBACK", &[]).await;
                return Ok(false);
            }
        };

        if item_result.is_empty() {
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        let i_price: f64 = item_result.rows[0][0].as_f64().unwrap_or(0.0);
        let _i_data = &item_result.rows[0][2];

        // Get stock info with dynamic column name
        let stock_sql = format!(
            "SELECT s_quantity, s_dist_{:02}, s_ytd, s_order_cnt, s_remote_cnt, s_data FROM stock WHERE s_i_id = ? AND s_w_id = ?",
            d_id
        );
        let stock_result = cursor
            .execute(
                &stock_sql,
                &[
                    SqlParam::Int(ol_i_ids[idx] as i64),
                    SqlParam::Int(ol_supply_w_ids[idx] as i64),
                ],
            )
            .await;

        let stock_result = match stock_result {
            Ok(r) => r,
            Err(e) => {
                warn!("[NewOrder Phase 3] SELECT stock 失败 (ol_number={ol_number}): {e}");
                let _ = cursor.execute_update("ROLLBACK", &[]).await;
                return Ok(false);
            }
        };

        if stock_result.is_empty() {
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        let mut s_quantity: i32 = stock_result.rows[0][0].as_i32().unwrap_or(0);
        let s_dist = stock_result.rows[0][1].as_str().to_string();
        let mut s_ytd: f64 = stock_result.rows[0][2].as_f64().unwrap_or(0.0);
        let mut s_order_cnt: i32 = stock_result.rows[0][3].as_i32().unwrap_or(0);
        let mut s_remote_cnt: i32 = stock_result.rows[0][4].as_i32().unwrap_or(0);
        let _s_data = &stock_result.rows[0][5];

        // Update stock quantity
        if s_quantity >= ol_quantities[idx] + 10 {
            s_quantity -= ol_quantities[idx];
        } else {
            s_quantity = s_quantity - ol_quantities[idx] + 91;
        }

        s_ytd += ol_quantities[idx] as f64;
        s_order_cnt += 1;
        if ol_supply_w_ids[idx] != w_id {
            s_remote_cnt += 1;
        }

        // Update stock
        if let Err(e) = cursor
            .execute_update(
                "UPDATE stock SET s_quantity = ?, s_ytd = ?, s_order_cnt = ?, s_remote_cnt = ? WHERE s_i_id = ? AND s_w_id = ?",
                &[
                    SqlParam::Int(s_quantity as i64),
                    SqlParam::Float(s_ytd),
                    SqlParam::Int(s_order_cnt as i64),
                    SqlParam::Int(s_remote_cnt as i64),
                    SqlParam::Int(ol_i_ids[idx] as i64),
                    SqlParam::Int(ol_supply_w_ids[idx] as i64),
                ],
            )
            .await
        {
            warn!("[NewOrder Phase 3] UPDATE stock 失败 (ol_number={ol_number}): {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        // Calculate order line amount
        let ol_amount = ol_quantities[idx] as f64 * i_price;

        // Insert order line
        if let Err(e) = cursor
            .execute_update(
                "INSERT INTO order_line VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                &[
                    SqlParam::Int(o_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Int(w_id as i64),
                    SqlParam::Int(ol_number as i64),
                    SqlParam::Int(ol_i_ids[idx] as i64),
                    SqlParam::Int(ol_supply_w_ids[idx] as i64),
                    SqlParam::Str("1970-01-01 00:00:00".to_string()),
                    SqlParam::Int(ol_quantities[idx] as i64),
                    SqlParam::Float(ol_amount),
                    SqlParam::Str(s_dist.clone()),
                ],
            )
            .await
        {
            warn!("[NewOrder Phase 3] INSERT order_line 失败 (ol_number={ol_number}): {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        total_amount += ol_amount;
    }

    // Calculate final amount
    let _total = total_amount * (1.0 - c_discount) * (1.0 + w_tax + d_tax);

    // Commit
    if let Err(e) = cursor.execute_update("COMMIT", &[]).await {
        error!("[NewOrder] COMMIT 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    debug!("[NewOrder] 事务成功: w_id={w_id}, d_id={d_id}, o_id={o_id}, ol_cnt={ol_cnt}");
    Ok(true)
}
