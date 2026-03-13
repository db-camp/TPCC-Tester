use chrono::Local;
use tracing::{debug, error, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

pub async fn execute(cursor: &mut RmdbCursor, gen: &TpccDataGen) -> Result<bool, TpccError> {
    let w_id = gen.get_random_warehouse_id();
    let o_carrier_id = gen.get_random_carrier_id();

    // BEGIN
    if let Err(e) = cursor.execute_update("BEGIN", &[]).await {
        error!("[Delivery] BEGIN 失败: {e}");
        return Err(e);
    }

    for d_id in 1..=10 {
        // Step 1: Find oldest undelivered order
        let new_order_result = cursor
            .execute(
                "SELECT MIN(no_o_id) FROM new_orders WHERE no_d_id = ? AND no_w_id = ?",
                &[SqlParam::Int(d_id as i64), SqlParam::Int(w_id as i64)],
            )
            .await;

        let new_order_result = match new_order_result {
            Ok(r) => r,
            Err(e) => {
                warn!("[Delivery Step 1] SELECT MIN(no_o_id) 失败 (d_id={d_id}): {e}");
                continue;
            }
        };

        if new_order_result.is_empty() || new_order_result.rows[0][0].is_empty() {
            continue;
        }

        let o_id: i32 = match new_order_result.rows[0][0].parse() {
            Ok(v) => v,
            Err(_) => continue,
        };

        // Step 2: Remove from new_orders
        if let Err(e) = cursor
            .execute_update(
                "DELETE FROM new_orders WHERE no_o_id = ? AND no_d_id = ? AND no_w_id = ?",
                &[
                    SqlParam::Int(o_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Int(w_id as i64),
                ],
            )
            .await
        {
            warn!("[Delivery Step 2] DELETE new_orders 失败 (d_id={d_id}): {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        // Step 3: Update order with carrier_id
        if let Err(e) = cursor
            .execute_update(
                "UPDATE orders SET o_carrier_id = ? WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?",
                &[
                    SqlParam::Int(o_carrier_id as i64),
                    SqlParam::Int(o_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Int(w_id as i64),
                ],
            )
            .await
        {
            warn!("[Delivery Step 3] UPDATE orders 失败 (d_id={d_id}): {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        // Step 4: Update order_lines with delivery date
        let delivery_date = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        if let Err(e) = cursor
            .execute_update(
                "UPDATE order_line SET ol_delivery_d = ? WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?",
                &[
                    SqlParam::Str(delivery_date),
                    SqlParam::Int(o_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Int(w_id as i64),
                ],
            )
            .await
        {
            warn!("[Delivery Step 4] UPDATE order_line 失败 (d_id={d_id}): {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        // Step 5: Get customer ID
        let order_result = cursor
            .execute(
                "SELECT o_c_id FROM orders WHERE o_id = ? AND o_d_id = ? AND o_w_id = ?",
                &[
                    SqlParam::Int(o_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Int(w_id as i64),
                ],
            )
            .await;

        let order_result = match order_result {
            Ok(r) => r,
            Err(e) => {
                warn!("[Delivery Step 5] SELECT o_c_id 失败 (d_id={d_id}): {e}");
                let _ = cursor.execute_update("ROLLBACK", &[]).await;
                return Ok(false);
            }
        };

        if order_result.is_empty() {
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        let o_c_id: i32 = order_result.rows[0][0].parse().unwrap_or(0);

        // Calculate total amount
        let total_result = cursor
            .execute(
                "SELECT SUM(ol_amount) FROM order_line WHERE ol_o_id = ? AND ol_d_id = ? AND ol_w_id = ?",
                &[
                    SqlParam::Int(o_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Int(w_id as i64),
                ],
            )
            .await;

        let total_result = match total_result {
            Ok(r) => r,
            Err(e) => {
                warn!("[Delivery Step 5] SELECT SUM(ol_amount) 失败 (d_id={d_id}): {e}");
                let _ = cursor.execute_update("ROLLBACK", &[]).await;
                return Ok(false);
            }
        };

        if total_result.is_empty() {
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }

        let order_total: f64 = total_result.rows[0][0].parse().unwrap_or(0.0);

        // Step 6: Update customer balance
        if let Err(e) = cursor
            .execute_update(
                "UPDATE customer SET c_balance = c_balance+?, c_delivery_cnt = c_delivery_cnt+1 WHERE c_id = ? AND c_d_id = ? AND c_w_id = ?",
                &[
                    SqlParam::Float(order_total),
                    SqlParam::Int(o_c_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Int(w_id as i64),
                ],
            )
            .await
        {
            warn!("[Delivery Step 6] UPDATE customer 失败 (d_id={d_id}): {e}");
            let _ = cursor.execute_update("ROLLBACK", &[]).await;
            return Ok(false);
        }
    }

    // COMMIT
    if let Err(e) = cursor.execute_update("COMMIT", &[]).await {
        error!("[Delivery] COMMIT 失败: {e}");
        let _ = cursor.execute_update("ROLLBACK", &[]).await;
        return Ok(false);
    }

    debug!("[Delivery] 事务成功: w_id={w_id}, carrier_id={o_carrier_id}");
    Ok(true)
}
