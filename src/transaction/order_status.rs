use tracing::{debug, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

pub async fn execute(cursor: &mut RmdbCursor, gen: &TpccDataGen) -> Result<bool, TpccError> {
    let w_id = gen.get_random_warehouse_id();
    let d_id = gen.get_random_district_id();
    let c_id = gen.get_random_customer_id();
    let c_last = TpccDataGen::generate_last_name(c_id);

    if rand::random::<f64>() < 0.6 {
        let count_result = cursor
            .execute(
                "SELECT COUNT(c_id) FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ?",
                &[
                    SqlParam::Int(w_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Str(c_last.clone()),
                ],
            )
            .await;

        match count_result {
            Ok(r) if !r.is_empty() => {}
            Ok(_) => return Ok(false),
            Err(e) => {
                warn!("[OrderStatus] SELECT COUNT(c_id) 失败: {e}");
                return Ok(false);
            }
        }

        let customer_result = cursor
            .execute(
                "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_last = ? ORDER BY c_first",
                &[
                    SqlParam::Int(w_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Str(c_last),
                ],
            )
            .await;

        match customer_result {
            Ok(r) if !r.is_empty() => {}
            Ok(_) => return Ok(false),
            Err(e) => {
                warn!("[OrderStatus] SELECT customer by last name 失败: {e}");
                return Ok(false);
            }
        }
    } else {
        let customer_result = cursor
            .execute(
                "SELECT c_balance, c_first, c_middle, c_last FROM customer WHERE c_w_id = ? AND c_d_id = ? AND c_id = ?",
                &[
                    SqlParam::Int(w_id as i64),
                    SqlParam::Int(d_id as i64),
                    SqlParam::Int(c_id as i64),
                ],
            )
            .await;

        match customer_result {
            Ok(r) if !r.is_empty() => {}
            Ok(_) => return Ok(false),
            Err(e) => {
                warn!("[OrderStatus] SELECT customer by id 失败: {e}");
                return Ok(false);
            }
        }
    }

    let latest_order = cursor
        .execute(
            "SELECT o_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1",
            &[
                SqlParam::Int(w_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(c_id as i64),
            ],
        )
        .await;

    let latest_order = match latest_order {
        Ok(r) if !r.is_empty() => r,
        Ok(_) => return Ok(false),
        Err(e) => {
            warn!("[OrderStatus] SELECT latest order id 失败: {e}");
            return Ok(false);
        }
    };
    let o_id: i32 = latest_order.rows[0][0].parse().unwrap_or(0);

    let order_result = cursor
        .execute(
            "SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = ?",
            &[
                SqlParam::Int(w_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(c_id as i64),
                SqlParam::Int(o_id as i64),
            ],
        )
        .await;

    match order_result {
        Ok(r) if !r.is_empty() => {}
        Ok(_) => return Ok(false),
        Err(e) => {
            warn!("[OrderStatus] SELECT order 失败: {e}");
            return Ok(false);
        }
    }

    let order_line_result = cursor
        .execute(
            "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id = ?",
            &[
                SqlParam::Int(w_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(o_id as i64),
            ],
        )
        .await;

    match order_line_result {
        Ok(r) => {
            let success = !r.is_empty();
            if success {
                debug!(
                    "[OrderStatus] 查询成功: w_id={w_id}, d_id={d_id}, c_id={c_id}, o_id={o_id}"
                );
            }
            Ok(success)
        }
        Err(e) => {
            warn!("[OrderStatus] SELECT order_line 失败: {e}");
            Ok(false)
        }
    }
}
