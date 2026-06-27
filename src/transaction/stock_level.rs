use tracing::{debug, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

pub async fn execute(cursor: &mut RmdbCursor, gen: &TpccDataGen) -> Result<bool, TpccError> {
    let w_id = gen.get_random_warehouse_id();
    let d_id = gen.get_random_district_id();
    let threshold = gen.get_random_stock_threshold();

    let district_result = cursor
        .execute(
            "SELECT d_next_o_id FROM district WHERE d_id = ? AND d_w_id = ?",
            &[SqlParam::Int(d_id as i64), SqlParam::Int(w_id as i64)],
        )
        .await;

    let district_result = match district_result {
        Ok(r) if !r.is_empty() => r,
        Ok(_) => return Ok(false),
        Err(e) => {
            warn!("[StockLevel] SELECT district 失败: {e}");
            return Ok(false);
        }
    };

    let next_o_id: i32 = district_result.rows[0][0].parse().unwrap_or(0);
    let lower_o_id = next_o_id - 20;

    let order_line_result = cursor
        .execute(
            "SELECT ol_i_id FROM order_line WHERE ol_w_id = ? AND ol_d_id = ? AND ol_o_id < ? AND ol_o_id >= ?",
            &[
                SqlParam::Int(w_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(next_o_id as i64),
                SqlParam::Int(lower_o_id as i64),
            ],
        )
        .await;

    let order_line_result = match order_line_result {
        Ok(r) => r,
        Err(e) => {
            warn!("[StockLevel] SELECT order_line 失败: {e}");
            return Ok(false);
        }
    };

    for row in &order_line_result.rows {
        if row.is_empty() {
            continue;
        }
        let Ok(i_id) = row[0].parse::<i32>() else {
            continue;
        };
        let stock_result = cursor
            .execute(
                "SELECT COUNT(*) FROM stock WHERE s_w_id = ? AND s_i_id = ? AND s_quantity < ?",
                &[
                    SqlParam::Int(w_id as i64),
                    SqlParam::Int(i_id as i64),
                    SqlParam::Int(threshold as i64),
                ],
            )
            .await;
        match stock_result {
            Ok(r) if !r.is_empty() => {}
            Ok(_) => return Ok(false),
            Err(e) => {
                warn!("[StockLevel] SELECT stock count 失败: {e}");
                return Ok(false);
            }
        }
    }

    debug!(
        "[StockLevel] 查询成功: w_id={w_id}, d_id={d_id}, threshold={threshold}, item_rows={}",
        order_line_result.rows.len()
    );
    Ok(true)
}
