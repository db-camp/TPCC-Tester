use tracing::debug;

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

pub async fn execute(cursor: &mut RmdbCursor, gen: &TpccDataGen) -> Result<bool, TpccError> {
    let w_id = gen.get_random_warehouse_id();
    let _d_id = gen.get_random_district_id();
    let threshold = gen.get_random_stock_threshold();

    let result = cursor
        .execute(
            "SELECT COUNT(s_i_id) FROM stock WHERE s_w_id = ? AND s_quantity < ?",
            &[
                SqlParam::Int(w_id as i64),
                SqlParam::Int(threshold as i64),
            ],
        )
        .await;

    match result {
        Ok(r) => {
            let success = !r.is_empty();
            if success {
                debug!("[StockLevel] 查询成功: w_id={w_id}, threshold={threshold}");
            }
            Ok(success)
        }
        Err(_) => Ok(false),
    }
}
