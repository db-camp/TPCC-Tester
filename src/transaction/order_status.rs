use tracing::debug;

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;

pub async fn execute(cursor: &mut RmdbCursor, gen: &TpccDataGen) -> Result<bool, TpccError> {
    let w_id = gen.get_random_warehouse_id();
    let d_id = gen.get_random_district_id();
    let c_id = gen.get_random_customer_id();

    let result = cursor
        .execute(
            "SELECT * FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? ORDER BY o_id DESC LIMIT 1",
            &[
                SqlParam::Int(w_id as i64),
                SqlParam::Int(d_id as i64),
                SqlParam::Int(c_id as i64),
            ],
        )
        .await;

    match result {
        Ok(r) => {
            let success = !r.is_empty();
            if success {
                debug!("[OrderStatus] 查询成功: w_id={w_id}, d_id={d_id}, c_id={c_id}");
            }
            Ok(success)
        }
        Err(_) => Ok(false),
    }
}
