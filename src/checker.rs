use tracing::{error, info, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::error::TpccError;

pub struct ConsistencyChecker<'a> {
    cursor: &'a mut RmdbCursor,
    scale_factor: i32,
}

impl<'a> ConsistencyChecker<'a> {
    pub fn new(cursor: &'a mut RmdbCursor, scale_factor: i32) -> Self {
        Self {
            cursor,
            scale_factor,
        }
    }

    pub async fn run_all_checks(&mut self) -> Result<bool, TpccError> {
        info!("========================================");
        info!("       TPC-C 一致性检查");
        info!("========================================");

        let mut all_passed = true;

        // Check 1: Table row counts
        if !self.check_table_counts().await? {
            all_passed = false;
        }

        // Check 2: District-Order consistency
        if !self.check_district_order_consistency().await? {
            all_passed = false;
        }

        // Check 3: NewOrder continuity
        if !self.check_new_orders_consistency().await? {
            all_passed = false;
        }

        // Check 4: OrderLine consistency
        if !self.check_order_line_consistency().await? {
            all_passed = false;
        }

        info!("========================================");
        if all_passed {
            info!("  所有一致性检查通过");
        } else {
            error!("  部分一致性检查失败");
        }
        info!("========================================");

        Ok(all_passed)
    }

    pub async fn show_stats(&mut self) -> Result<(), TpccError> {
        info!("========================================");
        info!("       数据库表行数统计");
        info!("========================================");

        let tables = [
            "warehouse",
            "district",
            "customer",
            "item",
            "stock",
            "orders",
            "order_line",
            "new_orders",
            "history",
        ];

        for table in &tables {
            let sql = format!("SELECT COUNT(*) FROM {table}");
            match self.cursor.execute(&sql, &[]).await {
                Ok(result) => {
                    if let Some(row) = result.rows.first() {
                        if let Some(val) = row.first() {
                            info!("  {table:>15}: {val:>12}");
                        }
                    }
                }
                Err(e) => {
                    warn!("  {table:>15}: 查询失败 - {e}");
                    warn!("  建议: 该查询使用了 COUNT 聚合函数，请确认数据库支持此功能");
                }
            }
        }

        info!("========================================");
        Ok(())
    }

    async fn check_table_counts(&mut self) -> Result<bool, TpccError> {
        info!("[检查 1/4] 表行数检查");

        let sf = self.scale_factor as i64;
        let expected: Vec<(&str, i64)> = vec![
            ("warehouse", sf),
            ("district", sf * 10),
            ("item", 100_000),
            ("customer", sf * 10 * 3000),
            ("stock", sf * 100_000),
            ("orders", sf * 10 * 3000),
            ("order_line", sf * 10 * 3000 * 10),
            ("new_orders", sf * 10 * 900),
            ("history", sf * 10 * 3000),
        ];

        let mut all_ok = true;
        for (table, exp) in &expected {
            let sql = format!("SELECT COUNT(*) FROM {table}");
            match self.cursor.execute(&sql, &[]).await {
                Ok(result) => {
                    if let Some(row) = result.rows.first() {
                        if let Some(val) = row.first() {
                            let actual: i64 = val.parse().unwrap_or(0);
                            if actual == *exp {
                                info!("  {table:>15}: {actual}/{exp} PASS");
                            } else {
                                error!(
                                    "  {table:>15}: {actual}/{exp} FAIL (差异: {})",
                                    actual - exp
                                );
                                all_ok = false;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("  {table:>15}: 查询失败 FAIL - {e}");
                    error!("  建议: 请检查 INSERT 语句是否正确执行，可使用 --stats 查看各表行数");
                    all_ok = false;
                }
            }
        }
        Ok(all_ok)
    }

    async fn check_district_order_consistency(&mut self) -> Result<bool, TpccError> {
        info!("[检查 2/4] District-Order 一致性 (d_next_o_id - 1 == MAX(o_id) == MAX(no_o_id))");

        let mut all_ok = true;

        for w_id in 1..=self.scale_factor {
            for d_id in 1..=10 {
                // Get d_next_o_id
                let d_result = self
                    .cursor
                    .execute(
                        "SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?",
                        &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
                    )
                    .await;

                let d_next_o_id: i64 = match d_result {
                    Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
                    Ok(_) => {
                        warn!("  District {w_id}-{d_id} 未找到");
                        all_ok = false;
                        continue;
                    }
                    Err(e) => {
                        error!("  District {w_id}-{d_id} 查询失败: {e}");
                        all_ok = false;
                        continue;
                    }
                };

                // Get MAX(o_id)
                let max_o = self
                    .cursor
                    .execute(
                        "SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ?",
                        &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
                    )
                    .await;

                let max_o_id: i64 = match max_o {
                    Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
                    Ok(_) => {
                        warn!("  Orders MAX(o_id) {w_id}-{d_id} 未找到");
                        all_ok = false;
                        continue;
                    }
                    Err(e) => {
                        error!("  Orders MAX(o_id) {w_id}-{d_id} 查询失败: {e}");
                        error!("  建议: 该查询使用了 MAX 聚合函数，请确认数据库支持此功能");
                        all_ok = false;
                        continue;
                    }
                };

                // Get MAX(no_o_id)
                let max_no = self
                    .cursor
                    .execute(
                        "SELECT MAX(no_o_id) FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                        &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
                    )
                    .await;

                let max_no_o_id: i64 = match max_no {
                    Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
                    Ok(_) => {
                        warn!("  NewOrders MAX(no_o_id) {w_id}-{d_id} 未找到");
                        all_ok = false;
                        continue;
                    }
                    Err(e) => {
                        error!("  NewOrders MAX(no_o_id) {w_id}-{d_id} 查询失败: {e}");
                        all_ok = false;
                        continue;
                    }
                };

                let consistent =
                    d_next_o_id - 1 == max_o_id && d_next_o_id - 1 == max_no_o_id;

                if !consistent {
                    error!(
                        "  FAIL: warehouse={w_id}, district={d_id}: d_next_o_id={d_next_o_id}, MAX(o_id)={max_o_id}, MAX(no_o_id)={max_no_o_id}"
                    );
                    error!(
                        "  建议: d_next_o_id 应等于 MAX(o_id)+1，请检查 NewOrder 事务中 UPDATE district 的实现"
                    );
                    all_ok = false;
                }
            }
        }

        if all_ok {
            info!("  District-Order 一致性检查 PASS");
        }
        Ok(all_ok)
    }

    async fn check_new_orders_consistency(&mut self) -> Result<bool, TpccError> {
        info!("[检查 3/4] NewOrder 连续性 (COUNT == MAX - MIN + 1)");

        let mut all_ok = true;

        for w_id in 1..=self.scale_factor {
            for d_id in 1..=10 {
                // COUNT(no_o_id)
                let count_result = self
                    .cursor
                    .execute(
                        "SELECT COUNT(no_o_id) FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                        &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
                    )
                    .await;

                let count: i64 = match count_result {
                    Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
                    _ => {
                        all_ok = false;
                        continue;
                    }
                };

                // MAX(no_o_id)
                let max_result = self
                    .cursor
                    .execute(
                        "SELECT MAX(no_o_id) FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                        &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
                    )
                    .await;

                let max_no: i64 = match max_result {
                    Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
                    _ => {
                        all_ok = false;
                        continue;
                    }
                };

                // MIN(no_o_id)
                let min_result = self
                    .cursor
                    .execute(
                        "SELECT MIN(no_o_id) FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                        &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
                    )
                    .await;

                let min_no: i64 = match min_result {
                    Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
                    _ => {
                        all_ok = false;
                        continue;
                    }
                };

                let expected = max_no - min_no + 1;
                if count != expected {
                    error!(
                        "  FAIL: warehouse={w_id}, district={d_id}: COUNT={count}, MAX={max_no}, MIN={min_no}, expected={expected}"
                    );
                    error!(
                        "  建议: new_orders 中存在空洞 (缺失 {} 条记录)，请检查 DELETE 和 INSERT 的并发控制",
                        expected - count
                    );
                    all_ok = false;
                }
            }
        }

        if all_ok {
            info!("  NewOrder 连续性检查 PASS");
        }
        Ok(all_ok)
    }

    async fn check_order_line_consistency(&mut self) -> Result<bool, TpccError> {
        info!("[检查 4/4] OrderLine 一致性 (SUM(o_ol_cnt) == COUNT(ol_o_id))");

        let mut all_ok = true;

        for w_id in 1..=self.scale_factor {
            for d_id in 1..=10 {
                // SUM(o_ol_cnt)
                let sum_result = self
                    .cursor
                    .execute(
                        "SELECT SUM(o_ol_cnt) FROM orders WHERE o_w_id = ? AND o_d_id = ?",
                        &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
                    )
                    .await;

                let sum_ol_cnt: i64 = match sum_result {
                    Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
                    Ok(_) => {
                        all_ok = false;
                        continue;
                    }
                    Err(e) => {
                        error!("  SUM(o_ol_cnt) 查询失败 ({w_id}-{d_id}): {e}");
                        error!("  建议: 该查询使用了 SUM 聚合函数，请确认数据库支持此功能");
                        all_ok = false;
                        continue;
                    }
                };

                // COUNT(ol_o_id)
                let count_result = self
                    .cursor
                    .execute(
                        "SELECT COUNT(ol_o_id) FROM order_line WHERE ol_w_id = ? AND ol_d_id = ?",
                        &[SqlParam::Int(w_id as i64), SqlParam::Int(d_id as i64)],
                    )
                    .await;

                let count_ol: i64 = match count_result {
                    Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
                    _ => {
                        all_ok = false;
                        continue;
                    }
                };

                if sum_ol_cnt != count_ol {
                    error!(
                        "  FAIL: warehouse={w_id}, district={d_id}: SUM(o_ol_cnt)={sum_ol_cnt}, COUNT(ol_o_id)={count_ol}"
                    );
                    error!(
                        "  建议: SUM(o_ol_cnt) != COUNT(order_line)，请检查 INSERT order_line 的实现"
                    );
                    all_ok = false;
                }
            }
        }

        if all_ok {
            info!("  OrderLine 一致性检查 PASS");
        }
        Ok(all_ok)
    }
}
