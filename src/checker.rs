use tracing::{error, info, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::{DISTRICTS_PER_WAREHOUSE, ORDERS_PER_DISTRICT};
use crate::error::TpccError;
use crate::runtime_schema::{LogicalTable, RuntimeSchema};

pub struct ConsistencyChecker<'a> {
    cursor: &'a mut RmdbCursor,
    schema: &'a RuntimeSchema,
    scale_factor: i32,
    expected_new_orders: Option<i64>,
}

impl<'a> ConsistencyChecker<'a> {
    pub fn new(
        cursor: &'a mut RmdbCursor,
        schema: &'a RuntimeSchema,
        scale_factor: i32,
        expected_new_orders: Option<i64>,
    ) -> Self {
        Self {
            cursor,
            schema,
            scale_factor,
            expected_new_orders,
        }
    }

    pub async fn run_all_checks(&mut self) -> Result<bool, TpccError> {
        info!("========================================");
        info!("       TPC-C 一致性检查");
        info!("========================================");

        let mut all_passed = true;

        // Rule 1: District, orders and new_orders maxima stay aligned.
        if !self.check_district_order_consistency().await? {
            all_passed = false;
        }

        // Rule 2: new_orders has no gaps per warehouse/district.
        if !self.check_new_orders_consistency().await? {
            all_passed = false;
        }

        // Rule 3: orders.o_ol_cnt matches order_line row count.
        if !self.check_order_line_consistency().await? {
            all_passed = false;
        }

        // Rule 4: orders count equals initial orders plus committed NewOrder count.
        if !self.check_orders_count().await? {
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

        for table in LogicalTable::ALL {
            let logical = table.canonical();
            let sql = format!("SELECT COUNT(*) FROM {}", self.schema.table(table));
            match self.cursor.execute(&sql, &[]).await {
                Ok(result) => {
                    if let Some(row) = result.rows.first() {
                        if let Some(val) = row.first() {
                            info!("  {logical:>15}: {val:>12}");
                        }
                    }
                }
                Err(e) => {
                    warn!("  {logical:>15}: 查询失败 - {e}");
                    warn!("  建议: 该查询使用了 COUNT 聚合函数，请确认数据库支持此功能");
                }
            }
        }

        info!("========================================");
        Ok(())
    }

    async fn check_district_order_consistency(&mut self) -> Result<bool, TpccError> {
        info!("[检查 1/4] District-Order 一致性 (d_next_o_id - 1 == MAX(o_id) == MAX(no_o_id))");

        let mut all_ok = true;

        for w_id in 1..=self.scale_factor {
            for d_id in 1..=10 {
                // Get d_next_o_id
                let sql = self
                    .schema
                    .render_sql("SELECT d_next_o_id FROM district WHERE d_w_id = ? AND d_id = ?");
                let d_result = self
                    .cursor
                    .execute(
                        &sql,
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
                let sql = self
                    .schema
                    .render_sql("SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ?");
                let max_o = self
                    .cursor
                    .execute(
                        &sql,
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
                let sql = self.schema.render_sql(
                    "SELECT MAX(no_o_id) FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                );
                let max_no = self
                    .cursor
                    .execute(
                        &sql,
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

                let consistent = d_next_o_id - 1 == max_o_id && d_next_o_id - 1 == max_no_o_id;

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
        info!("[检查 2/4] NewOrder 连续性 (COUNT == MAX - MIN + 1)");

        let mut all_ok = true;

        for w_id in 1..=self.scale_factor {
            for d_id in 1..=10 {
                // COUNT(no_o_id)
                let sql = self.schema.render_sql(
                    "SELECT COUNT(no_o_id) FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                );
                let count_result = self
                    .cursor
                    .execute(
                        &sql,
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
                let sql = self.schema.render_sql(
                    "SELECT MAX(no_o_id) FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                );
                let max_result = self
                    .cursor
                    .execute(
                        &sql,
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
                let sql = self.schema.render_sql(
                    "SELECT MIN(no_o_id) FROM new_orders WHERE no_w_id = ? AND no_d_id = ?",
                );
                let min_result = self
                    .cursor
                    .execute(
                        &sql,
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
        info!("[检查 3/4] OrderLine 一致性 (SUM(o_ol_cnt) == COUNT(ol_o_id))");

        let mut all_ok = true;

        for w_id in 1..=self.scale_factor {
            for d_id in 1..=10 {
                // SUM(o_ol_cnt)
                let sql = self
                    .schema
                    .render_sql("SELECT SUM(o_ol_cnt) FROM orders WHERE o_w_id = ? AND o_d_id = ?");
                let sum_result = self
                    .cursor
                    .execute(
                        &sql,
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
                let sql = self.schema.render_sql(
                    "SELECT COUNT(ol_o_id) FROM order_line WHERE ol_w_id = ? AND ol_d_id = ?",
                );
                let count_result = self
                    .cursor
                    .execute(
                        &sql,
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

    async fn check_orders_count(&mut self) -> Result<bool, TpccError> {
        info!("[检查 4/4] Orders 总数 (COUNT(*) == 初始订单数 + NewOrder 成功数)");

        let Some(committed_new_orders) = self.expected_new_orders else {
            warn!("  未提供 --expected-new-orders，跳过 Orders 总数检查");
            return Ok(true);
        };

        let sql = self.schema.render_sql("SELECT COUNT(*) FROM orders");
        let result = self.cursor.execute(&sql, &[]).await;
        let count_orders: i64 = match result {
            Ok(r) if !r.is_empty() => r.rows[0][0].parse().unwrap_or(0),
            Ok(_) => {
                error!("  Orders COUNT(*) 查询结果为空");
                return Ok(false);
            }
            Err(e) => {
                error!("  Orders COUNT(*) 查询失败: {e}");
                return Ok(false);
            }
        };

        let initial_orders =
            self.scale_factor as i64 * DISTRICTS_PER_WAREHOUSE as i64 * ORDERS_PER_DISTRICT as i64;
        let expected = initial_orders + committed_new_orders;
        if count_orders == expected {
            info!("  Orders 总数检查 PASS: {count_orders}/{expected}");
            return Ok(true);
        }

        error!(
            "  FAIL: COUNT(orders)={count_orders}, initial_orders={initial_orders}, committed_new_orders={committed_new_orders}, expected={expected}"
        );
        error!(
            "  建议: 检查 NewOrder 是否完整插入 orders，或 benchmark 成功数统计是否与数据状态一致"
        );
        Ok(false)
    }
}
