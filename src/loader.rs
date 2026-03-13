use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::*;
use crate::error::TpccError;

pub struct Loader<'a> {
    cursor: &'a mut RmdbCursor,
    scale_factor: i32,
}

impl<'a> Loader<'a> {
    pub fn new(cursor: &'a mut RmdbCursor, scale_factor: i32) -> Self {
        Self {
            cursor,
            scale_factor,
        }
    }

    pub async fn create_tables(&mut self) -> Result<(), TpccError> {
        info!("[建表] 读取 sql/create_tables.sql ...");
        let sql = std::fs::read_to_string("sql/create_tables.sql")
            .map_err(|e| TpccError::Io(e))?;

        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        for (i, stmt) in stmts.iter().enumerate() {
            let stmt = stmt.trim();
            debug!("[建表] 执行语句 {}: {}...", i + 1, &stmt[..stmt.len().min(60)]);
            match self.cursor.execute_update(stmt, &[]).await {
                Ok(_) => {}
                Err(TpccError::Abort(msg)) if msg.contains("already exists") || msg.contains("table already exists") => {
                    warn!("[建表] 表已存在，如需重新初始化请先手动删除: {msg}");
                }
                Err(e) => {
                    error!("[建表] 语句 {} 执行失败: {e}", i + 1);
                    error!("[建表] 完整 SQL: {stmt}");
                    return Err(e);
                }
            }
        }
        info!("[建表] 全部建表语句执行完成");
        Ok(())
    }

    pub async fn create_indexes(&mut self) -> Result<(), TpccError> {
        info!("[建索引] 读取 sql/create_index.sql ...");
        let sql = std::fs::read_to_string("sql/create_index.sql")
            .map_err(|e| TpccError::Io(e))?;

        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        for (i, stmt) in stmts.iter().enumerate() {
            let stmt = stmt.trim();
            debug!("[建索引] 执行语句 {}: {stmt}", i + 1);
            match self.cursor.execute_update(stmt, &[]).await {
                Ok(_) => {}
                Err(e) => {
                    warn!("[建索引] 语句 {} 执行失败 (继续): {e}", i + 1);
                }
            }
        }
        info!("[建索引] 全部建索引语句执行完成");
        Ok(())
    }

    pub async fn load_all_data(&mut self) -> Result<(), TpccError> {
        info!("[数据加载] 开始加载 TPC-C 数据 (scale_factor={})", self.scale_factor);
        let gen = TpccDataGen::new(self.scale_factor);

        // Load in correct order
        self.load_table("warehouse", &gen.generate_warehouses().iter().map(|w| w.to_sql_params()).collect::<Vec<_>>(), 9).await?;
        self.load_table("district", &gen.generate_districts().iter().map(|d| d.to_sql_params()).collect::<Vec<_>>(), 11).await?;
        self.load_table("item", &gen.generate_items().iter().map(|i| i.to_sql_params()).collect::<Vec<_>>(), 5).await?;
        self.load_table("customer", &gen.generate_customers().iter().map(|c| c.to_sql_params()).collect::<Vec<_>>(), 21).await?;
        self.load_table("stock", &gen.generate_stock().iter().map(|s| s.to_sql_params()).collect::<Vec<_>>(), 17).await?;
        self.load_table("orders", &gen.generate_orders().iter().map(|o| o.to_sql_params()).collect::<Vec<_>>(), 8).await?;
        self.load_table("new_orders", &gen.generate_new_orders().iter().map(|n| n.to_sql_params()).collect::<Vec<_>>(), 3).await?;
        self.load_table("history", &gen.generate_history().iter().map(|h| h.to_sql_params()).collect::<Vec<_>>(), 8).await?;
        self.load_table("order_line", &gen.generate_order_lines().iter().map(|ol| ol.to_sql_params()).collect::<Vec<_>>(), 10).await?;

        info!("[数据加载] 全部数据加载完成");
        self.verify_counts().await?;
        Ok(())
    }

    async fn load_table(
        &mut self,
        table_name: &str,
        rows: &[Vec<SqlParam>],
        col_count: usize,
    ) -> Result<(), TpccError> {
        let total = rows.len();
        let start = Instant::now();
        info!("[表加载] {table_name}: 开始加载 {total} 行...");

        let placeholders = (0..col_count).map(|_| "?").collect::<Vec<_>>().join(", ");
        let insert_sql = format!("INSERT INTO {table_name} VALUES ({placeholders})");

        let mut loaded = 0usize;
        let mut failed = 0usize;

        for (i, params) in rows.iter().enumerate() {
            match self.cursor.execute_update(&insert_sql, params).await {
                Ok(_) => {
                    loaded += 1;
                }
                Err(e) => {
                    failed += 1;
                    if failed <= 5 {
                        warn!(
                            "[表加载] {table_name} 第 {} 行 INSERT 失败: {e}",
                            i + 1
                        );
                    }
                }
            }

            // Progress reporting for large tables
            let done = i + 1;
            if total >= 10000 && done % 10000 == 0 {
                let pct = (done as f64 / total as f64) * 100.0;
                let elapsed = start.elapsed().as_secs_f64();
                info!(
                    "[表加载] {table_name}: {done}/{total} ({pct:.1}%) - {elapsed:.1}s"
                );
            }
        }

        let elapsed = start.elapsed().as_secs_f64();
        if failed > 0 {
            warn!(
                "[表加载] {table_name}: {loaded}/{total} 行已加载, {failed} 行失败 ({elapsed:.2}s)"
            );
        } else {
            info!(
                "[表加载] {table_name}: {loaded}/{total} 行已加载 ({elapsed:.2}s)"
            );
        }

        Ok(())
    }

    async fn verify_counts(&mut self) -> Result<(), TpccError> {
        info!("[数据验证] 检查各表行数...");
        let sf = self.scale_factor as i64;
        let expected: Vec<(&str, i64)> = vec![
            ("warehouse", sf),
            ("district", sf * 10),
            ("item", 100_000),
            ("customer", sf * 10 * 3000),
            ("stock", sf * 100_000),
            ("orders", sf * 10 * 3000),
            ("new_orders", sf * 10 * 900),
            ("history", sf * 10 * 3000),
            ("order_line", sf * 10 * 3000 * 10),
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
                                debug!("[数据验证] {table}: {actual}/{exp} OK");
                            } else {
                                warn!(
                                    "[数据验证] {table}: 实际 {actual} / 期望 {exp} - 差异 {}",
                                    actual - exp
                                );
                                all_ok = false;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("[数据验证] {table} COUNT 查询失败: {e}");
                    all_ok = false;
                }
            }
        }

        if all_ok {
            info!("[数据验证] 所有表行数正确");
        } else {
            warn!("[数据验证] 部分表行数不匹配，请检查加载过程中的错误");
        }
        Ok(())
    }
}
