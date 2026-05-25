use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;

use tracing::{debug, error, info, warn};

use crate::connection::cursor::RmdbCursor;
use crate::data_gen::*;
use crate::error::TpccError;
use crate::model::ToCsvRow;

pub struct Loader<'a> {
    cursor: &'a mut RmdbCursor,
    scale_factor: i32,
    csv_path: String,
}

impl<'a> Loader<'a> {
    pub fn new(cursor: &'a mut RmdbCursor, scale_factor: i32, csv_path: String) -> Self {
        Self {
            cursor,
            scale_factor,
            csv_path,
        }
    }

    pub async fn create_tables(&mut self) -> Result<(), TpccError> {
        info!("[建表] 读取 sql/create_tables.sql ...");
        let sql = fs::read_to_string("sql/create_tables.sql")
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
        let sql = fs::read_to_string("sql/create_index.sql")
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
        let csv_dir = &self.csv_path;
        let local_dir = Path::new(csv_dir);

        fs::create_dir_all(local_dir)
            .map_err(|e| TpccError::Io(e))?;

        info!("[数据加载] 生成 CSV 到 {csv_dir}, 然后 LOAD 导入 (scale_factor={})", self.scale_factor);
        let gen = TpccDataGen::new(self.scale_factor);
        let start = Instant::now();

        self.write_csv_and_load("warehouse", &gen.generate_warehouses()).await?;
        self.write_csv_and_load("district", &gen.generate_districts()).await?;
        self.write_csv_and_load("item", &gen.generate_items()).await?;
        self.write_csv_and_load("customer", &gen.generate_customers()).await?;
        self.write_csv_and_load("stock", &gen.generate_stock()).await?;
        self.write_csv_and_load("orders", &gen.generate_orders()).await?;
        self.write_csv_and_load("new_orders", &gen.generate_new_orders()).await?;
        self.write_csv_and_load("history", &gen.generate_history()).await?;
        self.write_csv_and_load("order_line", &gen.generate_order_lines()).await?;

        info!("[数据加载] 全部加载完成 ({:.2}s)", start.elapsed().as_secs_f64());
        self.verify_counts().await?;
        Ok(())
    }

    async fn write_csv_and_load<T: ToCsvRow>(
        &mut self,
        table_name: &str,
        rows: &[T],
    ) -> Result<(), TpccError> {
        let csv_path = Path::new(&self.csv_path).join(format!("{table_name}.csv"));
        let csv_file = csv_path.to_string_lossy();
        let start = Instant::now();

        info!("[CSV] {table_name}: 生成 {} 行...", rows.len());

        let file = File::create(&csv_path)
            .map_err(|e| TpccError::Io(e))?;
        let mut writer = BufWriter::with_capacity(1024 * 1024, file);
        let mut buf = String::with_capacity(512);

        for row in rows {
            buf.clear();
            row.to_csv_row(&mut buf);
            writer.write_all(buf.as_bytes())
                .map_err(|e| TpccError::Io(e))?;
        }
        writer.flush().map_err(|e| TpccError::Io(e))?;

        let gen_time = start.elapsed().as_secs_f64();
        info!("[CSV] {table_name}: CSV 写入完成 ({gen_time:.2}s), 发送 LOAD 命令...");

        let load_sql = format!("load {csv_file} into {table_name}");
        match self.cursor.execute_update(&load_sql, &[]).await {
            Ok(_) => {
                let total_time = start.elapsed().as_secs_f64();
                info!("[CSV] {table_name}: LOAD 完成 (总计 {total_time:.2}s)");
            }
            Err(e) => {
                error!("[CSV] {table_name}: LOAD 失败: {e}");
                error!("[CSV] 命令: {load_sql}");
                return Err(e);
            }
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
