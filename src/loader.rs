use std::fs::{create_dir_all, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::data_gen::*;
use crate::error::TpccError;

const TABLE_DATA_DIR_FROM_TESTER: &str = "../src/test/performance_test/table_data";
const TABLE_DATA_DIR_FROM_DATABASE: &str = "../src/test/performance_test/table_data";

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
        let sql = std::fs::read_to_string("sql/create_tables.sql").map_err(|e| TpccError::Io(e))?;

        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        for (i, stmt) in stmts.iter().enumerate() {
            let stmt = stmt.trim();
            debug!(
                "[建表] 执行语句 {}: {}...",
                i + 1,
                &stmt[..stmt.len().min(60)]
            );
            match self.cursor.execute_update(stmt, &[]).await {
                Ok(_) => {}
                Err(TpccError::Abort(msg))
                    if msg.contains("already exists") || msg.contains("table already exists") =>
                {
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
        let sql = std::fs::read_to_string("sql/create_index.sql").map_err(|e| TpccError::Io(e))?;

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
        info!(
            "[数据加载] 开始生成 CSV 并通过 load 导入 TPC-C 数据 (scale_factor={})",
            self.scale_factor
        );
        let gen = TpccDataGen::new(self.scale_factor);
        let csv_dir = Path::new(TABLE_DATA_DIR_FROM_TESTER);
        // RMDB switches its working directory to the database directory after open_db().
        let load_dir = TABLE_DATA_DIR_FROM_DATABASE;
        create_dir_all(csv_dir)?;

        self.write_and_load_table(
            csv_dir,
            load_dir,
            "warehouse",
            &[
                "w_id",
                "w_name",
                "w_street_1",
                "w_street_2",
                "w_city",
                "w_state",
                "w_zip",
                "w_tax",
                "w_ytd",
            ],
            gen.generate_warehouses()
                .into_iter()
                .map(|w| w.to_sql_params()),
        )
        .await?;
        self.write_and_load_table(
            csv_dir,
            load_dir,
            "district",
            &[
                "d_id",
                "d_w_id",
                "d_name",
                "d_street_1",
                "d_street_2",
                "d_city",
                "d_state",
                "d_zip",
                "d_tax",
                "d_ytd",
                "d_next_o_id",
            ],
            gen.generate_districts()
                .into_iter()
                .map(|d| d.to_sql_params()),
        )
        .await?;
        self.write_and_load_table(
            csv_dir,
            load_dir,
            "item",
            &["i_id", "i_im_id", "i_name", "i_price", "i_data"],
            gen.generate_items().into_iter().map(|i| i.to_sql_params()),
        )
        .await?;
        self.write_and_load_table(
            csv_dir,
            load_dir,
            "customer",
            &[
                "c_id",
                "c_d_id",
                "c_w_id",
                "c_first",
                "c_middle",
                "c_last",
                "c_street_1",
                "c_street_2",
                "c_city",
                "c_state",
                "c_zip",
                "c_phone",
                "c_since",
                "c_credit",
                "c_credit_lim",
                "c_discount",
                "c_balance",
                "c_ytd_payment",
                "c_payment_cnt",
                "c_delivery_cnt",
                "c_data",
            ],
            gen.generate_customers()
                .into_iter()
                .map(|c| c.to_sql_params()),
        )
        .await?;
        self.write_and_load_table(
            csv_dir,
            load_dir,
            "stock",
            &[
                "s_i_id",
                "s_w_id",
                "s_quantity",
                "s_dist_01",
                "s_dist_02",
                "s_dist_03",
                "s_dist_04",
                "s_dist_05",
                "s_dist_06",
                "s_dist_07",
                "s_dist_08",
                "s_dist_09",
                "s_dist_10",
                "s_ytd",
                "s_order_cnt",
                "s_remote_cnt",
                "s_data",
            ],
            gen.generate_stock().into_iter().map(|s| s.to_sql_params()),
        )
        .await?;
        self.write_and_load_table(
            csv_dir,
            load_dir,
            "orders",
            &[
                "o_id",
                "o_d_id",
                "o_w_id",
                "o_c_id",
                "o_entry_d",
                "o_carrier_id",
                "o_ol_cnt",
                "o_all_local",
            ],
            gen.generate_orders().into_iter().map(|o| o.to_sql_params()),
        )
        .await?;
        self.write_and_load_table(
            csv_dir,
            load_dir,
            "new_orders",
            &["no_o_id", "no_d_id", "no_w_id"],
            gen.generate_new_orders()
                .into_iter()
                .map(|n| n.to_sql_params()),
        )
        .await?;
        self.write_and_load_table(
            csv_dir,
            load_dir,
            "history",
            &[
                "h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount",
                "h_data",
            ],
            gen.generate_history()
                .into_iter()
                .map(|h| h.to_sql_params()),
        )
        .await?;
        self.write_and_load_table(
            csv_dir,
            load_dir,
            "order_line",
            &[
                "ol_o_id",
                "ol_d_id",
                "ol_w_id",
                "ol_number",
                "ol_i_id",
                "ol_supply_w_id",
                "ol_delivery_d",
                "ol_quantity",
                "ol_amount",
                "ol_dist_info",
            ],
            gen.generate_order_lines()
                .into_iter()
                .map(|ol| ol.to_sql_params()),
        )
        .await?;

        info!("[数据加载] 全部数据加载完成");
        self.verify_counts().await?;
        Ok(())
    }

    async fn write_and_load_table<I>(
        &mut self,
        csv_dir: &Path,
        load_dir: &str,
        table_name: &str,
        columns: &[&str],
        rows: I,
    ) -> Result<(), TpccError>
    where
        I: IntoIterator<Item = Vec<SqlParam>>,
    {
        let start = Instant::now();
        let csv_file = csv_dir.join(format!("{table_name}.csv"));
        let file = File::create(&csv_file)?;
        let mut writer = BufWriter::new(file);

        writeln!(writer, "{}", columns.join(","))?;
        let mut total = 0usize;
        for row in rows {
            Self::write_csv_row(&mut writer, &row)?;
            total += 1;
            if total >= 10000 && total % 10000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                info!("[CSV] {table_name}: 已生成 {total} 行 ({elapsed:.1}s)");
            }
        }
        writer.flush()?;

        let load_path = format!("{load_dir}/{table_name}.csv");

        info!("[表加载] {table_name}: 通过 load 导入 {total} 行 -> {load_path}");
        self.cursor
            .execute_update(&format!("load {load_path} into {table_name}"), &[])
            .await?;
        let elapsed = start.elapsed().as_secs_f64();
        info!("[表加载] {table_name}: load 完成 ({elapsed:.2}s)");

        Ok(())
    }

    fn write_csv_row(writer: &mut BufWriter<File>, row: &[SqlParam]) -> Result<(), TpccError> {
        for (idx, param) in row.iter().enumerate() {
            if idx > 0 {
                write!(writer, ",")?;
            }
            write!(writer, "{}", Self::csv_value(param))?;
        }
        writeln!(writer)?;
        Ok(())
    }

    fn csv_value(param: &SqlParam) -> String {
        match param {
            SqlParam::Int(v) => v.to_string(),
            SqlParam::Float(v) => format!("{v}"),
            SqlParam::Str(v) => {
                if v.contains(',') || v.contains('"') || v.contains('\n') || v.contains('\r') {
                    format!("\"{}\"", v.replace('"', "\"\""))
                } else {
                    v.clone()
                }
            }
            SqlParam::Null => String::new(),
        }
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
