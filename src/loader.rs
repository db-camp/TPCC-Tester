use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::{create_dir_all, rename, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{debug, error, info};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::consistency::NonNegativeF32Accumulator;
use crate::data_gen::*;
use crate::error::TpccError;
use crate::runtime_schema::{LogicalIndex, LogicalTable, RuntimeSchema};

pub struct Loader<'a> {
    cursor: &'a mut RmdbCursor,
    scale_factor: i32,
    schema: &'a RuntimeSchema,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionLoadSummary {
    pub warehouse_id: i32,
    pub district_id: i32,
    pub order_line_rows: i64,
    pub undelivered_order_line_rows: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LoadSummary {
    pub order_line_rows: i64,
    pub undelivered_order_line_rows: i64,
    pub order_line_amounts: NonNegativeF32Accumulator,
    pub partitions: Vec<PartitionLoadSummary>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CsvAsset {
    pub table: LogicalTable,
    pub host_path: PathBuf,
    pub load_path: String,
    pub row_count: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedLoad {
    scale_factor: i32,
    schema_fingerprint: u64,
    assets: BTreeMap<LogicalTable, CsvAsset>,
    summary: LoadSummary,
}

impl MaterializedLoad {
    pub fn asset(&self, table: LogicalTable) -> Result<&CsvAsset, TpccError> {
        self.assets.get(&table).ok_or_else(|| {
            TpccError::Protocol(format!(
                "materialized load is missing logical table {}",
                table.canonical()
            ))
        })
    }

    pub fn assets(&self) -> impl Iterator<Item = &CsvAsset> {
        self.assets.values()
    }
}

impl<'a> Loader<'a> {
    pub fn new(cursor: &'a mut RmdbCursor, scale_factor: i32, schema: &'a RuntimeSchema) -> Self {
        Self {
            cursor,
            scale_factor,
            schema,
        }
    }

    pub async fn create_tables(&mut self) -> Result<(), TpccError> {
        info!("[建表] 读取 sql/create_tables.sql ...");
        let sql = std::fs::read_to_string("sql/create_tables.sql").map_err(|e| TpccError::Io(e))?;

        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        if stmts.len() != LogicalTable::ALL.len() {
            return Err(TpccError::Protocol(format!(
                "logical DDL has {} CREATE TABLE statements, expected {}",
                stmts.len(),
                LogicalTable::ALL.len()
            )));
        }
        for (i, table) in self.schema.schedule().create_tables().iter().enumerate() {
            let logical_ordinal = LogicalTable::ALL
                .iter()
                .position(|candidate| candidate == table)
                .expect("logical table disappeared from the complete schedule");
            let stmt = self.schema.render_sql(stmts[logical_ordinal].trim());
            debug!(
                "[建表] 执行语句 {}: {}...",
                i + 1,
                &stmt[..stmt.len().min(60)]
            );
            if let Err(error) = self.cursor.execute_update(&stmt, &[]).await {
                error!("[建表] 语句 {} 执行失败: {error}", i + 1);
                return Err(error);
            }
        }
        info!("[建表] 全部建表语句执行完成");
        Ok(())
    }

    pub async fn create_indexes(&mut self) -> Result<(), TpccError> {
        info!("[建索引] 读取 sql/create_index.sql ...");
        let sql = std::fs::read_to_string("sql/create_index.sql").map_err(|e| TpccError::Io(e))?;

        let stmts: Vec<&str> = sql.split(';').filter(|s| !s.trim().is_empty()).collect();
        if stmts.len() != LogicalIndex::ALL.len() {
            return Err(TpccError::Protocol(format!(
                "logical DDL has {} CREATE INDEX statements, expected {}",
                stmts.len(),
                LogicalIndex::ALL.len()
            )));
        }
        for (i, index) in self.schema.schedule().create_indexes().iter().enumerate() {
            let stmt = self
                .schema
                .render_sql(stmts[usize::from(index.ordinal())].trim());
            debug!("[建索引] 执行语句 {}: {stmt}", i + 1);
            match self.cursor.execute_update(&stmt, &[]).await {
                Ok(_) => {}
                Err(e) => {
                    error!("[建索引] 语句 {} 执行失败: {e}", i + 1);
                    return Err(e);
                }
            }
        }
        info!("[建索引] 全部建索引语句执行完成");
        Ok(())
    }

    pub async fn load_materialized(
        &mut self,
        materialized: MaterializedLoad,
    ) -> Result<LoadSummary, TpccError> {
        if materialized.scale_factor != self.scale_factor
            || materialized.schema_fingerprint != self.schema.fingerprint()
        {
            return Err(TpccError::Protocol(
                "materialized CSV assets do not match this setup runtime".to_owned(),
            ));
        }
        for (ordinal, table) in self.schema.schedule().load_tables().iter().enumerate() {
            let asset = materialized.asset(*table)?;
            info!(
                "[表加载] ordinal={}: 通过 load 导入 {} 行",
                ordinal + 1,
                asset.row_count
            );
            let sql = format!(
                "load {} into {}",
                asset.load_path,
                self.schema.table(*table)
            );
            self.cursor.execute_update(&sql, &[]).await?;
        }
        self.verify_counts(&materialized).await?;
        Ok(materialized.summary)
    }
}

pub struct CsvMaterializer<'a> {
    scale_factor: i32,
    schema: &'a RuntimeSchema,
    csv_dir: PathBuf,
    load_dir: String,
    assets: BTreeMap<LogicalTable, CsvAsset>,
}

impl<'a> CsvMaterializer<'a> {
    pub fn new(scale_factor: i32, schema: &'a RuntimeSchema) -> Result<Self, TpccError> {
        if scale_factor <= 0 {
            return Err(TpccError::Protocol(
                "CSV materialization scale factor must be positive".to_owned(),
            ));
        }
        let default_csv_dir = std::env::temp_dir().join(format!(
            "rmdb-tpcc-sf{}-seed{}-pid{}",
            scale_factor,
            schema.seed(),
            std::process::id()
        ));
        let csv_dir = std::env::var("RMDB_TPCC_CSV_DIR")
            .map(PathBuf::from)
            .unwrap_or(default_csv_dir);
        let load_dir = std::env::var("RMDB_TPCC_LOAD_DIR")
            .unwrap_or_else(|_| csv_dir.to_string_lossy().into_owned());
        Ok(Self {
            scale_factor,
            schema,
            csv_dir,
            load_dir,
            assets: BTreeMap::new(),
        })
    }

    pub fn materialize(mut self) -> Result<MaterializedLoad, TpccError> {
        info!(
            "[数据物化] 在连接数据库前生成全部 TPC-C CSV (scale_factor={})",
            self.scale_factor
        );
        let gen = TpccDataGen::new(self.scale_factor);
        if gen.load_seed() != self.schema.seed() {
            return Err(TpccError::Protocol(format!(
                "data seed {} does not match runtime schema seed {}",
                gen.load_seed(),
                self.schema.seed()
            )));
        }
        info!(
            "[数据物化] 使用公开可配置本地 seed={}、装载时间={} (非官方隐藏配置)",
            gen.load_seed(),
            gen.load_timestamp()
        );
        let csv_dir_path = self.csv_dir.clone();
        let load_dir_path = self.load_dir.clone();
        let csv_dir = csv_dir_path.as_path();
        let load_dir = load_dir_path.as_str();
        create_dir_all(csv_dir)?;

        self.write_table(
            csv_dir,
            load_dir,
            LogicalTable::Warehouse,
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
        )?;
        self.write_table(
            csv_dir,
            load_dir,
            LogicalTable::District,
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
        )?;
        self.write_table(
            csv_dir,
            load_dir,
            LogicalTable::Item,
            &["i_id", "i_im_id", "i_name", "i_price", "i_data"],
            gen.generate_items().into_iter().map(|i| i.to_sql_params()),
        )?;
        self.write_table(
            csv_dir,
            load_dir,
            LogicalTable::Customer,
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
        )?;
        self.write_table(
            csv_dir,
            load_dir,
            LogicalTable::Stock,
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
        )?;
        // Sum O_OL_CNT while the order CSV is already being streamed. This
        // avoids a second 1.5-million-order shape traversal at final SF=50.
        let partition_shapes = RefCell::new(
            (1..=self.scale_factor)
                .flat_map(|warehouse_id| {
                    (1..=10).map(move |district_id| PartitionLoadSummary {
                        warehouse_id,
                        district_id,
                        order_line_rows: 0,
                        undelivered_order_line_rows: 0,
                    })
                })
                .collect::<Vec<_>>(),
        );
        self.write_table(
            csv_dir,
            load_dir,
            LogicalTable::Orders,
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
            gen.generate_orders().into_iter().map(|o| {
                let index = ((o.o_w_id - 1) * 10 + (o.o_d_id - 1)) as usize;
                let mut shapes = partition_shapes.borrow_mut();
                let shape = &mut shapes[index];
                shape.order_line_rows += i64::from(o.o_ol_cnt);
                if o.o_carrier_id == 0 {
                    shape.undelivered_order_line_rows += i64::from(o.o_ol_cnt);
                }
                o.to_sql_params()
            }),
        )?;
        self.write_table(
            csv_dir,
            load_dir,
            LogicalTable::NewOrders,
            &["no_o_id", "no_d_id", "no_w_id"],
            gen.generate_new_orders()
                .into_iter()
                .map(|n| n.to_sql_params()),
        )?;
        self.write_table(
            csv_dir,
            load_dir,
            LogicalTable::History,
            &[
                "h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount",
                "h_data",
            ],
            gen.generate_history()
                .into_iter()
                .map(|h| h.to_sql_params()),
        )?;
        let mut order_line_amounts = NonNegativeF32Accumulator::default();
        let generated_order_line_count = self.write_table_observed(
            csv_dir,
            load_dir,
            LogicalTable::OrderLine,
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
            |row| {
                let amount = match row.get(8) {
                    Some(SqlParam::Float(amount)) => *amount as f32,
                    _ => {
                        return Err(TpccError::Protocol(
                            "generated order_line row lost its FLOAT ol_amount".to_owned(),
                        ));
                    }
                };
                order_line_amounts
                    .add_bits(amount.to_bits())
                    .map_err(|error| {
                        TpccError::Protocol(format!(
                            "initial order_line FLOAT accumulator failed: {error}"
                        ))
                    })
            },
        )?;
        let partitions = partition_shapes.into_inner();
        let expected_order_line_count = partitions
            .iter()
            .map(|partition| partition.order_line_rows)
            .sum::<i64>();
        if i64::try_from(generated_order_line_count).ok() != Some(expected_order_line_count) {
            return Err(TpccError::QueryError(format!(
                "order_line 生成计数不一致: generated={generated_order_line_count}, expected={expected_order_line_count}"
            )));
        }
        let undelivered_order_line_rows = partitions
            .iter()
            .map(|partition| partition.undelivered_order_line_rows)
            .sum();
        if self.assets.len() != LogicalTable::ALL.len()
            || order_line_amounts.term_count()
                != u64::try_from(expected_order_line_count).unwrap_or(u64::MAX)
        {
            return Err(TpccError::Protocol(
                "materialized CSV set is incomplete or lost order-line FLOAT evidence".to_owned(),
            ));
        }

        info!("[数据物化] 9 个 CSV 已全部完成");
        Ok(MaterializedLoad {
            scale_factor: self.scale_factor,
            schema_fingerprint: self.schema.fingerprint(),
            assets: self.assets,
            summary: LoadSummary {
                order_line_rows: expected_order_line_count,
                undelivered_order_line_rows,
                order_line_amounts,
                partitions,
            },
        })
    }

    fn write_table<I>(
        &mut self,
        csv_dir: &Path,
        load_dir: &str,
        table: LogicalTable,
        columns: &[&str],
        rows: I,
    ) -> Result<u64, TpccError>
    where
        I: IntoIterator<Item = Vec<SqlParam>>,
    {
        self.write_table_observed(csv_dir, load_dir, table, columns, rows, |_| Ok(()))
    }

    fn write_table_observed<I, F>(
        &mut self,
        csv_dir: &Path,
        load_dir: &str,
        table: LogicalTable,
        columns: &[&str],
        rows: I,
        mut observe: F,
    ) -> Result<u64, TpccError>
    where
        I: IntoIterator<Item = Vec<SqlParam>>,
        F: FnMut(&[SqlParam]) -> Result<(), TpccError>,
    {
        if columns != table.columns() {
            return Err(TpccError::Protocol(format!(
                "CSV column order for {} does not match logical DDL",
                table.canonical()
            )));
        }
        let start = Instant::now();
        let basename = self.schema.csv_basename(table);
        let csv_file = csv_dir.join(basename);
        if csv_file.exists() {
            return Err(TpccError::Io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("refusing to overwrite sealed CSV {}", csv_file.display()),
            )));
        }
        let partial_file = csv_dir.join(format!(".{basename}.part"));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&partial_file)?;
        let mut writer = BufWriter::new(file);

        let runtime_columns = self
            .schema
            .columns(table)
            .map_err(|error| TpccError::Protocol(error.to_string()))?;
        writeln!(writer, "{}", runtime_columns.join(","))?;
        let mut total = 0_u64;
        for row in rows {
            if row.len() != columns.len() {
                return Err(TpccError::Protocol(format!(
                    "generated {} row has {} fields, expected {}",
                    table.canonical(),
                    row.len(),
                    columns.len()
                )));
            }
            observe(&row)?;
            Self::write_csv_row(&mut writer, &row)?;
            total += 1;
            if total >= 10000 && total % 10000 == 0 {
                let elapsed = start.elapsed().as_secs_f64();
                info!(
                    "[CSV] {}: 已生成 {total} 行 ({elapsed:.1}s)",
                    table.canonical()
                );
            }
        }
        writer.flush()?;
        drop(writer);
        rename(&partial_file, &csv_file)?;
        let load_path = format!("{load_dir}/{basename}");
        let row_count = i64::try_from(total).map_err(|_| {
            TpccError::Protocol(format!("{} CSV row count overflow", table.canonical()))
        })?;
        let asset = CsvAsset {
            table,
            host_path: csv_file,
            load_path,
            row_count,
        };
        if self.assets.insert(table, asset).is_some() {
            return Err(TpccError::Protocol(format!(
                "duplicate CSV asset for {}",
                table.canonical()
            )));
        }
        let elapsed = start.elapsed().as_secs_f64();
        info!("[CSV] {}: 物化完成 ({elapsed:.2}s)", table.canonical());

        Ok(total)
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
            // SQL FLOAT is binary32. Formatting the narrowed value gives a shortest
            // decimal representation that round-trips to the exact generated bits.
            SqlParam::Float(v) => format!("{}", *v as f32),
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
}

impl Loader<'_> {
    async fn verify_counts(&mut self, materialized: &MaterializedLoad) -> Result<(), TpccError> {
        info!("[数据验证] 检查各表行数...");
        let mut all_ok = true;
        for table in self.schema.schedule().count_tables() {
            let expected = materialized.asset(*table)?.row_count;
            let logical = table.canonical();
            let sql = format!("SELECT COUNT(*) FROM {}", self.schema.table(*table));
            match self.cursor.execute(&sql, &[]).await {
                Ok(result) => match result.rows.first().and_then(|row| row.first()) {
                    Some(value) => match value.parse::<i64>() {
                        Ok(actual) if actual == expected => {
                            debug!("[数据验证] {logical}: {actual}/{expected} OK");
                        }
                        Ok(actual) => {
                            error!(
                                "[数据验证] {logical}: 实际 {actual} / 期望 {expected} - 差异 {}",
                                actual - expected
                            );
                            all_ok = false;
                        }
                        Err(e) => {
                            error!("[数据验证] {logical}: COUNT 结果无法解析 ({value}): {e}");
                            all_ok = false;
                        }
                    },
                    None => {
                        error!("[数据验证] {logical}: COUNT 查询没有返回值");
                        all_ok = false;
                    }
                },
                Err(e) => {
                    error!("[数据验证] {logical} COUNT 查询失败: {e}");
                    all_ok = false;
                }
            }
        }

        if all_ok {
            info!("[数据验证] 所有表行数正确");
            Ok(())
        } else {
            Err(TpccError::QueryError(
                "初始数据精确行数校验失败".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn float_csv_serialization_round_trips_binary32_bits() {
        for cents in [1, 2, 3, 99, 100, 101, 16_777, 500_001, 999_998, 999_999] {
            let value = cents as f32 / 100.0_f32;
            let encoded = CsvMaterializer::csv_value(&SqlParam::Float(f64::from(value)));
            let decoded: f32 = encoded.parse().unwrap();
            assert_eq!(decoded.to_bits(), value.to_bits(), "cents={cents}");
        }
    }

    #[test]
    fn final_2026_schema_has_ten_logical_indexes() {
        let statements: Vec<_> = include_str!("../sql/create_index.sql")
            .split(';')
            .filter(|statement| !statement.trim().is_empty())
            .collect();
        assert_eq!(statements.len(), 10);
        assert!(statements
            .iter()
            .any(|sql| sql.contains("customer(c_w_id, c_d_id, c_last, c_id)")));
        assert!(statements
            .iter()
            .any(|sql| sql.contains("orders(o_w_id, o_d_id, o_c_id, o_id)")));
    }

    #[test]
    fn final_2026_schema_uses_public_column_types() {
        let ddl = include_str!("../sql/create_tables.sql");
        for expected in [
            "s_ytd        float",
            "c_since        char(30)",
            "c_credit_lim   int",
            "c_data         char(50)",
            "h_date     char(30)",
            "o_entry_d    char(30)",
            "ol_delivery_d  char(30)",
        ] {
            assert!(ddl.contains(expected), "missing DDL fragment: {expected}");
        }
    }

    #[test]
    fn csv_asset_uses_opaque_basename_and_ddl_ordered_header() {
        let schema = RuntimeSchema::opaque(73).unwrap();
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "tpcc-opaque-csv-test-{}-{unique}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let mut materializer = CsvMaterializer {
            scale_factor: 1,
            schema: &schema,
            csv_dir: directory.clone(),
            load_dir: directory.to_string_lossy().into_owned(),
            assets: BTreeMap::new(),
        };
        let row = vec![
            SqlParam::Int(1),
            SqlParam::Str("W".to_owned()),
            SqlParam::Str("A".to_owned()),
            SqlParam::Str("B".to_owned()),
            SqlParam::Str("C".to_owned()),
            SqlParam::Str("ST".to_owned()),
            SqlParam::Str("123456789".to_owned()),
            SqlParam::Float(0.1),
            SqlParam::Float(300_000.0),
        ];
        materializer
            .write_table(
                &directory,
                directory.to_str().unwrap(),
                LogicalTable::Warehouse,
                LogicalTable::Warehouse.columns(),
                [row],
            )
            .unwrap();

        let asset = materializer.assets.get(&LogicalTable::Warehouse).unwrap();
        assert_eq!(
            asset.host_path.file_name().unwrap().to_str().unwrap(),
            schema.csv_basename(LogicalTable::Warehouse)
        );
        assert_ne!(
            schema.csv_basename(LogicalTable::Warehouse),
            "warehouse.csv"
        );
        let header = fs::read_to_string(&asset.host_path)
            .unwrap()
            .lines()
            .next()
            .unwrap()
            .to_owned();
        assert_eq!(
            header,
            schema.columns(LogicalTable::Warehouse).unwrap().join(",")
        );
        assert!(!header.contains("w_id"));
        fs::remove_dir_all(directory).unwrap();
    }
}
