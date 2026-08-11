use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::{create_dir_all, rename, symlink_metadata, File, Metadata, OpenOptions};
use std::io::{BufWriter, Read, Write};
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::{debug, error, info};

use crate::connection::cursor::{RmdbCursor, SqlParam};
use crate::consistency::NonNegativeF32Accumulator;
use crate::data_gen::*;
use crate::error::TpccError;
use crate::runtime_schema::{LogicalIndex, LogicalTable, RuntimeSchema};
use crate::sample_evidence::{SetupEvidence, SetupEvidenceCollector};

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
    pub setup_evidence: SetupEvidence,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CsvAsset {
    pub table: LogicalTable,
    pub host_path: PathBuf,
    pub load_path: String,
    pub row_count: i64,
    load_host_path: PathBuf,
    seal: CsvFileSeal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CsvFileSeal {
    byte_len: u64,
    content_sha256: [u8; 32],
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    link_count: u64,
}

impl CsvFileSeal {
    fn capture(path: &Path, content_sha256: [u8; 32]) -> Result<Self, TpccError> {
        let metadata = symlink_metadata(path)?;
        Self::require_regular_readonly(path, &metadata)?;
        #[cfg(unix)]
        if metadata.nlink() != 1 {
            return Err(TpccError::Protocol(format!(
                "refusing to seal CSV {} with {} hard links",
                path.display(),
                metadata.nlink()
            )));
        }
        Ok(Self {
            byte_len: metadata.len(),
            content_sha256,
            #[cfg(unix)]
            device: metadata.dev(),
            #[cfg(unix)]
            inode: metadata.ino(),
            #[cfg(unix)]
            link_count: metadata.nlink(),
        })
    }

    fn validate_metadata(&self, path: &Path, metadata: &Metadata) -> Result<(), TpccError> {
        Self::require_regular_readonly(path, metadata)?;
        if metadata.len() != self.byte_len {
            return Err(TpccError::Protocol(format!(
                "sealed CSV {} changed length from {} to {}",
                path.display(),
                self.byte_len,
                metadata.len()
            )));
        }
        #[cfg(unix)]
        if (metadata.dev(), metadata.ino(), metadata.nlink())
            != (self.device, self.inode, self.link_count)
        {
            return Err(TpccError::Protocol(format!(
                "sealed CSV {} changed file identity",
                path.display()
            )));
        }
        Ok(())
    }

    fn require_regular_readonly(path: &Path, metadata: &Metadata) -> Result<(), TpccError> {
        if !metadata.file_type().is_file() {
            return Err(TpccError::Protocol(format!(
                "sealed CSV {} is not a regular file",
                path.display()
            )));
        }
        if !metadata.permissions().readonly() {
            return Err(TpccError::Protocol(format!(
                "sealed CSV {} is writable",
                path.display()
            )));
        }
        Ok(())
    }
}

impl CsvAsset {
    fn verify_sealed(&self, boundary: &str) -> Result<(), TpccError> {
        let path_metadata = symlink_metadata(&self.host_path)?;
        self.seal
            .validate_metadata(&self.host_path, &path_metadata)?;
        self.seal.validate_metadata(
            &self.load_host_path,
            &symlink_metadata(&self.load_host_path)?,
        )?;

        let mut file = File::open(&self.load_host_path)?;
        self.seal
            .validate_metadata(&self.load_host_path, &file.metadata()?)?;
        let mut hasher = Sha256::new();
        let mut buffer = [0_u8; 1024 * 1024];
        let mut remaining = self.seal.byte_len;
        while remaining > 0 {
            let limit = usize::try_from(remaining.min(buffer.len() as u64))
                .expect("bounded CSV verification chunk fits usize");
            let read = file.read(&mut buffer[..limit])?;
            if read == 0 {
                return Err(TpccError::Protocol(format!(
                    "sealed CSV {} was truncated {boundary}",
                    self.load_host_path.display()
                )));
            }
            hasher.update(&buffer[..read]);
            remaining -= read as u64;
        }
        if file.read(&mut buffer[..1])? != 0 {
            return Err(TpccError::Protocol(format!(
                "sealed CSV {} grew beyond {} bytes {boundary}",
                self.load_host_path.display(),
                self.seal.byte_len
            )));
        }
        let actual: [u8; 32] = hasher.finalize().into();
        if actual != self.seal.content_sha256 {
            return Err(TpccError::Protocol(format!(
                "sealed CSV {} failed SHA-256 verification {boundary}",
                self.load_host_path.display()
            )));
        }
        self.seal
            .validate_metadata(&self.load_host_path, &file.metadata()?)?;
        self.seal
            .validate_metadata(&self.host_path, &symlink_metadata(&self.host_path)?)?;
        self.seal.validate_metadata(
            &self.load_host_path,
            &symlink_metadata(&self.load_host_path)?,
        )?;
        Ok(())
    }

    async fn verify_sealed_async(&self, boundary: &'static str) -> Result<(), TpccError> {
        let asset = self.clone();
        tokio::task::spawn_blocking(move || asset.verify_sealed(boundary))
            .await
            .map_err(|error| {
                TpccError::Protocol(format!(
                    "sealed CSV verification task failed {boundary}: {error}"
                ))
            })?
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaterializedLoad {
    scale_factor: i32,
    schema_fingerprint: u64,
    dataset_checksum: [u8; 32],
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
            || materialized.summary.setup_evidence.load_seed != self.schema.seed()
            || materialized
                .summary
                .setup_evidence
                .runtime_schema_fingerprint
                != self.schema.fingerprint()
            || materialized.summary.setup_evidence.dataset_checksum != materialized.dataset_checksum
        {
            return Err(TpccError::Protocol(
                "materialized CSV assets do not match this setup runtime".to_owned(),
            ));
        }
        for table in self.schema.schedule().load_tables() {
            materialized
                .asset(*table)?
                .verify_sealed_async("before the first LOAD")
                .await?;
        }
        for (ordinal, table) in self.schema.schedule().load_tables().iter().enumerate() {
            let asset = materialized.asset(*table)?;
            asset.verify_sealed_async("immediately before LOAD").await?;
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
            let load_result = self.cursor.execute_update(&sql, &[]).await;
            asset.verify_sealed_async("after LOAD").await?;
            load_result?;
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
    server_cwd: Option<PathBuf>,
    dataset_hasher: Sha256,
    assets: BTreeMap<LogicalTable, CsvAsset>,
}

struct DigestWriter<'a, W> {
    inner: W,
    dataset_digest: &'a mut Sha256,
    asset_digest: &'a mut Sha256,
}

impl<'a, W> DigestWriter<'a, W> {
    fn new(inner: W, dataset_digest: &'a mut Sha256, asset_digest: &'a mut Sha256) -> Self {
        Self {
            inner,
            dataset_digest,
            asset_digest,
        }
    }

    fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write> Write for DigestWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.dataset_digest.update(&bytes[..written]);
        self.asset_digest.update(&bytes[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
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
        let server_cwd = std::env::var_os("RMDB_TPCC_SERVER_CWD").map(PathBuf::from);
        Ok(Self {
            scale_factor,
            schema,
            csv_dir,
            load_dir,
            server_cwd,
            dataset_hasher: Self::initial_dataset_hasher(scale_factor, schema),
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
        let mut setup_evidence =
            SetupEvidenceCollector::new(&gen, self.scale_factor, self.schema.fingerprint())?;
        let csv_dir_path = self.csv_dir.clone();
        let load_dir_path = self.load_dir.clone();
        let csv_dir = csv_dir_path.as_path();
        let load_dir = load_dir_path.as_str();
        create_dir_all(csv_dir)?;

        self.write_table_observed(
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
            |row| setup_evidence.observe_warehouse(row),
        )?;
        self.write_table_observed(
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
            |row| setup_evidence.observe_district(row),
        )?;
        self.write_table_observed(
            csv_dir,
            load_dir,
            LogicalTable::Item,
            &["i_id", "i_im_id", "i_name", "i_price", "i_data"],
            gen.generate_items().into_iter().map(|i| i.to_sql_params()),
            |row| setup_evidence.observe_item(row),
        )?;
        self.write_table_observed(
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
            |row| setup_evidence.observe_customer(row),
        )?;
        self.write_table_observed(
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
            |row| setup_evidence.observe_stock(row),
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
        self.write_table_observed(
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
            |row| setup_evidence.observe_order(row),
        )?;
        self.write_table_observed(
            csv_dir,
            load_dir,
            LogicalTable::NewOrders,
            &["no_o_id", "no_d_id", "no_w_id"],
            gen.generate_new_orders()
                .into_iter()
                .map(|n| n.to_sql_params()),
            |row| setup_evidence.observe_new_order(row),
        )?;
        self.write_table_observed(
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
            |row| setup_evidence.observe_history(row),
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
                    })?;
                setup_evidence.observe_order_line(row)
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
        let dataset_checksum: [u8; 32] = self.dataset_hasher.clone().finalize().into();
        let setup_evidence = setup_evidence.finish(dataset_checksum)?;

        info!("[数据物化] 9 个 CSV 已全部完成");
        Ok(MaterializedLoad {
            scale_factor: self.scale_factor,
            schema_fingerprint: self.schema.fingerprint(),
            dataset_checksum,
            assets: self.assets,
            summary: LoadSummary {
                order_line_rows: expected_order_line_count,
                undelivered_order_line_rows,
                order_line_amounts,
                partitions,
                setup_evidence,
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

        let runtime_columns = self
            .schema
            .columns(table)
            .map_err(|error| TpccError::Protocol(error.to_string()))?;
        let ordinal = LogicalTable::ALL
            .iter()
            .position(|candidate| *candidate == table)
            .expect("logical table disappeared from the complete set");
        self.dataset_hasher.update([0xff, ordinal as u8]);
        self.dataset_hasher
            .update((basename.len() as u32).to_be_bytes());
        self.dataset_hasher.update(basename.as_bytes());
        let mut asset_hasher = Sha256::new();
        let mut writer = DigestWriter::new(
            BufWriter::new(file),
            &mut self.dataset_hasher,
            &mut asset_hasher,
        );
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
        drop(writer.into_inner());
        self.dataset_hasher.update([0xfe, ordinal as u8]);
        self.dataset_hasher.update(total.to_be_bytes());
        rename(&partial_file, &csv_file)?;
        let mut permissions = symlink_metadata(&csv_file)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&csv_file, permissions)?;
        let content_sha256 = asset_hasher.finalize().into();
        let load_path = format!("{load_dir}/{basename}");
        let load_host_path = Self::load_host_path(&load_path, self.server_cwd.as_deref())?;
        let row_count = i64::try_from(total).map_err(|_| {
            TpccError::Protocol(format!("{} CSV row count overflow", table.canonical()))
        })?;
        let asset = CsvAsset {
            table,
            host_path: csv_file,
            load_path,
            row_count,
            load_host_path: load_host_path.clone(),
            seal: CsvFileSeal::capture(&csv_dir.join(basename), content_sha256)?,
        };
        asset
            .seal
            .validate_metadata(&load_host_path, &symlink_metadata(&load_host_path)?)?;
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

    fn write_csv_row(writer: &mut impl Write, row: &[SqlParam]) -> Result<(), TpccError> {
        for (idx, param) in row.iter().enumerate() {
            if idx > 0 {
                writer.write_all(b",")?;
            }
            writer.write_all(Self::csv_value(param).as_bytes())?;
        }
        writer.write_all(b"\n")?;
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

    fn initial_dataset_hasher(scale_factor: i32, schema: &RuntimeSchema) -> Sha256 {
        let mut hasher = Sha256::new();
        hasher.update(b"rmdb-tpcc-generated-csv-v1\0");
        hasher.update(scale_factor.to_be_bytes());
        hasher.update(schema.seed().to_be_bytes());
        hasher.update(schema.fingerprint().to_be_bytes());
        hasher
    }

    fn load_host_path(load_path: &str, server_cwd: Option<&Path>) -> Result<PathBuf, TpccError> {
        let path = PathBuf::from(load_path);
        if path.is_absolute() {
            return Ok(path);
        }
        let server_cwd = server_cwd.ok_or_else(|| {
            TpccError::Protocol(
                "relative RMDB_TPCC_LOAD_DIR requires RMDB_TPCC_SERVER_CWD".to_owned(),
            )
        })?;
        if !server_cwd.is_absolute() {
            return Err(TpccError::Protocol(
                "RMDB_TPCC_SERVER_CWD must be absolute".to_owned(),
            ));
        }
        Ok(server_cwd.join(path))
    }
}

impl Loader<'_> {
    async fn verify_counts(&mut self, materialized: &MaterializedLoad) -> Result<(), TpccError> {
        info!("[数据验证] 检查各表行数...");
        let mut all_ok = true;
        for table in self.schema.schedule().count_tables() {
            let expected = materialized.asset(*table)?.row_count;
            let logical = table.canonical();
            let sql = row_count_sql(self.schema.table(*table));
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

fn row_count_sql(table: &str) -> String {
    format!("SELECT COUNT(*) AS row_count FROM {table}")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;

    #[test]
    fn load_verification_count_uses_the_official_alias_shape() {
        assert_eq!(
            row_count_sql("opaque_stock"),
            "SELECT COUNT(*) AS row_count FROM opaque_stock"
        );
    }

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
    fn relative_load_paths_require_and_use_the_server_working_directory() {
        let server_cwd = Path::new("/tmp/rmdb/database");
        assert_eq!(
            CsvMaterializer::load_host_path("../csv/table.csv", Some(server_cwd)).unwrap(),
            server_cwd.join("../csv/table.csv")
        );
        assert!(CsvMaterializer::load_host_path("../csv/table.csv", None).is_err());
        assert!(CsvMaterializer::load_host_path(
            "../csv/table.csv",
            Some(Path::new("relative/database"))
        )
        .is_err());
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
            server_cwd: None,
            dataset_hasher: CsvMaterializer::initial_dataset_hasher(1, &schema),
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
        let empty_checksum: [u8; 32] = CsvMaterializer::initial_dataset_hasher(1, &schema)
            .finalize()
            .into();
        let streamed_checksum: [u8; 32] = materializer.dataset_hasher.clone().finalize().into();
        assert_ne!(streamed_checksum, empty_checksum);

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
        asset.verify_sealed("during test").unwrap();

        let original = fs::read(&asset.host_path).unwrap();
        let mut permissions = fs::metadata(&asset.host_path).unwrap().permissions();
        permissions.set_readonly(false);
        fs::set_permissions(&asset.host_path, permissions).unwrap();
        let mut tampered = original.clone();
        let last = tampered.last_mut().unwrap();
        *last ^= 1;
        fs::write(&asset.host_path, tampered).unwrap();
        let mut permissions = fs::metadata(&asset.host_path).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&asset.host_path, permissions).unwrap();
        assert!(asset
            .verify_sealed("after same-length modification")
            .unwrap_err()
            .to_string()
            .contains("SHA-256"));

        let mut permissions = fs::metadata(&asset.host_path).unwrap().permissions();
        permissions.set_readonly(false);
        fs::set_permissions(&asset.host_path, permissions).unwrap();
        fs::write(&asset.host_path, &original).unwrap();
        let mut permissions = fs::metadata(&asset.host_path).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&asset.host_path, permissions).unwrap();
        asset.verify_sealed("after restoration").unwrap();

        let wrong_load_path = directory.join("wrong-load.csv");
        fs::write(&wrong_load_path, &original).unwrap();
        let mut permissions = fs::metadata(&wrong_load_path).unwrap().permissions();
        permissions.set_readonly(true);
        fs::set_permissions(&wrong_load_path, permissions).unwrap();
        let mut wrong_asset = asset.clone();
        wrong_asset.load_host_path = wrong_load_path;
        assert!(wrong_asset
            .verify_sealed("with mismatched LOAD path")
            .unwrap_err()
            .to_string()
            .contains("file identity"));

        #[cfg(unix)]
        {
            let replacement = directory.join("replacement.csv");
            fs::write(&replacement, &original).unwrap();
            let mut permissions = fs::metadata(&replacement).unwrap().permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&replacement, permissions).unwrap();
            fs::rename(replacement, &asset.host_path).unwrap();
            assert!(asset
                .verify_sealed("after replacement")
                .unwrap_err()
                .to_string()
                .contains("file identity"));
        }
        fs::remove_dir_all(directory).unwrap();
    }
}
