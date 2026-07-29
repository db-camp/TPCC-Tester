//! Versioned cross-process state for the public final-2026 workflow.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::consistency::{FloatAggregateId, FLOAT_AGGREGATES};
use crate::loader::{LoadSummary, PartitionLoadSummary};
use crate::ranking::ledger::RunLedger;

const STATE_VERSION: u32 = 1;
const DATASET_FILE: &str = "dataset.state";
const MAX_DATASET_STATE_BYTES: usize = 256 * 1024;
const ARTIFACT_VERSION: u32 = 1;
const LEDGER_ARTIFACT: &str = "run_ledger";
const LEDGER_FILE: &str = "run_ledger.state";
const FLOAT_BASELINE_ARTIFACT: &str = "float_baseline";
const FLOAT_BASELINE_FILE: &str = "float_baseline.state";
const MAX_ARTIFACT_HEADER_BYTES: u64 = 4 * 1024;
const MAX_LEDGER_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
const MAX_FLOAT_BASELINE_PAYLOAD_BYTES: usize = 4 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetState {
    pub run_id: String,
    pub seed: u64,
    pub warehouses: i32,
    pub order_line_rows: i64,
    pub undelivered_order_line_rows: i64,
    pub partitions: Vec<PartitionLoadSummary>,
}

impl DatasetState {
    pub fn from_load(
        run_id: String,
        seed: u64,
        warehouses: i32,
        load: LoadSummary,
    ) -> Result<Self, StateError> {
        validate_run_id(&run_id)?;
        if warehouses <= 0 || load.order_line_rows < 0 || load.undelivered_order_line_rows < 0 {
            return Err(StateError::Invalid(
                "dataset counts must be non-negative and warehouses positive".to_owned(),
            ));
        }
        if load.partitions.len() != warehouses as usize * 10 {
            return Err(StateError::Invalid(format!(
                "dataset has {} partitions, expected {}",
                load.partitions.len(),
                warehouses * 10
            )));
        }
        let state = Self {
            run_id,
            seed,
            warehouses,
            order_line_rows: load.order_line_rows,
            undelivered_order_line_rows: load.undelivered_order_line_rows,
            partitions: load.partitions,
        };
        state.validate()?;
        Ok(state)
    }

    fn validate(&self) -> Result<(), StateError> {
        validate_run_id(&self.run_id)?;
        if self.warehouses <= 0 || self.order_line_rows < 0 || self.undelivered_order_line_rows < 0
        {
            return Err(StateError::Invalid("invalid dataset dimensions".to_owned()));
        }
        if self.partitions.len() != self.warehouses as usize * 10 {
            return Err(StateError::Invalid(
                "dataset partition count does not match warehouses".to_owned(),
            ));
        }
        let mut expected_warehouse = 1;
        let mut expected_district = 1;
        let mut order_lines = 0_i64;
        let mut undelivered = 0_i64;
        for partition in &self.partitions {
            if partition.warehouse_id != expected_warehouse
                || partition.district_id != expected_district
                || partition.order_line_rows < 0
                || partition.undelivered_order_line_rows < 0
                || partition.undelivered_order_line_rows > partition.order_line_rows
            {
                return Err(StateError::Invalid(format!(
                    "invalid or unordered partition ({}, {})",
                    partition.warehouse_id, partition.district_id
                )));
            }
            order_lines = order_lines
                .checked_add(partition.order_line_rows)
                .ok_or_else(|| StateError::Invalid("order-line total overflow".to_owned()))?;
            undelivered = undelivered
                .checked_add(partition.undelivered_order_line_rows)
                .ok_or_else(|| StateError::Invalid("undelivered total overflow".to_owned()))?;
            expected_district += 1;
            if expected_district == 11 {
                expected_district = 1;
                expected_warehouse += 1;
            }
        }
        if order_lines != self.order_line_rows || undelivered != self.undelivered_order_line_rows {
            return Err(StateError::Invalid(
                "dataset partition totals do not match global totals".to_owned(),
            ));
        }
        Ok(())
    }

    fn encode(&self) -> String {
        let mut output = format!(
            "version={STATE_VERSION}\nrun_id={}\nseed={}\nwarehouses={}\norder_line_rows={}\nundelivered_order_line_rows={}\n",
            self.run_id,
            self.seed,
            self.warehouses,
            self.order_line_rows,
            self.undelivered_order_line_rows
        );
        for partition in &self.partitions {
            output.push_str(&format!(
                "partition={},{},{},{}\n",
                partition.warehouse_id,
                partition.district_id,
                partition.order_line_rows,
                partition.undelivered_order_line_rows
            ));
        }
        output
    }

    fn decode(input: &str) -> Result<Self, StateError> {
        let mut lines = input.lines();
        expect_exact(&mut lines, "version", STATE_VERSION)?;
        let run_id = value(&mut lines, "run_id")?.to_owned();
        let seed = parse(value(&mut lines, "seed")?, "seed")?;
        let warehouses = parse(value(&mut lines, "warehouses")?, "warehouses")?;
        let order_line_rows = parse(value(&mut lines, "order_line_rows")?, "order_line_rows")?;
        let undelivered_order_line_rows = parse(
            value(&mut lines, "undelivered_order_line_rows")?,
            "undelivered_order_line_rows",
        )?;
        let mut partitions = Vec::new();
        for line in lines {
            let raw = line
                .strip_prefix("partition=")
                .ok_or_else(|| StateError::Invalid(format!("unknown dataset line {line:?}")))?;
            let fields = raw.split(',').collect::<Vec<_>>();
            if fields.len() != 4 {
                return Err(StateError::Invalid(format!(
                    "invalid partition line {line:?}"
                )));
            }
            partitions.push(PartitionLoadSummary {
                warehouse_id: parse(fields[0], "partition warehouse")?,
                district_id: parse(fields[1], "partition district")?,
                order_line_rows: parse(fields[2], "partition order-line rows")?,
                undelivered_order_line_rows: parse(fields[3], "partition undelivered rows")?,
            });
        }
        let state = Self {
            run_id,
            seed,
            warehouses,
            order_line_rows,
            undelivered_order_line_rows,
            partitions,
        };
        state.validate()?;
        Ok(state)
    }
}

#[derive(Clone, Debug)]
pub struct StateStore {
    root: PathBuf,
}

impl StateStore {
    pub fn open(root: &Path) -> Result<Self, StateError> {
        match fs::symlink_metadata(root) {
            Ok(metadata) => validate_real_directory(root, &metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(root)?;
                validate_real_directory(root, &fs::symlink_metadata(root)?)?;
            }
            Err(error) => return Err(StateError::Io(error)),
        }
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    pub fn save_dataset(&self, state: &DatasetState) -> Result<(), StateError> {
        state.validate()?;
        let encoded = state.encode();
        if encoded.len() > MAX_DATASET_STATE_BYTES {
            return Err(StateError::Invalid(format!(
                "dataset state exceeds {MAX_DATASET_STATE_BYTES} bytes"
            )));
        }
        atomic_write(&self.root, DATASET_FILE, encoded.as_bytes())
    }

    pub fn load_dataset(&self) -> Result<DatasetState, StateError> {
        let input = read_limited(
            &self.root.join(DATASET_FILE),
            MAX_DATASET_STATE_BYTES as u64,
        )?;
        DatasetState::decode(&input)
    }

    pub fn save_ledger(
        &self,
        dataset: &DatasetState,
        ledger: &RunLedger,
    ) -> Result<(), StateError> {
        dataset.validate()?;
        let payload = ledger.encode();
        let encoded =
            encode_artifact(LEDGER_ARTIFACT, dataset, &payload, MAX_LEDGER_PAYLOAD_BYTES)?;
        atomic_write(&self.root, LEDGER_FILE, encoded.as_bytes())
    }

    pub fn load_ledger(&self, dataset: &DatasetState) -> Result<RunLedger, StateError> {
        dataset.validate()?;
        let input = read_limited(
            &self.root.join(LEDGER_FILE),
            MAX_LEDGER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
        )?;
        let payload = decode_artifact(&input, LEDGER_ARTIFACT, dataset, MAX_LEDGER_PAYLOAD_BYTES)?;
        RunLedger::decode(payload)
            .map_err(|error| StateError::Invalid(format!("invalid run ledger payload: {error}")))
    }

    pub fn save_float_baseline(
        &self,
        dataset: &DatasetState,
        values: &BTreeMap<FloatAggregateId, u32>,
    ) -> Result<(), StateError> {
        dataset.validate()?;
        let ledger_checksum = self.read_ledger_checksum(dataset)?;
        let payload = encode_float_baseline(values, ledger_checksum)?;
        let encoded = encode_artifact(
            FLOAT_BASELINE_ARTIFACT,
            dataset,
            &payload,
            MAX_FLOAT_BASELINE_PAYLOAD_BYTES,
        )?;
        atomic_write(&self.root, FLOAT_BASELINE_FILE, encoded.as_bytes())
    }

    pub fn load_float_baseline(
        &self,
        dataset: &DatasetState,
    ) -> Result<BTreeMap<FloatAggregateId, u32>, StateError> {
        dataset.validate()?;
        let ledger_checksum = self.read_ledger_checksum(dataset)?;
        let input = read_limited(
            &self.root.join(FLOAT_BASELINE_FILE),
            MAX_FLOAT_BASELINE_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
        )?;
        let payload = decode_artifact(
            &input,
            FLOAT_BASELINE_ARTIFACT,
            dataset,
            MAX_FLOAT_BASELINE_PAYLOAD_BYTES,
        )?;
        decode_float_baseline(payload, ledger_checksum)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn read_ledger_checksum(&self, dataset: &DatasetState) -> Result<u64, StateError> {
        read_artifact_header_checksum(
            &self.root.join(LEDGER_FILE),
            LEDGER_ARTIFACT,
            dataset,
            MAX_LEDGER_PAYLOAD_BYTES,
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("state I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid final2026 state: {0}")]
    Invalid(String),
}

fn validate_run_id(value: &str) -> Result<(), StateError> {
    if value.is_empty()
        || value.len() > 120
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._:-".contains(&byte))
    {
        return Err(StateError::Invalid("run_id is not a safe token".to_owned()));
    }
    Ok(())
}

fn value<'a>(lines: &mut impl Iterator<Item = &'a str>, key: &str) -> Result<&'a str, StateError> {
    let line = lines
        .next()
        .ok_or_else(|| StateError::Invalid(format!("missing {key}")))?;
    line.strip_prefix(&format!("{key}="))
        .ok_or_else(|| StateError::Invalid(format!("expected {key}, got {line:?}")))
}

fn expect_exact<'a, T>(
    lines: &mut impl Iterator<Item = &'a str>,
    key: &str,
    expected: T,
) -> Result<(), StateError>
where
    T: std::str::FromStr + PartialEq + std::fmt::Display,
{
    let actual = parse::<T>(value(lines, key)?, key)?;
    if actual == expected {
        Ok(())
    } else {
        Err(StateError::Invalid(format!(
            "{key} version mismatch: expected {expected}, got {actual}"
        )))
    }
}

fn parse<T: std::str::FromStr>(value: &str, name: &str) -> Result<T, StateError> {
    value
        .parse()
        .map_err(|_| StateError::Invalid(format!("{name} is not a valid number")))
}

fn encode_artifact(
    artifact: &str,
    dataset: &DatasetState,
    payload: &str,
    payload_limit: usize,
) -> Result<String, StateError> {
    if payload.len() > payload_limit {
        return Err(StateError::Invalid(format!(
            "{artifact} payload exceeds {payload_limit} bytes"
        )));
    }
    let metadata = artifact_metadata(artifact, dataset, payload.len());
    let checksum = checksum64(metadata.as_bytes(), payload.as_bytes());
    Ok(format!("{metadata}checksum={checksum:016x}\n{payload}"))
}

struct ArtifactHeader<'a> {
    run_id: &'a str,
    seed: u64,
    warehouses: i32,
    dataset_checksum: u64,
    payload_len: usize,
    checksum: u64,
}

fn decode_artifact<'a>(
    input: &'a str,
    expected_artifact: &str,
    dataset: &DatasetState,
    payload_limit: usize,
) -> Result<&'a str, StateError> {
    let (header, payload) =
        parse_artifact_header(input, expected_artifact, dataset, payload_limit)?;
    if payload.as_bytes().len() != header.payload_len {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} payload length mismatch: expected {}, got {}",
            header.payload_len,
            payload.len()
        )));
    }

    let metadata = artifact_metadata_fields(
        expected_artifact,
        header.run_id,
        header.seed,
        header.warehouses,
        header.dataset_checksum,
        header.payload_len,
    );
    let actual_checksum = checksum64(metadata.as_bytes(), payload.as_bytes());
    if actual_checksum != header.checksum {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} checksum mismatch"
        )));
    }
    Ok(payload)
}

fn parse_artifact_header<'a>(
    input: &'a str,
    expected_artifact: &str,
    dataset: &DatasetState,
    payload_limit: usize,
) -> Result<(ArtifactHeader<'a>, &'a str), StateError> {
    let mut sections = input.splitn(9, '\n');
    let artifact = value(&mut sections, "artifact")?;
    if artifact != expected_artifact {
        return Err(StateError::Invalid(format!(
            "expected {expected_artifact} artifact, got {artifact:?}"
        )));
    }
    expect_exact(&mut sections, "version", ARTIFACT_VERSION)?;
    let run_id = value(&mut sections, "run_id")?;
    validate_run_id(run_id)?;
    let seed = parse(value(&mut sections, "seed")?, "seed")?;
    let warehouses = parse(value(&mut sections, "warehouses")?, "warehouses")?;
    let encoded_dataset_checksum = parse_checksum(value(&mut sections, "dataset_checksum")?)?;
    let payload_len: usize = parse(value(&mut sections, "payload_len")?, "payload_len")?;
    let encoded_checksum = parse_checksum(value(&mut sections, "checksum")?)?;
    let payload = sections
        .next()
        .ok_or_else(|| StateError::Invalid("missing artifact payload".to_owned()))?;

    if payload_len > payload_limit {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} payload exceeds {payload_limit} bytes"
        )));
    }
    let expected_dataset_checksum = dataset_checksum(dataset);
    let identity_matches = run_id == dataset.run_id
        && seed == dataset.seed
        && warehouses == dataset.warehouses
        && encoded_dataset_checksum == expected_dataset_checksum;
    if !identity_matches {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} identity does not match dataset.state"
        )));
    }

    Ok((
        ArtifactHeader {
            run_id,
            seed,
            warehouses,
            dataset_checksum: encoded_dataset_checksum,
            payload_len,
            checksum: encoded_checksum,
        },
        payload,
    ))
}

fn artifact_metadata(artifact: &str, dataset: &DatasetState, payload_len: usize) -> String {
    artifact_metadata_fields(
        artifact,
        &dataset.run_id,
        dataset.seed,
        dataset.warehouses,
        dataset_checksum(dataset),
        payload_len,
    )
}

fn artifact_metadata_fields(
    artifact: &str,
    run_id: &str,
    seed: u64,
    warehouses: i32,
    dataset_checksum: u64,
    payload_len: usize,
) -> String {
    format!(
        "artifact={artifact}\nversion={ARTIFACT_VERSION}\nrun_id={run_id}\nseed={seed}\nwarehouses={warehouses}\ndataset_checksum={dataset_checksum:016x}\npayload_len={payload_len}\n"
    )
}

fn dataset_checksum(dataset: &DatasetState) -> u64 {
    checksum64(&[], dataset.encode().as_bytes())
}

fn checksum64(metadata: &[u8], payload: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    checksum64_update(checksum64_update(FNV_OFFSET_BASIS, metadata), payload)
}

fn checksum64_update(hash: u64, bytes: &[u8]) -> u64 {
    const FNV_PRIME: u64 = 0x100000001b3;
    bytes.iter().fold(hash, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
    })
}

fn parse_checksum(value: &str) -> Result<u64, StateError> {
    if value.len() != 16
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StateError::Invalid(
            "checksum must be 16 lower-case hexadecimal digits".to_owned(),
        ));
    }
    u64::from_str_radix(value, 16)
        .map_err(|_| StateError::Invalid("checksum is not valid hexadecimal".to_owned()))
}

fn encode_float_baseline(
    values: &BTreeMap<FloatAggregateId, u32>,
    ledger_checksum: u64,
) -> Result<String, StateError> {
    if values.len() != FLOAT_AGGREGATES.len()
        || FLOAT_AGGREGATES
            .iter()
            .any(|spec| !values.contains_key(&spec.id))
    {
        return Err(StateError::Invalid(
            "FLOAT baseline must contain all seven aggregate categories exactly once".to_owned(),
        ));
    }

    let mut payload = format!("ledger_checksum={ledger_checksum:016x}\n");
    for spec in FLOAT_AGGREGATES {
        let bits = values
            .get(&spec.id)
            .ok_or_else(|| StateError::Invalid("missing FLOAT baseline category".to_owned()))?;
        if !f32::from_bits(*bits).is_finite() {
            return Err(StateError::Invalid(format!(
                "FLOAT baseline {} must be finite",
                float_aggregate_name(spec.id)
            )));
        }
        payload.push_str(float_aggregate_name(spec.id));
        payload.push('=');
        payload.push_str(&format!("{bits:08x}"));
        payload.push('\n');
    }
    Ok(payload)
}

fn decode_float_baseline(
    payload: &str,
    ledger_checksum: u64,
) -> Result<BTreeMap<FloatAggregateId, u32>, StateError> {
    if !payload.ends_with('\n') {
        return Err(StateError::Invalid(
            "FLOAT baseline payload must end with a newline".to_owned(),
        ));
    }

    let mut lines = payload.split_terminator('\n');
    let encoded_ledger_checksum = parse_checksum(value(&mut lines, "ledger_checksum")?)?;
    if encoded_ledger_checksum != ledger_checksum {
        return Err(StateError::Invalid(
            "FLOAT baseline belongs to a different run ledger".to_owned(),
        ));
    }

    let mut values = BTreeMap::new();
    for line in lines {
        if line.is_empty() {
            return Err(StateError::Invalid(
                "FLOAT baseline contains an empty field".to_owned(),
            ));
        }
        let (name, encoded_bits) = line
            .split_once('=')
            .ok_or_else(|| StateError::Invalid(format!("invalid FLOAT baseline field {line:?}")))?;
        let id = parse_float_aggregate_name(name).ok_or_else(|| {
            StateError::Invalid(format!("unknown FLOAT baseline category {name:?}"))
        })?;
        if encoded_bits.len() != 8
            || !encoded_bits
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(StateError::Invalid(format!(
                "FLOAT baseline {name} must contain eight lower-case hexadecimal digits"
            )));
        }
        let bits = u32::from_str_radix(encoded_bits, 16).map_err(|_| {
            StateError::Invalid(format!("FLOAT baseline {name} is not valid hexadecimal"))
        })?;
        if !f32::from_bits(bits).is_finite() {
            return Err(StateError::Invalid(format!(
                "FLOAT baseline {name} must be finite"
            )));
        }
        if values.insert(id, bits).is_some() {
            return Err(StateError::Invalid(format!(
                "duplicate FLOAT baseline category {name:?}"
            )));
        }
    }

    if values.len() != FLOAT_AGGREGATES.len() {
        let missing = FLOAT_AGGREGATES
            .iter()
            .find(|spec| !values.contains_key(&spec.id))
            .map(|spec| float_aggregate_name(spec.id))
            .unwrap_or("unknown");
        return Err(StateError::Invalid(format!(
            "missing FLOAT baseline category {missing:?}"
        )));
    }
    Ok(values)
}

fn float_aggregate_name(id: FloatAggregateId) -> &'static str {
    match id {
        FloatAggregateId::WarehouseYtd => "warehouse_ytd",
        FloatAggregateId::DistrictYtd => "district_ytd",
        FloatAggregateId::CustomerBalance => "customer_balance",
        FloatAggregateId::CustomerYtdPayment => "customer_ytd_payment",
        FloatAggregateId::HistoryAmount => "history_amount",
        FloatAggregateId::StockYtd => "stock_ytd",
        FloatAggregateId::OrderLineAmount => "order_line_amount",
    }
}

fn parse_float_aggregate_name(value: &str) -> Option<FloatAggregateId> {
    match value {
        "warehouse_ytd" => Some(FloatAggregateId::WarehouseYtd),
        "district_ytd" => Some(FloatAggregateId::DistrictYtd),
        "customer_balance" => Some(FloatAggregateId::CustomerBalance),
        "customer_ytd_payment" => Some(FloatAggregateId::CustomerYtdPayment),
        "history_amount" => Some(FloatAggregateId::HistoryAmount),
        "stock_ytd" => Some(FloatAggregateId::StockYtd),
        "order_line_amount" => Some(FloatAggregateId::OrderLineAmount),
        _ => None,
    }
}

fn validate_real_directory(path: &Path, metadata: &fs::Metadata) -> Result<(), StateError> {
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(StateError::Invalid(format!(
            "state path is not a real directory: {}",
            path.display()
        )));
    }
    Ok(())
}

fn read_artifact_header_checksum(
    path: &Path,
    expected_artifact: &str,
    dataset: &DatasetState,
    payload_limit: usize,
) -> Result<u64, StateError> {
    let path_metadata = fs::symlink_metadata(path)?;
    let total_limit = payload_limit as u64 + MAX_ARTIFACT_HEADER_BYTES;
    if path_metadata.file_type().is_symlink()
        || !path_metadata.is_file()
        || path_metadata.len() > total_limit
    {
        return Err(StateError::Invalid(format!(
            "unsafe or oversized state file: {}",
            path.display()
        )));
    }

    let mut file = File::open(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.is_file()
        || opened_metadata.len() != path_metadata.len()
        || opened_metadata.len() > total_limit
    {
        return Err(StateError::Invalid(format!(
            "state file changed while opening: {}",
            path.display()
        )));
    }

    let mut prefix = String::new();
    (&mut file)
        .take(MAX_ARTIFACT_HEADER_BYTES)
        .read_to_string(&mut prefix)?;
    let (header, _) = parse_artifact_header(&prefix, expected_artifact, dataset, payload_limit)?;
    let metadata = artifact_metadata_fields(
        expected_artifact,
        header.run_id,
        header.seed,
        header.warehouses,
        header.dataset_checksum,
        header.payload_len,
    );
    let canonical_header = format!("{metadata}checksum={:016x}\n", header.checksum);
    if !prefix.starts_with(&canonical_header) {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} header is not canonical"
        )));
    }
    let expected_file_len = canonical_header
        .len()
        .checked_add(header.payload_len)
        .ok_or_else(|| StateError::Invalid("artifact file length overflow".to_owned()))?;
    if opened_metadata.len() != expected_file_len as u64 {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} file length does not match its header"
        )));
    }

    let mut actual_checksum = checksum64(metadata.as_bytes(), &[]);
    let prefix_payload = &prefix.as_bytes()[canonical_header.len()..];
    actual_checksum = checksum64_update(actual_checksum, prefix_payload);
    let mut payload_bytes = prefix_payload.len();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        payload_bytes = payload_bytes
            .checked_add(read)
            .ok_or_else(|| StateError::Invalid("artifact payload length overflow".to_owned()))?;
        if payload_bytes > header.payload_len {
            return Err(StateError::Invalid(format!(
                "{expected_artifact} payload exceeds its header length"
            )));
        }
        actual_checksum = checksum64_update(actual_checksum, &buffer[..read]);
    }
    if payload_bytes != header.payload_len || actual_checksum != header.checksum {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} checksum mismatch"
        )));
    }
    Ok(header.checksum)
}

fn read_limited(path: &Path, limit: u64) -> Result<String, StateError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
        return Err(StateError::Invalid(format!(
            "unsafe or oversized state file: {}",
            path.display()
        )));
    }
    let file = File::open(path)?;
    let opened_metadata = file.metadata()?;
    if !opened_metadata.is_file()
        || opened_metadata.len() != metadata.len()
        || opened_metadata.len() > limit
    {
        return Err(StateError::Invalid(format!(
            "state file changed while opening: {}",
            path.display()
        )));
    }
    let mut input = String::with_capacity(metadata.len() as usize);
    file.take(limit + 1).read_to_string(&mut input)?;
    if input.len() as u64 > limit {
        return Err(StateError::Invalid(format!(
            "state file exceeds {limit} bytes"
        )));
    }
    Ok(input)
}

fn atomic_write(root: &Path, name: &str, bytes: &[u8]) -> Result<(), StateError> {
    let target = root.join(name);
    match fs::symlink_metadata(&target) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || !metadata.is_file() {
                return Err(StateError::Invalid(format!(
                    "state target is not a real file: {}",
                    target.display()
                )));
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(StateError::Io(error)),
    }
    let root_metadata = fs::symlink_metadata(root)?;
    if root_metadata.file_type().is_symlink() || !root_metadata.is_dir() {
        return Err(StateError::Invalid(format!(
            "state path is not a real directory: {}",
            root.display()
        )));
    }
    let temporary = root.join(format!(".{name}.{}.tmp", std::process::id()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    if let Err(error) = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temporary, &target)?;
        File::open(root)?.sync_all()?;
        Ok(())
    })() {
        let _ = fs::remove_file(&temporary);
        return Err(StateError::Io(error));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_TEST_DIRECTORY: AtomicU64 = AtomicU64::new(0);

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "rmdb-tpcc-state-{}-{}",
                std::process::id(),
                NEXT_TEST_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn sample_dataset(run_id: &str, seed: u64) -> DatasetState {
        let load = LoadSummary {
            order_line_rows: 7,
            undelivered_order_line_rows: 7,
            partitions: (1..=10)
                .map(|district_id| PartitionLoadSummary {
                    warehouse_id: 1,
                    district_id,
                    order_line_rows: if district_id == 1 { 7 } else { 0 },
                    undelivered_order_line_rows: if district_id == 1 { 7 } else { 0 },
                })
                .collect(),
        };
        DatasetState::from_load(run_id.to_owned(), seed, 1, load).unwrap()
    }

    fn sample_float_baseline() -> BTreeMap<FloatAggregateId, u32> {
        FLOAT_AGGREGATES
            .iter()
            .enumerate()
            .map(|(index, spec)| (spec.id, 0x3f80_0000 + index as u32))
            .collect()
    }

    #[test]
    fn dataset_round_trip_rejects_wrong_partition_totals() {
        let state = sample_dataset("run-1", 9);
        assert_eq!(DatasetState::decode(&state.encode()).unwrap(), state);

        let malformed = state
            .encode()
            .replace("order_line_rows=7", "order_line_rows=8");
        assert!(DatasetState::decode(&malformed).is_err());
    }

    #[test]
    fn artifact_envelope_binds_identity_length_and_checksum() {
        let dataset = sample_dataset("run-envelope", 41);
        let encoded = encode_artifact("sample", &dataset, "payload\n", 128).unwrap();
        assert_eq!(
            decode_artifact(&encoded, "sample", &dataset, 128).unwrap(),
            "payload\n"
        );

        let different_dataset = sample_dataset("run-envelope", 42);
        assert!(decode_artifact(&encoded, "sample", &different_dataset, 128).is_err());
        let mut different_load = dataset.clone();
        different_load.order_line_rows = 8;
        different_load.undelivered_order_line_rows = 8;
        different_load.partitions[0].order_line_rows = 8;
        different_load.partitions[0].undelivered_order_line_rows = 8;
        assert!(decode_artifact(&encoded, "sample", &different_load, 128).is_err());

        let mut damaged = encoded;
        damaged.pop();
        damaged.push('x');
        assert!(decode_artifact(&damaged, "sample", &dataset, 128).is_err());

        let oversized = format!(
            "artifact=sample\nversion=1\nrun_id={}\nseed={}\nwarehouses={}\ndataset_checksum={:016x}\npayload_len=129\nchecksum=0000000000000000\nx",
            dataset.run_id,
            dataset.seed,
            dataset.warehouses,
            dataset_checksum(&dataset)
        );
        assert!(decode_artifact(&oversized, "sample", &dataset, 128).is_err());
    }

    #[test]
    fn state_store_round_trips_ledger_and_float_baseline() {
        let directory = TestDirectory::new();
        let store = StateStore::open(&directory.0).unwrap();
        let dataset = sample_dataset("run-state-store", 73);
        let ledger = RunLedger::default();
        let baseline = sample_float_baseline();

        store.save_ledger(&dataset, &ledger).unwrap();
        store.save_float_baseline(&dataset, &baseline).unwrap();

        assert_eq!(store.load_ledger(&dataset).unwrap(), ledger);
        assert_eq!(store.load_float_baseline(&dataset).unwrap(), baseline);
    }

    #[test]
    fn float_baseline_rejects_same_length_ledger_payload_damage() {
        let directory = TestDirectory::new();
        let store = StateStore::open(&directory.0).unwrap();
        let dataset = sample_dataset("run-ledger-damage", 74);
        store.save_ledger(&dataset, &RunLedger::default()).unwrap();
        store
            .save_float_baseline(&dataset, &sample_float_baseline())
            .unwrap();

        let ledger_path = directory.0.join(LEDGER_FILE);
        let mut bytes = fs::read(&ledger_path).unwrap();
        let payload_byte = bytes
            .iter_mut()
            .rev()
            .find(|byte| **byte == b'0')
            .expect("default ledger contains a decimal zero");
        *payload_byte = b'1';
        fs::write(&ledger_path, bytes).unwrap();

        assert!(store.load_float_baseline(&dataset).is_err());
    }

    #[test]
    fn float_baseline_rejects_incomplete_duplicate_and_unknown_fields() {
        let ledger_checksum = 0x1234_5678_90ab_cdef;
        let complete = encode_float_baseline(&sample_float_baseline(), ledger_checksum).unwrap();
        assert_eq!(
            decode_float_baseline(&complete, ledger_checksum).unwrap(),
            sample_float_baseline()
        );

        assert!(decode_float_baseline(&complete, ledger_checksum + 1).is_err());
        assert!(decode_float_baseline(
            "ledger_checksum=1234567890abcdef\nwarehouse_ytd=3f800000\n",
            ledger_checksum
        )
        .is_err());
        assert!(decode_float_baseline(
            "ledger_checksum=1234567890abcdef\nwarehouse_ytd=3f800000\nwarehouse_ytd=3f800000\n",
            ledger_checksum
        )
        .is_err());
        assert!(decode_float_baseline(
            "ledger_checksum=1234567890abcdef\nfuture_category=3f800000\n",
            ledger_checksum
        )
        .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn state_store_rejects_symlink_artifact_targets() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let store = StateStore::open(&directory.0).unwrap();
        let dataset = sample_dataset("run-symlink", 5);
        symlink(
            directory.0.join("missing-target"),
            directory.0.join(LEDGER_FILE),
        )
        .unwrap();

        assert!(store.save_ledger(&dataset, &RunLedger::default()).is_err());
        assert!(store.load_ledger(&dataset).is_err());
    }
}
