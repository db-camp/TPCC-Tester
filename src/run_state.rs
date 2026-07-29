//! Versioned cross-process state for the public final-2026 workflow.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crate::loader::{LoadSummary, PartitionLoadSummary};

const STATE_VERSION: u32 = 1;
const DATASET_FILE: &str = "dataset.state";

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
        if let Ok(metadata) = fs::symlink_metadata(root) {
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(StateError::Invalid(format!(
                    "state path is not a real directory: {}",
                    root.display()
                )));
            }
        } else {
            fs::create_dir_all(root)?;
        }
        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    pub fn save_dataset(&self, state: &DatasetState) -> Result<(), StateError> {
        state.validate()?;
        atomic_write(&self.root, DATASET_FILE, state.encode().as_bytes())
    }

    pub fn load_dataset(&self) -> Result<DatasetState, StateError> {
        let input = read_limited(&self.root.join(DATASET_FILE), 256 * 1024)?;
        DatasetState::decode(&input)
    }

    pub fn root(&self) -> &Path {
        &self.root
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

fn read_limited(path: &Path, limit: u64) -> Result<String, StateError> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > limit {
        return Err(StateError::Invalid(format!(
            "unsafe or oversized state file: {}",
            path.display()
        )));
    }
    let mut input = String::with_capacity(metadata.len() as usize);
    File::open(path)?
        .take(limit + 1)
        .read_to_string(&mut input)?;
    if input.len() as u64 > limit {
        return Err(StateError::Invalid(format!(
            "state file exceeds {limit} bytes"
        )));
    }
    Ok(input)
}

fn atomic_write(root: &Path, name: &str, bytes: &[u8]) -> Result<(), StateError> {
    let target = root.join(name);
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
    use super::*;

    #[test]
    fn dataset_round_trip_rejects_wrong_partition_totals() {
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
        let state = DatasetState::from_load("run-1".to_owned(), 9, 1, load).unwrap();
        assert_eq!(DatasetState::decode(&state.encode()).unwrap(), state);

        let malformed = state
            .encode()
            .replace("order_line_rows=7", "order_line_rows=8");
        assert!(DatasetState::decode(&malformed).is_err());
    }
}
