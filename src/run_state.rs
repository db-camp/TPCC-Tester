//! Versioned cross-process state for the public final-2026 workflow.

use std::collections::BTreeMap;
#[cfg(unix)]
use std::ffi::{CStr, CString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use sha2::{Digest, Sha256};

use crate::consistency::{
    FloatAggregateId, NonNegativeF32Accumulator, OnlineKeySample, FLOAT_AGGREGATES,
};
use crate::data_gen::TpccDataGen;
use crate::loader::{LoadSummary, PartitionLoadSummary};
use crate::profile::{
    LOAD_BUDGET_SECONDS, MEASUREMENT_SECONDS, MEASUREMENT_WINDOWS, OFFICIAL_CLIENTS,
    OFFICIAL_WAREHOUSES, RECOVERY_READY_BUDGET_SECONDS, WARMUP_SECONDS,
};
use crate::ranking::core_artifact_codec::{
    decode_terminal_artifact_hex, encode_terminal_artifact_hex, PersistedTerminalEvidence,
    TerminalArtifactBinding, MAX_TERMINAL_ARTIFACT_HEX_CHARS,
};
use crate::ranking::evidence_collector::CustomerKey;
use crate::ranking::rich_recovery_samples::{InitialCustomerData, InitialHistoryRow};
use crate::ranking::terminal_evidence::SealedTerminalEvidence;
use crate::runtime_schema::{RuntimeSchema, ENCODED_BEGIN_MARKER, ENCODED_END_MARKER};
use crate::sample_evidence::SetupEvidence;

const STATE_VERSION: u32 = 5;
const DATASET_FILE: &str = "dataset.state";
const MAX_DATASET_STATE_BYTES: usize = 256 * 1024;
const ARTIFACT_VERSION: u32 = 1;
const SETUP_INTENT_ARTIFACT: &str = "setup_claim";
const SETUP_INTENT_FILE: &str = "setup.started";
const SETUP_EXECUTION_ARTIFACT: &str = "setup_execution_claim";
const SETUP_EXECUTION_FILE: &str = "setup.execution.started";
const RUN_CONTRACT_ARTIFACT: &str = "run_contract";
const RUN_CONTRACT_FILE: &str = "run_contract.state";
const SETUP_CHECK_CLAIM_ARTIFACT: &str = "setup_check_claim";
const SETUP_CHECK_CLAIM_FILE: &str = "setup_check.started";
const SETUP_RECEIPT_ARTIFACT: &str = "setup_check_receipt";
const SETUP_RECEIPT_FILE: &str = "setup_check.passed";
const RANK_CLAIM_ARTIFACT: &str = "rank_claim";
const RANK_CLAIM_FILE: &str = "rank.started";
const LEDGER_FILE: &str = "run_ledger.state";
const RANKED_TERMINAL_EVIDENCE_ARTIFACT: &str = "ranked_terminal_evidence";
const NON_RANKED_TERMINAL_EVIDENCE_ARTIFACT: &str = "non_ranked_terminal_evidence";
const TERMINAL_EVIDENCE_FILE: &str = "terminal_evidence.state";
const ONLINE_CLAIM_ARTIFACT: &str = "online_check_claim";
const ONLINE_CLAIM_FILE: &str = "online_check.started";
const FLOAT_BASELINE_ARTIFACT: &str = "float_baseline";
const FLOAT_BASELINE_FILE: &str = "float_baseline.state";
const CRASH_INTENT_ARTIFACT: &str = "crash_intent";
const CRASH_INTENT_FILE: &str = "crash.intent";
const CRASH_KILLED_ARTIFACT: &str = "crash_killed";
const CRASH_KILLED_FILE: &str = "crash.killed";
const RESTART_STARTED_ARTIFACT: &str = "restart_started";
const RESTART_STARTED_FILE: &str = "restart.started";
const RESTART_READY_ARTIFACT: &str = "restart_ready";
const RESTART_READY_FILE: &str = "restart.ready";
const RECOVERY_CLAIM_ARTIFACT: &str = "recovery_check_claim";
const RECOVERY_CLAIM_FILE: &str = "recovery_check.started";
const RECOVERY_RECEIPT_ARTIFACT: &str = "recovery_check_receipt";
const RECOVERY_RECEIPT_FILE: &str = "recovery_check.passed";
const DIAGNOSTIC_WARMUP_CLAIM_ARTIFACT: &str = "diagnostic_warmup_claim";
const DIAGNOSTIC_WARMUP_CLAIM_FILE: &str = "diagnostic_warmup.started";
const DIAGNOSTIC_WARMUP_RECEIPT_ARTIFACT: &str = "diagnostic_warmup_receipt";
const DIAGNOSTIC_WARMUP_RECEIPT_FILE: &str = "diagnostic_warmup.passed";
const DIAGNOSTIC_OBSERVATION_CLAIM_ARTIFACT: &str = "diagnostic_observation_claim";
const DIAGNOSTIC_OBSERVATION_CLAIM_FILE: &str = "diagnostic_observation.started";
const DIAGNOSTIC_OBSERVATION_RECEIPT_ARTIFACT: &str = "diagnostic_observation_receipt";
const DIAGNOSTIC_OBSERVATION_RECEIPT_FILE: &str = "diagnostic_observation.passed";
const DATABASE_IDENTITY_FILE: &str = "database.identity";
const STATE_ARTIFACT_FILES: [&str; 20] = [
    DATASET_FILE,
    SETUP_INTENT_FILE,
    SETUP_EXECUTION_FILE,
    RUN_CONTRACT_FILE,
    SETUP_CHECK_CLAIM_FILE,
    SETUP_RECEIPT_FILE,
    RANK_CLAIM_FILE,
    TERMINAL_EVIDENCE_FILE,
    ONLINE_CLAIM_FILE,
    FLOAT_BASELINE_FILE,
    CRASH_INTENT_FILE,
    CRASH_KILLED_FILE,
    RESTART_STARTED_FILE,
    RESTART_READY_FILE,
    RECOVERY_CLAIM_FILE,
    RECOVERY_RECEIPT_FILE,
    DIAGNOSTIC_WARMUP_CLAIM_FILE,
    DIAGNOSTIC_WARMUP_RECEIPT_FILE,
    DIAGNOSTIC_OBSERVATION_CLAIM_FILE,
    DIAGNOSTIC_OBSERVATION_RECEIPT_FILE,
];
const FORMAL_STATE_DOMAIN_V2: &[u8] = b"RMDB_TPCC_FORMAL_CHAIN_V2\0";
const FORMAL_STATE_MAX_TOTAL_BYTES: u64 = 32 * 1024 * 1024;
const FORMAL_STATE_FILES_V2: [(&str, u64); 16] = [
    (DATASET_FILE, MAX_DATASET_STATE_BYTES as u64),
    (SETUP_INTENT_FILE, MAX_SETUP_INTENT_BYTES as u64),
    (SETUP_EXECUTION_FILE, MAX_SETUP_INTENT_BYTES as u64),
    (
        RUN_CONTRACT_FILE,
        MAX_CONTRACT_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        SETUP_CHECK_CLAIM_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        SETUP_RECEIPT_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        RANK_CLAIM_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (TERMINAL_EVIDENCE_FILE, MAX_TERMINAL_EVIDENCE_FILE_BYTES),
    (
        ONLINE_CLAIM_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        FLOAT_BASELINE_FILE,
        MAX_FLOAT_BASELINE_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        CRASH_INTENT_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        CRASH_KILLED_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        RESTART_STARTED_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        RESTART_READY_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        RECOVERY_CLAIM_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
    (
        RECOVERY_RECEIPT_FILE,
        MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
    ),
];
const DIAGNOSTIC_STATE_FILES: [&str; 4] = [
    DIAGNOSTIC_WARMUP_CLAIM_FILE,
    DIAGNOSTIC_WARMUP_RECEIPT_FILE,
    DIAGNOSTIC_OBSERVATION_CLAIM_FILE,
    DIAGNOSTIC_OBSERVATION_RECEIPT_FILE,
];
const MAX_ARTIFACT_HEADER_BYTES: u64 = 4 * 1024;
const MAX_TERMINAL_EVIDENCE_HEX_CHARS: usize = 16_777_216;
const MAX_BOUND_PAYLOAD_OVERHEAD_BYTES: usize = 128;
const MAX_TERMINAL_EVIDENCE_PAYLOAD_BYTES: usize = 16_777_344;
const MAX_TERMINAL_EVIDENCE_FILE_BYTES: u64 = 16_781_440;
const _: () = assert!(MAX_TERMINAL_ARTIFACT_HEX_CHARS == MAX_TERMINAL_EVIDENCE_HEX_CHARS);
const _: () = assert!(
    MAX_TERMINAL_EVIDENCE_PAYLOAD_BYTES
        == MAX_TERMINAL_EVIDENCE_HEX_CHARS + MAX_BOUND_PAYLOAD_OVERHEAD_BYTES
);
const _: () = assert!(
    MAX_TERMINAL_EVIDENCE_FILE_BYTES
        == MAX_TERMINAL_EVIDENCE_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES
);
const MAX_FLOAT_BASELINE_PAYLOAD_BYTES: usize = 4 * 1024;
const MAX_CONTRACT_PAYLOAD_BYTES: usize = 4 * 1024;
const MAX_MARKER_PAYLOAD_BYTES: usize = 256;
const MAX_SETUP_INTENT_BYTES: usize = 8 * 1024;
static NEXT_TEMPORARY_FILE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunConformance {
    PublicSpecAligned,
    NonRankedDeviation,
}

impl RunConformance {
    fn as_str(self) -> &'static str {
        match self {
            Self::PublicSpecAligned => "public_spec_aligned",
            Self::NonRankedDeviation => "non_ranked_deviation",
        }
    }

    fn parse(value: &str) -> Result<Self, StateError> {
        match value {
            "public_spec_aligned" => Ok(Self::PublicSpecAligned),
            "non_ranked_deviation" => Ok(Self::NonRankedDeviation),
            _ => Err(StateError::Invalid(format!(
                "unknown run conformance {value:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunContract {
    pub warehouses: u16,
    pub clients: u16,
    pub warmup_seconds: u64,
    pub measurement_windows: u8,
    pub window_seconds: u64,
    pub load_budget_seconds: u64,
    pub recovery_ready_budget_seconds: u64,
    pub response_timeout_seconds: u64,
    pub phase_tail_grace_seconds: u64,
    pub conformance: RunConformance,
}

impl RunContract {
    fn validate_shape(&self) -> Result<(), StateError> {
        if self.warehouses == 0
            || self.clients == 0
            || self.measurement_windows == 0
            || self.window_seconds == 0
            || self.load_budget_seconds == 0
            || self.recovery_ready_budget_seconds == 0
            || self.response_timeout_seconds == 0
            || self.phase_tail_grace_seconds == 0
        {
            return Err(StateError::Invalid(
                "run contract dimensions and positive durations must be non-zero".to_owned(),
            ));
        }
        let public_shape = self.warehouses == OFFICIAL_WAREHOUSES
            && self.clients == OFFICIAL_CLIENTS
            && self.warmup_seconds == WARMUP_SECONDS
            && self.measurement_windows == MEASUREMENT_WINDOWS
            && self.window_seconds == MEASUREMENT_SECONDS
            && self.load_budget_seconds == LOAD_BUDGET_SECONDS
            && self.recovery_ready_budget_seconds == RECOVERY_READY_BUDGET_SECONDS;
        if (self.conformance == RunConformance::PublicSpecAligned) != public_shape {
            return Err(StateError::Invalid(
                "run contract conformance does not match its published profile dimensions"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn validate(&self, dataset: &DatasetState) -> Result<(), StateError> {
        self.validate_shape()?;
        if i32::from(self.warehouses) != dataset.warehouses {
            return Err(StateError::Invalid(
                "run contract warehouses do not match dataset.state".to_owned(),
            ));
        }
        Ok(())
    }

    fn encode(&self) -> String {
        format!(
            "contract_version=2\nwarehouses={}\nclients={}\nwarmup_seconds={}\nmeasurement_windows={}\nwindow_seconds={}\nload_budget_seconds={}\nrecovery_ready_budget_seconds={}\nresponse_timeout_seconds={}\nphase_tail_grace_seconds={}\nconformance={}\n",
            self.warehouses,
            self.clients,
            self.warmup_seconds,
            self.measurement_windows,
            self.window_seconds,
            self.load_budget_seconds,
            self.recovery_ready_budget_seconds,
            self.response_timeout_seconds,
            self.phase_tail_grace_seconds,
            self.conformance.as_str()
        )
    }

    fn decode(input: &str) -> Result<Self, StateError> {
        let mut lines = input.lines();
        expect_exact(&mut lines, "contract_version", 2_u32)?;
        let contract = Self {
            warehouses: parse(value(&mut lines, "warehouses")?, "contract warehouses")?,
            clients: parse(value(&mut lines, "clients")?, "contract clients")?,
            warmup_seconds: parse(
                value(&mut lines, "warmup_seconds")?,
                "contract warmup seconds",
            )?,
            measurement_windows: parse(
                value(&mut lines, "measurement_windows")?,
                "contract measurement windows",
            )?,
            window_seconds: parse(
                value(&mut lines, "window_seconds")?,
                "contract window seconds",
            )?,
            load_budget_seconds: parse(
                value(&mut lines, "load_budget_seconds")?,
                "contract load budget",
            )?,
            recovery_ready_budget_seconds: parse(
                value(&mut lines, "recovery_ready_budget_seconds")?,
                "contract recovery readiness budget",
            )?,
            response_timeout_seconds: parse(
                value(&mut lines, "response_timeout_seconds")?,
                "contract response timeout",
            )?,
            phase_tail_grace_seconds: parse(
                value(&mut lines, "phase_tail_grace_seconds")?,
                "contract phase tail grace",
            )?,
            conformance: RunConformance::parse(value(&mut lines, "conformance")?)?,
        };
        if lines.next().is_some() {
            return Err(StateError::Invalid(
                "run contract contains trailing fields".to_owned(),
            ));
        }
        Ok(contract)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FormalStateReceipt {
    terminal_evidence_size: u64,
    terminal_evidence_sha256: [u8; 32],
    formal_chain_sha256: [u8; 32],
    state_device: u64,
    state_inode: u64,
}

impl std::fmt::Display for FormalStateReceipt {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            output,
            "FORMAL_STATE_V2 terminal_evidence_size={} terminal_evidence_sha256={} \
             formal_chain_sha256={} state_device={} state_inode={}",
            self.terminal_evidence_size,
            lowercase_sha256(self.terminal_evidence_sha256),
            lowercase_sha256(self.formal_chain_sha256),
            self.state_device,
            self.state_inode,
        )
    }
}

fn lowercase_sha256(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

#[derive(Debug)]
pub struct SetupClaim {
    intent_checksum: u64,
    execution_checksum: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SetupClaimOrigin {
    Created,
    Resumed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CrashLifecycleEvent {
    Intent,
    Killed,
    RestartStarted,
    RestartReady,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticStage {
    Warmup,
    Observation,
}

#[derive(Clone, Copy, Debug)]
struct ClaimToken {
    contract_checksum: u64,
    claim_checksum: u64,
    predecessor_checksum: u64,
}

#[derive(Debug)]
pub struct SetupCheckClaim(ClaimToken);

#[derive(Debug)]
pub struct RankClaim(ClaimToken);

#[derive(Debug)]
pub struct TerminalOnlineCheckClaim {
    token: ClaimToken,
    terminal_evidence_checksum: u64,
}

#[derive(Debug)]
pub struct TerminalRecoveryCheckClaim {
    token: ClaimToken,
    terminal_evidence_checksum: u64,
    baseline_checksum: u64,
}

#[derive(Debug)]
pub struct TerminalDiagnosticClaim {
    token: ClaimToken,
    stage: DiagnosticStage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatasetState {
    pub run_id: String,
    pub seed: u64,
    pub warehouses: i32,
    pub order_line_rows: i64,
    pub undelivered_order_line_rows: i64,
    generated_csv_sha256: [u8; 32],
    pub runtime_schema: RuntimeSchema,
    initial_order_line_amounts: NonNegativeF32Accumulator,
    pub partitions: Vec<PartitionLoadSummary>,
    setup_evidence: SetupEvidence,
}

impl DatasetState {
    pub fn from_load(
        run_id: String,
        seed: u64,
        warehouses: i32,
        load: LoadSummary,
    ) -> Result<Self, StateError> {
        let runtime_schema = RuntimeSchema::opaque(seed).map_err(|error| {
            StateError::Invalid(format!("cannot derive runtime schema: {error}"))
        })?;
        Self::from_load_with_schema(run_id, seed, warehouses, load, runtime_schema)
    }

    pub fn from_load_with_schema(
        run_id: String,
        seed: u64,
        warehouses: i32,
        load: LoadSummary,
        runtime_schema: RuntimeSchema,
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
        let generated_csv_sha256 = load.setup_evidence.dataset_checksum;
        let state = Self {
            run_id,
            seed,
            warehouses,
            order_line_rows: load.order_line_rows,
            undelivered_order_line_rows: load.undelivered_order_line_rows,
            generated_csv_sha256,
            runtime_schema,
            initial_order_line_amounts: load.order_line_amounts,
            partitions: load.partitions,
            setup_evidence: load.setup_evidence,
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
        self.runtime_schema
            .validate()
            .map_err(|error| StateError::Invalid(format!("invalid runtime schema: {error}")))?;
        if self.runtime_schema.seed() != self.seed {
            return Err(StateError::Invalid(
                "runtime schema seed does not match dataset seed".to_owned(),
            ));
        }
        if self.partitions.len() != self.warehouses as usize * 10 {
            return Err(StateError::Invalid(
                "dataset partition count does not match warehouses".to_owned(),
            ));
        }
        if self.initial_order_line_amounts.term_count()
            != u64::try_from(self.order_line_rows)
                .map_err(|_| StateError::Invalid("invalid order-line row count".to_owned()))?
        {
            return Err(StateError::Invalid(
                "initial order-line FLOAT term count does not match row count".to_owned(),
            ));
        }
        self.initial_order_line_amounts
            .boundary()
            .map_err(|error| {
                StateError::Invalid(format!(
                    "invalid initial order-line FLOAT accumulator: {error}"
                ))
            })?;
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
        self.validate_setup_evidence_binding()?;
        Ok(())
    }

    pub fn validate_setup_evidence_binding(&self) -> Result<(), StateError> {
        self.setup_evidence
            .validate_binding(
                self.warehouses,
                self.seed,
                self.runtime_schema.fingerprint(),
                &self.generated_csv_sha256,
            )
            .map_err(|error| StateError::Invalid(format!("invalid setup evidence: {error}")))
    }

    fn encode(&self) -> String {
        let (amount_terms, amount_words) = self.initial_order_line_amounts.to_words();
        let amount_words = amount_words
            .iter()
            .map(|word| format!("{word:016x}"))
            .collect::<Vec<_>>()
            .join(",");
        let mut output = format!(
            "version={STATE_VERSION}\nrun_id={}\nseed={}\nwarehouses={}\norder_line_rows={}\nundelivered_order_line_rows={}\norder_line_amount_terms={amount_terms}\norder_line_amount_words={amount_words}\ngenerated_csv_sha256={}\n",
            self.run_id,
            self.seed,
            self.warehouses,
            self.order_line_rows,
            self.undelivered_order_line_rows,
            hex_encode(&self.generated_csv_sha256),
        );
        output.push_str(&self.runtime_schema.encode());
        output.push_str(&format!(
            "setup_evidence={}\n",
            self.setup_evidence.encode_hex()
        ));
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
        let amount_terms = parse(
            value(&mut lines, "order_line_amount_terms")?,
            "order_line_amount_terms",
        )?;
        let amount_words = parse_accumulator_words(value(&mut lines, "order_line_amount_words")?)?;
        let initial_order_line_amounts =
            NonNegativeF32Accumulator::from_words(amount_terms, &amount_words).map_err(
                |error| {
                    StateError::Invalid(format!(
                        "invalid initial order-line FLOAT accumulator: {error}"
                    ))
                },
            )?;
        let generated_csv_sha256 = parse_sha256(
            value(&mut lines, "generated_csv_sha256")?,
            "generated CSV checksum",
        )?;
        let first_schema_line = lines
            .next()
            .ok_or_else(|| StateError::Invalid("missing runtime schema".to_owned()))?;
        if first_schema_line != ENCODED_BEGIN_MARKER {
            return Err(StateError::Invalid(format!(
                "expected {ENCODED_BEGIN_MARKER}, got {first_schema_line:?}"
            )));
        }
        let mut encoded_schema = String::from(ENCODED_BEGIN_MARKER);
        encoded_schema.push('\n');
        loop {
            let line = lines
                .next()
                .ok_or_else(|| StateError::Invalid(format!("missing {ENCODED_END_MARKER}")))?;
            encoded_schema.push_str(line);
            encoded_schema.push('\n');
            if line == ENCODED_END_MARKER {
                break;
            }
        }
        let runtime_schema = RuntimeSchema::decode(&encoded_schema)
            .map_err(|error| StateError::Invalid(format!("invalid runtime schema: {error}")))?;
        let setup_evidence =
            SetupEvidence::decode_hex(value(&mut lines, "setup_evidence")?, warehouses)
                .map_err(|error| StateError::Invalid(format!("invalid setup evidence: {error}")))?;
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
            generated_csv_sha256,
            runtime_schema,
            initial_order_line_amounts,
            partitions,
            setup_evidence,
        };
        state.validate()?;
        if state.encode() != input {
            return Err(StateError::Invalid(
                "dataset state encoding is not canonical".to_owned(),
            ));
        }
        Ok(state)
    }

    pub fn initial_order_line_amounts(&self) -> &NonNegativeF32Accumulator {
        &self.initial_order_line_amounts
    }

    pub const fn generated_csv_sha256(&self) -> &[u8; 32] {
        &self.generated_csv_sha256
    }

    pub fn setup_evidence(&self) -> &SetupEvidence {
        &self.setup_evidence
    }

    pub fn online_key_sample(&self) -> Result<OnlineKeySample, StateError> {
        self.validate()?;
        self.setup_evidence
            .online_key_sample(self.warehouses, self.seed)
            .map_err(|error| {
                StateError::Invalid(format!(
                    "cannot select dataset-bound online setup evidence: {error}"
                ))
            })
    }
}

#[derive(Clone, Debug)]
pub struct StateStore {
    root: PathBuf,
}

impl StateStore {
    pub fn open_terminal(root: &Path) -> Result<Self, StateError> {
        match fs::symlink_metadata(root) {
            Ok(metadata) => validate_real_directory(root, &metadata)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                fs::create_dir_all(root)?;
                validate_real_directory(root, &fs::symlink_metadata(root)?)?;
            }
            Err(error) => return Err(StateError::Io(error)),
        }
        let store = Self {
            root: root.to_path_buf(),
        };
        let _directory_lock = store.lock_clean()?;
        drop(_directory_lock);
        Ok(store)
    }

    pub fn open_existing_terminal(root: &Path) -> Result<Self, StateError> {
        let metadata = fs::symlink_metadata(root).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                StateError::Invalid(format!(
                    "state directory does not exist: {}",
                    root.display()
                ))
            } else {
                StateError::Io(error)
            }
        })?;
        validate_real_directory(root, &metadata)?;
        let store = Self {
            root: root.to_path_buf(),
        };
        let _directory_lock = store.lock_clean()?;
        drop(_directory_lock);
        Ok(store)
    }

    fn lock_clean(&self) -> Result<StateDirectoryLock, StateError> {
        lock_clean_terminal_state_directory(&self.root)
    }

    pub fn publish_setup_intent(
        &self,
        run_id: &str,
        seed: u64,
        contract: &RunContract,
    ) -> Result<(), StateError> {
        let _directory_lock = self.lock_clean()?;
        self.create_setup_intent(run_id, seed, contract)?;
        Ok(())
    }

    fn create_setup_intent(
        &self,
        run_id: &str,
        seed: u64,
        contract: &RunContract,
    ) -> Result<u64, StateError> {
        validate_run_id(run_id)?;
        contract.validate_shape()?;
        self.ensure_fresh_setup_root()?;
        let encoded = encode_setup_intent(run_id, seed, contract)?;
        atomic_publish_new(&self.root, SETUP_INTENT_FILE, encoded.as_bytes())?;
        let (_, checksum) = decode_setup_intent(&read_limited(
            &self.root.join(SETUP_INTENT_FILE),
            MAX_SETUP_INTENT_BYTES as u64,
        )?)?;
        Ok(checksum)
    }

    pub fn begin_or_resume_setup(
        &self,
        run_id: &str,
        seed: u64,
        contract: &RunContract,
    ) -> Result<(SetupClaim, SetupClaimOrigin), StateError> {
        let _directory_lock = self.lock_clean()?;
        let (intent_checksum, origin) =
            match fs::symlink_metadata(self.root.join(SETUP_INTENT_FILE)) {
                Ok(_) => (
                    self.resume_setup_intent(run_id, seed, contract)?,
                    SetupClaimOrigin::Resumed,
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => (
                    self.create_setup_intent(run_id, seed, contract)?,
                    SetupClaimOrigin::Created,
                ),
                Err(error) => return Err(StateError::Io(error)),
            };
        let encoded = encode_setup_execution(run_id, seed, contract, intent_checksum)?;
        atomic_publish_new(&self.root, SETUP_EXECUTION_FILE, encoded.as_bytes())?;
        let (execution, execution_checksum) = self.load_setup_execution()?;
        if execution.intent_checksum != intent_checksum
            || execution.run_id != run_id
            || execution.seed != seed
            || execution.contract != *contract
        {
            return Err(StateError::Invalid(
                "setup execution claim changed while it was published".to_owned(),
            ));
        }
        Ok((
            SetupClaim {
                intent_checksum,
                execution_checksum,
            },
            origin,
        ))
    }

    fn resume_setup_intent(
        &self,
        run_id: &str,
        seed: u64,
        contract: &RunContract,
    ) -> Result<u64, StateError> {
        validate_run_id(run_id)?;
        contract.validate_shape()?;
        self.ensure_pending_setup_only()?;
        let (intent, checksum) = self.load_setup_intent()?;
        if intent.run_id != run_id || intent.seed != seed || intent.contract != *contract {
            return Err(StateError::Invalid(
                "pre-start setup claim does not match requested run/seed/profile".to_owned(),
            ));
        }
        Ok(checksum)
    }

    pub fn complete_dataset(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        claim: SetupClaim,
    ) -> Result<(), StateError> {
        let _directory_lock = self.lock_clean()?;
        dataset.validate()?;
        contract.validate(dataset)?;
        self.ensure_setup_execution_pending()?;
        let (intent, checksum) = self.load_setup_intent()?;
        let (execution, execution_checksum) = self.load_setup_execution()?;
        if checksum != claim.intent_checksum
            || execution_checksum != claim.execution_checksum
            || intent.run_id != dataset.run_id
            || intent.seed != dataset.seed
            || intent.contract != *contract
            || execution.run_id != dataset.run_id
            || execution.seed != dataset.seed
            || execution.contract != *contract
            || execution.intent_checksum != checksum
        {
            return Err(StateError::Invalid(
                "loaded dataset does not match its setup intent and execution claim".to_owned(),
            ));
        }
        self.save_dataset(dataset)?;
        let payload = encode_setup_bound_contract(checksum, execution_checksum, contract);
        let encoded = encode_artifact(
            RUN_CONTRACT_ARTIFACT,
            dataset,
            &payload,
            MAX_CONTRACT_PAYLOAD_BYTES,
        )?;
        atomic_publish_new(&self.root, RUN_CONTRACT_FILE, encoded.as_bytes())
    }

    #[cfg(test)]
    pub fn initialize_run(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<(), StateError> {
        let (claim, _) = self.begin_or_resume_setup(&dataset.run_id, dataset.seed, contract)?;
        self.complete_dataset(dataset, contract, claim)
    }

    pub fn load_bound_dataset(&self, expected: &RunContract) -> Result<DatasetState, StateError> {
        let _directory_lock = self.lock_clean()?;
        let dataset = self.load_dataset_unlocked()?;
        self.contract_checksum(&dataset, expected)?;
        Ok(dataset)
    }

    pub fn begin_setup_check(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<SetupCheckClaim, StateError> {
        let _directory_lock = self.lock_clean()?;
        self.ensure_no_diagnostic_drift()?;
        let contract_checksum = self.contract_checksum(dataset, contract)?;
        let claim_checksum = self.publish_marker(
            SETUP_CHECK_CLAIM_FILE,
            SETUP_CHECK_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            contract_checksum,
        )?;
        Ok(SetupCheckClaim(ClaimToken {
            contract_checksum,
            claim_checksum,
            predecessor_checksum: contract_checksum,
        }))
    }

    pub fn complete_setup_check(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        claim: SetupCheckClaim,
    ) -> Result<(), StateError> {
        let _directory_lock = self.lock_clean()?;
        self.validate_claim(
            dataset,
            contract,
            claim.0,
            SETUP_CHECK_CLAIM_FILE,
            SETUP_CHECK_CLAIM_ARTIFACT,
        )?;
        self.publish_marker(
            SETUP_RECEIPT_FILE,
            SETUP_RECEIPT_ARTIFACT,
            dataset,
            claim.0.contract_checksum,
            claim.0.claim_checksum,
        )?;
        Ok(())
    }

    pub fn begin_rank(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<RankClaim, StateError> {
        let _directory_lock = self.lock_clean()?;
        self.ensure_no_diagnostic_drift()?;
        let contract_checksum = self.contract_checksum(dataset, contract)?;
        let setup_claim = self.load_marker(
            SETUP_CHECK_CLAIM_FILE,
            SETUP_CHECK_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            contract_checksum,
        )?;
        let setup_receipt = self.load_marker(
            SETUP_RECEIPT_FILE,
            SETUP_RECEIPT_ARTIFACT,
            dataset,
            contract_checksum,
            setup_claim,
        )?;
        let claim_checksum = self.publish_marker(
            RANK_CLAIM_FILE,
            RANK_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            setup_receipt,
        )?;
        Ok(RankClaim(ClaimToken {
            contract_checksum,
            claim_checksum,
            predecessor_checksum: setup_receipt,
        }))
    }

    pub fn complete_rank_with_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        claim: RankClaim,
        terminal_evidence: &SealedTerminalEvidence,
    ) -> Result<(), StateError> {
        let _directory_lock = self.lock_clean()?;
        self.validate_claim(
            dataset,
            contract,
            claim.0,
            RANK_CLAIM_FILE,
            RANK_CLAIM_ARTIFACT,
        )?;
        let binding = terminal_artifact_binding(dataset)?;
        let inner = encode_terminal_artifact_hex(terminal_evidence, binding).map_err(|error| {
            StateError::Invalid(format!("cannot encode terminal evidence: {error}"))
        })?;
        let payload =
            encode_bound_payload(claim.0.contract_checksum, claim.0.claim_checksum, &inner);
        let artifact = terminal_evidence_artifact(contract.conformance);
        let encoded = encode_artifact(
            artifact,
            dataset,
            &payload,
            MAX_TERMINAL_EVIDENCE_PAYLOAD_BYTES,
        )?;
        if encoded.len() as u64 > MAX_TERMINAL_EVIDENCE_FILE_BYTES {
            return Err(StateError::Invalid(format!(
                "{artifact} file exceeds {MAX_TERMINAL_EVIDENCE_FILE_BYTES} bytes"
            )));
        }
        atomic_publish_new(&self.root, TERMINAL_EVIDENCE_FILE, encoded.as_bytes())
    }

    pub fn begin_online_check_from_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<(TerminalOnlineCheckClaim, PersistedTerminalEvidence), StateError> {
        let _directory_lock = self.lock_clean()?;
        self.ensure_no_diagnostic_drift()?;
        let (contract_checksum, _, terminal_evidence, terminal_evidence_checksum) =
            self.load_bound_terminal_evidence(dataset, contract)?;
        let claim_checksum = self.publish_marker(
            ONLINE_CLAIM_FILE,
            ONLINE_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
        )?;
        Ok((
            TerminalOnlineCheckClaim {
                token: ClaimToken {
                    contract_checksum,
                    claim_checksum,
                    predecessor_checksum: terminal_evidence_checksum,
                },
                terminal_evidence_checksum,
            },
            terminal_evidence,
        ))
    }

    pub fn complete_online_check_from_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        claim: TerminalOnlineCheckClaim,
        values: &BTreeMap<FloatAggregateId, u32>,
    ) -> Result<(), StateError> {
        let _directory_lock = self.lock_clean()?;
        self.validate_claim(
            dataset,
            contract,
            claim.token,
            ONLINE_CLAIM_FILE,
            ONLINE_CLAIM_ARTIFACT,
        )?;
        let (_, _, _, terminal_evidence_checksum) =
            self.load_bound_terminal_evidence(dataset, contract)?;
        if terminal_evidence_checksum != claim.terminal_evidence_checksum {
            return Err(StateError::Invalid(
                "online claim belongs to different terminal evidence".to_owned(),
            ));
        }
        let baseline = encode_terminal_float_baseline(values, terminal_evidence_checksum)?;
        let payload = encode_bound_payload(
            claim.token.contract_checksum,
            claim.token.claim_checksum,
            &baseline,
        );
        let encoded = encode_artifact(
            FLOAT_BASELINE_ARTIFACT,
            dataset,
            &payload,
            MAX_FLOAT_BASELINE_PAYLOAD_BYTES,
        )?;
        atomic_publish_new(&self.root, FLOAT_BASELINE_FILE, encoded.as_bytes())
    }

    pub fn record_crash_lifecycle_from_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        event: CrashLifecycleEvent,
    ) -> Result<(), StateError> {
        let _directory_lock = self.lock_clean()?;
        self.ensure_no_diagnostic_drift()?;
        let (contract_checksum, terminal_evidence_checksum, baseline_checksum) =
            self.load_terminal_crash_context(dataset, contract)?;
        let specifications = crash_lifecycle_specs();
        let target_index = crash_lifecycle_index(event);
        let mut predecessor_checksum = baseline_checksum;
        for &(file, artifact) in &specifications[..target_index] {
            predecessor_checksum = self.load_terminal_crash_lifecycle_marker(
                file,
                artifact,
                dataset,
                contract_checksum,
                terminal_evidence_checksum,
                baseline_checksum,
                predecessor_checksum,
            )?;
        }
        self.ensure_lifecycle_tail_absent(&specifications[target_index..])?;
        let (file, artifact) = specifications[target_index];
        self.publish_terminal_crash_lifecycle_marker(
            file,
            artifact,
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
            baseline_checksum,
            predecessor_checksum,
        )?;
        Ok(())
    }

    pub fn begin_recovery_check_from_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<
        (
            TerminalRecoveryCheckClaim,
            PersistedTerminalEvidence,
            BTreeMap<FloatAggregateId, u32>,
        ),
        StateError,
    > {
        let _directory_lock = self.lock_clean()?;
        self.ensure_no_diagnostic_drift()?;
        let (contract_checksum, _, terminal_evidence, terminal_evidence_checksum) =
            self.load_bound_terminal_evidence(dataset, contract)?;
        let (baseline, baseline_checksum) = self.load_bound_terminal_baseline(
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
        )?;
        let restart_ready_checksum = self.load_complete_terminal_crash_lifecycle(
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
            baseline_checksum,
        )?;
        let claim_checksum = self.publish_marker(
            RECOVERY_CLAIM_FILE,
            RECOVERY_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            restart_ready_checksum,
        )?;
        Ok((
            TerminalRecoveryCheckClaim {
                token: ClaimToken {
                    contract_checksum,
                    claim_checksum,
                    predecessor_checksum: restart_ready_checksum,
                },
                terminal_evidence_checksum,
                baseline_checksum,
            },
            terminal_evidence,
            baseline,
        ))
    }

    pub fn complete_recovery_check_from_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        claim: TerminalRecoveryCheckClaim,
    ) -> Result<(), StateError> {
        let _directory_lock = self.lock_clean()?;
        self.validate_claim(
            dataset,
            contract,
            claim.token,
            RECOVERY_CLAIM_FILE,
            RECOVERY_CLAIM_ARTIFACT,
        )?;
        let contract_checksum = self.contract_checksum(dataset, contract)?;
        let (_, _, _, terminal_evidence_checksum) =
            self.load_bound_terminal_evidence(dataset, contract)?;
        let (_, baseline_checksum) = self.load_bound_terminal_baseline(
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
        )?;
        let restart_ready_checksum = self.load_complete_terminal_crash_lifecycle(
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
            baseline_checksum,
        )?;
        if terminal_evidence_checksum != claim.terminal_evidence_checksum {
            return Err(StateError::Invalid(
                "recovery claim belongs to different terminal evidence".to_owned(),
            ));
        }
        if baseline_checksum != claim.baseline_checksum {
            return Err(StateError::Invalid(
                "recovery claim belongs to a different FLOAT baseline".to_owned(),
            ));
        }
        if restart_ready_checksum != claim.token.predecessor_checksum {
            return Err(StateError::Invalid(
                "recovery claim belongs to a different restart-ready transition".to_owned(),
            ));
        }
        self.publish_marker(
            RECOVERY_RECEIPT_FILE,
            RECOVERY_RECEIPT_ARTIFACT,
            dataset,
            claim.token.contract_checksum,
            claim.token.claim_checksum,
        )?;
        Ok(())
    }

    pub fn begin_diagnostic_from_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        stage: DiagnosticStage,
    ) -> Result<TerminalDiagnosticClaim, StateError> {
        let _directory_lock = self.lock_clean()?;
        let contract_checksum = self.contract_checksum(dataset, contract)?;
        let predecessor_checksum = match stage {
            DiagnosticStage::Warmup => {
                let (_, _, _, terminal_evidence_checksum) =
                    self.load_bound_terminal_evidence(dataset, contract)?;
                let (_, baseline_checksum) = self.load_bound_terminal_baseline(
                    dataset,
                    contract_checksum,
                    terminal_evidence_checksum,
                )?;
                let restart_ready_checksum = self.load_complete_terminal_crash_lifecycle(
                    dataset,
                    contract_checksum,
                    terminal_evidence_checksum,
                    baseline_checksum,
                )?;
                let recovery_claim = self.load_marker(
                    RECOVERY_CLAIM_FILE,
                    RECOVERY_CLAIM_ARTIFACT,
                    dataset,
                    contract_checksum,
                    restart_ready_checksum,
                )?;
                self.load_marker(
                    RECOVERY_RECEIPT_FILE,
                    RECOVERY_RECEIPT_ARTIFACT,
                    dataset,
                    contract_checksum,
                    recovery_claim,
                )?
            }
            DiagnosticStage::Observation => {
                let warmup_claim = self.load_terminal_diagnostic_warmup_claim(dataset, contract)?;
                self.load_marker(
                    DIAGNOSTIC_WARMUP_RECEIPT_FILE,
                    DIAGNOSTIC_WARMUP_RECEIPT_ARTIFACT,
                    dataset,
                    contract_checksum,
                    warmup_claim,
                )?
            }
        };
        let (file, artifact) = diagnostic_claim_spec(stage);
        let claim_checksum = self.publish_marker(
            file,
            artifact,
            dataset,
            contract_checksum,
            predecessor_checksum,
        )?;
        Ok(TerminalDiagnosticClaim {
            token: ClaimToken {
                contract_checksum,
                claim_checksum,
                predecessor_checksum,
            },
            stage,
        })
    }

    pub fn complete_diagnostic_from_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        claim: TerminalDiagnosticClaim,
    ) -> Result<(), StateError> {
        let _directory_lock = self.lock_clean()?;
        let (claim_file, claim_artifact) = diagnostic_claim_spec(claim.stage);
        self.validate_claim(dataset, contract, claim.token, claim_file, claim_artifact)?;
        let (receipt_file, receipt_artifact) = diagnostic_receipt_spec(claim.stage);
        self.publish_marker(
            receipt_file,
            receipt_artifact,
            dataset,
            claim.token.contract_checksum,
            claim.token.claim_checksum,
        )?;
        Ok(())
    }

    fn ensure_fresh_setup_root(&self) -> Result<(), StateError> {
        if let Some(entry) = fs::read_dir(&self.root)?.next().transpose()? {
            return Err(StateError::Invalid(format!(
                "setup state directory is not empty; refusing DDL/LOAD before write-once claim: {}",
                entry.path().display()
            )));
        }
        Ok(())
    }

    fn ensure_pending_setup_only(&self) -> Result<(), StateError> {
        let mut entries = fs::read_dir(&self.root)?;
        let entry = entries
            .next()
            .transpose()?
            .ok_or_else(|| StateError::Invalid("pre-start setup claim is missing".to_owned()))?;
        if entry.file_name() != SETUP_INTENT_FILE || entries.next().transpose()?.is_some() {
            return Err(StateError::Invalid(
                "setup state contains orphan or out-of-order artifacts".to_owned(),
            ));
        }
        let metadata = entry.metadata()?;
        if !metadata.is_file() || entry.file_type()?.is_symlink() {
            return Err(StateError::Invalid(
                "pre-start setup claim is not a real file".to_owned(),
            ));
        }
        Ok(())
    }

    fn ensure_setup_execution_pending(&self) -> Result<(), StateError> {
        let mut saw_intent = false;
        let mut saw_execution = false;
        for entry in fs::read_dir(&self.root)? {
            let entry = entry?;
            let file_type = entry.file_type()?;
            if file_type.is_symlink() || !file_type.is_file() {
                return Err(StateError::Invalid(format!(
                    "unsafe pre-dataset state entry: {}",
                    entry.path().display()
                )));
            }
            if entry.file_name() == SETUP_INTENT_FILE {
                if saw_intent {
                    return Err(StateError::Invalid(
                        "duplicate setup intent directory entry".to_owned(),
                    ));
                }
                saw_intent = true;
            } else if entry.file_name() == SETUP_EXECUTION_FILE {
                if saw_execution {
                    return Err(StateError::Invalid(
                        "duplicate setup execution directory entry".to_owned(),
                    ));
                }
                saw_execution = true;
            } else {
                return Err(StateError::Invalid(format!(
                    "setup state contains an orphan or out-of-order artifact: {}",
                    entry.path().display()
                )));
            }
        }
        if !saw_intent || !saw_execution {
            return Err(StateError::Invalid(
                "setup intent or execution claim is missing".to_owned(),
            ));
        }
        Ok(())
    }

    fn load_setup_intent(&self) -> Result<(SetupIntent, u64), StateError> {
        let input = read_limited(
            &self.root.join(SETUP_INTENT_FILE),
            MAX_SETUP_INTENT_BYTES as u64,
        )?;
        decode_setup_intent(&input)
    }

    fn load_setup_execution(&self) -> Result<(SetupExecution, u64), StateError> {
        let input = read_limited(
            &self.root.join(SETUP_EXECUTION_FILE),
            MAX_SETUP_INTENT_BYTES as u64,
        )?;
        decode_setup_execution(&input)
    }

    fn save_dataset(&self, state: &DatasetState) -> Result<(), StateError> {
        state.validate()?;
        let encoded = state.encode();
        if encoded.len() > MAX_DATASET_STATE_BYTES {
            return Err(StateError::Invalid(format!(
                "dataset state exceeds {MAX_DATASET_STATE_BYTES} bytes"
            )));
        }
        atomic_publish_new(&self.root, DATASET_FILE, encoded.as_bytes())
    }

    pub fn load_dataset(&self) -> Result<DatasetState, StateError> {
        let _directory_lock = self.lock_clean()?;
        self.load_dataset_unlocked()
    }

    fn load_dataset_unlocked(&self) -> Result<DatasetState, StateError> {
        let input = read_limited(
            &self.root.join(DATASET_FILE),
            MAX_DATASET_STATE_BYTES as u64,
        )?;
        DatasetState::decode(&input)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn contract_checksum(
        &self,
        dataset: &DatasetState,
        expected: &RunContract,
    ) -> Result<u64, StateError> {
        dataset.validate()?;
        expected.validate(dataset)?;
        let (intent, setup_checksum) = self.load_setup_intent()?;
        if intent.run_id != dataset.run_id
            || intent.seed != dataset.seed
            || intent.contract != *expected
        {
            return Err(StateError::Invalid(
                "setup claim does not match dataset.state and requested contract".to_owned(),
            ));
        }
        let (execution, execution_checksum) = self.load_setup_execution()?;
        if execution.run_id != dataset.run_id
            || execution.seed != dataset.seed
            || execution.contract != *expected
            || execution.intent_checksum != setup_checksum
        {
            return Err(StateError::Invalid(
                "setup execution claim does not match dataset.state and requested contract"
                    .to_owned(),
            ));
        }
        let input = read_limited(
            &self.root.join(RUN_CONTRACT_FILE),
            MAX_CONTRACT_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
        )?;
        let (payload, checksum) = decode_artifact_and_checksum(
            &input,
            RUN_CONTRACT_ARTIFACT,
            dataset,
            MAX_CONTRACT_PAYLOAD_BYTES,
        )?;
        let actual = decode_setup_bound_contract(payload, setup_checksum, execution_checksum)?;
        actual.validate(dataset)?;
        if actual != *expected {
            return Err(StateError::Invalid(format!(
                "run contract mismatch: stored={actual:?}, requested={expected:?}"
            )));
        }
        Ok(checksum)
    }

    fn publish_marker(
        &self,
        file: &str,
        artifact: &str,
        dataset: &DatasetState,
        contract_checksum: u64,
        predecessor_checksum: u64,
    ) -> Result<u64, StateError> {
        let payload = encode_bound_payload(contract_checksum, predecessor_checksum, "");
        let encoded = encode_artifact(artifact, dataset, &payload, MAX_MARKER_PAYLOAD_BYTES)?;
        atomic_publish_new(&self.root, file, encoded.as_bytes())?;
        read_artifact_header_checksum(
            &self.root.join(file),
            artifact,
            dataset,
            MAX_MARKER_PAYLOAD_BYTES,
        )
    }

    fn load_marker(
        &self,
        file: &str,
        artifact: &str,
        dataset: &DatasetState,
        contract_checksum: u64,
        predecessor_checksum: u64,
    ) -> Result<u64, StateError> {
        let input = read_limited(
            &self.root.join(file),
            MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
        )?;
        let (payload, checksum) =
            decode_artifact_and_checksum(&input, artifact, dataset, MAX_MARKER_PAYLOAD_BYTES)?;
        let inner =
            decode_bound_payload(payload, contract_checksum, predecessor_checksum, artifact)?;
        if !inner.is_empty() {
            return Err(StateError::Invalid(format!(
                "{artifact} marker payload must be empty"
            )));
        }
        Ok(checksum)
    }

    fn validate_claim(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
        claim: ClaimToken,
        file: &str,
        artifact: &str,
    ) -> Result<(), StateError> {
        let contract_checksum = self.contract_checksum(dataset, contract)?;
        if contract_checksum != claim.contract_checksum {
            return Err(StateError::Invalid(
                "phase claim belongs to a different run contract".to_owned(),
            ));
        }
        let checksum = self.load_marker(
            file,
            artifact,
            dataset,
            contract_checksum,
            claim.predecessor_checksum,
        )?;
        if checksum != claim.claim_checksum {
            return Err(StateError::Invalid(
                "phase claim checksum changed after it was issued".to_owned(),
            ));
        }
        Ok(())
    }

    fn load_rank_claim(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<(u64, u64), StateError> {
        let contract_checksum = self.contract_checksum(dataset, contract)?;
        let setup_claim = self.load_marker(
            SETUP_CHECK_CLAIM_FILE,
            SETUP_CHECK_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            contract_checksum,
        )?;
        let setup_receipt = self.load_marker(
            SETUP_RECEIPT_FILE,
            SETUP_RECEIPT_ARTIFACT,
            dataset,
            contract_checksum,
            setup_claim,
        )?;
        let rank_claim = self.load_marker(
            RANK_CLAIM_FILE,
            RANK_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            setup_receipt,
        )?;
        Ok((contract_checksum, rank_claim))
    }

    fn load_bound_terminal_evidence(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<(u64, u64, PersistedTerminalEvidence, u64), StateError> {
        self.ensure_terminal_evidence_authority()?;
        let (contract_checksum, rank_claim) = self.load_rank_claim(dataset, contract)?;
        let input = read_limited(
            &self.root.join(TERMINAL_EVIDENCE_FILE),
            MAX_TERMINAL_EVIDENCE_FILE_BYTES,
        )?;
        let artifact = terminal_evidence_artifact(contract.conformance);
        let (payload, terminal_evidence_checksum) = decode_artifact_and_checksum(
            &input,
            artifact,
            dataset,
            MAX_TERMINAL_EVIDENCE_PAYLOAD_BYTES,
        )?;
        let inner =
            decode_bound_payload(payload, contract_checksum, rank_claim, "terminal evidence")?;
        let terminal_evidence = decode_persisted_terminal_evidence(dataset, inner)?;
        Ok((
            contract_checksum,
            rank_claim,
            terminal_evidence,
            terminal_evidence_checksum,
        ))
    }

    fn load_bound_terminal_baseline(
        &self,
        dataset: &DatasetState,
        contract_checksum: u64,
        terminal_evidence_checksum: u64,
    ) -> Result<(BTreeMap<FloatAggregateId, u32>, u64), StateError> {
        let online_claim = self.load_marker(
            ONLINE_CLAIM_FILE,
            ONLINE_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
        )?;
        let input = read_limited(
            &self.root.join(FLOAT_BASELINE_FILE),
            MAX_FLOAT_BASELINE_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
        )?;
        let (payload, baseline_checksum) = decode_artifact_and_checksum(
            &input,
            FLOAT_BASELINE_ARTIFACT,
            dataset,
            MAX_FLOAT_BASELINE_PAYLOAD_BYTES,
        )?;
        let inner =
            decode_bound_payload(payload, contract_checksum, online_claim, "FLOAT baseline")?;
        Ok((
            decode_terminal_float_baseline(inner, terminal_evidence_checksum)?,
            baseline_checksum,
        ))
    }

    fn load_terminal_crash_context(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<(u64, u64, u64), StateError> {
        let (contract_checksum, _, _, terminal_evidence_checksum) =
            self.load_bound_terminal_evidence(dataset, contract)?;
        let (_, baseline_checksum) = self.load_bound_terminal_baseline(
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
        )?;
        Ok((
            contract_checksum,
            terminal_evidence_checksum,
            baseline_checksum,
        ))
    }

    fn publish_terminal_crash_lifecycle_marker(
        &self,
        file: &str,
        artifact: &str,
        dataset: &DatasetState,
        contract_checksum: u64,
        terminal_evidence_checksum: u64,
        baseline_checksum: u64,
        predecessor_checksum: u64,
    ) -> Result<u64, StateError> {
        let inner =
            encode_terminal_crash_lifecycle_binding(terminal_evidence_checksum, baseline_checksum);
        let payload = encode_bound_payload(contract_checksum, predecessor_checksum, inner.as_str());
        let encoded = encode_artifact(artifact, dataset, &payload, MAX_MARKER_PAYLOAD_BYTES)?;
        atomic_publish_new(&self.root, file, encoded.as_bytes())?;
        self.load_terminal_crash_lifecycle_marker(
            file,
            artifact,
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
            baseline_checksum,
            predecessor_checksum,
        )
    }

    fn load_terminal_crash_lifecycle_marker(
        &self,
        file: &str,
        artifact: &str,
        dataset: &DatasetState,
        contract_checksum: u64,
        terminal_evidence_checksum: u64,
        baseline_checksum: u64,
        predecessor_checksum: u64,
    ) -> Result<u64, StateError> {
        let input = read_limited(
            &self.root.join(file),
            MAX_MARKER_PAYLOAD_BYTES as u64 + MAX_ARTIFACT_HEADER_BYTES,
        )?;
        let (payload, checksum) =
            decode_artifact_and_checksum(&input, artifact, dataset, MAX_MARKER_PAYLOAD_BYTES)?;
        let inner =
            decode_bound_payload(payload, contract_checksum, predecessor_checksum, artifact)?;
        decode_terminal_crash_lifecycle_binding(
            inner,
            terminal_evidence_checksum,
            baseline_checksum,
            artifact,
        )?;
        Ok(checksum)
    }

    fn load_complete_terminal_crash_lifecycle(
        &self,
        dataset: &DatasetState,
        contract_checksum: u64,
        terminal_evidence_checksum: u64,
        baseline_checksum: u64,
    ) -> Result<u64, StateError> {
        let mut predecessor_checksum = baseline_checksum;
        for (file, artifact) in crash_lifecycle_specs() {
            predecessor_checksum = self.load_terminal_crash_lifecycle_marker(
                file,
                artifact,
                dataset,
                contract_checksum,
                terminal_evidence_checksum,
                baseline_checksum,
                predecessor_checksum,
            )?;
        }
        Ok(predecessor_checksum)
    }

    fn ensure_lifecycle_tail_absent(
        &self,
        lifecycle_tail: &[(&str, &str)],
    ) -> Result<(), StateError> {
        for (file, _) in lifecycle_tail.iter().copied().chain([
            (RECOVERY_CLAIM_FILE, RECOVERY_CLAIM_ARTIFACT),
            (RECOVERY_RECEIPT_FILE, RECOVERY_RECEIPT_ARTIFACT),
            (
                DIAGNOSTIC_WARMUP_CLAIM_FILE,
                DIAGNOSTIC_WARMUP_CLAIM_ARTIFACT,
            ),
            (
                DIAGNOSTIC_WARMUP_RECEIPT_FILE,
                DIAGNOSTIC_WARMUP_RECEIPT_ARTIFACT,
            ),
            (
                DIAGNOSTIC_OBSERVATION_CLAIM_FILE,
                DIAGNOSTIC_OBSERVATION_CLAIM_ARTIFACT,
            ),
            (
                DIAGNOSTIC_OBSERVATION_RECEIPT_FILE,
                DIAGNOSTIC_OBSERVATION_RECEIPT_ARTIFACT,
            ),
        ]) {
            let path = self.root.join(file);
            match fs::symlink_metadata(&path) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(StateError::Io(error)),
                Ok(_) => {
                    return Err(StateError::Invalid(format!(
                        "lifecycle transition is repeated or has an orphan successor: {}",
                        path.display()
                    )));
                }
            }
        }
        Ok(())
    }

    fn load_terminal_diagnostic_warmup_claim(
        &self,
        dataset: &DatasetState,
        contract: &RunContract,
    ) -> Result<u64, StateError> {
        let contract_checksum = self.contract_checksum(dataset, contract)?;
        let (_, _, _, terminal_evidence_checksum) =
            self.load_bound_terminal_evidence(dataset, contract)?;
        let (_, baseline_checksum) = self.load_bound_terminal_baseline(
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
        )?;
        let restart_ready_checksum = self.load_complete_terminal_crash_lifecycle(
            dataset,
            contract_checksum,
            terminal_evidence_checksum,
            baseline_checksum,
        )?;
        let recovery_claim = self.load_marker(
            RECOVERY_CLAIM_FILE,
            RECOVERY_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            restart_ready_checksum,
        )?;
        let recovery_receipt = self.load_marker(
            RECOVERY_RECEIPT_FILE,
            RECOVERY_RECEIPT_ARTIFACT,
            dataset,
            contract_checksum,
            recovery_claim,
        )?;
        self.load_marker(
            DIAGNOSTIC_WARMUP_CLAIM_FILE,
            DIAGNOSTIC_WARMUP_CLAIM_ARTIFACT,
            dataset,
            contract_checksum,
            recovery_receipt,
        )
    }

    fn ensure_terminal_evidence_authority(&self) -> Result<(), StateError> {
        let path = self.root.join(LEDGER_FILE);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StateError::Io(error)),
            Ok(_) => Err(StateError::Invalid(format!(
                "legacy run ledger is incompatible with terminal-evidence authority: {}",
                path.display()
            ))),
        }
    }

    fn ensure_no_diagnostic_drift(&self) -> Result<(), StateError> {
        let path = self.root.join(DIAGNOSTIC_WARMUP_CLAIM_FILE);
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                Err(StateError::Invalid(
                    "database state is diagnostic-dirty; formal checks and ranking are closed"
                        .to_owned(),
                ))
            }
            Ok(_) => Err(StateError::Invalid(format!(
                "unsafe diagnostic state marker: {}",
                path.display()
            ))),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StateError::Io(error)),
        }
    }
}

pub fn attest_formal_state(
    root: &Path,
    expected_seed: u64,
    expected_contract: &RunContract,
) -> Result<FormalStateReceipt, StateError> {
    if !root.is_absolute() {
        return Err(StateError::Invalid(
            "formal state directory must be an absolute path".to_owned(),
        ));
    }
    #[cfg(not(unix))]
    {
        let _ = (root, expected_seed, expected_contract);
        return Err(StateError::Invalid(
            "formal-state attestation requires Unix descriptor semantics".to_owned(),
        ));
    }

    #[cfg(unix)]
    {
        expected_contract.validate_shape()?;

        let directory_lock = lock_readonly_formal_state_directory(root)?;
        let directory_metadata = directory_lock.0.metadata()?;
        let state_device = directory_metadata.dev();
        let state_inode = directory_metadata.ino();
        let initial_entries = formal_directory_snapshot(&directory_lock.0)?;
        validate_formal_directory_entries(&initial_entries)?;

        let mut total_bytes = 0_u64;
        let mut files = Vec::with_capacity(FORMAL_STATE_FILES_V2.len());
        for &(name, limit) in &FORMAL_STATE_FILES_V2 {
            ensure_formal_directory_snapshot_unchanged(&directory_lock.0, &initial_entries)?;
            let bytes = read_formal_file_at(&directory_lock.0, name, limit)?;
            total_bytes = total_bytes
                .checked_add(bytes.len() as u64)
                .ok_or_else(|| StateError::Invalid("formal state size overflow".to_owned()))?;
            if total_bytes > FORMAL_STATE_MAX_TOTAL_BYTES {
                return Err(StateError::Invalid(format!(
                    "formal state exceeds {FORMAL_STATE_MAX_TOTAL_BYTES} bytes"
                )));
            }
            files.push(FormalFileBytes { name, bytes });
            ensure_formal_directory_snapshot_unchanged(&directory_lock.0, &initial_entries)?;
        }

        for name in DIAGNOSTIC_STATE_FILES {
            if initial_entries
                .binary_search_by(|candidate| candidate.as_str().cmp(name))
                .is_ok()
            {
                ensure_formal_directory_snapshot_unchanged(&directory_lock.0, &initial_entries)?;
                validate_auxiliary_formal_entry_at(&directory_lock.0, name)?;
                ensure_formal_directory_snapshot_unchanged(&directory_lock.0, &initial_entries)?;
            }
        }
        ensure_formal_directory_snapshot_unchanged(&directory_lock.0, &initial_entries)?;
        validate_auxiliary_formal_entry_at(&directory_lock.0, DATABASE_IDENTITY_FILE)?;
        ensure_formal_directory_snapshot_unchanged(&directory_lock.0, &initial_entries)?;

        validate_formal_state_semantics(&files, expected_seed, expected_contract)?;
        ensure_formal_directory_snapshot_unchanged(&directory_lock.0, &initial_entries)?;
        validate_locked_directory_identity(root, &directory_lock.0)?;

        let terminal_evidence = formal_file(&files, TERMINAL_EVIDENCE_FILE)?;
        let terminal_evidence_sha256: [u8; 32] = Sha256::digest(terminal_evidence)
            .as_slice()
            .try_into()
            .map_err(|_| {
                StateError::Invalid(
                    "SHA-256 implementation returned a wrong digest size".to_owned(),
                )
            })?;
        let formal_chain_sha256 =
            formal_chain_sha256_v2(state_device, state_inode, files.as_slice())?;

        Ok(FormalStateReceipt {
            terminal_evidence_size: terminal_evidence.len() as u64,
            terminal_evidence_sha256,
            formal_chain_sha256,
            state_device,
            state_inode,
        })
    }
}

struct FormalFileBytes {
    name: &'static str,
    bytes: Vec<u8>,
}

fn formal_file<'a>(
    files: &'a [FormalFileBytes],
    expected_name: &str,
) -> Result<&'a [u8], StateError> {
    files
        .iter()
        .find(|file| file.name == expected_name)
        .map(|file| file.bytes.as_slice())
        .ok_or_else(|| {
            StateError::Invalid(format!(
                "formal state is missing required artifact {expected_name}"
            ))
        })
}

fn formal_utf8<'a>(
    files: &'a [FormalFileBytes],
    expected_name: &str,
) -> Result<&'a str, StateError> {
    std::str::from_utf8(formal_file(files, expected_name)?).map_err(|_| {
        StateError::Invalid(format!(
            "formal state artifact {expected_name} is not valid UTF-8"
        ))
    })
}

fn formal_chain_sha256_v2(
    state_device: u64,
    state_inode: u64,
    files: &[FormalFileBytes],
) -> Result<[u8; 32], StateError> {
    let count = u32::try_from(FORMAL_STATE_FILES_V2.len())
        .map_err(|_| StateError::Invalid("formal state file count overflow".to_owned()))?;
    let mut hasher = Sha256::new();
    hasher.update(FORMAL_STATE_DOMAIN_V2);
    hasher.update(state_device.to_be_bytes());
    hasher.update(state_inode.to_be_bytes());
    hasher.update(count.to_be_bytes());
    for &(name, _) in &FORMAL_STATE_FILES_V2 {
        if !name.is_ascii() {
            return Err(StateError::Invalid(
                "formal state contains a non-ASCII canonical name".to_owned(),
            ));
        }
        let name_len = u32::try_from(name.len())
            .map_err(|_| StateError::Invalid("formal state name length overflow".to_owned()))?;
        let content = formal_file(files, name)?;
        let content_len = u64::try_from(content.len())
            .map_err(|_| StateError::Invalid("formal state content length overflow".to_owned()))?;
        hasher.update(name_len.to_be_bytes());
        hasher.update(name.as_bytes());
        hasher.update(content_len.to_be_bytes());
        hasher.update(content);
        hasher.update(content_len.to_be_bytes());
    }
    hasher
        .finalize()
        .as_slice()
        .try_into()
        .map_err(|_| StateError::Invalid("SHA-256 implementation returned a wrong size".to_owned()))
}

fn validate_formal_state_semantics(
    files: &[FormalFileBytes],
    expected_seed: u64,
    expected_contract: &RunContract,
) -> Result<(), StateError> {
    let dataset = DatasetState::decode(formal_utf8(files, DATASET_FILE)?)?;
    if dataset.seed != expected_seed {
        return Err(StateError::Invalid(format!(
            "dataset seed mismatch: expected {expected_seed}, got {}",
            dataset.seed
        )));
    }
    expected_contract.validate(&dataset)?;

    let (intent, intent_checksum) = decode_setup_intent(formal_utf8(files, SETUP_INTENT_FILE)?)?;
    if intent.run_id != dataset.run_id
        || intent.seed != expected_seed
        || intent.contract != *expected_contract
    {
        return Err(StateError::Invalid(
            "setup intent does not bind the attested dataset, seed, and contract".to_owned(),
        ));
    }

    let (execution, execution_checksum) =
        decode_setup_execution(formal_utf8(files, SETUP_EXECUTION_FILE)?)?;
    if execution.run_id != dataset.run_id
        || execution.seed != expected_seed
        || execution.intent_checksum != intent_checksum
        || execution.contract != *expected_contract
    {
        return Err(StateError::Invalid(
            "setup execution does not bind the attested setup intent".to_owned(),
        ));
    }

    let (contract_payload, contract_checksum) = decode_artifact_and_checksum(
        formal_utf8(files, RUN_CONTRACT_FILE)?,
        RUN_CONTRACT_ARTIFACT,
        &dataset,
        MAX_CONTRACT_PAYLOAD_BYTES,
    )?;
    let stored_contract =
        decode_setup_bound_contract(contract_payload, intent_checksum, execution_checksum)?;
    stored_contract.validate(&dataset)?;
    if stored_contract != *expected_contract {
        return Err(StateError::Invalid(
            "stored run contract differs from the requested formal contract".to_owned(),
        ));
    }

    let setup_claim = decode_formal_marker(
        formal_utf8(files, SETUP_CHECK_CLAIM_FILE)?,
        SETUP_CHECK_CLAIM_ARTIFACT,
        &dataset,
        contract_checksum,
        contract_checksum,
    )?;
    let setup_receipt = decode_formal_marker(
        formal_utf8(files, SETUP_RECEIPT_FILE)?,
        SETUP_RECEIPT_ARTIFACT,
        &dataset,
        contract_checksum,
        setup_claim,
    )?;
    let rank_claim = decode_formal_marker(
        formal_utf8(files, RANK_CLAIM_FILE)?,
        RANK_CLAIM_ARTIFACT,
        &dataset,
        contract_checksum,
        setup_receipt,
    )?;

    let (terminal_payload, terminal_evidence_checksum) = decode_artifact_and_checksum(
        formal_utf8(files, TERMINAL_EVIDENCE_FILE)?,
        terminal_evidence_artifact(expected_contract.conformance),
        &dataset,
        MAX_TERMINAL_EVIDENCE_PAYLOAD_BYTES,
    )?;
    let terminal_inner = decode_bound_payload(
        terminal_payload,
        contract_checksum,
        rank_claim,
        "terminal evidence",
    )?;
    let _terminal_evidence = decode_persisted_terminal_evidence(&dataset, terminal_inner)?;

    let online_claim = decode_formal_marker(
        formal_utf8(files, ONLINE_CLAIM_FILE)?,
        ONLINE_CLAIM_ARTIFACT,
        &dataset,
        contract_checksum,
        terminal_evidence_checksum,
    )?;
    let (baseline_payload, baseline_checksum) = decode_artifact_and_checksum(
        formal_utf8(files, FLOAT_BASELINE_FILE)?,
        FLOAT_BASELINE_ARTIFACT,
        &dataset,
        MAX_FLOAT_BASELINE_PAYLOAD_BYTES,
    )?;
    let baseline_inner = decode_bound_payload(
        baseline_payload,
        contract_checksum,
        online_claim,
        "FLOAT baseline",
    )?;
    let _baseline = decode_terminal_float_baseline(baseline_inner, terminal_evidence_checksum)?;

    let mut predecessor_checksum = baseline_checksum;
    for (file, artifact) in crash_lifecycle_specs() {
        let (payload, checksum) = decode_artifact_and_checksum(
            formal_utf8(files, file)?,
            artifact,
            &dataset,
            MAX_MARKER_PAYLOAD_BYTES,
        )?;
        let inner =
            decode_bound_payload(payload, contract_checksum, predecessor_checksum, artifact)?;
        decode_terminal_crash_lifecycle_binding(
            inner,
            terminal_evidence_checksum,
            baseline_checksum,
            artifact,
        )?;
        predecessor_checksum = checksum;
    }

    let recovery_claim = decode_formal_marker(
        formal_utf8(files, RECOVERY_CLAIM_FILE)?,
        RECOVERY_CLAIM_ARTIFACT,
        &dataset,
        contract_checksum,
        predecessor_checksum,
    )?;
    decode_formal_marker(
        formal_utf8(files, RECOVERY_RECEIPT_FILE)?,
        RECOVERY_RECEIPT_ARTIFACT,
        &dataset,
        contract_checksum,
        recovery_claim,
    )?;
    Ok(())
}

fn decode_formal_marker(
    input: &str,
    artifact: &str,
    dataset: &DatasetState,
    contract_checksum: u64,
    predecessor_checksum: u64,
) -> Result<u64, StateError> {
    let (payload, checksum) =
        decode_artifact_and_checksum(input, artifact, dataset, MAX_MARKER_PAYLOAD_BYTES)?;
    let inner = decode_bound_payload(payload, contract_checksum, predecessor_checksum, artifact)?;
    if !inner.is_empty() {
        return Err(StateError::Invalid(format!(
            "{artifact} marker payload must be empty"
        )));
    }
    Ok(checksum)
}

#[derive(Debug, thiserror::Error)]
pub enum StateError {
    #[error("state I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid final2026 state: {0}")]
    Invalid(String),
}

#[derive(Debug, Eq, PartialEq)]
struct SetupIntent {
    run_id: String,
    seed: u64,
    contract: RunContract,
}

#[derive(Debug, Eq, PartialEq)]
struct SetupExecution {
    run_id: String,
    seed: u64,
    intent_checksum: u64,
    contract: RunContract,
}

fn encode_setup_intent(
    run_id: &str,
    seed: u64,
    contract: &RunContract,
) -> Result<String, StateError> {
    validate_run_id(run_id)?;
    contract.validate_shape()?;
    let payload = contract.encode();
    let metadata = format!(
        "artifact={SETUP_INTENT_ARTIFACT}\nversion={ARTIFACT_VERSION}\nrun_id={run_id}\nseed={seed}\nwarehouses={}\npayload_len={}\n",
        contract.warehouses,
        payload.len()
    );
    let checksum = checksum64(metadata.as_bytes(), payload.as_bytes());
    let encoded = format!("{metadata}checksum={checksum:016x}\n{payload}");
    if encoded.len() > MAX_SETUP_INTENT_BYTES {
        return Err(StateError::Invalid(format!(
            "setup claim exceeds {MAX_SETUP_INTENT_BYTES} bytes"
        )));
    }
    Ok(encoded)
}

fn decode_setup_intent(input: &str) -> Result<(SetupIntent, u64), StateError> {
    if input.len() > MAX_SETUP_INTENT_BYTES {
        return Err(StateError::Invalid(format!(
            "setup claim exceeds {MAX_SETUP_INTENT_BYTES} bytes"
        )));
    }
    let mut sections = input.splitn(8, '\n');
    let artifact = value(&mut sections, "artifact")?;
    if artifact != SETUP_INTENT_ARTIFACT {
        return Err(StateError::Invalid(format!(
            "expected {SETUP_INTENT_ARTIFACT} artifact, got {artifact:?}"
        )));
    }
    expect_exact(&mut sections, "version", ARTIFACT_VERSION)?;
    let run_id = value(&mut sections, "run_id")?.to_owned();
    validate_run_id(&run_id)?;
    let seed = parse(value(&mut sections, "seed")?, "setup seed")?;
    let warehouses = parse(value(&mut sections, "warehouses")?, "setup warehouses")?;
    let payload_len: usize = parse(value(&mut sections, "payload_len")?, "setup payload length")?;
    let checksum = parse_checksum(value(&mut sections, "checksum")?)?;
    let payload = sections
        .next()
        .ok_or_else(|| StateError::Invalid("setup claim is missing its contract".to_owned()))?;
    if payload_len != payload.len() || payload_len > MAX_CONTRACT_PAYLOAD_BYTES {
        return Err(StateError::Invalid(
            "setup claim contract length is invalid".to_owned(),
        ));
    }
    let metadata = format!(
        "artifact={SETUP_INTENT_ARTIFACT}\nversion={ARTIFACT_VERSION}\nrun_id={run_id}\nseed={seed}\nwarehouses={warehouses}\npayload_len={payload_len}\n"
    );
    if checksum64(metadata.as_bytes(), payload.as_bytes()) != checksum {
        return Err(StateError::Invalid(
            "setup claim checksum mismatch".to_owned(),
        ));
    }
    let contract = RunContract::decode(payload)?;
    contract.validate_shape()?;
    if contract.warehouses != warehouses {
        return Err(StateError::Invalid(
            "setup claim warehouse count does not match its contract".to_owned(),
        ));
    }
    let intent = SetupIntent {
        run_id,
        seed,
        contract,
    };
    if encode_setup_intent(&intent.run_id, intent.seed, &intent.contract)? != input {
        return Err(StateError::Invalid(
            "setup claim is not canonically encoded".to_owned(),
        ));
    }
    Ok((intent, checksum))
}

fn encode_setup_execution(
    run_id: &str,
    seed: u64,
    contract: &RunContract,
    intent_checksum: u64,
) -> Result<String, StateError> {
    validate_run_id(run_id)?;
    contract.validate_shape()?;
    let payload = format!(
        "setup_intent_checksum={intent_checksum:016x}\n{}",
        contract.encode()
    );
    let metadata = format!(
        "artifact={SETUP_EXECUTION_ARTIFACT}\nversion={ARTIFACT_VERSION}\nrun_id={run_id}\nseed={seed}\nwarehouses={}\npayload_len={}\n",
        contract.warehouses,
        payload.len()
    );
    let checksum = checksum64(metadata.as_bytes(), payload.as_bytes());
    let encoded = format!("{metadata}checksum={checksum:016x}\n{payload}");
    if encoded.len() > MAX_SETUP_INTENT_BYTES {
        return Err(StateError::Invalid(format!(
            "setup execution claim exceeds {MAX_SETUP_INTENT_BYTES} bytes"
        )));
    }
    Ok(encoded)
}

fn decode_setup_execution(input: &str) -> Result<(SetupExecution, u64), StateError> {
    if input.len() > MAX_SETUP_INTENT_BYTES {
        return Err(StateError::Invalid(format!(
            "setup execution claim exceeds {MAX_SETUP_INTENT_BYTES} bytes"
        )));
    }
    let mut sections = input.splitn(8, '\n');
    let artifact = value(&mut sections, "artifact")?;
    if artifact != SETUP_EXECUTION_ARTIFACT {
        return Err(StateError::Invalid(format!(
            "expected {SETUP_EXECUTION_ARTIFACT} artifact, got {artifact:?}"
        )));
    }
    expect_exact(&mut sections, "version", ARTIFACT_VERSION)?;
    let run_id = value(&mut sections, "run_id")?.to_owned();
    validate_run_id(&run_id)?;
    let seed = parse(value(&mut sections, "seed")?, "setup execution seed")?;
    let warehouses = parse(
        value(&mut sections, "warehouses")?,
        "setup execution warehouses",
    )?;
    let payload_len: usize = parse(
        value(&mut sections, "payload_len")?,
        "setup execution payload length",
    )?;
    let checksum = parse_checksum(value(&mut sections, "checksum")?)?;
    let payload = sections.next().ok_or_else(|| {
        StateError::Invalid("setup execution claim is missing its binding".to_owned())
    })?;
    if payload_len != payload.len() || payload_len > MAX_CONTRACT_PAYLOAD_BYTES {
        return Err(StateError::Invalid(
            "setup execution claim payload length is invalid".to_owned(),
        ));
    }
    let metadata = format!(
        "artifact={SETUP_EXECUTION_ARTIFACT}\nversion={ARTIFACT_VERSION}\nrun_id={run_id}\nseed={seed}\nwarehouses={warehouses}\npayload_len={payload_len}\n"
    );
    if checksum64(metadata.as_bytes(), payload.as_bytes()) != checksum {
        return Err(StateError::Invalid(
            "setup execution claim checksum mismatch".to_owned(),
        ));
    }
    let mut payload_sections = payload.splitn(2, '\n');
    let intent_checksum = parse_checksum(value(&mut payload_sections, "setup_intent_checksum")?)?;
    let encoded_contract = payload_sections.next().ok_or_else(|| {
        StateError::Invalid("setup execution claim is missing its contract".to_owned())
    })?;
    let contract = RunContract::decode(encoded_contract)?;
    contract.validate_shape()?;
    if contract.warehouses != warehouses {
        return Err(StateError::Invalid(
            "setup execution warehouse count does not match its contract".to_owned(),
        ));
    }
    let execution = SetupExecution {
        run_id,
        seed,
        intent_checksum,
        contract,
    };
    if encode_setup_execution(
        &execution.run_id,
        execution.seed,
        &execution.contract,
        execution.intent_checksum,
    )? != input
    {
        return Err(StateError::Invalid(
            "setup execution claim is not canonically encoded".to_owned(),
        ));
    }
    Ok((execution, checksum))
}

fn encode_setup_bound_contract(
    setup_checksum: u64,
    execution_checksum: u64,
    contract: &RunContract,
) -> String {
    format!(
        "setup_claim_checksum={setup_checksum:016x}\nsetup_execution_checksum={execution_checksum:016x}\n{}",
        contract.encode()
    )
}

fn decode_setup_bound_contract(
    payload: &str,
    expected_setup_checksum: u64,
    expected_execution_checksum: u64,
) -> Result<RunContract, StateError> {
    let mut sections = payload.splitn(3, '\n');
    let setup_checksum = parse_checksum(value(&mut sections, "setup_claim_checksum")?)?;
    if setup_checksum != expected_setup_checksum {
        return Err(StateError::Invalid(
            "run contract belongs to a different setup claim".to_owned(),
        ));
    }
    let execution_checksum = parse_checksum(value(&mut sections, "setup_execution_checksum")?)?;
    if execution_checksum != expected_execution_checksum {
        return Err(StateError::Invalid(
            "run contract belongs to a different setup execution claim".to_owned(),
        ));
    }
    let encoded_contract = sections
        .next()
        .ok_or_else(|| StateError::Invalid("run contract payload is missing".to_owned()))?;
    RunContract::decode(encoded_contract)
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

fn terminal_evidence_artifact(conformance: RunConformance) -> &'static str {
    match conformance {
        RunConformance::PublicSpecAligned => RANKED_TERMINAL_EVIDENCE_ARTIFACT,
        RunConformance::NonRankedDeviation => NON_RANKED_TERMINAL_EVIDENCE_ARTIFACT,
    }
}

fn terminal_artifact_binding(
    dataset: &DatasetState,
) -> Result<TerminalArtifactBinding, StateError> {
    dataset.validate_setup_evidence_binding()?;
    let warehouses = u16::try_from(dataset.warehouses).map_err(|_| {
        StateError::Invalid("dataset warehouse count does not fit terminal evidence".to_owned())
    })?;
    Ok(TerminalArtifactBinding::new(
        warehouses,
        dataset.seed,
        dataset.setup_evidence().load_seed,
    ))
}

fn terminal_evidence_generator(dataset: &DatasetState) -> Result<TpccDataGen, StateError> {
    dataset.validate_setup_evidence_binding()?;
    let setup = dataset.setup_evidence();
    let load_timestamp = String::from_utf8(setup.load_timestamp.clone())
        .map_err(|_| StateError::Invalid("setup load timestamp is not valid UTF-8".to_owned()))?;
    Ok(TpccDataGen::with_seed_and_timestamp(
        dataset.warehouses,
        setup.load_seed,
        load_timestamp,
    ))
}

fn decode_persisted_terminal_evidence(
    dataset: &DatasetState,
    input: &str,
) -> Result<PersistedTerminalEvidence, StateError> {
    let binding = terminal_artifact_binding(dataset)?;
    let history_generator = terminal_evidence_generator(dataset)?;
    let customer_generator = terminal_evidence_generator(dataset)?;
    let initial_history = |customer: CustomerKey| {
        history_generator
            .initial_history(
                customer.warehouse_id,
                customer.district_id,
                customer.customer_id,
            )
            .map(|history| {
                InitialHistoryRow::new(
                    history.h_date.into_bytes(),
                    (history.h_amount as f32).to_bits(),
                    history.h_data.into_bytes(),
                )
                .expect("validated setup generator produces bounded History roots")
            })
    };
    let initial_customer = |customer: CustomerKey| {
        customer_generator
            .initial_customer_profile(
                customer.warehouse_id,
                customer.district_id,
                customer.customer_id,
            )
            .map(|profile| {
                InitialCustomerData::new(*profile.credit(), profile.data().to_vec())
                    .expect("validated setup generator produces bounded Customer roots")
            })
    };
    decode_terminal_artifact_hex(input, binding, &initial_history, &initial_customer)
        .map_err(|error| StateError::Invalid(format!("invalid terminal evidence payload: {error}")))
}

fn crash_lifecycle_specs() -> [(&'static str, &'static str); 4] {
    [
        (CRASH_INTENT_FILE, CRASH_INTENT_ARTIFACT),
        (CRASH_KILLED_FILE, CRASH_KILLED_ARTIFACT),
        (RESTART_STARTED_FILE, RESTART_STARTED_ARTIFACT),
        (RESTART_READY_FILE, RESTART_READY_ARTIFACT),
    ]
}

fn crash_lifecycle_index(event: CrashLifecycleEvent) -> usize {
    match event {
        CrashLifecycleEvent::Intent => 0,
        CrashLifecycleEvent::Killed => 1,
        CrashLifecycleEvent::RestartStarted => 2,
        CrashLifecycleEvent::RestartReady => 3,
    }
}

fn encode_terminal_crash_lifecycle_binding(
    terminal_evidence_checksum: u64,
    baseline_checksum: u64,
) -> String {
    format!(
        "terminal_evidence_checksum={terminal_evidence_checksum:016x}\nbaseline_checksum={baseline_checksum:016x}\n"
    )
}

fn decode_terminal_crash_lifecycle_binding(
    input: &str,
    expected_terminal_evidence_checksum: u64,
    expected_baseline_checksum: u64,
    artifact: &str,
) -> Result<(), StateError> {
    if !input.ends_with('\n') {
        return Err(StateError::Invalid(format!(
            "{artifact} binding must end with a newline"
        )));
    }
    let mut lines = input.split_terminator('\n');
    let terminal_evidence_checksum =
        parse_checksum(value(&mut lines, "terminal_evidence_checksum")?)?;
    let baseline_checksum = parse_checksum(value(&mut lines, "baseline_checksum")?)?;
    if lines.next().is_some() {
        return Err(StateError::Invalid(format!(
            "{artifact} binding contains trailing fields"
        )));
    }
    if terminal_evidence_checksum != expected_terminal_evidence_checksum
        || baseline_checksum != expected_baseline_checksum
    {
        return Err(StateError::Invalid(format!(
            "{artifact} belongs to different terminal evidence or online baseline"
        )));
    }
    Ok(())
}

fn diagnostic_claim_spec(stage: DiagnosticStage) -> (&'static str, &'static str) {
    match stage {
        DiagnosticStage::Warmup => (
            DIAGNOSTIC_WARMUP_CLAIM_FILE,
            DIAGNOSTIC_WARMUP_CLAIM_ARTIFACT,
        ),
        DiagnosticStage::Observation => (
            DIAGNOSTIC_OBSERVATION_CLAIM_FILE,
            DIAGNOSTIC_OBSERVATION_CLAIM_ARTIFACT,
        ),
    }
}

fn diagnostic_receipt_spec(stage: DiagnosticStage) -> (&'static str, &'static str) {
    match stage {
        DiagnosticStage::Warmup => (
            DIAGNOSTIC_WARMUP_RECEIPT_FILE,
            DIAGNOSTIC_WARMUP_RECEIPT_ARTIFACT,
        ),
        DiagnosticStage::Observation => (
            DIAGNOSTIC_OBSERVATION_RECEIPT_FILE,
            DIAGNOSTIC_OBSERVATION_RECEIPT_ARTIFACT,
        ),
    }
}

fn encode_bound_payload(contract_checksum: u64, predecessor_checksum: u64, inner: &str) -> String {
    format!(
        "contract_checksum={contract_checksum:016x}\npredecessor_checksum={predecessor_checksum:016x}\n{inner}"
    )
}

fn decode_bound_payload<'a>(
    payload: &'a str,
    expected_contract_checksum: u64,
    expected_predecessor_checksum: u64,
    name: &str,
) -> Result<&'a str, StateError> {
    let mut sections = payload.splitn(3, '\n');
    let contract_checksum = parse_checksum(value(&mut sections, "contract_checksum")?)?;
    let predecessor_checksum = parse_checksum(value(&mut sections, "predecessor_checksum")?)?;
    let inner = sections
        .next()
        .ok_or_else(|| StateError::Invalid(format!("{name} is missing its bound payload")))?;
    if contract_checksum != expected_contract_checksum
        || predecessor_checksum != expected_predecessor_checksum
    {
        return Err(StateError::Invalid(format!(
            "{name} does not match its run contract or predecessor"
        )));
    }
    Ok(inner)
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

#[cfg(test)]
fn decode_artifact<'a>(
    input: &'a str,
    expected_artifact: &str,
    dataset: &DatasetState,
    payload_limit: usize,
) -> Result<&'a str, StateError> {
    decode_artifact_and_checksum(input, expected_artifact, dataset, payload_limit)
        .map(|(payload, _)| payload)
}

fn decode_artifact_and_checksum<'a>(
    input: &'a str,
    expected_artifact: &str,
    dataset: &DatasetState,
    payload_limit: usize,
) -> Result<(&'a str, u64), StateError> {
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
    let canonical_header = format!("{metadata}checksum={:016x}\n", header.checksum);
    let actual_header_len = input
        .len()
        .checked_sub(payload.len())
        .ok_or_else(|| StateError::Invalid("artifact header length underflow".to_owned()))?;
    if input.as_bytes().get(..actual_header_len) != Some(canonical_header.as_bytes()) {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} header is not canonical"
        )));
    }
    let actual_checksum = checksum64(metadata.as_bytes(), payload.as_bytes());
    if actual_checksum != header.checksum {
        return Err(StateError::Invalid(format!(
            "{expected_artifact} checksum mismatch"
        )));
    }
    Ok((payload, header.checksum))
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

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn parse_sha256(value: &str, name: &str) -> Result<[u8; 32], StateError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(StateError::Invalid(format!(
            "{name} must be 64 lower-case hexadecimal digits"
        )));
    }
    let mut checksum = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let encoded = std::str::from_utf8(pair)
            .map_err(|_| StateError::Invalid(format!("{name} is not ASCII")))?;
        checksum[index] = u8::from_str_radix(encoded, 16)
            .map_err(|_| StateError::Invalid(format!("{name} is not valid hexadecimal")))?;
    }
    Ok(checksum)
}

fn parse_accumulator_words(value: &str) -> Result<Vec<u64>, StateError> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    value
        .split(',')
        .map(|word| {
            if word.len() != 16
                || !word
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            {
                return Err(StateError::Invalid(
                    "order-line FLOAT accumulator words must be 16 lower-case hexadecimal digits"
                        .to_owned(),
                ));
            }
            u64::from_str_radix(word, 16).map_err(|_| {
                StateError::Invalid(
                    "order-line FLOAT accumulator word is not valid hexadecimal".to_owned(),
                )
            })
        })
        .collect()
}

fn encode_terminal_float_baseline(
    values: &BTreeMap<FloatAggregateId, u32>,
    terminal_evidence_checksum: u64,
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

    let mut payload = format!("terminal_evidence_checksum={terminal_evidence_checksum:016x}\n");
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

fn decode_terminal_float_baseline(
    payload: &str,
    terminal_evidence_checksum: u64,
) -> Result<BTreeMap<FloatAggregateId, u32>, StateError> {
    if !payload.ends_with('\n') {
        return Err(StateError::Invalid(
            "FLOAT baseline payload must end with a newline".to_owned(),
        ));
    }

    let mut lines = payload.split_terminator('\n');
    let encoded_terminal_evidence_checksum =
        parse_checksum(value(&mut lines, "terminal_evidence_checksum")?)?;
    if encoded_terminal_evidence_checksum != terminal_evidence_checksum {
        return Err(StateError::Invalid(
            "FLOAT baseline belongs to different terminal evidence".to_owned(),
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

#[cfg(unix)]
fn lock_readonly_formal_state_directory(root: &Path) -> Result<StateDirectoryLock, StateError> {
    let path_metadata = fs::symlink_metadata(root).map_err(|error| {
        if error.kind() == std::io::ErrorKind::NotFound {
            StateError::Invalid(format!(
                "formal state directory does not exist: {}",
                root.display()
            ))
        } else {
            StateError::Io(error)
        }
    })?;
    validate_real_directory(root, &path_metadata)?;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(root)?;
    directory.lock()?;
    validate_locked_directory_identity(root, &directory)?;
    Ok(StateDirectoryLock(directory))
}

#[cfg(unix)]
struct FormalDirectoryStream(*mut libc::DIR);

#[cfg(unix)]
impl Drop for FormalDirectoryStream {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0);
        }
    }
}

#[cfg(unix)]
fn formal_directory_snapshot(directory: &File) -> Result<Vec<String>, StateError> {
    let current_directory = CString::new(".").expect("static directory name contains no NUL");
    let duplicate = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            current_directory.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if duplicate < 0 {
        return Err(StateError::Io(std::io::Error::last_os_error()));
    }
    let stream = unsafe { libc::fdopendir(duplicate) };
    if stream.is_null() {
        let error = std::io::Error::last_os_error();
        unsafe {
            libc::close(duplicate);
        }
        return Err(StateError::Io(error));
    }
    let stream = FormalDirectoryStream(stream);
    let mut entries = Vec::new();
    loop {
        unsafe {
            *formal_errno_location() = 0;
        }
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            let errno = unsafe { *formal_errno_location() };
            if errno != 0 {
                return Err(StateError::Io(std::io::Error::from_raw_os_error(errno)));
            }
            break;
        }
        let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name == b"." || name == b".." {
            continue;
        }
        if !name.is_ascii() {
            return Err(StateError::Invalid(
                "formal state directory contains a non-ASCII entry".to_owned(),
            ));
        }
        let name = std::str::from_utf8(name)
            .expect("ASCII directory entry is valid UTF-8")
            .to_owned();
        entries.push(name);
    }
    entries.sort();
    Ok(entries)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
unsafe fn formal_errno_location() -> *mut libc::c_int {
    unsafe { libc::__errno_location() }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
))]
unsafe fn formal_errno_location() -> *mut libc::c_int {
    unsafe { libc::__error() }
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly"
    ))
))]
compile_error!("formal-state attestation needs a supported Unix errno accessor");

#[cfg(unix)]
fn validate_formal_directory_entries(entries: &[String]) -> Result<(), StateError> {
    for name in entries {
        if name == LEDGER_FILE || is_legacy_temporary_name(name) {
            return Err(StateError::Invalid(format!(
                "legacy run-ledger state is forbidden during formal attestation: {name}"
            )));
        }
        let is_formal = FORMAL_STATE_FILES_V2
            .iter()
            .any(|(candidate, _)| *candidate == name);
        let is_diagnostic = DIAGNOSTIC_STATE_FILES.contains(&name.as_str());
        if !is_formal && !is_diagnostic && name != DATABASE_IDENTITY_FILE {
            return Err(StateError::Invalid(format!(
                "unknown state entry is forbidden during formal attestation: {name}"
            )));
        }
    }
    for &(name, _) in &FORMAL_STATE_FILES_V2 {
        if entries
            .binary_search_by(|candidate| candidate.as_str().cmp(name))
            .is_err()
        {
            return Err(StateError::Invalid(format!(
                "formal state is missing required artifact {name}"
            )));
        }
    }
    if entries
        .binary_search_by(|candidate| candidate.as_str().cmp(DATABASE_IDENTITY_FILE))
        .is_err()
    {
        return Err(StateError::Invalid(format!(
            "formal state is missing required artifact {DATABASE_IDENTITY_FILE}"
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_formal_directory_snapshot_unchanged(
    directory: &File,
    expected: &[String],
) -> Result<(), StateError> {
    let actual = formal_directory_snapshot(directory)?;
    validate_formal_directory_entries(&actual)?;
    if actual != expected {
        return Err(StateError::Invalid(
            "formal state directory entries changed during attestation".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FormalFileIdentity {
    device: u64,
    inode: u64,
    links: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    size: u64,
    modified_seconds: i64,
    modified_nanoseconds: i64,
    changed_seconds: i64,
    changed_nanoseconds: i64,
}

#[cfg(unix)]
impl FormalFileIdentity {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
            links: metadata.nlink(),
            mode: metadata.mode(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            size: metadata.size(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        }
    }

    fn from_stat(stat: &libc::stat) -> Self {
        let (modified_seconds, modified_nanoseconds, changed_seconds, changed_nanoseconds) =
            formal_stat_times(stat);
        Self {
            device: stat.st_dev as u64,
            inode: stat.st_ino as u64,
            links: stat.st_nlink as u64,
            mode: stat.st_mode as u32,
            uid: stat.st_uid,
            gid: stat.st_gid,
            size: stat.st_size as u64,
            modified_seconds,
            modified_nanoseconds,
            changed_seconds,
            changed_nanoseconds,
        }
    }

    fn validate_regular(self, name: &str, limit: Option<u64>) -> Result<(), StateError> {
        if self.mode & libc::S_IFMT as u32 != libc::S_IFREG as u32 {
            return Err(StateError::Invalid(format!(
                "formal state entry is not a regular file: {name}"
            )));
        }
        if self.size == 0 {
            return Err(StateError::Invalid(format!(
                "formal state entry is empty: {name}"
            )));
        }
        if let Some(limit) = limit {
            if self.size > limit {
                return Err(StateError::Invalid(format!(
                    "formal state artifact {name} exceeds {limit} bytes"
                )));
            }
        }
        Ok(())
    }
}

fn formal_stat_times(stat: &libc::stat) -> (i64, i64, i64, i64) {
    (
        stat.st_mtime as i64,
        stat.st_mtime_nsec as i64,
        stat.st_ctime as i64,
        stat.st_ctime_nsec as i64,
    )
}

#[cfg(unix)]
fn formal_entry_identity_at(
    directory: &File,
    name: &str,
) -> Result<FormalFileIdentity, StateError> {
    let name = CString::new(name)
        .map_err(|_| StateError::Invalid("formal state name contains NUL".to_owned()))?;
    let mut stat = std::mem::MaybeUninit::<libc::stat>::zeroed();
    let result = unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            name.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result != 0 {
        return Err(StateError::Io(std::io::Error::last_os_error()));
    }
    Ok(FormalFileIdentity::from_stat(unsafe {
        &stat.assume_init()
    }))
}

#[cfg(unix)]
fn open_formal_entry_at(directory: &File, name: &str) -> Result<File, StateError> {
    let name = CString::new(name)
        .map_err(|_| StateError::Invalid("formal state name contains NUL".to_owned()))?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if descriptor < 0 {
        return Err(StateError::Io(std::io::Error::last_os_error()));
    }
    let descriptor = unsafe { OwnedFd::from_raw_fd(descriptor) };
    Ok(File::from(descriptor))
}

#[cfg(unix)]
fn read_formal_file_at(directory: &File, name: &str, limit: u64) -> Result<Vec<u8>, StateError> {
    let before = formal_entry_identity_at(directory, name)?;
    before.validate_regular(name, Some(limit))?;
    let mut file = open_formal_entry_at(directory, name)?;
    let opened = FormalFileIdentity::from_metadata(&file.metadata()?);
    opened.validate_regular(name, Some(limit))?;
    if opened != before {
        return Err(StateError::Invalid(format!(
            "formal state artifact changed while opening: {name}"
        )));
    }

    let capacity = usize::try_from(before.size)
        .map_err(|_| StateError::Invalid(format!("formal state artifact is too large: {name}")))?;
    let read_limit = limit
        .checked_add(1)
        .ok_or_else(|| StateError::Invalid("formal state read limit overflow".to_owned()))?;
    let mut bytes = Vec::with_capacity(capacity);
    (&mut file).take(read_limit).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != before.size || bytes.len() as u64 > limit {
        return Err(StateError::Invalid(format!(
            "formal state artifact changed while reading: {name}"
        )));
    }

    let after_open = FormalFileIdentity::from_metadata(&file.metadata()?);
    let after_path = formal_entry_identity_at(directory, name)?;
    if after_open != before || after_path != before {
        return Err(StateError::Invalid(format!(
            "formal state artifact identity changed while reading: {name}"
        )));
    }
    Ok(bytes)
}

#[cfg(unix)]
fn validate_auxiliary_formal_entry_at(directory: &File, name: &str) -> Result<(), StateError> {
    let before = formal_entry_identity_at(directory, name)?;
    before.validate_regular(name, Some(FORMAL_STATE_MAX_TOTAL_BYTES))?;
    let file = open_formal_entry_at(directory, name)?;
    let opened = FormalFileIdentity::from_metadata(&file.metadata()?);
    let after_path = formal_entry_identity_at(directory, name)?;
    if opened != before || after_path != before {
        return Err(StateError::Invalid(format!(
            "optional diagnostic entry changed while opening: {name}"
        )));
    }
    Ok(())
}

struct StateDirectoryLock(File);

impl Drop for StateDirectoryLock {
    fn drop(&mut self) {
        let _ = self.0.unlock();
    }
}

/// Serialize every dependent read/claim/publication sequence on the state
/// directory itself. Directory locks are released by the kernel on process
/// exit, so a crash cannot leave a permanent lock artifact.
fn lock_state_directory(root: &Path) -> Result<StateDirectoryLock, StateError> {
    let directory = File::open(root)?;
    directory.lock()?;
    validate_locked_directory_identity(root, &directory)?;
    Ok(StateDirectoryLock(directory))
}

fn lock_clean_terminal_state_directory(root: &Path) -> Result<StateDirectoryLock, StateError> {
    lock_clean_terminal_state_directory_with_hook(root, || Ok(()))
}

fn lock_clean_terminal_state_directory_with_hook(
    root: &Path,
    after_snapshot: impl FnOnce() -> Result<(), StateError>,
) -> Result<StateDirectoryLock, StateError> {
    let directory_lock = lock_state_directory(root)?;
    let entries = state_directory_snapshot(root)?;
    ensure_snapshot_legacy_state_absent(&entries)?;
    let temporaries = validate_orphan_temporary_snapshot(root, &entries)?;
    after_snapshot()?;
    cleanup_orphan_temporary_batch_with_fault(root, &directory_lock.0, &temporaries, true, |_| {
        Ok(())
    })?;
    ensure_legacy_state_absent(root)?;
    validate_locked_directory_identity(root, &directory_lock.0)?;
    Ok(directory_lock)
}

fn ensure_snapshot_legacy_state_absent(entries: &[PathBuf]) -> Result<(), StateError> {
    for path in entries {
        let Some(file_name) = path.file_name() else {
            continue;
        };
        if file_name == LEDGER_FILE || file_name.to_str().is_some_and(is_legacy_temporary_name) {
            return Err(StateError::Invalid(format!(
                "legacy run-ledger state is incompatible with terminal-evidence authority: {}",
                path.display()
            )));
        }
    }
    Ok(())
}

fn ensure_legacy_state_absent(root: &Path) -> Result<(), StateError> {
    let legacy = root.join(LEDGER_FILE);
    match fs::symlink_metadata(&legacy) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(StateError::Io(error)),
        Ok(_) => {
            return Err(StateError::Invalid(format!(
                "legacy run ledger is incompatible with terminal-evidence authority: {}",
                legacy.display()
            )));
        }
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let Some(file_name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if is_legacy_temporary_name(&file_name) {
            return Err(StateError::Invalid(format!(
                "legacy run-ledger publication temporary is incompatible with terminal-evidence authority: {}",
                entry.path().display()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
fn cleanup_orphan_temporary_files(root: &Path, directory: &File) -> Result<(), StateError> {
    cleanup_orphan_temporary_files_with_fault(root, directory, |_| Ok(()))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OrphanCleanupStep {
    BeforeTargetSync,
    BeforeCleanupSync,
}

struct OrphanTemporary {
    path: PathBuf,
    linked_to_target: bool,
    metadata: fs::Metadata,
}

#[cfg(test)]
fn cleanup_orphan_temporary_files_with_fault(
    root: &Path,
    directory: &File,
    inject_fault: impl FnMut(OrphanCleanupStep) -> std::io::Result<()>,
) -> Result<(), StateError> {
    let entries = state_directory_snapshot(root)?;
    let temporaries = validate_orphan_temporary_snapshot(root, &entries)?;
    cleanup_orphan_temporary_batch_with_fault(root, directory, &temporaries, false, inject_fault)
}

fn state_directory_snapshot(root: &Path) -> Result<Vec<PathBuf>, StateError> {
    let mut entries = fs::read_dir(root)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;
    entries.sort_by(|left, right| left.file_name().cmp(&right.file_name()));
    Ok(entries)
}

fn validate_orphan_temporary_snapshot(
    root: &Path,
    entries: &[PathBuf],
) -> Result<Vec<OrphanTemporary>, StateError> {
    let mut temporaries = Vec::new();
    for entry in entries {
        let Some(file_name) = entry.file_name() else {
            continue;
        };
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(target_name) = temporary_target_name(file_name) else {
            continue;
        };

        let temporary = entry;
        let temporary_metadata = fs::symlink_metadata(&temporary)?;
        if temporary_metadata.file_type().is_symlink() || !temporary_metadata.is_file() {
            return Err(StateError::Invalid(format!(
                "orphan publication temporary is not a real file: {}",
                temporary.display()
            )));
        }
        validate_temporary_owner(root, &temporary, &temporary_metadata)?;

        let target = root.join(target_name);
        let linked_to_target = match fs::symlink_metadata(&target) {
            Ok(target_metadata) => {
                if target_metadata.file_type().is_symlink() || !target_metadata.is_file() {
                    return Err(StateError::Invalid(format!(
                        "orphan publication target is not a real file: {}",
                        target.display()
                    )));
                }
                validate_temporary_hard_link(
                    &temporary,
                    &temporary_metadata,
                    &target,
                    &target_metadata,
                )?;
                true
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                validate_unlinked_temporary(&temporary, &temporary_metadata)?;
                false
            }
            Err(error) => return Err(StateError::Io(error)),
        };
        temporaries.push(OrphanTemporary {
            path: temporary.to_path_buf(),
            linked_to_target,
            metadata: temporary_metadata,
        });
    }
    Ok(temporaries)
}

fn cleanup_orphan_temporary_batch_with_fault(
    root: &Path,
    directory: &File,
    temporaries: &[OrphanTemporary],
    reject_legacy: bool,
    mut inject_fault: impl FnMut(OrphanCleanupStep) -> std::io::Result<()>,
) -> Result<(), StateError> {
    validate_cleanup_batch_before_mutation(root, directory, temporaries, reject_legacy)?;
    if temporaries
        .iter()
        .any(|temporary| temporary.linked_to_target)
    {
        inject_fault(OrphanCleanupStep::BeforeTargetSync)?;
        directory.sync_all()?;
    }
    for (index, temporary) in temporaries.iter().enumerate() {
        // Atomic publishers serialize on this directory lock. Revalidate all
        // remaining captured entries at the actual unlink boundary so every
        // cooperating writer either precedes or follows this cleanup batch.
        // A raw writer that deliberately ignores flock can always race a
        // pathname operation and is outside the supported publication model.
        validate_cleanup_batch_before_mutation(
            root,
            directory,
            &temporaries[index..],
            reject_legacy,
        )?;
        fs::remove_file(&temporary.path)?;
    }
    if !temporaries.is_empty() {
        inject_fault(OrphanCleanupStep::BeforeCleanupSync)?;
        directory.sync_all()?;
    }
    Ok(())
}

fn validate_cleanup_batch_before_mutation(
    root: &Path,
    directory: &File,
    captured: &[OrphanTemporary],
    reject_legacy: bool,
) -> Result<(), StateError> {
    validate_locked_directory_identity(root, directory)?;
    if reject_legacy {
        ensure_legacy_state_absent(root)?;
    }
    let paths = captured
        .iter()
        .map(|temporary| temporary.path.clone())
        .collect::<Vec<_>>();
    let refreshed = validate_orphan_temporary_snapshot(root, &paths)?;
    if refreshed.len() != captured.len()
        || refreshed.iter().zip(captured).any(|(current, prior)| {
            current.path != prior.path
                || current.linked_to_target != prior.linked_to_target
                || !same_file_identity(&current.metadata, &prior.metadata)
        })
    {
        return Err(StateError::Invalid(
            "orphan publication batch changed before cleanup".to_owned(),
        ));
    }
    if reject_legacy {
        ensure_legacy_state_absent(root)?;
    }
    validate_locked_directory_identity(root, directory)
}

#[cfg(unix)]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;

    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.nlink() == right.nlink()
        && left.len() == right.len()
}

#[cfg(not(unix))]
fn same_file_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
        && left.created().ok() == right.created().ok()
        && left.modified().ok() == right.modified().ok()
}

#[cfg(unix)]
fn validate_locked_directory_identity(root: &Path, directory: &File) -> Result<(), StateError> {
    use std::os::unix::fs::MetadataExt;

    let path_metadata = fs::symlink_metadata(root)?;
    validate_real_directory(root, &path_metadata)?;
    let locked_metadata = directory.metadata()?;
    if !locked_metadata.is_dir()
        || locked_metadata.dev() != path_metadata.dev()
        || locked_metadata.ino() != path_metadata.ino()
    {
        return Err(StateError::Invalid(format!(
            "state path changed while locking: {}",
            root.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_locked_directory_identity(root: &Path, directory: &File) -> Result<(), StateError> {
    let path_metadata = fs::symlink_metadata(root)?;
    validate_real_directory(root, &path_metadata)?;
    if !directory.metadata()?.is_dir() {
        return Err(StateError::Invalid(format!(
            "state path changed while locking: {}",
            root.display()
        )));
    }
    Ok(())
}

fn temporary_target_name(file_name: &str) -> Option<&str> {
    let body = file_name.strip_prefix('.')?.strip_suffix(".tmp")?;
    let (target_and_process, counter) = body.rsplit_once('.')?;
    let (target, process) = target_and_process.rsplit_once('.')?;
    if !canonical_decimal(process)
        || !canonical_decimal(counter)
        || !STATE_ARTIFACT_FILES.contains(&target)
    {
        return None;
    }
    Some(target)
}

fn is_legacy_temporary_name(file_name: &str) -> bool {
    let Some(body) = file_name
        .strip_prefix(".run_ledger.state.")
        .and_then(|body| body.strip_suffix(".tmp"))
    else {
        return false;
    };
    let Some((process, counter)) = body.split_once('.') else {
        return false;
    };
    !counter.contains('.') && canonical_decimal(process) && canonical_decimal(counter)
}

fn canonical_decimal(value: &str) -> bool {
    !value.is_empty()
        && value.bytes().all(|byte| byte.is_ascii_digit())
        && (value == "0" || !value.starts_with('0'))
}

#[cfg(unix)]
fn validate_temporary_owner(
    root: &Path,
    temporary: &Path,
    metadata: &fs::Metadata,
) -> Result<(), StateError> {
    use std::os::unix::fs::MetadataExt;

    if metadata.uid() != fs::symlink_metadata(root)?.uid() {
        return Err(StateError::Invalid(format!(
            "orphan publication temporary has a foreign owner: {}",
            temporary.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_temporary_owner(
    _root: &Path,
    _temporary: &Path,
    _metadata: &fs::Metadata,
) -> Result<(), StateError> {
    Ok(())
}

#[cfg(unix)]
fn validate_temporary_hard_link(
    temporary: &Path,
    temporary_metadata: &fs::Metadata,
    target: &Path,
    target_metadata: &fs::Metadata,
) -> Result<(), StateError> {
    use std::os::unix::fs::MetadataExt;

    if temporary_metadata.dev() != target_metadata.dev()
        || temporary_metadata.ino() != target_metadata.ino()
        || temporary_metadata.nlink() != 2
        || target_metadata.nlink() != 2
    {
        return Err(StateError::Invalid(format!(
            "orphan publication temporary is not the target's sole hard link: {} -> {}",
            temporary.display(),
            target.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_temporary_hard_link(
    temporary: &Path,
    _temporary_metadata: &fs::Metadata,
    target: &Path,
    _target_metadata: &fs::Metadata,
) -> Result<(), StateError> {
    Err(StateError::Invalid(format!(
        "cannot safely recover a linked publication temporary on this platform: {} -> {}",
        temporary.display(),
        target.display()
    )))
}

#[cfg(unix)]
fn validate_unlinked_temporary(
    temporary: &Path,
    metadata: &fs::Metadata,
) -> Result<(), StateError> {
    use std::os::unix::fs::MetadataExt;

    if metadata.nlink() != 1 {
        return Err(StateError::Invalid(format!(
            "orphan publication temporary has unexpected hard links: {}",
            temporary.display()
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn validate_unlinked_temporary(
    _temporary: &Path,
    _metadata: &fs::Metadata,
) -> Result<(), StateError> {
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AtomicPublishStep {
    FirstDirectorySync,
    TemporaryUnlink,
    SecondDirectorySync,
}

fn atomic_publish_new(root: &Path, name: &str, bytes: &[u8]) -> Result<(), StateError> {
    atomic_publish_new_with_fault(root, name, bytes, |_| Ok(()))
}

fn atomic_publish_new_with_fault(
    root: &Path,
    name: &str,
    bytes: &[u8],
    mut inject_fault: impl FnMut(AtomicPublishStep) -> std::io::Result<()>,
) -> Result<(), StateError> {
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
    let temporary = root.join(format!(
        ".{name}.{}.{}.tmp",
        std::process::id(),
        NEXT_TEMPORARY_FILE.fetch_add(1, Ordering::Relaxed)
    ));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let mut target_linked = false;
    if let Err(error) = (|| -> std::io::Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        match fs::hard_link(&temporary, &target) {
            Ok(()) => target_linked = true,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("state artifact is write-once: {}", target.display()),
                ));
            }
            Err(error) => return Err(error),
        }
        inject_fault(AtomicPublishStep::FirstDirectorySync)?;
        File::open(root)?.sync_all()
    })() {
        drop(file);
        let cleanup = rollback_atomic_publish(root, &temporary, &target, target_linked);
        if let Err(cleanup_error) = cleanup {
            return Err(StateError::Invalid(format!(
                "publication failed ({error}); rollback also failed ({cleanup_error})"
            )));
        }
        return Err(StateError::Io(error));
    }
    drop(file);

    if let Err(error) = (|| -> std::io::Result<()> {
        inject_fault(AtomicPublishStep::TemporaryUnlink)?;
        fs::remove_file(&temporary)?;
        inject_fault(AtomicPublishStep::SecondDirectorySync)?;
        File::open(root)?.sync_all()
    })() {
        return match finish_committed_publish(root, &temporary) {
            Ok(()) => Ok(()),
            Err(cleanup_error) => Err(StateError::Invalid(format!(
                "state artifact was published durably, but cleanup failed ({error}); cleanup recovery also failed ({cleanup_error})"
            ))),
        };
    }
    Ok(())
}

fn finish_committed_publish(root: &Path, temporary: &Path) -> std::io::Result<()> {
    match fs::remove_file(temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    File::open(root)?.sync_all()
}

fn rollback_atomic_publish(
    root: &Path,
    temporary: &Path,
    target: &Path,
    target_linked: bool,
) -> std::io::Result<()> {
    if target_linked {
        match fs::remove_file(target) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    match fs::remove_file(temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    File::open(root)?.sync_all()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::ranking::evidence_collector::StockKey;
    use crate::ranking::preflight::PreparedPathPreflightProof;
    use crate::ranking::runner::StockVersion;
    use crate::ranking::terminal_evidence::TerminalEvidenceCollector;
    use crate::sample_evidence::setup_evidence_fixture;

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
        sample_dataset_with_warehouses(run_id, seed, 1)
    }

    fn sample_dataset_with_warehouses(run_id: &str, seed: u64, warehouses: i32) -> DatasetState {
        let mut order_line_amounts = NonNegativeF32Accumulator::default();
        order_line_amounts
            .add_repeated_bits(1.0_f32.to_bits(), 7)
            .unwrap();
        let load = LoadSummary {
            order_line_rows: 7,
            undelivered_order_line_rows: 7,
            order_line_amounts,
            partitions: (1..=warehouses)
                .flat_map(|warehouse_id| {
                    (1..=10).map(move |district_id| PartitionLoadSummary {
                        warehouse_id,
                        district_id,
                        order_line_rows: if warehouse_id == 1 && district_id == 1 {
                            7
                        } else {
                            0
                        },
                        undelivered_order_line_rows: if warehouse_id == 1 && district_id == 1 {
                            7
                        } else {
                            0
                        },
                    })
                })
                .collect(),
            setup_evidence: setup_evidence_fixture(warehouses, seed),
        };
        DatasetState::from_load(run_id.to_owned(), seed, warehouses, load).unwrap()
    }

    fn sample_contract(warehouses: u16) -> RunContract {
        RunContract {
            warehouses,
            clients: if warehouses == OFFICIAL_WAREHOUSES {
                OFFICIAL_CLIENTS
            } else {
                1
            },
            warmup_seconds: if warehouses == OFFICIAL_WAREHOUSES {
                WARMUP_SECONDS
            } else {
                0
            },
            measurement_windows: MEASUREMENT_WINDOWS,
            window_seconds: if warehouses == OFFICIAL_WAREHOUSES {
                MEASUREMENT_SECONDS
            } else {
                1
            },
            load_budget_seconds: LOAD_BUDGET_SECONDS,
            recovery_ready_budget_seconds: RECOVERY_READY_BUDGET_SECONDS,
            response_timeout_seconds: 30,
            phase_tail_grace_seconds: 5,
            conformance: if warehouses == OFFICIAL_WAREHOUSES {
                RunConformance::PublicSpecAligned
            } else {
                RunConformance::NonRankedDeviation
            },
        }
    }

    fn initialize_checked_run(store: &StateStore, dataset: &DatasetState, contract: &RunContract) {
        store.initialize_run(dataset, contract).unwrap();
        let setup = store.begin_setup_check(dataset, contract).unwrap();
        store
            .complete_setup_check(dataset, contract, setup)
            .unwrap();
    }

    fn initialize_online_run(store: &StateStore, dataset: &DatasetState, contract: &RunContract) {
        initialize_checked_run(store, dataset, contract);
        let rank = store.begin_rank(dataset, contract).unwrap();
        let evidence = sample_terminal_evidence(dataset);
        store
            .complete_rank_with_terminal_evidence(dataset, contract, rank, &evidence)
            .unwrap();
        let (online, _) = store
            .begin_online_check_from_terminal_evidence(dataset, contract)
            .unwrap();
        store
            .complete_online_check_from_terminal_evidence(
                dataset,
                contract,
                online,
                &sample_float_baseline(),
            )
            .unwrap();
    }

    fn initialize_formal_run(directory: &TestDirectory) -> (DatasetState, RunContract) {
        initialize_formal_run_with_shape(
            directory,
            "run-formal-attestation",
            0x7a11_ce55,
            OFFICIAL_WAREHOUSES,
        )
    }

    fn initialize_formal_run_with_shape(
        directory: &TestDirectory,
        run_id: &str,
        seed: u64,
        warehouses: u16,
    ) -> (DatasetState, RunContract) {
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset_with_warehouses(run_id, seed, i32::from(warehouses));
        let contract = sample_contract(warehouses);
        initialize_online_run(&store, &dataset, &contract);
        for event in [
            CrashLifecycleEvent::Intent,
            CrashLifecycleEvent::Killed,
            CrashLifecycleEvent::RestartStarted,
            CrashLifecycleEvent::RestartReady,
        ] {
            store
                .record_crash_lifecycle_from_terminal_evidence(&dataset, &contract, event)
                .unwrap();
        }
        let (recovery, _, _) = store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .unwrap();
        store
            .complete_recovery_check_from_terminal_evidence(&dataset, &contract, recovery)
            .unwrap();
        fs::write(
            directory.0.join(DATABASE_IDENTITY_FILE),
            b"database-identity-v2\n",
        )
        .unwrap();
        (dataset, contract)
    }

    fn copy_state_directory(source: &Path, target: &Path) {
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
        }
    }

    fn sample_float_baseline() -> BTreeMap<FloatAggregateId, u32> {
        FLOAT_AGGREGATES
            .iter()
            .enumerate()
            .map(|(index, spec)| (spec.id, 0x3f80_0000 + index as u32))
            .collect()
    }

    fn sample_terminal_evidence(dataset: &DatasetState) -> SealedTerminalEvidence {
        let warehouses = u16::try_from(dataset.warehouses).unwrap();
        let history_generator = terminal_evidence_generator(dataset).unwrap();
        let customer_generator = terminal_evidence_generator(dataset).unwrap();
        let initial_history = move |customer: CustomerKey| {
            history_generator
                .initial_history(
                    customer.warehouse_id,
                    customer.district_id,
                    customer.customer_id,
                )
                .map(|history| {
                    InitialHistoryRow::new(
                        history.h_date.into_bytes(),
                        (history.h_amount as f32).to_bits(),
                        history.h_data.into_bytes(),
                    )
                    .unwrap()
                })
        };
        let initial_customer = move |customer: CustomerKey| {
            customer_generator
                .initial_customer_profile(
                    customer.warehouse_id,
                    customer.district_id,
                    customer.customer_id,
                )
                .map(|profile| {
                    InitialCustomerData::new(*profile.credit(), profile.data().to_vec()).unwrap()
                })
        };
        let stock_roots = |_stock: StockKey| {
            Some(StockVersion {
                quantity: 50,
                ytd_bits: 0.0_f32.to_bits(),
                order_count: 0,
                remote_count: 0,
            })
        };
        let collector = TerminalEvidenceCollector::new(
            warehouses,
            1,
            dataset.seed,
            stock_roots,
            initial_history,
            initial_customer,
            PreparedPathPreflightProof::verified_for_test(dataset.seed, warehouses),
        )
        .unwrap();
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                collector.worker_finished(0).await.unwrap();
                collector.seal().await.unwrap()
            })
    }

    #[test]
    fn formal_state_v2_matches_the_cross_language_golden_receipt() {
        let files = FORMAL_STATE_FILES_V2
            .iter()
            .map(|(name, _)| FormalFileBytes {
                name,
                bytes: format!("{name}\n").into_bytes(),
            })
            .collect::<Vec<_>>();
        let encoded_len = FORMAL_STATE_DOMAIN_V2.len()
            + 8
            + 8
            + 4
            + files
                .iter()
                .map(|file| 4 + file.name.len() + 8 + file.bytes.len() + 8)
                .sum::<usize>();
        assert_eq!(encoded_len, 930);

        let formal_digest = formal_chain_sha256_v2(1, 2, &files).unwrap();
        assert_eq!(
            lowercase_sha256(formal_digest),
            "95293d5ca572b8805ff6cd84c361f8d4b90b163848e03b76c894ad64756b4aae"
        );
        let terminal = formal_file(&files, TERMINAL_EVIDENCE_FILE).unwrap();
        assert_eq!(terminal.len(), 24);
        let terminal_digest: [u8; 32] = Sha256::digest(terminal).into();
        assert_eq!(
            lowercase_sha256(terminal_digest),
            "4e001b1a88a1fe5db74c8f13c88dcb3d4d83ea85259927b3033121ab03674b5f"
        );

        let receipt = FormalStateReceipt {
            terminal_evidence_size: terminal.len() as u64,
            terminal_evidence_sha256: terminal_digest,
            formal_chain_sha256: formal_digest,
            state_device: 1,
            state_inode: 2,
        }
        .to_string();
        assert!(receipt.is_ascii());
        assert_eq!(
            receipt,
            "FORMAL_STATE_V2 terminal_evidence_size=24 \
             terminal_evidence_sha256=4e001b1a88a1fe5db74c8f13c88dcb3d4d83ea85259927b3033121ab03674b5f \
             formal_chain_sha256=95293d5ca572b8805ff6cd84c361f8d4b90b163848e03b76c894ad64756b4aae \
             state_device=1 state_inode=2"
        );
    }

    #[cfg(unix)]
    #[test]
    fn formal_attestation_is_read_only_and_diagnostics_do_not_rank() {
        let directory = TestDirectory::new();
        let (dataset, contract) = initialize_formal_run(&directory);
        let before = FORMAL_STATE_FILES_V2
            .iter()
            .map(|(name, _)| (*name, fs::read(directory.0.join(name)).unwrap()))
            .collect::<BTreeMap<_, _>>();

        let baseline = attest_formal_state(&directory.0, dataset.seed, &contract).unwrap();
        assert!(baseline.to_string().is_ascii());
        assert_eq!(
            baseline.terminal_evidence_size,
            fs::metadata(directory.0.join(TERMINAL_EVIDENCE_FILE))
                .unwrap()
                .len()
        );
        assert_eq!(
            before,
            FORMAL_STATE_FILES_V2
                .iter()
                .map(|(name, _)| (*name, fs::read(directory.0.join(name)).unwrap()))
                .collect()
        );

        fs::write(
            directory.0.join(DIAGNOSTIC_WARMUP_CLAIM_FILE),
            b"diagnostic process failed after its claim\n",
        )
        .unwrap();
        assert_eq!(
            attest_formal_state(&directory.0, dataset.seed, &contract).unwrap(),
            baseline
        );
        for name in [
            DIAGNOSTIC_WARMUP_RECEIPT_FILE,
            DIAGNOSTIC_OBSERVATION_CLAIM_FILE,
            DIAGNOSTIC_OBSERVATION_RECEIPT_FILE,
        ] {
            fs::write(directory.0.join(name), format!("{name}\n")).unwrap();
        }
        assert_eq!(
            attest_formal_state(&directory.0, dataset.seed, &contract).unwrap(),
            baseline
        );

        fs::write(
            directory.0.join(DATABASE_IDENTITY_FILE),
            b"changed-but-valid-database-identity\n",
        )
        .unwrap();
        assert_eq!(
            attest_formal_state(&directory.0, dataset.seed, &contract).unwrap(),
            baseline
        );

        fs::write(directory.0.join(DIAGNOSTIC_OBSERVATION_RECEIPT_FILE), []).unwrap();
        assert!(attest_formal_state(&directory.0, dataset.seed, &contract).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn formal_attestation_accepts_a_valid_explicit_non_ranked_chain() {
        let directory = TestDirectory::new();
        let (dataset, contract) =
            initialize_formal_run_with_shape(&directory, "run-formal-smoke", 0x5a0c_e001, 1);
        assert_eq!(contract.conformance, RunConformance::NonRankedDeviation);

        let receipt = attest_formal_state(&directory.0, dataset.seed, &contract)
            .unwrap()
            .to_string();
        assert!(receipt.is_ascii());
        assert!(receipt.starts_with("FORMAL_STATE_V2 terminal_evidence_size="));
        assert!(!receipt.contains("ranking"));
        assert!(!receipt.contains("public_spec_aligned"));
    }

    #[cfg(unix)]
    #[test]
    fn formal_attestation_fails_closed_without_mutating_unsafe_state() {
        let source = TestDirectory::new();
        let (dataset, contract) = initialize_formal_run(&source);

        let empty = TestDirectory::new();
        assert!(attest_formal_state(&empty.0, dataset.seed, &contract).is_err());
        assert!(attest_formal_state(Path::new("relative"), dataset.seed, &contract).is_err());

        let legacy_target = TestDirectory::new();
        copy_state_directory(&source.0, &legacy_target.0);
        fs::write(legacy_target.0.join(LEDGER_FILE), b"legacy target\n").unwrap();
        assert!(attest_formal_state(&legacy_target.0, dataset.seed, &contract).is_err());
        assert_eq!(
            fs::read(legacy_target.0.join(LEDGER_FILE)).unwrap(),
            b"legacy target\n"
        );

        let legacy_temporary = TestDirectory::new();
        copy_state_directory(&source.0, &legacy_temporary.0);
        let temporary = legacy_temporary.0.join(".run_ledger.state.17.3.tmp");
        fs::write(&temporary, b"legacy temporary\n").unwrap();
        assert!(attest_formal_state(&legacy_temporary.0, dataset.seed, &contract).is_err());
        assert_eq!(fs::read(&temporary).unwrap(), b"legacy temporary\n");

        let unknown = TestDirectory::new();
        copy_state_directory(&source.0, &unknown.0);
        fs::write(unknown.0.join("unknown.state"), b"unknown\n").unwrap();
        assert!(attest_formal_state(&unknown.0, dataset.seed, &contract).is_err());
        assert!(unknown.0.join("unknown.state").is_file());

        let symlink = TestDirectory::new();
        copy_state_directory(&source.0, &symlink.0);
        fs::remove_file(symlink.0.join(DATABASE_IDENTITY_FILE)).unwrap();
        std::os::unix::fs::symlink(
            symlink.0.join(TERMINAL_EVIDENCE_FILE),
            symlink.0.join(DATABASE_IDENTITY_FILE),
        )
        .unwrap();
        assert!(attest_formal_state(&symlink.0, dataset.seed, &contract).is_err());
        assert!(fs::symlink_metadata(symlink.0.join(DATABASE_IDENTITY_FILE))
            .unwrap()
            .file_type()
            .is_symlink());

        let missing = TestDirectory::new();
        copy_state_directory(&source.0, &missing.0);
        fs::remove_file(missing.0.join(RECOVERY_RECEIPT_FILE)).unwrap();
        assert!(attest_formal_state(&missing.0, dataset.seed, &contract).is_err());

        let tampered = TestDirectory::new();
        copy_state_directory(&source.0, &tampered.0);
        let terminal_path = tampered.0.join(TERMINAL_EVIDENCE_FILE);
        let mut terminal_bytes = fs::read(&terminal_path).unwrap();
        *terminal_bytes.last_mut().unwrap() ^= 1;
        fs::write(&terminal_path, &terminal_bytes).unwrap();
        assert!(attest_formal_state(&tampered.0, dataset.seed, &contract).is_err());
        assert_eq!(fs::read(terminal_path).unwrap(), terminal_bytes);

        let missing_identity = TestDirectory::new();
        copy_state_directory(&source.0, &missing_identity.0);
        fs::remove_file(missing_identity.0.join(DATABASE_IDENTITY_FILE)).unwrap();
        assert!(attest_formal_state(&missing_identity.0, dataset.seed, &contract).is_err());
    }

    #[test]
    fn dataset_round_trip_rejects_wrong_partition_totals() {
        let state = sample_dataset("run-1", 9);
        let encoded = state.encode();
        assert!(encoded.len() < MAX_DATASET_STATE_BYTES);
        assert_eq!(DatasetState::decode(&encoded).unwrap(), state);
        assert!(DatasetState::decode(encoded.trim_end()).is_err());
        assert!(DatasetState::decode(&encoded.replacen("version=5", "version=4", 1)).is_err());
        assert_eq!(
            state.generated_csv_sha256(),
            &state.setup_evidence.dataset_checksum
        );
        assert_eq!(
            DatasetState::decode(&encoded)
                .unwrap()
                .runtime_schema
                .fingerprint(),
            state.runtime_schema.fingerprint()
        );

        let malformed = encoded.replace("order_line_rows=7", "order_line_rows=8");
        assert!(DatasetState::decode(&malformed).is_err());

        let encoded_fingerprint =
            format!("fingerprint={:016x}", state.runtime_schema.fingerprint());
        let damaged_fingerprint = format!(
            "fingerprint={:016x}",
            state.runtime_schema.fingerprint() ^ 1
        );
        assert!(DatasetState::decode(&encoded.replacen(
            &encoded_fingerprint,
            &damaged_fingerprint,
            1
        ))
        .is_err());
        let encoded_csv_checksum = hex_encode(state.generated_csv_sha256());
        let mut damaged_csv_checksum = encoded_csv_checksum.clone().into_bytes();
        damaged_csv_checksum[0] = if damaged_csv_checksum[0] == b'0' {
            b'1'
        } else {
            b'0'
        };
        let damaged_csv_checksum = String::from_utf8(damaged_csv_checksum).unwrap();
        assert!(DatasetState::decode(&encoded.replacen(
            &encoded_csv_checksum,
            &damaged_csv_checksum,
            1
        ))
        .is_err());

        let mut changed_evidence = state.clone();
        changed_evidence.setup_evidence.anchors[0].lines[0].amount_bits ^= 1;
        changed_evidence.validate().unwrap();
        assert_ne!(
            dataset_checksum(&changed_evidence),
            dataset_checksum(&state)
        );

        let mut wrong_seed = state;
        wrong_seed.runtime_schema = RuntimeSchema::opaque(10).unwrap();
        assert!(wrong_seed.validate().is_err());
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

        let mut damaged = encoded.clone();
        damaged.pop();
        damaged.push('x');
        assert!(decode_artifact(&damaged, "sample", &dataset, 128).is_err());

        let noncanonical_version = encoded.replacen("version=1\n", "version=01\n", 1);
        assert!(
            decode_artifact(&noncanonical_version, "sample", &dataset, 128).is_err(),
            "numeric aliases must not bypass canonical artifact encoding"
        );

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
    fn online_key_sample_is_stable_seeded_and_dataset_bound() {
        let dataset = sample_dataset_with_warehouses("online-keys-a", 2026, 50);
        let sample = dataset.online_key_sample().unwrap();
        assert_eq!(sample, dataset.online_key_sample().unwrap());

        let different_seed = sample_dataset_with_warehouses("online-keys-b", 2027, 50);
        assert_ne!(sample, different_seed.online_key_sample().unwrap());

        let mut crossed_evidence = different_seed.clone();
        crossed_evidence.setup_evidence = dataset.setup_evidence.clone();
        assert!(crossed_evidence.online_key_sample().is_err());

        let mut crossed_schema = dataset;
        crossed_schema.runtime_schema = RuntimeSchema::opaque(2027).unwrap();
        assert!(crossed_schema.online_key_sample().is_err());
    }

    #[test]
    fn terminal_evidence_authority_round_trips_the_complete_state_chain() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-terminal-chain", 79);
        let contract = sample_contract(1);
        let evidence = sample_terminal_evidence(&dataset);
        let binding = terminal_artifact_binding(&dataset).unwrap();
        let expected = encode_terminal_artifact_hex(&evidence, binding).unwrap();
        initialize_checked_run(&store, &dataset, &contract);

        let rank = store.begin_rank(&dataset, &contract).unwrap();
        store
            .complete_rank_with_terminal_evidence(&dataset, &contract, rank, &evidence)
            .unwrap();
        assert!(directory.0.join(TERMINAL_EVIDENCE_FILE).is_file());
        assert!(!directory.0.join(LEDGER_FILE).exists());

        let (online, restored) = store
            .begin_online_check_from_terminal_evidence(&dataset, &contract)
            .unwrap();
        assert_eq!(
            encode_terminal_artifact_hex(&restored, binding).unwrap(),
            expected
        );
        store
            .complete_online_check_from_terminal_evidence(
                &dataset,
                &contract,
                online,
                &sample_float_baseline(),
            )
            .unwrap();
        let baseline = fs::read_to_string(directory.0.join(FLOAT_BASELINE_FILE)).unwrap();
        assert!(baseline.contains("terminal_evidence_checksum="));
        assert!(!baseline.contains("ledger_checksum="));

        for event in [
            CrashLifecycleEvent::Intent,
            CrashLifecycleEvent::Killed,
            CrashLifecycleEvent::RestartStarted,
            CrashLifecycleEvent::RestartReady,
        ] {
            store
                .record_crash_lifecycle_from_terminal_evidence(&dataset, &contract, event)
                .unwrap();
        }
        for (file, _) in crash_lifecycle_specs() {
            let marker = fs::read_to_string(directory.0.join(file)).unwrap();
            assert!(marker.contains("terminal_evidence_checksum="));
            assert!(!marker.contains("ledger_checksum="));
        }

        let (recovery, restored, baseline) = store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .unwrap();
        assert_eq!(
            encode_terminal_artifact_hex(&restored, binding).unwrap(),
            expected
        );
        assert_eq!(baseline, sample_float_baseline());
        store
            .complete_recovery_check_from_terminal_evidence(&dataset, &contract, recovery)
            .unwrap();

        let warmup = store
            .begin_diagnostic_from_terminal_evidence(&dataset, &contract, DiagnosticStage::Warmup)
            .unwrap();
        store
            .complete_diagnostic_from_terminal_evidence(&dataset, &contract, warmup)
            .unwrap();
        let observation = store
            .begin_diagnostic_from_terminal_evidence(
                &dataset,
                &contract,
                DiagnosticStage::Observation,
            )
            .unwrap();
        store
            .complete_diagnostic_from_terminal_evidence(&dataset, &contract, observation)
            .unwrap();
    }

    #[test]
    fn terminal_evidence_rejects_same_length_payload_damage_and_republication() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-terminal-damage", 80);
        let contract = sample_contract(1);
        let evidence = sample_terminal_evidence(&dataset);
        initialize_checked_run(&store, &dataset, &contract);

        let rank = store.begin_rank(&dataset, &contract).unwrap();
        let duplicate_rank = RankClaim(rank.0);
        store
            .complete_rank_with_terminal_evidence(&dataset, &contract, rank, &evidence)
            .unwrap();
        let path = directory.0.join(TERMINAL_EVIDENCE_FILE);
        let original = fs::read(&path).unwrap();
        assert!(store
            .complete_rank_with_terminal_evidence(&dataset, &contract, duplicate_rank, &evidence)
            .is_err());
        assert_eq!(fs::read(&path).unwrap(), original);

        let mut damaged = original;
        let last = damaged.last_mut().unwrap();
        *last = if *last == b'0' { b'1' } else { b'0' };
        fs::write(&path, damaged).unwrap();
        assert!(store
            .begin_online_check_from_terminal_evidence(&dataset, &contract)
            .is_err());
    }

    #[test]
    fn strict_terminal_lock_rejects_legacy_target_and_temporary_without_mutation() {
        let target_directory = TestDirectory::new();
        let target_store = StateStore::open_terminal(&target_directory.0).unwrap();
        let target_dataset = sample_dataset("run-terminal-legacy-target", 82);
        let target_contract = sample_contract(1);
        let target_evidence = sample_terminal_evidence(&target_dataset);
        initialize_checked_run(&target_store, &target_dataset, &target_contract);
        let target_rank = target_store
            .begin_rank(&target_dataset, &target_contract)
            .unwrap();
        target_store
            .complete_rank_with_terminal_evidence(
                &target_dataset,
                &target_contract,
                target_rank,
                &target_evidence,
            )
            .unwrap();
        let terminal_before = fs::read(target_directory.0.join(TERMINAL_EVIDENCE_FILE)).unwrap();
        fs::write(
            target_directory.0.join(LEDGER_FILE),
            b"forbidden legacy target",
        )
        .unwrap();
        let target_unrelated_temporary = target_directory
            .0
            .join(format!(".{DATASET_FILE}.{}.100.tmp", std::process::id()));
        fs::write(&target_unrelated_temporary, b"dataset-provisional").unwrap();
        assert!(target_store
            .begin_online_check_from_terminal_evidence(&target_dataset, &target_contract)
            .is_err());
        assert_eq!(
            fs::read(target_directory.0.join(TERMINAL_EVIDENCE_FILE)).unwrap(),
            terminal_before
        );
        assert!(target_directory.0.join(LEDGER_FILE).exists());
        assert_eq!(
            fs::read(&target_unrelated_temporary).unwrap(),
            b"dataset-provisional"
        );

        let temporary_directory = TestDirectory::new();
        let temporary_store = StateStore::open_terminal(&temporary_directory.0).unwrap();
        let temporary_dataset = sample_dataset("run-terminal-legacy-temporary", 83);
        let temporary_contract = sample_contract(1);
        initialize_checked_run(&temporary_store, &temporary_dataset, &temporary_contract);
        let legacy_temporary = temporary_directory
            .0
            .join(format!(".{LEDGER_FILE}.{}.0.tmp", std::process::id()));
        fs::write(&legacy_temporary, b"legacy-provisional").unwrap();
        let unrelated_temporary = temporary_directory.0.join(format!(
            ".{TERMINAL_EVIDENCE_FILE}.{}.1.tmp",
            std::process::id()
        ));
        fs::write(&unrelated_temporary, b"terminal-provisional").unwrap();

        assert!(temporary_store
            .begin_rank(&temporary_dataset, &temporary_contract)
            .is_err());
        assert_eq!(fs::read(&legacy_temporary).unwrap(), b"legacy-provisional");
        assert_eq!(
            fs::read(&unrelated_temporary).unwrap(),
            b"terminal-provisional"
        );
        assert!(!temporary_directory.0.join(TERMINAL_EVIDENCE_FILE).exists());
    }

    #[test]
    fn terminal_open_rejects_preexisting_legacy_state_without_cleanup() {
        let target_directory = TestDirectory::new();
        let target = target_directory.0.join(LEDGER_FILE);
        let ordinary = target_directory
            .0
            .join(format!(".{DATASET_FILE}.{}.101.tmp", std::process::id()));
        fs::write(&target, b"legacy-target").unwrap();
        fs::write(&ordinary, b"ordinary-orphan").unwrap();

        assert!(StateStore::open_terminal(&target_directory.0).is_err());
        assert_eq!(fs::read(&target).unwrap(), b"legacy-target");
        assert_eq!(fs::read(&ordinary).unwrap(), b"ordinary-orphan");

        let temporary_directory = TestDirectory::new();
        let legacy = temporary_directory
            .0
            .join(format!(".{LEDGER_FILE}.{}.102.tmp", std::process::id()));
        let ordinary = temporary_directory
            .0
            .join(format!(".{DATASET_FILE}.{}.103.tmp", std::process::id()));
        fs::write(&legacy, b"legacy-orphan").unwrap();
        fs::write(&ordinary, b"ordinary-orphan").unwrap();

        assert!(StateStore::open_existing_terminal(&temporary_directory.0).is_err());
        assert_eq!(fs::read(&legacy).unwrap(), b"legacy-orphan");
        assert_eq!(fs::read(&ordinary).unwrap(), b"ordinary-orphan");
    }

    #[test]
    fn terminal_cleanup_rechecks_legacy_state_before_mutating_snapshot() {
        let directory = TestDirectory::new();
        let ordinary = directory
            .0
            .join(format!(".{DATASET_FILE}.{}.104.tmp", std::process::id()));
        let injected_legacy = directory
            .0
            .join(format!(".{LEDGER_FILE}.{}.105.tmp", std::process::id()));
        fs::write(&ordinary, b"ordinary-orphan").unwrap();

        let result = lock_clean_terminal_state_directory_with_hook(&directory.0, || {
            fs::write(&injected_legacy, b"injected-after-snapshot")?;
            Ok(())
        });

        assert!(result.is_err());
        assert_eq!(fs::read(&ordinary).unwrap(), b"ordinary-orphan");
        assert_eq!(
            fs::read(&injected_legacy).unwrap(),
            b"injected-after-snapshot"
        );
    }

    #[cfg(unix)]
    #[test]
    fn terminal_cleanup_rejects_replaced_snapshot_entry_without_unlinking_it() {
        let directory = TestDirectory::new();
        let ordinary = directory
            .0
            .join(format!(".{DATASET_FILE}.{}.106.tmp", std::process::id()));
        let staged_replacement = directory.0.join("staged-replacement");
        fs::write(&ordinary, b"captured-orphan").unwrap();
        fs::write(&staged_replacement, b"replacement-victim").unwrap();

        let result = lock_clean_terminal_state_directory_with_hook(&directory.0, || {
            fs::remove_file(&ordinary)?;
            fs::rename(&staged_replacement, &ordinary)?;
            Ok(())
        });

        assert!(result.is_err());
        assert_eq!(fs::read(&ordinary).unwrap(), b"replacement-victim");
    }

    #[cfg(unix)]
    #[test]
    fn terminal_cleanup_rejects_root_replacement_before_unlinking() {
        let parent = TestDirectory::new();
        let root = parent.0.join("state");
        let moved = parent.0.join("moved-state");
        fs::create_dir(&root).unwrap();
        let file_name = format!(".{DATASET_FILE}.{}.107.tmp", std::process::id());
        let captured = root.join(&file_name);
        fs::write(&captured, b"captured-orphan").unwrap();

        let result = lock_clean_terminal_state_directory_with_hook(&root, || {
            fs::rename(&root, &moved)?;
            fs::create_dir(&root)?;
            fs::write(root.join(&file_name), b"new-root-victim")?;
            Ok(())
        });

        assert!(result.is_err());
        assert_eq!(
            fs::read(moved.join(&file_name)).unwrap(),
            b"captured-orphan"
        );
        assert_eq!(fs::read(root.join(&file_name)).unwrap(), b"new-root-victim");
    }

    #[test]
    fn append_only_state_chain_gates_recovery_and_two_diagnostic_segments() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-chain", 81);
        let contract = sample_contract(1);
        initialize_checked_run(&store, &dataset, &contract);

        assert!(store
            .begin_diagnostic_from_terminal_evidence(&dataset, &contract, DiagnosticStage::Warmup)
            .is_err());

        let rank = store.begin_rank(&dataset, &contract).unwrap();
        let evidence = sample_terminal_evidence(&dataset);
        store
            .complete_rank_with_terminal_evidence(&dataset, &contract, rank, &evidence)
            .unwrap();
        let (online, _) = store
            .begin_online_check_from_terminal_evidence(&dataset, &contract)
            .unwrap();
        store
            .complete_online_check_from_terminal_evidence(
                &dataset,
                &contract,
                online,
                &sample_float_baseline(),
            )
            .unwrap();

        assert!(store
            .begin_diagnostic_from_terminal_evidence(&dataset, &contract, DiagnosticStage::Warmup)
            .is_err());
        assert!(store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .is_err());
        for event in [
            CrashLifecycleEvent::Intent,
            CrashLifecycleEvent::Killed,
            CrashLifecycleEvent::RestartStarted,
            CrashLifecycleEvent::RestartReady,
        ] {
            store
                .record_crash_lifecycle_from_terminal_evidence(&dataset, &contract, event)
                .unwrap();
        }
        let (recovery, _, baseline) = store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .unwrap();
        assert_eq!(baseline, sample_float_baseline());
        store
            .complete_recovery_check_from_terminal_evidence(&dataset, &contract, recovery)
            .unwrap();

        assert!(store
            .begin_diagnostic_from_terminal_evidence(
                &dataset,
                &contract,
                DiagnosticStage::Observation
            )
            .is_err());
        let warmup = store
            .begin_diagnostic_from_terminal_evidence(&dataset, &contract, DiagnosticStage::Warmup)
            .unwrap();
        assert!(store
            .begin_online_check_from_terminal_evidence(&dataset, &contract)
            .is_err());
        assert!(store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .is_err());
        store
            .complete_diagnostic_from_terminal_evidence(&dataset, &contract, warmup)
            .unwrap();

        let reopened = StateStore::open_existing_terminal(&directory.0).unwrap();
        let observation = reopened
            .begin_diagnostic_from_terminal_evidence(
                &dataset,
                &contract,
                DiagnosticStage::Observation,
            )
            .unwrap();
        reopened
            .complete_diagnostic_from_terminal_evidence(&dataset, &contract, observation)
            .unwrap();
        assert!(reopened.begin_rank(&dataset, &contract).is_err());
        assert!(reopened
            .begin_diagnostic_from_terminal_evidence(
                &dataset,
                &contract,
                DiagnosticStage::Observation
            )
            .is_err());
    }

    #[test]
    fn crash_lifecycle_rejects_repeats_and_out_of_order_transitions() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-crash-order", 91);
        let contract = sample_contract(1);
        initialize_online_run(&store, &dataset, &contract);

        assert!(store
            .record_crash_lifecycle_from_terminal_evidence(
                &dataset,
                &contract,
                CrashLifecycleEvent::Killed
            )
            .is_err());
        assert!(store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .is_err());

        store
            .record_crash_lifecycle_from_terminal_evidence(
                &dataset,
                &contract,
                CrashLifecycleEvent::Intent,
            )
            .unwrap();
        assert!(store
            .record_crash_lifecycle_from_terminal_evidence(
                &dataset,
                &contract,
                CrashLifecycleEvent::Intent
            )
            .is_err());
        assert!(store
            .record_crash_lifecycle_from_terminal_evidence(
                &dataset,
                &contract,
                CrashLifecycleEvent::RestartStarted
            )
            .is_err());
        assert!(store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .is_err());

        store
            .record_crash_lifecycle_from_terminal_evidence(
                &dataset,
                &contract,
                CrashLifecycleEvent::Killed,
            )
            .unwrap();
        assert!(store
            .record_crash_lifecycle_from_terminal_evidence(
                &dataset,
                &contract,
                CrashLifecycleEvent::RestartReady
            )
            .is_err());
        store
            .record_crash_lifecycle_from_terminal_evidence(
                &dataset,
                &contract,
                CrashLifecycleEvent::RestartStarted,
            )
            .unwrap();
        assert!(store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .is_err());
        store
            .record_crash_lifecycle_from_terminal_evidence(
                &dataset,
                &contract,
                CrashLifecycleEvent::RestartReady,
            )
            .unwrap();
        assert!(store
            .begin_recovery_check_from_terminal_evidence(&dataset, &contract)
            .is_ok());
    }

    #[test]
    fn crash_lifecycle_is_fail_closed_on_orphans_and_tampering() {
        let orphan_directory = TestDirectory::new();
        let orphan_store = StateStore::open_terminal(&orphan_directory.0).unwrap();
        let orphan_dataset = sample_dataset("run-crash-orphan", 92);
        let contract = sample_contract(1);
        initialize_online_run(&orphan_store, &orphan_dataset, &contract);
        fs::write(orphan_directory.0.join(RESTART_READY_FILE), b"orphan").unwrap();
        assert!(orphan_store
            .record_crash_lifecycle_from_terminal_evidence(
                &orphan_dataset,
                &contract,
                CrashLifecycleEvent::Intent
            )
            .is_err());
        assert!(orphan_store
            .begin_recovery_check_from_terminal_evidence(&orphan_dataset, &contract)
            .is_err());

        let tamper_directory = TestDirectory::new();
        let tamper_store = StateStore::open_terminal(&tamper_directory.0).unwrap();
        let tamper_dataset = sample_dataset("run-crash-tamper", 93);
        initialize_online_run(&tamper_store, &tamper_dataset, &contract);
        for event in [
            CrashLifecycleEvent::Intent,
            CrashLifecycleEvent::Killed,
            CrashLifecycleEvent::RestartStarted,
            CrashLifecycleEvent::RestartReady,
        ] {
            tamper_store
                .record_crash_lifecycle_from_terminal_evidence(&tamper_dataset, &contract, event)
                .unwrap();
        }
        let killed_path = tamper_directory.0.join(CRASH_KILLED_FILE);
        let mut killed = fs::read(&killed_path).unwrap();
        *killed.last_mut().unwrap() ^= 1;
        fs::write(killed_path, killed).unwrap();
        assert!(tamper_store
            .begin_recovery_check_from_terminal_evidence(&tamper_dataset, &contract)
            .is_err());
    }

    #[test]
    fn rank_claim_and_terminal_evidence_are_write_once_across_contracts() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset_with_warehouses(
            "run-ranked-write-once",
            82,
            i32::from(OFFICIAL_WAREHOUSES),
        );
        let contract = sample_contract(OFFICIAL_WAREHOUSES);
        initialize_checked_run(&store, &dataset, &contract);

        let rank = store.begin_rank(&dataset, &contract).unwrap();
        let evidence = sample_terminal_evidence(&dataset);
        store
            .complete_rank_with_terminal_evidence(&dataset, &contract, rank, &evidence)
            .unwrap();
        let evidence_path = directory.0.join(TERMINAL_EVIDENCE_FILE);
        let original = fs::read(&evidence_path).unwrap();

        let mut non_ranked = contract.clone();
        non_ranked.clients = 1;
        non_ranked.conformance = RunConformance::NonRankedDeviation;
        assert!(store.begin_rank(&dataset, &non_ranked).is_err());
        assert_eq!(fs::read(evidence_path).unwrap(), original);
    }

    #[test]
    fn run_contract_binds_client_timing_deadlines_and_conformance() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-contract-binding", 83);
        let contract = sample_contract(1);
        store.initialize_run(&dataset, &contract).unwrap();
        assert_eq!(store.load_bound_dataset(&contract).unwrap(), dataset);

        let mut mutations = Vec::new();
        let mut changed = contract.clone();
        changed.clients += 1;
        mutations.push(changed);
        let mut changed = contract.clone();
        changed.warmup_seconds += 1;
        mutations.push(changed);
        let mut changed = contract.clone();
        changed.window_seconds += 1;
        mutations.push(changed);
        let mut changed = contract.clone();
        changed.load_budget_seconds += 1;
        mutations.push(changed);
        let mut changed = contract.clone();
        changed.recovery_ready_budget_seconds += 1;
        mutations.push(changed);
        let mut changed = contract.clone();
        changed.response_timeout_seconds += 1;
        mutations.push(changed);
        let mut changed = contract.clone();
        changed.phase_tail_grace_seconds += 1;
        mutations.push(changed);

        for changed in mutations {
            assert!(store.load_bound_dataset(&changed).is_err());
        }

        let mut falsely_ranked = contract;
        falsely_ranked.conformance = RunConformance::PublicSpecAligned;
        assert!(store.load_bound_dataset(&falsely_ranked).is_err());
    }

    #[test]
    fn setup_claim_is_persisted_before_dataset_and_rejects_reuse() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-setup-claim", 87);
        let contract = sample_contract(1);

        store
            .publish_setup_intent(&dataset.run_id, dataset.seed, &contract)
            .unwrap();
        assert!(directory.0.join(SETUP_INTENT_FILE).is_file());
        assert!(!directory.0.join(DATASET_FILE).exists());
        assert!(store
            .publish_setup_intent(&dataset.run_id, dataset.seed, &contract)
            .is_err());
        assert!(store
            .begin_or_resume_setup(&dataset.run_id, dataset.seed + 1, &contract)
            .is_err());
        let (claim, origin) = store
            .begin_or_resume_setup(&dataset.run_id, dataset.seed, &contract)
            .unwrap();
        assert_eq!(origin, SetupClaimOrigin::Resumed);
        assert!(directory.0.join(SETUP_EXECUTION_FILE).is_file());
        assert!(store
            .begin_or_resume_setup(&dataset.run_id, dataset.seed, &contract)
            .is_err());

        let mut wrong_dataset = dataset.clone();
        wrong_dataset.seed += 1;
        assert!(store
            .complete_dataset(&wrong_dataset, &contract, claim)
            .is_err());
        assert!(!directory.0.join(DATASET_FILE).exists());
    }

    #[test]
    fn concurrent_init_claims_cannot_consume_one_setup_intent_twice() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-concurrent-setup", 94);
        let contract = sample_contract(1);
        store
            .publish_setup_intent(&dataset.run_id, dataset.seed, &contract)
            .unwrap();

        let barrier = Arc::new(Barrier::new(3));
        let seed = dataset.seed;
        let handles = (0..2)
            .map(|_| {
                let store = store.clone();
                let run_id = dataset.run_id.clone();
                let contract = contract.clone();
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    store
                        .begin_or_resume_setup(&run_id, seed, &contract)
                        .is_ok()
                })
            })
            .collect::<Vec<_>>();
        barrier.wait();
        let successes = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|success| *success)
            .count();
        assert_eq!(successes, 1);
        assert!(directory.0.join(SETUP_EXECUTION_FILE).is_file());
    }

    #[test]
    fn completed_dataset_is_bound_to_pre_ddl_setup_claim() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-complete-setup", 88);
        let contract = sample_contract(1);
        store
            .publish_setup_intent(&dataset.run_id, dataset.seed, &contract)
            .unwrap();
        let (claim, origin) = store
            .begin_or_resume_setup(&dataset.run_id, dataset.seed, &contract)
            .unwrap();
        assert_eq!(origin, SetupClaimOrigin::Resumed);
        store.complete_dataset(&dataset, &contract, claim).unwrap();

        assert_eq!(store.load_bound_dataset(&contract).unwrap(), dataset);
        assert!(store
            .publish_setup_intent("different-run", dataset.seed, &contract)
            .is_err());
    }

    #[test]
    fn incomplete_rank_claim_is_fail_closed() {
        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-incomplete-rank", 84);
        let contract = sample_contract(1);
        initialize_checked_run(&store, &dataset, &contract);

        let _claim = store.begin_rank(&dataset, &contract).unwrap();
        assert!(store.begin_rank(&dataset, &contract).is_err());
        assert!(store
            .begin_online_check_from_terminal_evidence(&dataset, &contract)
            .is_err());
        assert!(!directory.0.join(TERMINAL_EVIDENCE_FILE).exists());
    }

    #[test]
    fn atomic_publish_new_never_replaces_a_completed_artifact() {
        let directory = TestDirectory::new();
        atomic_publish_new(&directory.0, "write-once.state", b"first").unwrap();
        assert!(atomic_publish_new(&directory.0, "write-once.state", b"second").is_err());
        assert_eq!(
            fs::read(directory.0.join("write-once.state")).unwrap(),
            b"first"
        );
    }

    #[test]
    fn atomic_publish_unlink_failure_after_commit_is_recovered_before_success() {
        let directory = TestDirectory::new();
        let name = "unlink-failure.state";
        let mut injected = false;

        atomic_publish_new_with_fault(&directory.0, name, b"durable", |step| {
            if step == AtomicPublishStep::TemporaryUnlink {
                injected = true;
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "injected temporary unlink failure",
                ))
            } else {
                Ok(())
            }
        })
        .unwrap();

        assert!(injected);
        assert_eq!(fs::read(directory.0.join(name)).unwrap(), b"durable");
        let temporary_prefix = format!(".{name}.");
        let temporary_files = fs::read_dir(&directory.0)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                let file_name = entry.file_name();
                let file_name = file_name.to_string_lossy();
                file_name.starts_with(&temporary_prefix) && file_name.ends_with(".tmp")
            })
            .count();
        assert_eq!(temporary_files, 0);
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
        assert!(atomic_publish_new(&directory.0, name, b"replacement").is_err());
        assert_eq!(fs::read(directory.0.join(name)).unwrap(), b"durable");
    }

    #[test]
    fn atomic_publish_first_directory_sync_failure_rolls_back_and_can_retry() {
        let directory = TestDirectory::new();
        let name = "first-directory-sync-failure.state";
        let mut injected = false;

        let error = atomic_publish_new_with_fault(&directory.0, name, b"durable", |step| {
            if step == AtomicPublishStep::FirstDirectorySync {
                injected = true;
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "injected first directory sync failure",
                ))
            } else {
                Ok(())
            }
        })
        .unwrap_err();

        assert!(injected);
        assert!(error
            .to_string()
            .contains("injected first directory sync failure"));
        assert!(!directory.0.join(name).exists());
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
        atomic_publish_new(&directory.0, name, b"durable").unwrap();
        assert_eq!(fs::read(directory.0.join(name)).unwrap(), b"durable");
    }

    #[test]
    fn atomic_publish_second_directory_sync_failure_is_recovered_before_success() {
        let directory = TestDirectory::new();
        let name = "second-directory-sync-failure.state";
        let mut injected = false;

        atomic_publish_new_with_fault(&directory.0, name, b"durable", |step| {
            if step == AtomicPublishStep::SecondDirectorySync {
                injected = true;
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "injected second directory sync failure",
                ))
            } else {
                Ok(())
            }
        })
        .unwrap();

        assert!(injected);
        assert_eq!(fs::read(directory.0.join(name)).unwrap(), b"durable");
        assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 1);
        assert!(atomic_publish_new(&directory.0, name, b"replacement").is_err());
        assert_eq!(fs::read(directory.0.join(name)).unwrap(), b"durable");
    }

    #[test]
    fn dependent_claim_cannot_consume_a_provisional_hard_link() {
        let directory = TestDirectory::new();
        let run_id = "run-provisional-link";
        let seed = 48;
        let contract = sample_contract(1);
        let encoded = encode_setup_intent(run_id, seed, &contract).unwrap();
        let linked = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let claimant_store = StateStore::open_existing_terminal(&directory.0).unwrap();

        let publisher_root = directory.0.clone();
        let publisher_linked = Arc::clone(&linked);
        let publisher_release = Arc::clone(&release);
        let publisher = thread::spawn(move || {
            let _directory_lock = lock_state_directory(&publisher_root).unwrap();
            atomic_publish_new_with_fault(
                &publisher_root,
                SETUP_INTENT_FILE,
                encoded.as_bytes(),
                |step| {
                    if step == AtomicPublishStep::FirstDirectorySync {
                        publisher_linked.wait();
                        publisher_release.wait();
                        Err(std::io::Error::other(
                            "injected first directory sync failure",
                        ))
                    } else {
                        Ok(())
                    }
                },
            )
        });

        linked.wait();
        assert!(directory.0.join(SETUP_INTENT_FILE).exists());

        let claimant_contract = contract.clone();
        let (send, receive) = mpsc::channel();
        let claimant = thread::spawn(move || {
            send.send(claimant_store.begin_or_resume_setup(run_id, seed, &claimant_contract))
                .unwrap();
        });
        assert!(matches!(
            receive.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        release.wait();
        assert!(publisher.join().unwrap().is_err());
        let (claim, origin) = receive
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        claimant.join().unwrap();
        assert_eq!(origin, SetupClaimOrigin::Created);
        assert_ne!(claim.intent_checksum, 0);
        assert!(directory.0.join(SETUP_INTENT_FILE).is_file());
        assert!(directory.0.join(SETUP_EXECUTION_FILE).is_file());
    }

    #[test]
    fn open_existing_does_not_create_a_missing_state_directory() {
        let directory = TestDirectory::new();
        let missing = directory.0.join("missing");
        assert!(StateStore::open_existing_terminal(&missing).is_err());
        assert!(!missing.exists());
    }

    #[test]
    fn reopening_cleans_strict_unlinked_publication_temporary() {
        let directory = TestDirectory::new();
        let temporary = directory
            .0
            .join(format!(".{SETUP_INTENT_FILE}.{}.0.tmp", std::process::id()));
        fs::write(&temporary, b"crash-before-link").unwrap();

        let store = StateStore::open_terminal(&directory.0).unwrap();
        assert!(!temporary.exists());
        let contract = sample_contract(1);
        store
            .publish_setup_intent("run-clean-orphan", 51, &contract)
            .unwrap();
        assert!(directory.0.join(SETUP_INTENT_FILE).is_file());
    }

    #[test]
    fn orphan_cleanup_validates_the_complete_batch_before_unlinking() {
        let directory = TestDirectory::new();
        let valid = directory
            .0
            .join(format!(".{DATASET_FILE}.{}.90.tmp", std::process::id()));
        fs::write(&valid, b"valid-unlinked-orphan").unwrap();

        let invalid = directory.0.join(format!(
            ".{SETUP_INTENT_FILE}.{}.91.tmp",
            std::process::id()
        ));
        let target = directory.0.join(SETUP_INTENT_FILE);
        fs::write(&invalid, b"unrelated-temporary").unwrap();
        fs::write(&target, b"different-target-inode").unwrap();

        let directory_file = File::open(&directory.0).unwrap();
        assert!(cleanup_orphan_temporary_files(&directory.0, &directory_file).is_err());
        assert_eq!(fs::read(&valid).unwrap(), b"valid-unlinked-orphan");
        assert_eq!(fs::read(&invalid).unwrap(), b"unrelated-temporary");
        assert_eq!(fs::read(&target).unwrap(), b"different-target-inode");
    }

    #[cfg(unix)]
    #[test]
    fn linked_orphan_target_sync_failure_preserves_provenance_pair() {
        use std::os::unix::fs::MetadataExt;

        let directory = TestDirectory::new();
        let temporary = directory
            .0
            .join(format!(".{DATASET_FILE}.{}.92.tmp", std::process::id()));
        let target = directory.0.join(DATASET_FILE);
        fs::write(&temporary, b"provisional-target").unwrap();
        fs::hard_link(&temporary, &target).unwrap();

        let directory_file = File::open(&directory.0).unwrap();
        let result =
            cleanup_orphan_temporary_files_with_fault(&directory.0, &directory_file, |step| {
                if step == OrphanCleanupStep::BeforeTargetSync {
                    Err(std::io::Error::other("injected target sync failure"))
                } else {
                    Ok(())
                }
            });
        assert!(result.is_err());
        let temporary_metadata = fs::symlink_metadata(&temporary).unwrap();
        let target_metadata = fs::symlink_metadata(&target).unwrap();
        assert_eq!(temporary_metadata.dev(), target_metadata.dev());
        assert_eq!(temporary_metadata.ino(), target_metadata.ino());
        assert_eq!(temporary_metadata.nlink(), 2);
        assert_eq!(target_metadata.nlink(), 2);
        assert_eq!(fs::read(&temporary).unwrap(), b"provisional-target");
        assert_eq!(fs::read(&target).unwrap(), b"provisional-target");
    }

    #[test]
    fn linked_orphan_cleanup_sync_failure_keeps_the_durable_target() {
        let directory = TestDirectory::new();
        let temporary = directory
            .0
            .join(format!(".{DATASET_FILE}.{}.93.tmp", std::process::id()));
        let target = directory.0.join(DATASET_FILE);
        fs::write(&temporary, b"durable-target").unwrap();
        fs::hard_link(&temporary, &target).unwrap();

        let directory_file = File::open(&directory.0).unwrap();
        let result =
            cleanup_orphan_temporary_files_with_fault(&directory.0, &directory_file, |step| {
                if step == OrphanCleanupStep::BeforeCleanupSync {
                    Err(std::io::Error::other("injected cleanup sync failure"))
                } else {
                    Ok(())
                }
            });
        assert!(result.is_err());
        assert!(!temporary.exists());
        assert_eq!(fs::read(&target).unwrap(), b"durable-target");
        cleanup_orphan_temporary_files(&directory.0, &directory_file).unwrap();
        assert_eq!(fs::read(&target).unwrap(), b"durable-target");
    }

    #[cfg(unix)]
    #[test]
    fn locked_state_directory_rejects_path_replacement() {
        let parent = TestDirectory::new();
        let root = parent.0.join("state");
        let moved = parent.0.join("moved-state");
        fs::create_dir(&root).unwrap();
        let locked = File::open(&root).unwrap();
        locked.lock().unwrap();
        fs::rename(&root, &moved).unwrap();
        fs::create_dir(&root).unwrap();

        assert!(validate_locked_directory_identity(&root, &locked).is_err());
        locked.unlock().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn preopened_store_recovers_linked_orphan_after_publisher_exit() {
        let directory = TestDirectory::new();
        let claimant_store = StateStore::open_existing_terminal(&directory.0).unwrap();
        let dataset = sample_dataset("run-preopened-orphan", 53);
        let encoded = dataset.encode();
        let publisher_root = directory.0.clone();
        let (linked_send, linked_receive) = mpsc::channel();
        let (release_send, release_receive) = mpsc::channel();

        let publisher = thread::spawn(move || {
            let _directory_lock = lock_state_directory(&publisher_root).unwrap();
            let temporary =
                publisher_root.join(format!(".{DATASET_FILE}.{}.2.tmp", std::process::id()));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temporary)
                .unwrap();
            file.write_all(encoded.as_bytes()).unwrap();
            file.sync_all().unwrap();
            fs::hard_link(&temporary, publisher_root.join(DATASET_FILE)).unwrap();
            linked_send.send(temporary).unwrap();
            release_receive.recv().unwrap();
        });

        let temporary = linked_receive.recv().unwrap();
        assert!(temporary.is_file());
        assert!(directory.0.join(DATASET_FILE).is_file());

        let (result_send, result_receive) = mpsc::channel();
        let claimant = thread::spawn(move || {
            result_send.send(claimant_store.load_dataset()).unwrap();
        });
        assert!(matches!(
            result_receive.recv_timeout(Duration::from_millis(100)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        release_send.send(()).unwrap();
        publisher.join().unwrap();
        let loaded = result_receive
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        claimant.join().unwrap();

        assert_eq!(loaded, dataset);
        assert!(!temporary.exists());
        assert!(directory.0.join(DATASET_FILE).is_file());
    }

    #[cfg(unix)]
    #[test]
    fn reopening_unlinks_only_the_verified_target_hard_link() {
        let directory = TestDirectory::new();
        let run_id = "run-linked-orphan";
        let seed = 52;
        let contract = sample_contract(1);
        let temporary = directory
            .0
            .join(format!(".{SETUP_INTENT_FILE}.{}.1.tmp", std::process::id()));
        fs::write(
            &temporary,
            encode_setup_intent(run_id, seed, &contract).unwrap(),
        )
        .unwrap();
        let target = directory.0.join(SETUP_INTENT_FILE);
        fs::hard_link(&temporary, &target).unwrap();

        let store = StateStore::open_existing_terminal(&directory.0).unwrap();
        assert!(!temporary.exists());
        assert!(target.is_file());
        let (_, origin) = store
            .begin_or_resume_setup(run_id, seed, &contract)
            .unwrap();
        assert_eq!(origin, SetupClaimOrigin::Resumed);
        assert!(directory.0.join(SETUP_EXECUTION_FILE).is_file());
    }

    #[cfg(unix)]
    #[test]
    fn state_store_rejects_symlink_artifact_targets() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new();
        let store = StateStore::open_terminal(&directory.0).unwrap();
        symlink(
            directory.0.join("missing-target"),
            directory.0.join(LEDGER_FILE),
        )
        .unwrap();

        assert!(store.load_dataset().is_err());
        assert!(directory.0.join(LEDGER_FILE).is_symlink());
    }
}
