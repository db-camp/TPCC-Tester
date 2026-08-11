//! Explicitly non-ranked final-2026 diagnostic workload.
//!
//! This executor intentionally does not use the ranked phase scheduler or
//! durable ledger.  It exists only for the post-pass 10-second warmup and
//! 60-second tracing workload, while retaining the published transaction mix,
//! hotspot routing, prepared sessions, retry classification, and no-think-time
//! dispatch contract.

use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Local;
use tokio::sync::{watch, Barrier};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::{info, warn};

use crate::config::{
    Config, DiagnosticSegment, ResolvedProfile, PRELIMINARY_MEASUREMENT_SECONDS,
    PRELIMINARY_WARMUP_SECONDS,
};
use crate::connection::client::RmdbClient;
use crate::error::TpccError;
use crate::profile::{TransactionKind, OFFICIAL_CLIENTS, OFFICIAL_WAREHOUSES};
use crate::ranking::catalog::RuntimeCatalog;
use crate::ranking::common::install_statement_layout;
use crate::ranking::dispatch::{self, FrozenTransaction};
use crate::ranking::runner::RankedTransactionOutcome;
use crate::ranking::session::open_ranked_session;
use crate::routing::{ClientSequence, OfficialRouter, StageId, WarehouseWheel, WorkloadSeed};
use crate::run_state::StateStore;
use crate::runtime_schema::{RuntimeSchema, SchemaMode};
use crate::workload::Final2026Workload;

// ASCII "diagwarm" and "diagobsv". Each diagnostic process starts its client
// sequences at zero, so the two public segments need independent routing
// domains rather than replaying the same deterministic transaction prefix.
const DIAGNOSTIC_WARMUP_STAGE: StageId = StageId::custom(0x6469_6167_7761_726d);
const DIAGNOSTIC_OBSERVATION_STAGE: StageId = StageId::custom(0x6469_6167_6f62_7376);
const PRELIMINARY_WARMUP_STAGE: StageId = StageId::custom(0x7072_656c_7761_726d);
const PRELIMINARY_MEASUREMENT_STAGE: StageId = StageId::custom(0x7072_656c_6d65_6173);
const RESOURCE_TIMELINE_ENV: &str = "RMDB_TPCC_RESOURCE_TIMELINE_FILE";
const DIAGNOSTIC_FAMILIES: [(TransactionKind, &str); 5] = [
    (TransactionKind::NewOrder, "new_order"),
    (TransactionKind::Payment, "payment"),
    (TransactionKind::OrderStatus, "order_status"),
    (TransactionKind::Delivery, "delivery"),
    (TransactionKind::StockLevel, "stock_level"),
];

const fn diagnostic_stage(segment: DiagnosticSegment) -> StageId {
    match segment {
        DiagnosticSegment::Warmup => DIAGNOSTIC_WARMUP_STAGE,
        DiagnosticSegment::Observation => DIAGNOSTIC_OBSERVATION_STAGE,
    }
}

fn publish_fast_resource_timeline(
    _phase_start: Instant,
    warmup: Duration,
    measurement: Duration,
) {
    let Some(output) = std::env::var_os(RESOURCE_TIMELINE_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
    else {
        return;
    };
    let origin_unix_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(value) => value.as_nanos(),
        Err(error) => {
            warn!("fast resource timeline clock is invalid: {error}");
            return;
        }
    };
    let payload = format!(
        "schema_version=1\n\
         kind=final2026_rank_timeline\n\
         origin_unix_ns={origin_unix_ns}\n\
         warmup_ns={}\n\
         measurement_windows=1\n\
         measurement_window_ns={}\n",
        warmup.as_nanos(),
        measurement.as_nanos(),
    );
    if let Err(error) = publish_resource_timeline(&output, payload.as_bytes()) {
        warn!(
            "could not publish non-ranked fast resource timeline {}: {error}",
            output.display()
        );
    }
}

fn publish_resource_timeline(output: &Path, payload: &[u8]) -> io::Result<()> {
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let file_name = output
        .file_name()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| OsStr::new("fast_timeline"));
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(payload)?;
        file.sync_all()?;
        match fs::symlink_metadata(output) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "resource timeline already exists",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::rename(&temporary, output)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

pub struct DiagnosticExecutor {
    config: Config,
    effective: ResolvedProfile,
}

impl DiagnosticExecutor {
    pub fn new(config: Config, effective: ResolvedProfile) -> Self {
        Self { config, effective }
    }

    pub async fn run(&self) -> Result<DiagnosticRunResult, TpccError> {
        let segment = self.config.diagnostic_segment.ok_or_else(|| {
            TpccError::Protocol("diagnostic executor requires an explicit segment".to_owned())
        })?;
        let duration_seconds = segment.duration_seconds();
        self.run_timed(
            segment,
            diagnostic_stage(segment),
            diagnostic_stage(segment),
            Duration::ZERO,
            Duration::from_secs(duration_seconds),
            false,
        )
        .await
    }

    pub async fn run_fast(&self) -> Result<DiagnosticRunResult, TpccError> {
        self.run_timed(
            DiagnosticSegment::Observation,
            PRELIMINARY_WARMUP_STAGE,
            PRELIMINARY_MEASUREMENT_STAGE,
            Duration::from_secs(PRELIMINARY_WARMUP_SECONDS),
            Duration::from_secs(PRELIMINARY_MEASUREMENT_SECONDS),
            true,
        )
        .await
    }

    async fn run_timed(
        &self,
        segment: DiagnosticSegment,
        warmup_stage: StageId,
        measurement_stage: StageId,
        warmup: Duration,
        measurement: Duration,
        fast: bool,
    ) -> Result<DiagnosticRunResult, TpccError> {
        let seed = self.effective.seed.ok_or_else(|| {
            TpccError::Protocol("diagnostic workload requires an explicit seed".to_owned())
        })?;
        let runtime_schema = self.validate_state_binding(seed)?;
        let catalog = Arc::new(
            RuntimeCatalog::from_schema(&runtime_schema).map_err(|error| {
                TpccError::Protocol(format!("invalid diagnostic runtime catalogue: {error}"))
            })?,
        );
        install_statement_layout(catalog.statement_layout()).map_err(|error| {
            TpccError::Protocol(format!("diagnostic statement layout: {error}"))
        })?;

        let response_timeout = Duration::from_secs(self.config.response_timeout_seconds);
        let phase_tail_grace = Duration::from_secs(self.config.phase_tail_grace_seconds);
        let duration = warmup.saturating_add(measurement);
        let router = OfficialRouter::new(WorkloadSeed(seed));
        let warmup_wheel = router.wheel(warmup_stage);
        let measurement_wheel = router.wheel(measurement_stage);
        let router = Arc::new(DiagnosticRouting {
            router,
            warmup_wheel,
            measurement_wheel,
        });

        info!(
            "preparing {} diagnostic Wire v3 sessions with SNAPSHOT ISOLATION and PREPARE_SET",
            OFFICIAL_CLIENTS
        );
        let sessions = self
            .open_sessions(response_timeout, Arc::clone(&catalog))
            .await?;
        let cancelled = Arc::new(AtomicBool::new(false));
        let ready_barrier = Arc::new(Barrier::new(usize::from(OFFICIAL_CLIENTS) + 1));
        let start_barrier = Arc::new(Barrier::new(usize::from(OFFICIAL_CLIENTS) + 1));
        let (timeline_sender, timeline_receiver) = watch::channel(None);
        let mut workers = JoinSet::new();

        for (worker, client) in sessions.into_iter().enumerate() {
            let routing = Arc::clone(&router);
            let cancelled = Arc::clone(&cancelled);
            let ready_barrier = Arc::clone(&ready_barrier);
            let start_barrier = Arc::clone(&start_barrier);
            let timeline = timeline_receiver.clone();
            let rtt_sim_ms = self.config.rtt_sim_ms;
            workers.spawn(async move {
                run_worker(
                    worker as u16,
                    client,
                    routing,
                    cancelled,
                    ready_barrier,
                    start_barrier,
                    timeline,
                    rtt_sim_ms,
                )
                .await
            });
        }
        drop(timeline_receiver);

        ready_barrier.wait().await;
        start_barrier.wait().await;
        // Sample the shared origin only after the start barrier has actually
        // released. Workers wait on this one-shot timing value before dispatch.
        let phase_start = Instant::now();
        let phase_timing =
            DiagnosticTimeline::new(phase_start, warmup, measurement, phase_tail_grace)?;
        if fast {
            publish_fast_resource_timeline(phase_start, warmup, measurement);
        }
        timeline_sender.send(Some(phase_timing)).map_err(|_| {
            TpccError::Protocol("diagnostic workers left before timing release".to_owned())
        })?;
        info!(
            "mode=non_ranked_diagnostic; all {} clients released for {}s with no think time",
            OFFICIAL_CLIENTS,
            duration.as_secs()
        );

        let mut aggregate = DiagnosticStats::default();
        let mut first_error = None;
        while let Some(joined) = workers.join_next().await {
            match joined {
                Ok(Ok(stats)) => aggregate.merge(stats),
                Ok(Err(error)) => {
                    cancelled.store(true, Ordering::Release);
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(error) => {
                    cancelled.store(true, Ordering::Release);
                    if first_error.is_none() {
                        first_error = Some(TpccError::Protocol(format!(
                            "diagnostic worker task failed: {error}"
                        )));
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }

        Ok(DiagnosticRunResult {
            segment,
            warmup,
            measurement,
            fast,
            response_timeout,
            phase_tail_grace,
            stats: aggregate,
        })
    }

    fn validate_state_binding(&self, seed: u64) -> Result<RuntimeSchema, TpccError> {
        let state_dir = self.config.state_dir.as_deref().ok_or_else(|| {
            TpccError::Protocol("diagnostic workload requires a state directory".to_owned())
        })?;
        let metadata = std::fs::symlink_metadata(state_dir).map_err(|error| {
            TpccError::Protocol(format!(
                "diagnostic state directory must already exist (read-only binding): {error}"
            ))
        })?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(TpccError::Protocol(format!(
                "diagnostic state path is not a real directory: {}",
                state_dir.display()
            )));
        }
        let store = StateStore::open_existing_terminal(state_dir)
            .map_err(|error| TpccError::Protocol(format!("diagnostic state: {error}")))?;
        let dataset = store
            .load_dataset()
            .map_err(|error| TpccError::Protocol(format!("diagnostic dataset state: {error}")))?;
        if dataset.seed != seed
            || dataset.warehouses != i32::from(OFFICIAL_WAREHOUSES)
            || self.config.scale_factor != i32::from(OFFICIAL_WAREHOUSES)
        {
            return Err(TpccError::Protocol(format!(
                "diagnostic dataset state mismatch: state seed/SF={}/{}, CLI seed/SF={}/{}",
                dataset.seed, dataset.warehouses, seed, self.config.scale_factor
            )));
        }
        if dataset.runtime_schema.mode() != SchemaMode::LocalSeedOpaqueV1 {
            return Err(TpccError::Protocol(format!(
                "diagnostic workload requires local_seed_opaque_v1 state, found {}",
                dataset.runtime_schema.mode().as_str()
            )));
        }
        if let Ok(run_id) = std::env::var("RMDB_TPCC_RUN_ID") {
            if dataset.run_id != run_id {
                return Err(TpccError::Protocol(format!(
                    "diagnostic run id mismatch: state={}, environment={run_id}",
                    dataset.run_id
                )));
            }
        }
        Ok(dataset.runtime_schema)
    }

    async fn open_sessions(
        &self,
        response_timeout: Duration,
        catalog: Arc<RuntimeCatalog>,
    ) -> Result<Vec<RmdbClient>, TpccError> {
        let mut tasks = JoinSet::new();
        for worker in 0..OFFICIAL_CLIENTS {
            let host = self.config.host.clone();
            let port = self.config.port;
            let catalog = Arc::clone(&catalog);
            tasks.spawn(async move {
                (
                    worker,
                    open_ranked_session(&host, port, response_timeout, catalog).await,
                )
            });
        }

        let mut sessions: Vec<Option<RmdbClient>> = std::iter::repeat_with(|| None)
            .take(usize::from(OFFICIAL_CLIENTS))
            .collect();
        while let Some(joined) = tasks.join_next().await {
            let (worker, session) = joined.map_err(|error| {
                TpccError::Protocol(format!(
                    "diagnostic session preparation task failed: {error}"
                ))
            })?;
            match session {
                Ok(session) => sessions[usize::from(worker)] = Some(session),
                Err(error) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(TpccError::Protocol(format!(
                        "diagnostic worker {worker} failed before the all-session barrier: {error}"
                    )));
                }
            }
        }

        sessions
            .into_iter()
            .enumerate()
            .map(|(worker, session)| {
                session.ok_or_else(|| {
                    TpccError::Protocol(format!(
                        "diagnostic worker {worker} did not prepare its session"
                    ))
                })
            })
            .collect()
    }
}

struct DiagnosticRouting {
    router: OfficialRouter,
    warmup_wheel: WarehouseWheel,
    measurement_wheel: WarehouseWheel,
}

impl DiagnosticRouting {
    fn wheel(&self, phase: DiagnosticPhase) -> &WarehouseWheel {
        match phase {
            DiagnosticPhase::Warmup => &self.warmup_wheel,
            DiagnosticPhase::Measurement => &self.measurement_wheel,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiagnosticPhase {
    Warmup,
    Measurement,
}

#[derive(Clone, Copy, Debug)]
struct DiagnosticTimeline {
    measurement_start: Instant,
    warmup_drain_deadline: Instant,
    stop_at: Instant,
    drain_deadline: Instant,
}

impl DiagnosticTimeline {
    fn new(
        start: Instant,
        warmup: Duration,
        measurement: Duration,
        grace: Duration,
    ) -> Result<Self, TpccError> {
        let measurement_start = start.checked_add(warmup).ok_or_else(|| {
            TpccError::Protocol("diagnostic workload deadline overflow".to_owned())
        })?;
        let warmup_drain_deadline = measurement_start
            .checked_add(grace)
            .ok_or_else(|| TpccError::Protocol("diagnostic grace deadline overflow".to_owned()))?;
        let stop_at = measurement_start.checked_add(measurement).ok_or_else(|| {
            TpccError::Protocol("diagnostic workload deadline overflow".to_owned())
        })?;
        let drain_deadline = stop_at
            .checked_add(grace)
            .ok_or_else(|| TpccError::Protocol("diagnostic grace deadline overflow".to_owned()))?;
        Ok(Self {
            measurement_start,
            warmup_drain_deadline,
            stop_at,
            drain_deadline,
        })
    }

    fn phase(self, now: Instant) -> Option<DiagnosticPhase> {
        if now < self.measurement_start {
            Some(DiagnosticPhase::Warmup)
        } else if now < self.stop_at {
            Some(DiagnosticPhase::Measurement)
        } else {
            None
        }
    }

    fn attempt_deadline(self, phase: DiagnosticPhase) -> Instant {
        match phase {
            DiagnosticPhase::Warmup => self.warmup_drain_deadline,
            DiagnosticPhase::Measurement => self.drain_deadline,
        }
    }
}

async fn run_worker(
    worker: u16,
    mut client: RmdbClient,
    routing: Arc<DiagnosticRouting>,
    cancelled: Arc<AtomicBool>,
    ready_barrier: Arc<Barrier>,
    start_barrier: Arc<Barrier>,
    mut timeline: watch::Receiver<Option<DiagnosticTimeline>>,
    rtt_sim_ms: u64,
) -> Result<DiagnosticStats, TpccError> {
    ready_barrier.wait().await;
    start_barrier.wait().await;
    let timeline = loop {
        if let Some(timing) = *timeline.borrow_and_update() {
            break timing;
        }
        timeline.changed().await.map_err(|_| {
            TpccError::Protocol("diagnostic timing channel closed before release".to_owned())
        })?;
    };
    let mut sequence =
        ClientSequence::new(worker).map_err(|error| TpccError::Protocol(error.to_string()))?;
    let mut active_phase = None;
    let mut stats = DiagnosticStats::default();

    loop {
        if cancelled.load(Ordering::Acquire) {
            break;
        }
        let Some(phase) = timeline.phase(Instant::now()) else {
            break;
        };
        if active_phase != Some(phase) {
            sequence = ClientSequence::new(worker)
                .map_err(|error| TpccError::Protocol(error.to_string()))?;
            active_phase = Some(phase);
        }
        let workload = Final2026Workload::new(&routing.router, routing.wheel(phase));
        let selected = workload
            .select(&mut sequence)
            .map_err(|error| TpccError::Protocol(error.to_string()))?;
        let selected_at = Instant::now();
        // Selection is local and may race a phase boundary. Discard an old
        // phase selection instead of dispatching it in the next phase; the
        // next loop resets that phase's sequence to txn_no zero.
        if timeline.phase(selected_at) != Some(phase) {
            continue;
        }
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let frozen = FrozenTransaction::new(selected, timestamp)
            .map_err(|message| TpccError::Protocol(message.to_owned()))?;
        let kind = frozen.ticket().kind();
        let measured = phase == DiagnosticPhase::Measurement;
        let mut logical_attempt_recorded = false;

        loop {
            if cancelled.load(Ordering::Acquire) {
                return Ok(stats);
            }
            let attempt_started = Instant::now();
            if timeline.phase(attempt_started) != Some(phase) {
                break;
            }
            if measured {
                if !logical_attempt_recorded {
                    stats.record_selection(kind);
                    logical_attempt_recorded = true;
                }
                stats.record_physical_attempt();
            }
            let attempt_deadline = timeline.attempt_deadline(phase);
            // Environment-alignment knob: simulate the official cross-host
            // round trip before each physical attempt (0 = loopback). This is
            // a non-ranked diagnostic option because it adds per-attempt think
            // time the public contract forbids.
            if rtt_sim_ms > 0 {
                let rtt = Duration::from_millis(rtt_sim_ms);
                if let Some(remaining) = attempt_deadline.checked_duration_since(Instant::now()) {
                    tokio::time::sleep(rtt.min(remaining)).await;
                }
            }
            let response =
                tokio::time::timeout_at(attempt_deadline, dispatch::execute(&mut client, &frozen))
                    .await;
            let completed_at = Instant::now();
            if completed_at > attempt_deadline {
                cancelled.store(true, Ordering::Release);
                return Err(TpccError::Timeout {
                    context: format!(
                        "non-ranked diagnostic worker {worker} exceeded its absolute \
                         phase drain deadline"
                    ),
                });
            }

            match response {
                Err(_) => {
                    cancelled.store(true, Ordering::Release);
                    return Err(TpccError::Timeout {
                        context: format!(
                            "non-ranked diagnostic worker {worker} exceeded its absolute \
                             phase drain deadline"
                        ),
                    });
                }
                Ok(Ok(outcome)) => {
                    if measured {
                        stats.record_terminal(
                            kind,
                            frozen.ticket().route().home_warehouse,
                            &outcome,
                            completed_at >= timeline.stop_at,
                            completed_at.saturating_duration_since(selected_at),
                        );
                    }
                    break;
                }
                Ok(Err(error)) if error.is_retryable_abort() => {
                    if measured {
                        stats.retryable_aborts += 1;
                    }
                    // A retry preserves `frozen`, but it may only start before
                    // the current phase cutoff just like the ranked scheduler.
                    if timeline.phase(Instant::now()) != Some(phase) {
                        break;
                    }
                    // Retry the exact frozen transaction directly. Only the
                    // phase cutoff can stop another physical attempt.
                }
                Ok(Err(error)) => {
                    cancelled.store(true, Ordering::Release);
                    return Err(TpccError::Protocol(format!(
                        "non-ranked diagnostic transaction failed: {error}"
                    )));
                }
            }
        }
    }

    Ok(stats)
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DiagnosticFamilyStats {
    attempted: u64,
    committed: u64,
    terminals: u64,
    grace_tail_committed: u64,
}

impl DiagnosticFamilyStats {
    fn commit_rate(self) -> f64 {
        if self.attempted == 0 {
            0.0
        } else {
            self.committed as f64 / self.attempted as f64
        }
    }

    fn merge(&mut self, other: Self) {
        self.attempted = self.attempted.saturating_add(other.attempted);
        self.committed = self.committed.saturating_add(other.committed);
        self.terminals = self.terminals.saturating_add(other.terminals);
        self.grace_tail_committed = self
            .grace_tail_committed
            .saturating_add(other.grace_tail_committed);
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DiagnosticStats {
    selected: u64,
    physical_attempts: u64,
    committed: u64,
    expected_rollbacks: u64,
    retryable_aborts: u64,
    abandoned: u64,
    grace_tail: u64,
    delivery_processed: u64,
    warehouses: [u64; 50],
    new_order_latencies: Vec<Duration>,
    families: [DiagnosticFamilyStats; DIAGNOSTIC_FAMILIES.len()],
}

impl Default for DiagnosticStats {
    fn default() -> Self {
        Self {
            selected: 0,
            physical_attempts: 0,
            committed: 0,
            expected_rollbacks: 0,
            retryable_aborts: 0,
            abandoned: 0,
            grace_tail: 0,
            delivery_processed: 0,
            warehouses: [0; 50],
            new_order_latencies: Vec::new(),
            families: [DiagnosticFamilyStats::default(); DIAGNOSTIC_FAMILIES.len()],
        }
    }
}

impl DiagnosticStats {
    fn record_selection(&mut self, kind: TransactionKind) {
        self.selected = self.selected.saturating_add(1);
        let family = &mut self.families[diagnostic_family_index(kind)];
        family.attempted = family.attempted.saturating_add(1);
    }

    fn record_physical_attempt(&mut self) {
        self.physical_attempts = self.physical_attempts.saturating_add(1);
    }

    fn record_terminal(
        &mut self,
        kind: TransactionKind,
        warehouse: u16,
        outcome: &RankedTransactionOutcome,
        grace_tail: bool,
        latency: Duration,
    ) {
        let family = &mut self.families[diagnostic_family_index(kind)];
        family.terminals = family.terminals.saturating_add(1);
        match outcome {
            RankedTransactionOutcome::Committed(_) => {
                self.committed = self.committed.saturating_add(1);
                family.committed = family.committed.saturating_add(1);
                if grace_tail {
                    family.grace_tail_committed = family.grace_tail_committed.saturating_add(1);
                } else {
                    if let Some(slot) = self.warehouses.get_mut(usize::from(warehouse - 1)) {
                        *slot = slot.saturating_add(1);
                    }
                    if kind == TransactionKind::NewOrder {
                        self.new_order_latencies.push(latency);
                    }
                    if let RankedTransactionOutcome::Committed(commit) = outcome {
                        self.delivery_processed = self
                            .delivery_processed
                            .saturating_add(commit.delivery_processed());
                    }
                }
            }
            RankedTransactionOutcome::ExpectedRollback => {
                self.expected_rollbacks = self.expected_rollbacks.saturating_add(1);
                if !grace_tail {
                    if let Some(slot) = self.warehouses.get_mut(usize::from(warehouse - 1)) {
                        *slot = slot.saturating_add(1);
                    }
                }
            }
        }
        if grace_tail {
            self.grace_tail = self.grace_tail.saturating_add(1);
        }
    }

    fn merge(&mut self, other: Self) {
        self.selected = self.selected.saturating_add(other.selected);
        self.physical_attempts = self
            .physical_attempts
            .saturating_add(other.physical_attempts);
        self.committed = self.committed.saturating_add(other.committed);
        self.expected_rollbacks = self
            .expected_rollbacks
            .saturating_add(other.expected_rollbacks);
        self.retryable_aborts = self.retryable_aborts.saturating_add(other.retryable_aborts);
        self.abandoned = self.abandoned.saturating_add(other.abandoned);
        self.grace_tail = self.grace_tail.saturating_add(other.grace_tail);
        self.delivery_processed = self
            .delivery_processed
            .saturating_add(other.delivery_processed);
        for (slot, incoming) in self.warehouses.iter_mut().zip(other.warehouses) {
            *slot = slot.saturating_add(incoming);
        }
        self.new_order_latencies.extend(other.new_order_latencies);
        for (family, incoming) in self.families.iter_mut().zip(other.families) {
            family.merge(incoming);
        }
    }

    fn family(&self, kind: TransactionKind) -> DiagnosticFamilyStats {
        self.families[diagnostic_family_index(kind)]
    }

    fn family_report_line(&self, kind: TransactionKind, label: &str) -> String {
        let family = self.family(kind);
        format!(
            "diagnostic_family={label},attempted={},committed={},commit_rate={:.6},grace_tail_committed={}",
            family.attempted,
            family.committed,
            family.commit_rate(),
            family.grace_tail_committed
        )
    }
}

const fn diagnostic_family_index(kind: TransactionKind) -> usize {
    match kind {
        TransactionKind::NewOrder => 0,
        TransactionKind::Payment => 1,
        TransactionKind::OrderStatus => 2,
        TransactionKind::Delivery => 3,
        TransactionKind::StockLevel => 4,
    }
}

pub struct DiagnosticRunResult {
    segment: DiagnosticSegment,
    warmup: Duration,
    measurement: Duration,
    fast: bool,
    response_timeout: Duration,
    phase_tail_grace: Duration,
    stats: DiagnosticStats,
}

impl DiagnosticRunResult {
    pub fn print_report(&self) {
        println!("=== TPCC final2026 diagnostic workload ===");
        println!(
            "mode={}",
            if self.fast {
                "non_ranked_fast"
            } else {
                "non_ranked_diagnostic"
            }
        );
        println!("segment={}", self.segment.as_str());
        println!(
            "clients={},warmup_seconds={},measurement_windows=1,measurement_seconds={},mix=45/43/4/4/4,no_think_time=true",
            OFFICIAL_CLIENTS,
            self.warmup.as_secs(),
            self.measurement.as_secs()
        );
        println!(
            "local_safety=response_timeout:{}s,phase_tail_grace:{}s (official values unpublished)",
            self.response_timeout.as_secs(),
            self.phase_tail_grace.as_secs()
        );
        println!(
            "terminal_mix=new_order:{},payment:{},order_status:{},delivery:{},stock_level:{}",
            self.stats.family(TransactionKind::NewOrder).terminals,
            self.stats.family(TransactionKind::Payment).terminals,
            self.stats.family(TransactionKind::OrderStatus).terminals,
            self.stats.family(TransactionKind::Delivery).terminals,
            self.stats.family(TransactionKind::StockLevel).terminals
        );
        println!(
            "selected={},physical_attempts={},committed={},expected_rollback={},retry_abort={},abandoned={},grace_tail={}",
            self.stats.selected,
            self.stats.physical_attempts,
            self.stats.committed,
            self.stats.expected_rollbacks,
            self.stats.retryable_aborts,
            self.stats.abandoned,
            self.stats.grace_tail
        );
        println!(
            "diagnostic_commit_semantics=successful_physical_commit_including_grace_tail,expected_rollback_excluded=true"
        );
        for (kind, label) in DIAGNOSTIC_FAMILIES {
            println!("{}", self.stats.family_report_line(kind, label));
        }
        let new_order_family = self.stats.family(TransactionKind::NewOrder);
        let new_orders = new_order_family
            .committed
            .saturating_sub(new_order_family.grace_tail_committed);
        let mut latencies = self.stats.new_order_latencies.clone();
        latencies.sort_unstable();
        println!(
            "new_order_per_min={:.3},delivery_processed={},warehouses={}/50",
            new_orders as f64 * 60.0 / self.measurement.as_secs_f64(),
            self.stats.delivery_processed,
            self.stats
                .warehouses
                .iter()
                .filter(|&&count| count > 0)
                .count()
        );
        println!(
            "new_order_latency_ms=p50:{},p99:{}",
            format_latency(nearest_rank(&latencies, 50)),
            format_latency(nearest_rank(&latencies, 99))
        );
        println!(
            "state_artifacts_written={}",
            if self.fast {
                "none"
            } else {
                "append_only_phase_claim_and_receipt"
            }
        );
    }
}

fn nearest_rank(samples: &[Duration], percentile: usize) -> Option<Duration> {
    (!samples.is_empty()).then(|| samples[(percentile * samples.len()).div_ceil(100) - 1])
}

fn format_latency(value: Option<Duration>) -> String {
    value
        .map(|duration| format!("{:.3}", duration.as_secs_f64() * 1_000.0))
        .unwrap_or_else(|| "unavailable".to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diagnostic_segments_restart_at_zero_in_distinct_routing_domains() {
        let router = OfficialRouter::new(WorkloadSeed(0x2026_0715));
        let warmup_stage = diagnostic_stage(DiagnosticSegment::Warmup);
        let observation_stage = diagnostic_stage(DiagnosticSegment::Observation);
        assert_ne!(warmup_stage, observation_stage);

        let warmup_wheel = router.wheel(warmup_stage);
        let observation_wheel = router.wheel(observation_stage);
        let mut warmup_sequence = ClientSequence::new(0).unwrap();
        let mut observation_sequence = ClientSequence::new(0).unwrap();
        let warmup_ticket = Final2026Workload::new(&router, &warmup_wheel)
            .select(&mut warmup_sequence)
            .unwrap();
        let observation_ticket = Final2026Workload::new(&router, &observation_wheel)
            .select(&mut observation_sequence)
            .unwrap();

        assert_eq!(warmup_ticket.route().txn_no, 0);
        assert_eq!(observation_ticket.route().txn_no, 0);
        assert_eq!(warmup_ticket.route().stage, warmup_stage);
        assert_eq!(observation_ticket.route().stage, observation_stage);
        assert_eq!(warmup_sequence.next_txn_no(), 1);
        assert_eq!(observation_sequence.next_txn_no(), 1);
    }

    #[test]
    fn fast_stages_restart_at_zero_in_distinct_routing_domains() {
        let router = OfficialRouter::new(WorkloadSeed(0x2026_0731));
        assert_ne!(PRELIMINARY_WARMUP_STAGE, PRELIMINARY_MEASUREMENT_STAGE);

        let warmup_wheel = router.wheel(PRELIMINARY_WARMUP_STAGE);
        let measurement_wheel = router.wheel(PRELIMINARY_MEASUREMENT_STAGE);
        let mut warmup_sequence = ClientSequence::new(31).unwrap();
        let mut measurement_sequence = ClientSequence::new(31).unwrap();
        let warmup_ticket = Final2026Workload::new(&router, &warmup_wheel)
            .select(&mut warmup_sequence)
            .unwrap();
        let measurement_ticket = Final2026Workload::new(&router, &measurement_wheel)
            .select(&mut measurement_sequence)
            .unwrap();

        assert_eq!(warmup_ticket.route().txn_no, 0);
        assert_eq!(measurement_ticket.route().txn_no, 0);
        assert_eq!(warmup_ticket.route().stage, PRELIMINARY_WARMUP_STAGE);
        assert_eq!(
            measurement_ticket.route().stage,
            PRELIMINARY_MEASUREMENT_STAGE
        );
    }

    #[test]
    fn absolute_attempt_deadlines_are_bounded_per_phase() {
        let start = Instant::now();
        let timeline = DiagnosticTimeline::new(
            start,
            Duration::from_secs(30),
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap();

        assert_eq!(
            timeline.attempt_deadline(DiagnosticPhase::Warmup),
            start + Duration::from_secs(35)
        );
        assert_eq!(
            timeline.attempt_deadline(DiagnosticPhase::Measurement),
            start + Duration::from_secs(95)
        );
    }

    #[test]
    fn timeline_uses_half_open_warmup_and_measurement_intervals() {
        let start = Instant::now();
        let timeline = DiagnosticTimeline::new(
            start,
            Duration::from_secs(30),
            Duration::from_secs(60),
            Duration::from_secs(5),
        )
        .unwrap();

        assert_eq!(
            timeline.phase(start + Duration::from_secs(29)),
            Some(DiagnosticPhase::Warmup)
        );
        assert_eq!(
            timeline.phase(start + Duration::from_secs(30)),
            Some(DiagnosticPhase::Measurement)
        );
        assert_eq!(
            timeline.phase(start + Duration::from_secs(89)),
            Some(DiagnosticPhase::Measurement)
        );
        assert_eq!(timeline.phase(start + Duration::from_secs(90)), None);
        assert_eq!(timeline.phase(start + Duration::from_secs(91)), None);
    }

    #[test]
    fn diagnostic_stats_separate_logical_selection_from_physical_retries() {
        let mut stats = DiagnosticStats {
            retryable_aborts: 1,
            ..DiagnosticStats::default()
        };
        stats.record_selection(TransactionKind::Payment);
        stats.record_selection(TransactionKind::NewOrder);
        stats.record_physical_attempt();
        stats.record_physical_attempt();
        stats.record_physical_attempt();
        stats.record_terminal(
            TransactionKind::Payment,
            1,
            &RankedTransactionOutcome::Committed(crate::ranking::runner::RankedCommit::OrderStatus),
            false,
            Duration::ZERO,
        );
        stats.record_terminal(
            TransactionKind::NewOrder,
            1,
            &RankedTransactionOutcome::ExpectedRollback,
            true,
            Duration::ZERO,
        );

        assert_eq!(
            stats.family(TransactionKind::Payment),
            DiagnosticFamilyStats {
                attempted: 1,
                committed: 1,
                terminals: 1,
                grace_tail_committed: 0,
            }
        );
        assert_eq!(
            stats.family(TransactionKind::NewOrder),
            DiagnosticFamilyStats {
                attempted: 1,
                committed: 0,
                terminals: 1,
                grace_tail_committed: 0,
            }
        );
        assert_eq!(stats.physical_attempts, 3);
        assert_eq!(stats.committed, 1);
        assert_eq!(stats.expected_rollbacks, 1);
        assert_eq!(stats.retryable_aborts, 1);
        assert_eq!(stats.grace_tail, 1);
        assert_eq!(stats.selected, 2);
    }

    #[test]
    fn grace_tail_commit_remains_a_physical_diagnostic_commit() {
        let mut stats = DiagnosticStats::default();
        stats.record_selection(TransactionKind::Delivery);
        stats.record_physical_attempt();
        stats.record_terminal(
            TransactionKind::Delivery,
            1,
            &RankedTransactionOutcome::Committed(crate::ranking::runner::RankedCommit::Delivery(
                Vec::new(),
            )),
            true,
            Duration::ZERO,
        );

        let family = stats.family(TransactionKind::Delivery);
        assert_eq!(family.committed, 1);
        assert_eq!(family.grace_tail_committed, 1);
        assert_eq!(family.commit_rate(), 1.0);
        assert_eq!(stats.committed, 1);
        assert_eq!(stats.grace_tail, 1);
    }

    #[test]
    fn family_report_is_stable_and_zero_attempt_rate_is_safe() {
        let stats = DiagnosticStats::default();

        assert_eq!(
            stats.family_report_line(TransactionKind::StockLevel, "stock_level"),
            "diagnostic_family=stock_level,attempted=0,committed=0,commit_rate=0.000000,grace_tail_committed=0"
        );
    }

    #[test]
    fn merge_preserves_per_family_attempt_and_commit_totals() {
        let mut left = DiagnosticStats::default();
        left.record_selection(TransactionKind::OrderStatus);
        left.record_physical_attempt();

        let mut right = DiagnosticStats::default();
        right.record_selection(TransactionKind::OrderStatus);
        right.record_physical_attempt();
        right.record_terminal(
            TransactionKind::OrderStatus,
            1,
            &RankedTransactionOutcome::Committed(crate::ranking::runner::RankedCommit::OrderStatus),
            false,
            Duration::ZERO,
        );

        left.merge(right);

        let family = left.family(TransactionKind::OrderStatus);
        assert_eq!(family.attempted, 2);
        assert_eq!(family.committed, 1);
        assert_eq!(family.terminals, 1);
        assert_eq!(family.commit_rate(), 0.5);
        assert_eq!(left.physical_attempts, 2);
        assert_eq!(left.committed, 1);
    }
}
