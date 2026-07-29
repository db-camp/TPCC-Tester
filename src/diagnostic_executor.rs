//! Explicitly non-ranked final-2026 diagnostic workload.
//!
//! This executor intentionally does not use the ranked phase scheduler or
//! durable ledger.  It exists only for the post-pass 10-second warmup and
//! 60-second tracing workload, while retaining the published transaction mix,
//! hotspot routing, prepared sessions, retry classification, and no-think-time
//! dispatch contract.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Local;
use tokio::sync::{watch, Barrier};
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::info;

use crate::config::{Config, DiagnosticSegment, ResolvedProfile};
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
        let duration = Duration::from_secs(duration_seconds);
        let router = OfficialRouter::new(WorkloadSeed(seed));
        let wheel = router.wheel(diagnostic_stage(segment));
        let router = Arc::new(DiagnosticRouting { router, wheel });

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
            workers.spawn(async move {
                run_worker(
                    worker as u16,
                    client,
                    routing,
                    cancelled,
                    ready_barrier,
                    start_barrier,
                    timeline,
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
        let phase_timing = DiagnosticTimeline::new(phase_start, duration, phase_tail_grace)?;
        timeline_sender.send(Some(phase_timing)).map_err(|_| {
            TpccError::Protocol("diagnostic workers left before timing release".to_owned())
        })?;
        info!(
            "mode=non_ranked_diagnostic; all {} clients released for {}s with no think time",
            OFFICIAL_CLIENTS, duration_seconds
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
            duration,
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
        let store = StateStore::open_existing(state_dir)
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
    wheel: WarehouseWheel,
}

#[derive(Clone, Copy, Debug)]
struct DiagnosticTimeline {
    stop_at: Instant,
    drain_deadline: Instant,
}

impl DiagnosticTimeline {
    fn new(start: Instant, duration: Duration, grace: Duration) -> Result<Self, TpccError> {
        let stop_at = start.checked_add(duration).ok_or_else(|| {
            TpccError::Protocol("diagnostic workload deadline overflow".to_owned())
        })?;
        let drain_deadline = stop_at
            .checked_add(grace)
            .ok_or_else(|| TpccError::Protocol("diagnostic grace deadline overflow".to_owned()))?;
        Ok(Self {
            stop_at,
            drain_deadline,
        })
    }

    fn may_start(self, now: Instant) -> bool {
        now < self.stop_at
    }

    fn attempt_deadline(self) -> Instant {
        self.drain_deadline
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
    let workload = Final2026Workload::new(&routing.router, &routing.wheel);
    let mut stats = DiagnosticStats::default();

    while !cancelled.load(Ordering::Acquire) && timeline.may_start(Instant::now()) {
        let selected = workload
            .select(&mut sequence)
            .map_err(|error| TpccError::Protocol(error.to_string()))?;
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let frozen = FrozenTransaction::new(selected, timestamp)
            .map_err(|message| TpccError::Protocol(message.to_owned()))?;
        let kind = frozen.ticket().kind();

        // Selection is local and may race the cutoff.  Recheck immediately
        // before the first request so no transaction begins after stop_at.
        if !timeline.may_start(Instant::now()) {
            break;
        }
        stats.selected += 1;

        loop {
            if cancelled.load(Ordering::Acquire) {
                return Ok(stats);
            }
            let attempt_started = Instant::now();
            if !timeline.may_start(attempt_started) {
                stats.abandoned += 1;
                break;
            }
            stats.record_attempt(kind);
            let attempt_deadline = timeline.attempt_deadline();
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
                    stats.record_terminal(kind, &outcome, completed_at >= timeline.stop_at);
                    break;
                }
                Ok(Err(error)) if error.is_retryable_abort() => {
                    stats.retryable_aborts += 1;
                    // A retry preserves `frozen`, but it may only start before
                    // the workload cutoff just like the ranked scheduler.
                    if !timeline.may_start(Instant::now()) {
                        stats.abandoned += 1;
                        break;
                    }
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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct DiagnosticStats {
    selected: u64,
    physical_attempts: u64,
    committed: u64,
    expected_rollbacks: u64,
    retryable_aborts: u64,
    abandoned: u64,
    grace_tail: u64,
    families: [DiagnosticFamilyStats; DIAGNOSTIC_FAMILIES.len()],
}

impl DiagnosticStats {
    fn record_attempt(&mut self, kind: TransactionKind) {
        self.physical_attempts = self.physical_attempts.saturating_add(1);
        let family = &mut self.families[diagnostic_family_index(kind)];
        family.attempted = family.attempted.saturating_add(1);
    }

    fn record_terminal(
        &mut self,
        kind: TransactionKind,
        outcome: &RankedTransactionOutcome,
        grace_tail: bool,
    ) {
        let family = &mut self.families[diagnostic_family_index(kind)];
        family.terminals = family.terminals.saturating_add(1);
        match outcome {
            RankedTransactionOutcome::Committed(_) => {
                self.committed = self.committed.saturating_add(1);
                family.committed = family.committed.saturating_add(1);
                if grace_tail {
                    family.grace_tail_committed = family.grace_tail_committed.saturating_add(1);
                }
            }
            RankedTransactionOutcome::ExpectedRollback => {
                self.expected_rollbacks = self.expected_rollbacks.saturating_add(1);
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
    duration: Duration,
    response_timeout: Duration,
    phase_tail_grace: Duration,
    stats: DiagnosticStats,
}

impl DiagnosticRunResult {
    pub fn print_report(&self) {
        println!("=== TPCC final2026 diagnostic workload ===");
        println!("mode=non_ranked_diagnostic");
        println!("segment={}", self.segment.as_str());
        println!(
            "clients={},duration_seconds={},mix=45/43/4/4/4,no_think_time=true",
            OFFICIAL_CLIENTS,
            self.duration.as_secs()
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
        println!("state_artifacts_written=append_only_phase_claim_and_receipt");
    }
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
    fn absolute_attempt_deadline_is_the_phase_drain_deadline() {
        let start = Instant::now();
        let timeline =
            DiagnosticTimeline::new(start, Duration::from_secs(10), Duration::from_secs(5))
                .unwrap();

        assert_eq!(timeline.attempt_deadline(), start + Duration::from_secs(15));
    }

    #[test]
    fn transaction_start_is_forbidden_at_and_after_cutoff() {
        let start = Instant::now();
        let timeline =
            DiagnosticTimeline::new(start, Duration::from_secs(10), Duration::from_secs(5))
                .unwrap();

        assert!(timeline.may_start(start + Duration::from_secs(9)));
        assert!(!timeline.may_start(start + Duration::from_secs(10)));
        assert!(!timeline.may_start(start + Duration::from_secs(11)));
    }

    #[test]
    fn diagnostic_stats_classify_every_dispatch_and_only_successful_commits() {
        let mut stats = DiagnosticStats {
            selected: 2,
            retryable_aborts: 1,
            ..DiagnosticStats::default()
        };
        stats.record_attempt(TransactionKind::Payment);
        stats.record_attempt(TransactionKind::Payment);
        stats.record_attempt(TransactionKind::NewOrder);
        stats.record_terminal(
            TransactionKind::Payment,
            &RankedTransactionOutcome::Committed(crate::ranking::runner::RankedCommit::OrderStatus),
            false,
        );
        stats.record_terminal(
            TransactionKind::NewOrder,
            &RankedTransactionOutcome::ExpectedRollback,
            true,
        );

        assert_eq!(
            stats.family(TransactionKind::Payment),
            DiagnosticFamilyStats {
                attempted: 2,
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
    }

    #[test]
    fn grace_tail_commit_remains_a_physical_diagnostic_commit() {
        let mut stats = DiagnosticStats::default();
        stats.record_attempt(TransactionKind::Delivery);
        stats.record_terminal(
            TransactionKind::Delivery,
            &RankedTransactionOutcome::Committed(crate::ranking::runner::RankedCommit::Delivery(
                Vec::new(),
            )),
            true,
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
        left.record_attempt(TransactionKind::OrderStatus);

        let mut right = DiagnosticStats::default();
        right.record_attempt(TransactionKind::OrderStatus);
        right.record_terminal(
            TransactionKind::OrderStatus,
            &RankedTransactionOutcome::Committed(crate::ranking::runner::RankedCommit::OrderStatus),
            false,
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
