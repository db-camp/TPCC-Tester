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
use crate::profile::{TransactionKind, OFFICIAL_CLIENTS};
use crate::ranking::dispatch::{self, FrozenTransaction};
use crate::ranking::runner::RankedTransactionOutcome;
use crate::ranking::session::open_ranked_session;
use crate::routing::{ClientSequence, OfficialRouter, StageId, WarehouseWheel, WorkloadSeed};
use crate::workload::Final2026Workload;

const DIAGNOSTIC_STAGE: StageId = StageId::custom(0x6469_6167_3230_3236);

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

        let response_timeout = Duration::from_secs(self.config.response_timeout_seconds);
        let phase_tail_grace = Duration::from_secs(self.config.phase_tail_grace_seconds);
        let duration = Duration::from_secs(duration_seconds);
        let router = OfficialRouter::new(WorkloadSeed(seed));
        let wheel = router.wheel(DIAGNOSTIC_STAGE);
        let router = Arc::new(DiagnosticRouting { router, wheel });

        info!(
            "preparing {} diagnostic Wire v3 sessions with SNAPSHOT ISOLATION and PREPARE_SET",
            OFFICIAL_CLIENTS
        );
        let sessions = self.open_sessions(response_timeout).await?;
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

    async fn open_sessions(
        &self,
        response_timeout: Duration,
    ) -> Result<Vec<RmdbClient>, TpccError> {
        let mut tasks = JoinSet::new();
        for worker in 0..OFFICIAL_CLIENTS {
            let host = self.config.host.clone();
            let port = self.config.port;
            tasks.spawn(async move {
                (
                    worker,
                    open_ranked_session(&host, port, response_timeout).await,
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
            stats.physical_attempts += 1;
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
struct DiagnosticStats {
    selected: u64,
    physical_attempts: u64,
    committed: u64,
    expected_rollbacks: u64,
    retryable_aborts: u64,
    abandoned: u64,
    grace_tail: u64,
    new_order: u64,
    payment: u64,
    order_status: u64,
    delivery: u64,
    stock_level: u64,
}

impl DiagnosticStats {
    fn record_terminal(
        &mut self,
        kind: TransactionKind,
        outcome: &RankedTransactionOutcome,
        grace_tail: bool,
    ) {
        match outcome {
            RankedTransactionOutcome::Committed(_) => self.committed += 1,
            RankedTransactionOutcome::ExpectedRollback => self.expected_rollbacks += 1,
        }
        if grace_tail {
            self.grace_tail += 1;
        }
        match kind {
            TransactionKind::NewOrder => self.new_order += 1,
            TransactionKind::Payment => self.payment += 1,
            TransactionKind::OrderStatus => self.order_status += 1,
            TransactionKind::Delivery => self.delivery += 1,
            TransactionKind::StockLevel => self.stock_level += 1,
        }
    }

    fn merge(&mut self, other: Self) {
        self.selected += other.selected;
        self.physical_attempts += other.physical_attempts;
        self.committed += other.committed;
        self.expected_rollbacks += other.expected_rollbacks;
        self.retryable_aborts += other.retryable_aborts;
        self.abandoned += other.abandoned;
        self.grace_tail += other.grace_tail;
        self.new_order += other.new_order;
        self.payment += other.payment;
        self.order_status += other.order_status;
        self.delivery += other.delivery;
        self.stock_level += other.stock_level;
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
            self.stats.new_order,
            self.stats.payment,
            self.stats.order_status,
            self.stats.delivery,
            self.stats.stock_level
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
        println!("state_artifacts_written=append_only_phase_claim_and_receipt");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn diagnostic_stats_keep_retry_attempts_out_of_terminal_mix() {
        let mut stats = DiagnosticStats {
            selected: 2,
            physical_attempts: 3,
            retryable_aborts: 1,
            ..DiagnosticStats::default()
        };
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

        assert_eq!(stats.payment, 1);
        assert_eq!(stats.new_order, 1);
        assert_eq!(stats.committed, 1);
        assert_eq!(stats.expected_rollbacks, 1);
        assert_eq!(stats.retryable_aborts, 1);
        assert_eq!(stats.grace_tail, 1);
    }
}
