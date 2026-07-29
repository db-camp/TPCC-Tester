//! Native final-2026 ranked timeline and worker pool.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use chrono::Local;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::{Config, ResolvedProfile};
use crate::connection::client::RmdbClient;
use crate::error::TpccError;
use crate::measurement::{MeasurementSummary, WindowStats, FORMAL_WINDOW_COUNT};
use crate::phases::{
    AttemptDisposition, AttemptOutcome, EventRecorder, Final2026Scheduler, LocalRuntimeLimits,
    MonotonicClock, PhaseId, PhaseScheduleConfig, PreparedSessionId, SchedulerError,
    SystemMonotonicClock, TransactionIdentity, WorkerId,
};
use crate::ranking::dispatch::{self, FrozenTransaction};
use crate::ranking::ledger::{LedgerError, RunLedger};
use crate::ranking::runner::RankedTransactionOutcome;
use crate::ranking::session::open_ranked_session;
use crate::routing::{ClientSequence, OfficialRouter, StageId, WarehouseWheel, WorkloadSeed};
use crate::workload::Final2026Workload;

#[derive(Default)]
struct NoopRecorder;

impl EventRecorder for NoopRecorder {
    fn record(&mut self, _event: crate::phases::SchedulerEvent) {}
}

type Scheduler = Final2026Scheduler<SystemMonotonicClock, NoopRecorder>;

pub struct BenchmarkExecutor {
    config: Config,
    effective: ResolvedProfile,
}

impl BenchmarkExecutor {
    pub fn new(config: Config, effective: ResolvedProfile) -> Self {
        Self { config, effective }
    }

    pub async fn run(&self) -> Result<Final2026RunResult, TpccError> {
        let profile = &self.effective.final2026;
        let seed = self.effective.seed.ok_or_else(|| {
            TpccError::Protocol("ranked run requires an explicit seed".to_owned())
        })?;
        let response_timeout = Duration::from_secs(self.config.response_timeout_seconds);
        let limits = LocalRuntimeLimits::new(
            response_timeout,
            Duration::from_secs(self.config.phase_tail_grace_seconds),
        )
        .map_err(scheduler_error)?;
        let schedule =
            PhaseScheduleConfig::new(profile.clients, profile.warmup, profile.measurement_window)
                .map_err(scheduler_error)?;

        let router = if self.effective.is_ranked_configuration() {
            OfficialRouter::new(WorkloadSeed(seed))
        } else {
            OfficialRouter::new_for_warehouses(WorkloadSeed(seed), profile.warehouses)
                .map_err(|error| TpccError::Protocol(error.to_string()))?
        };
        let routing = Arc::new(RunRouting::new(router));
        let monotonic_clock = SystemMonotonicClock::new();
        let scheduler = Arc::new(Mutex::new(
            Final2026Scheduler::new_with_schedule(
                monotonic_clock.clone(),
                NoopRecorder,
                limits,
                *routing.router.hot_warehouses(),
                schedule,
            )
            .map_err(scheduler_error)?,
        ));

        info!(
            "preparing {} persistent Wire v3 sessions before the timing barrier",
            profile.clients
        );
        let sessions = self
            .open_sessions(profile.clients, response_timeout)
            .await?;
        {
            let mut state = lock_scheduler(&scheduler)?;
            for worker in 0..profile.clients {
                state
                    .worker_prepared(
                        WorkerId::new(worker).map_err(scheduler_error)?,
                        PreparedSessionId(u64::from(worker) + 1),
                    )
                    .map_err(scheduler_error)?;
            }
        }

        let cancelled = Arc::new(AtomicBool::new(false));
        let ready_barrier = Arc::new(Barrier::new(usize::from(profile.clients) + 1));
        let start_barrier = Arc::new(Barrier::new(usize::from(profile.clients) + 1));
        let mut workers = JoinSet::new();
        for (worker_index, session) in sessions.into_iter().enumerate() {
            let scheduler = Arc::clone(&scheduler);
            let routing = Arc::clone(&routing);
            let cancelled = Arc::clone(&cancelled);
            let ready_barrier = Arc::clone(&ready_barrier);
            let start_barrier = Arc::clone(&start_barrier);
            let monotonic_clock = monotonic_clock.clone();
            workers.spawn(async move {
                run_worker(
                    worker_index as u16,
                    session,
                    scheduler,
                    routing,
                    cancelled,
                    ready_barrier,
                    start_barrier,
                    monotonic_clock,
                )
                .await
            });
        }

        ready_barrier.wait().await;
        {
            let mut state = lock_scheduler(&scheduler)?;
            state.start().map_err(scheduler_error)?;
        }
        start_barrier.wait().await;
        info!(
            "all worker tasks ready; timing started: one {}s warmup followed continuously by 3x{}s windows",
            profile.warmup.as_secs(),
            profile.measurement_window.as_secs()
        );

        let mut first_error = None;
        let mut worker_ledgers: Vec<Option<RunLedger>> = std::iter::repeat_with(|| None)
            .take(usize::from(profile.clients))
            .collect();
        while let Some(joined) = workers.join_next().await {
            match joined {
                Ok(Ok((worker, ledger))) => {
                    let index = usize::from(worker);
                    if index >= worker_ledgers.len() {
                        cancelled.store(true, Ordering::Release);
                        if first_error.is_none() {
                            first_error = Some(TpccError::Protocol(format!(
                                "ranked worker returned out-of-range id {worker}"
                            )));
                        }
                    } else if worker_ledgers[index].replace(ledger).is_some() {
                        cancelled.store(true, Ordering::Release);
                        if first_error.is_none() {
                            first_error = Some(TpccError::Protocol(format!(
                                "ranked worker returned duplicate ledger for id {worker}"
                            )));
                        }
                    }
                }
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
                            "ranked worker task failed: {error}"
                        )));
                    }
                }
            }
        }
        if let Some(error) = first_error {
            return Err(error);
        }
        let worker_ledgers = worker_ledgers
            .into_iter()
            .enumerate()
            .map(|(worker, ledger)| {
                ledger.ok_or_else(|| {
                    TpccError::Protocol(format!(
                        "ranked worker {worker} did not return its physical commit ledger"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ledger = RunLedger::merge_all(worker_ledgers).map_err(ledger_error)?;

        let (windows, summary) = {
            let mut state = lock_scheduler(&scheduler)?;
            let summary = state.measurement_summary().map_err(scheduler_error)?;
            (state.windows().clone(), summary)
        };
        let window_rates = std::array::from_fn(|index| {
            windows[index]
                .new_order_per_minute_for(profile.measurement_window)
                .unwrap_or(0.0)
        });
        let median_new_order_per_minute = median_of_three(window_rates);
        let result = Final2026RunResult {
            ranked: self.effective.is_ranked_configuration(),
            windows,
            summary,
            window_rates,
            median_new_order_per_minute,
            response_timeout,
            phase_tail_grace: limits.phase_tail_grace,
            ledger,
        };

        if result.ranked && !result.summary.passed() {
            result.print_report();
            return Err(TpccError::QueryError(
                "formal final2026 measurement failed a mandatory semantic/coverage gate".to_owned(),
            ));
        }
        Ok(result)
    }

    async fn open_sessions(
        &self,
        clients: u16,
        response_timeout: Duration,
    ) -> Result<Vec<RmdbClient>, TpccError> {
        let mut tasks = JoinSet::new();
        for worker in 0..clients {
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
            .take(clients as usize)
            .collect();
        while let Some(joined) = tasks.join_next().await {
            let (worker, session) = joined.map_err(|error| {
                TpccError::Protocol(format!("session preparation task failed: {error}"))
            })?;
            match session {
                Ok(session) => sessions[usize::from(worker)] = Some(session),
                Err(error) => {
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    return Err(TpccError::Protocol(format!(
                        "worker {worker} failed before the all-session barrier: {error}"
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
                        "worker {worker} did not reach the prepared-session barrier"
                    ))
                })
            })
            .collect()
    }
}

struct RunRouting {
    router: OfficialRouter,
    warmup: WarehouseWheel,
    formal: [WarehouseWheel; FORMAL_WINDOW_COUNT],
}

impl RunRouting {
    fn new(router: OfficialRouter) -> Self {
        let warmup = router.wheel(StageId::WARMUP);
        let formal = std::array::from_fn(|index| router.wheel(StageId::measurement(index as u8)));
        Self {
            router,
            warmup,
            formal,
        }
    }

    fn wheel(&self, phase: PhaseId) -> &WarehouseWheel {
        match phase {
            PhaseId::Warmup => &self.warmup,
            PhaseId::FormalWindow(index) => &self.formal[usize::from(index)],
        }
    }
}

async fn run_worker(
    worker_value: u16,
    mut client: RmdbClient,
    scheduler: Arc<Mutex<Scheduler>>,
    routing: Arc<RunRouting>,
    cancelled: Arc<AtomicBool>,
    ready_barrier: Arc<Barrier>,
    start_barrier: Arc<Barrier>,
    monotonic_clock: SystemMonotonicClock,
) -> Result<(u16, RunLedger), TpccError> {
    let worker = WorkerId::new(worker_value).map_err(scheduler_error)?;
    wait_for_timing_release(&ready_barrier, &start_barrier).await;

    let mut ledger = RunLedger::default();
    let mut sequence_phase = None;
    let mut sequence = ClientSequence::new(worker_value)
        .map_err(|error| TpccError::Protocol(error.to_string()))?;

    loop {
        if cancelled.load(Ordering::Acquire) {
            return Ok((worker_value, ledger));
        }

        let reservation = {
            let mut state = lock_scheduler(&scheduler)?;
            match state.reserve_transaction(worker) {
                Ok(reservation) => reservation,
                Err(SchedulerError::TimelineEnded) => return Ok((worker_value, ledger)),
                Err(error) => return Err(scheduler_error(error)),
            }
        };

        if sequence_phase != Some(reservation.phase) {
            sequence = ClientSequence::new(worker_value)
                .map_err(|error| TpccError::Protocol(error.to_string()))?;
            sequence_phase = Some(reservation.phase);
        }
        if sequence.next_txn_no() != reservation.txn_no {
            return fail_worker(
                &scheduler,
                &cancelled,
                worker,
                format!(
                    "routing sequence {} does not match scheduler reservation {}",
                    sequence.next_txn_no(),
                    reservation.txn_no
                ),
            );
        }

        let workload = Final2026Workload::new(&routing.router, routing.wheel(reservation.phase));
        let selected = workload
            .select(&mut sequence)
            .map_err(|error| TpccError::Protocol(error.to_string()))?;
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        let frozen = FrozenTransaction::new(selected, timestamp)
            .map_err(|message| TpccError::Protocol(message.to_owned()))?;
        let identity = TransactionIdentity::new(
            frozen.transaction_type(),
            frozen.ticket().route().home_warehouse,
            frozen.fingerprint(),
            frozen.expects_business_rollback(),
        )
        .map_err(scheduler_error)?;

        let (mut phase_ticket, mut attempt_deadline) = {
            let mut state = lock_scheduler(&scheduler)?;
            match state.start_transaction(reservation, identity) {
                Ok(ticket) => {
                    let deadline = state.attempt_deadline(ticket).map_err(scheduler_error)?;
                    (ticket, deadline)
                }
                Err(SchedulerError::ReservationDeadlinePassed) => continue,
                Err(error) => return Err(scheduler_error(error)),
            }
        };

        loop {
            if cancelled.load(Ordering::Acquire) {
                return Ok((worker_value, ledger));
            }
            let deadline = tokio::time::Instant::from_std(
                monotonic_clock
                    .instant_at(attempt_deadline)
                    .map_err(scheduler_error)?,
            );
            let result =
                tokio::time::timeout_at(deadline, dispatch::execute(&mut client, &frozen)).await;
            let completed_at = monotonic_clock.now();
            match result {
                Err(_) => {
                    return fail_worker(
                        &scheduler,
                        &cancelled,
                        worker,
                        format!(
                            "physical attempt {} exceeded absolute local deadline {:?}; \
                             connection state is unknown and will not be reused",
                            phase_ticket.id(),
                            attempt_deadline
                        ),
                    );
                }
                Ok(Ok(outcome)) => {
                    let attempt = match &outcome {
                        RankedTransactionOutcome::Committed(commit) => AttemptOutcome::Commit {
                            delivery_processed: commit.delivery_processed(),
                        },
                        RankedTransactionOutcome::ExpectedRollback => {
                            AttemptOutcome::ExpectedRollback
                        }
                    };
                    let disposition = {
                        let mut state = lock_scheduler(&scheduler)?;
                        state
                            .finish_attempt_at(phase_ticket, attempt, completed_at)
                            .map_err(scheduler_error)?
                    };
                    let record_result = match disposition {
                        AttemptDisposition::Finished => ledger.record(frozen.ticket(), &outcome),
                        AttemptDisposition::GraceTail => {
                            ledger.record_grace_tail(frozen.ticket(), &outcome)
                        }
                        other => {
                            return fail_worker(
                                &scheduler,
                                &cancelled,
                                worker,
                                format!(
                                    "terminal transaction received invalid scheduler disposition \
                                     {other:?}"
                                ),
                            );
                        }
                    };
                    if let Err(error) = record_result {
                        return fail_worker(
                            &scheduler,
                            &cancelled,
                            worker,
                            format!("physical commit ledger rejected terminal: {error}"),
                        );
                    }
                    break;
                }
                Ok(Err(error)) if error.is_retryable_abort() => {
                    let disposition = {
                        let mut state = lock_scheduler(&scheduler)?;
                        state
                            .finish_attempt_at(
                                phase_ticket,
                                AttemptOutcome::RetryableAbort,
                                completed_at,
                            )
                            .map_err(scheduler_error)?
                    };
                    if disposition != AttemptDisposition::RetrySameParameters {
                        break;
                    }
                    (phase_ticket, attempt_deadline) = {
                        let mut state = lock_scheduler(&scheduler)?;
                        match state.start_retry(phase_ticket) {
                            Ok(ticket) => {
                                let deadline =
                                    state.attempt_deadline(ticket).map_err(scheduler_error)?;
                                (ticket, deadline)
                            }
                            Err(SchedulerError::RetryDeadlinePassed) => break,
                            Err(error) => return Err(scheduler_error(error)),
                        }
                    };
                }
                Ok(Err(error)) => {
                    return fail_worker(
                        &scheduler,
                        &cancelled,
                        worker,
                        format!("ranked transaction failed: {error}"),
                    );
                }
            }
        }
    }
}

async fn wait_for_timing_release(ready_barrier: &Barrier, start_barrier: &Barrier) {
    ready_barrier.wait().await;
    start_barrier.wait().await;
}

fn fail_worker<T>(
    scheduler: &Arc<Mutex<Scheduler>>,
    cancelled: &Arc<AtomicBool>,
    worker: WorkerId,
    reason: String,
) -> Result<T, TpccError> {
    cancelled.store(true, Ordering::Release);
    if let Ok(mut state) = scheduler.lock() {
        let _ = state.worker_failed(worker, reason.clone());
    }
    Err(TpccError::Protocol(reason))
}

fn lock_scheduler(
    scheduler: &Arc<Mutex<Scheduler>>,
) -> Result<MutexGuard<'_, Scheduler>, TpccError> {
    scheduler
        .lock()
        .map_err(|_| TpccError::Protocol("ranked scheduler mutex was poisoned".to_owned()))
}

fn scheduler_error(error: SchedulerError) -> TpccError {
    TpccError::Protocol(format!("ranked scheduler: {error}"))
}

fn ledger_error(error: LedgerError) -> TpccError {
    TpccError::Protocol(format!("ranked physical commit ledger: {error}"))
}

fn median_of_three(mut values: [f64; FORMAL_WINDOW_COUNT]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[1]
}

pub struct Final2026RunResult {
    ranked: bool,
    windows: [WindowStats; FORMAL_WINDOW_COUNT],
    summary: MeasurementSummary,
    window_rates: [f64; FORMAL_WINDOW_COUNT],
    median_new_order_per_minute: f64,
    response_timeout: Duration,
    phase_tail_grace: Duration,
    ledger: RunLedger,
}

impl Final2026RunResult {
    pub fn ledger(&self) -> &RunLedger {
        &self.ledger
    }

    pub fn print_report(&self) {
        println!("=== TPCC final2026 public-spec measurement ===");
        println!(
            "conformance={}",
            if self.ranked {
                "public_spec_aligned"
            } else {
                "non_ranked_deviation"
            }
        );
        println!(
            "local_safety=response_timeout:{}s,phase_tail_grace:{}s (official values unpublished)",
            self.response_timeout.as_secs(),
            self.phase_tail_grace.as_secs()
        );
        for index in 0..FORMAL_WINDOW_COUNT {
            let window = &self.windows[index];
            let gate = &self.summary.window_gates[index];
            println!(
                "window{}: new_order_per_min={:.3}, committed={}, expected_rollback={}, retry_abort={}, abandoned={}, grace_tail={}, delivery_processed={}, warehouses={}/{}, gate={}",
                index + 1,
                self.window_rates[index],
                window.committed,
                window.expected_rollbacks,
                window.retry_aborts,
                window.abandoned,
                window.grace_tail,
                window.delivery_processed,
                gate.coverage.covered_warehouses,
                gate.coverage.required_warehouses,
                if gate.passed() { "pass" } else { "fail" }
            );
        }
        println!(
            "ranked_new_order_per_min_median={:.3}",
            self.median_new_order_per_minute
        );
        println!(
            "combined_coverage={}/{}, combined_gate={}",
            self.summary.combined_coverage.covered_warehouses,
            self.summary.combined_coverage.required_warehouses,
            if self.summary.combined_coverage.passed() {
                "pass"
            } else {
                "fail"
            }
        );
        if !self.ranked {
            warn!("this smoke result is deliberately non-ranked");
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;

    #[tokio::test]
    async fn worker_tasks_wait_for_explicit_timing_release_after_ready() {
        const WORKERS: usize = 2;
        let ready_barrier = Arc::new(Barrier::new(WORKERS + 1));
        let start_barrier = Arc::new(Barrier::new(WORKERS + 1));
        let (started_tx, mut started_rx) = mpsc::unbounded_channel();
        let mut tasks = JoinSet::new();

        for worker in 0..WORKERS {
            let ready_barrier = Arc::clone(&ready_barrier);
            let start_barrier = Arc::clone(&start_barrier);
            let started_tx = started_tx.clone();
            tasks.spawn(async move {
                wait_for_timing_release(&ready_barrier, &start_barrier).await;
                started_tx.send(worker).unwrap();
            });
        }
        drop(started_tx);

        ready_barrier.wait().await;
        assert!(matches!(
            started_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        start_barrier.wait().await;
        let mut started = vec![
            started_rx.recv().await.unwrap(),
            started_rx.recv().await.unwrap(),
        ];
        started.sort_unstable();
        assert_eq!(started, vec![0, 1]);
        while tasks.join_next().await.is_some() {}
    }
}
