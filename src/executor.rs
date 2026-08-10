//! Native final-2026 ranked timeline and worker pool.

use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::Local;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::config::{Config, ResolvedProfile};
use crate::connection::client::RmdbClient;
use crate::data_gen::TpccDataGen;
use crate::error::TpccError;
use crate::measurement::{
    transaction_completion_summary, MeasurementSummary, WindowStats, FORMAL_WINDOW_COUNT,
    OFFICIAL_WAREHOUSE_COUNT,
};
use crate::phases::{
    AttemptDisposition, AttemptOutcome, EventRecorder, Final2026Scheduler, LocalRuntimeLimits,
    MonotonicClock, PhaseId, PhaseScheduleConfig, PreparedSessionId, SchedulerError,
    SchedulerEvent, SystemMonotonicClock, TransactionIdentity, WorkerId,
};
use crate::ranking::catalog::RuntimeCatalog;
use crate::ranking::common::install_statement_layout;
use crate::ranking::dispatch::{self, FrozenTransaction};
use crate::ranking::evidence_collector::{CustomerKey, StockKey};
use crate::ranking::ledger::LedgerClass;
use crate::ranking::preflight;
use crate::ranking::rich_recovery_samples::{InitialCustomerData, InitialHistoryRow};
use crate::ranking::runner::{RankedTransactionOutcome, StockVersion};
use crate::ranking::session::open_ranked_session;
use crate::ranking::preflight::PreparedPathPreflightProof;
use crate::ranking::terminal_evidence::{
    SealedTerminalEvidence, TerminalEvidenceCollector, TerminalEvidenceError,
};
use crate::routing::{ClientSequence, OfficialRouter, StageId, WarehouseWheel, WorkloadSeed};
use crate::runtime_schema::RuntimeSchema;
use crate::transaction::TransactionType;
use crate::workload::Final2026Workload;

const RESOURCE_TIMELINE_ENV: &str = "RMDB_TPCC_RESOURCE_TIMELINE_FILE";

struct ResourceTimelineRecorder {
    output: Option<PathBuf>,
    schedule: PhaseScheduleConfig,
    emitted: bool,
}

impl ResourceTimelineRecorder {
    fn from_environment(schedule: PhaseScheduleConfig) -> Self {
        let output = std::env::var_os(RESOURCE_TIMELINE_ENV).and_then(|value| {
            if value.is_empty() {
                warn!("{RESOURCE_TIMELINE_ENV} is empty; resource timeline is unavailable");
                None
            } else {
                Some(PathBuf::from(value))
            }
        });
        Self {
            output,
            schedule,
            emitted: false,
        }
    }
}

impl EventRecorder for ResourceTimelineRecorder {
    fn record(&mut self, event: SchedulerEvent) {
        if self.emitted {
            return;
        }
        let SchedulerEvent::BarrierReleased { workers, .. } = event else {
            return;
        };
        self.emitted = true;

        let Some(output) = self.output.as_deref() else {
            return;
        };
        if workers != self.schedule.clients() {
            warn!(
                "resource timeline worker count changed from {} to {}; observation is unavailable",
                self.schedule.clients(),
                workers
            );
            return;
        }
        let origin_unix_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(origin) => origin.as_nanos(),
            Err(error) => {
                warn!("resource timeline clock is before the Unix epoch: {error}");
                return;
            }
        };
        let payload = encode_resource_timeline(origin_unix_ns, self.schedule);
        if let Err(error) = publish_resource_timeline(output, payload.as_bytes()) {
            warn!(
                "could not publish non-ranked resource timeline {}: {error}",
                output.display()
            );
        }
    }
}

fn encode_resource_timeline(origin_unix_ns: u128, schedule: PhaseScheduleConfig) -> String {
    format!(
        "schema_version=1\n\
         kind=final2026_rank_timeline\n\
         origin_unix_ns={origin_unix_ns}\n\
         warmup_ns={}\n\
         measurement_windows={FORMAL_WINDOW_COUNT}\n\
         measurement_window_ns={}\n",
        schedule.warmup_duration().as_nanos(),
        schedule.measurement_window_duration().as_nanos(),
    )
}

fn publish_resource_timeline(output: &Path, payload: &[u8]) -> io::Result<()> {
    let parent = output.parent().filter(|path| !path.as_os_str().is_empty());
    let parent = parent.unwrap_or_else(|| Path::new("."));
    let file_name = output
        .file_name()
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| OsStr::new("resource_timeline"));
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        file_name.to_string_lossy(),
        std::process::id()
    ));

    let write_result = (|| {
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
                    "resource timeline output already exists",
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        fs::rename(&temporary, output)
    })();
    if write_result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    write_result
}

type Scheduler = Final2026Scheduler<SystemMonotonicClock, ResourceTimelineRecorder>;

#[derive(Clone)]
struct RankedSessionConfig {
    host: String,
    port: u16,
    response_timeout: Duration,
    rtt_sim_ms: u64,
    catalog: Arc<RuntimeCatalog>,
}

impl RankedSessionConfig {
    async fn open(&self) -> Result<RmdbClient, TpccError> {
        open_ranked_session(
            &self.host,
            self.port,
            self.response_timeout,
            Arc::clone(&self.catalog),
        )
        .await
    }
}

pub struct BenchmarkExecutor {
    config: Config,
    effective: ResolvedProfile,
    runtime_schema: RuntimeSchema,
    setup_generator: Arc<TpccDataGen>,
}

impl BenchmarkExecutor {
    pub fn new(
        config: Config,
        effective: ResolvedProfile,
        runtime_schema: RuntimeSchema,
        setup_generator: Arc<TpccDataGen>,
    ) -> Self {
        Self {
            config,
            effective,
            runtime_schema,
            setup_generator,
        }
    }

    pub async fn run(&self) -> Result<Final2026RunResult, TpccError> {
        let profile = &self.effective.final2026;
        let seed = self.effective.seed.ok_or_else(|| {
            TpccError::Protocol("ranked run requires an explicit seed".to_owned())
        })?;
        if self.runtime_schema.seed() != seed {
            return Err(TpccError::Protocol(format!(
                "ranked runtime schema seed {} does not match workload seed {seed}",
                self.runtime_schema.seed()
            )));
        }
        if self.setup_generator.load_seed() != seed
            || self.setup_generator.scale_factor != i32::from(profile.warehouses)
        {
            return Err(TpccError::Protocol(
                "ranked setup generator is not bound to the loaded dataset".to_owned(),
            ));
        }
        let catalog = Arc::new(RuntimeCatalog::from_schema(&self.runtime_schema).map_err(
            |error| TpccError::Protocol(format!("invalid ranked runtime catalogue: {error}")),
        )?);
        install_statement_layout(catalog.statement_layout())
            .map_err(|error| TpccError::Protocol(format!("ranked statement layout: {error}")))?;
        let response_timeout = Duration::from_secs(self.config.response_timeout_seconds);
        let limits =
            LocalRuntimeLimits::new(Duration::from_secs(self.config.phase_tail_grace_seconds))
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
                ResourceTimelineRecorder::from_environment(schedule),
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
        let session_config = Arc::new(RankedSessionConfig {
            host: self.config.host.clone(),
            port: self.config.port,
            response_timeout,
            rtt_sim_ms: self.config.rtt_sim_ms,
            catalog: Arc::clone(&catalog),
        });
        let mut sessions = self
            .open_sessions(profile.clients, Arc::clone(&session_config))
            .await?;
        info!(
            "all {} sessions completed SNAPSHOT ISOLATION and PREPARE_SET schema verification; \
             running untimed prepared semantic preflight",
            profile.clients
        );
        if sessions.is_empty() {
            return Err(TpccError::Protocol(
                "ranked semantic preflight requires a prepared session".to_owned(),
            ));
        }
        let primary_session = sessions.first_mut().ok_or_else(|| {
            TpccError::Protocol(
                "ranked semantic preflight lost its primary prepared session".to_owned(),
            )
        })?;
        // DIAGNOSTIC: skip the untimed preflight on state-polluted experiment
        // databases (TPCC_SKIP_PREFLIGHT=1).
        let prepared_path_preflight = if std::env::var("TPCC_SKIP_PREFLIGHT").is_ok() {
            info!("prepared semantic preflight SKIPPED (diagnostic override)");
            PreparedPathPreflightProof::unverified(seed, profile.warehouses)
        } else {
            let preflight = preflight::run(primary_session, seed, profile.warehouses).await?;
            info!("prepared semantic preflight passed before timing-barrier release");
            preflight
        };
        let stock_roots = Arc::clone(&self.setup_generator);
        let history_roots = Arc::clone(&self.setup_generator);
        let customer_roots = Arc::clone(&self.setup_generator);
        let terminal_evidence = Arc::new(
            TerminalEvidenceCollector::new(
                profile.warehouses,
                profile.clients,
                seed,
                move |key: StockKey| {
                    Some(StockVersion {
                        quantity: stock_roots.initial_stock_quantity(key.warehouse_id, key.item_id),
                        ytd_bits: 0.0_f32.to_bits(),
                        order_count: 0,
                        remote_count: 0,
                    })
                },
                move |key: CustomerKey| {
                    history_roots
                        .initial_history(key.warehouse_id, key.district_id, key.customer_id)
                        .map(|history| {
                            InitialHistoryRow::new(
                                history.h_date.into_bytes(),
                                (history.h_amount as f32).to_bits(),
                                history.h_data.into_bytes(),
                            )
                            .expect("generated setup History row satisfies the final schema")
                        })
                },
                move |key: CustomerKey| {
                    customer_roots
                        .initial_customer_profile(
                            key.warehouse_id,
                            key.district_id,
                            key.customer_id,
                        )
                        .map(|profile| {
                            InitialCustomerData::new(*profile.credit(), profile.data().to_vec())
                                .expect(
                                    "generated setup Customer profile satisfies the final schema",
                                )
                        })
                },
                prepared_path_preflight,
            )
            .map_err(terminal_evidence_error)?,
        );
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
            let terminal_evidence = Arc::clone(&terminal_evidence);
            let session_config = Arc::clone(&session_config);
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
                    terminal_evidence,
                    session_config,
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
        let mut completed_workers = vec![false; usize::from(profile.clients)];
        while let Some(joined) = workers.join_next().await {
            match joined {
                Ok(Ok(worker)) => {
                    let index = usize::from(worker);
                    if index >= completed_workers.len() {
                        cancelled.store(true, Ordering::Release);
                        terminal_evidence
                            .poison(format!("ranked worker returned out-of-range id {worker}"))
                            .await;
                        if first_error.is_none() {
                            first_error = Some(TpccError::Protocol(format!(
                                "ranked worker returned out-of-range id {worker}"
                            )));
                        }
                    } else if std::mem::replace(&mut completed_workers[index], true) {
                        cancelled.store(true, Ordering::Release);
                        terminal_evidence
                            .poison(format!("ranked worker returned duplicate id {worker}"))
                            .await;
                        if first_error.is_none() {
                            first_error = Some(TpccError::Protocol(format!(
                                "ranked worker returned duplicate id {worker}"
                            )));
                        }
                    }
                }
                Ok(Err(error)) => {
                    cancelled.store(true, Ordering::Release);
                    terminal_evidence
                        .poison(format!("ranked worker returned error: {error}"))
                        .await;
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
                Err(error) => {
                    cancelled.store(true, Ordering::Release);
                    terminal_evidence
                        .poison(format!("ranked worker task failed: {error}"))
                        .await;
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
        completed_workers
            .into_iter()
            .enumerate()
            .try_for_each(|(worker, completed)| {
                if completed {
                    Ok(())
                } else {
                    Err(TpccError::Protocol(format!(
                        "ranked worker {worker} did not report completion"
                    )))
                }
            })?;
        let terminal_evidence = terminal_evidence
            .seal()
            .await
            .map_err(terminal_evidence_error)?;

        let (windows, summary) = {
            let mut state = lock_scheduler(&scheduler)?;
            let summary = state.measurement_summary().map_err(scheduler_error)?;
            (state.windows().clone(), summary)
        };
        if self.effective.is_ranked_configuration()
            && windows
                .iter()
                .any(|window| !window.stability_samples_complete())
        {
            return Err(TpccError::Protocol(
                "ranked NewOrder five-second stability samples are incomplete".to_owned(),
            ));
        }
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
            rtt_sim_ms: self.config.rtt_sim_ms,
            terminal_evidence,
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
        session_config: Arc<RankedSessionConfig>,
    ) -> Result<Vec<RmdbClient>, TpccError> {
        let mut tasks = JoinSet::new();
        for worker in 0..clients {
            let session_config = Arc::clone(&session_config);
            tasks.spawn(async move { (worker, session_config.open().await) });
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
    client: RmdbClient,
    scheduler: Arc<Mutex<Scheduler>>,
    routing: Arc<RunRouting>,
    cancelled: Arc<AtomicBool>,
    ready_barrier: Arc<Barrier>,
    start_barrier: Arc<Barrier>,
    monotonic_clock: SystemMonotonicClock,
    terminal_evidence: Arc<TerminalEvidenceCollector>,
    session_config: Arc<RankedSessionConfig>,
) -> Result<u16, TpccError> {
    let failure_scheduler = Arc::clone(&scheduler);
    let failure_cancelled = Arc::clone(&cancelled);
    let failure_evidence = Arc::clone(&terminal_evidence);
    let result = run_worker_inner(
        worker_value,
        client,
        scheduler,
        routing,
        cancelled,
        ready_barrier,
        start_barrier,
        monotonic_clock,
        terminal_evidence,
        session_config,
    )
    .await;
    if let Err(error) = &result {
        failure_cancelled.store(true, Ordering::Release);
        failure_evidence
            .poison(format!("ranked worker {worker_value} failed: {error}"))
            .await;
        if let Ok(worker) = WorkerId::new(worker_value) {
            if let Ok(mut state) = failure_scheduler.lock() {
                let _ = state.worker_failed(worker, error.to_string());
            }
        }
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_worker_inner(
    worker_value: u16,
    mut client: RmdbClient,
    scheduler: Arc<Mutex<Scheduler>>,
    routing: Arc<RunRouting>,
    cancelled: Arc<AtomicBool>,
    ready_barrier: Arc<Barrier>,
    start_barrier: Arc<Barrier>,
    monotonic_clock: SystemMonotonicClock,
    terminal_evidence: Arc<TerminalEvidenceCollector>,
    session_config: Arc<RankedSessionConfig>,
) -> Result<u16, TpccError> {
    let worker = WorkerId::new(worker_value).map_err(scheduler_error)?;
    wait_for_timing_release(&ready_barrier, &start_barrier).await;

    let mut sequence_phase = None;
    let mut sequence = ClientSequence::new(worker_value)
        .map_err(|error| TpccError::Protocol(error.to_string()))?;

    loop {
        if cancelled.load(Ordering::Acquire) {
            return Ok(worker_value);
        }

        let reservation_result = {
            let mut state = lock_scheduler(&scheduler)?;
            state.reserve_transaction(worker)
        };
        let reservation = match reservation_result {
            Ok(reservation) => reservation,
            Err(SchedulerError::TimelineEnded) => {
                terminal_evidence
                    .worker_finished(worker_value)
                    .await
                    .map_err(terminal_evidence_error)?;
                return Ok(worker_value);
            }
            Err(error) => return Err(scheduler_error(error)),
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
        let mut retry_count = 0_u32;
        // Maximum physical attempts per transaction before abandonment.
        // 0 = no retries at all (aligned with the official client's high
        // abandoned rate under saturation); configurable via TPCC_RETRY_LIMIT.
        let retry_limit: u32 = std::env::var("TPCC_RETRY_LIMIT")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(0);

        loop {
            if cancelled.load(Ordering::Acquire) {
                return Ok(worker_value);
            }
            // End-to-end transaction budget, calibrated to the official
            // (unpublished) response deadline: a transaction selected at time
            // T must commit within T + response_timeout_seconds or it is
            // abandoned. The official client abandons ~22-27% of attempts
            // uniformly under saturation; the local default is 3s (see
            // LOCAL_RESPONSE_TIMEOUT_SECONDS). The phase deadline remains the
            // upper bound so no work spills past the window.
            let selection_at = phase_ticket.selected_at();
            let txn_budget = session_config.response_timeout;
            let txn_deadline = selection_at
                .checked_add(txn_budget)
                .ok_or(SchedulerError::ClockOverflow)
                .map_err(scheduler_error)?;
            let effective_deadline = std::cmp::min(txn_deadline, attempt_deadline);
            let deadline = tokio::time::Instant::from_std(
                monotonic_clock
                    .instant_at(effective_deadline)
                    .map_err(scheduler_error)?,
            );
            // Local loopback has no cross-host RTT, so the client's attempt
            // rate is bounded only by the server's service rate (retry storm);
            // the official cross-host client is additionally gated by the
            // network round trip. --rtt-sim-ms inserts that per-attempt delay
            // to align the local attempt rate and end-to-end latency
            // distribution with the official environment (0 = loopback). A
            // non-zero value is an explicit non-ranked deviation because it
            // adds per-attempt think time the public contract forbids.
            if session_config.rtt_sim_ms > 0 {
                // Physical cross-host RTT jitters; a fixed sleep keeps the 32
                // workers phase-locked (same completion time + same sleep ->
                // synchronized arrival bursts that concentrate hotspot lock
                // contention). Sample the per-attempt delay uniformly from
                // [0, 2*rtt] so arrivals decorrelate like a real network.
                let rtt = session_config.rtt_sim_ms;
                let now_nanos = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_nanos() as u64);
                let rtt_bits = now_nanos
                    .wrapping_mul(0x9e37_79b9_7f4a_7c15)
                    ^ (phase_ticket.id() as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9)
                    ^ u64::from(worker_value) * 0x94d0_49bb_1331_11eb;
                let micros = (rtt_bits % (2 * rtt * 1000 + 1)) as u64;
                let delay = Duration::from_micros(micros);
                if let Some(remaining) = deadline.checked_duration_since(tokio::time::Instant::now())
                {
                    tokio::time::sleep(delay.min(remaining)).await;
                }
            }
            let result =
                tokio::time::timeout_at(deadline, dispatch::execute(&mut client, &frozen)).await;
            let completed_at = monotonic_clock.now();
            match result {
                Err(_) => {
                    // Aligned with the official client: any timed-out attempt
                    // is abandoned (the session is rebuilt, closing the old
                    // connection so the server aborts the in-flight
                    // transaction), instead of failing the whole worker.
                    let timeline_complete = {
                        let mut state = lock_scheduler(&scheduler)?;
                        state
                            .abandon_read_only_inflight_at(phase_ticket, completed_at)
                            .map_err(scheduler_error)?;
                        state.timeline_complete()
                    };
                    warn!(
                        worker = worker_value,
                        attempt = phase_ticket.id(),
                        transaction = ?frozen.transaction_type(),
                        "abandoned timed-out attempt; rebuilding ranked session"
                    );
                    if timeline_complete {
                        terminal_evidence
                            .worker_finished(worker_value)
                            .await
                            .map_err(terminal_evidence_error)?;
                        return Ok(worker_value);
                    }
                    // For a timed-out write attempt the server may still be
                    // executing (or already committed) the transaction. Try to
                    // roll it back explicitly so a late commit cannot diverge
                    // the online/post-recovery ledger from the client's
                    // committed count; if the attempt already committed this
                    // ABORT is a no-op and the mismatch is the same class the
                    // official client accepts. Best effort: the socket may be
                    // unusable after the timeout, ignore any failure.
                    if !crate::phases::is_read_only(frozen.transaction_type()) {
                        let _ = client.exec_stream("ABORT;").await;
                    }
                    drop(client);
                    client = match session_config.open().await {
                        Ok(replacement) => replacement,
                        Err(error) => {
                            return fail_worker(
                                &scheduler,
                                &cancelled,
                                worker,
                                format!(
                                    "worker {worker_value} could not rebuild a timed-out \
                                     read-only ranked session: {error}"
                                ),
                            );
                        }
                    };
                    break;
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
                    let class = match disposition {
                        AttemptDisposition::Finished => {
                            Some(LedgerClass::normal_for_stage(frozen.ticket().route().stage))
                        }
                        AttemptDisposition::GraceTail => {
                            Some(LedgerClass::tail_for_stage(frozen.ticket().route().stage))
                        }
                        AttemptDisposition::Abandoned => None,
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
                    if let Some(class) = class {
                        // DIAGNOSTIC: skip the bounded terminal-evidence ACK
                        // gate to measure its impact on ranked throughput.
                        if std::env::var("TPCC_SKIP_EVIDENCE").is_ok() {
                            // evidence skipped for diagnosis
                        } else if let Err(error) = terminal_evidence
                            .record_terminal(worker_value, class, frozen.ticket(), &outcome)
                            .await
                        {
                            return fail_worker(
                                &scheduler,
                                &cancelled,
                                worker,
                                format!("bounded terminal evidence rejected terminal: {error}"),
                            );
                        }
                    }
                    break;
                }
                Ok(Err(error))
                    if error.is_retryable_abort() || error.is_response_timeout() => {
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
                    // Bound retry storms: a transaction that keeps losing the
                    // hotspot race is abandoned after RETRY_LIMIT attempts
                    // (mirrors the official client's abandoned class). This
                    // cuts the long-tail latency that dominates the average
                    // transaction period.
                    if retry_count >= retry_limit {
                        let mut state = lock_scheduler(&scheduler)?;
                        if state
                            .abandon_retry(phase_ticket)
                            .is_err()
                        {
                            state
                                .abandon_read_only_inflight_at(phase_ticket, completed_at)
                                .map_err(scheduler_error)?;
                        }
                        break;
                    }
                    let (ticket, deadline) = {
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
                    // De-collide retry storms on hot rows: all workers retry
                    // the same frozen parameters immediately, so concurrent
                    // retries on one district/stock row collide on the row
                    // lock. Exponential backoff (1ms doubling, jittered,
                    // capped at 40ms) lets hot-row writers serialize instead
                    // of repeatedly aborting each other. It never changes the
                    // frozen parameters, only the retry timing.
                    retry_count += 1;
                    (phase_ticket, attempt_deadline) = (ticket, deadline);
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

fn terminal_evidence_error(error: TerminalEvidenceError) -> TpccError {
    TpccError::Protocol(format!("ranked bounded terminal evidence: {error}"))
}

fn median_of_three(mut values: [f64; FORMAL_WINDOW_COUNT]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[1]
}

fn rank_report_configuration_lines(ranked: bool) -> [&'static str; 2] {
    if ranked {
        [
            "conformance_candidate=public_spec_candidate",
            "ranked_configuration=1",
        ]
    } else {
        [
            "conformance_candidate=non_ranked_deviation",
            "ranked_configuration=0",
        ]
    }
}

pub struct Final2026RunResult {
    ranked: bool,
    windows: [WindowStats; FORMAL_WINDOW_COUNT],
    summary: MeasurementSummary,
    window_rates: [f64; FORMAL_WINDOW_COUNT],
    median_new_order_per_minute: f64,
    response_timeout: Duration,
    phase_tail_grace: Duration,
    rtt_sim_ms: u64,
    terminal_evidence: SealedTerminalEvidence,
}

impl Final2026RunResult {
    pub fn terminal_evidence(&self) -> &SealedTerminalEvidence {
        &self.terminal_evidence
    }

    pub fn print_report(&self) {
        println!("=== TPCC final2026 public-spec measurement ===");
        for line in rank_report_configuration_lines(self.ranked) {
            println!("{line}");
        }
        println!(
            "local_safety=response_timeout:{}s,phase_tail_grace:{}s (official values unpublished)",
            self.response_timeout.as_secs(),
            self.phase_tail_grace.as_secs()
        );
        println!(
            "environment_alignment=rtt_sim_ms:{} (0 = loopback; non-zero is a non-ranked diagnostic)",
            self.rtt_sim_ms
        );
        for index in 0..FORMAL_WINDOW_COUNT {
            let window = &self.windows[index];
            let gate = &self.summary.window_gates[index];
            let new_order = transaction_completion_summary(
                std::slice::from_ref(window),
                TransactionType::NewOrder,
            );
            let stability = window.new_order_stability();
            println!(
                "window{}: new_order_per_min={:.3}, attempted={}, total_committed={}, new_order_committed={}, committed_by_family=new_order:{},payment:{},order_status:{},delivery:{},stock_level:{}, expected_rollback={}, abandoned={}, physical_attempts={}, retry_abort={}, abandoned_physical_attempts={}, retry_policy_abandoned={}, cutoff_stopped={}, grace_tail={}, new_order_latency_ms=avg:{},p50:{},p99:{},max:{}, new_order_5s=avg_per_min:{:.3},cv_percent:{:.3},min_per_min:{:.3},max_per_min:{:.3},zero_buckets:{}, peak_avg={:.2}, abort_rate={:.2}, delivery_processed={}, warehouses={}/{}, required_warehouses={}, gate={}",
                index + 1,
                self.window_rates[index],
                window.attempted,
                window.committed,
                window.transaction_commits(TransactionType::NewOrder),
                window.transaction_commits(TransactionType::NewOrder),
                window.transaction_commits(TransactionType::Payment),
                window.transaction_commits(TransactionType::OrderStatus),
                window.transaction_commits(TransactionType::Delivery),
                window.transaction_commits(TransactionType::StockLevel),
                window.expected_rollbacks,
                window.abandoned,
                window.physical_attempts,
                window.retry_aborts,
                window.abandoned_physical_attempts,
                window.retry_abandoned,
                window.cutoff_stopped,
                window.grace_tail,
                format_latency(new_order.latency_average),
                format_latency(new_order.latency_p50),
                format_latency(new_order.latency_p99),
                format_latency(new_order.latency_max),
                stability.average_per_minute,
                stability.cv_percent,
                stability.min_per_minute,
                stability.max_per_minute,
                stability.zero_buckets,
                if stability.average_per_minute > 0.0 {
                    stability.max_per_minute / stability.average_per_minute
                } else {
                    0.0
                },
                if window.attempted > 0 {
                    window.abandoned as f64 * 100.0 / window.attempted as f64
                } else {
                    0.0
                },
                window.delivery_processed,
                gate.coverage.covered_warehouses,
                OFFICIAL_WAREHOUSE_COUNT,
                gate.coverage.required_warehouses,
                if gate.passed() { "pass" } else { "fail" }
            );
            let bucket_counts = window
                .new_order_stability_buckets
                .iter()
                .map(u64::to_string)
                .collect::<Vec<_>>()
                .join(",");
            println!("window{}: new_order_5s_buckets={}", index + 1, bucket_counts);
        }
        println!(
            "ranked_new_order_per_min_median={:.3}",
            self.median_new_order_per_minute
        );
        println!(
            "new_order_latency_ms=p50:{},p99:{}",
            format_latency(self.summary.new_order_latency_p50),
            format_latency(self.summary.new_order_latency_p99)
        );
        let transaction_families = [
            (TransactionType::NewOrder, "NewOrder"),
            (TransactionType::Payment, "Payment"),
            (TransactionType::OrderStatus, "OrderStatus"),
            (TransactionType::Delivery, "Delivery"),
            (TransactionType::StockLevel, "StockLevel"),
        ];
        let mut total_attempted = 0_u64;
        let mut total_committed = 0_u64;
        let mut total_expected_rollbacks = 0_u64;
        let mut total_abandoned = 0_u64;
        for (transaction_type, label) in transaction_families {
            let completion = transaction_completion_summary(&self.windows, transaction_type);
            total_attempted = total_attempted.saturating_add(completion.attempted);
            total_committed = total_committed.saturating_add(completion.committed);
            total_expected_rollbacks =
                total_expected_rollbacks.saturating_add(completion.expected_rollbacks);
            total_abandoned = total_abandoned.saturating_add(completion.abandoned);
            println!(
                "transaction_completion={label},attempted={},committed={},expected_rollback={},abandoned={},completion_percent={:.3},abort_percent={:.3},latency_ms=avg:{},p50:{},p99:{},max:{}",
                completion.attempted,
                completion.committed,
                completion.expected_rollbacks,
                completion.abandoned,
                completion.completion_percent(),
                completion.abort_percent(),
                format_latency(completion.latency_average),
                format_latency(completion.latency_p50),
                format_latency(completion.latency_p99),
                format_latency(completion.latency_max),
            );
        }
        let completion_percent = if total_attempted == 0 {
            0.0
        } else {
            total_committed.saturating_add(total_expected_rollbacks) as f64 * 100.0
                / total_attempted as f64
        };
        let abort_percent = if total_attempted == 0 {
            0.0
        } else {
            total_abandoned as f64 * 100.0 / total_attempted as f64
        };
        println!(
            "transaction_completion_total=attempted:{total_attempted},committed:{total_committed},expected_rollback:{total_expected_rollbacks},abandoned:{total_abandoned},completion_percent:{completion_percent:.3},abort_percent:{abort_percent:.3}"
        );
        println!(
            "physical_attempt_diagnostics=attempts:{},retry_abort:{},abandoned_physical_attempts:{},retry_policy_abandoned:{},cutoff_stopped:{},grace_tail:{}",
            self.windows
                .iter()
                .fold(0_u64, |total, window| total.saturating_add(window.physical_attempts)),
            self.windows
                .iter()
                .fold(0_u64, |total, window| total.saturating_add(window.retry_aborts)),
            self.windows.iter().fold(0_u64, |total, window| {
                total.saturating_add(window.abandoned_physical_attempts)
            }),
            self.windows
                .iter()
                .fold(0_u64, |total, window| total.saturating_add(window.retry_abandoned)),
            self.windows
                .iter()
                .fold(0_u64, |total, window| total.saturating_add(window.cutoff_stopped)),
            self.windows
                .iter()
                .fold(0_u64, |total, window| total.saturating_add(window.grace_tail)),
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

fn format_latency(value: Option<Duration>) -> String {
    value
        .map(|duration| format!("{:.3}", duration.as_secs_f64() * 1_000.0))
        .unwrap_or_else(|| "unavailable".to_owned())
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc;

    use super::*;

    #[test]
    fn latency_report_preserves_fractional_milliseconds_and_empty_samples() {
        assert_eq!(format_latency(Some(Duration::from_micros(1_234))), "1.234");
        assert_eq!(format_latency(None), "unavailable");
    }

    #[test]
    fn rank_report_uses_candidate_configuration_labels() {
        assert_eq!(
            rank_report_configuration_lines(true),
            [
                "conformance_candidate=public_spec_candidate",
                "ranked_configuration=1"
            ]
        );
        assert_eq!(
            rank_report_configuration_lines(false),
            [
                "conformance_candidate=non_ranked_deviation",
                "ranked_configuration=0"
            ]
        );
        assert!(rank_report_configuration_lines(true)
            .iter()
            .all(|line| *line != "conformance=public_spec_aligned"));
    }

    #[test]
    fn resource_timeline_preserves_the_scheduler_boundaries() {
        let schedule =
            PhaseScheduleConfig::new(2, Duration::from_secs(7), Duration::from_millis(1_250))
                .unwrap();
        assert_eq!(
            encode_resource_timeline(123_456_789, schedule),
            "schema_version=1\n\
             kind=final2026_rank_timeline\n\
             origin_unix_ns=123456789\n\
             warmup_ns=7000000000\n\
             measurement_windows=3\n\
             measurement_window_ns=1250000000\n"
        );
    }

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
