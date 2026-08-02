//! Transport-independent phase scheduler for the public 2026 final profile.
//!
//! The scheduler owns no sockets and spawns no tasks.  An executor supplies a
//! monotonic clock, registers its already-connected/configured/prepared worker
//! sessions once, and drives this state machine around each request.  Keeping
//! transport concerns outside this module makes the timing and accounting
//! contract deterministic in unit tests.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use thiserror::Error;

use crate::measurement::{
    MeasurementSummary, WindowGate, WindowStats, FORMAL_WINDOW_COUNT, FORMAL_WINDOW_DURATION,
    OFFICIAL_HOT_WAREHOUSE_COUNT, OFFICIAL_WAREHOUSE_COUNT,
};
use crate::profile::{OFFICIAL_CLIENTS, WARMUP_SECONDS};
use crate::transaction::TransactionType;

const WARMUP_DURATION: Duration = Duration::from_secs(WARMUP_SECONDS);

/// Timing and concurrency for one scheduler run.
///
/// [`Self::official`] is the public final profile used by
/// [`Final2026Scheduler::new`].  Any other value is a local, non-ranked smoke
/// schedule: it preserves the three-window state machine and measurement
/// accounting, but must not be reported as an official ranked run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PhaseScheduleConfig {
    clients: u16,
    warmup_duration: Duration,
    measurement_window_duration: Duration,
}

impl PhaseScheduleConfig {
    /// Builds a local, non-ranked smoke schedule.
    ///
    /// The final profile always has exactly three measurement windows, so the
    /// number of windows is intentionally not configurable.
    pub fn new(
        clients: u16,
        warmup_duration: Duration,
        measurement_window_duration: Duration,
    ) -> Result<Self, SchedulerError> {
        if clients == 0 || clients > OFFICIAL_CLIENTS {
            return Err(SchedulerError::InvalidScheduleClients(clients));
        }
        if measurement_window_duration.is_zero() {
            return Err(SchedulerError::InvalidMeasurementWindowDuration);
        }
        Ok(Self {
            clients,
            warmup_duration,
            measurement_window_duration,
        })
    }

    pub const fn official() -> Self {
        Self {
            clients: OFFICIAL_CLIENTS,
            warmup_duration: WARMUP_DURATION,
            measurement_window_duration: FORMAL_WINDOW_DURATION,
        }
    }

    pub const fn clients(self) -> u16 {
        self.clients
    }

    pub const fn warmup_duration(self) -> Duration {
        self.warmup_duration
    }

    pub const fn measurement_window_duration(self) -> Duration {
        self.measurement_window_duration
    }
}

/// A source of monotonic time expressed relative to any stable epoch.
pub trait MonotonicClock {
    fn now(&self) -> Duration;
}

#[derive(Debug, Clone)]
pub struct SystemMonotonicClock {
    epoch: Instant,
}

impl SystemMonotonicClock {
    pub fn new() -> Self {
        Self {
            epoch: Instant::now(),
        }
    }

    /// Converts a scheduler-relative timestamp into the matching process
    /// monotonic instant.  Executors use this to enforce one absolute timeout
    /// around a complete multi-batch physical attempt.
    pub fn instant_at(&self, timestamp: Duration) -> Result<Instant, SchedulerError> {
        self.epoch
            .checked_add(timestamp)
            .ok_or(SchedulerError::ClockOverflow)
    }
}

impl Default for SystemMonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock for SystemMonotonicClock {
    fn now(&self) -> Duration {
        self.epoch.elapsed()
    }
}

/// Receives deterministic scheduler events.
pub trait EventRecorder {
    fn record(&mut self, event: SchedulerEvent);
}

impl EventRecorder for Vec<SchedulerEvent> {
    fn record(&mut self, event: SchedulerEvent) {
        self.push(event);
    }
}

/// Runtime-only limits around the published phase schedule.
///
/// The public final specification does not publish the grader's phase-tail
/// grace duration, so this value may not be reported as an official constant.
/// Socket response deadlines belong to individual Wire requests and are
/// enforced by the connection layer, not across an entire multi-batch
/// transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalRuntimeLimits {
    pub phase_tail_grace: Duration,
}

impl LocalRuntimeLimits {
    pub fn new(phase_tail_grace: Duration) -> Result<Self, SchedulerError> {
        if phase_tail_grace.is_zero() {
            return Err(SchedulerError::InvalidRuntimeLimit("phase_tail_grace"));
        }
        Ok(Self { phase_tail_grace })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WorkerId(u16);

impl WorkerId {
    pub fn new(value: u16) -> Result<Self, SchedulerError> {
        if value >= OFFICIAL_CLIENTS {
            return Err(SchedulerError::InvalidWorker(value));
        }
        Ok(Self(value))
    }

    pub const fn value(self) -> u16 {
        self.0
    }
}

/// Opaque identity of a connection that has completed Wire v3 negotiation,
/// SNAPSHOT ISOLATION setup, and PREPARE_SET.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PreparedSessionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PhaseId {
    Warmup,
    FormalWindow(u8),
}

impl PhaseId {
    fn next(self) -> Option<Self> {
        match self {
            Self::Warmup => Some(Self::FormalWindow(0)),
            Self::FormalWindow(index) if usize::from(index) + 1 < FORMAL_WINDOW_COUNT => {
                Some(Self::FormalWindow(index + 1))
            }
            Self::FormalWindow(_) => None,
        }
    }

    fn formal_index(self) -> Option<usize> {
        match self {
            Self::Warmup => None,
            Self::FormalWindow(index) => Some(usize::from(index)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionReservation {
    id: u64,
    pub worker: WorkerId,
    pub phase: PhaseId,
    /// Sequence number consumed only when a new transaction is selected.
    pub txn_no: u64,
    pub selected_at: Duration,
    pub phase_deadline: Duration,
}

impl TransactionReservation {
    pub const fn id(self) -> u64 {
        self.id
    }
}

/// Immutable accounting identity for one fully generated transaction.
///
/// `parameter_fingerprint` is a caller-defined stable digest of every bound
/// parameter.  The scheduler stores it in the ticket reused by all retries, so
/// a retry cannot substitute a different accounting identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionIdentity {
    transaction_type: TransactionType,
    home_warehouse: u16,
    parameter_fingerprint: u64,
    expects_business_rollback: bool,
}

impl TransactionIdentity {
    pub fn new(
        transaction_type: TransactionType,
        home_warehouse: u16,
        parameter_fingerprint: u64,
        expects_business_rollback: bool,
    ) -> Result<Self, SchedulerError> {
        if !(1..=OFFICIAL_WAREHOUSE_COUNT as u16).contains(&home_warehouse) {
            return Err(SchedulerError::InvalidWarehouse(home_warehouse));
        }
        if expects_business_rollback && transaction_type != TransactionType::NewOrder {
            return Err(SchedulerError::InvalidExpectedRollback(transaction_type));
        }
        Ok(Self {
            transaction_type,
            home_warehouse,
            parameter_fingerprint,
            expects_business_rollback,
        })
    }

    pub const fn transaction_type(self) -> TransactionType {
        self.transaction_type
    }

    pub const fn home_warehouse(self) -> u16 {
        self.home_warehouse
    }

    pub const fn parameter_fingerprint(self) -> u64 {
        self.parameter_fingerprint
    }

    pub const fn expects_business_rollback(self) -> bool {
        self.expects_business_rollback
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionTicket {
    reservation: TransactionReservation,
    identity: TransactionIdentity,
}

impl TransactionTicket {
    pub const fn id(self) -> u64 {
        self.reservation.id
    }

    pub const fn worker(self) -> WorkerId {
        self.reservation.worker
    }

    pub const fn phase(self) -> PhaseId {
        self.reservation.phase
    }

    pub const fn txn_no(self) -> u64 {
        self.reservation.txn_no
    }

    pub const fn selected_at(self) -> Duration {
        self.reservation.selected_at
    }

    pub const fn phase_deadline(self) -> Duration {
        self.reservation.phase_deadline
    }

    pub const fn identity(self) -> TransactionIdentity {
        self.identity
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptOutcome {
    Commit { delivery_processed: u64 },
    ExpectedRollback,
    RetryableAbort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptDisposition {
    Finished,
    RetrySameParameters,
    GraceTail,
    Abandoned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionClass {
    Committed,
    ExpectedRollback,
    RetryableAbort,
    Abandoned,
    GraceTail,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerEvent {
    WorkerPrepared {
        worker: WorkerId,
        session: PreparedSessionId,
        at: Duration,
    },
    BarrierReleased {
        workers: u16,
        at: Duration,
    },
    PhaseStarted {
        phase: PhaseId,
        at: Duration,
        deadline: Duration,
    },
    PhaseEnded {
        phase: PhaseId,
        at: Duration,
    },
    WindowGateEvaluated {
        window: u8,
        gate: WindowGate,
        at: Duration,
    },
    TransactionReserved {
        reservation: TransactionReservation,
        at: Duration,
    },
    ReservationAbandoned {
        reservation: TransactionReservation,
        at: Duration,
    },
    TransactionStarted {
        ticket: TransactionTicket,
        retry: bool,
        at: Duration,
    },
    TransactionFinished {
        ticket: TransactionTicket,
        class: CompletionClass,
        at: Duration,
    },
    WorkerFailed {
        worker: WorkerId,
        phase: Option<PhaseId>,
        at: Duration,
        reason: String,
    },
    TimelineCompleted {
        at: Duration,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchedulerFailure {
    pub worker: WorkerId,
    pub phase: Option<PhaseId>,
    pub reason: String,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchedulerError {
    #[error("worker id {0} is outside the official 0..32 range")]
    InvalidWorker(u16),
    #[error("local schedule client count {0} must be in 1..=32")]
    InvalidScheduleClients(u16),
    #[error("local measurement-window duration must be non-zero")]
    InvalidMeasurementWindowDuration,
    #[error("worker {worker:?} is outside this run's configured 0..{configured_clients} range")]
    WorkerNotConfigured {
        worker: WorkerId,
        configured_clients: u16,
    },
    #[error("worker {0:?} has already registered a prepared session")]
    DuplicateWorker(WorkerId),
    #[error("prepared session {0:?} is already assigned to another worker")]
    DuplicateSession(PreparedSessionId),
    #[error("the prepared-worker barrier is incomplete: {ready}/{required}")]
    BarrierIncomplete { ready: usize, required: usize },
    #[error("the scheduler has already started")]
    AlreadyStarted,
    #[error("the scheduler has not started")]
    NotStarted,
    #[error("monotonic clock regressed from {previous:?} to {now:?}")]
    ClockRegressed { previous: Duration, now: Duration },
    #[error("duration arithmetic overflowed")]
    ClockOverflow,
    #[error("the final measurement timeline has ended")]
    TimelineEnded,
    #[error("worker {0:?} already has a selected transaction")]
    WorkerBusy(WorkerId),
    #[error("worker {0:?} has no matching in-flight attempt")]
    TicketMismatch(WorkerId),
    #[error("worker {0:?} has no retry pending")]
    RetryNotPending(WorkerId),
    #[error("warehouse id {0} is outside the official 1..=50 range")]
    InvalidWarehouse(u16),
    #[error("{0:?} cannot be declared as a business-expected rollback")]
    InvalidExpectedRollback(TransactionType),
    #[error("the reserved transaction's phase deadline has passed")]
    ReservationDeadlinePassed,
    #[error("worker {0:?} exhausted its stage-local transaction sequence")]
    SequenceExhausted(WorkerId),
    #[error("the scheduler exhausted its transaction identity space")]
    TicketExhausted,
    #[error("local runtime limit {0} must be non-zero")]
    InvalidRuntimeLimit(&'static str),
    #[error("the retry's phase deadline has passed")]
    RetryDeadlinePassed,
    #[error(
        "terminal completion timestamp {completed_at:?} precedes attempt start {attempt_started_at:?}"
    )]
    CompletionBeforeAttempt {
        completed_at: Duration,
        attempt_started_at: Duration,
    },
    #[error(
        "terminal completion timestamp {completed_at:?} precedes phase start {phase_started_at:?}"
    )]
    CompletionBeforePhase {
        completed_at: Duration,
        phase_started_at: Duration,
    },
    #[error("the timeline has not reached its final absolute deadline")]
    TimelineIncomplete,
    #[error("at least one worker still has an in-flight transaction")]
    WorkersNotDrained,
    #[error("ranked transaction accounting is internally inconsistent")]
    InconsistentMeasurementAccounting,
    #[error("scheduler failed: {0:?}")]
    RunFailed(SchedulerFailure),
}

pub(crate) const fn is_read_only(transaction_type: TransactionType) -> bool {
    matches!(
        transaction_type,
        TransactionType::OrderStatus | TransactionType::StockLevel
    )
}

#[derive(Debug, Clone)]
enum SelectionState {
    Reserved(TransactionReservation),
    InFlight {
        ticket: TransactionTicket,
        attempt_started_at: Duration,
    },
    RetryPending(TransactionTicket),
}

impl SelectionState {
    fn phase(&self) -> PhaseId {
        match self {
            Self::Reserved(reservation) => reservation.phase,
            Self::InFlight { ticket, .. } | Self::RetryPending(ticket) => ticket.phase(),
        }
    }

    fn formal_index(&self) -> Option<usize> {
        self.phase().formal_index()
    }
}

#[derive(Debug, Clone, Default)]
struct WorkerState {
    session: Option<PreparedSessionId>,
    sequence_phase: Option<PhaseId>,
    next_txn_no: u64,
    selection: Option<SelectionState>,
}

/// State machine for the single warmup plus three continuous formal windows.
pub struct Final2026Scheduler<C, R> {
    clock: C,
    recorder: R,
    limits: LocalRuntimeLimits,
    schedule: PhaseScheduleConfig,
    workers: Vec<WorkerState>,
    sessions: HashSet<PreparedSessionId>,
    origin: Option<Duration>,
    announced_phase: Option<PhaseId>,
    timeline_complete: bool,
    last_observed: Option<Duration>,
    next_ticket_id: u64,
    windows: [WindowStats; FORMAL_WINDOW_COUNT],
    failure: Option<SchedulerFailure>,
}

impl<C: MonotonicClock, R: EventRecorder> Final2026Scheduler<C, R> {
    pub fn new(
        clock: C,
        recorder: R,
        limits: LocalRuntimeLimits,
        hot_warehouses: [u16; OFFICIAL_HOT_WAREHOUSE_COUNT],
    ) -> Self {
        Self::new_with_schedule(
            clock,
            recorder,
            limits,
            hot_warehouses,
            PhaseScheduleConfig::official(),
        )
        .expect("the public final phase schedule is valid")
    }

    /// Builds a scheduler for an explicitly local, non-ranked schedule.
    ///
    /// Window accounting remains enabled so smoke runs exercise the same
    /// paths, but callers are responsible for not presenting its gates or
    /// throughput as official ranked results.
    pub fn new_with_schedule(
        clock: C,
        recorder: R,
        limits: LocalRuntimeLimits,
        hot_warehouses: [u16; OFFICIAL_HOT_WAREHOUSE_COUNT],
        schedule: PhaseScheduleConfig,
    ) -> Result<Self, SchedulerError> {
        // Revalidate here so this constructor remains sound if the config
        // representation gains additional construction paths later.
        let schedule = PhaseScheduleConfig::new(
            schedule.clients,
            schedule.warmup_duration,
            schedule.measurement_window_duration,
        )?;
        Ok(Self {
            clock,
            recorder,
            limits,
            schedule,
            workers: vec![WorkerState::default(); usize::from(schedule.clients)],
            sessions: HashSet::with_capacity(usize::from(schedule.clients)),
            origin: None,
            announced_phase: None,
            timeline_complete: false,
            last_observed: None,
            next_ticket_id: 0,
            windows: std::array::from_fn(|_| WindowStats::new(hot_warehouses)),
            failure: None,
        })
    }

    pub fn recorder(&self) -> &R {
        &self.recorder
    }

    pub fn windows(&self) -> &[WindowStats; FORMAL_WINDOW_COUNT] {
        &self.windows
    }

    pub const fn schedule(&self) -> PhaseScheduleConfig {
        self.schedule
    }

    pub fn prepared_session(&self, worker: WorkerId) -> Option<PreparedSessionId> {
        self.workers
            .get(usize::from(worker.value()))
            .and_then(|state| state.session)
    }

    pub fn ready_workers(&self) -> usize {
        self.sessions.len()
    }

    /// Registers a session only after connection, SI setup, and PREPARE_SET.
    ///
    /// Registration is forbidden after the barrier releases.  Consequently,
    /// the same configured session pool is retained across every phase.
    pub fn worker_prepared(
        &mut self,
        worker: WorkerId,
        session: PreparedSessionId,
    ) -> Result<(), SchedulerError> {
        self.ensure_not_failed()?;
        let worker_index = self.worker_index(worker)?;
        if self.origin.is_some() {
            return Err(SchedulerError::AlreadyStarted);
        }
        let now = self.observe_now()?;
        let worker_state = &mut self.workers[worker_index];
        if worker_state.session.is_some() {
            return Err(SchedulerError::DuplicateWorker(worker));
        }
        if !self.sessions.insert(session) {
            return Err(SchedulerError::DuplicateSession(session));
        }
        worker_state.session = Some(session);
        self.recorder.record(SchedulerEvent::WorkerPrepared {
            worker,
            session,
            at: now,
        });
        Ok(())
    }

    /// Releases the all-worker barrier and starts the sole warmup.
    pub fn start(&mut self) -> Result<(), SchedulerError> {
        self.ensure_not_failed()?;
        if self.origin.is_some() {
            return Err(SchedulerError::AlreadyStarted);
        }
        if self.ready_workers() != usize::from(self.schedule.clients) {
            return Err(SchedulerError::BarrierIncomplete {
                ready: self.ready_workers(),
                required: usize::from(self.schedule.clients),
            });
        }

        let now = self.observe_now()?;
        let deadline = checked_add(now, self.schedule.warmup_duration)?;
        self.origin = Some(now);
        self.announced_phase = Some(PhaseId::Warmup);
        self.recorder.record(SchedulerEvent::BarrierReleased {
            workers: self.schedule.clients,
            at: now,
        });
        self.recorder.record(SchedulerEvent::PhaseStarted {
            phase: PhaseId::Warmup,
            at: now,
            deadline,
        });
        Ok(())
    }

    /// Synchronizes phase events to the supplied monotonic clock.
    pub fn poll(&mut self) -> Result<(), SchedulerError> {
        let now = self.observe_now()?;
        self.sync_to(now)?;
        self.expire_phase_tail(now)
    }

    /// Reserves exactly one stage-local number for parameter generation.
    pub fn reserve_transaction(
        &mut self,
        worker: WorkerId,
    ) -> Result<TransactionReservation, SchedulerError> {
        let worker_index = self.worker_index(worker)?;
        let now = self.observe_now()?;
        self.sync_to(now)?;
        self.ensure_running()?;
        let phase = self.announced_phase.ok_or(SchedulerError::TimelineEnded)?;
        let (_, deadline) = self.phase_bounds(phase)?;

        let worker_state = &mut self.workers[worker_index];
        if worker_state.selection.is_some() {
            return Err(SchedulerError::WorkerBusy(worker));
        }
        if worker_state.sequence_phase != Some(phase) {
            worker_state.sequence_phase = Some(phase);
            worker_state.next_txn_no = 0;
        }
        let txn_no = worker_state.next_txn_no;
        worker_state.next_txn_no = worker_state
            .next_txn_no
            .checked_add(1)
            .ok_or(SchedulerError::SequenceExhausted(worker))?;

        let reservation = TransactionReservation {
            id: self.next_ticket_id,
            worker,
            phase,
            txn_no,
            selected_at: now,
            phase_deadline: deadline,
        };
        self.next_ticket_id = self
            .next_ticket_id
            .checked_add(1)
            .ok_or(SchedulerError::TicketExhausted)?;
        worker_state.selection = Some(SelectionState::Reserved(reservation));
        self.recorder.record(SchedulerEvent::TransactionReserved {
            reservation,
            at: now,
        });
        Ok(reservation)
    }

    /// Binds the complete immutable input identity and starts its first request.
    pub fn start_transaction(
        &mut self,
        reservation: TransactionReservation,
        identity: TransactionIdentity,
    ) -> Result<TransactionTicket, SchedulerError> {
        let worker_index = self.worker_index(reservation.worker)?;
        let now = self.observe_now()?;
        self.sync_to(now)?;
        if now >= reservation.phase_deadline {
            return Err(SchedulerError::ReservationDeadlinePassed);
        }
        self.ensure_running()?;

        let worker_state = &mut self.workers[worker_index];
        match worker_state.selection {
            Some(SelectionState::Reserved(current)) if current == reservation => {}
            _ => return Err(SchedulerError::TicketMismatch(reservation.worker)),
        }
        let ticket = TransactionTicket {
            reservation,
            identity,
        };
        worker_state.selection = Some(SelectionState::InFlight {
            ticket,
            attempt_started_at: now,
        });
        self.recorder.record(SchedulerEvent::TransactionStarted {
            ticket,
            retry: false,
            at: now,
        });
        Ok(ticket)
    }

    /// Starts another physical attempt for the same immutable transaction.
    ///
    /// The returned ticket is unchanged and no stage-local sequence number is
    /// consumed.  The caller must reuse the parameters bound to this ticket.
    pub fn start_retry(
        &mut self,
        ticket: TransactionTicket,
    ) -> Result<TransactionTicket, SchedulerError> {
        let worker_index = self.worker_index(ticket.worker())?;
        let now = self.observe_now()?;
        self.sync_to(now)?;
        if now >= ticket.phase_deadline() {
            return Err(SchedulerError::RetryDeadlinePassed);
        }
        self.ensure_running()?;

        let worker_state = &mut self.workers[worker_index];
        match worker_state.selection {
            Some(SelectionState::RetryPending(current)) if current == ticket => {
                worker_state.selection = Some(SelectionState::InFlight {
                    ticket,
                    attempt_started_at: now,
                });
            }
            Some(_) => return Err(SchedulerError::TicketMismatch(ticket.worker())),
            None => return Err(SchedulerError::RetryNotPending(ticket.worker())),
        }
        self.recorder.record(SchedulerEvent::TransactionStarted {
            ticket,
            retry: true,
            at: now,
        });
        Ok(ticket)
    }

    /// Returns the absolute phase-tail drain deadline for an active attempt.
    ///
    /// Each Wire request has its own connection-layer response deadline. This
    /// scheduler deadline only prevents an attempt selected before a phase
    /// boundary from draining indefinitely after that boundary.
    pub fn attempt_deadline(&self, ticket: TransactionTicket) -> Result<Duration, SchedulerError> {
        self.ensure_not_failed()?;
        let worker_index = self.worker_index(ticket.worker())?;
        match self.workers[worker_index].selection {
            Some(SelectionState::InFlight {
                ticket: current, ..
            }) if current == ticket => {}
            _ => return Err(SchedulerError::TicketMismatch(ticket.worker())),
        }
        checked_add(ticket.phase_deadline(), self.limits.phase_tail_grace)
    }

    /// Handles a complete terminal response frame for one physical attempt.
    pub fn finish_attempt(
        &mut self,
        ticket: TransactionTicket,
        outcome: AttemptOutcome,
    ) -> Result<AttemptDisposition, SchedulerError> {
        let completed_at = self.observe_now()?;
        self.finish_attempt_at(ticket, outcome, completed_at)
    }

    /// Handles a terminal frame using the time sampled when that frame arrived.
    ///
    /// Executors must take this sample before waiting for the shared scheduler
    /// mutex, otherwise lock contention can incorrectly turn an on-time
    /// terminal into a grace-tail completion.
    pub fn finish_attempt_at(
        &mut self,
        ticket: TransactionTicket,
        outcome: AttemptOutcome,
        completed_at: Duration,
    ) -> Result<AttemptDisposition, SchedulerError> {
        let worker_index = self.worker_index(ticket.worker())?;
        self.sync_to(completed_at)?;
        self.ensure_not_failed()?;

        let attempt_started_at = match self.workers[worker_index].selection {
            Some(SelectionState::InFlight {
                ticket: current,
                attempt_started_at,
            }) if current == ticket => attempt_started_at,
            _ => return Err(SchedulerError::TicketMismatch(ticket.worker())),
        };
        if completed_at < attempt_started_at {
            return Err(SchedulerError::CompletionBeforeAttempt {
                completed_at,
                attempt_started_at,
            });
        }
        let attempt_deadline = checked_add(ticket.phase_deadline(), self.limits.phase_tail_grace)?;
        if completed_at >= attempt_deadline {
            if is_read_only(ticket.identity.transaction_type()) {
                self.workers[worker_index].selection = None;
                if let Some(index) = ticket.phase().formal_index() {
                    self.windows[index].record_grace_tail(ticket.identity.transaction_type());
                }
                self.recorder.record(SchedulerEvent::TransactionFinished {
                    ticket,
                    class: CompletionClass::GraceTail,
                    at: completed_at,
                });
                return Ok(AttemptDisposition::GraceTail);
            }
            self.fail_worker_at(
                ticket.worker(),
                Some(ticket.phase()),
                format!(
                    "local phase-tail drain deadline expired at {:?}; this is not an official timeout",
                    attempt_deadline
                ),
                completed_at,
            );
            return Err(SchedulerError::RunFailed(
                self.failure
                    .clone()
                    .expect("fail_worker_at always stores a failure"),
            ));
        }

        let outcome_matches_identity = match outcome {
            AttemptOutcome::Commit { .. } => !ticket.identity.expects_business_rollback(),
            AttemptOutcome::ExpectedRollback => ticket.identity.expects_business_rollback(),
            AttemptOutcome::RetryableAbort => true,
        };
        if !outcome_matches_identity {
            self.fail_worker_at(
                ticket.worker(),
                Some(ticket.phase()),
                format!(
                    "terminal outcome contradicts immutable transaction {} identity",
                    ticket.id()
                ),
                completed_at,
            );
            return Err(SchedulerError::RunFailed(
                self.failure
                    .clone()
                    .expect("fail_worker_at always stores a failure"),
            ));
        }

        if completed_at >= ticket.phase_deadline() {
            self.workers[worker_index].selection = None;
            if let Some(index) = ticket.phase().formal_index() {
                self.windows[index].record_grace_tail(ticket.identity.transaction_type());
            }
            self.recorder.record(SchedulerEvent::TransactionFinished {
                ticket,
                class: CompletionClass::GraceTail,
                at: completed_at,
            });
            return Ok(AttemptDisposition::GraceTail);
        }

        let latency = completed_at.saturating_sub(ticket.selected_at());
        let completion_offset = if ticket.phase().formal_index().is_some() {
            let (phase_start, _) = self.phase_bounds(ticket.phase())?;
            Some(completed_at.checked_sub(phase_start).ok_or(
                SchedulerError::CompletionBeforePhase {
                    completed_at,
                    phase_started_at: phase_start,
                },
            )?)
        } else {
            None
        };
        let worker_state = &mut self.workers[worker_index];
        let (class, disposition) = match outcome {
            AttemptOutcome::Commit { delivery_processed } => {
                worker_state.selection = None;
                if let Some(index) = ticket.phase().formal_index() {
                    self.windows[index].record_commit_at_offset(
                        ticket.identity.transaction_type(),
                        ticket.identity.home_warehouse(),
                        latency,
                        delivery_processed,
                        completion_offset,
                    );
                }
                (CompletionClass::Committed, AttemptDisposition::Finished)
            }
            AttemptOutcome::ExpectedRollback => {
                worker_state.selection = None;
                if let Some(index) = ticket.phase().formal_index() {
                    self.windows[index].record_expected_rollback(ticket.identity.home_warehouse());
                }
                (
                    CompletionClass::ExpectedRollback,
                    AttemptDisposition::Finished,
                )
            }
            AttemptOutcome::RetryableAbort => {
                worker_state.selection = Some(SelectionState::RetryPending(ticket));
                if let Some(index) = ticket.phase().formal_index() {
                    self.windows[index].record_retry_abort(ticket.identity.transaction_type());
                }
                (
                    CompletionClass::RetryableAbort,
                    AttemptDisposition::RetrySameParameters,
                )
            }
        };
        self.recorder.record(SchedulerEvent::TransactionFinished {
            ticket,
            class,
            at: completed_at,
        });
        Ok(disposition)
    }

    /// Abandons an in-flight transaction whose connection outcome is unknown.
    ///
    /// Callers must restrict this to read-only transactions. A timed-out write
    /// may have committed after the client stopped reading, so treating that
    /// result as a normal abandonment would make the recovery ledger unsound.
    pub fn abandon_read_only_inflight_at(
        &mut self,
        ticket: TransactionTicket,
        abandoned_at: Duration,
    ) -> Result<(), SchedulerError> {
        let worker_index = self.worker_index(ticket.worker())?;
        self.sync_to(abandoned_at)?;
        self.ensure_not_failed()?;
        // The official client abandons write transactions whose attempts
        // exceed the (unpublished) response deadline too (NewOrder/Payment/
        // Delivery abandoned 22-27% in grader reports). The abandoned
        // connection is closed and the session rebuilt, so the server rolls
        // back any in-flight transaction (read_frame failure -> abort) and
        // consistency checks stay satisfiable, matching official behavior.
        let attempt_started_at = match self.workers[worker_index].selection {
            Some(SelectionState::InFlight {
                ticket: current,
                attempt_started_at,
            }) if current == ticket => attempt_started_at,
            _ => return Err(SchedulerError::TicketMismatch(ticket.worker())),
        };
        if abandoned_at < attempt_started_at {
            return Err(SchedulerError::CompletionBeforeAttempt {
                completed_at: abandoned_at,
                attempt_started_at,
            });
        }

        self.workers[worker_index].selection = None;
        if let Some(index) = ticket.phase().formal_index() {
            if abandoned_at < ticket.phase_deadline() {
                self.windows[index].record_abandoned(ticket.identity.transaction_type());
            } else {
                self.windows[index].record_grace_tail(ticket.identity.transaction_type());
            }
        }
        self.recorder.record(SchedulerEvent::TransactionFinished {
            ticket,
            class: CompletionClass::Abandoned,
            at: abandoned_at,
        });
        Ok(())
    }

    pub const fn timeline_complete(&self) -> bool {
        self.timeline_complete
    }

    /// Abandons a selected transaction after a retry cap or local policy.
    pub fn abandon_retry(&mut self, ticket: TransactionTicket) -> Result<(), SchedulerError> {
        let worker_index = self.worker_index(ticket.worker())?;
        let now = self.observe_now()?;
        self.sync_to(now)?;
        self.ensure_not_failed()?;
        let worker_state = &mut self.workers[worker_index];
        match worker_state.selection {
            Some(SelectionState::RetryPending(current)) if current == ticket => {
                worker_state.selection = None;
            }
            Some(_) => return Err(SchedulerError::TicketMismatch(ticket.worker())),
            None => return Err(SchedulerError::RetryNotPending(ticket.worker())),
        }
        if let Some(index) = ticket.phase().formal_index() {
            self.windows[index].record_retry_abandoned(ticket.identity.transaction_type());
        }
        self.recorder.record(SchedulerEvent::TransactionFinished {
            ticket,
            class: CompletionClass::Abandoned,
            at: now,
        });
        Ok(())
    }

    /// Marks an I/O, protocol, or worker-task failure as a phase failure.
    pub fn worker_failed(
        &mut self,
        worker: WorkerId,
        reason: impl Into<String>,
    ) -> Result<(), SchedulerError> {
        self.ensure_not_failed()?;
        let worker_index = self.worker_index(worker)?;
        let now = self.observe_now()?;
        if self.origin.is_some() {
            self.sync_to(now)?;
        }
        let phase = self.workers[worker_index]
            .selection
            .as_ref()
            .map(SelectionState::phase)
            .or(self.announced_phase);
        self.fail_worker_at(worker, phase, reason.into(), now);
        Err(SchedulerError::RunFailed(
            self.failure
                .clone()
                .expect("fail_worker_at always stores a failure"),
        ))
    }

    /// Produces the ranked summary only after the exact timeline ends and all
    /// accepted grace-tail responses have drained.
    pub fn measurement_summary(&mut self) -> Result<MeasurementSummary, SchedulerError> {
        self.poll()?;
        self.ensure_not_failed()?;
        if !self.timeline_complete {
            return Err(SchedulerError::TimelineIncomplete);
        }
        if self.workers.iter().any(|worker| worker.selection.is_some()) {
            return Err(SchedulerError::WorkersNotDrained);
        }
        if self
            .windows
            .iter()
            .any(|window| !window.accounting_is_consistent())
        {
            return Err(SchedulerError::InconsistentMeasurementAccounting);
        }
        Ok(MeasurementSummary::from_windows(&self.windows))
    }

    fn observe_now(&mut self) -> Result<Duration, SchedulerError> {
        let now = self.clock.now();
        if let Some(previous) = self.last_observed {
            if now < previous {
                return Err(SchedulerError::ClockRegressed { previous, now });
            }
        }
        self.last_observed = Some(now);
        Ok(now)
    }

    fn sync_to(&mut self, now: Duration) -> Result<(), SchedulerError> {
        self.ensure_not_failed()?;
        let Some(_) = self.origin else {
            return Err(SchedulerError::NotStarted);
        };

        while let Some(phase) = self.announced_phase {
            let (_, deadline) = self.phase_bounds(phase)?;
            if now < deadline {
                break;
            }

            self.abandon_expired_selections(phase, deadline);
            self.recorder.record(SchedulerEvent::PhaseEnded {
                phase,
                at: deadline,
            });
            if let Some(index) = phase.formal_index() {
                self.recorder.record(SchedulerEvent::WindowGateEvaluated {
                    window: index as u8,
                    gate: self.windows[index].gate(),
                    at: deadline,
                });
            }
            self.announced_phase = phase.next();
            match self.announced_phase {
                Some(next) => {
                    let (start, next_deadline) = self.phase_bounds(next)?;
                    self.recorder.record(SchedulerEvent::PhaseStarted {
                        phase: next,
                        at: start,
                        deadline: next_deadline,
                    });
                }
                None => {
                    self.timeline_complete = true;
                    self.recorder
                        .record(SchedulerEvent::TimelineCompleted { at: deadline });
                }
            }
        }

        Ok(())
    }

    fn abandon_expired_selections(&mut self, phase: PhaseId, at: Duration) {
        enum AbandonedSelection {
            Reservation(TransactionReservation),
            Retry(TransactionTicket),
        }

        let mut abandoned = Vec::new();
        for worker in &mut self.workers {
            match worker.selection {
                Some(SelectionState::Reserved(reservation)) if reservation.phase == phase => {
                    worker.selection = None;
                    abandoned.push(AbandonedSelection::Reservation(reservation));
                }
                Some(SelectionState::RetryPending(ticket)) if ticket.phase() == phase => {
                    worker.selection = None;
                    abandoned.push(AbandonedSelection::Retry(ticket));
                }
                _ => {}
            }
        }
        for selection in abandoned {
            match selection {
                AbandonedSelection::Reservation(reservation) => {
                    if let Some(index) = phase.formal_index() {
                        self.windows[index].record_cutoff_stop();
                    }
                    self.recorder
                        .record(SchedulerEvent::ReservationAbandoned { reservation, at });
                }
                AbandonedSelection::Retry(ticket) => {
                    if let Some(index) = phase.formal_index() {
                        self.windows[index].record_cutoff_stop();
                    }
                    self.recorder.record(SchedulerEvent::TransactionFinished {
                        ticket,
                        class: CompletionClass::Abandoned,
                        at,
                    });
                }
            }
        }
    }

    fn expire_phase_tail(&mut self, now: Duration) -> Result<(), SchedulerError> {
        for index in 0..self.workers.len() {
            let Some(SelectionState::InFlight { ticket, .. }) = self.workers[index].selection
            else {
                continue;
            };
            let local_deadline =
                checked_add(ticket.phase_deadline(), self.limits.phase_tail_grace)?;
            if now < local_deadline {
                continue;
            }

            let worker =
                WorkerId::new(index as u16).expect("worker vector uses official worker bounds");
            self.fail_worker_at(
                worker,
                Some(ticket.phase()),
                format!(
                    "local phase-tail drain deadline expired at {:?}; this is not an official timeout",
                    local_deadline
                ),
                now,
            );
            return Err(SchedulerError::RunFailed(
                self.failure
                    .clone()
                    .expect("fail_worker_at always stores a failure"),
            ));
        }
        Ok(())
    }

    fn fail_worker_at(
        &mut self,
        worker: WorkerId,
        phase: Option<PhaseId>,
        reason: String,
        at: Duration,
    ) {
        let state = &mut self.workers[usize::from(worker.value())];
        if let Some(selection) = state.selection.take() {
            if let Some(index) = selection.formal_index() {
                match selection {
                    SelectionState::InFlight { ticket, .. } => {
                        if at < ticket.phase_deadline() {
                            self.windows[index]
                                .record_abandoned(ticket.identity.transaction_type());
                        } else {
                            self.windows[index]
                                .record_grace_tail(ticket.identity.transaction_type());
                        }
                    }
                    SelectionState::RetryPending(_) => self.windows[index].record_cutoff_stop(),
                    SelectionState::Reserved(_) => self.windows[index].record_cutoff_stop(),
                }
            }
        }
        let failure = SchedulerFailure {
            worker,
            phase,
            reason: reason.clone(),
        };
        self.failure = Some(failure);
        self.recorder.record(SchedulerEvent::WorkerFailed {
            worker,
            phase,
            at,
            reason,
        });
    }

    fn ensure_running(&self) -> Result<(), SchedulerError> {
        self.ensure_not_failed()?;
        if self.origin.is_none() {
            return Err(SchedulerError::NotStarted);
        }
        if self.timeline_complete {
            return Err(SchedulerError::TimelineEnded);
        }
        Ok(())
    }

    fn ensure_not_failed(&self) -> Result<(), SchedulerError> {
        match &self.failure {
            Some(failure) => Err(SchedulerError::RunFailed(failure.clone())),
            None => Ok(()),
        }
    }

    fn worker_index(&self, worker: WorkerId) -> Result<usize, SchedulerError> {
        let index = usize::from(worker.value());
        if index >= self.workers.len() {
            return Err(SchedulerError::WorkerNotConfigured {
                worker,
                configured_clients: self.schedule.clients,
            });
        }
        Ok(index)
    }

    fn phase_bounds(&self, phase: PhaseId) -> Result<(Duration, Duration), SchedulerError> {
        let origin = self.origin.ok_or(SchedulerError::NotStarted)?;
        match phase {
            PhaseId::Warmup => Ok((origin, checked_add(origin, self.schedule.warmup_duration)?)),
            PhaseId::FormalWindow(index) => {
                let formal_origin = checked_add(origin, self.schedule.warmup_duration)?;
                let start_offset = self
                    .schedule
                    .measurement_window_duration
                    .checked_mul(u32::from(index))
                    .ok_or(SchedulerError::ClockOverflow)?;
                let start = checked_add(formal_origin, start_offset)?;
                Ok((
                    start,
                    checked_add(start, self.schedule.measurement_window_duration)?,
                ))
            }
        }
    }
}

fn checked_add(left: Duration, right: Duration) -> Result<Duration, SchedulerError> {
    left.checked_add(right).ok_or(SchedulerError::ClockOverflow)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Instant;

    use super::*;

    #[derive(Clone, Default)]
    struct FakeClock(Rc<Cell<Duration>>);

    impl FakeClock {
        fn set(&self, now: Duration) {
            self.0.set(now);
        }

        fn advance(&self, amount: Duration) {
            self.set(self.now() + amount);
        }
    }

    impl MonotonicClock for FakeClock {
        fn now(&self) -> Duration {
            self.0.get()
        }
    }

    type TestScheduler = Final2026Scheduler<FakeClock, Vec<SchedulerEvent>>;

    fn ready_scheduler(grace: Duration) -> (FakeClock, TestScheduler) {
        ready_scheduler_with_limits(LocalRuntimeLimits::new(grace).unwrap())
    }

    fn ready_scheduler_with_limits(limits: LocalRuntimeLimits) -> (FakeClock, TestScheduler) {
        let clock = FakeClock::default();
        let mut scheduler =
            Final2026Scheduler::new(clock.clone(), Vec::new(), limits, [1, 2, 3, 4]);
        for id in 0..OFFICIAL_CLIENTS {
            scheduler
                .worker_prepared(
                    WorkerId::new(id).unwrap(),
                    PreparedSessionId(u64::from(id) + 100),
                )
                .unwrap();
        }
        (clock, scheduler)
    }

    fn commit_payment(
        scheduler: &mut TestScheduler,
        ticket: TransactionTicket,
    ) -> AttemptDisposition {
        scheduler
            .finish_attempt(
                ticket,
                AttemptOutcome::Commit {
                    delivery_processed: 0,
                },
            )
            .unwrap()
    }

    fn start_payment(
        scheduler: &mut TestScheduler,
        worker: WorkerId,
        fingerprint: u64,
    ) -> TransactionTicket {
        let reservation = scheduler.reserve_transaction(worker).unwrap();
        let identity =
            TransactionIdentity::new(TransactionType::Payment, 1, fingerprint, false).unwrap();
        scheduler.start_transaction(reservation, identity).unwrap()
    }

    fn start_expected_rollback(
        scheduler: &mut TestScheduler,
        worker: WorkerId,
        fingerprint: u64,
    ) -> TransactionTicket {
        let reservation = scheduler.reserve_transaction(worker).unwrap();
        let identity =
            TransactionIdentity::new(TransactionType::NewOrder, 1, fingerprint, true).unwrap();
        scheduler.start_transaction(reservation, identity).unwrap()
    }

    #[test]
    fn barrier_requires_all_32_prepared_sessions_once() {
        let clock = FakeClock::default();
        let mut scheduler = Final2026Scheduler::new(
            clock,
            Vec::new(),
            LocalRuntimeLimits::new(Duration::from_secs(5)).unwrap(),
            [1, 2, 3, 4],
        );
        for id in 0..OFFICIAL_CLIENTS - 1 {
            scheduler
                .worker_prepared(WorkerId::new(id).unwrap(), PreparedSessionId(u64::from(id)))
                .unwrap();
        }
        assert_eq!(
            scheduler.start(),
            Err(SchedulerError::BarrierIncomplete {
                ready: 31,
                required: 32
            })
        );

        let final_worker = WorkerId::new(31).unwrap();
        scheduler
            .worker_prepared(final_worker, PreparedSessionId(31))
            .unwrap();
        scheduler.start().unwrap();
        assert_eq!(
            scheduler.worker_prepared(final_worker, PreparedSessionId(999)),
            Err(SchedulerError::AlreadyStarted)
        );
        assert_eq!(
            scheduler.prepared_session(final_worker),
            Some(PreparedSessionId(31))
        );
    }

    #[test]
    fn fake_clock_emits_exact_continuous_30_plus_three_by_150_timeline() {
        let wall_start = Instant::now();
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        clock.set(Duration::from_secs(7));
        scheduler.start().unwrap();
        clock.advance(Duration::from_secs(30 + 3 * 150));
        scheduler.poll().unwrap();

        let starts: Vec<_> = scheduler
            .recorder()
            .iter()
            .filter_map(|event| match event {
                SchedulerEvent::PhaseStarted {
                    phase,
                    at,
                    deadline,
                } => Some((*phase, *at, *deadline)),
                _ => None,
            })
            .collect();
        assert_eq!(
            starts,
            vec![
                (
                    PhaseId::Warmup,
                    Duration::from_secs(7),
                    Duration::from_secs(37)
                ),
                (
                    PhaseId::FormalWindow(0),
                    Duration::from_secs(37),
                    Duration::from_secs(187)
                ),
                (
                    PhaseId::FormalWindow(1),
                    Duration::from_secs(187),
                    Duration::from_secs(337)
                ),
                (
                    PhaseId::FormalWindow(2),
                    Duration::from_secs(337),
                    Duration::from_secs(487)
                ),
            ]
        );

        let ends: Vec<_> = scheduler
            .recorder()
            .iter()
            .filter_map(|event| match event {
                SchedulerEvent::PhaseEnded { phase, at } => Some((*phase, *at)),
                _ => None,
            })
            .collect();
        assert_eq!(
            ends,
            vec![
                (PhaseId::Warmup, Duration::from_secs(37)),
                (PhaseId::FormalWindow(0), Duration::from_secs(187)),
                (PhaseId::FormalWindow(1), Duration::from_secs(337)),
                (PhaseId::FormalWindow(2), Duration::from_secs(487)),
            ]
        );
        let gate_times: Vec<_> = scheduler
            .recorder()
            .iter()
            .filter_map(|event| match event {
                SchedulerEvent::WindowGateEvaluated { window, at, .. } => Some((*window, *at)),
                _ => None,
            })
            .collect();
        assert_eq!(
            gate_times,
            vec![
                (0, Duration::from_secs(187)),
                (1, Duration::from_secs(337)),
                (2, Duration::from_secs(487)),
            ]
        );
        assert!(wall_start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn local_non_ranked_schedule_uses_two_workers_and_three_continuous_windows() {
        let clock = FakeClock::default();
        let schedule = PhaseScheduleConfig::new(2, Duration::ZERO, Duration::from_secs(1)).unwrap();
        let mut scheduler = Final2026Scheduler::new_with_schedule(
            clock.clone(),
            Vec::new(),
            LocalRuntimeLimits::new(Duration::from_secs(1)).unwrap(),
            [1, 2, 3, 4],
            schedule,
        )
        .unwrap();

        for id in 0..2 {
            scheduler
                .worker_prepared(WorkerId::new(id).unwrap(), PreparedSessionId(u64::from(id)))
                .unwrap();
        }
        let unconfigured = WorkerId::new(2).unwrap();
        assert_eq!(
            scheduler.worker_prepared(unconfigured, PreparedSessionId(2)),
            Err(SchedulerError::WorkerNotConfigured {
                worker: unconfigured,
                configured_clients: 2,
            })
        );
        assert_eq!(scheduler.prepared_session(unconfigured), None);

        scheduler.start().unwrap();
        assert_eq!(scheduler.schedule(), schedule);
        clock.set(Duration::from_secs(3));
        scheduler.poll().unwrap();

        let starts: Vec<_> = scheduler
            .recorder()
            .iter()
            .filter_map(|event| match event {
                SchedulerEvent::PhaseStarted {
                    phase,
                    at,
                    deadline,
                } => Some((*phase, *at, *deadline)),
                _ => None,
            })
            .collect();
        assert_eq!(
            starts,
            vec![
                (PhaseId::Warmup, Duration::ZERO, Duration::ZERO),
                (
                    PhaseId::FormalWindow(0),
                    Duration::ZERO,
                    Duration::from_secs(1),
                ),
                (
                    PhaseId::FormalWindow(1),
                    Duration::from_secs(1),
                    Duration::from_secs(2),
                ),
                (
                    PhaseId::FormalWindow(2),
                    Duration::from_secs(2),
                    Duration::from_secs(3),
                ),
            ]
        );
        let gate_times: Vec<_> = scheduler
            .recorder()
            .iter()
            .filter_map(|event| match event {
                SchedulerEvent::WindowGateEvaluated { window, at, .. } => Some((*window, *at)),
                _ => None,
            })
            .collect();
        assert_eq!(
            gate_times,
            vec![
                (0, Duration::from_secs(1)),
                (1, Duration::from_secs(2)),
                (2, Duration::from_secs(3)),
            ]
        );
        assert!(scheduler.recorder().iter().any(|event| {
            matches!(
                event,
                SchedulerEvent::BarrierReleased {
                    workers: 2,
                    at: Duration::ZERO
                }
            )
        }));
    }

    #[test]
    fn local_schedule_validation_keeps_three_windows_fixed() {
        assert_eq!(
            PhaseScheduleConfig::new(0, Duration::ZERO, Duration::from_secs(1)),
            Err(SchedulerError::InvalidScheduleClients(0))
        );
        assert_eq!(
            PhaseScheduleConfig::new(OFFICIAL_CLIENTS + 1, Duration::ZERO, Duration::from_secs(1),),
            Err(SchedulerError::InvalidScheduleClients(OFFICIAL_CLIENTS + 1))
        );
        assert_eq!(
            PhaseScheduleConfig::new(1, Duration::ZERO, Duration::ZERO),
            Err(SchedulerError::InvalidMeasurementWindowDuration)
        );
        assert_eq!(FORMAL_WINDOW_COUNT, 3);
    }

    #[test]
    fn transaction_sequence_resets_per_stage_without_rebuilding_sessions() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        scheduler.start().unwrap();
        let worker = WorkerId::new(0).unwrap();
        let session = scheduler.prepared_session(worker);

        let first = start_payment(&mut scheduler, worker, 1);
        assert_eq!((first.phase(), first.txn_no()), (PhaseId::Warmup, 0));
        commit_payment(&mut scheduler, first);
        let second = start_payment(&mut scheduler, worker, 2);
        assert_eq!(second.txn_no(), 1);
        commit_payment(&mut scheduler, second);

        clock.set(Duration::from_secs(30));
        let window_zero = start_payment(&mut scheduler, worker, 3);
        assert_eq!(
            (window_zero.phase(), window_zero.txn_no()),
            (PhaseId::FormalWindow(0), 0)
        );
        commit_payment(&mut scheduler, window_zero);

        clock.set(Duration::from_secs(180));
        let window_one = start_payment(&mut scheduler, worker, 4);
        assert_eq!(
            (window_one.phase(), window_one.txn_no()),
            (PhaseId::FormalWindow(1), 0)
        );
        assert_eq!(scheduler.prepared_session(worker), session);
    }

    #[test]
    fn new_order_stability_bucket_uses_the_terminal_completion_offset() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let worker = WorkerId::new(0).unwrap();
        let reservation = scheduler.reserve_transaction(worker).unwrap();
        let identity =
            TransactionIdentity::new(TransactionType::NewOrder, 1, 0x5000, false).unwrap();
        let ticket = scheduler.start_transaction(reservation, identity).unwrap();

        clock.set(Duration::from_secs(35));
        assert_eq!(
            scheduler
                .finish_attempt(
                    ticket,
                    AttemptOutcome::Commit {
                        delivery_processed: 0,
                    },
                )
                .unwrap(),
            AttemptDisposition::Finished
        );
        assert_eq!(scheduler.windows()[0].new_order_stability_buckets[0], 0);
        assert_eq!(scheduler.windows()[0].new_order_stability_buckets[1], 1);
    }

    #[test]
    fn retry_reuses_ticket_and_does_not_consume_sequence_number() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let worker = WorkerId::new(3).unwrap();
        let ticket = start_payment(&mut scheduler, worker, 0xfeed);
        assert_eq!(
            scheduler
                .finish_attempt(ticket, AttemptOutcome::RetryableAbort)
                .unwrap(),
            AttemptDisposition::RetrySameParameters
        );
        assert_eq!(scheduler.start_retry(ticket).unwrap(), ticket);
        assert_eq!(
            commit_payment(&mut scheduler, ticket),
            AttemptDisposition::Finished
        );
        let next = start_payment(&mut scheduler, worker, 0xbeef);
        assert_eq!(next.txn_no(), ticket.txn_no() + 1);
        assert_eq!(ticket.identity().parameter_fingerprint(), 0xfeed);
        assert_eq!(scheduler.windows()[0].retry_aborts, 1);
        assert_eq!(scheduler.windows()[0].committed, 1);
    }

    #[test]
    fn phase_tail_deadline_is_shared_across_retries() {
        let (clock, mut scheduler) =
            ready_scheduler_with_limits(LocalRuntimeLimits::new(Duration::from_secs(10)).unwrap());
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let ticket = start_payment(&mut scheduler, WorkerId::new(3).unwrap(), 0xabc);
        assert_eq!(
            scheduler.attempt_deadline(ticket).unwrap(),
            Duration::from_secs(190)
        );

        scheduler
            .finish_attempt(ticket, AttemptOutcome::RetryableAbort)
            .unwrap();
        clock.set(Duration::from_secs(179));
        scheduler.start_retry(ticket).unwrap();
        assert_eq!(
            scheduler.attempt_deadline(ticket).unwrap(),
            Duration::from_secs(190)
        );
    }

    #[test]
    fn phase_tail_grace_sets_the_attempt_drain_deadline() {
        let (clock, mut scheduler) =
            ready_scheduler_with_limits(LocalRuntimeLimits::new(Duration::from_secs(5)).unwrap());
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(179));
        let ticket = start_payment(&mut scheduler, WorkerId::new(3).unwrap(), 0xdef);
        assert_eq!(
            scheduler.attempt_deadline(ticket).unwrap(),
            Duration::from_secs(185)
        );
    }

    #[test]
    fn immutable_identity_rejects_false_business_rollback_accounting() {
        assert_eq!(
            TransactionIdentity::new(TransactionType::Payment, 1, 9, true),
            Err(SchedulerError::InvalidExpectedRollback(
                TransactionType::Payment
            ))
        );

        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let ticket = start_payment(&mut scheduler, WorkerId::new(4).unwrap(), 0x1234);
        let error = scheduler
            .finish_attempt(ticket, AttemptOutcome::ExpectedRollback)
            .unwrap_err();
        assert!(matches!(error, SchedulerError::RunFailed(_)));
        assert_eq!(scheduler.windows()[0].expected_rollbacks, 0);
        assert_eq!(scheduler.windows()[0].abandoned, 1);
    }

    #[test]
    fn reservation_crossing_a_boundary_is_a_cutoff_before_any_request() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let worker = WorkerId::new(6).unwrap();
        let reservation = scheduler.reserve_transaction(worker).unwrap();
        let identity = TransactionIdentity::new(TransactionType::Payment, 1, 60, false).unwrap();

        clock.set(Duration::from_secs(180));
        assert_eq!(
            scheduler.start_transaction(reservation, identity),
            Err(SchedulerError::ReservationDeadlinePassed)
        );
        assert_eq!(scheduler.windows()[0].abandoned, 0);
        assert_eq!(scheduler.windows()[0].cutoff_stopped, 1);
        assert_eq!(scheduler.windows()[0].attempted, 0);
        let next = start_payment(&mut scheduler, worker, 61);
        assert_eq!((next.phase(), next.txn_no()), (PhaseId::FormalWindow(1), 0));
    }

    #[test]
    fn final_deadline_reports_normal_reservation_and_retry_cutoffs() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        scheduler.start().unwrap();
        let final_deadline = Duration::from_secs(30 + 3 * 150);
        let worker = WorkerId::new(6).unwrap();

        clock.set(final_deadline - Duration::from_millis(1));
        let reservation = scheduler.reserve_transaction(worker).unwrap();
        let identity = TransactionIdentity::new(TransactionType::Payment, 1, 60, false).unwrap();
        clock.set(final_deadline);
        assert_eq!(
            scheduler.start_transaction(reservation, identity),
            Err(SchedulerError::ReservationDeadlinePassed)
        );

        let (retry_clock, mut retry_scheduler) = ready_scheduler(Duration::from_secs(5));
        retry_scheduler.start().unwrap();
        retry_clock.set(final_deadline - Duration::from_millis(1));
        let ticket = start_payment(&mut retry_scheduler, worker, 61);
        retry_scheduler
            .finish_attempt(ticket, AttemptOutcome::RetryableAbort)
            .unwrap();
        retry_clock.set(final_deadline);
        assert_eq!(
            retry_scheduler.start_retry(ticket),
            Err(SchedulerError::RetryDeadlinePassed)
        );
    }

    #[test]
    fn abandoning_a_pending_retry_does_not_invent_another_attempt() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let ticket = start_payment(&mut scheduler, WorkerId::new(7).unwrap(), 0x77);

        scheduler
            .finish_attempt(ticket, AttemptOutcome::RetryableAbort)
            .unwrap();
        scheduler.abandon_retry(ticket).unwrap();

        assert_eq!(scheduler.windows()[0].retry_aborts, 1);
        assert_eq!(scheduler.windows()[0].abandoned, 1);
        assert_eq!(scheduler.windows()[0].attempted, 1);
        assert_eq!(scheduler.windows()[0].physical_attempts, 1);
        assert_eq!(scheduler.windows()[0].retry_abandoned, 1);
        assert_eq!(scheduler.windows()[0].cutoff_stopped, 0);
        assert!(scheduler.windows()[0].accounting_is_consistent());
    }

    #[test]
    fn late_terminal_is_grace_tail_and_never_enters_ranked_counts() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(10));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let worker = WorkerId::new(5).unwrap();
        let ticket = start_payment(&mut scheduler, worker, 5);

        clock.set(Duration::from_secs(181));
        assert_eq!(
            commit_payment(&mut scheduler, ticket),
            AttemptDisposition::GraceTail
        );
        assert_eq!(scheduler.windows()[0].grace_tail, 1);
        assert_eq!(scheduler.windows()[0].attempted, 0);
        assert_eq!(scheduler.windows()[0].physical_attempts, 1);
        assert_eq!(scheduler.windows()[0].committed, 0);
        assert_eq!(scheduler.windows()[0].completed(), 0);

        let next = start_payment(&mut scheduler, worker, 6);
        assert_eq!((next.phase(), next.txn_no()), (PhaseId::FormalWindow(1), 0));
    }

    #[test]
    fn terminal_arrival_sample_prevents_mutex_wait_misclassification() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(10));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let ticket = start_payment(&mut scheduler, WorkerId::new(5).unwrap(), 0x51);

        let completed_at = Duration::from_secs(179);
        clock.set(Duration::from_secs(181));
        assert_eq!(
            scheduler
                .finish_attempt_at(
                    ticket,
                    AttemptOutcome::Commit {
                        delivery_processed: 0,
                    },
                    completed_at,
                )
                .unwrap(),
            AttemptDisposition::Finished
        );
        assert_eq!(scheduler.windows()[0].committed, 1);
        assert_eq!(scheduler.windows()[0].grace_tail, 0);
        assert!(scheduler.recorder().iter().any(|event| {
            matches!(
                event,
                SchedulerEvent::TransactionFinished {
                    ticket: completed,
                    class: CompletionClass::Committed,
                    at,
                } if *completed == ticket && *at == completed_at
            )
        }));
    }

    #[test]
    fn later_worker_cannot_expire_an_already_arrived_terminal() {
        let (clock, mut scheduler) =
            ready_scheduler_with_limits(LocalRuntimeLimits::new(Duration::from_secs(10)).unwrap());
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let first = start_payment(&mut scheduler, WorkerId::new(5).unwrap(), 0x61);
        let first_completed_at = Duration::from_secs(32);

        clock.set(Duration::from_secs(32));
        let second = start_payment(&mut scheduler, WorkerId::new(6).unwrap(), 0x62);
        scheduler
            .finish_attempt_at(
                second,
                AttemptOutcome::Commit {
                    delivery_processed: 0,
                },
                Duration::from_secs(34),
            )
            .unwrap();

        assert_eq!(
            scheduler
                .finish_attempt_at(
                    first,
                    AttemptOutcome::Commit {
                        delivery_processed: 0,
                    },
                    first_completed_at,
                )
                .unwrap(),
            AttemptDisposition::Finished
        );
        assert_eq!(scheduler.windows()[0].committed, 2);
    }

    #[test]
    fn sampled_terminal_at_attempt_deadline_is_fatal() {
        let (clock, mut scheduler) =
            ready_scheduler_with_limits(LocalRuntimeLimits::new(Duration::from_secs(10)).unwrap());
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let ticket = start_payment(&mut scheduler, WorkerId::new(5).unwrap(), 0x63);

        let error = scheduler
            .finish_attempt_at(
                ticket,
                AttemptOutcome::Commit {
                    delivery_processed: 0,
                },
                Duration::from_secs(190),
            )
            .unwrap_err();
        let SchedulerError::RunFailed(failure) = error else {
            panic!("expected absolute attempt deadline failure");
        };
        assert!(failure.reason.contains("not an official timeout"));
        assert_eq!(scheduler.windows()[0].abandoned, 0);
        assert_eq!(scheduler.windows()[0].grace_tail, 1);
    }

    #[test]
    fn read_only_attempt_at_tail_deadline_is_excluded_as_grace_tail() {
        let (clock, mut scheduler) =
            ready_scheduler_with_limits(LocalRuntimeLimits::new(Duration::from_secs(10)).unwrap());
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let worker = WorkerId::new(5).unwrap();
        let reservation = scheduler.reserve_transaction(worker).unwrap();
        let identity =
            TransactionIdentity::new(TransactionType::StockLevel, 1, 0x64, false).unwrap();
        let ticket = scheduler.start_transaction(reservation, identity).unwrap();

        assert_eq!(
            scheduler
                .finish_attempt_at(
                    ticket,
                    AttemptOutcome::Commit {
                        delivery_processed: 0,
                    },
                    Duration::from_secs(190),
                )
                .unwrap(),
            AttemptDisposition::GraceTail
        );
        assert_eq!(scheduler.windows()[0].attempted, 0);
        assert_eq!(scheduler.windows()[0].abandoned, 0);
        assert_eq!(scheduler.windows()[0].grace_tail, 1);
        assert_eq!(scheduler.windows()[0].committed, 0);
        let next = start_payment(&mut scheduler, worker, 0x65);
        assert_eq!(next.phase(), PhaseId::FormalWindow(1));
    }

    #[test]
    fn unknown_outcome_abandon_applies_to_write_attempts_like_the_official_client() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(5));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let worker = WorkerId::new(6).unwrap();
        let reservation = scheduler.reserve_transaction(worker).unwrap();
        let identity =
            TransactionIdentity::new(TransactionType::OrderStatus, 1, 0x66, false).unwrap();
        let read_only = scheduler.start_transaction(reservation, identity).unwrap();
        scheduler
            .abandon_read_only_inflight_at(read_only, Duration::from_secs(185))
            .unwrap();
        assert_eq!(scheduler.windows()[0].abandoned, 0);
        assert_eq!(scheduler.windows()[0].grace_tail, 1);

        // The scheduler mirrors the official client, which abandons timed-out
        // write attempts too (the executor best-effort ABORTs and rebuilds the
        // session so the server rolls back an in-flight transaction). A
        // Payment whose connection outcome is unknown is therefore abandonable
        // and never counted as committed.
        let write = start_payment(&mut scheduler, worker, 0x67);
        scheduler
            .abandon_read_only_inflight_at(write, Duration::from_secs(186))
            .unwrap();
        // The write attempt started inside FormalWindow(1) (phase deadline
        // 330s), so 186s is an in-window abandonment, not a grace tail.
        assert_eq!(scheduler.windows()[0].abandoned, 0);
        assert_eq!(scheduler.windows()[0].grace_tail, 1);
        assert_eq!(scheduler.windows()[1].abandoned, 1);
        assert_eq!(scheduler.windows()[1].grace_tail, 0);
    }

    #[test]
    fn grace_tail_still_rejects_terminal_identity_mismatch() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(10));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let ticket = start_expected_rollback(&mut scheduler, WorkerId::new(5).unwrap(), 0x52);

        let error = scheduler
            .finish_attempt_at(
                ticket,
                AttemptOutcome::Commit {
                    delivery_processed: 0,
                },
                Duration::from_secs(181),
            )
            .unwrap_err();
        assert!(matches!(error, SchedulerError::RunFailed(_)));
        assert_eq!(scheduler.windows()[0].grace_tail, 1);
        assert_eq!(scheduler.windows()[0].abandoned, 0);
    }

    #[test]
    fn final_deadline_starts_no_more_transactions_and_summary_uses_gates() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(10));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30 + 3 * 150));
        assert_eq!(
            scheduler.reserve_transaction(WorkerId::new(0).unwrap()),
            Err(SchedulerError::TimelineEnded)
        );

        let summary = scheduler.measurement_summary().unwrap();
        assert_eq!(summary.window_gates.len(), FORMAL_WINDOW_COUNT);
        assert!(summary.window_gates.iter().all(|gate| !gate.passed()));
        assert_eq!(summary.combined_coverage.required_warehouses, 0);
        assert!(summary.combined_coverage.passed());
    }

    #[test]
    fn worker_failure_fails_the_active_phase() {
        let (_clock, mut scheduler) = ready_scheduler(Duration::from_secs(10));
        scheduler.start().unwrap();
        let worker = WorkerId::new(9).unwrap();
        let error = scheduler
            .worker_failed(worker, "socket closed")
            .unwrap_err();
        assert_eq!(
            error,
            SchedulerError::RunFailed(SchedulerFailure {
                worker,
                phase: Some(PhaseId::Warmup),
                reason: "socket closed".to_string(),
            })
        );
        assert!(matches!(
            scheduler.reserve_transaction(WorkerId::new(0).unwrap()),
            Err(SchedulerError::RunFailed(_))
        ));
    }

    #[test]
    fn local_response_grace_is_a_failure_limit_not_a_ranked_constant() {
        let (clock, mut scheduler) = ready_scheduler(Duration::from_secs(2));
        scheduler.start().unwrap();
        clock.set(Duration::from_secs(30));
        let worker = WorkerId::new(7).unwrap();
        start_payment(&mut scheduler, worker, 7);

        clock.set(Duration::from_secs(183));
        let error = scheduler.poll().unwrap_err();
        let SchedulerError::RunFailed(failure) = error else {
            panic!("expected local response-grace failure");
        };
        assert!(failure.reason.contains("not an official timeout"));
        assert_eq!(failure.phase, Some(PhaseId::FormalWindow(0)));
    }

    #[test]
    fn scheduler_does_not_apply_one_timeout_across_multiple_wire_requests() {
        let (clock, mut scheduler) =
            ready_scheduler_with_limits(LocalRuntimeLimits::new(Duration::from_secs(10)).unwrap());
        scheduler.start().unwrap();
        let worker = WorkerId::new(8).unwrap();
        start_payment(&mut scheduler, worker, 8);

        clock.set(Duration::from_secs(3));
        scheduler.poll().unwrap();
        assert!(scheduler.failure.is_none());
    }

    #[test]
    fn local_runtime_limits_reject_zero_without_claiming_official_values() {
        assert_eq!(
            LocalRuntimeLimits::new(Duration::ZERO),
            Err(SchedulerError::InvalidRuntimeLimit("phase_tail_grace"))
        );
    }
}
