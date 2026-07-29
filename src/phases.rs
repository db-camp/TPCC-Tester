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
/// Both fields are deliberately named and documented as local safety settings.
/// The public final specification does not publish the grader's socket response
/// deadline or phase-tail grace duration, so neither value may be reported as
/// an official constant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalRuntimeLimits {
    pub response_timeout: Duration,
    pub phase_tail_grace: Duration,
}

impl LocalRuntimeLimits {
    pub fn new(
        response_timeout: Duration,
        phase_tail_grace: Duration,
    ) -> Result<Self, SchedulerError> {
        if response_timeout.is_zero() {
            return Err(SchedulerError::InvalidRuntimeLimit("response_timeout"));
        }
        if phase_tail_grace.is_zero() {
            return Err(SchedulerError::InvalidRuntimeLimit("phase_tail_grace"));
        }
        Ok(Self {
            response_timeout,
            phase_tail_grace,
        })
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
    #[error("the timeline has not reached its final absolute deadline")]
    TimelineIncomplete,
    #[error("at least one worker still has an in-flight transaction")]
    WorkersNotDrained,
    #[error("scheduler failed: {0:?}")]
    RunFailed(SchedulerFailure),
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
        Self {
            clock,
            recorder,
            limits,
            workers: vec![WorkerState::default(); usize::from(OFFICIAL_CLIENTS)],
            sessions: HashSet::with_capacity(usize::from(OFFICIAL_CLIENTS)),
            origin: None,
            announced_phase: None,
            timeline_complete: false,
            last_observed: None,
            next_ticket_id: 0,
            windows: std::array::from_fn(|_| WindowStats::new(hot_warehouses)),
            failure: None,
        }
    }

    pub fn recorder(&self) -> &R {
        &self.recorder
    }

    pub fn windows(&self) -> &[WindowStats; FORMAL_WINDOW_COUNT] {
        &self.windows
    }

    pub fn prepared_session(&self, worker: WorkerId) -> Option<PreparedSessionId> {
        self.workers[usize::from(worker.value())].session
    }

    pub fn ready_workers(&self) -> usize {
        self.sessions.len()
    }

    /// Registers a session only after connection, SI setup, and PREPARE_SET.
    ///
    /// Registration is forbidden after the barrier releases.  Consequently,
    /// the same 32-session pool is retained across every phase.
    pub fn worker_prepared(
        &mut self,
        worker: WorkerId,
        session: PreparedSessionId,
    ) -> Result<(), SchedulerError> {
        self.ensure_not_failed()?;
        if self.origin.is_some() {
            return Err(SchedulerError::AlreadyStarted);
        }
        let now = self.observe_now()?;
        let worker_state = &mut self.workers[usize::from(worker.value())];
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
        if self.ready_workers() != usize::from(OFFICIAL_CLIENTS) {
            return Err(SchedulerError::BarrierIncomplete {
                ready: self.ready_workers(),
                required: usize::from(OFFICIAL_CLIENTS),
            });
        }

        let now = self.observe_now()?;
        let deadline = checked_add(now, WARMUP_DURATION)?;
        self.origin = Some(now);
        self.announced_phase = Some(PhaseId::Warmup);
        self.recorder.record(SchedulerEvent::BarrierReleased {
            workers: OFFICIAL_CLIENTS,
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
        self.sync_to(now)
    }

    /// Reserves exactly one stage-local number for parameter generation.
    pub fn reserve_transaction(
        &mut self,
        worker: WorkerId,
    ) -> Result<TransactionReservation, SchedulerError> {
        let now = self.observe_now()?;
        self.sync_to(now)?;
        self.ensure_running()?;
        let phase = self.announced_phase.ok_or(SchedulerError::TimelineEnded)?;
        let (_, deadline) = self.phase_bounds(phase)?;

        let worker_state = &mut self.workers[usize::from(worker.value())];
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
        let now = self.observe_now()?;
        self.sync_to(now)?;
        self.ensure_running()?;
        if now >= reservation.phase_deadline {
            return Err(SchedulerError::ReservationDeadlinePassed);
        }

        let worker_state = &mut self.workers[usize::from(reservation.worker.value())];
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
        let now = self.observe_now()?;
        self.sync_to(now)?;
        self.ensure_running()?;
        if now >= ticket.phase_deadline() {
            return Err(SchedulerError::RetryDeadlinePassed);
        }

        let worker_state = &mut self.workers[usize::from(ticket.worker().value())];
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

    /// Handles a complete terminal response frame for one physical attempt.
    pub fn finish_attempt(
        &mut self,
        ticket: TransactionTicket,
        outcome: AttemptOutcome,
    ) -> Result<AttemptDisposition, SchedulerError> {
        let now = self.observe_now()?;
        self.sync_to(now)?;
        self.ensure_not_failed()?;

        let worker_index = usize::from(ticket.worker().value());
        match self.workers[worker_index].selection {
            Some(SelectionState::InFlight {
                ticket: current, ..
            }) if current == ticket => {}
            _ => return Err(SchedulerError::TicketMismatch(ticket.worker())),
        }

        if now >= ticket.phase_deadline() {
            self.workers[worker_index].selection = None;
            if let Some(index) = ticket.phase().formal_index() {
                self.windows[index].record_grace_tail();
            }
            self.recorder.record(SchedulerEvent::TransactionFinished {
                ticket,
                class: CompletionClass::GraceTail,
                at: now,
            });
            return Ok(AttemptDisposition::GraceTail);
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
                now,
            );
            return Err(SchedulerError::RunFailed(
                self.failure
                    .clone()
                    .expect("fail_worker_at always stores a failure"),
            ));
        }

        let latency = now.saturating_sub(ticket.selected_at());
        let worker_state = &mut self.workers[worker_index];
        let (class, disposition) = match outcome {
            AttemptOutcome::Commit { delivery_processed } => {
                worker_state.selection = None;
                if let Some(index) = ticket.phase().formal_index() {
                    self.windows[index].record_commit(
                        ticket.identity.transaction_type(),
                        ticket.identity.home_warehouse(),
                        latency,
                        delivery_processed,
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
                    self.windows[index].record_retry_abort();
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
            at: now,
        });
        Ok(disposition)
    }

    /// Abandons a selected transaction after a retry cap or local policy.
    pub fn abandon_retry(&mut self, ticket: TransactionTicket) -> Result<(), SchedulerError> {
        let now = self.observe_now()?;
        self.sync_to(now)?;
        self.ensure_not_failed()?;
        let worker_state = &mut self.workers[usize::from(ticket.worker().value())];
        match worker_state.selection {
            Some(SelectionState::RetryPending(current)) if current == ticket => {
                worker_state.selection = None;
            }
            Some(_) => return Err(SchedulerError::TicketMismatch(ticket.worker())),
            None => return Err(SchedulerError::RetryNotPending(ticket.worker())),
        }
        if let Some(index) = ticket.phase().formal_index() {
            self.windows[index].record_abandoned();
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
        let now = self.observe_now()?;
        if self.origin.is_some() {
            self.sync_to(now)?;
        }
        let phase = self.workers[usize::from(worker.value())]
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

        self.expire_response_grace(now)?;
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
            if let Some(index) = phase.formal_index() {
                self.windows[index].record_abandoned();
            }
            match selection {
                AbandonedSelection::Reservation(reservation) => {
                    self.recorder
                        .record(SchedulerEvent::ReservationAbandoned { reservation, at });
                }
                AbandonedSelection::Retry(ticket) => {
                    self.recorder.record(SchedulerEvent::TransactionFinished {
                        ticket,
                        class: CompletionClass::Abandoned,
                        at,
                    });
                }
            }
        }
    }

    fn expire_response_grace(&mut self, now: Duration) -> Result<(), SchedulerError> {
        for index in 0..self.workers.len() {
            let Some(SelectionState::InFlight {
                ticket,
                attempt_started_at,
            }) = self.workers[index].selection
            else {
                continue;
            };
            let response_deadline = checked_add(attempt_started_at, self.limits.response_timeout)?;
            let grace_deadline =
                checked_add(ticket.phase_deadline(), self.limits.phase_tail_grace)?;
            let local_deadline = response_deadline.min(grace_deadline);
            if now < local_deadline {
                continue;
            }

            let worker =
                WorkerId::new(index as u16).expect("worker vector uses official worker bounds");
            self.fail_worker_at(
                worker,
                Some(ticket.phase()),
                format!(
                    "local response/grace deadline expired at {:?}; this is not an official timeout",
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
                self.windows[index].record_abandoned();
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

    fn phase_bounds(&self, phase: PhaseId) -> Result<(Duration, Duration), SchedulerError> {
        let origin = self.origin.ok_or(SchedulerError::NotStarted)?;
        match phase {
            PhaseId::Warmup => Ok((origin, checked_add(origin, WARMUP_DURATION)?)),
            PhaseId::FormalWindow(index) => {
                let formal_origin = checked_add(origin, WARMUP_DURATION)?;
                let start_offset = FORMAL_WINDOW_DURATION
                    .checked_mul(u32::from(index))
                    .ok_or(SchedulerError::ClockOverflow)?;
                let start = checked_add(formal_origin, start_offset)?;
                Ok((start, checked_add(start, FORMAL_WINDOW_DURATION)?))
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
        ready_scheduler_with_limits(
            LocalRuntimeLimits::new(Duration::from_secs(300), grace).unwrap(),
        )
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

    #[test]
    fn barrier_requires_all_32_prepared_sessions_once() {
        let clock = FakeClock::default();
        let mut scheduler = Final2026Scheduler::new(
            clock,
            Vec::new(),
            LocalRuntimeLimits::new(Duration::from_secs(300), Duration::from_secs(5)).unwrap(),
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
    fn reservation_crossing_a_boundary_is_abandoned_before_any_request() {
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
        assert_eq!(scheduler.windows()[0].abandoned, 1);
        let next = start_payment(&mut scheduler, worker, 61);
        assert_eq!((next.phase(), next.txn_no()), (PhaseId::FormalWindow(1), 0));
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
        assert_eq!(scheduler.windows()[0].committed, 0);
        assert_eq!(scheduler.windows()[0].completed(), 0);

        let next = start_payment(&mut scheduler, worker, 6);
        assert_eq!((next.phase(), next.txn_no()), (PhaseId::FormalWindow(1), 0));
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
    fn local_per_attempt_response_timeout_is_also_enforced() {
        let (clock, mut scheduler) = ready_scheduler_with_limits(
            LocalRuntimeLimits::new(Duration::from_secs(2), Duration::from_secs(10)).unwrap(),
        );
        scheduler.start().unwrap();
        let worker = WorkerId::new(8).unwrap();
        start_payment(&mut scheduler, worker, 8);

        clock.set(Duration::from_secs(3));
        let error = scheduler.poll().unwrap_err();
        let SchedulerError::RunFailed(failure) = error else {
            panic!("expected local per-attempt response-timeout failure");
        };
        assert!(failure.reason.contains("not an official timeout"));
        assert_eq!(failure.phase, Some(PhaseId::Warmup));
    }

    #[test]
    fn local_runtime_limits_reject_zero_without_claiming_official_values() {
        assert_eq!(
            LocalRuntimeLimits::new(Duration::ZERO, Duration::from_secs(1)),
            Err(SchedulerError::InvalidRuntimeLimit("response_timeout"))
        );
        assert_eq!(
            LocalRuntimeLimits::new(Duration::from_secs(1), Duration::ZERO),
            Err(SchedulerError::InvalidRuntimeLimit("phase_tail_grace"))
        );
    }
}
