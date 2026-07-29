//! One shared, fail-closed acknowledgement path for ranked terminals.
//!
//! Every worker offers all evidence domains for one terminal before awaiting
//! either rooted-chain receipt. A worker remains blocked until both the
//! Customer/Stock sample component and the complete Warehouse/District
//! Payment component are connected to their setup roots. This keeps unresolved
//! state bounded by the number of clients and prevents a worker from issuing a
//! successor request before its predecessor evidence is acknowledged.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::{oneshot, watch, Mutex};

use crate::consistency::{CustomerLogicalVersion, CustomerUpdateEvidence, CustomerUpdateKind};
use crate::workload::{TransactionParameters, TransactionTicket};

use super::bounded_stats::{BoundedPhysicalStats, BoundedStatsError};
use super::evidence_collector::{
    CollectorError, CustomerKey, CustomerMutation, IntervalCollector, SealedIntervalEvidence,
    StockKey, StockMutation, StockRootProvider, TerminalEvidence as IntervalTerminalEvidence,
};
use super::ledger::LedgerClass;
use super::payment_endpoints::{
    PaymentAckReceipt, PaymentEndpointCollector, PaymentEndpointError, PaymentFloatEdge,
    PaymentTerminalEvidence, SealedPaymentEvidence,
};
use super::preflight::StalePaymentPreflightProof;
use super::runner::{CustomerVersion, RankedCommit, RankedTransactionOutcome};

pub const TERMINAL_EVIDENCE_POLICY_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WorkerState {
    Idle,
    Waiting,
    Finished,
}

#[derive(Debug)]
struct AckTracker {
    abandoned: AtomicBool,
    abandoned_tx: watch::Sender<bool>,
}

impl AckTracker {
    fn new() -> Self {
        let (abandoned_tx, _) = watch::channel(false);
        Self {
            abandoned: AtomicBool::new(false),
            abandoned_tx,
        }
    }

    fn is_abandoned(&self) -> bool {
        self.abandoned.load(Ordering::Acquire)
    }

    fn abandon(&self) {
        if !self.abandoned.swap(true, Ordering::AcqRel) {
            self.abandoned_tx.send_replace(true);
        }
    }
}

struct IntervalAckReceipt {
    ready: oneshot::Receiver<()>,
    tracker: Arc<AckTracker>,
    abandoned_rx: watch::Receiver<bool>,
}

impl IntervalAckReceipt {
    async fn wait(mut self) -> Result<(), TerminalEvidenceError> {
        if self.tracker.is_abandoned() || *self.abandoned_rx.borrow() {
            return Err(TerminalEvidenceError::Poisoned {
                cause: "terminal ACK gate was abandoned".to_owned(),
            });
        }
        tokio::select! {
            result = &mut self.ready => {
                if result.is_err() || self.tracker.is_abandoned() {
                    Err(TerminalEvidenceError::Poisoned {
                        cause: "terminal ACK gate was abandoned".to_owned(),
                    })
                } else {
                    Ok(())
                }
            }
            _ = self.abandoned_rx.changed() => {
                Err(TerminalEvidenceError::Poisoned {
                    cause: "terminal ACK gate was abandoned".to_owned(),
                })
            }
        }
    }
}

struct TerminalCallGuard {
    tracker: Arc<AckTracker>,
    completed: bool,
}

impl TerminalCallGuard {
    fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for TerminalCallGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.tracker.abandon();
        }
    }
}

struct CollectorState {
    stats: BoundedPhysicalStats,
    intervals: Option<IntervalCollector>,
    workers: Box<[WorkerState]>,
    interval_waiters: Vec<oneshot::Sender<()>>,
    poisoned: Option<String>,
    sealed: bool,
}

enum PreparedIntervals {
    Empty,
    Customers(Vec<CustomerMutation>),
    Stocks(Vec<StockMutation>),
}

impl PreparedIntervals {
    fn offer(&self, collector: &mut IntervalCollector) -> Result<(), CollectorError> {
        match self {
            Self::Empty => collector.record_terminal(IntervalTerminalEvidence::empty()),
            Self::Customers(updates) => {
                collector.record_terminal(IntervalTerminalEvidence::customers(updates))
            }
            Self::Stocks(updates) => {
                collector.record_terminal(IntervalTerminalEvidence::stocks(updates))
            }
        }
    }
}

struct PreparedTerminal {
    intervals: PreparedIntervals,
    payment: Option<PaymentTerminalEvidence>,
}

struct Accounting<'a> {
    class: LedgerClass,
    ticket: &'a TransactionTicket,
    outcome: &'a RankedTransactionOutcome,
}

struct RegisteredCall {
    interval_receipt: IntervalAckReceipt,
    payment_receipt: Option<PaymentAckReceipt>,
    guard: TerminalCallGuard,
}

/// Bounded evidence accumulated by exactly one shared terminal ACK path.
///
/// The type intentionally cannot be cloned. Workers share it through one
/// `Arc<TerminalEvidenceCollector>`.
pub struct TerminalEvidenceCollector {
    clients: usize,
    tracker: Arc<AckTracker>,
    payment: PaymentEndpointCollector,
    state: Mutex<CollectorState>,
}

impl TerminalEvidenceCollector {
    /// Construct the shared gate only after the controlled stale-Writer
    /// Payment preflight has succeeded.
    ///
    pub fn new<P>(
        warehouses: u16,
        clients: u16,
        sample_seed: u64,
        stock_roots: P,
        stale_payment_preflight: StalePaymentPreflightProof,
    ) -> Result<Self, TerminalEvidenceError>
    where
        P: StockRootProvider + 'static,
    {
        if !stale_payment_preflight.matches(sample_seed, warehouses) {
            return Err(TerminalEvidenceError::StalePaymentPreflightBinding);
        }
        let intervals = IntervalCollector::new(warehouses, clients, sample_seed, stock_roots)?;
        let payment = PaymentEndpointCollector::new(warehouses, clients)?;
        let clients = usize::from(clients);
        Ok(Self {
            clients,
            tracker: Arc::new(AckTracker::new()),
            payment,
            state: Mutex::new(CollectorState {
                stats: BoundedPhysicalStats::default(),
                intervals: Some(intervals),
                workers: vec![WorkerState::Idle; clients].into_boxed_slice(),
                interval_waiters: Vec::with_capacity(clients),
                poisoned: None,
                sealed: false,
            }),
        })
    }

    /// Offer every evidence domain, then await both rooted-chain receipts.
    ///
    /// A successful return is the terminal ACK. The caller must not issue the
    /// worker's next database request before this future returns `Ok(())`.
    pub async fn record_terminal(
        &self,
        worker_id: u16,
        class: LedgerClass,
        ticket: &TransactionTicket,
        outcome: &RankedTransactionOutcome,
    ) -> Result<(), TerminalEvidenceError> {
        let prepared = match prepare_terminal(ticket, outcome) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.poison_with(error.to_string()).await;
                return Err(error);
            }
        };
        self.record_prepared(
            worker_id,
            Some(Accounting {
                class,
                ticket,
                outcome,
            }),
            prepared,
        )
        .await
    }

    async fn record_prepared(
        &self,
        worker_id: u16,
        accounting: Option<Accounting<'_>>,
        prepared: PreparedTerminal,
    ) -> Result<(), TerminalEvidenceError> {
        let registered = match self
            .register_terminal(worker_id, accounting, prepared)
            .await
        {
            Ok(registered) => registered,
            Err(error) => {
                self.poison_with(error.to_string()).await;
                return Err(error);
            }
        };
        let RegisteredCall {
            interval_receipt,
            payment_receipt,
            mut guard,
        } = registered;

        if let Err(error) = tokio::try_join!(
            interval_receipt.wait(),
            wait_for_payment(payment_receipt, Arc::clone(&self.tracker))
        ) {
            self.poison_with(error.to_string()).await;
            return Err(error);
        }

        let mut state = self.state.lock().await;
        if self.tracker.is_abandoned() {
            let cause = state
                .poisoned
                .clone()
                .unwrap_or_else(|| "terminal ACK gate was abandoned".to_owned());
            drop(state);
            self.poison_with(cause.clone()).await;
            return Err(TerminalEvidenceError::Poisoned { cause });
        }
        if let Some(cause) = &state.poisoned {
            return Err(TerminalEvidenceError::Poisoned {
                cause: cause.clone(),
            });
        }
        let worker = state.workers.get_mut(usize::from(worker_id)).ok_or(
            TerminalEvidenceError::InvalidWorker {
                worker_id,
                clients: self.clients,
            },
        )?;
        if *worker != WorkerState::Waiting {
            let error = TerminalEvidenceError::WorkerState {
                worker_id,
                expected: "waiting",
            };
            drop(state);
            self.poison_with(error.to_string()).await;
            return Err(error);
        }
        *worker = WorkerState::Idle;
        guard.complete();
        Ok(())
    }

    async fn register_terminal(
        &self,
        worker_id: u16,
        accounting: Option<Accounting<'_>>,
        prepared: PreparedTerminal,
    ) -> Result<RegisteredCall, TerminalEvidenceError> {
        let result = self
            .try_register_terminal(worker_id, accounting, prepared)
            .await;
        if result.is_err() {
            self.tracker.abandon();
        }
        result
    }

    async fn try_register_terminal(
        &self,
        worker_id: u16,
        accounting: Option<Accounting<'_>>,
        prepared: PreparedTerminal,
    ) -> Result<RegisteredCall, TerminalEvidenceError> {
        if self.tracker.is_abandoned() {
            return Err(TerminalEvidenceError::Poisoned {
                cause: "terminal ACK gate was abandoned".to_owned(),
            });
        }

        let mut state = self.state.lock().await;
        if let Some(cause) = &state.poisoned {
            return Err(TerminalEvidenceError::Poisoned {
                cause: cause.clone(),
            });
        }
        if state.sealed {
            return Err(TerminalEvidenceError::AlreadySealed);
        }
        let worker_index = usize::from(worker_id);
        let worker_state = state.workers.get(worker_index).copied().ok_or(
            TerminalEvidenceError::InvalidWorker {
                worker_id,
                clients: self.clients,
            },
        )?;
        if worker_state != WorkerState::Idle {
            return Err(TerminalEvidenceError::WorkerState {
                worker_id,
                expected: "idle",
            });
        }

        let mut next_stats = state.stats.clone();
        if let Some(accounting) = accounting {
            next_stats.offer_terminal(accounting.class, accounting.ticket, accounting.outcome)?;
        }
        let intervals = state
            .intervals
            .as_mut()
            .ok_or(TerminalEvidenceError::AlreadySealed)?;
        prepared.intervals.offer(intervals)?;

        // Payment offer is deliberately after every other fallible mapping and
        // interval offer, but before either receipt is awaited.
        let payment_receipt = match prepared.payment {
            Some(payment) => Some(self.payment.offer_terminal(payment).await?),
            None => None,
        };
        state.stats = next_stats;
        state.workers[worker_index] = WorkerState::Waiting;

        let (ready_tx, ready_rx) = oneshot::channel();
        let interval_pending = state
            .intervals
            .as_ref()
            .expect("unsealed collector retains intervals")
            .storage()
            .pending_intervals();
        if interval_pending == 0 {
            for waiter in state.interval_waiters.drain(..) {
                let _ = waiter.send(());
            }
            let _ = ready_tx.send(());
        } else {
            state.interval_waiters.push(ready_tx);
        }

        if interval_pending != 0
            && !state
                .workers
                .iter()
                .any(|worker| *worker == WorkerState::Idle)
        {
            return Err(TerminalEvidenceError::NoPotentialBridge {
                pending: interval_pending,
            });
        }
        if state.interval_waiters.len() > self.clients {
            return Err(TerminalEvidenceError::UnacknowledgedLimit {
                actual: state.interval_waiters.len(),
                limit: self.clients,
            });
        }
        if self.tracker.is_abandoned() {
            return Err(TerminalEvidenceError::Poisoned {
                cause: "terminal ACK gate was abandoned during registration".to_owned(),
            });
        }

        Ok(RegisteredCall {
            interval_receipt: IntervalAckReceipt {
                ready: ready_rx,
                tracker: Arc::clone(&self.tracker),
                abandoned_rx: self.tracker.abandoned_tx.subscribe(),
            },
            payment_receipt,
            guard: TerminalCallGuard {
                tracker: Arc::clone(&self.tracker),
                completed: false,
            },
        })
    }

    /// Mark a worker permanently unable to supply another bridge terminal.
    pub async fn worker_finished(&self, worker_id: u16) -> Result<(), TerminalEvidenceError> {
        let result = self.try_finish_worker(worker_id).await;
        if let Err(error) = result {
            self.poison_with(error.to_string()).await;
            return Err(error);
        }
        Ok(())
    }

    async fn try_finish_worker(&self, worker_id: u16) -> Result<(), TerminalEvidenceError> {
        if self.tracker.is_abandoned() {
            return Err(TerminalEvidenceError::Poisoned {
                cause: "terminal ACK gate was abandoned".to_owned(),
            });
        }
        let mut state = self.state.lock().await;
        if let Some(cause) = &state.poisoned {
            return Err(TerminalEvidenceError::Poisoned {
                cause: cause.clone(),
            });
        }
        let worker = state.workers.get_mut(usize::from(worker_id)).ok_or(
            TerminalEvidenceError::InvalidWorker {
                worker_id,
                clients: self.clients,
            },
        )?;
        if *worker != WorkerState::Idle {
            return Err(TerminalEvidenceError::WorkerState {
                worker_id,
                expected: "idle before finish",
            });
        }
        *worker = WorkerState::Finished;

        let interval_pending = state
            .intervals
            .as_ref()
            .ok_or(TerminalEvidenceError::AlreadySealed)?
            .storage()
            .pending_intervals();
        let payment_pending = self.payment.storage().await.pending_edges;
        let pending = interval_pending.checked_add(payment_pending).ok_or(
            TerminalEvidenceError::CounterOverflow("pending terminal evidence"),
        )?;
        if pending != 0
            && !state
                .workers
                .iter()
                .any(|worker| *worker == WorkerState::Idle)
        {
            return Err(TerminalEvidenceError::NoPotentialBridge { pending });
        }
        Ok(())
    }

    /// Poison all domains after a worker, transport, or executor failure.
    pub async fn poison(&self, cause: impl Into<String>) {
        self.poison_with(cause.into()).await;
    }

    async fn poison_with(&self, cause: String) {
        self.tracker.abandon();
        let waiters = {
            let mut state = self.state.lock().await;
            if state.poisoned.is_none() {
                state.poisoned = Some(cause);
            }
            state.interval_waiters.drain(..).collect::<Vec<_>>()
        };
        drop(waiters);
        self.payment.poison().await;
    }

    pub async fn storage(&self) -> TerminalCollectorStorage {
        let state = self.state.lock().await;
        let mut idle_workers = 0;
        let mut waiting_workers = 0;
        let mut finished_workers = 0;
        for worker in &state.workers {
            match worker {
                WorkerState::Idle => idle_workers += 1,
                WorkerState::Waiting => waiting_workers += 1,
                WorkerState::Finished => finished_workers += 1,
            }
        }
        let interval_pending = state
            .intervals
            .as_ref()
            .map_or(0, |collector| collector.storage().pending_intervals());
        let payment = self.payment.storage().await;
        TerminalCollectorStorage {
            clients: self.clients,
            idle_workers,
            waiting_workers,
            finished_workers,
            interval_pending,
            interval_waiters: state.interval_waiters.len(),
            payment_pending_edges: payment.pending_edges,
            payment_unacknowledged: payment.unacknowledged_terminals,
            poisoned: state.poisoned.is_some() || self.tracker.is_abandoned(),
        }
    }

    pub async fn seal(&self) -> Result<SealedTerminalEvidence, TerminalEvidenceError> {
        let result = self.try_seal().await;
        if let Err(error) = &result {
            self.poison_with(error.to_string()).await;
        }
        result
    }

    async fn try_seal(&self) -> Result<SealedTerminalEvidence, TerminalEvidenceError> {
        if self.tracker.is_abandoned() {
            return Err(TerminalEvidenceError::Poisoned {
                cause: "terminal ACK gate was abandoned".to_owned(),
            });
        }
        let (stats, intervals) = {
            let mut state = self.state.lock().await;
            if let Some(cause) = &state.poisoned {
                return Err(TerminalEvidenceError::Poisoned {
                    cause: cause.clone(),
                });
            }
            if state.sealed {
                return Err(TerminalEvidenceError::AlreadySealed);
            }
            if state
                .workers
                .iter()
                .any(|worker| *worker != WorkerState::Finished)
            {
                return Err(TerminalEvidenceError::WorkersNotFinished);
            }
            if !state.interval_waiters.is_empty() {
                return Err(TerminalEvidenceError::UnacknowledgedLimit {
                    actual: state.interval_waiters.len(),
                    limit: 0,
                });
            }
            state.stats.validate()?;
            let stats = std::mem::take(&mut state.stats);
            let intervals = state
                .intervals
                .take()
                .ok_or(TerminalEvidenceError::AlreadySealed)?;
            state.sealed = true;
            (stats, intervals)
        };

        let intervals = intervals.seal()?;
        let payment = self.payment.seal().await?;
        let totals = stats.totals()?;
        let expected_customer_updates = totals
            .payment_commits
            .checked_add(totals.delivered_orders)
            .ok_or(TerminalEvidenceError::CounterOverflow(
                "customer update count",
            ))?;
        if intervals.customer_update_count() != expected_customer_updates {
            return Err(TerminalEvidenceError::CrossInvariant(
                "Customer edge count differs from Payment commits plus delivered orders",
            ));
        }
        if intervals.stock_update_count() != totals.new_order_lines {
            return Err(TerminalEvidenceError::CrossInvariant(
                "Stock edge count differs from committed NewOrder lines",
            ));
        }
        if payment.terminal_count() != totals.payment_commits
            || payment.warehouse_edge_count() != totals.payment_commits
            || payment.district_edge_count() != totals.payment_commits
        {
            return Err(TerminalEvidenceError::CrossInvariant(
                "complete Payment endpoint counts differ from Payment commits",
            ));
        }
        Ok(SealedTerminalEvidence {
            policy_version: TERMINAL_EVIDENCE_POLICY_VERSION,
            stats,
            intervals,
            payment,
        })
    }

    #[cfg(test)]
    async fn record_prepared_without_stats(
        &self,
        worker_id: u16,
        intervals: PreparedIntervals,
        payment: Option<PaymentTerminalEvidence>,
    ) -> Result<(), TerminalEvidenceError> {
        self.record_prepared(worker_id, None, PreparedTerminal { intervals, payment })
            .await
    }
}

async fn wait_for_payment(
    receipt: Option<PaymentAckReceipt>,
    tracker: Arc<AckTracker>,
) -> Result<(), TerminalEvidenceError> {
    match receipt {
        Some(receipt) => {
            let mut abandoned = tracker.abandoned_tx.subscribe();
            if tracker.is_abandoned() || *abandoned.borrow() {
                return Err(TerminalEvidenceError::Poisoned {
                    cause: "terminal ACK gate was abandoned".to_owned(),
                });
            }
            tokio::select! {
                result = receipt.wait() => result.map_err(TerminalEvidenceError::Payment),
                _ = abandoned.changed() => Err(TerminalEvidenceError::Poisoned {
                    cause: "terminal ACK gate was abandoned".to_owned(),
                }),
            }
        }
        None => Ok(()),
    }
}

/// Exact bounded state exposed for executor assertions and adversarial tests.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TerminalCollectorStorage {
    pub clients: usize,
    pub idle_workers: usize,
    pub waiting_workers: usize,
    pub finished_workers: usize,
    pub interval_pending: usize,
    pub interval_waiters: usize,
    pub payment_pending_edges: usize,
    pub payment_unacknowledged: usize,
    pub poisoned: bool,
}

/// Sealed bounded evidence; fields are private and the type is not clonable.
pub struct SealedTerminalEvidence {
    policy_version: u32,
    stats: BoundedPhysicalStats,
    intervals: SealedIntervalEvidence,
    payment: SealedPaymentEvidence,
}

impl SealedTerminalEvidence {
    pub fn policy_version(&self) -> u32 {
        self.policy_version
    }

    pub fn stats(&self) -> &BoundedPhysicalStats {
        &self.stats
    }

    pub fn intervals(&self) -> &SealedIntervalEvidence {
        &self.intervals
    }

    pub fn payment(&self) -> &SealedPaymentEvidence {
        &self.payment
    }
}

#[derive(Debug, Error)]
pub enum TerminalEvidenceError {
    #[error("bounded terminal statistics rejected evidence: {0}")]
    Stats(#[from] BoundedStatsError),
    #[error("bounded interval collector rejected evidence: {0}")]
    Intervals(#[from] CollectorError),
    #[error("bounded Payment endpoint collector rejected evidence: {0}")]
    Payment(#[from] PaymentEndpointError),
    #[error("invalid terminal evidence mapping: {0}")]
    InvalidMapping(&'static str),
    #[error("terminal evidence collector is poisoned by an earlier rejection: {cause}")]
    Poisoned { cause: String },
    #[error("terminal evidence counter overflow: {0}")]
    CounterOverflow(&'static str),
    #[error("sealed terminal evidence violates a cross-component invariant: {0}")]
    CrossInvariant(&'static str),
    #[error("worker {worker_id} is outside the configured {clients}-worker domain")]
    InvalidWorker { worker_id: u16, clients: usize },
    #[error("worker {worker_id} is not {expected}")]
    WorkerState {
        worker_id: u16,
        expected: &'static str,
    },
    #[error("no worker can supply a bridge for {pending} disconnected sampled interval(s)")]
    NoPotentialBridge { pending: usize },
    #[error("terminal collector has {actual} unacknowledged calls; limit is {limit}")]
    UnacknowledgedLimit { actual: usize, limit: usize },
    #[error("all workers must finish before terminal evidence can be sealed")]
    WorkersNotFinished,
    #[error("terminal evidence collector is already sealed")]
    AlreadySealed,
    #[error("controlled stale-Writer Payment preflight proof has the wrong run binding")]
    StalePaymentPreflightBinding,
}

fn prepare_terminal(
    ticket: &TransactionTicket,
    outcome: &RankedTransactionOutcome,
) -> Result<PreparedTerminal, TerminalEvidenceError> {
    match outcome {
        RankedTransactionOutcome::ExpectedRollback
        | RankedTransactionOutcome::Committed(RankedCommit::OrderStatus)
        | RankedTransactionOutcome::Committed(RankedCommit::StockLevel { .. }) => {
            Ok(PreparedTerminal {
                intervals: PreparedIntervals::Empty,
                payment: None,
            })
        }
        RankedTransactionOutcome::Committed(RankedCommit::Payment(payment)) => {
            let mutation = CustomerMutation::new(
                CustomerKey {
                    warehouse_id: i32::from(payment.customer_warehouse_id),
                    district_id: i32::from(payment.customer_district_id),
                    customer_id: payment.customer_id,
                },
                CustomerUpdateEvidence {
                    kind: CustomerUpdateKind::Payment,
                    before_version: customer_version(payment.customer_version_before),
                    after_version: customer_version(payment.customer_version_after),
                    amount_bits: payment.amount_bits,
                    balance_before_bits: payment.customer_balance_before_bits,
                    balance_after_bits: payment.customer_balance_after_bits,
                    ytd_payment_before_bits: Some(payment.customer_ytd_before_bits),
                    ytd_payment_after_bits: Some(payment.customer_ytd_after_bits),
                },
            );
            Ok(PreparedTerminal {
                intervals: PreparedIntervals::Customers(vec![mutation]),
                payment: Some(PaymentTerminalEvidence {
                    warehouse_id: payment.warehouse_id,
                    district_id: payment.district_id,
                    warehouse: PaymentFloatEdge {
                        before_bits: payment.warehouse_before_bits,
                        after_bits: payment.warehouse_after_bits,
                        amount_bits: payment.amount_bits,
                    },
                    district: PaymentFloatEdge {
                        before_bits: payment.district_before_bits,
                        after_bits: payment.district_after_bits,
                        amount_bits: payment.amount_bits,
                    },
                }),
            })
        }
        RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)) => {
            let mutations = orders
                .iter()
                .map(|order| {
                    CustomerMutation::new(
                        CustomerKey {
                            warehouse_id: i32::from(order.warehouse_id),
                            district_id: i32::from(order.district_id),
                            customer_id: order.customer_id,
                        },
                        CustomerUpdateEvidence {
                            kind: CustomerUpdateKind::Delivery,
                            before_version: customer_version(order.customer_version_before),
                            after_version: customer_version(order.customer_version_after),
                            amount_bits: order.amount_bits,
                            balance_before_bits: order.customer_balance_before_bits,
                            balance_after_bits: order.customer_balance_after_bits,
                            ytd_payment_before_bits: None,
                            ytd_payment_after_bits: None,
                        },
                    )
                })
                .collect::<Vec<_>>();
            Ok(PreparedTerminal {
                intervals: PreparedIntervals::Customers(mutations),
                payment: None,
            })
        }
        RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence)) => {
            let TransactionParameters::NewOrder(input) = ticket.parameters() else {
                return Err(TerminalEvidenceError::InvalidMapping(
                    "NewOrder outcome has a non-NewOrder ticket",
                ));
            };
            if usize::from(evidence.line_count) != input.lines().len()
                || evidence.line_amount_bits.len() != input.lines().len()
                || evidence.recovery_lines.len() != input.lines().len()
            {
                return Err(TerminalEvidenceError::InvalidMapping(
                    "NewOrder recovery-line count differs from its frozen ticket",
                ));
            }
            let mut mutations = Vec::with_capacity(evidence.recovery_lines.len());
            for (position, (line, frozen)) in evidence
                .recovery_lines
                .iter()
                .zip(input.lines())
                .enumerate()
            {
                if usize::from(line.number) != position + 1
                    || line.number != frozen.number()
                    || line.item_id != frozen.item_id()
                    || line.supply_warehouse != frozen.supply_warehouse()
                    || line.quantity != frozen.quantity()
                    || evidence.line_amount_bits.get(position) != Some(&line.amount_bits)
                {
                    return Err(TerminalEvidenceError::InvalidMapping(
                        "NewOrder recovery line differs from its frozen input or amount evidence",
                    ));
                }
                mutations.push(StockMutation::new(
                    StockKey {
                        warehouse_id: i32::from(line.supply_warehouse),
                        item_id: i32::try_from(line.item_id).map_err(|_| {
                            TerminalEvidenceError::InvalidMapping(
                                "NewOrder item id does not fit the Stock key domain",
                            )
                        })?,
                    },
                    line.quantity,
                    u8::from(line.supply_warehouse != evidence.warehouse_id),
                    line.stock_before.clone(),
                    line.stock_after.clone(),
                ));
            }
            Ok(PreparedTerminal {
                intervals: PreparedIntervals::Stocks(mutations),
                payment: None,
            })
        }
    }
}

fn customer_version(version: CustomerVersion) -> CustomerLogicalVersion {
    CustomerLogicalVersion {
        payment_count: version.payment_count,
        delivery_count: version.delivery_count,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::profile::TransactionKind;
    use crate::ranking::payment_endpoints::{DISTRICT_YTD_ROOT_BITS, WAREHOUSE_YTD_ROOT_BITS};
    use crate::ranking::runner::{PaymentEvidence, StockVersion};
    use crate::routing::{ClientSequence, OfficialRouter, StageId, WorkloadSeed};
    use crate::workload::{CustomerSelector, Final2026Workload};

    use super::*;

    const TEST_SEED: u64 = 0x1234_5678_9abc_def0;

    fn stock_roots(_: StockKey) -> Option<StockVersion> {
        Some(StockVersion {
            quantity: 50,
            ytd_bits: 0.0_f32.to_bits(),
            order_count: 0,
            remote_count: 0,
        })
    }

    fn collector(clients: u16) -> Arc<TerminalEvidenceCollector> {
        Arc::new(
            TerminalEvidenceCollector::new(
                50,
                clients,
                TEST_SEED,
                stock_roots,
                StalePaymentPreflightProof::verified_for_test(TEST_SEED, 50),
            )
            .unwrap(),
        )
    }

    fn customer_key() -> CustomerKey {
        CustomerKey {
            warehouse_id: 1,
            district_id: 1,
            customer_id: 1,
        }
    }

    fn customer_payment(
        payment_count: i32,
        balance: f32,
        ytd: f32,
        amount: f32,
    ) -> CustomerMutation {
        customer_payment_for(1, payment_count, balance, ytd, amount)
    }

    fn customer_payment_for(
        customer_id: i32,
        payment_count: i32,
        balance: f32,
        ytd: f32,
        amount: f32,
    ) -> CustomerMutation {
        CustomerMutation::new(
            CustomerKey {
                customer_id,
                ..customer_key()
            },
            CustomerUpdateEvidence {
                kind: CustomerUpdateKind::Payment,
                before_version: CustomerLogicalVersion {
                    payment_count,
                    delivery_count: 0,
                },
                after_version: CustomerLogicalVersion {
                    payment_count: payment_count + 1,
                    delivery_count: 0,
                },
                amount_bits: amount.to_bits(),
                balance_before_bits: balance.to_bits(),
                balance_after_bits: (balance - amount).to_bits(),
                ytd_payment_before_bits: Some(ytd.to_bits()),
                ytd_payment_after_bits: Some((ytd + amount).to_bits()),
            },
        )
    }

    fn payment_terminal(before_offset: f32, amount: f32) -> PaymentTerminalEvidence {
        let warehouse_before = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS) + before_offset;
        let district_before = f32::from_bits(DISTRICT_YTD_ROOT_BITS) + before_offset;
        PaymentTerminalEvidence {
            warehouse_id: 1,
            district_id: 1,
            warehouse: PaymentFloatEdge {
                before_bits: warehouse_before.to_bits(),
                after_bits: (warehouse_before + amount).to_bits(),
                amount_bits: amount.to_bits(),
            },
            district: PaymentFloatEdge {
                before_bits: district_before.to_bits(),
                after_bits: (district_before + amount).to_bits(),
                amount_bits: amount.to_bits(),
            },
        }
    }

    async fn wait_for_state(
        collector: &TerminalEvidenceCollector,
        waiting_workers: usize,
        interval_pending: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let storage = collector.storage().await;
                if storage.waiting_workers == waiting_workers
                    && storage.interval_pending == interval_pending
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("collector state did not converge");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn disconnected_terminal_is_not_acked_until_another_worker_bridges_it() {
        let collector = collector(2);
        let future_collector = Arc::clone(&collector);
        let future = tokio::spawn(async move {
            future_collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Customers(vec![customer_payment(2, -11.0, 11.0, 1.0)]),
                    None,
                )
                .await
        });
        wait_for_state(&collector, 1, 1).await;
        assert!(!future.is_finished());

        collector
            .record_prepared_without_stats(
                1,
                PreparedIntervals::Customers(vec![customer_payment(1, -10.0, 10.0, 1.0)]),
                None,
            )
            .await
            .unwrap();
        future.await.unwrap().unwrap();
        let storage = collector.storage().await;
        assert_eq!(storage.waiting_workers, 0);
        assert_eq!(storage.interval_pending, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn all_domains_are_offered_before_either_receipt_is_awaited() {
        let collector = collector(2);
        let first_collector = Arc::clone(&collector);
        let first = tokio::spawn(async move {
            first_collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Customers(vec![customer_payment(1, -10.0, 10.0, 1.0)]),
                    Some(payment_terminal(1.0, 1.0)),
                )
                .await
        });
        wait_for_state(&collector, 1, 0).await;
        assert_eq!(collector.storage().await.payment_pending_edges, 2);
        assert!(!first.is_finished());

        collector
            .record_prepared_without_stats(
                1,
                PreparedIntervals::Customers(vec![customer_payment(2, -11.0, 11.0, 1.0)]),
                Some(payment_terminal(0.0, 1.0)),
            )
            .await
            .unwrap();
        first.await.unwrap().unwrap();
        let storage = collector.storage().await;
        assert_eq!(storage.interval_pending, 0);
        assert_eq!(storage.payment_pending_edges, 0);
        assert_eq!(storage.payment_unacknowledged, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exhausting_all_possible_bridge_workers_poison_wakes_every_waiter() {
        let collector = collector(2);
        let first_collector = Arc::clone(&collector);
        let first = tokio::spawn(async move {
            first_collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Customers(vec![customer_payment(2, -11.0, 11.0, 1.0)]),
                    None,
                )
                .await
        });
        wait_for_state(&collector, 1, 1).await;

        assert!(matches!(
            collector
                .record_prepared_without_stats(1, PreparedIntervals::Empty, None)
                .await,
            Err(TerminalEvidenceError::NoPotentialBridge { pending: 1 })
                | Err(TerminalEvidenceError::Poisoned { .. })
        ));
        assert!(first.await.unwrap().is_err());
        assert!(collector.storage().await.poisoned);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn one_worker_cannot_offer_a_successor_before_its_ack() {
        let collector = collector(2);
        let first_collector = Arc::clone(&collector);
        let first = tokio::spawn(async move {
            first_collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Customers(vec![customer_payment(2, -11.0, 11.0, 1.0)]),
                    None,
                )
                .await
        });
        wait_for_state(&collector, 1, 1).await;

        assert!(matches!(
            collector
                .record_prepared_without_stats(0, PreparedIntervals::Empty, None)
                .await,
            Err(TerminalEvidenceError::WorkerState { worker_id: 0, .. })
                | Err(TerminalEvidenceError::Poisoned { .. })
        ));
        assert!(first.await.unwrap().is_err());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelling_one_unacked_call_immediately_wakes_other_waiters() {
        let collector = collector(3);
        let first_collector = Arc::clone(&collector);
        let first = tokio::spawn(async move {
            first_collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Customers(vec![customer_payment(2, -11.0, 11.0, 1.0)]),
                    None,
                )
                .await
        });
        wait_for_state(&collector, 1, 1).await;

        let second_collector = Arc::clone(&collector);
        let second = tokio::spawn(async move {
            second_collector
                .record_prepared_without_stats(1, PreparedIntervals::Empty, None)
                .await
        });
        wait_for_state(&collector, 2, 1).await;

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let second_result = tokio::time::timeout(Duration::from_secs(1), second)
            .await
            .expect("cancellation must wake every other waiter")
            .unwrap();
        assert!(second_result.is_err());
        assert!(collector.storage().await.poisoned);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn finished_workers_are_not_counted_as_future_bridges() {
        let collector = collector(2);
        collector.worker_finished(1).await.unwrap();
        assert!(matches!(
            collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Customers(vec![customer_payment(2, -11.0, 11.0, 1.0,)]),
                    None,
                )
                .await,
            Err(TerminalEvidenceError::NoPotentialBridge { pending: 1 })
                | Err(TerminalEvidenceError::Poisoned { .. })
        ));
        assert!(collector.storage().await.poisoned);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn last_idle_worker_cannot_finish_while_payment_needs_a_bridge() {
        let collector = collector(2);
        let first_collector = Arc::clone(&collector);
        let first = tokio::spawn(async move {
            first_collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Empty,
                    Some(payment_terminal(1.0, 1.0)),
                )
                .await
        });
        wait_for_state(&collector, 1, 0).await;
        assert_eq!(collector.storage().await.payment_pending_edges, 2);

        collector
            .record_prepared_without_stats(1, PreparedIntervals::Empty, None)
            .await
            .unwrap();
        assert!(matches!(
            collector.worker_finished(1).await,
            Err(TerminalEvidenceError::NoPotentialBridge { pending: 2 })
                | Err(TerminalEvidenceError::Poisoned { .. })
        ));
        assert!(tokio::time::timeout(Duration::from_secs(1), first)
            .await
            .expect("poison must wake the Payment-only waiter")
            .unwrap()
            .is_err());
        assert!(collector.storage().await.poisoned);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelling_interval_waiter_wakes_payment_only_waiter() {
        let collector = collector(3);
        let payment_collector = Arc::clone(&collector);
        let payment_waiter = tokio::spawn(async move {
            payment_collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Empty,
                    Some(payment_terminal(1.0, 1.0)),
                )
                .await
        });
        wait_for_state(&collector, 1, 0).await;

        let interval_collector = Arc::clone(&collector);
        let interval_waiter = tokio::spawn(async move {
            interval_collector
                .record_prepared_without_stats(
                    1,
                    PreparedIntervals::Customers(vec![customer_payment(2, -11.0, 11.0, 1.0)]),
                    None,
                )
                .await
        });
        wait_for_state(&collector, 2, 1).await;

        interval_waiter.abort();
        assert!(interval_waiter.await.unwrap_err().is_cancelled());
        assert!(tokio::time::timeout(Duration::from_secs(1), payment_waiter)
            .await
            .expect("composite cancellation must wake the Payment-only waiter")
            .unwrap()
            .is_err());
        assert!(collector.storage().await.poisoned);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn evicted_pending_customer_waits_in_retired_set_for_its_bridge() {
        let collector = collector(3);
        for customer_id in 1..=64 {
            collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Customers(vec![customer_payment_for(
                        customer_id,
                        1,
                        -10.0,
                        10.0,
                        1.0,
                    )]),
                    None,
                )
                .await
                .unwrap();
        }

        let gap_collector = Arc::clone(&collector);
        let gap = tokio::spawn(async move {
            gap_collector
                .record_prepared_without_stats(
                    0,
                    PreparedIntervals::Customers(vec![customer_payment_for(
                        58, 3, -12.0, 12.0, 1.0,
                    )]),
                    None,
                )
                .await
        });
        wait_for_state(&collector, 1, 1).await;

        let replacement_collector = Arc::clone(&collector);
        let replacement = tokio::spawn(async move {
            replacement_collector
                .record_prepared_without_stats(
                    1,
                    PreparedIntervals::Customers(vec![customer_payment_for(
                        65, 1, -10.0, 10.0, 1.0,
                    )]),
                    None,
                )
                .await
        });
        wait_for_state(&collector, 2, 1).await;
        assert!(!gap.is_finished());
        assert!(!replacement.is_finished());
        let storage = collector.storage().await;
        assert_eq!(storage.interval_pending, 1);

        collector
            .record_prepared_without_stats(
                2,
                PreparedIntervals::Customers(vec![customer_payment_for(58, 2, -11.0, 11.0, 1.0)]),
                None,
            )
            .await
            .unwrap();
        replacement.await.unwrap().unwrap();
        gap.await.unwrap().unwrap();
        assert_eq!(collector.storage().await.interval_pending, 0);
    }

    fn payment_ticket() -> TransactionTicket {
        let router = OfficialRouter::new(WorkloadSeed(TEST_SEED));
        let wheel = router.wheel(StageId::WARMUP);
        let workload = Final2026Workload::new(&router, &wheel);
        let mut sequence = ClientSequence::new(0).unwrap();
        loop {
            let ticket = workload.select(&mut sequence).unwrap();
            if ticket.kind() == TransactionKind::Payment {
                return ticket;
            }
        }
    }

    fn payment_outcome(ticket: &TransactionTicket) -> RankedTransactionOutcome {
        let TransactionParameters::Payment(input) = ticket.parameters() else {
            panic!("Payment ticket");
        };
        let amount = f32::from_bits(input.amount_bits());
        let customer_id = match input.customer() {
            CustomerSelector::Id(customer_id) => i32::from(*customer_id),
            CustomerSelector::LastName(_) => 42,
        };
        let warehouse_before = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let district_before = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        RankedTransactionOutcome::Committed(RankedCommit::Payment(PaymentEvidence {
            warehouse_id: ticket.route().home_warehouse,
            district_id: ticket.route().home_district,
            customer_warehouse_id: input.customer_warehouse(),
            customer_district_id: input.customer_district(),
            customer_id,
            amount_bits: input.amount_bits(),
            warehouse_before_bits: warehouse_before.to_bits(),
            warehouse_after_bits: (warehouse_before + amount).to_bits(),
            district_before_bits: district_before.to_bits(),
            district_after_bits: (district_before + amount).to_bits(),
            customer_balance_before_bits: (-10.0_f32).to_bits(),
            customer_balance_after_bits: (-10.0_f32 - amount).to_bits(),
            customer_ytd_before_bits: 10.0_f32.to_bits(),
            customer_ytd_after_bits: (10.0_f32 + amount).to_bits(),
            customer_version_before: CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            customer_version_after: CustomerVersion {
                payment_count: 2,
                delivery_count: 0,
            },
            history_timestamp: b"2026-07-29 10:20:30".to_vec(),
            history_data: b"W D".to_vec(),
            customer_is_bad_credit: false,
            customer_data_before: Vec::new(),
            customer_data_after: Vec::new(),
        }))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn production_mapping_seals_all_three_bounded_domains() {
        let collector = collector(1);
        let ticket = payment_ticket();
        let outcome = payment_outcome(&ticket);
        collector
            .record_terminal(0, LedgerClass::Warmup, &ticket, &outcome)
            .await
            .unwrap();
        collector.worker_finished(0).await.unwrap();
        let sealed = collector.seal().await.unwrap();
        assert_eq!(sealed.policy_version(), TERMINAL_EVIDENCE_POLICY_VERSION);
        assert_eq!(sealed.stats().totals().unwrap().payment_commits, 1);
        assert_eq!(sealed.intervals().customer_update_count(), 1);
        assert_eq!(sealed.payment().terminal_count(), 1);
    }

    #[test]
    fn stale_payment_preflight_is_bound_to_the_collector_configuration() {
        assert!(matches!(
            TerminalEvidenceCollector::new(
                50,
                1,
                TEST_SEED,
                stock_roots,
                StalePaymentPreflightProof::verified_for_test(TEST_SEED ^ 1, 50),
            ),
            Err(TerminalEvidenceError::StalePaymentPreflightBinding)
        ));
    }
}
