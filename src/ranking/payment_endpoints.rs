//! Exact, bounded Payment endpoint chains for the public Warehouse and
//! District rows.
//!
//! Each successful Payment contributes a paired Warehouse/District terminal.
//! An offer returns a receipt immediately without holding the state lock; the
//! caller gates its response by awaiting that receipt after offering every
//! other domain for the same logical terminal.  That backpressure prevents a
//! client from issuing an unbounded stream of successors while a response gap
//! remains unresolved.  Both row edges advance in one common serial order.
//!
//! Information-theoretic boundary: fixed space cannot recognize a self-loop
//! response that was committed but has not yet been offered to this gate after
//! its predecessor value has left the bounded frontier.  The harness must offer
//! terminal evidence synchronously before that client issues its next request;
//! integration must also run the controlled stale-write Payment preflight
//! before publishing this evidence.  Without that preflight, the remaining
//! invisible stale/self-loop ambiguity is not certified and publication must
//! fail closed.

use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};

use thiserror::Error;
use tokio::sync::{watch, Mutex, Notify};

pub const MAX_PAYMENT_WAREHOUSES: u16 = 50;
pub const DISTRICTS_PER_WAREHOUSE: u8 = 10;
pub const WAREHOUSE_YTD_ROOT_BITS: u32 = 300_000.0_f32.to_bits();
pub const DISTRICT_YTD_ROOT_BITS: u32 = 30_000.0_f32.to_bits();

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentFloatEdge {
    pub before_bits: u32,
    pub after_bits: u32,
    pub amount_bits: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentTerminalEvidence {
    pub warehouse_id: u16,
    pub district_id: u8,
    pub warehouse: PaymentFloatEdge,
    pub district: PaymentFloatEdge,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum PaymentEndpointError {
    #[error("invalid Payment endpoint configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("invalid Payment endpoint key: warehouse {warehouse_id}, district {district_id:?}")]
    InvalidKey {
        warehouse_id: u16,
        district_id: Option<u8>,
    },
    #[error("{domain} {field} is not a finite binary32 value")]
    NonFinite {
        domain: &'static str,
        field: &'static str,
    },
    #[error("{domain} Payment amount must be positive")]
    NonPositiveAmount { domain: &'static str },
    #[error("paired Warehouse and District Payment amounts differ")]
    PairedAmountMismatch,
    #[error("{domain} relative update is not bit-exact binary32 RNE")]
    FloatMismatch { domain: &'static str },
    #[error("{domain} interval starts behind the rooted frontier")]
    StaleInterval { domain: &'static str },
    #[error("{domain} has more than one forward interval from the same predecessor")]
    Fork { domain: &'static str },
    #[error("Payment pending edge limit reached: {actual} >= {limit}")]
    PendingLimit { actual: usize, limit: usize },
    #[error("Payment receipt limit reached: {actual} > {limit}")]
    ReceiptLimit { actual: usize, limit: usize },
    #[error("Payment endpoint counter overflow: {0}")]
    Overflow(&'static str),
    #[error(
        "Payment evidence has no common rooted terminal order ({pending_edges} pending edges)"
    )]
    Disconnected { pending_edges: usize },
    #[error("Payment endpoint collector is poisoned")]
    Poisoned,
    #[error("Payment endpoint collector is already sealed")]
    AlreadySealed,
    #[error("Payment endpoint collector has {actual} unacknowledged terminal receipts")]
    UnacknowledgedReceipts { actual: usize },
    #[error("invalid sealed Payment endpoint invariant: {0}")]
    InvalidInvariant(&'static str),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentCollectorStorage {
    pub warehouse_slots: usize,
    pub district_slots: usize,
    pub pending_edges: usize,
    pub pending_capacity: usize,
    pub terminal_count: u64,
    pub unacknowledged_terminals: usize,
}

#[derive(Clone, Copy, Debug)]
struct EndpointChain {
    root_bits: u32,
    endpoint_bits: u32,
    update_count: u64,
}

impl EndpointChain {
    fn new(root_bits: u32) -> Self {
        Self {
            root_bits,
            endpoint_bits: root_bits,
            update_count: 0,
        }
    }

    fn apply(
        &mut self,
        domain: &'static str,
        edge: ValidatedEdge,
    ) -> Result<(), PaymentEndpointError> {
        if self.endpoint_bits != edge.before_bits {
            return Err(PaymentEndpointError::InvalidInvariant(domain));
        }
        self.update_count = self
            .update_count
            .checked_add(1)
            .ok_or(PaymentEndpointError::Overflow("endpoint update count"))?;
        self.endpoint_bits = edge.after_bits;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct ValidatedEdge {
    before_bits: u32,
    after_bits: u32,
}

impl ValidatedEdge {
    fn is_self_loop(self) -> bool {
        self.before_bits == self.after_bits
    }
}

#[derive(Debug)]
struct TerminalWaiter {
    result: StdMutex<Option<Result<(), PaymentEndpointError>>>,
    ready: Notify,
}

impl TerminalWaiter {
    fn new() -> Self {
        Self {
            result: StdMutex::new(None),
            ready: Notify::new(),
        }
    }

    fn finish(&self, result: Result<(), PaymentEndpointError>) {
        let mut slot = self
            .result
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if slot.is_none() {
            *slot = Some(result);
            drop(slot);
            // Exactly one record call awaits this cell. `notify_one` stores a
            // permit when completion races ahead of `notified().await`.
            self.ready.notify_one();
        }
    }

    async fn wait(&self) -> Result<(), PaymentEndpointError> {
        loop {
            let notified = self.ready.notified();
            if let Some(result) = self
                .result
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .clone()
            {
                return result;
            }
            notified.await;
        }
    }
}

#[derive(Debug)]
struct ReceiptTracker {
    outstanding: AtomicUsize,
    abandoned: AtomicBool,
    abandoned_tx: watch::Sender<bool>,
}

impl ReceiptTracker {
    fn is_abandoned(&self) -> bool {
        self.abandoned.load(AtomicOrdering::Acquire)
    }

    fn abandon(&self) {
        if !self.abandoned.swap(true, AtomicOrdering::AcqRel) {
            self.abandoned_tx.send_replace(true);
        }
    }
}

/// Two-phase acknowledgement returned after a terminal has been offered.
///
/// The composite collector must offer every domain of the same logical
/// terminal before awaiting this receipt.  Dropping or cancelling the receipt
/// marks the gate abandoned; sealing then fails closed.
#[derive(Debug)]
#[must_use = "a Payment terminal is not ACKed until its receipt is awaited"]
pub struct PaymentAckReceipt {
    waiter: Arc<TerminalWaiter>,
    tracker: Arc<ReceiptTracker>,
    abandoned_rx: watch::Receiver<bool>,
    finished: bool,
}

impl PaymentAckReceipt {
    fn gate_is_abandoned(&self) -> bool {
        self.tracker.is_abandoned() || *self.abandoned_rx.borrow()
    }

    pub async fn wait(mut self) -> Result<(), PaymentEndpointError> {
        let result = if self.gate_is_abandoned() {
            Err(PaymentEndpointError::Poisoned)
        } else {
            let waiter = Arc::clone(&self.waiter);
            tokio::select! {
                result = waiter.wait() => {
                    if self.gate_is_abandoned() {
                        Err(PaymentEndpointError::Poisoned)
                    } else {
                        result
                    }
                }
                _ = self.abandoned_rx.changed() => Err(PaymentEndpointError::Poisoned),
            }
        };
        self.tracker
            .outstanding
            .fetch_sub(1, AtomicOrdering::AcqRel);
        self.finished = true;
        result
    }
}

impl Drop for PaymentAckReceipt {
    fn drop(&mut self) {
        if !self.finished {
            self.tracker.abandon();
        }
    }
}

#[derive(Clone, Debug)]
struct BufferedTerminal {
    warehouse_index: usize,
    district_index: usize,
    warehouse: ValidatedEdge,
    district: ValidatedEdge,
    waiter: Arc<TerminalWaiter>,
}

#[derive(Debug)]
struct CollectorState {
    warehouse_chains: Box<[EndpointChain]>,
    district_chains: Box<[EndpointChain]>,
    pending: Vec<BufferedTerminal>,
    terminal_count: u64,
    poisoned: bool,
    sealed: bool,
}

#[derive(Debug)]
struct TerminalPlan {
    warehouse_chains: Box<[EndpointChain]>,
    district_chains: Box<[EndpointChain]>,
    pending: Vec<BufferedTerminal>,
    terminal_count: u64,
    completed: Vec<Arc<TerminalWaiter>>,
}

/// One shared asynchronous gate for all ranked clients.
#[derive(Debug)]
pub struct PaymentEndpointCollector {
    warehouses: u16,
    client_limit: usize,
    receipt_tracker: Arc<ReceiptTracker>,
    state: Mutex<CollectorState>,
}

impl PaymentEndpointCollector {
    pub fn new(warehouses: u16, clients: u16) -> Result<Self, PaymentEndpointError> {
        if warehouses == 0 || warehouses > MAX_PAYMENT_WAREHOUSES {
            return Err(PaymentEndpointError::InvalidConfiguration(
                "warehouse count must be in 1..=50",
            ));
        }
        if clients == 0 {
            return Err(PaymentEndpointError::InvalidConfiguration(
                "client count must be positive",
            ));
        }
        let client_limit = usize::from(clients);
        let district_count = usize::from(warehouses)
            .checked_mul(usize::from(DISTRICTS_PER_WAREHOUSE))
            .ok_or(PaymentEndpointError::Overflow("district slot count"))?;
        let (abandoned_tx, _) = watch::channel(false);
        Ok(Self {
            warehouses,
            client_limit,
            receipt_tracker: Arc::new(ReceiptTracker {
                outstanding: AtomicUsize::new(0),
                abandoned: AtomicBool::new(false),
                abandoned_tx,
            }),
            state: Mutex::new(CollectorState {
                warehouse_chains: vec![
                    EndpointChain::new(WAREHOUSE_YTD_ROOT_BITS);
                    usize::from(warehouses)
                ]
                .into_boxed_slice(),
                district_chains: vec![EndpointChain::new(DISTRICT_YTD_ROOT_BITS); district_count]
                    .into_boxed_slice(),
                pending: Vec::with_capacity(client_limit),
                terminal_count: 0,
                poisoned: false,
                sealed: false,
            }),
        })
    }

    /// Offers one paired terminal without awaiting its chain predecessor.
    ///
    /// This is the first half of the composite terminal protocol.  The caller
    /// must finish offering other domains, then await the returned receipt.
    pub async fn offer_terminal(
        &self,
        terminal: PaymentTerminalEvidence,
    ) -> Result<PaymentAckReceipt, PaymentEndpointError> {
        if self.receipt_tracker.is_abandoned() {
            self.poison().await;
            return Err(PaymentEndpointError::Poisoned);
        }
        let waiter = Arc::new(TerminalWaiter::new());
        let mut completed = Vec::new();
        let mut poisoned_waiters = Vec::new();
        let mut immediate_error = None;

        {
            let mut state = self.state.lock().await;
            if state.poisoned {
                return Err(PaymentEndpointError::Poisoned);
            }
            if state.sealed {
                return Err(PaymentEndpointError::AlreadySealed);
            }
            if self.receipt_tracker.is_abandoned() {
                state.poisoned = true;
                poisoned_waiters = state
                    .pending
                    .drain(..)
                    .map(|terminal| terminal.waiter)
                    .collect();
                immediate_error = Some(PaymentEndpointError::Poisoned);
            } else if self
                .receipt_tracker
                .outstanding
                .load(AtomicOrdering::Acquire)
                >= self.client_limit
            {
                state.poisoned = true;
                poisoned_waiters = state
                    .pending
                    .drain(..)
                    .map(|terminal| terminal.waiter)
                    .collect();
                immediate_error = Some(PaymentEndpointError::ReceiptLimit {
                    actual: self.client_limit + 1,
                    limit: self.client_limit,
                });
            } else {
                match self.prepare_terminal(&state, terminal, Arc::clone(&waiter)) {
                    Ok(plan) => {
                        state.warehouse_chains = plan.warehouse_chains;
                        state.district_chains = plan.district_chains;
                        state.pending = plan.pending;
                        state.terminal_count = plan.terminal_count;
                        completed = plan.completed;
                        // Register the receipt before releasing the state
                        // mutex so concurrent seal cannot miss an offer.
                        self.receipt_tracker
                            .outstanding
                            .fetch_add(1, AtomicOrdering::AcqRel);
                    }
                    Err(error) => {
                        state.poisoned = true;
                        poisoned_waiters = state
                            .pending
                            .drain(..)
                            .map(|terminal| terminal.waiter)
                            .collect();
                        immediate_error = Some(error);
                    }
                }
            }
            if immediate_error.is_some() {
                // Poison is published before releasing the state mutex, so a
                // ready peer receipt cannot ACK after the sticky transition.
                self.receipt_tracker.abandon();
            }
        }

        for completed_waiter in completed {
            completed_waiter.finish(Ok(()));
        }
        for poisoned_waiter in poisoned_waiters {
            poisoned_waiter.finish(Err(PaymentEndpointError::Poisoned));
        }
        if let Some(error) = immediate_error {
            return Err(error);
        }
        Ok(PaymentAckReceipt {
            waiter,
            tracker: Arc::clone(&self.receipt_tracker),
            abandoned_rx: self.receipt_tracker.abandoned_tx.subscribe(),
            finished: false,
        })
    }

    /// Convenience for standalone use. Composite integration must call
    /// `offer_terminal`, offer its other domains, then await the receipt.
    pub async fn record_terminal(
        &self,
        terminal: PaymentTerminalEvidence,
    ) -> Result<(), PaymentEndpointError> {
        self.offer_terminal(terminal).await?.wait().await
    }

    /// Fails the gate and wakes every pending receipt after another composite
    /// domain has failed.
    pub async fn poison(&self) {
        self.receipt_tracker.abandon();
        let waiters = {
            let mut state = self.state.lock().await;
            if state.sealed {
                Vec::new()
            } else {
                state.poisoned = true;
                state
                    .pending
                    .drain(..)
                    .map(|terminal| terminal.waiter)
                    .collect::<Vec<_>>()
            }
        };
        for waiter in waiters {
            waiter.finish(Err(PaymentEndpointError::Poisoned));
        }
    }

    pub async fn storage(&self) -> PaymentCollectorStorage {
        let state = self.state.lock().await;
        PaymentCollectorStorage {
            warehouse_slots: state.warehouse_chains.len(),
            district_slots: state.district_chains.len(),
            pending_edges: state.pending.len() * 2,
            pending_capacity: state.pending.capacity() * 2,
            terminal_count: state.terminal_count,
            unacknowledged_terminals: self
                .receipt_tracker
                .outstanding
                .load(AtomicOrdering::Acquire),
        }
    }

    /// Seals only a quiescent gate.  A pending call means its terminal has not
    /// ACKed and therefore the executor must not begin recovery publication.
    pub async fn seal(&self) -> Result<SealedPaymentEvidence, PaymentEndpointError> {
        let mut state = self.state.lock().await;
        if state.poisoned {
            return Err(PaymentEndpointError::Poisoned);
        }
        if state.sealed {
            return Err(PaymentEndpointError::AlreadySealed);
        }
        let unacknowledged = self
            .receipt_tracker
            .outstanding
            .load(AtomicOrdering::Acquire);
        if self.receipt_tracker.is_abandoned() || unacknowledged != 0 {
            state.poisoned = true;
            self.receipt_tracker.abandon();
            let waiters = state
                .pending
                .drain(..)
                .map(|terminal| terminal.waiter)
                .collect::<Vec<_>>();
            drop(state);
            for waiter in waiters {
                waiter.finish(Err(PaymentEndpointError::Poisoned));
            }
            return Err(PaymentEndpointError::UnacknowledgedReceipts {
                actual: unacknowledged,
            });
        }
        if !state.pending.is_empty() {
            let pending_edges = state.pending.len() * 2;
            state.poisoned = true;
            self.receipt_tracker.abandon();
            let waiters = state
                .pending
                .drain(..)
                .map(|terminal| terminal.waiter)
                .collect::<Vec<_>>();
            drop(state);
            for waiter in waiters {
                waiter.finish(Err(PaymentEndpointError::Poisoned));
            }
            return Err(PaymentEndpointError::Disconnected { pending_edges });
        }

        if let Err(error) = validate_chain_totals(
            self.warehouses,
            state.terminal_count,
            &state.warehouse_chains,
            &state.district_chains,
        ) {
            state.poisoned = true;
            self.receipt_tracker.abandon();
            return Err(error);
        }
        state.sealed = true;
        Ok(SealedPaymentEvidence {
            warehouses: self.warehouses,
            terminal_count: state.terminal_count,
            warehouse_edge_count: state.terminal_count,
            district_edge_count: state.terminal_count,
            warehouse_endpoints: state
                .warehouse_chains
                .iter()
                .copied()
                .map(SealedEndpoint::from_chain)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            district_endpoints: state
                .district_chains
                .iter()
                .copied()
                .map(SealedEndpoint::from_chain)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        })
    }

    fn prepare_terminal(
        &self,
        state: &CollectorState,
        terminal: PaymentTerminalEvidence,
        waiter: Arc<TerminalWaiter>,
    ) -> Result<TerminalPlan, PaymentEndpointError> {
        let warehouse_index = self.warehouse_index(terminal.warehouse_id)?;
        let district_index = self.district_index(terminal.warehouse_id, terminal.district_id)?;
        if terminal.warehouse.amount_bits != terminal.district.amount_bits {
            return Err(PaymentEndpointError::PairedAmountMismatch);
        }
        let warehouse = validate_edge("warehouse.w_ytd", terminal.warehouse)?;
        let district = validate_edge("district.d_ytd", terminal.district)?;
        reject_behind_frontier(
            "warehouse.w_ytd",
            warehouse,
            state.warehouse_chains[warehouse_index],
        )?;
        reject_behind_frontier(
            "district.d_ytd",
            district,
            state.district_chains[district_index],
        )?;

        // The full mutation is scratch until both row chains, counts, and
        // pending bound have succeeded.
        let mut warehouse_chains = state.warehouse_chains.clone();
        let mut district_chains = state.district_chains.clone();
        let mut pending = state.pending.clone();
        pending.push(BufferedTerminal {
            warehouse_index,
            district_index,
            warehouse,
            district,
            waiter,
        });
        validate_visible_forks(&pending)?;

        let mut terminal_count = state.terminal_count;
        let mut completed = Vec::new();
        while apply_one_serializable(
            &mut warehouse_chains,
            &mut district_chains,
            &mut pending,
            &mut terminal_count,
            &mut completed,
        )? {}
        reject_stale_pending(&pending, &warehouse_chains, &district_chains)?;

        // With one outstanding call per client, reaching C unresolved calls
        // means no additional client can supply the missing predecessor.
        if !pending.is_empty() && pending.len() >= self.client_limit {
            return Err(PaymentEndpointError::PendingLimit {
                actual: pending.len() * 2,
                limit: self.client_limit * 2,
            });
        }

        let mut fixed_pending = Vec::with_capacity(self.client_limit);
        fixed_pending.extend(pending);
        Ok(TerminalPlan {
            warehouse_chains,
            district_chains,
            pending: fixed_pending,
            terminal_count,
            completed,
        })
    }

    fn warehouse_index(&self, warehouse_id: u16) -> Result<usize, PaymentEndpointError> {
        if warehouse_id == 0 || warehouse_id > self.warehouses {
            return Err(PaymentEndpointError::InvalidKey {
                warehouse_id,
                district_id: None,
            });
        }
        Ok(usize::from(warehouse_id - 1))
    }

    fn district_index(
        &self,
        warehouse_id: u16,
        district_id: u8,
    ) -> Result<usize, PaymentEndpointError> {
        if warehouse_id == 0
            || warehouse_id > self.warehouses
            || district_id == 0
            || district_id > DISTRICTS_PER_WAREHOUSE
        {
            return Err(PaymentEndpointError::InvalidKey {
                warehouse_id,
                district_id: Some(district_id),
            });
        }
        Ok(
            usize::from(warehouse_id - 1) * usize::from(DISTRICTS_PER_WAREHOUSE)
                + usize::from(district_id - 1),
        )
    }
}

fn reject_stale_pending(
    pending: &[BufferedTerminal],
    warehouse_chains: &[EndpointChain],
    district_chains: &[EndpointChain],
) -> Result<(), PaymentEndpointError> {
    for terminal in pending {
        reject_behind_frontier(
            "warehouse.w_ytd",
            terminal.warehouse,
            warehouse_chains[terminal.warehouse_index],
        )?;
        reject_behind_frontier(
            "district.d_ytd",
            terminal.district,
            district_chains[terminal.district_index],
        )?;
    }
    Ok(())
}

fn validate_edge(
    domain: &'static str,
    edge: PaymentFloatEdge,
) -> Result<ValidatedEdge, PaymentEndpointError> {
    let before = require_finite(domain, "before", edge.before_bits)?;
    require_finite(domain, "after", edge.after_bits)?;
    let amount = require_finite(domain, "amount", edge.amount_bits)?;
    if amount <= 0.0 {
        return Err(PaymentEndpointError::NonPositiveAmount { domain });
    }
    let expected = before + amount;
    if !expected.is_finite() || expected.to_bits() != edge.after_bits {
        return Err(PaymentEndpointError::FloatMismatch { domain });
    }
    Ok(ValidatedEdge {
        before_bits: edge.before_bits,
        after_bits: edge.after_bits,
    })
}

fn require_finite(
    domain: &'static str,
    field: &'static str,
    bits: u32,
) -> Result<f32, PaymentEndpointError> {
    let value = f32::from_bits(bits);
    if !value.is_finite() {
        return Err(PaymentEndpointError::NonFinite { domain, field });
    }
    Ok(value)
}

fn reject_behind_frontier(
    domain: &'static str,
    edge: ValidatedEdge,
    chain: EndpointChain,
) -> Result<(), PaymentEndpointError> {
    if compare_bits(edge.before_bits, chain.endpoint_bits) == Ordering::Less {
        return Err(PaymentEndpointError::StaleInterval { domain });
    }
    Ok(())
}

fn validate_visible_forks(pending: &[BufferedTerminal]) -> Result<(), PaymentEndpointError> {
    for left in 0..pending.len() {
        for right in left + 1..pending.len() {
            let left_terminal = &pending[left];
            let right_terminal = &pending[right];
            if left_terminal.warehouse_index == right_terminal.warehouse_index
                && left_terminal.warehouse.before_bits == right_terminal.warehouse.before_bits
                && !left_terminal.warehouse.is_self_loop()
                && !right_terminal.warehouse.is_self_loop()
            {
                return Err(PaymentEndpointError::Fork {
                    domain: "warehouse.w_ytd",
                });
            }
            if left_terminal.district_index == right_terminal.district_index
                && left_terminal.district.before_bits == right_terminal.district.before_bits
                && !left_terminal.district.is_self_loop()
                && !right_terminal.district.is_self_loop()
            {
                return Err(PaymentEndpointError::Fork {
                    domain: "district.d_ytd",
                });
            }
        }
    }
    Ok(())
}

fn apply_one_serializable(
    warehouse_chains: &mut [EndpointChain],
    district_chains: &mut [EndpointChain],
    pending: &mut Vec<BufferedTerminal>,
    terminal_count: &mut u64,
    completed: &mut Vec<Arc<TerminalWaiter>>,
) -> Result<bool, PaymentEndpointError> {
    validate_visible_forks(pending)?;
    let candidate = pending.iter().position(|terminal| {
        terminal_is_minimal(terminal, warehouse_chains, district_chains, pending)
    });
    let Some(index) = candidate else {
        return Ok(false);
    };
    let terminal = pending.swap_remove(index);
    warehouse_chains[terminal.warehouse_index].apply("warehouse.w_ytd", terminal.warehouse)?;
    district_chains[terminal.district_index].apply("district.d_ytd", terminal.district)?;
    *terminal_count = terminal_count
        .checked_add(1)
        .ok_or(PaymentEndpointError::Overflow("terminal count"))?;
    completed.push(terminal.waiter);
    Ok(true)
}

fn terminal_is_minimal(
    terminal: &BufferedTerminal,
    warehouse_chains: &[EndpointChain],
    district_chains: &[EndpointChain],
    pending: &[BufferedTerminal],
) -> bool {
    let warehouse_endpoint = warehouse_chains[terminal.warehouse_index].endpoint_bits;
    let district_endpoint = district_chains[terminal.district_index].endpoint_bits;
    if terminal.warehouse.before_bits != warehouse_endpoint
        || terminal.district.before_bits != district_endpoint
    {
        return false;
    }
    if !terminal.warehouse.is_self_loop()
        && pending.iter().any(|other| {
            other.warehouse_index == terminal.warehouse_index
                && other.warehouse.before_bits == warehouse_endpoint
                && other.warehouse.is_self_loop()
        })
    {
        return false;
    }
    if !terminal.district.is_self_loop()
        && pending.iter().any(|other| {
            other.district_index == terminal.district_index
                && other.district.before_bits == district_endpoint
                && other.district.is_self_loop()
        })
    {
        return false;
    }
    true
}

fn compare_bits(left: u32, right: u32) -> Ordering {
    f32::from_bits(left).total_cmp(&f32::from_bits(right))
}

fn validate_chain_totals(
    warehouses: u16,
    terminal_count: u64,
    warehouse_chains: &[EndpointChain],
    district_chains: &[EndpointChain],
) -> Result<(), PaymentEndpointError> {
    let expected_districts = usize::from(warehouses)
        .checked_mul(usize::from(DISTRICTS_PER_WAREHOUSE))
        .ok_or(PaymentEndpointError::Overflow("district count"))?;
    if warehouse_chains.len() != usize::from(warehouses)
        || district_chains.len() != expected_districts
    {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint cardinality does not match warehouse count",
        ));
    }
    let mut warehouse_total = 0_u64;
    let mut district_total = 0_u64;
    for (warehouse_index, warehouse) in warehouse_chains.iter().enumerate() {
        validate_sealed_chain(WAREHOUSE_YTD_ROOT_BITS, *warehouse)?;
        warehouse_total = warehouse_total
            .checked_add(warehouse.update_count)
            .ok_or(PaymentEndpointError::Overflow("warehouse update total"))?;
        let start = warehouse_index * usize::from(DISTRICTS_PER_WAREHOUSE);
        let end = start + usize::from(DISTRICTS_PER_WAREHOUSE);
        let district_count =
            district_chains[start..end]
                .iter()
                .try_fold(0_u64, |count, district| {
                    validate_sealed_chain(DISTRICT_YTD_ROOT_BITS, *district)?;
                    count
                        .checked_add(district.update_count)
                        .ok_or(PaymentEndpointError::Overflow("district update total"))
                })?;
        if district_count != warehouse.update_count {
            return Err(PaymentEndpointError::InvalidInvariant(
                "Warehouse count differs from its District counts",
            ));
        }
        district_total = district_total
            .checked_add(district_count)
            .ok_or(PaymentEndpointError::Overflow("district update total"))?;
    }
    if warehouse_total != terminal_count || district_total != terminal_count {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint counts differ from terminal total",
        ));
    }
    Ok(())
}

fn validate_sealed_chain(
    expected_root_bits: u32,
    chain: EndpointChain,
) -> Result<(), PaymentEndpointError> {
    if chain.root_bits != expected_root_bits {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint setup root is not bit-exact",
        ));
    }
    let endpoint = f32::from_bits(chain.endpoint_bits);
    if !endpoint.is_finite()
        || compare_bits(chain.endpoint_bits, expected_root_bits) == Ordering::Less
    {
        return Err(PaymentEndpointError::InvalidInvariant(
            "endpoint is outside the rooted finite domain",
        ));
    }
    if compare_bits(chain.endpoint_bits, expected_root_bits) == Ordering::Greater
        && chain.update_count == 0
    {
        return Err(PaymentEndpointError::InvalidInvariant(
            "empty chain endpoint differs from setup root",
        ));
    }
    Ok(())
}

#[derive(Debug, Eq, PartialEq)]
struct SealedEndpoint {
    root_bits: u32,
    endpoint_bits: u32,
    update_count: u64,
}

impl SealedEndpoint {
    fn from_chain(chain: EndpointChain) -> Self {
        Self {
            root_bits: chain.root_bits,
            endpoint_bits: chain.endpoint_bits,
            update_count: chain.update_count,
        }
    }
}

/// Validated public Payment endpoint certificate.
///
/// Fields are private and the type intentionally implements neither `Clone`
/// nor `Default`.  There is no endpoint-only decoder: endpoints plus counts
/// cannot prove equal paired amounts or one common terminal order.
#[derive(Debug, Eq, PartialEq)]
pub struct SealedPaymentEvidence {
    warehouses: u16,
    terminal_count: u64,
    warehouse_edge_count: u64,
    district_edge_count: u64,
    warehouse_endpoints: Box<[SealedEndpoint]>,
    district_endpoints: Box<[SealedEndpoint]>,
}

impl SealedPaymentEvidence {
    pub fn warehouses(&self) -> u16 {
        self.warehouses
    }

    pub fn terminal_count(&self) -> u64 {
        self.terminal_count
    }

    pub fn warehouse_edge_count(&self) -> u64 {
        self.warehouse_edge_count
    }

    pub fn district_edge_count(&self) -> u64 {
        self.district_edge_count
    }

    pub fn warehouse_endpoint_bits(&self, warehouse_id: u16) -> Option<u32> {
        warehouse_id
            .checked_sub(1)
            .and_then(|index| self.warehouse_endpoints.get(usize::from(index)))
            .map(|endpoint| endpoint.endpoint_bits)
    }

    pub fn warehouse_update_count(&self, warehouse_id: u16) -> Option<u64> {
        warehouse_id
            .checked_sub(1)
            .and_then(|index| self.warehouse_endpoints.get(usize::from(index)))
            .map(|endpoint| endpoint.update_count)
    }

    pub fn district_endpoint_bits(&self, warehouse_id: u16, district_id: u8) -> Option<u32> {
        self.district_index(warehouse_id, district_id)
            .and_then(|index| self.district_endpoints.get(index))
            .map(|endpoint| endpoint.endpoint_bits)
    }

    pub fn district_update_count(&self, warehouse_id: u16, district_id: u8) -> Option<u64> {
        self.district_index(warehouse_id, district_id)
            .and_then(|index| self.district_endpoints.get(index))
            .map(|endpoint| endpoint.update_count)
    }

    fn district_index(&self, warehouse_id: u16, district_id: u8) -> Option<usize> {
        if warehouse_id == 0
            || warehouse_id > self.warehouses
            || district_id == 0
            || district_id > DISTRICTS_PER_WAREHOUSE
        {
            return None;
        }
        Some(
            usize::from(warehouse_id - 1) * usize::from(DISTRICTS_PER_WAREHOUSE)
                + usize::from(district_id - 1),
        )
    }
}

/// Read-only Warehouse/District endpoint surface shared by live and restored
/// terminal evidence.
///
/// This trait exposes only the recovery oracle. It deliberately does not claim
/// that a restored endpoint set can reproduce the live paired-edge ordering
/// proof held by [`SealedPaymentEvidence`].
pub trait PaymentEndpointView {
    fn warehouses(&self) -> u16;
    fn terminal_count(&self) -> u64;
    fn warehouse_edge_count(&self) -> u64;
    fn district_edge_count(&self) -> u64;
    fn warehouse_endpoint_bits(&self, warehouse_id: u16) -> Option<u32>;
    fn warehouse_update_count(&self, warehouse_id: u16) -> Option<u64>;
    fn district_endpoint_bits(&self, warehouse_id: u16, district_id: u8) -> Option<u32>;
    fn district_update_count(&self, warehouse_id: u16, district_id: u8) -> Option<u64>;
}

impl PaymentEndpointView for SealedPaymentEvidence {
    fn warehouses(&self) -> u16 {
        self.warehouses()
    }

    fn terminal_count(&self) -> u64 {
        self.terminal_count()
    }

    fn warehouse_edge_count(&self) -> u64 {
        self.warehouse_edge_count()
    }

    fn district_edge_count(&self) -> u64 {
        self.district_edge_count()
    }

    fn warehouse_endpoint_bits(&self, warehouse_id: u16) -> Option<u32> {
        self.warehouse_endpoint_bits(warehouse_id)
    }

    fn warehouse_update_count(&self, warehouse_id: u16) -> Option<u64> {
        self.warehouse_update_count(warehouse_id)
    }

    fn district_endpoint_bits(&self, warehouse_id: u16, district_id: u8) -> Option<u32> {
        self.district_endpoint_bits(warehouse_id, district_id)
    }

    fn district_update_count(&self, warehouse_id: u16, district_id: u8) -> Option<u64> {
        self.district_update_count(warehouse_id, district_id)
    }
}

/// Structurally validated endpoint oracle restored from the checksum-bound
/// terminal artifact.
///
/// This remains a distinct type: endpoints and counts alone cannot recreate
/// the paired Warehouse/District amounts or their one common live order.
#[derive(Debug, Eq, PartialEq)]
pub struct PersistedPaymentEndpoints {
    warehouses: u16,
    terminal_count: u64,
    warehouse_edge_count: u64,
    district_edge_count: u64,
    warehouse_endpoints: Box<[(u32, u64)]>,
    district_endpoints: Box<[(u32, u64)]>,
}

impl PersistedPaymentEndpoints {
    pub(crate) fn from_canonical_endpoints(
        warehouses: u16,
        terminal_count: u64,
        warehouse_edge_count: u64,
        district_edge_count: u64,
        warehouse_endpoints: Vec<(u32, u64)>,
        district_endpoints: Vec<(u32, u64)>,
    ) -> Result<Self, PaymentEndpointError> {
        if warehouses == 0 || warehouses > MAX_PAYMENT_WAREHOUSES {
            return Err(PaymentEndpointError::InvalidConfiguration(
                "warehouses must be in 1..=50",
            ));
        }
        if warehouse_edge_count != terminal_count || district_edge_count != terminal_count {
            return Err(PaymentEndpointError::InvalidInvariant(
                "persisted edge totals differ from terminal total",
            ));
        }
        let warehouse_chains = warehouse_endpoints
            .iter()
            .map(|(endpoint_bits, update_count)| EndpointChain {
                root_bits: WAREHOUSE_YTD_ROOT_BITS,
                endpoint_bits: *endpoint_bits,
                update_count: *update_count,
            })
            .collect::<Vec<_>>();
        let district_chains = district_endpoints
            .iter()
            .map(|(endpoint_bits, update_count)| EndpointChain {
                root_bits: DISTRICT_YTD_ROOT_BITS,
                endpoint_bits: *endpoint_bits,
                update_count: *update_count,
            })
            .collect::<Vec<_>>();
        validate_chain_totals(
            warehouses,
            terminal_count,
            &warehouse_chains,
            &district_chains,
        )?;
        Ok(Self {
            warehouses,
            terminal_count,
            warehouse_edge_count,
            district_edge_count,
            warehouse_endpoints: warehouse_endpoints.into_boxed_slice(),
            district_endpoints: district_endpoints.into_boxed_slice(),
        })
    }
}

impl PaymentEndpointView for PersistedPaymentEndpoints {
    fn warehouses(&self) -> u16 {
        self.warehouses
    }

    fn terminal_count(&self) -> u64 {
        self.terminal_count
    }

    fn warehouse_edge_count(&self) -> u64 {
        self.warehouse_edge_count
    }

    fn district_edge_count(&self) -> u64 {
        self.district_edge_count
    }

    fn warehouse_endpoint_bits(&self, warehouse_id: u16) -> Option<u32> {
        warehouse_id
            .checked_sub(1)
            .and_then(|index| self.warehouse_endpoints.get(usize::from(index)))
            .map(|(bits, _)| *bits)
    }

    fn warehouse_update_count(&self, warehouse_id: u16) -> Option<u64> {
        warehouse_id
            .checked_sub(1)
            .and_then(|index| self.warehouse_endpoints.get(usize::from(index)))
            .map(|(_, count)| *count)
    }

    fn district_endpoint_bits(&self, warehouse_id: u16, district_id: u8) -> Option<u32> {
        district_index(self.warehouses, warehouse_id, district_id)
            .and_then(|index| self.district_endpoints.get(index))
            .map(|(bits, _)| *bits)
    }

    fn district_update_count(&self, warehouse_id: u16, district_id: u8) -> Option<u64> {
        district_index(self.warehouses, warehouse_id, district_id)
            .and_then(|index| self.district_endpoints.get(index))
            .map(|(_, count)| *count)
    }
}

fn district_index(warehouses: u16, warehouse_id: u16, district_id: u8) -> Option<usize> {
    if warehouse_id == 0
        || warehouse_id > warehouses
        || district_id == 0
        || district_id > DISTRICTS_PER_WAREHOUSE
    {
        return None;
    }
    Some(
        usize::from(warehouse_id - 1) * usize::from(DISTRICTS_PER_WAREHOUSE)
            + usize::from(district_id - 1),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persisted_endpoint_view_revalidates_fixed_roots_and_counts() {
        let warehouse_endpoints = vec![((300_000.0_f32 + 1.0).to_bits(), 1)];
        let mut district_endpoints =
            vec![(DISTRICT_YTD_ROOT_BITS, 0); usize::from(DISTRICTS_PER_WAREHOUSE)];
        district_endpoints[0] = ((30_000.0_f32 + 1.0).to_bits(), 1);
        let restored = PersistedPaymentEndpoints::from_canonical_endpoints(
            1,
            1,
            1,
            1,
            warehouse_endpoints.clone(),
            district_endpoints.clone(),
        )
        .unwrap();
        let view: &dyn PaymentEndpointView = &restored;
        assert_eq!(view.warehouses(), 1);
        assert_eq!(view.terminal_count(), 1);
        assert_eq!(
            view.warehouse_endpoint_bits(1),
            Some((300_000.0_f32 + 1.0).to_bits())
        );
        assert_eq!(
            view.district_endpoint_bits(1, 1),
            Some((30_000.0_f32 + 1.0).to_bits())
        );
        assert_eq!(view.district_endpoint_bits(1, 11), None);

        assert!(matches!(
            PersistedPaymentEndpoints::from_canonical_endpoints(
                1,
                1,
                0,
                1,
                warehouse_endpoints.clone(),
                district_endpoints.clone(),
            ),
            Err(PaymentEndpointError::InvalidInvariant(
                "persisted edge totals differ from terminal total"
            ))
        ));
        let mut non_finite = warehouse_endpoints;
        non_finite[0].0 = f32::NAN.to_bits();
        assert!(matches!(
            PersistedPaymentEndpoints::from_canonical_endpoints(
                1,
                1,
                1,
                1,
                non_finite,
                district_endpoints,
            ),
            Err(PaymentEndpointError::InvalidInvariant(
                "endpoint is outside the rooted finite domain"
            ))
        ));
    }

    fn edge(before: f32, amount: f32) -> PaymentFloatEdge {
        PaymentFloatEdge {
            before_bits: before.to_bits(),
            after_bits: (before + amount).to_bits(),
            amount_bits: amount.to_bits(),
        }
    }

    fn terminal(
        warehouse_id: u16,
        district_id: u8,
        warehouse_before: f32,
        district_before: f32,
        amount: f32,
    ) -> PaymentTerminalEvidence {
        PaymentTerminalEvidence {
            warehouse_id,
            district_id,
            warehouse: edge(warehouse_before, amount),
            district: edge(district_before, amount),
        }
    }

    async fn wait_for_pending(collector: &PaymentEndpointCollector, edges: usize) {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if collector.storage().await.pending_edges == edges {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("pending edge count did not reach {edges}"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_phase_offer_returns_before_chain_is_rooted() {
        let collector = Arc::new(PaymentEndpointCollector::new(1, 2).unwrap());
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let future_receipt = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            collector.offer_terminal(terminal(1, 1, w0 + 1.0, d0 + 1.0, 1.0)),
        )
        .await
        .expect("offering an unrooted terminal must not block")
        .unwrap();
        assert_eq!(collector.storage().await.pending_edges, 2);
        assert_eq!(collector.storage().await.unacknowledged_terminals, 1);

        let future_wait = tokio::spawn(async move { future_receipt.wait().await });
        tokio::task::yield_now().await;
        assert!(!future_wait.is_finished());

        let root_receipt = collector
            .offer_terminal(terminal(1, 1, w0, d0, 1.0))
            .await
            .unwrap();
        assert_eq!(root_receipt.wait().await, Ok(()));
        assert_eq!(future_wait.await.unwrap(), Ok(()));
        assert_eq!(collector.storage().await.unacknowledged_terminals, 0);
        assert_eq!(collector.seal().await.unwrap().terminal_count(), 2);
    }

    #[tokio::test]
    async fn rooted_jump_rejects_a_newly_stale_pending_terminal() {
        let collector = PaymentEndpointCollector::new(1, 2).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let pending_receipt = collector
            .offer_terminal(terminal(1, 1, w0 + 1.0, d0 + 1.0, 1.0))
            .await
            .unwrap();

        assert_eq!(
            collector
                .offer_terminal(terminal(1, 1, w0, d0, 3.0))
                .await
                .unwrap_err(),
            PaymentEndpointError::StaleInterval {
                domain: "warehouse.w_ytd"
            }
        );
        assert_eq!(
            pending_receipt.wait().await,
            Err(PaymentEndpointError::Poisoned)
        );
        let storage = collector.storage().await;
        assert_eq!(storage.pending_edges, 0);
        assert_eq!(storage.terminal_count, 0);
        assert_eq!(storage.unacknowledged_terminals, 0);
    }

    #[tokio::test]
    async fn retained_ready_receipts_cannot_exceed_the_client_bound() {
        let collector = PaymentEndpointCollector::new(1, 1).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let retained = collector
            .offer_terminal(terminal(1, 1, w0, d0, 1.0))
            .await
            .unwrap();

        assert_eq!(
            collector
                .offer_terminal(terminal(1, 1, w0 + 1.0, d0 + 1.0, 1.0))
                .await
                .unwrap_err(),
            PaymentEndpointError::ReceiptLimit {
                actual: 2,
                limit: 1
            }
        );
        assert_eq!(retained.wait().await, Err(PaymentEndpointError::Poisoned));
        assert_eq!(collector.storage().await.unacknowledged_terminals, 0);
    }

    #[tokio::test]
    async fn dropping_a_receipt_poison_prevents_a_ready_peer_ack() {
        let collector = PaymentEndpointCollector::new(1, 2).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let receipt = collector
            .offer_terminal(terminal(1, 1, w0, d0, 1.0))
            .await
            .unwrap();
        let peer_receipt = collector
            .offer_terminal(terminal(1, 1, w0 + 1.0, d0 + 1.0, 1.0))
            .await
            .unwrap();
        drop(receipt);

        assert_eq!(
            peer_receipt.wait().await,
            Err(PaymentEndpointError::Poisoned)
        );
        assert_eq!(collector.storage().await.unacknowledged_terminals, 1);
        assert_eq!(
            collector.seal().await,
            Err(PaymentEndpointError::UnacknowledgedReceipts { actual: 1 })
        );
        assert_eq!(
            collector
                .offer_terminal(terminal(1, 1, w0 + 2.0, d0 + 2.0, 1.0))
                .await
                .unwrap_err(),
            PaymentEndpointError::Poisoned
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn cancelling_a_receipt_wait_fails_closed_and_wakes_peers() {
        let collector = Arc::new(PaymentEndpointCollector::new(1, 3).unwrap());
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let abandoned_receipt = collector
            .offer_terminal(terminal(1, 1, w0 + 10.0, d0 + 10.0, 1.0))
            .await
            .unwrap();
        let peer_receipt = collector
            .offer_terminal(terminal(1, 2, w0 + 20.0, d0 + 20.0, 1.0))
            .await
            .unwrap();
        let abandoned_wait = tokio::spawn(async move { abandoned_receipt.wait().await });
        let peer_wait = tokio::spawn(async move { peer_receipt.wait().await });
        wait_for_pending(&collector, 4).await;
        abandoned_wait.abort();
        assert!(abandoned_wait.await.unwrap_err().is_cancelled());

        assert_eq!(
            peer_wait.await.unwrap(),
            Err(PaymentEndpointError::Poisoned)
        );
        assert_eq!(collector.storage().await.unacknowledged_terminals, 1);
        assert_eq!(
            collector.seal().await,
            Err(PaymentEndpointError::UnacknowledgedReceipts { actual: 1 })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn missing_root_cannot_ack_until_root_arrives() {
        let collector = Arc::new(PaymentEndpointCollector::new(1, 2).unwrap());
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let future_collector = Arc::clone(&collector);
        let future = tokio::spawn(async move {
            future_collector
                .record_terminal(terminal(1, 1, w0 + 1.0, d0 + 1.0, 1.0))
                .await
        });
        wait_for_pending(&collector, 2).await;
        assert!(!future.is_finished());

        collector
            .record_terminal(terminal(1, 1, w0, d0, 1.0))
            .await
            .unwrap();
        assert_eq!(future.await.unwrap(), Ok(()));
        let sealed = collector.seal().await.unwrap();
        assert_eq!(sealed.terminal_count(), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reverse_order_joins_one_common_chain() {
        let collector = Arc::new(PaymentEndpointCollector::new(50, 4).unwrap());
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let w1 = w0 + 2.0;
        let d1 = d0 + 2.0;
        let w2 = w1 + 3.0;
        let d2 = d1 + 3.0;

        let last_collector = Arc::clone(&collector);
        let last = tokio::spawn(async move {
            last_collector
                .record_terminal(terminal(1, 1, w2, d2, 4.0))
                .await
        });
        wait_for_pending(&collector, 2).await;
        let middle_collector = Arc::clone(&collector);
        let middle = tokio::spawn(async move {
            middle_collector
                .record_terminal(terminal(1, 1, w1, d1, 3.0))
                .await
        });
        wait_for_pending(&collector, 4).await;
        collector
            .record_terminal(terminal(1, 1, w0, d0, 2.0))
            .await
            .unwrap();
        assert_eq!(middle.await.unwrap(), Ok(()));
        assert_eq!(last.await.unwrap(), Ok(()));

        let sealed = collector.seal().await.unwrap();
        assert_eq!(sealed.terminal_count(), 3);
        assert_eq!(sealed.warehouse_edge_count(), 3);
        assert_eq!(sealed.district_edge_count(), 3);
        assert_eq!(sealed.warehouse_update_count(1), Some(3));
        assert_eq!(sealed.district_update_count(1, 1), Some(3));
        assert_eq!(sealed.warehouse_update_count(50), Some(0));
        assert_eq!(sealed.district_update_count(50, 10), Some(0));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn contradictory_pair_order_poison_wakes_waiter() {
        let collector = Arc::new(PaymentEndpointCollector::new(1, 2).unwrap());
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let first_collector = Arc::clone(&collector);
        let first = tokio::spawn(async move {
            first_collector
                .record_terminal(terminal(1, 1, w0, d0 + 2.0, 1.0))
                .await
        });
        wait_for_pending(&collector, 2).await;
        assert_eq!(
            collector
                .record_terminal(terminal(1, 1, w0 + 1.0, d0, 2.0))
                .await,
            Err(PaymentEndpointError::PendingLimit {
                actual: 4,
                limit: 4
            })
        );
        assert_eq!(first.await.unwrap(), Err(PaymentEndpointError::Poisoned));
        assert_eq!(collector.seal().await, Err(PaymentEndpointError::Poisoned));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn visible_fork_poison_wakes_all_waiters() {
        let collector = Arc::new(PaymentEndpointCollector::new(1, 3).unwrap());
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let first_collector = Arc::clone(&collector);
        let first = tokio::spawn(async move {
            first_collector
                .record_terminal(terminal(1, 1, w0 + 10.0, d0 + 10.0, 2.0))
                .await
        });
        wait_for_pending(&collector, 2).await;
        assert!(matches!(
            collector
                .record_terminal(terminal(1, 1, w0 + 10.0, d0 + 10.0, 3.0))
                .await,
            Err(PaymentEndpointError::Fork { .. })
        ));
        assert_eq!(first.await.unwrap(), Err(PaymentEndpointError::Poisoned));
        assert_eq!(
            collector.record_terminal(terminal(1, 1, w0, d0, 1.0)).await,
            Err(PaymentEndpointError::Poisoned)
        );
    }

    #[tokio::test]
    async fn wrong_rne_and_signed_zero_poison_without_partial_commit() {
        let collector = PaymentEndpointCollector::new(1, 1).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let before = collector.storage().await;
        let mut bad = terminal(1, 1, w0, d0, 2.0);
        bad.warehouse.after_bits ^= 1;
        assert_eq!(
            collector.record_terminal(bad).await,
            Err(PaymentEndpointError::FloatMismatch {
                domain: "warehouse.w_ytd"
            })
        );
        assert_eq!(collector.storage().await, before);

        let signed_zero = PaymentEndpointCollector::new(1, 1).unwrap();
        assert_eq!(
            signed_zero
                .record_terminal(PaymentTerminalEvidence {
                    warehouse_id: 1,
                    district_id: 1,
                    warehouse: edge(-0.0, 1.0),
                    district: edge(d0, 1.0),
                })
                .await,
            Err(PaymentEndpointError::StaleInterval {
                domain: "warehouse.w_ytd"
            })
        );
    }

    #[tokio::test]
    async fn self_loops_are_valid_and_counted() {
        let collector = PaymentEndpointCollector::new(1, 1).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let large = 33_554_432.0_f32;
        collector
            .record_terminal(terminal(1, 1, w0, d0, large))
            .await
            .unwrap();
        let w1 = w0 + large;
        let d1 = d0 + large;
        let loop_terminal = terminal(1, 1, w1, d1, 1.0);
        assert_eq!(
            loop_terminal.warehouse.before_bits,
            loop_terminal.warehouse.after_bits
        );
        assert_eq!(
            loop_terminal.district.before_bits,
            loop_terminal.district.after_bits
        );
        collector.record_terminal(loop_terminal).await.unwrap();
        let sealed = collector.seal().await.unwrap();
        assert_eq!(sealed.warehouse_update_count(1), Some(2));
        assert_eq!(sealed.district_update_count(1, 1), Some(2));
    }

    #[tokio::test]
    async fn client_bound_rejects_unresolvable_single_client_gap() {
        let collector = PaymentEndpointCollector::new(1, 1).unwrap();
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let before = collector.storage().await;
        assert_eq!(
            collector
                .record_terminal(terminal(1, 1, w0 + 10.0, d0 + 10.0, 1.0))
                .await,
            Err(PaymentEndpointError::PendingLimit {
                actual: 2,
                limit: 2
            })
        );
        assert_eq!(collector.storage().await.pending_edges, 0);
        assert_eq!(
            collector.storage().await.terminal_count,
            before.terminal_count
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn seal_rejects_and_wakes_in_flight_receipt() {
        let collector = Arc::new(PaymentEndpointCollector::new(1, 2).unwrap());
        let w0 = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let d0 = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        let pending_collector = Arc::clone(&collector);
        let pending = tokio::spawn(async move {
            pending_collector
                .record_terminal(terminal(1, 1, w0 + 10.0, d0 + 10.0, 1.0))
                .await
        });
        wait_for_pending(&collector, 2).await;
        assert_eq!(
            collector.seal().await,
            Err(PaymentEndpointError::UnacknowledgedReceipts { actual: 1 })
        );
        assert_eq!(pending.await.unwrap(), Err(PaymentEndpointError::Poisoned));
    }

    #[tokio::test]
    async fn one_million_updates_keep_fixed_collector_shape() {
        let collector = PaymentEndpointCollector::new(50, 32).unwrap();
        let initial = collector.storage().await;
        let mut warehouse = f32::from_bits(WAREHOUSE_YTD_ROOT_BITS);
        let mut district = f32::from_bits(DISTRICT_YTD_ROOT_BITS);
        for _ in 0..1_000_000 {
            collector
                .record_terminal(terminal(1, 1, warehouse, district, 1.0))
                .await
                .unwrap();
            warehouse += 1.0;
            district += 1.0;
        }
        let final_storage = collector.storage().await;
        assert_eq!(final_storage.warehouse_slots, initial.warehouse_slots);
        assert_eq!(final_storage.district_slots, initial.district_slots);
        assert_eq!(final_storage.pending_capacity, initial.pending_capacity);
        assert_eq!(final_storage.pending_edges, 0);
        assert_eq!(final_storage.terminal_count, 1_000_000);

        let sealed = collector.seal().await.unwrap();
        assert_eq!(sealed.terminal_count(), 1_000_000);
        assert_eq!(sealed.warehouse_update_count(1), Some(1_000_000));
        assert_eq!(sealed.district_update_count(1, 1), Some(1_000_000));
    }
}
