//! Bounded, order-independent validation for committed row-version evidence.
//!
//! Every Customer and Stock edge is checked immediately and contributes to
//! the global counters. A seed-bound bottom-k reservoir retains complete
//! rooted chains for at most 64 keys in each domain. Worker responses may
//! arrive out of commit order, so selected chains join exact version
//! intervals. Recording is terminal-atomic and any error permanently poisons
//! the collector.

use std::fmt;
use std::mem::{size_of, swap};

use thiserror::Error;

use crate::consistency::{
    CustomerLogicalVersion, CustomerUpdateEndpoint, CustomerUpdateEvidence, CustomerUpdateKind,
};
use crate::profile::ITEM_COUNT;

use super::runner::StockVersion;

const CUSTOMERS_PER_DISTRICT: usize = 3_000;
const DISTRICTS_PER_WAREHOUSE: usize = 10;
const MAX_EDGES_PER_TERMINAL: usize = 15;
const SAMPLE_LIMIT: usize = 64;
const CUSTOMER_INITIAL_BALANCE_BITS: u32 = (-10.0_f32).to_bits();
const CUSTOMER_INITIAL_YTD_BITS: u32 = 10.0_f32.to_bits();
const CUSTOMER_INITIAL_PAYMENT_COUNT: i32 = 1;
const CUSTOMER_INITIAL_DELIVERY_COUNT: i32 = 0;
const STOCK_INITIAL_YTD_BITS: u32 = 0.0_f32.to_bits();
const CUSTOMER_SAMPLE_DOMAIN: u64 = 0x4355_5354_4f4d_4552;
const STOCK_SAMPLE_DOMAIN: u64 = 0x5354_4f43_4b5f_4b45;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CustomerKey {
    pub warehouse_id: i32,
    pub district_id: i32,
    pub customer_id: i32,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct StockKey {
    pub warehouse_id: i32,
    pub item_id: i32,
}

/// Supplies the exact setup-time Stock root for a key.
pub trait StockRootProvider: Send + Sync {
    fn expected_root(&self, key: StockKey) -> Option<StockVersion>;
}

impl<F> StockRootProvider for F
where
    F: Fn(StockKey) -> Option<StockVersion> + Send + Sync,
{
    fn expected_root(&self, key: StockKey) -> Option<StockVersion> {
        self(key)
    }
}

pub struct CustomerMutation {
    key: CustomerKey,
    update: CustomerUpdateEvidence,
}

impl CustomerMutation {
    pub fn new(key: CustomerKey, update: CustomerUpdateEvidence) -> Self {
        Self { key, update }
    }
}

pub struct StockMutation {
    key: StockKey,
    ordered_quantity: u8,
    remote_increment: u8,
    before: StockVersion,
    after: StockVersion,
}

impl StockMutation {
    pub fn new(
        key: StockKey,
        ordered_quantity: u8,
        remote_increment: u8,
        before: StockVersion,
        after: StockVersion,
    ) -> Self {
        Self {
            key,
            ordered_quantity,
            remote_increment,
            before,
            after,
        }
    }
}

enum TerminalUpdates<'a> {
    Empty,
    Customers(&'a [CustomerMutation]),
    Stocks(&'a [StockMutation]),
}

/// Evidence emitted by exactly one committed terminal.
pub struct TerminalEvidence<'a> {
    updates: TerminalUpdates<'a>,
}

impl<'a> TerminalEvidence<'a> {
    pub fn empty() -> Self {
        Self {
            updates: TerminalUpdates::Empty,
        }
    }

    pub fn customers(updates: &'a [CustomerMutation]) -> Self {
        Self {
            updates: TerminalUpdates::Customers(updates),
        }
    }

    pub fn stocks(updates: &'a [StockMutation]) -> Self {
        Self {
            updates: TerminalUpdates::Stocks(updates),
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CustomerSlot {
    payment_count: i32,
    delivery_count: i32,
    balance_bits: u32,
    ytd_payment_bits: u32,
}

impl CustomerSlot {
    const EMPTY: Self = Self {
        payment_count: 0,
        delivery_count: 0,
        balance_bits: 0,
        ytd_payment_bits: 0,
    };

    const ROOT: Self = Self {
        payment_count: CUSTOMER_INITIAL_PAYMENT_COUNT,
        delivery_count: CUSTOMER_INITIAL_DELIVERY_COUNT,
        balance_bits: CUSTOMER_INITIAL_BALANCE_BITS,
        ytd_payment_bits: CUSTOMER_INITIAL_YTD_BITS,
    };

    fn is_rooted(self) -> bool {
        self.payment_count != 0
    }

    fn prefix(self) -> Self {
        if self.is_rooted() {
            self
        } else {
            Self::ROOT
        }
    }

    fn endpoint(self) -> CustomerUpdateEndpoint {
        CustomerUpdateEndpoint {
            version: CustomerLogicalVersion {
                payment_count: self.payment_count,
                delivery_count: self.delivery_count,
            },
            balance_bits: self.balance_bits,
            ytd_payment_bits: self.ytd_payment_bits,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StockSlot {
    quantity: i32,
    ytd_bits: u32,
    order_count: i32,
    remote_count: i32,
    initial_quantity: i32,
}

impl StockSlot {
    const EMPTY: Self = Self {
        quantity: 0,
        ytd_bits: 0,
        order_count: 0,
        remote_count: 0,
        initial_quantity: 0,
    };

    fn is_rooted(self) -> bool {
        self.initial_quantity != 0
    }

    fn endpoint(self) -> StockVersion {
        StockVersion {
            quantity: self.quantity,
            ytd_bits: self.ytd_bits,
            order_count: self.order_count,
            remote_count: self.remote_count,
        }
    }

    fn initial(self) -> StockVersion {
        StockVersion {
            quantity: self.initial_quantity,
            ytd_bits: STOCK_INITIAL_YTD_BITS,
            order_count: 0,
            remote_count: 0,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct StockState {
    quantity: i32,
    ytd_bits: u32,
    order_count: i32,
    remote_count: i32,
}

impl StockState {
    fn from_version(version: &StockVersion) -> Self {
        Self {
            quantity: version.quantity,
            ytd_bits: version.ytd_bits,
            order_count: version.order_count,
            remote_count: version.remote_count,
        }
    }

    fn version(self) -> StockVersion {
        StockVersion {
            quantity: self.quantity,
            ytd_bits: self.ytd_bits,
            order_count: self.order_count,
            remote_count: self.remote_count,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum YtdSpan {
    Identity,
    Known { start_bits: u32, end_bits: u32 },
}

#[derive(Clone, Copy, Debug)]
struct CustomerSegment {
    start_total: i64,
    end_total: i64,
    start_version: CustomerLogicalVersion,
    end_version: CustomerLogicalVersion,
    start_balance_bits: u32,
    end_balance_bits: u32,
    ytd: YtdSpan,
}

impl CustomerSegment {
    fn single(update: CustomerUpdateEvidence) -> Result<Self, CollectorError> {
        let start_total = customer_total(update.before_version)?;
        let end_total = customer_total(update.after_version)?;
        if end_total
            != start_total
                .checked_add(1)
                .ok_or(CollectorError::Overflow("customer logical version"))?
        {
            return Err(CollectorError::InvalidCustomerEdge(
                "logical version does not advance by one",
            ));
        }

        let expected_version = match update.kind {
            CustomerUpdateKind::Payment => CustomerLogicalVersion {
                payment_count: update
                    .before_version
                    .payment_count
                    .checked_add(1)
                    .ok_or(CollectorError::Overflow("customer payment version"))?,
                delivery_count: update.before_version.delivery_count,
            },
            CustomerUpdateKind::Delivery => CustomerLogicalVersion {
                payment_count: update.before_version.payment_count,
                delivery_count: update
                    .before_version
                    .delivery_count
                    .checked_add(1)
                    .ok_or(CollectorError::Overflow("customer delivery version"))?,
            },
        };
        if update.after_version != expected_version {
            return Err(CollectorError::InvalidCustomerEdge(
                "wrong family counter advanced",
            ));
        }

        let amount = require_finite("customer amount", update.amount_bits)?;
        if amount <= 0.0 {
            return Err(CollectorError::InvalidCustomerEdge(
                "amount must be positive",
            ));
        }
        let before_balance = require_finite("customer balance before", update.balance_before_bits)?;
        require_finite("customer balance after", update.balance_after_bits)?;
        let expected_balance = match update.kind {
            CustomerUpdateKind::Payment => before_balance - amount,
            CustomerUpdateKind::Delivery => before_balance + amount,
        };
        if expected_balance.to_bits() != update.balance_after_bits {
            return Err(CollectorError::FloatMismatch("customer balance"));
        }

        let ytd = match (
            update.kind,
            update.ytd_payment_before_bits,
            update.ytd_payment_after_bits,
        ) {
            (CustomerUpdateKind::Payment, Some(before_bits), Some(after_bits)) => {
                let before = require_finite("customer YTD before", before_bits)?;
                require_finite("customer YTD after", after_bits)?;
                if (before + amount).to_bits() != after_bits {
                    return Err(CollectorError::FloatMismatch("customer YTD"));
                }
                YtdSpan::Known {
                    start_bits: before_bits,
                    end_bits: after_bits,
                }
            }
            (CustomerUpdateKind::Payment, _, _) => {
                return Err(CollectorError::InvalidCustomerEdge(
                    "Payment omitted YTD evidence",
                ));
            }
            (CustomerUpdateKind::Delivery, None, None) => YtdSpan::Identity,
            (CustomerUpdateKind::Delivery, _, _) => {
                return Err(CollectorError::InvalidCustomerEdge(
                    "Delivery supplied Payment-only YTD evidence",
                ));
            }
        };

        let root_version = CustomerLogicalVersion {
            payment_count: CUSTOMER_INITIAL_PAYMENT_COUNT,
            delivery_count: CUSTOMER_INITIAL_DELIVERY_COUNT,
        };
        if update.before_version == root_version {
            if update.balance_before_bits != CUSTOMER_INITIAL_BALANCE_BITS {
                return Err(CollectorError::BoundaryMismatch("customer root"));
            }
            if let YtdSpan::Known { start_bits, .. } = ytd {
                if start_bits != CUSTOMER_INITIAL_YTD_BITS {
                    return Err(CollectorError::BoundaryMismatch("customer root YTD"));
                }
            }
        }

        Ok(Self {
            start_total,
            end_total,
            start_version: update.before_version,
            end_version: update.after_version,
            start_balance_bits: update.balance_before_bits,
            end_balance_bits: update.balance_after_bits,
            ytd,
        })
    }

    fn then(self, next: Self) -> Result<Self, CollectorError> {
        if self.end_total != next.start_total
            || self.end_version != next.start_version
            || self.end_balance_bits != next.start_balance_bits
        {
            return Err(CollectorError::BoundaryMismatch("customer"));
        }
        Ok(Self {
            start_total: self.start_total,
            end_total: next.end_total,
            start_version: self.start_version,
            end_version: next.end_version,
            start_balance_bits: self.start_balance_bits,
            end_balance_bits: next.end_balance_bits,
            ytd: compose_ytd(self.ytd, next.ytd)?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct StockSegment {
    start: StockState,
    end: StockState,
}

impl StockSegment {
    fn single(mutation: &StockMutation) -> Result<Self, CollectorError> {
        if !(1..=10).contains(&mutation.ordered_quantity) {
            return Err(CollectorError::InvalidStockEdge(
                "ordered quantity is outside 1..=10",
            ));
        }
        if mutation.remote_increment > 1 {
            return Err(CollectorError::InvalidStockEdge(
                "remote increment is outside 0..=1",
            ));
        }

        let before = StockState::from_version(&mutation.before);
        let after = StockState::from_version(&mutation.after);
        validate_stock_state("before", before)?;
        validate_stock_state("after", after)?;
        if after.order_count
            != before
                .order_count
                .checked_add(1)
                .ok_or(CollectorError::Overflow("stock order count"))?
        {
            return Err(CollectorError::InvalidStockEdge(
                "order count does not advance by one",
            ));
        }
        if after.remote_count
            != before
                .remote_count
                .checked_add(i32::from(mutation.remote_increment))
                .ok_or(CollectorError::Overflow("stock remote count"))?
        {
            return Err(CollectorError::InvalidStockEdge(
                "remote count transition is wrong",
            ));
        }

        let ordered_quantity = i32::from(mutation.ordered_quantity);
        let expected_quantity = if before.quantity >= ordered_quantity + 10 {
            before.quantity - ordered_quantity
        } else {
            before.quantity + 91 - ordered_quantity
        };
        if after.quantity != expected_quantity {
            return Err(CollectorError::InvalidStockEdge(
                "quantity transition is wrong",
            ));
        }
        if (f32::from_bits(before.ytd_bits) + f32::from(mutation.ordered_quantity)).to_bits()
            != after.ytd_bits
        {
            return Err(CollectorError::FloatMismatch("stock YTD"));
        }
        Ok(Self {
            start: before,
            end: after,
        })
    }

    fn then(self, next: Self) -> Result<Self, CollectorError> {
        if self.end != next.start {
            return Err(CollectorError::BoundaryMismatch("stock"));
        }
        Ok(Self {
            start: self.start,
            end: next.end,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct CustomerSample {
    rank: u64,
    key_index: u32,
    slot: CustomerSlot,
}

#[derive(Clone, Copy, Debug)]
struct StockSample {
    rank: u64,
    key_index: u32,
    slot: StockSlot,
}

#[derive(Clone, Copy)]
struct ValidatedCustomer {
    key_index: u32,
    segment: CustomerSegment,
}

#[derive(Clone, Copy)]
struct ValidatedStock {
    key: StockKey,
    key_index: u32,
    segment: StockSegment,
}

#[derive(Clone, Copy, Debug)]
enum PendingSegment {
    Customer {
        key_index: u32,
        segment: CustomerSegment,
    },
    Stock {
        key_index: u32,
        segment: StockSegment,
    },
}

/// Central bounded collector. It intentionally cannot be cloned.
pub struct IntervalCollector {
    warehouses: u16,
    sample_seed: u64,
    pending_limit: usize,
    stock_roots: Box<dyn StockRootProvider>,
    customers: Vec<CustomerSample>,
    stocks: Vec<StockSample>,
    customer_scratch: Vec<CustomerSample>,
    stock_scratch: Vec<StockSample>,
    pending: Vec<PendingSegment>,
    pending_scratch: Vec<PendingSegment>,
    customer_updates: u64,
    stock_updates: u64,
    poisoned: Option<String>,
}

impl fmt::Debug for IntervalCollector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntervalCollector")
            .field("warehouses", &self.warehouses)
            .field("selected_customers", &self.customers.len())
            .field("selected_stocks", &self.stocks.len())
            .field("pending_intervals", &self.pending.len())
            .field("pending_limit", &self.pending_limit)
            .field("poisoned", &self.poisoned.is_some())
            .finish()
    }
}

impl IntervalCollector {
    pub fn new<P>(
        warehouses: u16,
        clients: u16,
        sample_seed: u64,
        stock_roots: P,
    ) -> Result<Self, CollectorError>
    where
        P: StockRootProvider + 'static,
    {
        validate_configuration(warehouses, clients)?;
        let pending_limit = usize::from(clients)
            .checked_mul(MAX_EDGES_PER_TERMINAL)
            .ok_or(CollectorError::Overflow("pending interval limit"))?;
        let pending_capacity = pending_limit
            .checked_add(MAX_EDGES_PER_TERMINAL)
            .ok_or(CollectorError::Overflow("pending scratch capacity"))?;
        Ok(Self {
            warehouses,
            sample_seed,
            pending_limit,
            stock_roots: Box::new(stock_roots),
            customers: Vec::with_capacity(SAMPLE_LIMIT),
            stocks: Vec::with_capacity(SAMPLE_LIMIT),
            customer_scratch: Vec::with_capacity(SAMPLE_LIMIT),
            stock_scratch: Vec::with_capacity(SAMPLE_LIMIT),
            pending: Vec::with_capacity(pending_capacity),
            pending_scratch: Vec::with_capacity(pending_capacity),
            customer_updates: 0,
            stock_updates: 0,
            poisoned: None,
        })
    }

    /// Atomically records all evidence from one committed terminal.
    ///
    /// Every edge is validated even when its key is outside the bottom-k
    /// reservoir. Any error poisons the collector and publishes no prefix of
    /// the failed terminal.
    pub fn record_terminal(
        &mut self,
        terminal: TerminalEvidence<'_>,
    ) -> Result<(), CollectorError> {
        if let Some(cause) = &self.poisoned {
            return Err(CollectorError::Poisoned {
                cause: cause.clone(),
            });
        }
        let result = match terminal.updates {
            TerminalUpdates::Empty => Ok(()),
            TerminalUpdates::Customers(updates) => self.record_customer_terminal(updates),
            TerminalUpdates::Stocks(updates) => self.record_stock_terminal(updates),
        };
        if let Err(error) = result {
            self.customer_scratch.clear();
            self.stock_scratch.clear();
            self.pending_scratch.clear();
            self.poisoned = Some(error.to_string());
            return Err(error);
        }
        Ok(())
    }

    pub fn storage(&self) -> CollectorStorage {
        CollectorStorage {
            selected_customers: self.customers.len(),
            selected_stocks: self.stocks.len(),
            pending_intervals: self.pending.len(),
            pending_limit: self.pending_limit,
            owned_buffer_capacity_bytes: self.owned_buffer_capacity_bytes(),
            poisoned: self.poisoned.is_some(),
        }
    }

    /// Exact upper bound for collector-owned `Vec` element capacity.
    ///
    /// The bound is independent of warehouse count. Allocator metadata and
    /// caller-owned root-provider state are not included.
    pub fn owned_buffer_capacity_upper_bound_for(
        warehouses: u16,
        clients: u16,
    ) -> Result<usize, CollectorError> {
        validate_configuration(warehouses, clients)?;
        let pending_capacity = usize::from(clients)
            .checked_mul(MAX_EDGES_PER_TERMINAL)
            .and_then(|limit| limit.checked_add(MAX_EDGES_PER_TERMINAL))
            .ok_or(CollectorError::Overflow("pending scratch capacity"))?;
        2_usize
            .checked_mul(SAMPLE_LIMIT)
            .and_then(|count| count.checked_mul(size_of::<CustomerSample>()))
            .and_then(|customer_bytes| {
                2_usize
                    .checked_mul(SAMPLE_LIMIT)
                    .and_then(|count| count.checked_mul(size_of::<StockSample>()))
                    .and_then(|stock_bytes| customer_bytes.checked_add(stock_bytes))
            })
            .and_then(|sample_bytes| {
                2_usize
                    .checked_mul(pending_capacity)
                    .and_then(|count| count.checked_mul(size_of::<PendingSegment>()))
                    .and_then(|pending_bytes| sample_bytes.checked_add(pending_bytes))
            })
            .ok_or(CollectorError::Overflow("collector buffer capacity"))
    }

    pub fn seal(mut self) -> Result<SealedIntervalEvidence, CollectorError> {
        if let Some(cause) = self.poisoned {
            return Err(CollectorError::Poisoned { cause });
        }
        if !self.pending.is_empty() {
            return Err(CollectorError::Disconnected {
                pending: self.pending.len(),
            });
        }
        if self.customers.iter().any(|sample| !sample.slot.is_rooted())
            || self.stocks.iter().any(|sample| !sample.slot.is_rooted())
        {
            return Err(CollectorError::Disconnected { pending: 0 });
        }
        self.customers
            .sort_unstable_by_key(|sample| (sample.rank, sample.key_index));
        self.stocks
            .sort_unstable_by_key(|sample| (sample.rank, sample.key_index));
        Ok(SealedIntervalEvidence {
            warehouses: self.warehouses,
            sample_seed: self.sample_seed,
            customers: self.customers,
            stocks: self.stocks,
            customer_updates: self.customer_updates,
            stock_updates: self.stock_updates,
        })
    }

    fn record_customer_terminal(
        &mut self,
        updates: &[CustomerMutation],
    ) -> Result<(), CollectorError> {
        validate_terminal_width(updates.len())?;
        let next_updates = self
            .customer_updates
            .checked_add(updates.len() as u64)
            .ok_or(CollectorError::Overflow("customer update count"))?;
        self.reset_customer_scratch();

        let mut validated = [None; MAX_EDGES_PER_TERMINAL];
        for (position, mutation) in updates.iter().enumerate() {
            let key_index = customer_index(self.warehouses, mutation.key)?;
            let segment = CustomerSegment::single(mutation.update)?;
            validated[position] = Some(ValidatedCustomer { key_index, segment });
        }
        for edge in validated[..updates.len()].iter().flatten() {
            let rank = sample_rank(self.sample_seed, CUSTOMER_SAMPLE_DOMAIN, edge.key_index);
            select_customer(
                &mut self.customer_scratch,
                &mut self.pending_scratch,
                edge.key_index,
                rank,
            );
        }
        for edge in validated[..updates.len()].iter().flatten() {
            if let Some(position) = self
                .customer_scratch
                .iter()
                .position(|sample| sample.key_index == edge.key_index)
            {
                insert_customer(
                    edge.key_index,
                    &mut self.customer_scratch[position].slot,
                    edge.segment,
                    &mut self.pending_scratch,
                )?;
            }
        }
        validate_pending_limit(self.pending_scratch.len(), self.pending_limit)?;

        swap(&mut self.customers, &mut self.customer_scratch);
        swap(&mut self.pending, &mut self.pending_scratch);
        self.customer_scratch.clear();
        self.pending_scratch.clear();
        self.customer_updates = next_updates;
        Ok(())
    }

    fn record_stock_terminal(&mut self, updates: &[StockMutation]) -> Result<(), CollectorError> {
        validate_terminal_width(updates.len())?;
        let next_updates = self
            .stock_updates
            .checked_add(updates.len() as u64)
            .ok_or(CollectorError::Overflow("stock update count"))?;
        self.reset_stock_scratch();

        let mut validated = [None; MAX_EDGES_PER_TERMINAL];
        for (position, mutation) in updates.iter().enumerate() {
            let key_index = stock_index(self.warehouses, mutation.key)?;
            let segment = StockSegment::single(mutation)?;
            if segment.start.order_count == 0 {
                validate_stock_root(self.stock_roots.as_ref(), mutation.key, segment.start)?;
            }
            validated[position] = Some(ValidatedStock {
                key: mutation.key,
                key_index,
                segment,
            });
        }
        for edge in validated[..updates.len()].iter().flatten() {
            let rank = sample_rank(self.sample_seed, STOCK_SAMPLE_DOMAIN, edge.key_index);
            select_stock(
                &mut self.stock_scratch,
                &mut self.pending_scratch,
                edge.key_index,
                rank,
            );
        }
        for edge in validated[..updates.len()].iter().flatten() {
            if let Some(position) = self
                .stock_scratch
                .iter()
                .position(|sample| sample.key_index == edge.key_index)
            {
                insert_stock(
                    self.stock_roots.as_ref(),
                    edge.key,
                    edge.key_index,
                    &mut self.stock_scratch[position].slot,
                    edge.segment,
                    &mut self.pending_scratch,
                )?;
            }
        }
        validate_pending_limit(self.pending_scratch.len(), self.pending_limit)?;

        swap(&mut self.stocks, &mut self.stock_scratch);
        swap(&mut self.pending, &mut self.pending_scratch);
        self.stock_scratch.clear();
        self.pending_scratch.clear();
        self.stock_updates = next_updates;
        Ok(())
    }

    fn reset_customer_scratch(&mut self) {
        self.customer_scratch.clear();
        self.customer_scratch.extend(self.customers.iter().copied());
        self.pending_scratch.clear();
        self.pending_scratch.extend(self.pending.iter().copied());
    }

    fn reset_stock_scratch(&mut self) {
        self.stock_scratch.clear();
        self.stock_scratch.extend(self.stocks.iter().copied());
        self.pending_scratch.clear();
        self.pending_scratch.extend(self.pending.iter().copied());
    }

    fn owned_buffer_capacity_bytes(&self) -> usize {
        (self.customers.capacity() + self.customer_scratch.capacity()) * size_of::<CustomerSample>()
            + (self.stocks.capacity() + self.stock_scratch.capacity()) * size_of::<StockSample>()
            + (self.pending.capacity() + self.pending_scratch.capacity())
                * size_of::<PendingSegment>()
    }
}

pub struct CollectorStorage {
    selected_customers: usize,
    selected_stocks: usize,
    pending_intervals: usize,
    pending_limit: usize,
    owned_buffer_capacity_bytes: usize,
    poisoned: bool,
}

impl CollectorStorage {
    pub fn selected_customer_count(&self) -> usize {
        self.selected_customers
    }

    pub fn selected_stock_count(&self) -> usize {
        self.selected_stocks
    }

    pub fn pending_intervals(&self) -> usize {
        self.pending_intervals
    }

    pub fn pending_limit(&self) -> usize {
        self.pending_limit
    }

    pub fn owned_buffer_capacity_bytes(&self) -> usize {
        self.owned_buffer_capacity_bytes
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
}

/// Validated, bounded samples produced only by consuming `seal`.
///
/// All fields are private, and the type has neither `Default` nor `Clone`.
pub struct SealedIntervalEvidence {
    warehouses: u16,
    sample_seed: u64,
    customers: Vec<CustomerSample>,
    stocks: Vec<StockSample>,
    customer_updates: u64,
    stock_updates: u64,
}

impl SealedIntervalEvidence {
    pub fn sample_seed(&self) -> u64 {
        self.sample_seed
    }

    pub fn customer_update_count(&self) -> u64 {
        self.customer_updates
    }

    pub fn stock_update_count(&self) -> u64 {
        self.stock_updates
    }

    pub fn customer_sample_count(&self) -> usize {
        self.customers.len()
    }

    pub fn stock_sample_count(&self) -> usize {
        self.stocks.len()
    }

    pub fn customer(
        &self,
        key: CustomerKey,
    ) -> Result<Option<SealedCustomerChain<'_>>, CollectorError> {
        let key_index = customer_index(self.warehouses, key)?;
        Ok(self
            .customers
            .iter()
            .find(|sample| sample.key_index == key_index)
            .map(|sample| SealedCustomerChain { key, sample }))
    }

    pub fn stock(&self, key: StockKey) -> Result<Option<SealedStockChain<'_>>, CollectorError> {
        let key_index = stock_index(self.warehouses, key)?;
        Ok(self
            .stocks
            .iter()
            .find(|sample| sample.key_index == key_index)
            .map(|sample| SealedStockChain { key, sample }))
    }

    pub fn customers(&self) -> SealedCustomerIter<'_> {
        SealedCustomerIter {
            warehouses: self.warehouses,
            samples: self.customers.iter(),
        }
    }

    pub fn stocks(&self) -> SealedStockIter<'_> {
        SealedStockIter {
            warehouses: self.warehouses,
            samples: self.stocks.iter(),
        }
    }
}

pub struct SealedCustomerChain<'a> {
    key: CustomerKey,
    sample: &'a CustomerSample,
}

impl SealedCustomerChain<'_> {
    pub fn key(&self) -> CustomerKey {
        self.key
    }

    pub fn sample_rank(&self) -> u64 {
        self.sample.rank
    }

    pub fn endpoint(&self) -> CustomerUpdateEndpoint {
        self.sample.slot.endpoint()
    }

    pub fn payment_updates(&self) -> u64 {
        (self.sample.slot.payment_count - CUSTOMER_INITIAL_PAYMENT_COUNT) as u64
    }

    pub fn delivery_updates(&self) -> u64 {
        (self.sample.slot.delivery_count - CUSTOMER_INITIAL_DELIVERY_COUNT) as u64
    }
}

pub struct SealedStockChain<'a> {
    key: StockKey,
    sample: &'a StockSample,
}

impl SealedStockChain<'_> {
    pub fn key(&self) -> StockKey {
        self.key
    }

    pub fn sample_rank(&self) -> u64 {
        self.sample.rank
    }

    pub fn initial(&self) -> StockVersion {
        self.sample.slot.initial()
    }

    pub fn endpoint(&self) -> StockVersion {
        self.sample.slot.endpoint()
    }

    pub fn update_count(&self) -> u64 {
        self.sample.slot.order_count as u64
    }
}

pub struct SealedCustomerIter<'a> {
    warehouses: u16,
    samples: std::slice::Iter<'a, CustomerSample>,
}

impl<'a> Iterator for SealedCustomerIter<'a> {
    type Item = SealedCustomerChain<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.samples.next().map(|sample| SealedCustomerChain {
            key: customer_key_from_index(self.warehouses, sample.key_index),
            sample,
        })
    }
}

pub struct SealedStockIter<'a> {
    warehouses: u16,
    samples: std::slice::Iter<'a, StockSample>,
}

impl<'a> Iterator for SealedStockIter<'a> {
    type Item = SealedStockChain<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.samples.next().map(|sample| SealedStockChain {
            key: stock_key_from_index(self.warehouses, sample.key_index),
            sample,
        })
    }
}

fn select_customer(
    samples: &mut Vec<CustomerSample>,
    pending: &mut Vec<PendingSegment>,
    key_index: u32,
    rank: u64,
) -> Option<usize> {
    if let Some(position) = samples
        .iter()
        .position(|sample| sample.key_index == key_index)
    {
        return Some(position);
    }
    if samples.len() == SAMPLE_LIMIT {
        let worst = samples
            .iter()
            .enumerate()
            .max_by_key(|(_, sample)| (sample.rank, sample.key_index))
            .map(|(position, _)| position)
            .expect("a full reservoir is nonempty");
        if (rank, key_index) >= (samples[worst].rank, samples[worst].key_index) {
            return None;
        }
        let evicted = samples.swap_remove(worst);
        pending.retain(|entry| {
            !matches!(
                entry,
                PendingSegment::Customer {
                    key_index: candidate,
                    ..
                } if *candidate == evicted.key_index
            )
        });
    }
    samples.push(CustomerSample {
        rank,
        key_index,
        slot: CustomerSlot::EMPTY,
    });
    Some(samples.len() - 1)
}

fn select_stock(
    samples: &mut Vec<StockSample>,
    pending: &mut Vec<PendingSegment>,
    key_index: u32,
    rank: u64,
) -> Option<usize> {
    if let Some(position) = samples
        .iter()
        .position(|sample| sample.key_index == key_index)
    {
        return Some(position);
    }
    if samples.len() == SAMPLE_LIMIT {
        let worst = samples
            .iter()
            .enumerate()
            .max_by_key(|(_, sample)| (sample.rank, sample.key_index))
            .map(|(position, _)| position)
            .expect("a full reservoir is nonempty");
        if (rank, key_index) >= (samples[worst].rank, samples[worst].key_index) {
            return None;
        }
        let evicted = samples.swap_remove(worst);
        pending.retain(|entry| {
            !matches!(
                entry,
                PendingSegment::Stock {
                    key_index: candidate,
                    ..
                } if *candidate == evicted.key_index
            )
        });
    }
    samples.push(StockSample {
        rank,
        key_index,
        slot: StockSlot::EMPTY,
    });
    Some(samples.len() - 1)
}

fn insert_customer(
    key_index: u32,
    slot: &mut CustomerSlot,
    segment: CustomerSegment,
    pending: &mut Vec<PendingSegment>,
) -> Result<(), CollectorError> {
    let prefix = slot.prefix();
    let prefix_total = customer_slot_total(prefix)?;
    if segment.start_total < prefix_total {
        return Err(CollectorError::DuplicatePredecessor {
            domain: "customer",
            predecessor: segment.start_total,
        });
    }
    if segment.start_total == prefix_total {
        absorb_customer(slot, segment)?;
        drain_customer(key_index, slot, pending)?;
        return Ok(());
    }
    insert_customer_pending(key_index, segment, pending)
}

fn absorb_customer(
    slot: &mut CustomerSlot,
    segment: CustomerSegment,
) -> Result<(), CollectorError> {
    let prefix = slot.prefix();
    let version = CustomerLogicalVersion {
        payment_count: prefix.payment_count,
        delivery_count: prefix.delivery_count,
    };
    if version != segment.start_version || prefix.balance_bits != segment.start_balance_bits {
        return Err(CollectorError::BoundaryMismatch("customer"));
    }
    let ytd_payment_bits = match segment.ytd {
        YtdSpan::Identity => prefix.ytd_payment_bits,
        YtdSpan::Known {
            start_bits,
            end_bits,
        } => {
            if prefix.ytd_payment_bits != start_bits {
                return Err(CollectorError::BoundaryMismatch("customer YTD"));
            }
            end_bits
        }
    };
    *slot = CustomerSlot {
        payment_count: segment.end_version.payment_count,
        delivery_count: segment.end_version.delivery_count,
        balance_bits: segment.end_balance_bits,
        ytd_payment_bits,
    };
    Ok(())
}

fn drain_customer(
    key_index: u32,
    slot: &mut CustomerSlot,
    pending: &mut Vec<PendingSegment>,
) -> Result<(), CollectorError> {
    loop {
        let endpoint = customer_slot_total(*slot)?;
        let Some(position) = pending.iter().position(|entry| {
            matches!(
                entry,
                PendingSegment::Customer {
                    key_index: candidate,
                    segment,
                } if *candidate == key_index && segment.start_total == endpoint
            )
        }) else {
            return Ok(());
        };
        let PendingSegment::Customer { segment, .. } = pending.swap_remove(position) else {
            unreachable!("position matched Customer");
        };
        absorb_customer(slot, segment)?;
    }
}

fn insert_customer_pending(
    key_index: u32,
    mut segment: CustomerSegment,
    pending: &mut Vec<PendingSegment>,
) -> Result<(), CollectorError> {
    loop {
        let mut joined = false;
        let mut position = 0;
        while position < pending.len() {
            let PendingSegment::Customer {
                key_index: candidate,
                segment: existing,
            } = pending[position]
            else {
                position += 1;
                continue;
            };
            if candidate != key_index {
                position += 1;
                continue;
            }
            if existing.end_total == segment.start_total {
                pending.swap_remove(position);
                segment = existing.then(segment)?;
                joined = true;
                break;
            }
            if segment.end_total == existing.start_total {
                pending.swap_remove(position);
                segment = segment.then(existing)?;
                joined = true;
                break;
            }
            if existing.start_total < segment.end_total && segment.start_total < existing.end_total
            {
                return Err(CollectorError::DuplicatePredecessor {
                    domain: "customer",
                    predecessor: segment.start_total,
                });
            }
            position += 1;
        }
        if !joined {
            pending.push(PendingSegment::Customer { key_index, segment });
            return Ok(());
        }
    }
}

fn insert_stock(
    roots: &dyn StockRootProvider,
    key: StockKey,
    key_index: u32,
    slot: &mut StockSlot,
    segment: StockSegment,
    pending: &mut Vec<PendingSegment>,
) -> Result<(), CollectorError> {
    if slot.is_rooted() {
        if segment.start.order_count < slot.order_count {
            return Err(CollectorError::DuplicatePredecessor {
                domain: "stock",
                predecessor: i64::from(segment.start.order_count),
            });
        }
        if segment.start.order_count == slot.order_count {
            absorb_stock(slot, segment)?;
            drain_stock(key_index, slot, pending)?;
            return Ok(());
        }
    } else if segment.start.order_count == 0 {
        validate_stock_root(roots, key, segment.start)?;
        *slot = StockSlot {
            quantity: segment.end.quantity,
            ytd_bits: segment.end.ytd_bits,
            order_count: segment.end.order_count,
            remote_count: segment.end.remote_count,
            initial_quantity: segment.start.quantity,
        };
        drain_stock(key_index, slot, pending)?;
        return Ok(());
    }
    insert_stock_pending(key_index, segment, pending)
}

fn absorb_stock(slot: &mut StockSlot, segment: StockSegment) -> Result<(), CollectorError> {
    let endpoint = StockState {
        quantity: slot.quantity,
        ytd_bits: slot.ytd_bits,
        order_count: slot.order_count,
        remote_count: slot.remote_count,
    };
    if endpoint != segment.start {
        return Err(CollectorError::BoundaryMismatch("stock"));
    }
    slot.quantity = segment.end.quantity;
    slot.ytd_bits = segment.end.ytd_bits;
    slot.order_count = segment.end.order_count;
    slot.remote_count = segment.end.remote_count;
    Ok(())
}

fn drain_stock(
    key_index: u32,
    slot: &mut StockSlot,
    pending: &mut Vec<PendingSegment>,
) -> Result<(), CollectorError> {
    loop {
        let Some(position) = pending.iter().position(|entry| {
            matches!(
                entry,
                PendingSegment::Stock {
                    key_index: candidate,
                    segment,
                } if *candidate == key_index && segment.start.order_count == slot.order_count
            )
        }) else {
            return Ok(());
        };
        let PendingSegment::Stock { segment, .. } = pending.swap_remove(position) else {
            unreachable!("position matched Stock");
        };
        absorb_stock(slot, segment)?;
    }
}

fn insert_stock_pending(
    key_index: u32,
    mut segment: StockSegment,
    pending: &mut Vec<PendingSegment>,
) -> Result<(), CollectorError> {
    loop {
        let mut joined = false;
        let mut position = 0;
        while position < pending.len() {
            let PendingSegment::Stock {
                key_index: candidate,
                segment: existing,
            } = pending[position]
            else {
                position += 1;
                continue;
            };
            if candidate != key_index {
                position += 1;
                continue;
            }
            if existing.end.order_count == segment.start.order_count {
                pending.swap_remove(position);
                segment = existing.then(segment)?;
                joined = true;
                break;
            }
            if segment.end.order_count == existing.start.order_count {
                pending.swap_remove(position);
                segment = segment.then(existing)?;
                joined = true;
                break;
            }
            if existing.start.order_count < segment.end.order_count
                && segment.start.order_count < existing.end.order_count
            {
                return Err(CollectorError::DuplicatePredecessor {
                    domain: "stock",
                    predecessor: i64::from(segment.start.order_count),
                });
            }
            position += 1;
        }
        if !joined {
            pending.push(PendingSegment::Stock { key_index, segment });
            return Ok(());
        }
    }
}

fn validate_stock_root(
    roots: &dyn StockRootProvider,
    key: StockKey,
    actual: StockState,
) -> Result<(), CollectorError> {
    let expected = roots
        .expected_root(key)
        .ok_or(CollectorError::MissingStockRoot(key))?;
    let expected = StockState::from_version(&expected);
    validate_stock_state("setup root", expected)?;
    if expected.ytd_bits != STOCK_INITIAL_YTD_BITS
        || expected.order_count != 0
        || expected.remote_count != 0
    {
        return Err(CollectorError::InvalidStockRoot {
            key,
            reason: "setup root must have +0.0 YTD and zero counters",
        });
    }
    if actual != expected {
        return Err(CollectorError::StockRootMismatch {
            key,
            expected: expected.version(),
            actual: actual.version(),
        });
    }
    Ok(())
}

fn validate_stock_state(position: &'static str, state: StockState) -> Result<(), CollectorError> {
    if !(10..=100).contains(&state.quantity) {
        return Err(CollectorError::InvalidStockEdge(match position {
            "before" => "before quantity is outside 10..=100",
            "after" => "after quantity is outside 10..=100",
            _ => "setup root quantity is outside 10..=100",
        }));
    }
    let ytd = require_finite("stock YTD", state.ytd_bits)?;
    if ytd < 0.0 {
        return Err(CollectorError::InvalidStockEdge(
            "stock YTD must be nonnegative",
        ));
    }
    if state.order_count < 0 {
        return Err(CollectorError::InvalidStockEdge(
            "stock order count must be nonnegative",
        ));
    }
    if state.remote_count < 0 || state.remote_count > state.order_count {
        return Err(CollectorError::InvalidStockEdge(
            "stock remote count must be in 0..=order count",
        ));
    }
    Ok(())
}

fn compose_ytd(left: YtdSpan, right: YtdSpan) -> Result<YtdSpan, CollectorError> {
    match (left, right) {
        (YtdSpan::Identity, YtdSpan::Identity) => Ok(YtdSpan::Identity),
        (YtdSpan::Identity, known @ YtdSpan::Known { .. })
        | (known @ YtdSpan::Known { .. }, YtdSpan::Identity) => Ok(known),
        (
            YtdSpan::Known {
                start_bits,
                end_bits,
            },
            YtdSpan::Known {
                start_bits: next_start,
                end_bits: next_end,
            },
        ) => {
            if end_bits != next_start {
                return Err(CollectorError::BoundaryMismatch("customer YTD"));
            }
            Ok(YtdSpan::Known {
                start_bits,
                end_bits: next_end,
            })
        }
    }
}

fn customer_total(version: CustomerLogicalVersion) -> Result<i64, CollectorError> {
    if version.payment_count < 0 || version.delivery_count < 0 {
        return Err(CollectorError::InvalidCustomerEdge(
            "logical version contains a negative counter",
        ));
    }
    i64::from(version.payment_count)
        .checked_add(i64::from(version.delivery_count))
        .ok_or(CollectorError::Overflow("customer logical version"))
}

fn customer_slot_total(slot: CustomerSlot) -> Result<i64, CollectorError> {
    customer_total(CustomerLogicalVersion {
        payment_count: slot.payment_count,
        delivery_count: slot.delivery_count,
    })
}

fn require_finite(field: &'static str, bits: u32) -> Result<f32, CollectorError> {
    let value = f32::from_bits(bits);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(CollectorError::NonFinite { field, bits })
    }
}

fn sample_rank(seed: u64, domain: u64, key_index: u32) -> u64 {
    mix64(seed ^ domain ^ mix64(u64::from(key_index)))
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn validate_configuration(warehouses: u16, clients: u16) -> Result<(), CollectorError> {
    if warehouses == 0 {
        return Err(CollectorError::InvalidConfiguration(
            "warehouse count must be positive",
        ));
    }
    if clients == 0 {
        return Err(CollectorError::InvalidConfiguration(
            "client count must be positive",
        ));
    }
    Ok(())
}

fn validate_terminal_width(actual: usize) -> Result<(), CollectorError> {
    if actual > MAX_EDGES_PER_TERMINAL {
        return Err(CollectorError::TerminalTooWide {
            actual,
            limit: MAX_EDGES_PER_TERMINAL,
        });
    }
    Ok(())
}

fn validate_pending_limit(actual: usize, limit: usize) -> Result<(), CollectorError> {
    if actual > limit {
        return Err(CollectorError::PendingLimit { actual, limit });
    }
    Ok(())
}

fn customer_index(warehouses: u16, key: CustomerKey) -> Result<u32, CollectorError> {
    if !(1..=i32::from(warehouses)).contains(&key.warehouse_id)
        || !(1..=DISTRICTS_PER_WAREHOUSE as i32).contains(&key.district_id)
        || !(1..=CUSTOMERS_PER_DISTRICT as i32).contains(&key.customer_id)
    {
        return Err(CollectorError::InvalidCustomerKey(key));
    }
    let index = (((key.warehouse_id - 1) as usize * DISTRICTS_PER_WAREHOUSE
        + (key.district_id - 1) as usize)
        * CUSTOMERS_PER_DISTRICT)
        + (key.customer_id - 1) as usize;
    u32::try_from(index).map_err(|_| CollectorError::Overflow("customer key index"))
}

fn stock_index(warehouses: u16, key: StockKey) -> Result<u32, CollectorError> {
    if !(1..=i32::from(warehouses)).contains(&key.warehouse_id)
        || !(1..=ITEM_COUNT as i32).contains(&key.item_id)
    {
        return Err(CollectorError::InvalidStockKey(key));
    }
    let index = (key.warehouse_id - 1) as usize * ITEM_COUNT as usize + (key.item_id - 1) as usize;
    u32::try_from(index).map_err(|_| CollectorError::Overflow("stock key index"))
}

fn customer_key_from_index(warehouses: u16, index: u32) -> CustomerKey {
    let index = index as usize;
    debug_assert!(
        index < usize::from(warehouses) * DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT
    );
    let per_warehouse = DISTRICTS_PER_WAREHOUSE * CUSTOMERS_PER_DISTRICT;
    let warehouse = index / per_warehouse;
    let within_warehouse = index % per_warehouse;
    CustomerKey {
        warehouse_id: warehouse as i32 + 1,
        district_id: (within_warehouse / CUSTOMERS_PER_DISTRICT) as i32 + 1,
        customer_id: (within_warehouse % CUSTOMERS_PER_DISTRICT) as i32 + 1,
    }
}

fn stock_key_from_index(warehouses: u16, index: u32) -> StockKey {
    let index = index as usize;
    debug_assert!(index < usize::from(warehouses) * ITEM_COUNT as usize);
    StockKey {
        warehouse_id: (index / ITEM_COUNT as usize) as i32 + 1,
        item_id: (index % ITEM_COUNT as usize) as i32 + 1,
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CollectorError {
    #[error("invalid collector configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("invalid customer evidence key {0:?}")]
    InvalidCustomerKey(CustomerKey),
    #[error("invalid stock evidence key {0:?}")]
    InvalidStockKey(StockKey),
    #[error("terminal contains {actual} evidence edges; limit is {limit}")]
    TerminalTooWide { actual: usize, limit: usize },
    #[error("invalid customer edge: {0}")]
    InvalidCustomerEdge(&'static str),
    #[error("invalid stock edge: {0}")]
    InvalidStockEdge(&'static str),
    #[error("{0} boundary does not join the rooted version chain")]
    BoundaryMismatch(&'static str),
    #[error("{0} FLOAT32 evidence differs by at least one bit")]
    FloatMismatch(&'static str),
    #[error("{field} is non-finite FLOAT32 0x{bits:08x}")]
    NonFinite { field: &'static str, bits: u32 },
    #[error("{domain} chain has duplicate or stale predecessor {predecessor}")]
    DuplicatePredecessor {
        domain: &'static str,
        predecessor: i64,
    },
    #[error("setup did not provide an initial Stock root for {0:?}")]
    MissingStockRoot(StockKey),
    #[error("invalid setup Stock root for {key:?}: {reason}")]
    InvalidStockRoot { key: StockKey, reason: &'static str },
    #[error(
        "Stock root for {key:?} differs from setup: expected {expected:?}, observed {actual:?}"
    )]
    StockRootMismatch {
        key: StockKey,
        expected: StockVersion,
        actual: StockVersion,
    },
    #[error("collector has {pending} disconnected selected interval(s)")]
    Disconnected { pending: usize },
    #[error("pending interval safety limit exceeded: {actual} > {limit}")]
    PendingLimit { actual: usize, limit: usize },
    #[error("collector counter overflow: {0}")]
    Overflow(&'static str),
    #[error("collector is poisoned by an earlier terminal: {cause}")]
    Poisoned { cause: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SEED: u64 = 0x1234_5678_9abc_def0;

    fn root_for(key: StockKey) -> Option<StockVersion> {
        Some(StockVersion {
            quantity: 10 + key.item_id.rem_euclid(91),
            ytd_bits: STOCK_INITIAL_YTD_BITS,
            order_count: 0,
            remote_count: 0,
        })
    }

    fn collector(clients: u16) -> IntervalCollector {
        IntervalCollector::new(50, clients, TEST_SEED, root_for).unwrap()
    }

    fn customer_version(payment_count: i32, delivery_count: i32) -> CustomerLogicalVersion {
        CustomerLogicalVersion {
            payment_count,
            delivery_count,
        }
    }

    fn payment_edge(
        before_version: CustomerLogicalVersion,
        before_balance: f32,
        before_ytd: f32,
        amount: f32,
    ) -> CustomerUpdateEvidence {
        CustomerUpdateEvidence {
            kind: CustomerUpdateKind::Payment,
            before_version,
            after_version: customer_version(
                before_version.payment_count + 1,
                before_version.delivery_count,
            ),
            amount_bits: amount.to_bits(),
            balance_before_bits: before_balance.to_bits(),
            balance_after_bits: (before_balance - amount).to_bits(),
            ytd_payment_before_bits: Some(before_ytd.to_bits()),
            ytd_payment_after_bits: Some((before_ytd + amount).to_bits()),
        }
    }

    fn delivery_edge(
        before_version: CustomerLogicalVersion,
        before_balance: f32,
        amount: f32,
    ) -> CustomerUpdateEvidence {
        CustomerUpdateEvidence {
            kind: CustomerUpdateKind::Delivery,
            before_version,
            after_version: customer_version(
                before_version.payment_count,
                before_version.delivery_count + 1,
            ),
            amount_bits: amount.to_bits(),
            balance_before_bits: before_balance.to_bits(),
            balance_after_bits: (before_balance + amount).to_bits(),
            ytd_payment_before_bits: None,
            ytd_payment_after_bits: None,
        }
    }

    fn stock_after(before: StockState, quantity: u8, remote: u8) -> StockState {
        let ordered = i32::from(quantity);
        StockState {
            quantity: if before.quantity >= ordered + 10 {
                before.quantity - ordered
            } else {
                before.quantity + 91 - ordered
            },
            ytd_bits: (f32::from_bits(before.ytd_bits) + f32::from(quantity)).to_bits(),
            order_count: before.order_count + 1,
            remote_count: before.remote_count + i32::from(remote),
        }
    }

    fn stock_mutation(
        key: StockKey,
        quantity: u8,
        remote: u8,
        before: StockState,
        after: StockState,
    ) -> StockMutation {
        StockMutation::new(key, quantity, remote, before.version(), after.version())
    }

    fn stock_root(key: StockKey) -> StockState {
        StockState::from_version(&root_for(key).unwrap())
    }

    fn record_stock_roots(collector: &mut IntervalCollector, keys: &[StockKey]) {
        for chunk in keys.chunks(MAX_EDGES_PER_TERMINAL) {
            let updates = chunk
                .iter()
                .map(|key| {
                    let root = stock_root(*key);
                    stock_mutation(*key, 1, 0, root, stock_after(root, 1, 0))
                })
                .collect::<Vec<_>>();
            collector
                .record_terminal(TerminalEvidence::stocks(&updates))
                .unwrap();
        }
    }

    #[test]
    fn sample_buffers_are_small_and_warehouse_independent() {
        assert_eq!(size_of::<CustomerSlot>(), 16);
        assert_eq!(size_of::<StockSlot>(), 20);
        assert_eq!(size_of::<CustomerSample>(), 32);
        assert_eq!(size_of::<StockSample>(), 32);
        assert_eq!(size_of::<PendingSegment>(), 64);
        let bound = IntervalCollector::owned_buffer_capacity_upper_bound_for(50, 32).unwrap();
        assert_eq!(bound, 71_552);
        let collector = collector(32);
        assert_eq!(collector.storage().owned_buffer_capacity_bytes(), bound);
        assert_eq!(collector.storage().pending_limit(), 480);
    }

    #[test]
    fn selected_stock_keys_match_the_bottom_k_oracle() {
        let keys = (1..=1_000)
            .map(|item_id| StockKey {
                warehouse_id: 1,
                item_id,
            })
            .collect::<Vec<_>>();
        let mut collector = collector(32);
        record_stock_roots(&mut collector, &keys);
        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.stock_update_count(), 1_000);
        assert_eq!(sealed.stock_sample_count(), SAMPLE_LIMIT);

        let mut expected = keys
            .iter()
            .map(|key| {
                let index = stock_index(50, *key).unwrap();
                (sample_rank(TEST_SEED, STOCK_SAMPLE_DOMAIN, index), *key)
            })
            .collect::<Vec<_>>();
        expected.sort_unstable();
        expected.truncate(SAMPLE_LIMIT);
        let actual = sealed
            .stocks()
            .map(|chain| (chain.sample_rank(), chain.key()))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        assert_eq!(sealed.sample_seed(), TEST_SEED);
    }

    #[test]
    fn equal_hash_ranks_use_the_canonical_key_tiebreak() {
        let mut forward = Vec::with_capacity(SAMPLE_LIMIT);
        let mut reverse = Vec::with_capacity(SAMPLE_LIMIT);
        let mut forward_pending = Vec::new();
        let mut reverse_pending = Vec::new();
        for key_index in 0..=SAMPLE_LIMIT as u32 {
            let _ = select_stock(&mut forward, &mut forward_pending, key_index, 7);
        }
        for key_index in (0..=SAMPLE_LIMIT as u32).rev() {
            let _ = select_stock(&mut reverse, &mut reverse_pending, key_index, 7);
        }
        let mut forward_keys = forward
            .iter()
            .map(|sample| sample.key_index)
            .collect::<Vec<_>>();
        let mut reverse_keys = reverse
            .iter()
            .map(|sample| sample.key_index)
            .collect::<Vec<_>>();
        forward_keys.sort_unstable();
        reverse_keys.sort_unstable();
        assert_eq!(forward_keys, (0..SAMPLE_LIMIT as u32).collect::<Vec<_>>());
        assert_eq!(reverse_keys, forward_keys);

        let before = reverse_keys;
        let _ = select_stock(&mut reverse, &mut reverse_pending, SAMPLE_LIMIT as u32, 7);
        let mut after = reverse
            .iter()
            .map(|sample| sample.key_index)
            .collect::<Vec<_>>();
        after.sort_unstable();
        assert_eq!(after, before);
    }

    #[test]
    fn evicting_a_selected_key_drops_its_chain_and_pending_interval() {
        let keys = (1..=SAMPLE_LIMIT as i32)
            .map(|item_id| StockKey {
                warehouse_id: 1,
                item_id,
            })
            .collect::<Vec<_>>();
        let mut collector = collector(32);
        record_stock_roots(&mut collector, &keys);

        let worst = collector
            .stocks
            .iter()
            .max_by_key(|sample| (sample.rank, sample.key_index))
            .copied()
            .unwrap();
        let worst_key = stock_key_from_index(50, worst.key_index);
        let initial = stock_root(worst_key);
        let first = stock_after(initial, 1, 0);
        let second = stock_after(first, 1, 0);
        let third = stock_after(second, 1, 0);
        let gap = [stock_mutation(worst_key, 1, 0, second, third)];
        collector
            .record_terminal(TerminalEvidence::stocks(&gap))
            .unwrap();
        assert_eq!(collector.storage().pending_intervals(), 1);

        let replacement = (1_000..=ITEM_COUNT as i32)
            .map(|item_id| StockKey {
                warehouse_id: 1,
                item_id,
            })
            .find(|key| {
                let index = stock_index(50, *key).unwrap();
                (sample_rank(TEST_SEED, STOCK_SAMPLE_DOMAIN, index), index)
                    < (worst.rank, worst.key_index)
                    && !collector
                        .stocks
                        .iter()
                        .any(|sample| sample.key_index == index)
            })
            .unwrap();
        let replacement_root = stock_root(replacement);
        let replacement_after = stock_after(replacement_root, 1, 0);
        let update = [stock_mutation(
            replacement,
            1,
            0,
            replacement_root,
            replacement_after,
        )];
        collector
            .record_terminal(TerminalEvidence::stocks(&update))
            .unwrap();
        assert_eq!(collector.storage().pending_intervals(), 0);
        assert!(!collector
            .stocks
            .iter()
            .any(|sample| sample.key_index == worst.key_index));

        let missing = [stock_mutation(worst_key, 1, 0, first, second)];
        collector
            .record_terminal(TerminalEvidence::stocks(&missing))
            .unwrap();
        assert!(!collector
            .stocks
            .iter()
            .any(|sample| sample.key_index == worst.key_index));
        assert_eq!(collector.storage().pending_intervals(), 0);
    }

    #[test]
    fn kth_failure_rolls_back_a_would_be_reservoir_eviction() {
        let keys = (1..=SAMPLE_LIMIT as i32)
            .map(|item_id| StockKey {
                warehouse_id: 1,
                item_id,
            })
            .collect::<Vec<_>>();
        let mut collector = collector(32);
        record_stock_roots(&mut collector, &keys);
        let before = collector
            .stocks
            .iter()
            .map(|sample| (sample.rank, sample.key_index, sample.slot))
            .collect::<Vec<_>>();
        let worst = before
            .iter()
            .max_by_key(|(rank, key, _)| (*rank, *key))
            .copied()
            .unwrap();
        let replacement = (1_000..=ITEM_COUNT as i32)
            .map(|item_id| StockKey {
                warehouse_id: 1,
                item_id,
            })
            .find(|key| {
                let index = stock_index(50, *key).unwrap();
                (sample_rank(TEST_SEED, STOCK_SAMPLE_DOMAIN, index), index) < (worst.0, worst.1)
            })
            .unwrap();
        let replacement_root = stock_root(replacement);
        let valid = stock_mutation(
            replacement,
            1,
            0,
            replacement_root,
            stock_after(replacement_root, 1, 0),
        );

        let bad_key = StockKey {
            warehouse_id: 1,
            item_id: 99_999,
        };
        let bad_root = stock_root(bad_key);
        let mut bad_after = stock_after(bad_root, 1, 0);
        bad_after.ytd_bits ^= 1;
        let bad = stock_mutation(bad_key, 1, 0, bad_root, bad_after);
        let terminal = [valid, bad];
        assert!(matches!(
            collector.record_terminal(TerminalEvidence::stocks(&terminal)),
            Err(CollectorError::FloatMismatch("stock YTD"))
        ));
        let after = collector
            .stocks
            .iter()
            .map(|sample| (sample.rank, sample.key_index, sample.slot))
            .collect::<Vec<_>>();
        assert_eq!(after, before);
        assert_eq!(collector.stock_updates, SAMPLE_LIMIT as u64);
        assert!(collector.storage().is_poisoned());
    }

    #[test]
    fn thirty_one_full_worker_gaps_are_legal_and_later_root() {
        let mut collector = collector(32);
        let mut roots = Vec::new();
        for worker in 0..31 {
            let mut future = Vec::new();
            let mut root_edges = Vec::new();
            for line in 0..15 {
                let key = StockKey {
                    warehouse_id: 1,
                    item_id: worker * 15 + line + 1,
                };
                let initial = stock_root(key);
                let first = stock_after(initial, 1, 0);
                let second = stock_after(first, 2, 0);
                future.push(stock_mutation(key, 2, 0, first, second));
                root_edges.push(stock_mutation(key, 1, 0, initial, first));
            }
            collector
                .record_terminal(TerminalEvidence::stocks(&future))
                .unwrap();
            roots.push(root_edges);
        }
        assert!(collector.storage().pending_intervals() <= SAMPLE_LIMIT);
        assert_eq!(collector.storage().selected_stock_count(), SAMPLE_LIMIT);
        for terminal in &roots {
            collector
                .record_terminal(TerminalEvidence::stocks(terminal))
                .unwrap();
        }
        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.stock_update_count(), (31 * 15 * 2) as u64);
        assert_eq!(sealed.stock_sample_count(), SAMPLE_LIMIT);
    }

    #[test]
    fn pending_cap_failure_stays_poisoned_after_root_arrives() {
        let key = StockKey {
            warehouse_id: 1,
            item_id: 1,
        };
        let mut versions = vec![stock_root(key)];
        for _ in 0..33 {
            versions.push(stock_after(*versions.last().unwrap(), 1, 0));
        }
        let mut collector = collector(1);
        let first_gaps = (0..15)
            .map(|index| {
                let start = index * 2 + 1;
                stock_mutation(key, 1, 0, versions[start], versions[start + 1])
            })
            .collect::<Vec<_>>();
        collector
            .record_terminal(TerminalEvidence::stocks(&first_gaps))
            .unwrap();
        assert_eq!(collector.storage().pending_intervals(), 15);

        let overflow = [stock_mutation(key, 1, 0, versions[31], versions[32])];
        assert!(matches!(
            collector.record_terminal(TerminalEvidence::stocks(&overflow)),
            Err(CollectorError::PendingLimit {
                actual: 16,
                limit: 15
            })
        ));
        assert_eq!(collector.storage().pending_intervals(), 15);
        assert!(collector.storage().is_poisoned());

        let root = [stock_mutation(key, 1, 0, versions[0], versions[1])];
        assert!(matches!(
            collector.record_terminal(TerminalEvidence::stocks(&root)),
            Err(CollectorError::Poisoned { .. })
        ));
        assert!(matches!(
            collector.seal(),
            Err(CollectorError::Poisoned { .. })
        ));
    }

    #[test]
    fn pending_limit_is_shared_across_customer_and_stock_samples() {
        let stock_key = StockKey {
            warehouse_id: 1,
            item_id: 1,
        };
        let mut stock_versions = vec![stock_root(stock_key)];
        for _ in 0..18 {
            stock_versions.push(stock_after(*stock_versions.last().unwrap(), 1, 0));
        }
        let stock_gaps = (0..8)
            .map(|index| {
                let start = index * 2 + 1;
                stock_mutation(
                    stock_key,
                    1,
                    0,
                    stock_versions[start],
                    stock_versions[start + 1],
                )
            })
            .collect::<Vec<_>>();
        let mut collector = collector(1);
        collector
            .record_terminal(TerminalEvidence::stocks(&stock_gaps))
            .unwrap();

        let customer_key = CustomerKey {
            warehouse_id: 1,
            district_id: 1,
            customer_id: 1,
        };
        let customer_gaps = (0..7)
            .map(|index| {
                let start = index * 2 + 1;
                CustomerMutation::new(
                    customer_key,
                    payment_edge(
                        customer_version(1 + start, 0),
                        -10.0 - start as f32,
                        10.0 + start as f32,
                        1.0,
                    ),
                )
            })
            .collect::<Vec<_>>();
        collector
            .record_terminal(TerminalEvidence::customers(&customer_gaps))
            .unwrap();
        assert_eq!(collector.storage().pending_intervals(), 15);

        let overflow = [CustomerMutation::new(
            customer_key,
            payment_edge(customer_version(16, 0), -25.0, 25.0, 1.0),
        )];
        assert!(matches!(
            collector.record_terminal(TerminalEvidence::customers(&overflow)),
            Err(CollectorError::PendingLimit {
                actual: 16,
                limit: 15
            })
        ));
        assert_eq!(collector.storage().pending_intervals(), 15);
        assert!(collector.storage().is_poisoned());
    }

    #[test]
    fn bad_kth_edge_does_not_publish_a_terminal_prefix() {
        let mut collector = collector(32);
        let mut updates = Vec::new();
        for item_id in 1..=3 {
            let key = StockKey {
                warehouse_id: 1,
                item_id,
            };
            let initial = stock_root(key);
            let mut after = stock_after(initial, 1, 0);
            if item_id == 3 {
                after.ytd_bits ^= 1;
            }
            updates.push(stock_mutation(key, 1, 0, initial, after));
        }
        assert!(matches!(
            collector.record_terminal(TerminalEvidence::stocks(&updates)),
            Err(CollectorError::FloatMismatch("stock YTD"))
        ));
        assert_eq!(collector.storage().selected_stock_count(), 0);
        assert_eq!(collector.storage().pending_intervals(), 0);
        assert!(collector.storage().is_poisoned());
    }

    #[test]
    fn one_million_successors_merge_into_one_pending_interval() {
        let mut collector = collector(32);
        let key = StockKey {
            warehouse_id: 1,
            item_id: 1,
        };
        let initial = stock_root(key);
        let mut before = stock_after(initial, 1, 0);
        for index in 0..1_000_000 {
            let quantity = (index % 10 + 1) as u8;
            let remote = (index % 11 == 0) as u8;
            let after = stock_after(before, quantity, remote);
            let update = [stock_mutation(key, quantity, remote, before, after)];
            collector
                .record_terminal(TerminalEvidence::stocks(&update))
                .unwrap();
            before = after;
        }
        assert_eq!(collector.storage().pending_intervals(), 1);
        let first = stock_after(initial, 1, 0);
        let root = [stock_mutation(key, 1, 0, initial, first)];
        collector
            .record_terminal(TerminalEvidence::stocks(&root))
            .unwrap();
        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.stock_update_count(), 1_000_001);
        assert_eq!(sealed.stocks().next().unwrap().update_count(), 1_000_001);
    }

    #[test]
    fn float_self_loop_is_valid_but_signed_zero_substitution_is_not() {
        let key = CustomerKey {
            warehouse_id: 1,
            district_id: 1,
            customer_id: 1,
        };
        let huge = 1_073_741_824.0_f32;
        let delivered_balance = -10.0_f32 + huge;
        assert_eq!(
            (delivered_balance - 1.0).to_bits(),
            delivered_balance.to_bits()
        );
        let updates = [
            CustomerMutation::new(key, delivery_edge(customer_version(1, 0), -10.0, huge)),
            CustomerMutation::new(
                key,
                payment_edge(customer_version(1, 1), delivered_balance, 10.0, 1.0),
            ),
        ];
        let mut valid = collector(32);
        valid
            .record_terminal(TerminalEvidence::customers(&updates))
            .unwrap();
        assert_eq!(
            valid
                .seal()
                .unwrap()
                .customer(key)
                .unwrap()
                .unwrap()
                .endpoint()
                .balance_bits,
            delivered_balance.to_bits()
        );

        let mut wrong_zero = delivery_edge(customer_version(1, 0), -1.0, 1.0);
        wrong_zero.balance_after_bits = (-0.0_f32).to_bits();
        let update = [CustomerMutation::new(key, wrong_zero)];
        let mut invalid = collector(32);
        assert!(matches!(
            invalid.record_terminal(TerminalEvidence::customers(&update)),
            Err(CollectorError::FloatMismatch("customer balance"))
        ));
    }

    #[test]
    fn unselected_edges_are_still_validated_and_stock_roots_are_exact() {
        let mut collector = collector(32);
        let keys = (1..=SAMPLE_LIMIT as i32)
            .map(|item_id| StockKey {
                warehouse_id: 1,
                item_id,
            })
            .collect::<Vec<_>>();
        record_stock_roots(&mut collector, &keys);

        let cutoff = collector
            .stocks
            .iter()
            .map(|sample| (sample.rank, sample.key_index))
            .max()
            .unwrap();
        let key = (1_000..=ITEM_COUNT as i32)
            .map(|item_id| StockKey {
                warehouse_id: 1,
                item_id,
            })
            .find(|key| {
                let index = stock_index(50, *key).unwrap();
                (sample_rank(TEST_SEED, STOCK_SAMPLE_DOMAIN, index), index) >= cutoff
            })
            .unwrap();
        let expected = stock_root(key);
        let forged = StockState {
            ytd_bits: (-0.0_f32).to_bits(),
            ..expected
        };
        let update = [stock_mutation(key, 1, 0, forged, stock_after(forged, 1, 0))];
        assert!(matches!(
            collector.record_terminal(TerminalEvidence::stocks(&update)),
            Err(CollectorError::StockRootMismatch { .. })
        ));
    }

    #[test]
    fn seal_moves_sample_storage_without_copying() {
        let key = StockKey {
            warehouse_id: 1,
            item_id: 42,
        };
        let root = stock_root(key);
        let after = stock_after(root, 3, 1);
        let update = [stock_mutation(key, 3, 1, root, after)];
        let mut collector = collector(32);
        collector
            .record_terminal(TerminalEvidence::stocks(&update))
            .unwrap();
        let customer_pointer = collector.customers.as_ptr();
        let stock_pointer = collector.stocks.as_ptr();
        let sealed = collector.seal().unwrap();
        assert_eq!(customer_pointer, sealed.customers.as_ptr());
        assert_eq!(stock_pointer, sealed.stocks.as_ptr());
        assert_eq!(sealed.customer_sample_count(), 0);
        let chain = sealed.stock(key).unwrap().unwrap();
        assert_eq!(chain.initial(), root.version());
        assert_eq!(chain.endpoint(), after.version());
    }
}
