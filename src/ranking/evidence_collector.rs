//! Bounded, order-independent validation for committed row-version evidence.
//!
//! Ranked responses from different clients need not arrive in database commit
//! order.  Customer and Stock rows expose monotonic logical counters, so the
//! collector joins disjoint version intervals instead of retaining every
//! committed event.  A correct run keeps one rooted prefix per touched key and
//! at most a client-bounded number of future intervals while responses drain.

use std::collections::BTreeMap;

use thiserror::Error;

use crate::consistency::{
    CustomerLogicalVersion, CustomerUpdateEndpoint, CustomerUpdateEvidence, CustomerUpdateKind,
};
use crate::profile::ITEM_COUNT;

use super::runner::StockVersion;

const CUSTOMERS_PER_DISTRICT: i32 = 3_000;
const DISTRICTS_PER_WAREHOUSE: i32 = 10;
const MAX_LINES_PER_NEW_ORDER: usize = 15;
const MAX_CUSTOMERS_PER_DELIVERY: usize = 10;
const CUSTOMER_INITIAL_BALANCE_BITS: u32 = (-10.0_f32).to_bits();
const CUSTOMER_INITIAL_YTD_BITS: u32 = 10.0_f32.to_bits();
const CUSTOMER_INITIAL_PAYMENT_COUNT: i32 = 1;
const CUSTOMER_INITIAL_DELIVERY_COUNT: i32 = 0;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedCustomerChain {
    pub key: CustomerKey,
    pub endpoint: CustomerUpdateEndpoint,
    pub payment_updates: u64,
    pub delivery_updates: u64,
    /// Order-independent binding for audit and canonical persistence.
    ///
    /// Correctness does not rely on this digest.  The rooted interval gate has
    /// already rejected duplicate, overlapping, and disconnected versions.
    pub audit_digest: [u64; 4],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedStockChain {
    pub key: StockKey,
    pub initial: StockVersion,
    pub endpoint: StockVersion,
    pub update_count: u64,
    /// Order-independent binding for audit and canonical persistence.
    pub audit_digest: [u64; 4],
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SealedIntervalEvidence {
    pub customers: Vec<SealedCustomerChain>,
    pub stocks: Vec<SealedStockChain>,
}

impl SealedIntervalEvidence {
    pub fn customer_update_count(&self) -> Result<u64, CollectorError> {
        self.customers.iter().try_fold(0_u64, |total, chain| {
            checked_add_u64(
                total,
                checked_add_u64(
                    chain.payment_updates,
                    chain.delivery_updates,
                    "customer update count",
                )?,
                "customer update count",
            )
        })
    }

    pub fn stock_update_count(&self) -> Result<u64, CollectorError> {
        self.stocks.iter().try_fold(0_u64, |total, chain| {
            checked_add_u64(total, chain.update_count, "stock update count")
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectorStorage {
    pub customer_keys: usize,
    pub stock_keys: usize,
    pub pending_intervals: usize,
}

/// One collector is shared by all ranked workers.
///
/// `pending_limit` is derived from the number of workers and the maximum
/// number of Stock and Customer rows one in-flight NewOrder and Delivery can
/// expose.  It is a safety bound for response reordering, not a
/// transaction-count bound.
#[derive(Clone, Debug)]
pub struct IntervalCollector {
    warehouses: i32,
    pending_limit: usize,
    audit_seed: u64,
    customers: BTreeMap<CustomerKey, CustomerChain>,
    stocks: BTreeMap<StockKey, StockChain>,
    pending_intervals: usize,
}

impl IntervalCollector {
    pub fn new(warehouses: u16, clients: u16, audit_seed: u64) -> Result<Self, CollectorError> {
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
        let pending_limit = usize::from(clients)
            .checked_mul(MAX_LINES_PER_NEW_ORDER + MAX_CUSTOMERS_PER_DELIVERY)
            .ok_or(CollectorError::Overflow("pending interval limit"))?;
        Ok(Self {
            warehouses: i32::from(warehouses),
            pending_limit,
            audit_seed,
            customers: BTreeMap::new(),
            stocks: BTreeMap::new(),
            pending_intervals: 0,
        })
    }

    pub fn record_customer(
        &mut self,
        key: CustomerKey,
        update: CustomerUpdateEvidence,
    ) -> Result<(), CollectorError> {
        self.validate_customer_key(key)?;
        let segment = CustomerSegment::single(self.audit_seed, key, update)?;
        let chain = self.customers.entry(key).or_default();
        let before = chain.pending.len();
        chain.insert(segment)?;
        let after = chain.pending.len();
        self.update_pending_count(before, after)
    }

    pub fn record_stock(
        &mut self,
        key: StockKey,
        ordered_quantity: u8,
        remote_increment: u8,
        before: StockVersion,
        after: StockVersion,
    ) -> Result<(), CollectorError> {
        self.validate_stock_key(key)?;
        let segment = StockSegment::single(
            self.audit_seed,
            key,
            ordered_quantity,
            remote_increment,
            before,
            after,
        )?;
        let chain = self.stocks.entry(key).or_default();
        let pending_before = chain.pending.len();
        chain.insert(segment)?;
        let pending_after = chain.pending.len();
        self.update_pending_count(pending_before, pending_after)
    }

    pub fn storage(&self) -> CollectorStorage {
        CollectorStorage {
            customer_keys: self.customers.len(),
            stock_keys: self.stocks.len(),
            pending_intervals: self.pending_intervals,
        }
    }

    pub fn seal(self) -> Result<SealedIntervalEvidence, CollectorError> {
        if self.pending_intervals != 0 {
            return Err(CollectorError::Disconnected {
                domain: "collector",
                key: "all keys".to_owned(),
                pending: self.pending_intervals,
            });
        }

        let customers = self
            .customers
            .into_iter()
            .map(|(key, chain)| chain.seal(key))
            .collect::<Result<Vec<_>, _>>()?;
        let stocks = self
            .stocks
            .into_iter()
            .map(|(key, chain)| chain.seal(key))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SealedIntervalEvidence { customers, stocks })
    }

    fn update_pending_count(&mut self, before: usize, after: usize) -> Result<(), CollectorError> {
        if after >= before {
            self.pending_intervals = self
                .pending_intervals
                .checked_add(after - before)
                .ok_or(CollectorError::Overflow("pending interval count"))?;
        } else {
            self.pending_intervals = self
                .pending_intervals
                .checked_sub(before - after)
                .ok_or(CollectorError::Overflow("pending interval count"))?;
        }
        if self.pending_intervals > self.pending_limit {
            return Err(CollectorError::PendingLimit {
                actual: self.pending_intervals,
                limit: self.pending_limit,
            });
        }
        Ok(())
    }

    fn validate_customer_key(&self, key: CustomerKey) -> Result<(), CollectorError> {
        if !(1..=self.warehouses).contains(&key.warehouse_id)
            || !(1..=DISTRICTS_PER_WAREHOUSE).contains(&key.district_id)
            || !(1..=CUSTOMERS_PER_DISTRICT).contains(&key.customer_id)
        {
            return Err(CollectorError::InvalidKey(format!(
                "customer ({},{},{})",
                key.warehouse_id, key.district_id, key.customer_id
            )));
        }
        Ok(())
    }

    fn validate_stock_key(&self, key: StockKey) -> Result<(), CollectorError> {
        if !(1..=self.warehouses).contains(&key.warehouse_id)
            || !(1..=ITEM_COUNT as i32).contains(&key.item_id)
        {
            return Err(CollectorError::InvalidKey(format!(
                "stock ({},{})",
                key.warehouse_id, key.item_id
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum YtdSpan {
    Identity,
    Known { start_bits: u32, end_bits: u32 },
}

#[derive(Clone, Debug)]
struct CustomerSegment {
    start_total: i64,
    end_total: i64,
    start_version: CustomerLogicalVersion,
    end_version: CustomerLogicalVersion,
    start_balance_bits: u32,
    end_balance_bits: u32,
    ytd: YtdSpan,
    payment_updates: u64,
    delivery_updates: u64,
    digest: [u64; 4],
}

impl CustomerSegment {
    fn single(
        audit_seed: u64,
        key: CustomerKey,
        update: CustomerUpdateEvidence,
    ) -> Result<Self, CollectorError> {
        let start_total = customer_total(update.before_version)?;
        let end_total = customer_total(update.after_version)?;
        if end_total != start_total + 1 {
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

        require_finite("customer amount", update.amount_bits)?;
        let amount = f32::from_bits(update.amount_bits);
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
        if canonical_zero(expected_balance.to_bits()) != canonical_zero(update.balance_after_bits) {
            return Err(CollectorError::FloatMismatch("customer balance"));
        }

        let (ytd, payment_updates, delivery_updates) = match (
            update.kind,
            update.ytd_payment_before_bits,
            update.ytd_payment_after_bits,
        ) {
            (CustomerUpdateKind::Payment, Some(before_bits), Some(after_bits)) => {
                let before = require_finite("customer YTD before", before_bits)?;
                require_finite("customer YTD after", after_bits)?;
                if canonical_zero((before + amount).to_bits()) != canonical_zero(after_bits) {
                    return Err(CollectorError::FloatMismatch("customer YTD"));
                }
                (
                    YtdSpan::Known {
                        start_bits: canonical_zero(before_bits),
                        end_bits: canonical_zero(after_bits),
                    },
                    1,
                    0,
                )
            }
            (CustomerUpdateKind::Payment, _, _) => {
                return Err(CollectorError::InvalidCustomerEdge(
                    "Payment omitted YTD evidence",
                ));
            }
            (CustomerUpdateKind::Delivery, None, None) => (YtdSpan::Identity, 0, 1),
            (CustomerUpdateKind::Delivery, _, _) => {
                return Err(CollectorError::InvalidCustomerEdge(
                    "Delivery supplied Payment-only YTD evidence",
                ));
            }
        };

        Ok(Self {
            start_total,
            end_total,
            start_version: update.before_version,
            end_version: update.after_version,
            start_balance_bits: canonical_zero(update.balance_before_bits),
            end_balance_bits: canonical_zero(update.balance_after_bits),
            ytd,
            payment_updates,
            delivery_updates,
            digest: audit_digest(
                audit_seed,
                0x4355_5354_4f4d_4552,
                &[
                    key.warehouse_id as u64,
                    key.district_id as u64,
                    key.customer_id as u64,
                    update.before_version.payment_count as u64,
                    update.before_version.delivery_count as u64,
                    update.after_version.payment_count as u64,
                    update.after_version.delivery_count as u64,
                    u64::from(update.amount_bits),
                    u64::from(update.balance_before_bits),
                    u64::from(update.balance_after_bits),
                    u64::from(update.ytd_payment_before_bits.unwrap_or_default()),
                    u64::from(update.ytd_payment_after_bits.unwrap_or_default()),
                ],
            ),
        })
    }

    fn then(self, next: Self) -> Result<Self, CollectorError> {
        if self.end_total != next.start_total
            || self.end_version != next.start_version
            || self.end_balance_bits != next.start_balance_bits
        {
            return Err(CollectorError::BoundaryMismatch("customer"));
        }
        let ytd = compose_ytd(self.ytd, next.ytd)?;
        Ok(Self {
            start_total: self.start_total,
            end_total: next.end_total,
            start_version: self.start_version,
            end_version: next.end_version,
            start_balance_bits: self.start_balance_bits,
            end_balance_bits: next.end_balance_bits,
            ytd,
            payment_updates: checked_add_u64(
                self.payment_updates,
                next.payment_updates,
                "customer payment updates",
            )?,
            delivery_updates: checked_add_u64(
                self.delivery_updates,
                next.delivery_updates,
                "customer delivery updates",
            )?,
            digest: add_digest(self.digest, next.digest),
        })
    }
}

#[derive(Clone, Debug)]
struct CustomerPrefix {
    endpoint: CustomerUpdateEndpoint,
    payment_updates: u64,
    delivery_updates: u64,
    digest: [u64; 4],
}

impl Default for CustomerPrefix {
    fn default() -> Self {
        Self {
            endpoint: CustomerUpdateEndpoint {
                version: CustomerLogicalVersion {
                    payment_count: CUSTOMER_INITIAL_PAYMENT_COUNT,
                    delivery_count: CUSTOMER_INITIAL_DELIVERY_COUNT,
                },
                balance_bits: CUSTOMER_INITIAL_BALANCE_BITS,
                ytd_payment_bits: CUSTOMER_INITIAL_YTD_BITS,
            },
            payment_updates: 0,
            delivery_updates: 0,
            digest: [0; 4],
        }
    }
}

#[derive(Clone, Debug, Default)]
struct CustomerChain {
    prefix: CustomerPrefix,
    pending: BTreeMap<i64, CustomerSegment>,
}

impl CustomerChain {
    fn insert(&mut self, segment: CustomerSegment) -> Result<(), CollectorError> {
        let prefix_end = customer_total(self.prefix.endpoint.version)?;
        if segment.start_total < prefix_end {
            return Err(CollectorError::DuplicatePredecessor {
                domain: "customer",
                predecessor: segment.start_total,
            });
        }
        if segment.start_total == prefix_end {
            self.absorb(segment)?;
            self.drain_pending()?;
            return Ok(());
        }
        insert_customer_pending(&mut self.pending, segment)
    }

    fn absorb(&mut self, segment: CustomerSegment) -> Result<(), CollectorError> {
        if self.prefix.endpoint.version != segment.start_version
            || canonical_zero(self.prefix.endpoint.balance_bits) != segment.start_balance_bits
        {
            return Err(CollectorError::BoundaryMismatch("customer"));
        }
        let ytd_payment_bits = match segment.ytd {
            YtdSpan::Identity => self.prefix.endpoint.ytd_payment_bits,
            YtdSpan::Known {
                start_bits,
                end_bits,
            } => {
                if canonical_zero(self.prefix.endpoint.ytd_payment_bits) != start_bits {
                    return Err(CollectorError::BoundaryMismatch("customer YTD"));
                }
                end_bits
            }
        };
        self.prefix.endpoint = CustomerUpdateEndpoint {
            version: segment.end_version,
            balance_bits: segment.end_balance_bits,
            ytd_payment_bits,
        };
        self.prefix.payment_updates = checked_add_u64(
            self.prefix.payment_updates,
            segment.payment_updates,
            "customer payment updates",
        )?;
        self.prefix.delivery_updates = checked_add_u64(
            self.prefix.delivery_updates,
            segment.delivery_updates,
            "customer delivery updates",
        )?;
        self.prefix.digest = add_digest(self.prefix.digest, segment.digest);
        Ok(())
    }

    fn drain_pending(&mut self) -> Result<(), CollectorError> {
        loop {
            let endpoint = customer_total(self.prefix.endpoint.version)?;
            let Some(segment) = self.pending.remove(&endpoint) else {
                break;
            };
            self.absorb(segment)?;
        }
        Ok(())
    }

    fn seal(self, key: CustomerKey) -> Result<SealedCustomerChain, CollectorError> {
        if !self.pending.is_empty() {
            return Err(CollectorError::Disconnected {
                domain: "customer",
                key: format!(
                    "({},{},{})",
                    key.warehouse_id, key.district_id, key.customer_id
                ),
                pending: self.pending.len(),
            });
        }
        Ok(SealedCustomerChain {
            key,
            endpoint: self.prefix.endpoint,
            payment_updates: self.prefix.payment_updates,
            delivery_updates: self.prefix.delivery_updates,
            audit_digest: self.prefix.digest,
        })
    }
}

fn insert_customer_pending(
    pending: &mut BTreeMap<i64, CustomerSegment>,
    mut segment: CustomerSegment,
) -> Result<(), CollectorError> {
    let predecessor = pending
        .range(..=segment.start_total)
        .next_back()
        .map(|(start, candidate)| (*start, candidate.clone()));
    if let Some((start, previous)) = predecessor {
        if previous.end_total > segment.start_total {
            return Err(CollectorError::DuplicatePredecessor {
                domain: "customer",
                predecessor: segment.start_total,
            });
        }
        if previous.end_total == segment.start_total {
            pending.remove(&start);
            segment = previous.then(segment)?;
        }
    }

    let successor = pending
        .range(segment.start_total..)
        .next()
        .map(|(start, candidate)| (*start, candidate.clone()));
    if let Some((start, next)) = successor {
        if start < segment.end_total {
            return Err(CollectorError::DuplicatePredecessor {
                domain: "customer",
                predecessor: start,
            });
        }
        if start == segment.end_total {
            pending.remove(&start);
            segment = segment.then(next)?;
        }
    }
    if pending.insert(segment.start_total, segment).is_some() {
        return Err(CollectorError::DuplicatePredecessor {
            domain: "customer",
            predecessor: 0,
        });
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct StockSegment {
    start_version: i64,
    end_version: i64,
    start: StockVersion,
    end: StockVersion,
    update_count: u64,
    digest: [u64; 4],
}

impl StockSegment {
    fn single(
        audit_seed: u64,
        key: StockKey,
        ordered_quantity: u8,
        remote_increment: u8,
        before: StockVersion,
        after: StockVersion,
    ) -> Result<Self, CollectorError> {
        if !(1..=10).contains(&ordered_quantity) {
            return Err(CollectorError::InvalidStockEdge(
                "ordered quantity is outside 1..=10",
            ));
        }
        if remote_increment > 1 {
            return Err(CollectorError::InvalidStockEdge(
                "remote increment is outside 0..=1",
            ));
        }
        for (field, value) in [
            ("before order count", before.order_count),
            ("before remote count", before.remote_count),
            ("after order count", after.order_count),
            ("after remote count", after.remote_count),
        ] {
            if value < 0 {
                return Err(CollectorError::InvalidStockEdge(field));
            }
        }
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
                .checked_add(i32::from(remote_increment))
                .ok_or(CollectorError::Overflow("stock remote count"))?
        {
            return Err(CollectorError::InvalidStockEdge(
                "remote count transition is wrong",
            ));
        }
        if !(10..=100).contains(&before.quantity) || !(10..=100).contains(&after.quantity) {
            return Err(CollectorError::InvalidStockEdge(
                "quantity is outside 10..=100",
            ));
        }
        let quantity = i32::from(ordered_quantity);
        let expected_quantity = if before.quantity >= quantity + 10 {
            before.quantity - quantity
        } else {
            before.quantity + 91 - quantity
        };
        if after.quantity != expected_quantity {
            return Err(CollectorError::InvalidStockEdge(
                "quantity transition is wrong",
            ));
        }
        let before_ytd = require_finite("stock YTD before", before.ytd_bits)?;
        require_finite("stock YTD after", after.ytd_bits)?;
        if canonical_zero((before_ytd + f32::from(ordered_quantity)).to_bits())
            != canonical_zero(after.ytd_bits)
        {
            return Err(CollectorError::FloatMismatch("stock YTD"));
        }

        let start_version = i64::from(before.order_count);
        let end_version = i64::from(after.order_count);
        Ok(Self {
            start_version,
            end_version,
            start: canonical_stock(before),
            end: canonical_stock(after.clone()),
            update_count: 1,
            digest: audit_digest(
                audit_seed,
                0x5354_4f43_4b5f_4544,
                &[
                    key.warehouse_id as u64,
                    key.item_id as u64,
                    u64::from(ordered_quantity),
                    u64::from(remote_increment),
                    after.quantity as u64,
                    u64::from(after.ytd_bits),
                    after.order_count as u64,
                    after.remote_count as u64,
                ],
            ),
        })
    }

    fn then(self, next: Self) -> Result<Self, CollectorError> {
        if self.end_version != next.start_version || self.end != next.start {
            return Err(CollectorError::BoundaryMismatch("stock"));
        }
        Ok(Self {
            start_version: self.start_version,
            end_version: next.end_version,
            start: self.start,
            end: next.end,
            update_count: checked_add_u64(
                self.update_count,
                next.update_count,
                "stock update count",
            )?,
            digest: add_digest(self.digest, next.digest),
        })
    }
}

#[derive(Clone, Debug)]
struct StockPrefix {
    initial: StockVersion,
    endpoint: StockVersion,
    update_count: u64,
    digest: [u64; 4],
}

#[derive(Clone, Debug, Default)]
struct StockChain {
    prefix: Option<StockPrefix>,
    pending: BTreeMap<i64, StockSegment>,
}

impl StockChain {
    fn insert(&mut self, segment: StockSegment) -> Result<(), CollectorError> {
        match &self.prefix {
            Some(prefix) => {
                let prefix_end = i64::from(prefix.endpoint.order_count);
                if segment.start_version < prefix_end {
                    return Err(CollectorError::DuplicatePredecessor {
                        domain: "stock",
                        predecessor: segment.start_version,
                    });
                }
                if segment.start_version == prefix_end {
                    self.absorb(segment)?;
                    self.drain_pending()?;
                    return Ok(());
                }
            }
            None if segment.start_version == 0 => {
                self.prefix = Some(StockPrefix {
                    initial: segment.start.clone(),
                    endpoint: segment.end,
                    update_count: segment.update_count,
                    digest: segment.digest,
                });
                self.drain_pending()?;
                return Ok(());
            }
            None => {}
        }
        insert_stock_pending(&mut self.pending, segment)
    }

    fn absorb(&mut self, segment: StockSegment) -> Result<(), CollectorError> {
        let prefix = self
            .prefix
            .as_mut()
            .ok_or(CollectorError::BoundaryMismatch("stock root"))?;
        if prefix.endpoint != segment.start {
            return Err(CollectorError::BoundaryMismatch("stock"));
        }
        prefix.endpoint = segment.end;
        prefix.update_count = checked_add_u64(
            prefix.update_count,
            segment.update_count,
            "stock update count",
        )?;
        prefix.digest = add_digest(prefix.digest, segment.digest);
        Ok(())
    }

    fn drain_pending(&mut self) -> Result<(), CollectorError> {
        loop {
            let Some(endpoint) = self
                .prefix
                .as_ref()
                .map(|prefix| i64::from(prefix.endpoint.order_count))
            else {
                break;
            };
            let Some(segment) = self.pending.remove(&endpoint) else {
                break;
            };
            self.absorb(segment)?;
        }
        Ok(())
    }

    fn seal(self, key: StockKey) -> Result<SealedStockChain, CollectorError> {
        if !self.pending.is_empty() {
            return Err(CollectorError::Disconnected {
                domain: "stock",
                key: format!("({},{})", key.warehouse_id, key.item_id),
                pending: self.pending.len(),
            });
        }
        let prefix = self.prefix.ok_or_else(|| CollectorError::Disconnected {
            domain: "stock",
            key: format!("({},{})", key.warehouse_id, key.item_id),
            pending: 0,
        })?;
        Ok(SealedStockChain {
            key,
            initial: prefix.initial,
            endpoint: prefix.endpoint,
            update_count: prefix.update_count,
            audit_digest: prefix.digest,
        })
    }
}

fn insert_stock_pending(
    pending: &mut BTreeMap<i64, StockSegment>,
    mut segment: StockSegment,
) -> Result<(), CollectorError> {
    let predecessor = pending
        .range(..=segment.start_version)
        .next_back()
        .map(|(start, candidate)| (*start, candidate.clone()));
    if let Some((start, previous)) = predecessor {
        if previous.end_version > segment.start_version {
            return Err(CollectorError::DuplicatePredecessor {
                domain: "stock",
                predecessor: segment.start_version,
            });
        }
        if previous.end_version == segment.start_version {
            pending.remove(&start);
            segment = previous.then(segment)?;
        }
    }
    let successor = pending
        .range(segment.start_version..)
        .next()
        .map(|(start, candidate)| (*start, candidate.clone()));
    if let Some((start, next)) = successor {
        if start < segment.end_version {
            return Err(CollectorError::DuplicatePredecessor {
                domain: "stock",
                predecessor: start,
            });
        }
        if start == segment.end_version {
            pending.remove(&start);
            segment = segment.then(next)?;
        }
    }
    if pending.insert(segment.start_version, segment).is_some() {
        return Err(CollectorError::DuplicatePredecessor {
            domain: "stock",
            predecessor: 0,
        });
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

fn canonical_stock(mut version: StockVersion) -> StockVersion {
    version.ytd_bits = canonical_zero(version.ytd_bits);
    version
}

fn canonical_zero(bits: u32) -> u32 {
    if bits & 0x7fff_ffff == 0 {
        0
    } else {
        bits
    }
}

fn require_finite(field: &'static str, bits: u32) -> Result<f32, CollectorError> {
    let value = f32::from_bits(bits);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(CollectorError::NonFinite { field, bits })
    }
}

fn checked_add_u64(left: u64, right: u64, field: &'static str) -> Result<u64, CollectorError> {
    left.checked_add(right)
        .ok_or(CollectorError::Overflow(field))
}

fn add_digest(left: [u64; 4], right: [u64; 4]) -> [u64; 4] {
    std::array::from_fn(|index| left[index].wrapping_add(right[index]))
}

fn audit_digest(seed: u64, domain: u64, values: &[u64]) -> [u64; 4] {
    std::array::from_fn(|lane| {
        let mut state = mix64(seed ^ domain ^ (lane as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        for value in values {
            state = mix64(state ^ mix64(*value));
        }
        state
    })
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum CollectorError {
    #[error("invalid collector configuration: {0}")]
    InvalidConfiguration(&'static str),

    #[error("invalid evidence key: {0}")]
    InvalidKey(String),

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

    #[error("{domain} chain {key} has {pending} disconnected interval(s)")]
    Disconnected {
        domain: &'static str,
        key: String,
        pending: usize,
    },

    #[error("pending interval safety limit exceeded: {actual} > {limit}")]
    PendingLimit { actual: usize, limit: usize },

    #[error("collector counter overflow: {0}")]
    Overflow(&'static str),
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::SeedableRng;

    use super::*;

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

    fn stock_after(
        before: &StockVersion,
        ordered_quantity: u8,
        remote_increment: u8,
    ) -> StockVersion {
        let quantity = i32::from(ordered_quantity);
        StockVersion {
            quantity: if before.quantity >= quantity + 10 {
                before.quantity - quantity
            } else {
                before.quantity + 91 - quantity
            },
            ytd_bits: (f32::from_bits(before.ytd_bits) + f32::from(ordered_quantity)).to_bits(),
            order_count: before.order_count + 1,
            remote_count: before.remote_count + i32::from(remote_increment),
        }
    }

    #[test]
    fn customer_intervals_join_in_arbitrary_response_order() {
        let key = CustomerKey {
            warehouse_id: 1,
            district_id: 1,
            customer_id: 7,
        };
        let payment = payment_edge(customer_version(1, 0), -10.0, 10.0, 2.0);
        let delivery = delivery_edge(customer_version(2, 0), -12.0, 5.0);
        let later_payment = payment_edge(customer_version(2, 1), -7.0, 12.0, 3.0);

        let mut ordered = IntervalCollector::new(50, 32, 91).unwrap();
        for edge in [payment, delivery, later_payment] {
            ordered.record_customer(key, edge).unwrap();
        }
        let expected = ordered.seal().unwrap();

        let mut shuffled = IntervalCollector::new(50, 32, 91).unwrap();
        for edge in [later_payment, delivery, payment] {
            shuffled.record_customer(key, edge).unwrap();
        }
        assert_eq!(shuffled.seal().unwrap(), expected);
        assert_eq!(expected.customer_update_count().unwrap(), 3);
        assert_eq!(
            expected.customers[0].endpoint,
            CustomerUpdateEndpoint {
                version: customer_version(3, 1),
                balance_bits: (-10.0_f32).to_bits(),
                ytd_payment_bits: 15.0_f32.to_bits(),
            }
        );
    }

    #[test]
    fn customer_rejects_duplicate_fork_disconnect_and_one_bit_float() {
        let key = CustomerKey {
            warehouse_id: 1,
            district_id: 1,
            customer_id: 1,
        };
        let first = payment_edge(customer_version(1, 0), -10.0, 10.0, 2.0);

        let mut duplicate = IntervalCollector::new(50, 32, 1).unwrap();
        duplicate.record_customer(key, first).unwrap();
        assert!(matches!(
            duplicate.record_customer(key, first),
            Err(CollectorError::DuplicatePredecessor {
                domain: "customer",
                ..
            })
        ));

        let mut fork = IntervalCollector::new(50, 32, 1).unwrap();
        fork.record_customer(key, first).unwrap();
        let stale = delivery_edge(customer_version(1, 0), -10.0, 1.0);
        assert!(matches!(
            fork.record_customer(key, stale),
            Err(CollectorError::DuplicatePredecessor {
                domain: "customer",
                ..
            })
        ));

        let mut disconnected = IntervalCollector::new(50, 32, 1).unwrap();
        disconnected
            .record_customer(key, payment_edge(customer_version(3, 0), -14.0, 14.0, 1.0))
            .unwrap();
        assert!(matches!(
            disconnected.seal(),
            Err(CollectorError::Disconnected {
                domain: "collector",
                ..
            })
        ));

        let mut wrong = payment_edge(customer_version(1, 0), -10.0, 10.0, 2.0);
        *wrong.ytd_payment_after_bits.as_mut().unwrap() ^= 1;
        let mut one_bit = IntervalCollector::new(50, 32, 1).unwrap();
        assert!(matches!(
            one_bit.record_customer(key, wrong),
            Err(CollectorError::FloatMismatch("customer YTD"))
        ));
    }

    #[test]
    fn stock_intervals_join_and_reject_stale_or_corrupt_edges() {
        let key = StockKey {
            warehouse_id: 2,
            item_id: 17,
        };
        let first = StockVersion {
            quantity: 20,
            ytd_bits: 0.0_f32.to_bits(),
            order_count: 0,
            remote_count: 0,
        };
        let second = stock_after(&first, 4, 0);
        let third = stock_after(&second, 8, 1);
        let fourth = stock_after(&third, 3, 0);

        let mut collector = IntervalCollector::new(50, 32, 44).unwrap();
        collector
            .record_stock(key, 3, 0, third.clone(), fourth.clone())
            .unwrap();
        collector
            .record_stock(key, 8, 1, second.clone(), third.clone())
            .unwrap();
        collector
            .record_stock(key, 4, 0, first.clone(), second.clone())
            .unwrap();
        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.stock_update_count().unwrap(), 3);
        assert_eq!(sealed.stocks[0].initial, first);
        assert_eq!(sealed.stocks[0].endpoint, fourth);

        let mut stale = IntervalCollector::new(50, 32, 44).unwrap();
        stale
            .record_stock(key, 4, 0, first.clone(), second.clone())
            .unwrap();
        assert!(matches!(
            stale.record_stock(key, 4, 0, first.clone(), second.clone()),
            Err(CollectorError::DuplicatePredecessor {
                domain: "stock",
                ..
            })
        ));

        let mut corrupt_after = second;
        corrupt_after.ytd_bits ^= 1;
        let mut corrupt = IntervalCollector::new(50, 32, 44).unwrap();
        assert!(matches!(
            corrupt.record_stock(key, 4, 0, first, corrupt_after),
            Err(CollectorError::FloatMismatch("stock YTD"))
        ));
    }

    #[test]
    fn thirty_two_worker_shuffled_chain_is_canonical() {
        let key = StockKey {
            warehouse_id: 1,
            item_id: 42,
        };
        let mut versions = vec![StockVersion {
            quantity: 100,
            ytd_bits: 0.0_f32.to_bits(),
            order_count: 0,
            remote_count: 0,
        }];
        for index in 0..512 {
            let before = versions.last().unwrap();
            versions.push(stock_after(
                before,
                (index % 10 + 1) as u8,
                (index % 7 == 0) as u8,
            ));
        }
        let mut edges = (0..512)
            .map(|index| {
                (
                    (index % 32) as u16,
                    (index % 10 + 1) as u8,
                    (index % 7 == 0) as u8,
                    versions[index].clone(),
                    versions[index + 1].clone(),
                )
            })
            .collect::<Vec<_>>();
        edges.shuffle(&mut StdRng::seed_from_u64(7));

        let mut collector = IntervalCollector::new(50, 32, 123).unwrap();
        for (_worker, quantity, remote, before, after) in edges {
            collector
                .record_stock(key, quantity, remote, before, after)
                .unwrap();
        }
        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.stocks[0].update_count, 512);
        assert_eq!(sealed.stocks[0].endpoint, versions[512]);
    }

    #[test]
    fn one_million_hot_key_updates_keep_constant_collector_shape() {
        let key = StockKey {
            warehouse_id: 1,
            item_id: 1,
        };
        let mut collector = IntervalCollector::new(50, 32, 0xfeed).unwrap();
        let mut before = StockVersion {
            quantity: 100,
            ytd_bits: 0.0_f32.to_bits(),
            order_count: 0,
            remote_count: 0,
        };
        for index in 0..1_000_000 {
            let quantity = (index % 10 + 1) as u8;
            let remote = (index % 11 == 0) as u8;
            let after = stock_after(&before, quantity, remote);
            collector
                .record_stock(key, quantity, remote, before, after.clone())
                .unwrap();
            before = after;
        }
        assert_eq!(
            collector.storage(),
            CollectorStorage {
                customer_keys: 0,
                stock_keys: 1,
                pending_intervals: 0,
            }
        );
        let sealed = collector.seal().unwrap();
        assert_eq!(sealed.stocks.len(), 1);
        assert_eq!(sealed.stocks[0].update_count, 1_000_000);
    }
}
