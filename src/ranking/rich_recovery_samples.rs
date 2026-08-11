//! Bounded, typed recovery samples for state that is not represented by the
//! numeric Customer and Stock interval endpoints.
//!
//! This collector is deliberately fed by the same terminal-ACK path as
//! [`super::terminal_evidence::TerminalEvidenceCollector`]. It is not an event
//! ledger: it retains only deterministic bottom-k row samples and the exact
//! multiplicity of retained History tuples. A failed offer poisons the
//! collector without publishing any part of that terminal.
//!
//! New-Order samples are mutable final-state projections. A later committed
//! Delivery for a retained order removes its queue row, installs the carrier,
//! and copies the delivery timestamp into every retained order line. Customer
//! and Stock numeric state is intentionally not copied here. Bad-credit
//! Customer data has its own independent rank domain. Its Payment chain is
//! rooted in the deterministic setup Customer row and therefore does not
//! require a probabilistic key intersection with numeric interval samples.
//! A caller using out-of-order worker receipts must include
//! [`RichRecoveryCollector::pending_edges`] in the same composite ACK gate as
//! numeric interval pending edges: one worker may not receive its next request
//! while its terminal leaves either collector unrooted. Under that contract
//! the exact pending-edge count is bounded by the configured client count.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

use crate::consistency::sum_f32_as_f64_once;
use crate::profile::{DISTRICTS_PER_WAREHOUSE, ITEM_COUNT, OFFICIAL_CLIENTS, OFFICIAL_WAREHOUSES};
use crate::workload::{
    NewOrderInput, PaymentInput, TransactionParameters, TransactionTicket, CUSTOMERS_PER_DISTRICT,
    INVALID_ITEM_ID, MAX_CARRIER_ID, MAX_ITEM_QUANTITY, MAX_ORDER_LINES, MAX_PAYMENT_CENTS,
    MIN_CARRIER_ID, MIN_ITEM_QUANTITY, MIN_ORDER_LINES, MIN_PAYMENT_CENTS,
};

use super::evidence_collector::{CustomerKey, SealedIntervalEvidence, StockKey};
use super::recovery_samples::{SampleScore, RECOVERY_SAMPLE_CAPACITY};
use super::runner::{
    CustomerVersion, DeliveredOrderEvidence, NewOrderEvidence, PaymentEvidence, RankedCommit,
    RankedTransactionOutcome,
};

pub const RICH_RECOVERY_POLICY_VERSION: u32 = 1;
pub const RICH_RECOVERY_SAMPLE_CAPACITY: usize = RECOVERY_SAMPLE_CAPACITY;
pub const RICH_HISTORY_SAMPLE_CAPACITY: usize = 2;
pub const MAX_RICH_RECOVERY_RAW_BYTES: usize = 128 * 1024;

const MAX_ENTRY_TIMESTAMP_BYTES: usize = 19;
const MAX_HISTORY_TIMESTAMP_BYTES: usize = 19;
const MAX_DELIVERY_TIMESTAMP_BYTES: usize = 30;
const MAX_HISTORY_DATA_BYTES: usize = 24;
const MAX_CUSTOMER_DATA_BYTES: usize = 50;
const MIN_BAD_CREDIT_PREFIX_BYTES: usize = 15;
const MAX_BAD_CREDIT_PREFIX_BYTES: usize = 25;
const MAX_BAD_CREDIT_SUFFIX_ENTRIES: usize = 4;
const DISTRICT_INFO_BYTES: usize = 24;
const INITIAL_ORDER_ID_CEILING: i32 = CUSTOMERS_PER_DISTRICT as i32;
const CUSTOMER_INITIAL_PAYMENT_COUNT: i32 = 1;
const CUSTOMER_INITIAL_DELIVERY_COUNT: i32 = 0;

const ORDER_SAMPLE_DOMAIN: &[u8] = b"recovery/rich-order/v1";
const DELIVERY_SAMPLE_DOMAIN: &[u8] = b"recovery/rich-delivery/v1";
const HISTORY_SAMPLE_DOMAIN: &[u8] = b"recovery/rich-history/v1";
const BAD_CUSTOMER_SAMPLE_DOMAIN: &[u8] = b"recovery/rich-bad-credit/v1";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OrderKey {
    warehouse_id: u16,
    district_id: u8,
    order_id: i32,
}

impl OrderKey {
    pub(crate) const fn from_parts(warehouse_id: u16, district_id: u8, order_id: i32) -> Self {
        Self {
            warehouse_id,
            district_id,
            order_id,
        }
    }

    pub const fn warehouse_id(self) -> u16 {
        self.warehouse_id
    }

    pub const fn district_id(self) -> u8 {
        self.district_id
    }

    pub const fn order_id(self) -> i32 {
        self.order_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedOrderLine {
    number: u8,
    item_id: u32,
    supply_warehouse: u16,
    delivery_timestamp: Vec<u8>,
    quantity: u8,
    amount_bits: u32,
    district_info: Vec<u8>,
}

impl SealedOrderLine {
    pub const fn number(&self) -> u8 {
        self.number
    }

    pub const fn item_id(&self) -> u32 {
        self.item_id
    }

    pub const fn supply_warehouse(&self) -> u16 {
        self.supply_warehouse
    }

    pub fn delivery_timestamp(&self) -> &[u8] {
        &self.delivery_timestamp
    }

    pub const fn quantity(&self) -> u8 {
        self.quantity
    }

    pub const fn amount_bits(&self) -> u32 {
        self.amount_bits
    }

    pub fn district_info(&self) -> &[u8] {
        &self.district_info
    }

    pub fn stock_reference(&self) -> StockKey {
        StockKey {
            warehouse_id: i32::from(self.supply_warehouse),
            item_id: self.item_id as i32,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedNewOrderSample {
    score: SampleScore,
    key: OrderKey,
    customer_id: u16,
    entry_timestamp: Vec<u8>,
    carrier_id: u8,
    line_count: u8,
    all_local: bool,
    queue_present: bool,
    lines: Vec<SealedOrderLine>,
}

impl SealedNewOrderSample {
    pub const fn score(&self) -> SampleScore {
        self.score
    }

    pub const fn key(&self) -> OrderKey {
        self.key
    }

    pub const fn customer_id(&self) -> u16 {
        self.customer_id
    }

    pub fn entry_timestamp(&self) -> &[u8] {
        &self.entry_timestamp
    }

    pub const fn carrier_id(&self) -> u8 {
        self.carrier_id
    }

    pub const fn line_count(&self) -> u8 {
        self.line_count
    }

    pub const fn all_local(&self) -> bool {
        self.all_local
    }

    pub const fn queue_present(&self) -> bool {
        self.queue_present
    }

    pub fn lines(&self) -> &[SealedOrderLine] {
        &self.lines
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedDeliveryLine {
    number: u8,
    delivery_timestamp: Vec<u8>,
    amount_bits: u32,
}

impl SealedDeliveryLine {
    pub const fn number(&self) -> u8 {
        self.number
    }

    pub fn delivery_timestamp(&self) -> &[u8] {
        &self.delivery_timestamp
    }

    pub const fn amount_bits(&self) -> u32 {
        self.amount_bits
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedDeliverySample {
    score: SampleScore,
    key: OrderKey,
    customer_id: i32,
    carrier_id: u8,
    queue_present: bool,
    delivery_timestamp: Vec<u8>,
    lines: Vec<SealedDeliveryLine>,
}

impl SealedDeliverySample {
    pub const fn score(&self) -> SampleScore {
        self.score
    }

    pub const fn key(&self) -> OrderKey {
        self.key
    }

    pub const fn customer_id(&self) -> i32 {
        self.customer_id
    }

    pub const fn carrier_id(&self) -> u8 {
        self.carrier_id
    }

    pub const fn queue_present(&self) -> bool {
        self.queue_present
    }

    pub fn delivery_timestamp(&self) -> &[u8] {
        &self.delivery_timestamp
    }

    pub fn lines(&self) -> &[SealedDeliveryLine] {
        &self.lines
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SealedBadCreditPaymentPrefix {
    home_warehouse_id: u16,
    home_district_id: u8,
    amount_cents: u32,
}

impl SealedBadCreditPaymentPrefix {
    pub const fn home_warehouse_id(&self) -> u16 {
        self.home_warehouse_id
    }

    pub const fn home_district_id(&self) -> u8 {
        self.home_district_id
    }

    pub const fn amount_cents(&self) -> u32 {
        self.amount_cents
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedBadCreditCustomerSample {
    score: SampleScore,
    key: CustomerKey,
    final_payment_count: i32,
    credit: [u8; 2],
    data: Vec<u8>,
    committed_payment_updates: u64,
    payment_suffix: Vec<SealedBadCreditPaymentPrefix>,
}

impl SealedBadCreditCustomerSample {
    pub const fn score(&self) -> SampleScore {
        self.score
    }

    pub const fn customer_key(&self) -> CustomerKey {
        self.key
    }

    /// Final committed Payment count for this sampled Customer. Delivery
    /// versions are intentionally absent: a Delivery may commit after the last
    /// Payment without changing either `c_payment_cnt` or `c_data`.
    pub const fn final_payment_count(&self) -> i32 {
        self.final_payment_count
    }

    pub const fn expected_credit(&self) -> &[u8; 2] {
        &self.credit
    }

    pub fn final_data(&self) -> &[u8] {
        &self.data
    }

    pub const fn committed_payment_updates(&self) -> u64 {
        self.committed_payment_updates
    }

    pub fn payment_suffix(&self) -> &[SealedBadCreditPaymentPrefix] {
        &self.payment_suffix
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct HistoryGroupKey {
    customer_id: i32,
    customer_district_id: u8,
    customer_warehouse_id: u16,
    home_district_id: u8,
    home_warehouse_id: u16,
}

impl HistoryGroupKey {
    pub(crate) const fn from_parts(
        customer_id: i32,
        customer_district_id: u8,
        customer_warehouse_id: u16,
        home_district_id: u8,
        home_warehouse_id: u16,
    ) -> Self {
        Self {
            customer_id,
            customer_district_id,
            customer_warehouse_id,
            home_district_id,
            home_warehouse_id,
        }
    }

    pub const fn customer_id(self) -> i32 {
        self.customer_id
    }

    pub const fn customer_district_id(self) -> u8 {
        self.customer_district_id
    }

    pub const fn customer_warehouse_id(self) -> u16 {
        self.customer_warehouse_id
    }

    pub const fn home_district_id(self) -> u8 {
        self.home_district_id
    }

    pub const fn home_warehouse_id(self) -> u16 {
        self.home_warehouse_id
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct HistoryTupleKey {
    group: HistoryGroupKey,
    timestamp: Vec<u8>,
    amount_bits: u32,
    data: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedHistoryTuple {
    score: SampleScore,
    timestamp: Vec<u8>,
    amount_bits: u32,
    data: Vec<u8>,
    committed_multiplicity: u64,
    setup_collision_multiplicity: u8,
}

impl SealedHistoryTuple {
    pub const fn score(&self) -> SampleScore {
        self.score
    }

    pub fn timestamp(&self) -> &[u8] {
        &self.timestamp
    }

    pub const fn amount_bits(&self) -> u32 {
        self.amount_bits
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Number of committed runtime Payment rows with this complete tuple.
    ///
    /// A recovery checker must add the deterministic setup-data collision
    /// multiplicity before comparing against the physical table.
    pub const fn committed_multiplicity(&self) -> u64 {
        self.committed_multiplicity
    }

    pub const fn setup_collision_multiplicity(&self) -> u8 {
        self.setup_collision_multiplicity
    }

    pub fn expected_total_multiplicity(&self) -> Result<u64, RichRecoveryError> {
        self.committed_multiplicity
            .checked_add(u64::from(self.setup_collision_multiplicity))
            .ok_or(RichRecoveryError::Overflow(
                "History expected total multiplicity",
            ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SealedHistoryGroup {
    key: HistoryGroupKey,
    tuples: Vec<SealedHistoryTuple>,
}

impl SealedHistoryGroup {
    pub const fn key(&self) -> HistoryGroupKey {
        self.key
    }

    pub fn tuples(&self) -> &[SealedHistoryTuple] {
        &self.tuples
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitialHistoryRow {
    timestamp: Vec<u8>,
    amount_bits: u32,
    data: Vec<u8>,
}

impl InitialHistoryRow {
    pub fn new(
        timestamp: Vec<u8>,
        amount_bits: u32,
        data: Vec<u8>,
    ) -> Result<Self, RichRecoveryError> {
        validate_bounded_char(
            "initial History timestamp",
            &timestamp,
            1,
            MAX_HISTORY_TIMESTAMP_BYTES,
        )?;
        validate_f32_range("initial History amount", amount_bits, 0.0, f32::MAX)?;
        validate_bounded_char("initial History data", &data, 1, MAX_HISTORY_DATA_BYTES)?;
        Ok(Self {
            timestamp,
            amount_bits,
            data,
        })
    }

    pub fn timestamp(&self) -> &[u8] {
        &self.timestamp
    }

    pub const fn amount_bits(&self) -> u32 {
        self.amount_bits
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// O(1) deterministic point lookup for the one setup History row owned by a
/// Customer. It prevents a runtime Payment tuple from hiding an equal setup
/// tuple behind a runtime-only multiplicity.
pub trait InitialHistoryProvider: Send + Sync {
    fn initial_history(&self, customer: CustomerKey) -> Option<InitialHistoryRow>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InitialCustomerData {
    credit: [u8; 2],
    data: Vec<u8>,
}

impl InitialCustomerData {
    pub fn new(credit: [u8; 2], data: Vec<u8>) -> Result<Self, RichRecoveryError> {
        if credit != *b"GC" && credit != *b"BC" {
            return Err(RichRecoveryError::InvalidEvidence(
                "initial Customer credit must be GC or BC",
            ));
        }
        validate_bounded_char("initial Customer data", &data, 0, MAX_CUSTOMER_DATA_BYTES)?;
        Ok(Self { credit, data })
    }

    pub const fn credit(&self) -> &[u8; 2] {
        &self.credit
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

pub trait InitialCustomerDataProvider: Send + Sync {
    fn initial_customer_data(&self, customer: CustomerKey) -> Option<InitialCustomerData>;
}

impl<F> InitialCustomerDataProvider for F
where
    F: Fn(CustomerKey) -> Option<InitialCustomerData> + Send + Sync,
{
    fn initial_customer_data(&self, customer: CustomerKey) -> Option<InitialCustomerData> {
        self(customer)
    }
}

impl<F> InitialHistoryProvider for F
where
    F: Fn(CustomerKey) -> Option<InitialHistoryRow> + Send + Sync,
{
    fn initial_history(&self, customer: CustomerKey) -> Option<InitialHistoryRow> {
        self(customer)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderCutoffWitness {
    score: SampleScore,
    key: OrderKey,
}

impl OrderCutoffWitness {
    pub const fn score(&self) -> SampleScore {
        self.score
    }

    pub const fn key(&self) -> OrderKey {
        self.key
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CustomerCutoffWitness {
    score: SampleScore,
    key: CustomerKey,
}

impl CustomerCutoffWitness {
    pub const fn score(&self) -> SampleScore {
        self.score
    }

    pub const fn key(&self) -> CustomerKey {
        self.key
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryCutoffWitness {
    score: SampleScore,
    group: HistoryGroupKey,
    timestamp: Vec<u8>,
    amount_bits: u32,
    data: Vec<u8>,
}

impl HistoryCutoffWitness {
    pub const fn score(&self) -> SampleScore {
        self.score
    }

    pub const fn group(&self) -> HistoryGroupKey {
        self.group
    }

    pub fn timestamp(&self) -> &[u8] {
        &self.timestamp
    }

    pub const fn amount_bits(&self) -> u32 {
        self.amount_bits
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// Parsed fixed-width metadata for one persisted rich-recovery section.
///
/// This is deliberately not a sealed value. The canonical reconstruction
/// entry point below treats every field as untrusted and recomputes all
/// derived state before constructing [`SealedRichRecoverySamples`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichRecoveryHeader {
    warehouses: u16,
    run_seed: u64,
    policy_version: u32,
    raw_size_bytes: usize,
    new_order_commits: u64,
    delivered_orders: u64,
    history_rows: u64,
    bad_credit_payments: u64,
}

impl CanonicalRichRecoveryHeader {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        warehouses: u16,
        run_seed: u64,
        policy_version: u32,
        raw_size_bytes: usize,
        new_order_commits: u64,
        delivered_orders: u64,
        history_rows: u64,
        bad_credit_payments: u64,
    ) -> Self {
        Self {
            warehouses,
            run_seed,
            policy_version,
            raw_size_bytes,
            new_order_commits,
            delivered_orders,
            history_rows,
            bad_credit_payments,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichOrderLine {
    number: u8,
    item_id: u32,
    supply_warehouse: u16,
    delivery_timestamp: Vec<u8>,
    quantity: u8,
    amount_bits: u32,
    district_info: Vec<u8>,
}

impl CanonicalRichOrderLine {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        number: u8,
        item_id: u32,
        supply_warehouse: u16,
        delivery_timestamp: Vec<u8>,
        quantity: u8,
        amount_bits: u32,
        district_info: Vec<u8>,
    ) -> Self {
        Self {
            number,
            item_id,
            supply_warehouse,
            delivery_timestamp,
            quantity,
            amount_bits,
            district_info,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichNewOrder {
    score: SampleScore,
    key: OrderKey,
    customer_id: u16,
    entry_timestamp: Vec<u8>,
    carrier_id: u8,
    line_count: u8,
    all_local: bool,
    queue_present: bool,
    lines: Vec<CanonicalRichOrderLine>,
}

impl CanonicalRichNewOrder {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new<I>(
        score: SampleScore,
        key: OrderKey,
        customer_id: u16,
        entry_timestamp: Vec<u8>,
        carrier_id: u8,
        line_count: u8,
        all_local: bool,
        queue_present: bool,
        lines: I,
    ) -> Result<Self, RichRecoveryError>
    where
        I: ExactSizeIterator<Item = CanonicalRichOrderLine>,
    {
        let lines = collect_canonical_exact(
            "NewOrder line DTOs",
            lines,
            usize::from(MIN_ORDER_LINES),
            usize::from(MAX_ORDER_LINES),
        )?;
        if usize::from(line_count) != lines.len() {
            return Err(RichRecoveryError::InvalidEvidence(
                "canonical NewOrder line_count differs from its DTO count",
            ));
        }
        Ok(Self {
            score,
            key,
            customer_id,
            entry_timestamp,
            carrier_id,
            line_count,
            all_local,
            queue_present,
            lines,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichDeliveryLine {
    number: u8,
    delivery_timestamp: Vec<u8>,
    amount_bits: u32,
}

impl CanonicalRichDeliveryLine {
    pub(crate) fn new(number: u8, delivery_timestamp: Vec<u8>, amount_bits: u32) -> Self {
        Self {
            number,
            delivery_timestamp,
            amount_bits,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichDelivery {
    score: SampleScore,
    key: OrderKey,
    customer_id: i32,
    carrier_id: u8,
    queue_present: bool,
    delivery_timestamp: Vec<u8>,
    lines: Vec<CanonicalRichDeliveryLine>,
}

impl CanonicalRichDelivery {
    pub(crate) fn new<I>(
        score: SampleScore,
        key: OrderKey,
        customer_id: i32,
        carrier_id: u8,
        queue_present: bool,
        delivery_timestamp: Vec<u8>,
        lines: I,
    ) -> Result<Self, RichRecoveryError>
    where
        I: ExactSizeIterator<Item = CanonicalRichDeliveryLine>,
    {
        let lines = collect_canonical_exact(
            "Delivery line DTOs",
            lines,
            usize::from(MIN_ORDER_LINES),
            usize::from(MAX_ORDER_LINES),
        )?;
        Ok(Self {
            score,
            key,
            customer_id,
            carrier_id,
            queue_present,
            delivery_timestamp,
            lines,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichBadCreditPrefix {
    home_warehouse_id: u16,
    home_district_id: u8,
    amount_cents: u32,
}

impl CanonicalRichBadCreditPrefix {
    pub(crate) const fn new(
        home_warehouse_id: u16,
        home_district_id: u8,
        amount_cents: u32,
    ) -> Self {
        Self {
            home_warehouse_id,
            home_district_id,
            amount_cents,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichBadCreditCustomer {
    score: SampleScore,
    key: CustomerKey,
    final_payment_count: i32,
    credit: [u8; 2],
    data: Vec<u8>,
    committed_payment_updates: u64,
    payment_suffix: Vec<CanonicalRichBadCreditPrefix>,
}

impl CanonicalRichBadCreditCustomer {
    pub(crate) fn new<I>(
        score: SampleScore,
        key: CustomerKey,
        final_payment_count: i32,
        credit: [u8; 2],
        data: Vec<u8>,
        committed_payment_updates: u64,
        payment_suffix: I,
    ) -> Result<Self, RichRecoveryError>
    where
        I: ExactSizeIterator<Item = CanonicalRichBadCreditPrefix>,
    {
        let payment_suffix = collect_canonical_exact(
            "bad-credit Payment suffix DTOs",
            payment_suffix,
            0,
            MAX_BAD_CREDIT_SUFFIX_ENTRIES,
        )?;
        Ok(Self {
            score,
            key,
            final_payment_count,
            credit,
            data,
            committed_payment_updates,
            payment_suffix,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichHistoryTuple {
    score: SampleScore,
    group: HistoryGroupKey,
    timestamp: Vec<u8>,
    amount_bits: u32,
    data: Vec<u8>,
    committed_multiplicity: u64,
    setup_collision_multiplicity: u8,
}

impl CanonicalRichHistoryTuple {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        score: SampleScore,
        group: HistoryGroupKey,
        timestamp: Vec<u8>,
        amount_bits: u32,
        data: Vec<u8>,
        committed_multiplicity: u64,
        setup_collision_multiplicity: u8,
    ) -> Self {
        Self {
            score,
            group,
            timestamp,
            amount_bits,
            data,
            committed_multiplicity,
            setup_collision_multiplicity,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichOrderWitness {
    score: SampleScore,
    key: OrderKey,
}

impl CanonicalRichOrderWitness {
    pub(crate) const fn new(score: SampleScore, key: OrderKey) -> Self {
        Self { score, key }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichCustomerWitness {
    score: SampleScore,
    key: CustomerKey,
}

impl CanonicalRichCustomerWitness {
    pub(crate) const fn new(score: SampleScore, key: CustomerKey) -> Self {
        Self { score, key }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CanonicalRichHistoryWitness {
    score: SampleScore,
    group: HistoryGroupKey,
    timestamp: Vec<u8>,
    amount_bits: u32,
    data: Vec<u8>,
}

impl CanonicalRichHistoryWitness {
    pub(crate) fn new(
        score: SampleScore,
        group: HistoryGroupKey,
        timestamp: Vec<u8>,
        amount_bits: u32,
        data: Vec<u8>,
    ) -> Self {
        Self {
            score,
            group,
            timestamp,
            amount_bits,
            data,
        }
    }
}

/// Fully validated typed recovery sample sections.
///
/// The section remains independent of the event ledger. Its observed totals
/// exist only to prove sample non-emptiness and bottom-k cutoff completeness,
/// and are cross-checked against the physical aggregate attestation by the
/// outer terminal evidence artifact.
pub struct SealedRichRecoverySamples {
    warehouses: u16,
    run_seed: u64,
    policy_version: u32,
    raw_size_bytes: usize,
    new_order_commits: u64,
    delivered_orders: u64,
    history_rows: u64,
    bad_credit_payments: u64,
    new_orders: Vec<SealedNewOrderSample>,
    deliveries: Vec<SealedDeliverySample>,
    bad_credit_customers: Vec<SealedBadCreditCustomerSample>,
    history_groups: Vec<SealedHistoryGroup>,
    order_rejected: Option<OrderCutoffWitness>,
    delivery_rejected: Option<OrderCutoffWitness>,
    customer_rejected: Option<CustomerCutoffWitness>,
    history_rejected: Option<HistoryCutoffWitness>,
}

impl SealedRichRecoverySamples {
    pub const fn warehouses(&self) -> u16 {
        self.warehouses
    }

    pub const fn run_seed(&self) -> u64 {
        self.run_seed
    }

    pub const fn policy_version(&self) -> u32 {
        self.policy_version
    }

    pub const fn raw_size_bytes(&self) -> usize {
        self.raw_size_bytes
    }

    pub const fn new_order_commit_count(&self) -> u64 {
        self.new_order_commits
    }

    pub const fn delivered_order_count(&self) -> u64 {
        self.delivered_orders
    }

    pub const fn committed_history_row_count(&self) -> u64 {
        self.history_rows
    }

    pub const fn bad_credit_payment_count(&self) -> u64 {
        self.bad_credit_payments
    }

    pub fn new_orders(&self) -> &[SealedNewOrderSample] {
        &self.new_orders
    }

    pub fn deliveries(&self) -> &[SealedDeliverySample] {
        &self.deliveries
    }

    pub fn bad_credit_customers(&self) -> &[SealedBadCreditCustomerSample] {
        &self.bad_credit_customers
    }

    pub fn history_groups(&self) -> &[SealedHistoryGroup] {
        &self.history_groups
    }

    pub fn history_tuples(&self) -> impl Iterator<Item = (HistoryGroupKey, &SealedHistoryTuple)> {
        self.history_groups
            .iter()
            .flat_map(|group| group.tuples.iter().map(move |tuple| (group.key, tuple)))
    }

    pub fn order_rejected_witness(&self) -> Option<&OrderCutoffWitness> {
        self.order_rejected.as_ref()
    }

    pub fn delivery_rejected_witness(&self) -> Option<&OrderCutoffWitness> {
        self.delivery_rejected.as_ref()
    }

    pub fn bad_customer_rejected_witness(&self) -> Option<&CustomerCutoffWitness> {
        self.customer_rejected.as_ref()
    }

    pub fn history_rejected_witness(&self) -> Option<&HistoryCutoffWitness> {
        self.history_rejected.as_ref()
    }

    /// Rebuild a persisted rich section from parsed DTOs.
    ///
    /// The codec is expected to reject encoded lengths before allocating its
    /// DTO vectors. This API independently requires exact-size iterators and
    /// checks every top-level and nested capacity before allocating sealed
    /// vectors. No encoded score, total, witness, setup collision, raw-size
    /// value, or row-domain claim is trusted.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_canonical_parts<O, D, C, H>(
        header: CanonicalRichRecoveryHeader,
        orders: O,
        deliveries: D,
        customers: C,
        histories: H,
        order_rejected: Option<CanonicalRichOrderWitness>,
        delivery_rejected: Option<CanonicalRichOrderWitness>,
        customer_rejected: Option<CanonicalRichCustomerWitness>,
        history_rejected: Option<CanonicalRichHistoryWitness>,
        intervals: &SealedIntervalEvidence,
        initial_history: &dyn InitialHistoryProvider,
        initial_customers: &dyn InitialCustomerDataProvider,
    ) -> Result<Self, RichRecoveryError>
    where
        O: ExactSizeIterator<Item = CanonicalRichNewOrder>,
        D: ExactSizeIterator<Item = CanonicalRichDelivery>,
        C: ExactSizeIterator<Item = CanonicalRichBadCreditCustomer>,
        H: ExactSizeIterator<Item = CanonicalRichHistoryTuple>,
    {
        if header.warehouses == 0 || header.warehouses > OFFICIAL_WAREHOUSES {
            return Err(RichRecoveryError::InvalidConfiguration(
                "warehouses must be in 1..=50",
            ));
        }
        if header.policy_version != RICH_RECOVERY_POLICY_VERSION {
            return Err(RichRecoveryError::UnsupportedPolicy {
                actual: header.policy_version,
                expected: RICH_RECOVERY_POLICY_VERSION,
            });
        }
        if header.raw_size_bytes > MAX_RICH_RECOVERY_RAW_BYTES {
            return Err(RichRecoveryError::RawSizeCeiling {
                actual: header.raw_size_bytes,
                limit: MAX_RICH_RECOVERY_RAW_BYTES,
            });
        }
        if intervals.warehouses() != header.warehouses || intervals.sample_seed() != header.run_seed
        {
            return Err(RichRecoveryError::IntervalBindingMismatch);
        }
        if header.bad_credit_payments > header.history_rows {
            return Err(RichRecoveryError::InvalidEvidence(
                "bad-credit Payment count exceeds the committed History row count",
            ));
        }

        let order_count = orders.len();
        let delivery_count = deliveries.len();
        let customer_count = customers.len();
        let history_count = histories.len();
        validate_canonical_count(
            "NewOrder sample DTOs",
            order_count,
            0,
            RICH_RECOVERY_SAMPLE_CAPACITY,
        )?;
        validate_canonical_count(
            "Delivery sample DTOs",
            delivery_count,
            0,
            RICH_RECOVERY_SAMPLE_CAPACITY,
        )?;
        validate_canonical_count(
            "bad-credit Customer sample DTOs",
            customer_count,
            0,
            RICH_RECOVERY_SAMPLE_CAPACITY,
        )?;
        validate_canonical_count(
            "History tuple DTOs",
            history_count,
            0,
            RICH_HISTORY_SAMPLE_CAPACITY,
        )?;

        let mut new_orders = Vec::with_capacity(order_count);
        let mut previous_order = None;
        for (index, encoded) in orders.enumerate() {
            validate_exact_iterator_step("NewOrder sample DTOs", index, order_count)?;
            validate_order_key(header.warehouses, encoded.key, true)?;
            let expected_score = order_score(header.run_seed, encoded.key);
            validate_decoded_rank(
                "NewOrder final state",
                encoded.score,
                expected_score,
                encoded.key,
                &mut previous_order,
            )?;
            if !(1..=CUSTOMERS_PER_DISTRICT).contains(&encoded.customer_id)
                || usize::from(encoded.line_count) != encoded.lines.len()
            {
                return Err(RichRecoveryError::InvalidEvidence(
                    "canonical NewOrder header is outside the row domain",
                ));
            }
            validate_canonical_count(
                "canonical NewOrder lines",
                encoded.lines.len(),
                usize::from(MIN_ORDER_LINES),
                usize::from(MAX_ORDER_LINES),
            )?;
            validate_bounded_char(
                "canonical NewOrder entry timestamp",
                &encoded.entry_timestamp,
                1,
                MAX_ENTRY_TIMESTAMP_BYTES,
            )?;

            let state_is_valid = if encoded.queue_present {
                encoded.carrier_id == 0
                    && encoded
                        .lines
                        .iter()
                        .all(|line| line.delivery_timestamp.is_empty())
            } else {
                (MIN_CARRIER_ID..=MAX_CARRIER_ID).contains(&encoded.carrier_id)
                    && encoded.lines.first().is_some_and(|first| {
                        !first.delivery_timestamp.is_empty()
                            && encoded
                                .lines
                                .iter()
                                .all(|line| line.delivery_timestamp == first.delivery_timestamp)
                    })
            };
            if !state_is_valid {
                return Err(RichRecoveryError::InvalidEvidence(
                    "canonical NewOrder queue, carrier, and delivery timestamps disagree",
                ));
            }

            let mut derived_all_local = true;
            let mut lines = Vec::with_capacity(encoded.lines.len());
            for (line_index, line) in encoded.lines.into_iter().enumerate() {
                if usize::from(line.number) != line_index + 1
                    || !(1..=ITEM_COUNT).contains(&line.item_id)
                    || line.supply_warehouse == 0
                    || line.supply_warehouse > header.warehouses
                    || !(MIN_ITEM_QUANTITY..=MAX_ITEM_QUANTITY).contains(&line.quantity)
                {
                    return Err(RichRecoveryError::InvalidEvidence(
                        "canonical NewOrder line is outside the row domain",
                    ));
                }
                validate_f32_range(
                    "canonical NewOrder line amount",
                    line.amount_bits,
                    f32::MIN_POSITIVE,
                    1_000.0,
                )?;
                let (minimum_timestamp, maximum_timestamp) = if encoded.queue_present {
                    (0, 0)
                } else {
                    (1, MAX_DELIVERY_TIMESTAMP_BYTES)
                };
                validate_bounded_char(
                    "canonical NewOrder line delivery timestamp",
                    &line.delivery_timestamp,
                    minimum_timestamp,
                    maximum_timestamp,
                )?;
                validate_bounded_char(
                    "canonical NewOrder district information",
                    &line.district_info,
                    DISTRICT_INFO_BYTES,
                    DISTRICT_INFO_BYTES,
                )?;
                derived_all_local &= line.supply_warehouse == encoded.key.warehouse_id;
                lines.push(SealedOrderLine {
                    number: line.number,
                    item_id: line.item_id,
                    supply_warehouse: line.supply_warehouse,
                    delivery_timestamp: line.delivery_timestamp,
                    quantity: line.quantity,
                    amount_bits: line.amount_bits,
                    district_info: line.district_info,
                });
            }
            if encoded.all_local != derived_all_local {
                return Err(RichRecoveryError::InvalidEvidence(
                    "canonical NewOrder all_local flag disagrees with its lines",
                ));
            }
            new_orders.push(SealedNewOrderSample {
                score: expected_score,
                key: encoded.key,
                customer_id: encoded.customer_id,
                entry_timestamp: encoded.entry_timestamp,
                carrier_id: encoded.carrier_id,
                line_count: encoded.line_count,
                all_local: encoded.all_local,
                queue_present: encoded.queue_present,
                lines,
            });
        }
        validate_exact_iterator_end("NewOrder sample DTOs", order_count, new_orders.len())?;

        let mut sealed_deliveries = Vec::with_capacity(delivery_count);
        let mut previous_delivery = None;
        for (index, encoded) in deliveries.enumerate() {
            validate_exact_iterator_step("Delivery sample DTOs", index, delivery_count)?;
            validate_order_key(header.warehouses, encoded.key, false)?;
            let expected_score = delivery_score(header.run_seed, encoded.key);
            validate_decoded_rank(
                "Delivery final state",
                encoded.score,
                expected_score,
                encoded.key,
                &mut previous_delivery,
            )?;
            if encoded.queue_present
                || !(MIN_CARRIER_ID..=MAX_CARRIER_ID).contains(&encoded.carrier_id)
                || !(1..=i32::from(CUSTOMERS_PER_DISTRICT)).contains(&encoded.customer_id)
            {
                return Err(RichRecoveryError::InvalidEvidence(
                    "canonical Delivery does not describe a delivered order",
                ));
            }
            validate_bounded_char(
                "canonical Delivery timestamp",
                &encoded.delivery_timestamp,
                1,
                MAX_DELIVERY_TIMESTAMP_BYTES,
            )?;
            validate_canonical_count(
                "canonical Delivery lines",
                encoded.lines.len(),
                usize::from(MIN_ORDER_LINES),
                usize::from(MAX_ORDER_LINES),
            )?;
            let mut lines = Vec::with_capacity(encoded.lines.len());
            for (line_index, line) in encoded.lines.into_iter().enumerate() {
                if usize::from(line.number) != line_index + 1
                    || line.delivery_timestamp != encoded.delivery_timestamp
                {
                    return Err(RichRecoveryError::InvalidEvidence(
                        "canonical Delivery line sequence or timestamp is invalid",
                    ));
                }
                validate_bounded_char(
                    "canonical Delivery line timestamp",
                    &line.delivery_timestamp,
                    1,
                    MAX_DELIVERY_TIMESTAMP_BYTES,
                )?;
                validate_f32_range(
                    "canonical Delivery line amount",
                    line.amount_bits,
                    0.01,
                    9_999.99,
                )?;
                lines.push(SealedDeliveryLine {
                    number: line.number,
                    delivery_timestamp: line.delivery_timestamp,
                    amount_bits: line.amount_bits,
                });
            }
            sealed_deliveries.push(SealedDeliverySample {
                score: expected_score,
                key: encoded.key,
                customer_id: encoded.customer_id,
                carrier_id: encoded.carrier_id,
                queue_present: false,
                delivery_timestamp: encoded.delivery_timestamp,
                lines,
            });
        }
        validate_exact_iterator_end(
            "Delivery sample DTOs",
            delivery_count,
            sealed_deliveries.len(),
        )?;
        validate_order_delivery_intersections(&new_orders, &sealed_deliveries)?;

        let mut bad_credit_customers = Vec::with_capacity(customer_count);
        let mut previous_customer = None;
        let mut selected_bad_credit_updates = 0_u64;
        for (index, encoded) in customers.enumerate() {
            validate_exact_iterator_step("bad-credit Customer sample DTOs", index, customer_count)?;
            validate_customer_key(header.warehouses, encoded.key)?;
            let expected_score = bad_customer_score(header.run_seed, encoded.key);
            validate_decoded_rank(
                "bad-credit Customer data",
                encoded.score,
                expected_score,
                encoded.key,
                &mut previous_customer,
            )?;
            let initial = initial_customers
                .initial_customer_data(encoded.key)
                .ok_or(RichRecoveryError::MissingInitialCustomer(encoded.key))?;
            if initial.credit != *b"BC" || encoded.credit != *b"BC" {
                return Err(RichRecoveryError::CustomerCreditFlagMismatch {
                    key: encoded.key,
                    generated: initial.credit,
                    claimed_bad_credit: encoded.credit == *b"BC",
                });
            }
            validate_bounded_char(
                "canonical bad-credit Customer data",
                &encoded.data,
                0,
                MAX_CUSTOMER_DATA_BYTES,
            )?;
            if encoded.committed_payment_updates == 0 {
                return Err(RichRecoveryError::InvalidEvidence(
                    "canonical bad-credit Customer has no committed Payment update",
                ));
            }
            let update_count = i64::try_from(encoded.committed_payment_updates)
                .map_err(|_| RichRecoveryError::Overflow("bad-credit Payment update count"))?;
            let expected_payment_count = i64::from(CUSTOMER_INITIAL_PAYMENT_COUNT)
                .checked_add(update_count)
                .ok_or(RichRecoveryError::Overflow(
                    "bad-credit final Payment count",
                ))?;
            if i64::from(encoded.final_payment_count) != expected_payment_count {
                return Err(RichRecoveryError::InvalidEvidence(
                    "canonical bad-credit final Payment count is not setup-rooted",
                ));
            }
            let payment_suffix = encoded
                .payment_suffix
                .into_iter()
                .map(|prefix| SealedBadCreditPaymentPrefix {
                    home_warehouse_id: prefix.home_warehouse_id,
                    home_district_id: prefix.home_district_id,
                    amount_cents: prefix.amount_cents,
                })
                .collect::<Vec<_>>();
            validate_bad_credit_suffix(
                header.warehouses,
                encoded.key,
                encoded.committed_payment_updates,
                &initial.data,
                &encoded.data,
                &payment_suffix,
            )?;
            selected_bad_credit_updates = selected_bad_credit_updates
                .checked_add(encoded.committed_payment_updates)
                .ok_or(RichRecoveryError::Overflow(
                    "selected bad-credit Payment updates",
                ))?;
            bad_credit_customers.push(SealedBadCreditCustomerSample {
                score: expected_score,
                key: encoded.key,
                final_payment_count: encoded.final_payment_count,
                credit: *b"BC",
                data: encoded.data,
                committed_payment_updates: encoded.committed_payment_updates,
                payment_suffix,
            });
        }
        validate_exact_iterator_end(
            "bad-credit Customer sample DTOs",
            customer_count,
            bad_credit_customers.len(),
        )?;

        let mut history_groups = BTreeMap::<HistoryGroupKey, Vec<SealedHistoryTuple>>::new();
        let mut previous_history = None;
        let mut selected_history_rows = 0_u64;
        let mut decoded_history_count = 0_usize;
        for (index, encoded) in histories.enumerate() {
            validate_exact_iterator_step("History tuple DTOs", index, history_count)?;
            decoded_history_count = decoded_history_count
                .checked_add(1)
                .ok_or(RichRecoveryError::Overflow("decoded History tuple count"))?;
            validate_history_group(header.warehouses, encoded.group)?;
            validate_bounded_char(
                "canonical History timestamp",
                &encoded.timestamp,
                1,
                MAX_HISTORY_TIMESTAMP_BYTES,
            )?;
            validate_f32_range(
                "canonical History amount",
                encoded.amount_bits,
                MIN_PAYMENT_CENTS as f32 / 100.0,
                MAX_PAYMENT_CENTS as f32 / 100.0,
            )?;
            validate_bounded_char(
                "canonical History data",
                &encoded.data,
                1,
                MAX_HISTORY_DATA_BYTES,
            )?;
            if encoded.committed_multiplicity == 0 {
                return Err(RichRecoveryError::InvalidEvidence(
                    "canonical History tuple multiplicity is zero",
                ));
            }
            let key = HistoryTupleKey {
                group: encoded.group,
                timestamp: encoded.timestamp.clone(),
                amount_bits: encoded.amount_bits,
                data: encoded.data.clone(),
            };
            let expected_score = history_score(header.run_seed, &key);
            validate_decoded_rank(
                "History tuple",
                encoded.score,
                expected_score,
                key.clone(),
                &mut previous_history,
            )?;
            let trusted_collision = setup_history_collision(initial_history, &key)?;
            if encoded.setup_collision_multiplicity != trusted_collision {
                return Err(RichRecoveryError::SetupCollisionMismatch {
                    encoded: encoded.setup_collision_multiplicity,
                    trusted: trusted_collision,
                });
            }
            selected_history_rows = selected_history_rows
                .checked_add(encoded.committed_multiplicity)
                .ok_or(RichRecoveryError::Overflow(
                    "selected History tuple multiplicities",
                ))?;
            history_groups
                .entry(encoded.group)
                .or_default()
                .push(SealedHistoryTuple {
                    score: expected_score,
                    timestamp: encoded.timestamp,
                    amount_bits: encoded.amount_bits,
                    data: encoded.data,
                    committed_multiplicity: encoded.committed_multiplicity,
                    setup_collision_multiplicity: trusted_collision,
                });
        }
        validate_exact_iterator_end("History tuple DTOs", history_count, decoded_history_count)?;
        let history_groups = history_groups
            .into_iter()
            .map(|(key, tuples)| SealedHistoryGroup { key, tuples })
            .collect::<Vec<_>>();

        let (order_rejected, order_rejected_rank) =
            decode_order_witness(header, order_rejected, true, order_score)?;
        let (delivery_rejected, delivery_rejected_rank) =
            decode_order_witness(header, delivery_rejected, false, delivery_score)?;
        let (customer_rejected, customer_rejected_rank) =
            decode_customer_witness(header, customer_rejected)?;
        let (history_rejected, history_rejected_rank) =
            decode_history_witness(header, history_rejected)?;

        validate_decoded_cutoff(
            "NewOrder final state",
            new_orders.len(),
            RICH_RECOVERY_SAMPLE_CAPACITY,
            new_orders.len() as u64,
            header.new_order_commits,
            previous_order.as_ref(),
            order_rejected_rank.as_ref(),
        )?;
        validate_decoded_cutoff(
            "Delivery final state",
            sealed_deliveries.len(),
            RICH_RECOVERY_SAMPLE_CAPACITY,
            sealed_deliveries.len() as u64,
            header.delivered_orders,
            previous_delivery.as_ref(),
            delivery_rejected_rank.as_ref(),
        )?;
        validate_decoded_cutoff(
            "bad-credit Customer data",
            bad_credit_customers.len(),
            RICH_RECOVERY_SAMPLE_CAPACITY,
            selected_bad_credit_updates,
            header.bad_credit_payments,
            previous_customer.as_ref(),
            customer_rejected_rank.as_ref(),
        )?;
        validate_decoded_cutoff(
            "History tuple",
            decoded_history_count,
            RICH_HISTORY_SAMPLE_CAPACITY,
            selected_history_rows,
            header.history_rows,
            previous_history.as_ref(),
            history_rejected_rank.as_ref(),
        )?;

        let raw_size_bytes = sealed_raw_size(
            &new_orders,
            &sealed_deliveries,
            &bad_credit_customers,
            &history_groups,
            order_rejected.as_ref(),
            delivery_rejected.as_ref(),
            customer_rejected.as_ref(),
            history_rejected.as_ref(),
        )?;
        if raw_size_bytes != header.raw_size_bytes {
            return Err(RichRecoveryError::RawSizeMismatch {
                encoded: header.raw_size_bytes,
                computed: raw_size_bytes,
            });
        }
        if raw_size_bytes > MAX_RICH_RECOVERY_RAW_BYTES {
            return Err(RichRecoveryError::RawSizeCeiling {
                actual: raw_size_bytes,
                limit: MAX_RICH_RECOVERY_RAW_BYTES,
            });
        }

        Ok(Self {
            warehouses: header.warehouses,
            run_seed: header.run_seed,
            policy_version: RICH_RECOVERY_POLICY_VERSION,
            raw_size_bytes,
            new_order_commits: header.new_order_commits,
            delivered_orders: header.delivered_orders,
            history_rows: header.history_rows,
            bad_credit_payments: header.bad_credit_payments,
            new_orders,
            deliveries: sealed_deliveries,
            bad_credit_customers,
            history_groups,
            order_rejected,
            delivery_rejected,
            customer_rejected,
            history_rejected,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RankedKey<K> {
    score: SampleScore,
    key: K,
}

impl<K: Ord> Ord for RankedKey<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .cmp(&other.score)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl<K: Ord> PartialOrd for RankedKey<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug)]
struct RankedReservoir<K, V> {
    capacity: usize,
    entries: BTreeMap<RankedKey<K>, V>,
    by_key: BTreeMap<K, SampleScore>,
    rejected: Option<RankedKey<K>>,
}

impl<K, V> RankedReservoir<K, V> {
    fn new(capacity: usize) -> Self {
        debug_assert!(capacity > 0);
        Self {
            capacity,
            entries: BTreeMap::new(),
            by_key: BTreeMap::new(),
            rejected: None,
        }
    }
}

impl<K: Clone + Eq + Ord, V> RankedReservoir<K, V> {
    fn insertion_eviction(&self, score: SampleScore, key: &K) -> Option<Option<K>> {
        if self.by_key.contains_key(key) {
            return Some(None);
        }
        if self.entries.len() < self.capacity {
            return Some(None);
        }
        let worst = self
            .entries
            .last_key_value()
            .map(|(ranked, _)| ranked)
            .expect("a full reservoir has a worst key");
        let candidate = RankedKey {
            score,
            key: key.clone(),
        };
        (candidate < *worst).then(|| Some(worst.key.clone()))
    }

    fn ensure(&mut self, score: SampleScore, key: K, value: impl FnOnce() -> V) -> bool {
        if self.by_key.contains_key(&key) {
            return true;
        }

        let candidate = RankedKey { score, key };
        if self.entries.len() == self.capacity {
            let worst = self
                .entries
                .last_key_value()
                .map(|(ranked, _)| ranked.clone())
                .expect("a full fixed-capacity reservoir has a worst key");
            if candidate >= worst {
                self.observe_rejected(candidate);
                return false;
            }
            self.entries.remove(&worst);
            self.by_key.remove(&worst.key);
            self.observe_rejected(worst);
        }
        self.by_key.insert(candidate.key.clone(), candidate.score);
        self.entries.insert(candidate, value());
        true
    }

    fn get(&self, key: &K) -> Option<&V> {
        let score = *self.by_key.get(key)?;
        self.entries.get(&RankedKey {
            score,
            key: key.clone(),
        })
    }

    fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        let score = *self.by_key.get(key)?;
        self.entries.get_mut(&RankedKey {
            score,
            key: key.clone(),
        })
    }

    fn take(&mut self, key: &K) -> Option<(RankedKey<K>, V)> {
        let score = self.by_key.remove(key)?;
        let ranked = RankedKey {
            score,
            key: key.clone(),
        };
        let value = self
            .entries
            .remove(&ranked)
            .expect("rank and key indexes remain synchronized");
        Some((ranked, value))
    }

    fn observe_rejected(&mut self, candidate: RankedKey<K>) {
        if self
            .rejected
            .as_ref()
            .is_none_or(|current| candidate < *current)
        {
            self.rejected = Some(candidate);
        }
    }

    fn validate_cutoff(
        &self,
        domain: &'static str,
        selected_weight: u64,
        global_weight: u64,
    ) -> Result<(), RichRecoveryError> {
        if let Some(rejected) = &self.rejected {
            if self.entries.len() != self.capacity {
                return Err(RichRecoveryError::InvalidEvidence(
                    "a bottom-k rejection witness requires a full reservoir",
                ));
            }
            let Some((selected_max, _)) = self.entries.last_key_value() else {
                return Err(RichRecoveryError::InvalidEvidence(
                    "a witnessed bottom-k reservoir cannot be empty",
                ));
            };
            if selected_max >= rejected {
                return Err(RichRecoveryError::InvalidEvidence(
                    "bottom-k selected maximum is not below its rejection witness",
                ));
            }
            if selected_weight >= global_weight {
                return Err(RichRecoveryError::InvalidWeightedSelection { domain });
            }
        } else if selected_weight != global_weight {
            return Err(RichRecoveryError::InvalidWeightedSelection { domain });
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct MutableOrderState {
    origin: Option<OrderOrigin>,
    delivery: Option<DeliveryProjection>,
}

#[derive(Clone, Debug)]
struct OrderOrigin {
    customer_id: u16,
    entry_timestamp: Vec<u8>,
    all_local: bool,
    lines: Vec<OriginLine>,
}

#[derive(Clone, Debug)]
struct OriginLine {
    number: u8,
    item_id: u32,
    supply_warehouse: u16,
    quantity: u8,
    amount_bits: u32,
    district_info: Vec<u8>,
}

#[derive(Clone, Debug)]
struct DeliveryProjection {
    customer_id: i32,
    carrier_id: u8,
    timestamp: Vec<u8>,
    line_amount_bits: Vec<u32>,
}

#[derive(Clone, Debug)]
struct CustomerDataState {
    warehouses: u16,
    key: CustomerKey,
    setup_data: Vec<u8>,
    update_count: u64,
    endpoint: CustomerVersion,
    endpoint_data: Vec<u8>,
    payment_suffix: Vec<SealedBadCreditPaymentPrefix>,
    pending: BTreeMap<i32, CustomerDataEdge>,
}

impl CustomerDataState {
    fn new(warehouses: u16, key: CustomerKey, setup_data: Vec<u8>) -> Self {
        Self {
            warehouses,
            key,
            setup_data: setup_data.clone(),
            update_count: 0,
            endpoint: CustomerVersion {
                payment_count: CUSTOMER_INITIAL_PAYMENT_COUNT,
                delivery_count: CUSTOMER_INITIAL_DELIVERY_COUNT,
            },
            endpoint_data: setup_data,
            payment_suffix: Vec::with_capacity(MAX_BAD_CREDIT_SUFFIX_ENTRIES),
            pending: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct CustomerDataEdge {
    before_version: CustomerVersion,
    after_version: CustomerVersion,
    before_data: Vec<u8>,
    after_data: Vec<u8>,
    prefix: SealedBadCreditPaymentPrefix,
}

#[derive(Clone, Debug)]
struct DeliverySampleState {
    customer_id: i32,
    carrier_id: u8,
    timestamp: Vec<u8>,
    line_amount_bits: Vec<u32>,
}

#[derive(Clone, Debug)]
struct HistoryMultiplicity {
    count: u64,
}

enum PreparedRichTerminal {
    Empty,
    NewOrder {
        key: OrderKey,
        origin: OrderOrigin,
    },
    Payment {
        history: HistoryTupleKey,
        bad_customer: Option<PreparedBadCustomer>,
    },
    Delivery(Vec<PreparedDeliveryOrder>),
}

struct PreparedBadCustomer {
    key: CustomerKey,
    setup_data: Vec<u8>,
    before_version: CustomerVersion,
    after_version: CustomerVersion,
    data_before: Vec<u8>,
    data_after: Vec<u8>,
    prefix: SealedBadCreditPaymentPrefix,
}

struct PreparedDeliveryOrder {
    key: OrderKey,
    projection: DeliveryProjection,
    sample: DeliverySampleState,
}

/// One bounded collector owned by the shared terminal evidence gate.
pub struct RichRecoveryCollector {
    warehouses: u16,
    clients: u16,
    run_seed: u64,
    orders: RankedReservoir<OrderKey, MutableOrderState>,
    deliveries: RankedReservoir<OrderKey, DeliverySampleState>,
    customers: RankedReservoir<CustomerKey, CustomerDataState>,
    retired_customers: BTreeMap<CustomerKey, CustomerDataState>,
    histories: RankedReservoir<HistoryTupleKey, HistoryMultiplicity>,
    pending_customer_edges: usize,
    new_order_commits: u64,
    delivered_orders: u64,
    history_rows: u64,
    bad_credit_payments: u64,
    poisoned: Option<String>,
    initial_history: Arc<dyn InitialHistoryProvider>,
    initial_customers: Arc<dyn InitialCustomerDataProvider>,
}

impl fmt::Debug for RichRecoveryCollector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RichRecoveryCollector")
            .field("warehouses", &self.warehouses)
            .field("clients", &self.clients)
            .field("run_seed", &self.run_seed)
            .field("orders", &self.orders.entries.len())
            .field("deliveries", &self.deliveries.entries.len())
            .field("bad_credit_customers", &self.customers.entries.len())
            .field(
                "retired_bad_credit_customers",
                &self.retired_customers.len(),
            )
            .field("history_tuples", &self.histories.entries.len())
            .field("pending_customer_edges", &self.pending_customer_edges)
            .field("poisoned", &self.poisoned.is_some())
            .finish()
    }
}

impl RichRecoveryCollector {
    pub fn new<H, C>(
        warehouses: u16,
        clients: u16,
        run_seed: u64,
        initial_history: H,
        initial_customers: C,
    ) -> Result<Self, RichRecoveryError>
    where
        H: InitialHistoryProvider + 'static,
        C: InitialCustomerDataProvider + 'static,
    {
        if warehouses == 0 || warehouses > OFFICIAL_WAREHOUSES {
            return Err(RichRecoveryError::InvalidConfiguration(
                "warehouses must be in 1..=50",
            ));
        }
        if clients == 0 || clients > OFFICIAL_CLIENTS {
            return Err(RichRecoveryError::InvalidConfiguration(
                "clients must be in 1..=32",
            ));
        }
        Ok(Self {
            warehouses,
            clients,
            run_seed,
            orders: RankedReservoir::new(RICH_RECOVERY_SAMPLE_CAPACITY),
            deliveries: RankedReservoir::new(RICH_RECOVERY_SAMPLE_CAPACITY),
            customers: RankedReservoir::new(RICH_RECOVERY_SAMPLE_CAPACITY),
            retired_customers: BTreeMap::new(),
            histories: RankedReservoir::new(RICH_HISTORY_SAMPLE_CAPACITY),
            pending_customer_edges: 0,
            new_order_commits: 0,
            delivered_orders: 0,
            history_rows: 0,
            bad_credit_payments: 0,
            poisoned: None,
            initial_history: Arc::new(initial_history),
            initial_customers: Arc::new(initial_customers),
        })
    }

    pub const fn policy_version(&self) -> u32 {
        RICH_RECOVERY_POLICY_VERSION
    }

    pub const fn warehouses(&self) -> u16 {
        self.warehouses
    }

    pub const fn clients(&self) -> u16 {
        self.clients
    }

    pub const fn run_seed(&self) -> u64 {
        self.run_seed
    }

    pub fn is_poisoned(&self) -> bool {
        self.poisoned.is_some()
    }

    /// Exact number of BC Payment edges that are not yet connected to their
    /// generated setup root.
    ///
    /// The terminal collector must withhold the worker ACK while this count or
    /// the numeric interval pending count is nonzero.
    pub const fn pending_edges(&self) -> usize {
        self.pending_customer_edges
    }

    /// Validate and atomically retain the rich state from one terminal.
    ///
    /// Retryable aborts are attempts rather than terminals and must not be
    /// offered here. Expected business rollback and read-only commits are
    /// validated but deliberately produce no recovery samples.
    pub fn offer_terminal(
        &mut self,
        ticket: &TransactionTicket,
        outcome: &RankedTransactionOutcome,
    ) -> Result<(), RichRecoveryError> {
        if let Some(cause) = &self.poisoned {
            return Err(RichRecoveryError::Poisoned(cause.clone()));
        }

        let result = self.prepare_terminal(ticket, outcome).and_then(|prepared| {
            self.preflight_terminal(&prepared)?;
            Ok(prepared)
        });
        match result {
            Ok(prepared) => {
                self.commit_terminal(prepared);
                Ok(())
            }
            Err(error) => {
                self.poisoned = Some(error.to_string());
                Err(error)
            }
        }
    }

    fn prepare_terminal(
        &self,
        ticket: &TransactionTicket,
        outcome: &RankedTransactionOutcome,
    ) -> Result<PreparedRichTerminal, RichRecoveryError> {
        match (ticket.parameters(), outcome) {
            (
                TransactionParameters::NewOrder(input),
                RankedTransactionOutcome::ExpectedRollback,
            ) => {
                if !input.expected_rollback()
                    || input
                        .lines()
                        .last()
                        .is_none_or(|line| line.item_id() != INVALID_ITEM_ID)
                {
                    return Err(RichRecoveryError::InvalidEvidence(
                        "ExpectedRollback does not match the frozen NewOrder input",
                    ));
                }
                Ok(PreparedRichTerminal::Empty)
            }
            (
                TransactionParameters::NewOrder(input),
                RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence)),
            ) => self.prepare_new_order(ticket, input, evidence),
            (
                TransactionParameters::Payment(input),
                RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)),
            ) => self.prepare_payment(ticket, input, evidence),
            (
                TransactionParameters::Delivery(input),
                RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)),
            ) => self.prepare_delivery(ticket, input.carrier_id(), orders),
            (
                TransactionParameters::OrderStatus(_),
                RankedTransactionOutcome::Committed(RankedCommit::OrderStatus),
            )
            | (
                TransactionParameters::StockLevel(_),
                RankedTransactionOutcome::Committed(RankedCommit::StockLevel { .. }),
            ) => Ok(PreparedRichTerminal::Empty),
            _ => Err(RichRecoveryError::InvalidEvidence(
                "terminal outcome kind does not match its frozen ticket",
            )),
        }
    }

    fn prepare_new_order(
        &self,
        ticket: &TransactionTicket,
        input: &NewOrderInput,
        evidence: &NewOrderEvidence,
    ) -> Result<PreparedRichTerminal, RichRecoveryError> {
        let route = ticket.route();
        if input.expected_rollback() {
            return Err(RichRecoveryError::InvalidEvidence(
                "expected-rollback NewOrder cannot have a committed terminal",
            ));
        }
        if route.home_warehouse > self.warehouses
            || evidence.warehouse_id != route.home_warehouse
            || evidence.district_id != route.home_district
            || evidence.order_id <= INITIAL_ORDER_ID_CEILING
            || !(1..=CUSTOMERS_PER_DISTRICT).contains(&input.customer_id())
            || evidence.line_count != input.lines().len() as u8
            || evidence.line_amount_bits.len() != input.lines().len()
            || evidence.recovery_lines.len() != input.lines().len()
            || evidence.line_count < MIN_ORDER_LINES
            || evidence.line_count > MAX_ORDER_LINES
        {
            return Err(RichRecoveryError::InvalidEvidence(
                "committed NewOrder header differs from its frozen ticket",
            ));
        }
        validate_bounded_char(
            "NewOrder entry timestamp",
            &evidence.entry_timestamp,
            1,
            MAX_ENTRY_TIMESTAMP_BYTES,
        )?;

        let mut lines = Vec::with_capacity(input.lines().len());
        let mut remote_lines = 0_u8;
        let mut quantity_total = 0_u32;
        for (index, ((frozen, recovery), amount_bits)) in input
            .lines()
            .iter()
            .zip(&evidence.recovery_lines)
            .zip(&evidence.line_amount_bits)
            .enumerate()
        {
            if frozen.number() as usize != index + 1
                || recovery.number != frozen.number()
                || recovery.item_id != frozen.item_id()
                || recovery.supply_warehouse != frozen.supply_warehouse()
                || recovery.quantity != frozen.quantity()
                || recovery.amount_bits != *amount_bits
                || frozen.item_id() == INVALID_ITEM_ID
                || !(1..=ITEM_COUNT).contains(&frozen.item_id())
                || frozen.supply_warehouse() == 0
                || frozen.supply_warehouse() > self.warehouses
                || !(MIN_ITEM_QUANTITY..=MAX_ITEM_QUANTITY).contains(&frozen.quantity())
            {
                return Err(RichRecoveryError::InvalidEvidence(
                    "NewOrder recovery line differs from its frozen input",
                ));
            }
            validate_f32_range(
                "NewOrder order-line amount",
                recovery.amount_bits,
                f32::MIN_POSITIVE,
                1_000.0,
            )?;
            validate_bounded_char(
                "NewOrder district information",
                &recovery.district_info,
                DISTRICT_INFO_BYTES,
                DISTRICT_INFO_BYTES,
            )?;
            remote_lines = remote_lines
                .checked_add(u8::from(frozen.supply_warehouse() != route.home_warehouse))
                .ok_or(RichRecoveryError::Overflow("NewOrder remote line count"))?;
            quantity_total = quantity_total
                .checked_add(u32::from(frozen.quantity()))
                .ok_or(RichRecoveryError::Overflow("NewOrder quantity total"))?;
            lines.push(OriginLine {
                number: recovery.number,
                item_id: recovery.item_id,
                supply_warehouse: recovery.supply_warehouse,
                quantity: recovery.quantity,
                amount_bits: recovery.amount_bits,
                district_info: recovery.district_info.clone(),
            });
        }
        if evidence.remote_line_count != remote_lines
            || evidence.stock_ytd_delta != quantity_total
            || input.all_local() != (remote_lines == 0)
        {
            return Err(RichRecoveryError::InvalidEvidence(
                "NewOrder derived line totals differ from its frozen input",
            ));
        }

        let key = OrderKey {
            warehouse_id: evidence.warehouse_id,
            district_id: evidence.district_id,
            order_id: evidence.order_id,
        };
        Ok(PreparedRichTerminal::NewOrder {
            key,
            origin: OrderOrigin {
                customer_id: input.customer_id(),
                entry_timestamp: evidence.entry_timestamp.clone(),
                all_local: input.all_local(),
                lines,
            },
        })
    }

    fn prepare_payment(
        &self,
        ticket: &TransactionTicket,
        input: &PaymentInput,
        evidence: &PaymentEvidence,
    ) -> Result<PreparedRichTerminal, RichRecoveryError> {
        let route = ticket.route();
        if route.home_warehouse > self.warehouses
            || evidence.warehouse_id != route.home_warehouse
            || evidence.district_id != route.home_district
            || evidence.customer_warehouse_id != input.customer_warehouse()
            || evidence.customer_district_id != input.customer_district()
            || evidence.customer_warehouse_id == 0
            || evidence.customer_warehouse_id > self.warehouses
            || evidence.customer_district_id == 0
            || evidence.customer_district_id > DISTRICTS_PER_WAREHOUSE
            || !(1..=i32::from(CUSTOMERS_PER_DISTRICT)).contains(&evidence.customer_id)
            || !(MIN_PAYMENT_CENTS..=MAX_PAYMENT_CENTS).contains(&input.amount_cents())
            || evidence.amount_bits != input.amount_bits()
        {
            return Err(RichRecoveryError::InvalidEvidence(
                "Payment evidence differs from its frozen ticket",
            ));
        }
        validate_customer_transition(
            evidence.customer_version_before,
            evidence.customer_version_after,
            CustomerTransition::Payment,
        )?;
        validate_f32_range(
            "Payment amount",
            evidence.amount_bits,
            MIN_PAYMENT_CENTS as f32 / 100.0,
            MAX_PAYMENT_CENTS as f32 / 100.0,
        )?;
        validate_relative_subtract(
            "Payment Customer balance",
            evidence.customer_balance_before_bits,
            evidence.amount_bits,
            evidence.customer_balance_after_bits,
        )?;
        validate_relative_add(
            "Payment Customer YTD",
            evidence.customer_ytd_before_bits,
            evidence.amount_bits,
            evidence.customer_ytd_after_bits,
        )?;
        validate_bounded_char(
            "Payment History timestamp",
            &evidence.history_timestamp,
            1,
            MAX_HISTORY_TIMESTAMP_BYTES,
        )?;
        validate_bounded_char(
            "Payment History data",
            &evidence.history_data,
            1,
            MAX_HISTORY_DATA_BYTES,
        )?;
        validate_bounded_char(
            "Payment Customer data before",
            &evidence.customer_data_before,
            0,
            MAX_CUSTOMER_DATA_BYTES,
        )?;
        validate_bounded_char(
            "Payment Customer data after",
            &evidence.customer_data_after,
            0,
            MAX_CUSTOMER_DATA_BYTES,
        )?;

        let customer_key = CustomerKey {
            warehouse_id: i32::from(evidence.customer_warehouse_id),
            district_id: i32::from(evidence.customer_district_id),
            customer_id: evidence.customer_id,
        };
        let initial = self
            .initial_customers
            .initial_customer_data(customer_key)
            .ok_or(RichRecoveryError::MissingInitialCustomer(customer_key))?;
        let generated_bad_credit = initial.credit == *b"BC";
        if evidence.customer_is_bad_credit != generated_bad_credit {
            return Err(RichRecoveryError::CustomerCreditFlagMismatch {
                key: customer_key,
                generated: initial.credit,
                claimed_bad_credit: evidence.customer_is_bad_credit,
            });
        }

        let payment_prefix = SealedBadCreditPaymentPrefix {
            home_warehouse_id: evidence.warehouse_id,
            home_district_id: evidence.district_id,
            amount_cents: input.amount_cents(),
        };
        if evidence.customer_is_bad_credit {
            let prefix = bad_credit_prefix(
                evidence.customer_id,
                evidence.customer_district_id,
                evidence.customer_warehouse_id,
                evidence.district_id,
                evidence.warehouse_id,
                input.amount_cents(),
            );
            let expected = prepend_bad_credit_data(&prefix, &evidence.customer_data_before);
            if evidence.customer_data_after != expected {
                return Err(RichRecoveryError::InvalidEvidence(
                    "bad-credit Customer data is not the exact frozen Payment transition",
                ));
            }
        } else if evidence.customer_data_after != evidence.customer_data_before {
            return Err(RichRecoveryError::InvalidEvidence(
                "good-credit Payment changed Customer data",
            ));
        }

        let history_key = HistoryTupleKey {
            group: HistoryGroupKey {
                customer_id: evidence.customer_id,
                customer_district_id: evidence.customer_district_id,
                customer_warehouse_id: evidence.customer_warehouse_id,
                home_district_id: evidence.district_id,
                home_warehouse_id: evidence.warehouse_id,
            },
            timestamp: evidence.history_timestamp.clone(),
            amount_bits: evidence.amount_bits,
            data: evidence.history_data.clone(),
        };
        let bad_customer = if evidence.customer_is_bad_credit {
            Some(PreparedBadCustomer {
                key: customer_key,
                setup_data: initial.data,
                before_version: evidence.customer_version_before,
                after_version: evidence.customer_version_after,
                data_before: evidence.customer_data_before.clone(),
                data_after: evidence.customer_data_after.clone(),
                prefix: payment_prefix,
            })
        } else {
            None
        };
        Ok(PreparedRichTerminal::Payment {
            history: history_key,
            bad_customer,
        })
    }

    fn prepare_delivery(
        &self,
        ticket: &TransactionTicket,
        carrier_id: u8,
        orders: &[DeliveredOrderEvidence],
    ) -> Result<PreparedRichTerminal, RichRecoveryError> {
        let route = ticket.route();
        if route.home_warehouse > self.warehouses
            || !(MIN_CARRIER_ID..=MAX_CARRIER_ID).contains(&carrier_id)
            || orders.len() > usize::from(DISTRICTS_PER_WAREHOUSE)
        {
            return Err(RichRecoveryError::InvalidEvidence(
                "Delivery header differs from its frozen ticket",
            ));
        }
        let mut seen_districts = [false; DISTRICTS_PER_WAREHOUSE as usize];
        let mut terminal_timestamp: Option<&[u8]> = None;
        let mut prepared = Vec::with_capacity(orders.len());
        for order in orders {
            if order.warehouse_id != route.home_warehouse
                || order.district_id == 0
                || order.district_id > DISTRICTS_PER_WAREHOUSE
                || order.order_id <= 0
                || !(1..=i32::from(CUSTOMERS_PER_DISTRICT)).contains(&order.customer_id)
                || order.line_count < MIN_ORDER_LINES
                || order.line_count > MAX_ORDER_LINES
                || order.line_amount_bits.len() != usize::from(order.line_count)
            {
                return Err(RichRecoveryError::InvalidEvidence(
                    "Delivery order evidence is outside the frozen row domain",
                ));
            }
            let district_index = usize::from(order.district_id - 1);
            if seen_districts[district_index] {
                return Err(RichRecoveryError::InvalidEvidence(
                    "Delivery processed one district more than once",
                ));
            }
            seen_districts[district_index] = true;
            validate_bounded_char(
                "Delivery timestamp",
                &order.delivery_timestamp,
                1,
                MAX_DELIVERY_TIMESTAMP_BYTES,
            )?;
            if let Some(expected) = terminal_timestamp {
                if order.delivery_timestamp != expected {
                    return Err(RichRecoveryError::InvalidEvidence(
                        "one Delivery terminal used more than one timestamp",
                    ));
                }
            } else {
                terminal_timestamp = Some(&order.delivery_timestamp);
            }
            for bits in &order.line_amount_bits {
                validate_f32_range("Delivery line amount", *bits, 0.01, 9_999.99)?;
            }
            let expected_amount = sum_f32_as_f64_once(order.line_amount_bits.iter().copied())
                .map_err(|_| {
                    RichRecoveryError::InvalidEvidence(
                        "Delivery line amounts cannot be summed as exact FLOAT32 evidence",
                    )
                })?;
            if expected_amount != order.amount_bits {
                return Err(RichRecoveryError::InvalidEvidence(
                    "Delivery amount differs from the exact order-line sum",
                ));
            }
            validate_customer_transition(
                order.customer_version_before,
                order.customer_version_after,
                CustomerTransition::Delivery,
            )?;
            validate_relative_add(
                "Delivery Customer balance",
                order.customer_balance_before_bits,
                order.amount_bits,
                order.customer_balance_after_bits,
            )?;

            let key = OrderKey {
                warehouse_id: order.warehouse_id,
                district_id: order.district_id,
                order_id: order.order_id,
            };
            let projection = DeliveryProjection {
                customer_id: order.customer_id,
                carrier_id,
                timestamp: order.delivery_timestamp.clone(),
                line_amount_bits: order.line_amount_bits.clone(),
            };
            prepared.push(PreparedDeliveryOrder {
                key,
                projection,
                sample: DeliverySampleState {
                    customer_id: order.customer_id,
                    carrier_id,
                    timestamp: order.delivery_timestamp.clone(),
                    line_amount_bits: order.line_amount_bits.clone(),
                },
            });
        }
        Ok(PreparedRichTerminal::Delivery(prepared))
    }

    fn preflight_terminal(&self, prepared: &PreparedRichTerminal) -> Result<(), RichRecoveryError> {
        match prepared {
            PreparedRichTerminal::Empty => {}
            PreparedRichTerminal::NewOrder { key, origin } => {
                self.new_order_commits
                    .checked_add(1)
                    .ok_or(RichRecoveryError::Overflow("committed NewOrder terminals"))?;
                if let Some(existing) = self.orders.get(key) {
                    if existing.origin.is_some() {
                        return Err(RichRecoveryError::InvalidEvidence(
                            "one order key received more than one NewOrder creation",
                        ));
                    }
                    if let Some(delivery) = &existing.delivery {
                        validate_origin_delivery(origin, delivery)?;
                    }
                }
            }
            PreparedRichTerminal::Payment {
                history,
                bad_customer,
            } => {
                self.history_rows
                    .checked_add(1)
                    .ok_or(RichRecoveryError::Overflow("committed History rows"))?;
                if let Some(existing) = self.histories.get(history) {
                    existing
                        .count
                        .checked_add(1)
                        .ok_or(RichRecoveryError::Overflow("History tuple multiplicity"))?;
                }
                if let Some(customer) = bad_customer {
                    self.bad_credit_payments
                        .checked_add(1)
                        .ok_or(RichRecoveryError::Overflow("bad-credit Payment terminals"))?;
                    // Validates the key domain even if its rank is rejected.
                    validate_customer_key(self.warehouses, customer.key)?;
                    self.preview_bad_customer(customer)?;
                }
            }
            PreparedRichTerminal::Delivery(orders) => {
                self.delivered_orders
                    .checked_add(orders.len() as u64)
                    .ok_or(RichRecoveryError::Overflow("delivered orders"))?;
                for order in orders {
                    if self.deliveries.get(&order.key).is_some() {
                        return Err(RichRecoveryError::InvalidEvidence(
                            "one Delivery order key was offered more than once",
                        ));
                    }
                    if order.key.order_id > INITIAL_ORDER_ID_CEILING {
                        if let Some(existing) = self.orders.get(&order.key) {
                            if existing.delivery.is_some() {
                                return Err(RichRecoveryError::InvalidEvidence(
                                    "one order key was delivered more than once",
                                ));
                            }
                            if let Some(origin) = &existing.origin {
                                validate_origin_delivery(origin, &order.projection)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn preview_bad_customer(
        &self,
        customer: &PreparedBadCustomer,
    ) -> Result<(), RichRecoveryError> {
        let score = bad_customer_score(self.run_seed, customer.key);
        let (mut state, evicted) = if let Some(state) = self.customers.get(&customer.key) {
            (state.clone(), None)
        } else if let Some(state) = self.retired_customers.get(&customer.key) {
            (state.clone(), None)
        } else {
            let Some(evicted) = self.customers.insertion_eviction(score, &customer.key) else {
                return Ok(());
            };
            (
                CustomerDataState::new(self.warehouses, customer.key, customer.setup_data.clone()),
                evicted,
            )
        };
        let old_pending = state.pending.len();
        state.update_count = state
            .update_count
            .checked_add(1)
            .ok_or(RichRecoveryError::Overflow("bad-credit Payment updates"))?;
        apply_customer_data_edge(
            &mut state,
            CustomerDataEdge {
                before_version: customer.before_version,
                after_version: customer.after_version,
                before_data: customer.data_before.clone(),
                after_data: customer.data_after.clone(),
                prefix: customer.prefix,
            },
        )?;
        let projected_pending = self
            .pending_customer_edges
            .checked_sub(old_pending)
            .and_then(|count| count.checked_add(state.pending.len()))
            .ok_or(RichRecoveryError::Overflow(
                "bad-credit pending Payment edges",
            ))?;
        if projected_pending > usize::from(self.clients) {
            return Err(RichRecoveryError::PendingCustomerLimit {
                actual: projected_pending,
                limit: usize::from(self.clients),
            });
        }

        let target_was_retired = self.retired_customers.contains_key(&customer.key);
        let target_remains_retired = target_was_retired && !state.pending.is_empty();
        let evicts_pending = evicted.as_ref().is_some_and(|key| {
            self.customers
                .get(key)
                .is_some_and(|state| !state.pending.is_empty())
        });
        let projected_retired = self
            .retired_customers
            .len()
            .checked_sub(usize::from(target_was_retired))
            .and_then(|count| count.checked_add(usize::from(target_remains_retired)))
            .and_then(|count| count.checked_add(usize::from(evicts_pending)))
            .ok_or(RichRecoveryError::Overflow(
                "retired bad-credit Customer chains",
            ))?;
        if projected_retired > usize::from(self.clients) {
            return Err(RichRecoveryError::RetiredCustomerLimit {
                actual: projected_retired,
                limit: usize::from(self.clients),
            });
        }
        Ok(())
    }

    /// Commit contains no validation, checked arithmetic, or whole-collector
    /// copy. Every recoverable failure was exhausted by `preflight_terminal`.
    fn commit_terminal(&mut self, prepared: PreparedRichTerminal) {
        match prepared {
            PreparedRichTerminal::Empty => {}
            PreparedRichTerminal::NewOrder { key, origin } => {
                let score = order_score(self.run_seed, key);
                if self.orders.ensure(score, key, || MutableOrderState {
                    origin: None,
                    delivery: None,
                }) {
                    self.orders
                        .get_mut(&key)
                        .expect("a retained order has state")
                        .origin = Some(origin);
                }
                self.new_order_commits += 1;
            }
            PreparedRichTerminal::Payment {
                history,
                bad_customer,
            } => {
                let score = history_score(self.run_seed, &history);
                if self
                    .histories
                    .ensure(score, history.clone(), || HistoryMultiplicity { count: 0 })
                {
                    self.histories
                        .get_mut(&history)
                        .expect("a retained History tuple has multiplicity")
                        .count += 1;
                }
                self.history_rows += 1;

                if let Some(customer) = bad_customer {
                    let score = bad_customer_score(self.run_seed, customer.key);
                    if let Some(mut state) = self.retired_customers.remove(&customer.key) {
                        self.pending_customer_edges -= state.pending.len();
                        state.update_count += 1;
                        apply_customer_data_edge(
                            &mut state,
                            CustomerDataEdge {
                                before_version: customer.before_version,
                                after_version: customer.after_version,
                                before_data: customer.data_before,
                                after_data: customer.data_after,
                                prefix: customer.prefix,
                            },
                        )
                        .expect("preflight validated the bad-credit data edge");
                        self.pending_customer_edges += state.pending.len();
                        if state.pending.is_empty() {
                            validate_rooted_customer_state(&state)
                                .expect("preflight validated the retired rooted chain");
                        } else {
                            self.retired_customers.insert(customer.key, state);
                        }
                    } else if self.customers.get(&customer.key).is_some() {
                        let mut state = self
                            .customers
                            .get(&customer.key)
                            .cloned()
                            .expect("a retained bad-credit Customer has state");
                        self.pending_customer_edges -= state.pending.len();
                        state.update_count += 1;
                        apply_customer_data_edge(
                            &mut state,
                            CustomerDataEdge {
                                before_version: customer.before_version,
                                after_version: customer.after_version,
                                before_data: customer.data_before,
                                after_data: customer.data_after,
                                prefix: customer.prefix,
                            },
                        )
                        .expect("preflight validated the bad-credit data edge");
                        self.pending_customer_edges += state.pending.len();
                        *self
                            .customers
                            .get_mut(&customer.key)
                            .expect("retained bad-credit Customer remains selected") = state;
                    } else if let Some(evicted) =
                        self.customers.insertion_eviction(score, &customer.key)
                    {
                        if let Some(evicted_key) = evicted {
                            let (ranked, state) = self
                                .customers
                                .take(&evicted_key)
                                .expect("preflight selected an existing eviction key");
                            self.customers.observe_rejected(ranked);
                            if !state.pending.is_empty() {
                                self.retired_customers.insert(evicted_key, state);
                            }
                        }
                        let mut state = CustomerDataState::new(
                            self.warehouses,
                            customer.key,
                            customer.setup_data,
                        );
                        state.update_count = 1;
                        apply_customer_data_edge(
                            &mut state,
                            CustomerDataEdge {
                                before_version: customer.before_version,
                                after_version: customer.after_version,
                                before_data: customer.data_before,
                                after_data: customer.data_after,
                                prefix: customer.prefix,
                            },
                        )
                        .expect("preflight validated the new bad-credit data edge");
                        self.pending_customer_edges += state.pending.len();
                        assert!(self.customers.ensure(score, customer.key, || state));
                    } else {
                        self.customers.observe_rejected(RankedKey {
                            score,
                            key: customer.key,
                        });
                    }
                    self.bad_credit_payments += 1;
                }
            }
            PreparedRichTerminal::Delivery(orders) => {
                for order in orders {
                    if order.key.order_id > INITIAL_ORDER_ID_CEILING {
                        let score = order_score(self.run_seed, order.key);
                        if self.orders.ensure(score, order.key, || MutableOrderState {
                            origin: None,
                            delivery: None,
                        }) {
                            self.orders
                                .get_mut(&order.key)
                                .expect("a retained runtime order has state")
                                .delivery = Some(order.projection);
                        }
                    }
                    let score = delivery_score(self.run_seed, order.key);
                    self.deliveries.ensure(score, order.key, || order.sample);
                    self.delivered_orders += 1;
                }
            }
        }
    }

    /// Seal every rich domain after binding it to the one numeric interval
    /// section produced by the same terminal collector.
    ///
    /// Bad-credit keys are sampled independently and need not occur in the
    /// numeric Customer bottom-k set. This binding therefore checks the run
    /// identity, not a probabilistic key intersection.
    pub fn seal(
        self,
        intervals: &SealedIntervalEvidence,
    ) -> Result<SealedRichRecoverySamples, RichRecoveryError> {
        if let Some(cause) = self.poisoned {
            return Err(RichRecoveryError::Poisoned(cause));
        }
        if intervals.warehouses() != self.warehouses || intervals.sample_seed() != self.run_seed {
            return Err(RichRecoveryError::IntervalBindingMismatch);
        }
        let recomputed_pending = self
            .customers
            .entries
            .values()
            .chain(self.retired_customers.values())
            .try_fold(0_usize, |total, state| {
                total
                    .checked_add(state.pending.len())
                    .ok_or(RichRecoveryError::Overflow(
                        "bad-credit pending Payment edges",
                    ))
            })?;
        if recomputed_pending != self.pending_customer_edges
            || self
                .retired_customers
                .values()
                .any(|state| state.pending.is_empty())
            || self.retired_customers.len() > self.pending_customer_edges
        {
            return Err(RichRecoveryError::InvalidEvidence(
                "bad-credit pending and retired chain accounting is inconsistent",
            ));
        }
        if self.pending_customer_edges != 0 {
            return Err(RichRecoveryError::DisconnectedCustomerData {
                pending: self.pending_customer_edges,
            });
        }
        let selected_orders = self.orders.entries.len() as u64;
        self.orders.validate_cutoff(
            "NewOrder final state",
            selected_orders,
            self.new_order_commits,
        )?;
        let selected_deliveries = self.deliveries.entries.len() as u64;
        self.deliveries.validate_cutoff(
            "Delivery final state",
            selected_deliveries,
            self.delivered_orders,
        )?;
        let selected_bad_updates =
            self.customers
                .entries
                .values()
                .try_fold(0_u64, |total, state| {
                    validate_rooted_customer_state(state)?;
                    total
                        .checked_add(state.update_count)
                        .ok_or(RichRecoveryError::Overflow(
                            "selected bad-credit Payment updates",
                        ))
                })?;
        self.customers.validate_cutoff(
            "bad-credit Customer data",
            selected_bad_updates,
            self.bad_credit_payments,
        )?;
        let selected_history_rows =
            self.histories
                .entries
                .values()
                .try_fold(0_u64, |total, multiplicity| {
                    total
                        .checked_add(multiplicity.count)
                        .ok_or(RichRecoveryError::Overflow(
                            "selected History multiplicities",
                        ))
                })?;
        self.histories.validate_cutoff(
            "History tuple",
            selected_history_rows,
            self.history_rows,
        )?;

        if self.new_order_commits > 0 && self.orders.entries.is_empty() {
            return Err(RichRecoveryError::InvalidEvidence(
                "committed NewOrder domain has no retained sample",
            ));
        }
        if self.delivered_orders > 0 && self.deliveries.entries.is_empty() {
            return Err(RichRecoveryError::InvalidEvidence(
                "committed Delivery domain has no retained sample",
            ));
        }
        if self.bad_credit_payments > 0 && self.customers.entries.is_empty() {
            return Err(RichRecoveryError::InvalidEvidence(
                "bad-credit Payment domain has no retained sample",
            ));
        }
        if self.history_rows > 0 && self.histories.entries.is_empty() {
            return Err(RichRecoveryError::InvalidEvidence(
                "committed History domain has no retained sample",
            ));
        }

        let order_rejected = self
            .orders
            .rejected
            .as_ref()
            .map(|ranked| OrderCutoffWitness {
                score: ranked.score,
                key: ranked.key,
            });
        let delivery_rejected =
            self.deliveries
                .rejected
                .as_ref()
                .map(|ranked| OrderCutoffWitness {
                    score: ranked.score,
                    key: ranked.key,
                });
        let customer_rejected =
            self.customers
                .rejected
                .as_ref()
                .map(|ranked| CustomerCutoffWitness {
                    score: ranked.score,
                    key: ranked.key,
                });
        let history_rejected =
            self.histories
                .rejected
                .as_ref()
                .map(|ranked| HistoryCutoffWitness {
                    score: ranked.score,
                    group: ranked.key.group,
                    timestamp: ranked.key.timestamp.clone(),
                    amount_bits: ranked.key.amount_bits,
                    data: ranked.key.data.clone(),
                });

        let mut new_orders = Vec::with_capacity(self.orders.entries.len());
        for (ranked, state) in self.orders.entries {
            let origin = state
                .origin
                .ok_or(RichRecoveryError::MissingNewOrderOrigin(ranked.key))?;
            if let Some(delivery) = &state.delivery {
                validate_origin_delivery(&origin, delivery)?;
            }
            let delivery_timestamp = state
                .delivery
                .as_ref()
                .map(|delivery| delivery.timestamp.clone())
                .unwrap_or_default();
            let lines: Vec<SealedOrderLine> = origin
                .lines
                .into_iter()
                .map(|line| SealedOrderLine {
                    number: line.number,
                    item_id: line.item_id,
                    supply_warehouse: line.supply_warehouse,
                    delivery_timestamp: delivery_timestamp.clone(),
                    quantity: line.quantity,
                    amount_bits: line.amount_bits,
                    district_info: line.district_info,
                })
                .collect();
            new_orders.push(SealedNewOrderSample {
                score: ranked.score,
                key: ranked.key,
                customer_id: origin.customer_id,
                entry_timestamp: origin.entry_timestamp,
                carrier_id: state
                    .delivery
                    .as_ref()
                    .map_or(0, |delivery| delivery.carrier_id),
                line_count: u8::try_from(lines.len())
                    .expect("validated NewOrder line count fits u8"),
                all_local: origin.all_local,
                queue_present: state.delivery.is_none(),
                lines,
            });
        }

        let mut deliveries = Vec::with_capacity(self.deliveries.entries.len());
        for (ranked, state) in self.deliveries.entries {
            let lines = state
                .line_amount_bits
                .into_iter()
                .enumerate()
                .map(|(index, amount_bits)| SealedDeliveryLine {
                    number: (index + 1) as u8,
                    delivery_timestamp: state.timestamp.clone(),
                    amount_bits,
                })
                .collect();
            deliveries.push(SealedDeliverySample {
                score: ranked.score,
                key: ranked.key,
                customer_id: state.customer_id,
                carrier_id: state.carrier_id,
                queue_present: false,
                delivery_timestamp: state.timestamp,
                lines,
            });
        }

        let mut bad_credit_customers = Vec::with_capacity(self.customers.entries.len());
        for (ranked, state) in self.customers.entries {
            validate_rooted_customer_state(&state)?;
            bad_credit_customers.push(SealedBadCreditCustomerSample {
                score: ranked.score,
                key: ranked.key,
                final_payment_count: state.endpoint.payment_count,
                credit: *b"BC",
                data: state.endpoint_data,
                committed_payment_updates: state.update_count,
                payment_suffix: state.payment_suffix,
            });
        }

        let mut grouped = BTreeMap::<HistoryGroupKey, Vec<SealedHistoryTuple>>::new();
        for (ranked, multiplicity) in self.histories.entries {
            let setup_collision =
                setup_history_collision(self.initial_history.as_ref(), &ranked.key)?;
            grouped
                .entry(ranked.key.group)
                .or_default()
                .push(SealedHistoryTuple {
                    score: ranked.score,
                    timestamp: ranked.key.timestamp,
                    amount_bits: ranked.key.amount_bits,
                    data: ranked.key.data,
                    committed_multiplicity: multiplicity.count,
                    setup_collision_multiplicity: setup_collision,
                });
        }
        let history_groups = grouped
            .into_iter()
            .map(|(key, tuples)| SealedHistoryGroup { key, tuples })
            .collect::<Vec<_>>();

        let raw_size_bytes = sealed_raw_size(
            &new_orders,
            &deliveries,
            &bad_credit_customers,
            &history_groups,
            order_rejected.as_ref(),
            delivery_rejected.as_ref(),
            customer_rejected.as_ref(),
            history_rejected.as_ref(),
        )?;
        if raw_size_bytes > MAX_RICH_RECOVERY_RAW_BYTES {
            return Err(RichRecoveryError::RawSizeCeiling {
                actual: raw_size_bytes,
                limit: MAX_RICH_RECOVERY_RAW_BYTES,
            });
        }
        Ok(SealedRichRecoverySamples {
            warehouses: self.warehouses,
            run_seed: self.run_seed,
            policy_version: RICH_RECOVERY_POLICY_VERSION,
            raw_size_bytes,
            new_order_commits: self.new_order_commits,
            delivered_orders: self.delivered_orders,
            history_rows: self.history_rows,
            bad_credit_payments: self.bad_credit_payments,
            new_orders,
            deliveries,
            bad_credit_customers,
            history_groups,
            order_rejected,
            delivery_rejected,
            customer_rejected,
            history_rejected,
        })
    }

    fn raw_size_bytes(&self) -> Result<usize, RichRecoveryError> {
        let mut size = 64_usize;
        for (ranked, state) in &self.orders.entries {
            size = checked_size_add(size, 32)?;
            size = checked_size_add(size, order_key_size(&ranked.key))?;
            if let Some(origin) = &state.origin {
                size = checked_size_add(size, 16 + origin.entry_timestamp.len())?;
                for line in &origin.lines {
                    size = checked_size_add(size, 16 + line.district_info.len())?;
                }
            }
            if let Some(delivery) = &state.delivery {
                size = checked_size_add(
                    size,
                    8 + delivery.timestamp.len() + delivery.line_amount_bits.len() * 4,
                )?;
            }
        }
        for (ranked, delivery) in &self.deliveries.entries {
            size = checked_size_add(
                size,
                48 + order_key_size(&ranked.key)
                    + delivery.timestamp.len()
                    + delivery.line_amount_bits.len() * 4,
            )?;
        }
        for (ranked, customer) in &self.customers.entries {
            size = checked_size_add(size, 48 + customer_key_size(ranked.key))?;
            size = checked_size_add(size, customer.endpoint_data.len())?;
            size = checked_size_add(size, customer.payment_suffix.len() * (2 + 1 + 4))?;
        }
        for (ranked, _) in &self.histories.entries {
            size = checked_size_add(
                size,
                48 + history_group_size(ranked.key.group)
                    + ranked.key.timestamp.len()
                    + ranked.key.data.len(),
            )?;
        }
        Ok(size)
    }
}

#[derive(Debug, Error, Clone, Eq, PartialEq)]
pub enum RichRecoveryError {
    #[error("invalid rich recovery configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("invalid rich recovery evidence: {0}")]
    InvalidEvidence(&'static str),
    #[error("unsupported rich recovery policy {actual}, expected {expected}")]
    UnsupportedPolicy { actual: u32, expected: u32 },
    #[error("{domain} canonical count is {actual}, expected {minimum}..={maximum}")]
    CanonicalCount {
        domain: &'static str,
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    #[error("{domain} persisted sample score is not canonical")]
    ForgedCanonicalScore { domain: &'static str },
    #[error("{domain} persisted samples are not in strict canonical rank order")]
    NonCanonicalOrder { domain: &'static str },
    #[error(
        "{field} CHAR bytes are unsafe: length {actual}, expected {minimum}..={maximum}, valid UTF-8 with no NUL or quote"
    )]
    InvalidChar {
        field: &'static str,
        actual: usize,
        minimum: usize,
        maximum: usize,
    },
    #[error("{field} has invalid FLOAT32 bits 0x{bits:08x}")]
    InvalidFloat { field: &'static str, bits: u32 },
    #[error("rich recovery collector is poisoned: {0}")]
    Poisoned(String),
    #[error("rich recovery counter overflow: {0}")]
    Overflow(&'static str),
    #[error("rich recovery raw evidence is {actual} bytes, limit is {limit}")]
    RawSizeCeiling { actual: usize, limit: usize },
    #[error("rich recovery raw-size metadata is {encoded}, recomputed value is {computed}")]
    RawSizeMismatch { encoded: usize, computed: usize },
    #[error(
        "History setup-collision multiplicity is {encoded}, trusted setup provider reports {trusted}"
    )]
    SetupCollisionMismatch { encoded: u8, trusted: u8 },
    #[error("{domain} weighted bottom-k evidence is inconsistent with its global count")]
    InvalidWeightedSelection { domain: &'static str },
    #[error("retained NewOrder {0:?} has no committed creation terminal")]
    MissingNewOrderOrigin(OrderKey),
    #[error("initial History provider has no setup row for Customer {0:?}")]
    MissingInitialHistory(CustomerKey),
    #[error("initial Customer provider has no setup row for Customer {0:?}")]
    MissingInitialCustomer(CustomerKey),
    #[error(
        "Customer {key:?} credit flag disagrees with generated credit {generated:?}: claimed_bad_credit={claimed_bad_credit}"
    )]
    CustomerCreditFlagMismatch {
        key: CustomerKey,
        generated: [u8; 2],
        claimed_bad_credit: bool,
    },
    #[error("bad-credit Customer data has {actual} pending edges, limit is {limit}")]
    PendingCustomerLimit { actual: usize, limit: usize },
    #[error("bad-credit Customer data has {actual} retired chains, limit is {limit}")]
    RetiredCustomerLimit { actual: usize, limit: usize },
    #[error("bad-credit Customer data has {pending} disconnected edges at seal")]
    DisconnectedCustomerData { pending: usize },
    #[error("rich recovery and numeric interval run bindings differ")]
    IntervalBindingMismatch,
}

fn checked_size_add(left: usize, right: usize) -> Result<usize, RichRecoveryError> {
    left.checked_add(right)
        .ok_or(RichRecoveryError::Overflow("raw sample size"))
}

fn validate_canonical_count(
    domain: &'static str,
    actual: usize,
    minimum: usize,
    maximum: usize,
) -> Result<(), RichRecoveryError> {
    if !(minimum..=maximum).contains(&actual) {
        return Err(RichRecoveryError::CanonicalCount {
            domain,
            actual,
            minimum,
            maximum,
        });
    }
    Ok(())
}

fn collect_canonical_exact<T, I>(
    domain: &'static str,
    values: I,
    minimum: usize,
    maximum: usize,
) -> Result<Vec<T>, RichRecoveryError>
where
    I: ExactSizeIterator<Item = T>,
{
    let declared = values.len();
    validate_canonical_count(domain, declared, minimum, maximum)?;
    let mut collected = Vec::with_capacity(declared);
    for (index, value) in values.enumerate() {
        if index >= declared || index >= maximum {
            return Err(RichRecoveryError::CanonicalCount {
                domain,
                actual: index.saturating_add(1),
                minimum: declared,
                maximum: declared,
            });
        }
        collected.push(value);
    }
    validate_exact_iterator_end(domain, declared, collected.len())?;
    Ok(collected)
}

fn validate_exact_iterator_step(
    domain: &'static str,
    zero_based_index: usize,
    declared: usize,
) -> Result<(), RichRecoveryError> {
    if zero_based_index >= declared {
        return Err(RichRecoveryError::CanonicalCount {
            domain,
            actual: zero_based_index.saturating_add(1),
            minimum: declared,
            maximum: declared,
        });
    }
    Ok(())
}

fn validate_exact_iterator_end(
    domain: &'static str,
    declared: usize,
    consumed: usize,
) -> Result<(), RichRecoveryError> {
    if declared != consumed {
        return Err(RichRecoveryError::CanonicalCount {
            domain,
            actual: consumed,
            minimum: declared,
            maximum: declared,
        });
    }
    Ok(())
}

fn validate_order_key(
    warehouses: u16,
    key: OrderKey,
    requires_runtime_origin: bool,
) -> Result<(), RichRecoveryError> {
    let minimum_order_id = if requires_runtime_origin {
        INITIAL_ORDER_ID_CEILING
            .checked_add(1)
            .ok_or(RichRecoveryError::Overflow("runtime Order id floor"))?
    } else {
        1
    };
    if key.warehouse_id == 0
        || key.warehouse_id > warehouses
        || key.district_id == 0
        || key.district_id > DISTRICTS_PER_WAREHOUSE
        || key.order_id < minimum_order_id
    {
        return Err(RichRecoveryError::InvalidEvidence(
            if requires_runtime_origin {
                "canonical NewOrder key is outside the configured runtime row domain"
            } else {
                "canonical Delivery key is outside the configured row domain"
            },
        ));
    }
    Ok(())
}

fn validate_history_group(warehouses: u16, key: HistoryGroupKey) -> Result<(), RichRecoveryError> {
    if !(1..=i32::from(CUSTOMERS_PER_DISTRICT)).contains(&key.customer_id)
        || key.customer_district_id == 0
        || key.customer_district_id > DISTRICTS_PER_WAREHOUSE
        || key.customer_warehouse_id == 0
        || key.customer_warehouse_id > warehouses
        || key.home_district_id == 0
        || key.home_district_id > DISTRICTS_PER_WAREHOUSE
        || key.home_warehouse_id == 0
        || key.home_warehouse_id > warehouses
    {
        return Err(RichRecoveryError::InvalidEvidence(
            "canonical History group key is outside the configured row domain",
        ));
    }
    Ok(())
}

fn validate_decoded_rank<K>(
    domain: &'static str,
    encoded_score: SampleScore,
    expected_score: SampleScore,
    key: K,
    previous: &mut Option<RankedKey<K>>,
) -> Result<(), RichRecoveryError>
where
    K: Clone + Ord,
{
    if encoded_score != expected_score {
        return Err(RichRecoveryError::ForgedCanonicalScore { domain });
    }
    let current = RankedKey {
        score: expected_score,
        key,
    };
    if previous.as_ref().is_some_and(|prior| prior >= &current) {
        return Err(RichRecoveryError::NonCanonicalOrder { domain });
    }
    *previous = Some(current);
    Ok(())
}

fn validate_order_delivery_intersections(
    orders: &[SealedNewOrderSample],
    deliveries: &[SealedDeliverySample],
) -> Result<(), RichRecoveryError> {
    let deliveries_by_key = deliveries
        .iter()
        .map(|delivery| (delivery.key, delivery))
        .collect::<BTreeMap<_, _>>();
    for order in orders {
        let Some(delivery) = deliveries_by_key.get(&order.key) else {
            continue;
        };
        let header_matches = !order.queue_present
            && i32::from(order.customer_id) == delivery.customer_id
            && order.carrier_id == delivery.carrier_id
            && order.lines.len() == delivery.lines.len()
            && order.lines.first().is_some_and(|line| {
                line.delivery_timestamp.as_slice() == delivery.delivery_timestamp.as_slice()
            });
        let lines_match =
            order
                .lines
                .iter()
                .zip(&delivery.lines)
                .all(|(order_line, delivery_line)| {
                    order_line.number == delivery_line.number
                        && order_line.amount_bits == delivery_line.amount_bits
                        && order_line.delivery_timestamp == delivery_line.delivery_timestamp
                });
        if !header_matches || !lines_match {
            return Err(RichRecoveryError::InvalidEvidence(
                "canonical NewOrder and Delivery samples disagree for one retained order",
            ));
        }
    }
    Ok(())
}

fn decode_order_witness(
    header: CanonicalRichRecoveryHeader,
    encoded: Option<CanonicalRichOrderWitness>,
    requires_runtime_origin: bool,
    score_fn: fn(u64, OrderKey) -> SampleScore,
) -> Result<(Option<OrderCutoffWitness>, Option<RankedKey<OrderKey>>), RichRecoveryError> {
    let Some(encoded) = encoded else {
        return Ok((None, None));
    };
    validate_order_key(header.warehouses, encoded.key, requires_runtime_origin)?;
    let expected_score = score_fn(header.run_seed, encoded.key);
    if encoded.score != expected_score {
        return Err(RichRecoveryError::ForgedCanonicalScore {
            domain: if requires_runtime_origin {
                "NewOrder rejection witness"
            } else {
                "Delivery rejection witness"
            },
        });
    }
    Ok((
        Some(OrderCutoffWitness {
            score: expected_score,
            key: encoded.key,
        }),
        Some(RankedKey {
            score: expected_score,
            key: encoded.key,
        }),
    ))
}

fn decode_customer_witness(
    header: CanonicalRichRecoveryHeader,
    encoded: Option<CanonicalRichCustomerWitness>,
) -> Result<
    (
        Option<CustomerCutoffWitness>,
        Option<RankedKey<CustomerKey>>,
    ),
    RichRecoveryError,
> {
    let Some(encoded) = encoded else {
        return Ok((None, None));
    };
    validate_customer_key(header.warehouses, encoded.key)?;
    let expected_score = bad_customer_score(header.run_seed, encoded.key);
    if encoded.score != expected_score {
        return Err(RichRecoveryError::ForgedCanonicalScore {
            domain: "bad-credit Customer rejection witness",
        });
    }
    Ok((
        Some(CustomerCutoffWitness {
            score: expected_score,
            key: encoded.key,
        }),
        Some(RankedKey {
            score: expected_score,
            key: encoded.key,
        }),
    ))
}

fn decode_history_witness(
    header: CanonicalRichRecoveryHeader,
    encoded: Option<CanonicalRichHistoryWitness>,
) -> Result<
    (
        Option<HistoryCutoffWitness>,
        Option<RankedKey<HistoryTupleKey>>,
    ),
    RichRecoveryError,
> {
    let Some(encoded) = encoded else {
        return Ok((None, None));
    };
    validate_history_group(header.warehouses, encoded.group)?;
    validate_bounded_char(
        "canonical History rejection timestamp",
        &encoded.timestamp,
        1,
        MAX_HISTORY_TIMESTAMP_BYTES,
    )?;
    validate_f32_range(
        "canonical History rejection amount",
        encoded.amount_bits,
        MIN_PAYMENT_CENTS as f32 / 100.0,
        MAX_PAYMENT_CENTS as f32 / 100.0,
    )?;
    validate_bounded_char(
        "canonical History rejection data",
        &encoded.data,
        1,
        MAX_HISTORY_DATA_BYTES,
    )?;
    let key = HistoryTupleKey {
        group: encoded.group,
        timestamp: encoded.timestamp.clone(),
        amount_bits: encoded.amount_bits,
        data: encoded.data.clone(),
    };
    let expected_score = history_score(header.run_seed, &key);
    if encoded.score != expected_score {
        return Err(RichRecoveryError::ForgedCanonicalScore {
            domain: "History rejection witness",
        });
    }
    Ok((
        Some(HistoryCutoffWitness {
            score: expected_score,
            group: encoded.group,
            timestamp: encoded.timestamp,
            amount_bits: encoded.amount_bits,
            data: encoded.data,
        }),
        Some(RankedKey {
            score: expected_score,
            key,
        }),
    ))
}

#[allow(clippy::too_many_arguments)]
fn validate_decoded_cutoff<K>(
    domain: &'static str,
    selected_count: usize,
    capacity: usize,
    selected_weight: u64,
    global_weight: u64,
    selected_cutoff: Option<&RankedKey<K>>,
    rejected: Option<&RankedKey<K>>,
) -> Result<(), RichRecoveryError>
where
    K: Ord,
{
    validate_canonical_count(domain, selected_count, 0, capacity)?;
    if (global_weight == 0) != (selected_count == 0)
        || u64::try_from(selected_count).map_or(true, |count| count > selected_weight)
        || selected_weight > global_weight
    {
        return Err(RichRecoveryError::InvalidWeightedSelection { domain });
    }
    match rejected {
        None if selected_weight == global_weight => Ok(()),
        None => Err(RichRecoveryError::InvalidWeightedSelection { domain }),
        Some(rejected) => {
            if selected_count != capacity || selected_weight >= global_weight {
                return Err(RichRecoveryError::InvalidWeightedSelection { domain });
            }
            let Some(selected_cutoff) = selected_cutoff else {
                return Err(RichRecoveryError::InvalidWeightedSelection { domain });
            };
            if selected_cutoff >= rejected {
                return Err(RichRecoveryError::InvalidEvidence(
                    "canonical rejection witness does not follow the selected cutoff",
                ));
            }
            Ok(())
        }
    }
}

const fn order_key_size(_: &OrderKey) -> usize {
    2 + 1 + 4
}

const fn customer_key_size(_: CustomerKey) -> usize {
    4 + 4 + 4
}

const fn history_group_size(_: HistoryGroupKey) -> usize {
    4 + 1 + 2 + 1 + 2
}

enum CustomerTransition {
    Payment,
    Delivery,
}

fn validate_customer_transition(
    before: CustomerVersion,
    after: CustomerVersion,
    transition: CustomerTransition,
) -> Result<(), RichRecoveryError> {
    if before.payment_count < CUSTOMER_INITIAL_PAYMENT_COUNT
        || before.delivery_count < CUSTOMER_INITIAL_DELIVERY_COUNT
        || after.payment_count < CUSTOMER_INITIAL_PAYMENT_COUNT
        || after.delivery_count < CUSTOMER_INITIAL_DELIVERY_COUNT
    {
        return Err(RichRecoveryError::InvalidEvidence(
            "Customer logical version precedes its setup root",
        ));
    }
    let valid = match transition {
        CustomerTransition::Payment => {
            before.payment_count.checked_add(1) == Some(after.payment_count)
                && before.delivery_count == after.delivery_count
        }
        CustomerTransition::Delivery => {
            before.payment_count == after.payment_count
                && before.delivery_count.checked_add(1) == Some(after.delivery_count)
        }
    };
    if !valid {
        return Err(RichRecoveryError::InvalidEvidence(
            "Customer logical version transition is not exact",
        ));
    }
    Ok(())
}

fn apply_customer_data_edge(
    state: &mut CustomerDataState,
    edge: CustomerDataEdge,
) -> Result<(), RichRecoveryError> {
    let prefix_bytes =
        validate_bad_credit_payment_prefix(state.warehouses, state.key, edge.prefix)?;
    if prepend_bad_credit_data(&prefix_bytes, &edge.before_data) != edge.after_data {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Payment prefix does not produce its after-data",
        ));
    }
    if edge.before_version.payment_count < state.endpoint.payment_count {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Payment edge precedes the rooted data endpoint",
        ));
    }

    if edge.before_version.payment_count == state.endpoint.payment_count {
        apply_rooted_customer_data_edge(state, edge)?;
        while let Some(next) = state.pending.remove(&state.endpoint.payment_count) {
            apply_rooted_customer_data_edge(state, next)?;
        }
        return Ok(());
    }

    let start = edge.before_version.payment_count;
    if state.pending.contains_key(&start) {
        return Err(RichRecoveryError::InvalidEvidence(
            "one bad-credit Payment edge was offered more than once",
        ));
    }
    if let Some((_, predecessor)) = state.pending.range(..start).next_back() {
        if predecessor.after_version.payment_count == start
            && (predecessor.after_data != edge.before_data
                || edge.before_version.delivery_count < predecessor.after_version.delivery_count)
        {
            return Err(RichRecoveryError::InvalidEvidence(
                "bad-credit pending Payment edges disagree",
            ));
        }
    }
    if let Some((_, successor)) = state.pending.range(start..).next() {
        if edge.after_version.payment_count == successor.before_version.payment_count
            && (edge.after_data != successor.before_data
                || successor.before_version.delivery_count < edge.after_version.delivery_count)
        {
            return Err(RichRecoveryError::InvalidEvidence(
                "bad-credit pending Payment edges disagree",
            ));
        }
    }
    state.pending.insert(start, edge);
    Ok(())
}

fn apply_rooted_customer_data_edge(
    state: &mut CustomerDataState,
    edge: CustomerDataEdge,
) -> Result<(), RichRecoveryError> {
    if state.endpoint_data != edge.before_data {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Payment before-data does not continue the rooted chain",
        ));
    }
    if edge.before_version.payment_count != state.endpoint.payment_count
        || edge.before_version.delivery_count < state.endpoint.delivery_count
    {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Customer logical version does not continue the rooted chain",
        ));
    }
    let mut suffix = state.payment_suffix.clone();
    append_bad_credit_suffix(state.warehouses, state.key, &mut suffix, edge.prefix)?;
    state.endpoint = edge.after_version;
    state.endpoint_data = edge.after_data;
    state.payment_suffix = suffix;
    Ok(())
}

fn validate_rooted_customer_state(state: &CustomerDataState) -> Result<(), RichRecoveryError> {
    if !state.pending.is_empty() {
        return Err(RichRecoveryError::DisconnectedCustomerData {
            pending: state.pending.len(),
        });
    }
    let expected_payment_count = i64::from(CUSTOMER_INITIAL_PAYMENT_COUNT)
        .checked_add(
            i64::try_from(state.update_count)
                .map_err(|_| RichRecoveryError::Overflow("bad-credit Customer update count"))?,
        )
        .ok_or(RichRecoveryError::Overflow(
            "bad-credit Customer payment count",
        ))?;
    if i64::from(state.endpoint.payment_count) != expected_payment_count {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit c_data chain is not complete and rooted",
        ));
    }
    validate_bad_credit_suffix(
        state.warehouses,
        state.key,
        state.update_count,
        &state.setup_data,
        &state.endpoint_data,
        &state.payment_suffix,
    )
}

fn validate_origin_delivery(
    origin: &OrderOrigin,
    delivery: &DeliveryProjection,
) -> Result<(), RichRecoveryError> {
    if i32::from(origin.customer_id) != delivery.customer_id {
        return Err(RichRecoveryError::InvalidEvidence(
            "Delivery Customer differs from retained NewOrder state",
        ));
    }
    if origin.lines.len() != delivery.line_amount_bits.len() {
        return Err(RichRecoveryError::InvalidEvidence(
            "Delivery line count differs from retained NewOrder state",
        ));
    }
    if origin
        .lines
        .iter()
        .zip(&delivery.line_amount_bits)
        .any(|(line, amount)| line.amount_bits != *amount)
    {
        return Err(RichRecoveryError::InvalidEvidence(
            "Delivery line amounts differ from retained NewOrder state",
        ));
    }
    Ok(())
}

fn validate_bounded_char(
    field: &'static str,
    value: &[u8],
    minimum: usize,
    maximum: usize,
) -> Result<(), RichRecoveryError> {
    if !(minimum..=maximum).contains(&value.len())
        || value.contains(&0)
        || value.contains(&b'\'')
        || std::str::from_utf8(value).is_err()
    {
        return Err(RichRecoveryError::InvalidChar {
            field,
            actual: value.len(),
            minimum,
            maximum,
        });
    }
    Ok(())
}

fn validate_f32_range(
    field: &'static str,
    bits: u32,
    minimum: f32,
    maximum: f32,
) -> Result<f32, RichRecoveryError> {
    let value = f32::from_bits(bits);
    if !value.is_finite() || value < minimum || value > maximum {
        return Err(RichRecoveryError::InvalidFloat { field, bits });
    }
    Ok(value)
}

fn validate_finite(field: &'static str, bits: u32) -> Result<f32, RichRecoveryError> {
    let value = f32::from_bits(bits);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(RichRecoveryError::InvalidFloat { field, bits })
    }
}

fn validate_relative_add(
    field: &'static str,
    before_bits: u32,
    amount_bits: u32,
    after_bits: u32,
) -> Result<(), RichRecoveryError> {
    let before = validate_finite(field, before_bits)?;
    let amount = validate_finite(field, amount_bits)?;
    validate_finite(field, after_bits)?;
    if (before + amount).to_bits() != after_bits {
        return Err(RichRecoveryError::InvalidEvidence(
            "relative FLOAT32 addition is not exact RNE",
        ));
    }
    Ok(())
}

fn validate_relative_subtract(
    field: &'static str,
    before_bits: u32,
    amount_bits: u32,
    after_bits: u32,
) -> Result<(), RichRecoveryError> {
    let before = validate_finite(field, before_bits)?;
    let amount = validate_finite(field, amount_bits)?;
    validate_finite(field, after_bits)?;
    if (before - amount).to_bits() != after_bits {
        return Err(RichRecoveryError::InvalidEvidence(
            "relative FLOAT32 subtraction is not exact RNE",
        ));
    }
    Ok(())
}

fn bad_credit_prefix(
    customer_id: i32,
    customer_district: u8,
    customer_warehouse: u16,
    home_district: u8,
    home_warehouse: u16,
    amount_cents: u32,
) -> Vec<u8> {
    format!(
        "{customer_id} {customer_district} {customer_warehouse} \
         {home_district} {home_warehouse} {}.{:02} ",
        amount_cents / 100,
        amount_cents % 100
    )
    .into_bytes()
}

fn validate_bad_credit_payment_prefix(
    warehouses: u16,
    customer: CustomerKey,
    prefix: SealedBadCreditPaymentPrefix,
) -> Result<Vec<u8>, RichRecoveryError> {
    validate_customer_key(warehouses, customer)?;
    if prefix.home_warehouse_id == 0
        || prefix.home_warehouse_id > warehouses
        || prefix.home_district_id == 0
        || prefix.home_district_id > DISTRICTS_PER_WAREHOUSE
        || !(MIN_PAYMENT_CENTS..=MAX_PAYMENT_CENTS).contains(&prefix.amount_cents)
    {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Payment suffix entry is outside the configured row domain",
        ));
    }
    let customer_warehouse = u16::try_from(customer.warehouse_id)
        .map_err(|_| RichRecoveryError::InvalidEvidence("invalid Customer warehouse id"))?;
    let customer_district = u8::try_from(customer.district_id)
        .map_err(|_| RichRecoveryError::InvalidEvidence("invalid Customer district id"))?;
    let bytes = bad_credit_prefix(
        customer.customer_id,
        customer_district,
        customer_warehouse,
        prefix.home_district_id,
        prefix.home_warehouse_id,
        prefix.amount_cents,
    );
    validate_bounded_char(
        "bad-credit Payment canonical prefix",
        &bytes,
        MIN_BAD_CREDIT_PREFIX_BYTES,
        MAX_BAD_CREDIT_PREFIX_BYTES,
    )?;
    Ok(bytes)
}

fn bad_credit_suffix_bytes(
    warehouses: u16,
    customer: CustomerKey,
    suffix: &[SealedBadCreditPaymentPrefix],
) -> Result<usize, RichRecoveryError> {
    suffix.iter().try_fold(0_usize, |total, prefix| {
        total
            .checked_add(validate_bad_credit_payment_prefix(warehouses, customer, *prefix)?.len())
            .ok_or(RichRecoveryError::Overflow(
                "bad-credit Payment suffix bytes",
            ))
    })
}

fn append_bad_credit_suffix(
    warehouses: u16,
    customer: CustomerKey,
    suffix: &mut Vec<SealedBadCreditPaymentPrefix>,
    prefix: SealedBadCreditPaymentPrefix,
) -> Result<(), RichRecoveryError> {
    validate_bad_credit_payment_prefix(warehouses, customer, prefix)?;
    suffix.push(prefix);
    while suffix.len() > 1
        && bad_credit_suffix_bytes(warehouses, customer, &suffix[1..])? >= MAX_CUSTOMER_DATA_BYTES
    {
        suffix.remove(0);
    }
    if suffix.len() > MAX_BAD_CREDIT_SUFFIX_ENTRIES {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Payment suffix exceeds its fixed bound",
        ));
    }
    Ok(())
}

fn validate_bad_credit_suffix(
    warehouses: u16,
    customer: CustomerKey,
    committed_updates: u64,
    initial_data: &[u8],
    final_data: &[u8],
    suffix: &[SealedBadCreditPaymentPrefix],
) -> Result<(), RichRecoveryError> {
    validate_canonical_count(
        "bad-credit Payment suffix",
        suffix.len(),
        0,
        MAX_BAD_CREDIT_SUFFIX_ENTRIES,
    )?;
    let suffix_count = u64::try_from(suffix.len())
        .map_err(|_| RichRecoveryError::Overflow("bad-credit Payment suffix count"))?;
    if suffix_count > committed_updates || (committed_updates > 0 && suffix.is_empty()) {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Payment suffix count is inconsistent with committed updates",
        ));
    }
    let suffix_bytes = bad_credit_suffix_bytes(warehouses, customer, suffix)?;
    if suffix.len() > 1
        && bad_credit_suffix_bytes(warehouses, customer, &suffix[1..])? >= MAX_CUSTOMER_DATA_BYTES
    {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Payment suffix is not minimally sufficient",
        ));
    }
    let rooted = committed_updates == suffix_count;
    if !rooted && suffix_bytes < MAX_CUSTOMER_DATA_BYTES {
        return Err(RichRecoveryError::InvalidEvidence(
            "truncated bad-credit Payment suffix does not overwrite the setup root",
        ));
    }
    let mut replayed = if rooted {
        initial_data.to_vec()
    } else {
        Vec::new()
    };
    for prefix in suffix {
        let bytes = validate_bad_credit_payment_prefix(warehouses, customer, *prefix)?;
        replayed = prepend_bad_credit_data(&bytes, &replayed);
    }
    if replayed != final_data {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit final data differs from its trusted Payment suffix replay",
        ));
    }
    Ok(())
}

fn prepend_bad_credit_data(prefix: &[u8], before: &[u8]) -> Vec<u8> {
    let mut expected = Vec::with_capacity(MAX_CUSTOMER_DATA_BYTES);
    expected.extend_from_slice(prefix);
    expected.extend_from_slice(before);
    expected.truncate(MAX_CUSTOMER_DATA_BYTES);
    expected
}

fn expected_bad_credit_data(
    customer_id: i32,
    customer_district: u8,
    customer_warehouse: u16,
    home_district: u8,
    home_warehouse: u16,
    amount_cents: u32,
    before: &[u8],
) -> Vec<u8> {
    prepend_bad_credit_data(
        &bad_credit_prefix(
            customer_id,
            customer_district,
            customer_warehouse,
            home_district,
            home_warehouse,
            amount_cents,
        ),
        before,
    )
}

fn order_score(run_seed: u64, key: OrderKey) -> SampleScore {
    let mut bytes = Vec::with_capacity(order_key_size(&key));
    bytes.extend_from_slice(&key.warehouse_id.to_le_bytes());
    bytes.push(key.district_id);
    bytes.extend_from_slice(&key.order_id.to_le_bytes());
    score_bytes(run_seed, ORDER_SAMPLE_DOMAIN, &bytes)
}

fn delivery_score(run_seed: u64, key: OrderKey) -> SampleScore {
    let mut bytes = Vec::with_capacity(order_key_size(&key));
    bytes.extend_from_slice(&key.warehouse_id.to_le_bytes());
    bytes.push(key.district_id);
    bytes.extend_from_slice(&key.order_id.to_le_bytes());
    score_bytes(run_seed, DELIVERY_SAMPLE_DOMAIN, &bytes)
}

fn history_score(run_seed: u64, key: &HistoryTupleKey) -> SampleScore {
    let mut bytes = Vec::with_capacity(
        history_group_size(key.group) + key.timestamp.len() + key.data.len() + 12,
    );
    bytes.extend_from_slice(&key.group.customer_id.to_le_bytes());
    bytes.push(key.group.customer_district_id);
    bytes.extend_from_slice(&key.group.customer_warehouse_id.to_le_bytes());
    bytes.push(key.group.home_district_id);
    bytes.extend_from_slice(&key.group.home_warehouse_id.to_le_bytes());
    bytes.extend_from_slice(&(key.timestamp.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&key.timestamp);
    bytes.extend_from_slice(&key.amount_bits.to_le_bytes());
    bytes.extend_from_slice(&(key.data.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&key.data);
    score_bytes(run_seed, HISTORY_SAMPLE_DOMAIN, &bytes)
}

fn validate_customer_key(warehouses: u16, key: CustomerKey) -> Result<(), RichRecoveryError> {
    if key.warehouse_id <= 0
        || key.warehouse_id > i32::from(warehouses)
        || key.district_id <= 0
        || key.district_id > i32::from(DISTRICTS_PER_WAREHOUSE)
        || key.customer_id <= 0
        || key.customer_id > i32::from(CUSTOMERS_PER_DISTRICT)
    {
        return Err(RichRecoveryError::InvalidEvidence(
            "bad-credit Customer key is outside the configured domain",
        ));
    }
    Ok(())
}

fn bad_customer_score(run_seed: u64, key: CustomerKey) -> SampleScore {
    let mut bytes = Vec::with_capacity(customer_key_size(key));
    bytes.extend_from_slice(&key.warehouse_id.to_le_bytes());
    bytes.extend_from_slice(&key.district_id.to_le_bytes());
    bytes.extend_from_slice(&key.customer_id.to_le_bytes());
    score_bytes(run_seed, BAD_CUSTOMER_SAMPLE_DOMAIN, &bytes)
}

fn score_bytes(run_seed: u64, domain: &[u8], key: &[u8]) -> SampleScore {
    SampleScore {
        high: hash_lane(
            run_seed ^ 0x243f_6a88_85a3_08d3,
            domain,
            key,
            0x9e37_79b9_7f4a_7c15,
        ),
        low: hash_lane(
            run_seed ^ 0x1319_8a2e_0370_7344,
            domain,
            key,
            0xd1b5_4a32_d192_ed03,
        ),
    }
}

fn hash_lane(seed: u64, domain: &[u8], key: &[u8], lane: u64) -> u64 {
    let mut state = mix64(
        seed ^ lane ^ (domain.len() as u64).rotate_left(17) ^ (key.len() as u64).rotate_left(41),
    );
    absorb(&mut state, domain, lane);
    state = mix64(state ^ 0xa409_3822_299f_31d0);
    absorb(&mut state, key, lane.rotate_left(23));
    mix64(state ^ lane ^ 0x082e_fa98_ec4e_6c89)
}

fn absorb(state: &mut u64, bytes: &[u8], lane: u64) {
    for (index, chunk) in bytes.chunks(8).enumerate() {
        let mut block = [0_u8; 8];
        block[..chunk.len()].copy_from_slice(chunk);
        if chunk.len() < block.len() {
            block[chunk.len()] = 0x80;
        }
        *state = mix64(
            *state
                ^ u64::from_le_bytes(block)
                ^ lane
                ^ (index as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15),
        );
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn setup_history_collision(
    provider: &dyn InitialHistoryProvider,
    tuple: &HistoryTupleKey,
) -> Result<u8, RichRecoveryError> {
    if tuple.group.home_warehouse_id != tuple.group.customer_warehouse_id
        || tuple.group.home_district_id != tuple.group.customer_district_id
    {
        return Ok(0);
    }
    let customer = CustomerKey {
        warehouse_id: i32::from(tuple.group.customer_warehouse_id),
        district_id: i32::from(tuple.group.customer_district_id),
        customer_id: tuple.group.customer_id,
    };
    let initial = provider
        .initial_history(customer)
        .ok_or(RichRecoveryError::MissingInitialHistory(customer))?;
    // InitialHistoryRow has private fields and can only be constructed through
    // its validating constructor.
    Ok(u8::from(
        initial.timestamp == tuple.timestamp
            && initial.amount_bits == tuple.amount_bits
            && initial.data == tuple.data,
    ))
}

fn sealed_raw_size(
    orders: &[SealedNewOrderSample],
    deliveries: &[SealedDeliverySample],
    customers: &[SealedBadCreditCustomerSample],
    histories: &[SealedHistoryGroup],
    order_rejected: Option<&OrderCutoffWitness>,
    delivery_rejected: Option<&OrderCutoffWitness>,
    customer_rejected: Option<&CustomerCutoffWitness>,
    history_rejected: Option<&HistoryCutoffWitness>,
) -> Result<usize, RichRecoveryError> {
    let mut size = 64_usize;
    for order in orders {
        size = checked_size_add(size, 48 + order.entry_timestamp.len())?;
        for line in &order.lines {
            size = checked_size_add(
                size,
                20 + line.delivery_timestamp.len() + line.district_info.len(),
            )?;
        }
    }
    for delivery in deliveries {
        size = checked_size_add(size, 68 + delivery.delivery_timestamp.len())?;
        for line in &delivery.lines {
            size = checked_size_add(size, 12 + line.delivery_timestamp.len())?;
        }
    }
    for customer in customers {
        size = checked_size_add(
            size,
            48 + customer.data.len() + customer.payment_suffix.len() * (2 + 1 + 4),
        )?;
    }
    for group in histories {
        size = checked_size_add(size, 16)?;
        for tuple in &group.tuples {
            size = checked_size_add(size, 48 + tuple.timestamp.len() + tuple.data.len())?;
        }
    }
    if order_rejected.is_some() {
        size = checked_size_add(size, 16 + 7)?;
    }
    if delivery_rejected.is_some() {
        size = checked_size_add(size, 16 + 7)?;
    }
    if customer_rejected.is_some() {
        size = checked_size_add(size, 16 + 12)?;
    }
    if let Some(witness) = history_rejected {
        size = checked_size_add(
            size,
            16 + 10 + 4 + witness.timestamp.len() + 4 + witness.data.len(),
        )?;
    }
    Ok(size)
}

// All variable-width fields are individually bounded before commit. This
// compile-time upper bound means the hot path never scans retained payloads to
// rediscover the same raw-size fact.
const MAX_FINAL_ORDER_BYTES: usize = 48
    + MAX_ENTRY_TIMESTAMP_BYTES
    + MAX_ORDER_LINES as usize * (20 + MAX_DELIVERY_TIMESTAMP_BYTES + 24);
const MAX_DELIVERY_BYTES: usize = 64
    + 4
    + MAX_DELIVERY_TIMESTAMP_BYTES
    + MAX_ORDER_LINES as usize * (12 + MAX_DELIVERY_TIMESTAMP_BYTES);
const MAX_BAD_CUSTOMER_BYTES: usize =
    48 + MAX_CUSTOMER_DATA_BYTES + MAX_BAD_CREDIT_SUFFIX_ENTRIES * (2 + 1 + 4);
const MAX_HISTORY_BYTES: usize = 16 + 48 + MAX_HISTORY_TIMESTAMP_BYTES + MAX_HISTORY_DATA_BYTES;
const MAX_WITNESS_BYTES: usize =
    (16 + 7) + (16 + 7) + (16 + 12) + (16 + 10 + 4 + MAX_HISTORY_TIMESTAMP_BYTES + 4 + 24);
const THEORETICAL_MAX_RICH_BYTES: usize = RICH_RECOVERY_SAMPLE_CAPACITY * MAX_FINAL_ORDER_BYTES
    + RICH_RECOVERY_SAMPLE_CAPACITY * MAX_DELIVERY_BYTES
    + RICH_RECOVERY_SAMPLE_CAPACITY * MAX_BAD_CUSTOMER_BYTES
    + RICH_HISTORY_SAMPLE_CAPACITY * MAX_HISTORY_BYTES
    + MAX_WITNESS_BYTES
    + 64;
const _: () = assert!(THEORETICAL_MAX_RICH_BYTES <= MAX_RICH_RECOVERY_RAW_BYTES);

#[cfg(test)]
mod tests {
    use super::*;

    use crate::profile::TransactionKind;
    use crate::ranking::evidence_collector::IntervalCollector;
    use crate::ranking::runner::RecoveryNewOrderLineEvidence;
    use crate::routing::{ClientSequence, OfficialRouter, StageId, WorkloadSeed};
    use crate::workload::{CustomerSelector, Final2026Workload};

    const TEST_SEED: u64 = 0x517a_2026_9d31_0042;
    const TEST_TIMESTAMP: &[u8] = b"2026-07-29 12:34:56";

    fn no_setup_collision(_: CustomerKey) -> Option<InitialHistoryRow> {
        Some(
            InitialHistoryRow::new(
                b"2026-01-01 00:00:00".to_vec(),
                10.0_f32.to_bits(),
                b"SETUP-HISTORY".to_vec(),
            )
            .unwrap(),
        )
    }

    fn bad_credit_setup(_: CustomerKey) -> Option<InitialCustomerData> {
        Some(InitialCustomerData::new(*b"BC", b"old-data".to_vec()).unwrap())
    }

    fn collector_with_customer(
        credit: [u8; 2],
        data: &[u8],
        clients: u16,
    ) -> RichRecoveryCollector {
        let setup = InitialCustomerData::new(credit, data.to_vec()).unwrap();
        RichRecoveryCollector::new(
            OFFICIAL_WAREHOUSES,
            clients,
            TEST_SEED,
            no_setup_collision,
            move |_| Some(setup.clone()),
        )
        .unwrap()
    }

    fn collector() -> RichRecoveryCollector {
        RichRecoveryCollector::new(
            OFFICIAL_WAREHOUSES,
            1,
            TEST_SEED,
            no_setup_collision,
            bad_credit_setup,
        )
        .unwrap()
    }

    fn empty_intervals() -> SealedIntervalEvidence {
        IntervalCollector::new(OFFICIAL_WAREHOUSES, 1, TEST_SEED, |_key: StockKey| None)
            .unwrap()
            .seal()
            .unwrap()
    }

    fn find_ticket(mut predicate: impl FnMut(&TransactionTicket) -> bool) -> TransactionTicket {
        let router = OfficialRouter::new(WorkloadSeed(0x77aa_1001));
        let wheel = router.wheel(StageId::WARMUP);
        let workload = Final2026Workload::new(&router, &wheel);
        let mut sequence = ClientSequence::new(0).unwrap();
        for _ in 0..1_000_000 {
            let ticket = workload.select(&mut sequence).unwrap();
            if predicate(&ticket) {
                return ticket;
            }
        }
        panic!("could not find deterministic test ticket");
    }

    fn normal_new_order_ticket() -> TransactionTicket {
        find_ticket(|ticket| {
            matches!(
                ticket.parameters(),
                TransactionParameters::NewOrder(input) if !input.expected_rollback()
            )
        })
    }

    fn rollback_new_order_ticket() -> TransactionTicket {
        find_ticket(|ticket| {
            matches!(
                ticket.parameters(),
                TransactionParameters::NewOrder(input) if input.expected_rollback()
            )
        })
    }

    fn delivery_ticket(warehouse: Option<u16>) -> TransactionTicket {
        find_ticket(|ticket| {
            ticket.kind() == TransactionKind::Delivery
                && warehouse.is_none_or(|expected| ticket.route().home_warehouse == expected)
        })
    }

    fn local_payment_ticket() -> TransactionTicket {
        find_ticket(|ticket| {
            let TransactionParameters::Payment(input) = ticket.parameters() else {
                return false;
            };
            input.customer_warehouse() == ticket.route().home_warehouse
                && input.customer_district() == ticket.route().home_district
        })
    }

    fn new_order_evidence(ticket: &TransactionTicket, order_id: i32) -> NewOrderEvidence {
        let TransactionParameters::NewOrder(input) = ticket.parameters() else {
            panic!("test ticket must be NewOrder");
        };
        assert!(!input.expected_rollback());
        let line_amount_bits = vec![1.0_f32.to_bits(); input.lines().len()];
        let recovery_lines = input
            .lines()
            .iter()
            .map(|line| RecoveryNewOrderLineEvidence {
                number: line.number(),
                item_id: line.item_id(),
                supply_warehouse: line.supply_warehouse(),
                quantity: line.quantity(),
                amount_bits: 1.0_f32.to_bits(),
                district_info: vec![b'D'; DISTRICT_INFO_BYTES],
            })
            .collect::<Vec<_>>();
        NewOrderEvidence {
            warehouse_id: ticket.route().home_warehouse,
            district_id: ticket.route().home_district,
            order_id,
            line_count: input.lines().len() as u8,
            remote_line_count: input
                .lines()
                .iter()
                .filter(|line| line.supply_warehouse() != ticket.route().home_warehouse)
                .count() as u8,
            stock_ytd_delta: input
                .lines()
                .iter()
                .map(|line| u32::from(line.quantity()))
                .sum(),
            line_amount_bits,
            entry_timestamp: TEST_TIMESTAMP.to_vec(),
            recovery_lines,
        }
    }

    fn payment_evidence(
        ticket: &TransactionTicket,
        bad_credit: bool,
        before_version: CustomerVersion,
        timestamp: &[u8],
        history_data: &[u8],
        customer_data_before: Vec<u8>,
    ) -> PaymentEvidence {
        let TransactionParameters::Payment(input) = ticket.parameters() else {
            panic!("test ticket must be Payment");
        };
        let customer_id = match input.customer() {
            CustomerSelector::Id(customer_id) => i32::from(*customer_id),
            CustomerSelector::LastName(_) => 1,
        };
        let after_version = CustomerVersion {
            payment_count: before_version.payment_count + 1,
            delivery_count: before_version.delivery_count,
        };
        let amount = f32::from_bits(input.amount_bits());
        let before_balance = 10_000.0_f32;
        let before_ytd = 10.0_f32;
        let customer_data_after = if bad_credit {
            expected_bad_credit_data(
                customer_id,
                input.customer_district(),
                input.customer_warehouse(),
                ticket.route().home_district,
                ticket.route().home_warehouse,
                input.amount_cents(),
                &customer_data_before,
            )
        } else {
            customer_data_before.clone()
        };
        PaymentEvidence {
            warehouse_id: ticket.route().home_warehouse,
            district_id: ticket.route().home_district,
            customer_warehouse_id: input.customer_warehouse(),
            customer_district_id: input.customer_district(),
            customer_id,
            amount_bits: input.amount_bits(),
            warehouse_before_bits: 1.0_f32.to_bits(),
            warehouse_after_bits: (1.0_f32 + amount).to_bits(),
            district_before_bits: 1.0_f32.to_bits(),
            district_after_bits: (1.0_f32 + amount).to_bits(),
            customer_balance_before_bits: before_balance.to_bits(),
            customer_balance_after_bits: (before_balance - amount).to_bits(),
            customer_ytd_before_bits: before_ytd.to_bits(),
            customer_ytd_after_bits: (before_ytd + amount).to_bits(),
            customer_version_before: before_version,
            customer_version_after: after_version,
            history_timestamp: timestamp.to_vec(),
            history_data: history_data.to_vec(),
            customer_is_bad_credit: bad_credit,
            customer_data_before,
            customer_data_after,
        }
    }

    fn bad_credit_chain(ticket: &TransactionTicket, updates: usize) -> Vec<PaymentEvidence> {
        let mut data = b"old-data".to_vec();
        (0..updates)
            .map(|index| {
                let evidence = payment_evidence(
                    ticket,
                    true,
                    CustomerVersion {
                        payment_count: CUSTOMER_INITIAL_PAYMENT_COUNT + index as i32,
                        delivery_count: index as i32,
                    },
                    TEST_TIMESTAMP,
                    b"CHAIN-HISTORY",
                    data.clone(),
                );
                data = evidence.customer_data_after.clone();
                evidence
            })
            .collect()
    }

    fn distinct_bad_credit_customers(count: usize) -> RichRecoveryCollector {
        let ticket = local_payment_ticket();
        let TransactionParameters::Payment(input) = ticket.parameters() else {
            unreachable!();
        };
        let mut collector = collector();
        for customer_id in 1..=count as i32 {
            let mut evidence = payment_evidence(
                &ticket,
                true,
                CustomerVersion {
                    payment_count: CUSTOMER_INITIAL_PAYMENT_COUNT,
                    delivery_count: CUSTOMER_INITIAL_DELIVERY_COUNT,
                },
                TEST_TIMESTAMP,
                b"DISTINCT-BC",
                b"old-data".to_vec(),
            );
            evidence.customer_id = customer_id;
            evidence.customer_data_after = expected_bad_credit_data(
                customer_id,
                input.customer_district(),
                input.customer_warehouse(),
                ticket.route().home_district,
                ticket.route().home_warehouse,
                input.amount_cents(),
                &evidence.customer_data_before,
            );
            collector
                .offer_terminal(
                    &ticket,
                    &RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)),
                )
                .unwrap();
        }
        collector
    }

    fn unrooted_customer_state(key: CustomerKey, start: i32) -> CustomerDataState {
        let prefix = SealedBadCreditPaymentPrefix {
            home_warehouse_id: 1,
            home_district_id: 1,
            amount_cents: MIN_PAYMENT_CENTS,
        };
        let before_data = b"before".to_vec();
        let after_data = prepend_bad_credit_data(
            &validate_bad_credit_payment_prefix(OFFICIAL_WAREHOUSES, key, prefix).unwrap(),
            &before_data,
        );
        let mut state = CustomerDataState::new(OFFICIAL_WAREHOUSES, key, b"old-data".to_vec());
        state.update_count = 1;
        state.pending.insert(
            start,
            CustomerDataEdge {
                before_version: CustomerVersion {
                    payment_count: start,
                    delivery_count: 0,
                },
                after_version: CustomerVersion {
                    payment_count: start + 1,
                    delivery_count: 0,
                },
                before_data,
                after_data,
                prefix,
            },
        );
        state
    }

    fn prepared_bad_customer(key: CustomerKey, before_count: i32) -> PreparedBadCustomer {
        let prefix = SealedBadCreditPaymentPrefix {
            home_warehouse_id: 1,
            home_district_id: 1,
            amount_cents: MIN_PAYMENT_CENTS,
        };
        let data_before = if before_count == CUSTOMER_INITIAL_PAYMENT_COUNT {
            b"old-data".to_vec()
        } else {
            b"before".to_vec()
        };
        let data_after = prepend_bad_credit_data(
            &validate_bad_credit_payment_prefix(OFFICIAL_WAREHOUSES, key, prefix).unwrap(),
            &data_before,
        );
        PreparedBadCustomer {
            key,
            setup_data: b"old-data".to_vec(),
            before_version: CustomerVersion {
                payment_count: before_count,
                delivery_count: 0,
            },
            after_version: CustomerVersion {
                payment_count: before_count + 1,
                delivery_count: 0,
            },
            data_before,
            data_after,
            prefix,
        }
    }

    fn delivery_evidence(
        key: OrderKey,
        customer_id: i32,
        timestamp: &[u8],
        line_amount_bits: Vec<u32>,
    ) -> DeliveredOrderEvidence {
        let amount_bits = sum_f32_as_f64_once(line_amount_bits.iter().copied()).unwrap();
        let amount = f32::from_bits(amount_bits);
        DeliveredOrderEvidence {
            warehouse_id: key.warehouse_id,
            district_id: key.district_id,
            order_id: key.order_id,
            customer_id,
            line_count: line_amount_bits.len() as u8,
            amount_bits,
            customer_balance_before_bits: 10.0_f32.to_bits(),
            customer_balance_after_bits: (10.0_f32 + amount).to_bits(),
            customer_version_before: CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            customer_version_after: CustomerVersion {
                payment_count: 1,
                delivery_count: 1,
            },
            delivery_timestamp: timestamp.to_vec(),
            line_amount_bits,
        }
    }

    #[derive(Clone)]
    struct CanonicalSnapshot {
        header: CanonicalRichRecoveryHeader,
        orders: Vec<CanonicalRichNewOrder>,
        deliveries: Vec<CanonicalRichDelivery>,
        customers: Vec<CanonicalRichBadCreditCustomer>,
        histories: Vec<CanonicalRichHistoryTuple>,
        order_rejected: Option<CanonicalRichOrderWitness>,
        delivery_rejected: Option<CanonicalRichOrderWitness>,
        customer_rejected: Option<CanonicalRichCustomerWitness>,
        history_rejected: Option<CanonicalRichHistoryWitness>,
    }

    struct LyingExact<I> {
        inner: I,
        declared: usize,
    }

    impl<I: Iterator> Iterator for LyingExact<I> {
        type Item = I::Item;

        fn next(&mut self) -> Option<Self::Item> {
            self.inner.next()
        }
    }

    impl<I: Iterator> ExactSizeIterator for LyingExact<I> {
        fn len(&self) -> usize {
            self.declared
        }
    }

    fn canonical_snapshot(sealed: &SealedRichRecoverySamples) -> CanonicalSnapshot {
        let orders = sealed
            .new_orders()
            .iter()
            .map(|order| {
                CanonicalRichNewOrder::new(
                    order.score(),
                    order.key(),
                    order.customer_id(),
                    order.entry_timestamp().to_vec(),
                    order.carrier_id(),
                    order.line_count(),
                    order.all_local(),
                    order.queue_present(),
                    order.lines().iter().map(|line| {
                        CanonicalRichOrderLine::new(
                            line.number(),
                            line.item_id(),
                            line.supply_warehouse(),
                            line.delivery_timestamp().to_vec(),
                            line.quantity(),
                            line.amount_bits(),
                            line.district_info().to_vec(),
                        )
                    }),
                )
                .unwrap()
            })
            .collect();
        let deliveries = sealed
            .deliveries()
            .iter()
            .map(|delivery| {
                CanonicalRichDelivery::new(
                    delivery.score(),
                    delivery.key(),
                    delivery.customer_id(),
                    delivery.carrier_id(),
                    delivery.queue_present(),
                    delivery.delivery_timestamp().to_vec(),
                    delivery.lines().iter().map(|line| {
                        CanonicalRichDeliveryLine::new(
                            line.number(),
                            line.delivery_timestamp().to_vec(),
                            line.amount_bits(),
                        )
                    }),
                )
                .unwrap()
            })
            .collect();
        let customers = sealed
            .bad_credit_customers()
            .iter()
            .map(|customer| {
                CanonicalRichBadCreditCustomer::new(
                    customer.score(),
                    customer.customer_key(),
                    customer.final_payment_count(),
                    *customer.expected_credit(),
                    customer.final_data().to_vec(),
                    customer.committed_payment_updates(),
                    customer.payment_suffix().iter().map(|prefix| {
                        CanonicalRichBadCreditPrefix::new(
                            prefix.home_warehouse_id(),
                            prefix.home_district_id(),
                            prefix.amount_cents(),
                        )
                    }),
                )
                .unwrap()
            })
            .collect();
        let mut histories = sealed
            .history_tuples()
            .map(|(group, tuple)| {
                CanonicalRichHistoryTuple::new(
                    tuple.score(),
                    group,
                    tuple.timestamp().to_vec(),
                    tuple.amount_bits(),
                    tuple.data().to_vec(),
                    tuple.committed_multiplicity(),
                    tuple.setup_collision_multiplicity(),
                )
            })
            .collect::<Vec<_>>();
        histories.sort_by(|left, right| {
            RankedKey {
                score: left.score,
                key: HistoryTupleKey {
                    group: left.group,
                    timestamp: left.timestamp.clone(),
                    amount_bits: left.amount_bits,
                    data: left.data.clone(),
                },
            }
            .cmp(&RankedKey {
                score: right.score,
                key: HistoryTupleKey {
                    group: right.group,
                    timestamp: right.timestamp.clone(),
                    amount_bits: right.amount_bits,
                    data: right.data.clone(),
                },
            })
        });
        CanonicalSnapshot {
            header: CanonicalRichRecoveryHeader::new(
                sealed.warehouses(),
                sealed.run_seed(),
                sealed.policy_version(),
                sealed.raw_size_bytes(),
                sealed.new_order_commit_count(),
                sealed.delivered_order_count(),
                sealed.committed_history_row_count(),
                sealed.bad_credit_payment_count(),
            ),
            orders,
            deliveries,
            customers,
            histories,
            order_rejected: sealed
                .order_rejected_witness()
                .map(|witness| CanonicalRichOrderWitness::new(witness.score(), witness.key())),
            delivery_rejected: sealed
                .delivery_rejected_witness()
                .map(|witness| CanonicalRichOrderWitness::new(witness.score(), witness.key())),
            customer_rejected: sealed
                .bad_customer_rejected_witness()
                .map(|witness| CanonicalRichCustomerWitness::new(witness.score(), witness.key())),
            history_rejected: sealed.history_rejected_witness().map(|witness| {
                CanonicalRichHistoryWitness::new(
                    witness.score(),
                    witness.group(),
                    witness.timestamp().to_vec(),
                    witness.amount_bits(),
                    witness.data().to_vec(),
                )
            }),
        }
    }

    fn reconstruct(
        snapshot: CanonicalSnapshot,
    ) -> Result<SealedRichRecoverySamples, RichRecoveryError> {
        SealedRichRecoverySamples::from_canonical_parts(
            snapshot.header,
            snapshot.orders.into_iter(),
            snapshot.deliveries.into_iter(),
            snapshot.customers.into_iter(),
            snapshot.histories.into_iter(),
            snapshot.order_rejected,
            snapshot.delivery_rejected,
            snapshot.customer_rejected,
            snapshot.history_rejected,
            &empty_intervals(),
            &no_setup_collision,
            &bad_credit_setup,
        )
    }

    fn all_domain_sealed() -> SealedRichRecoverySamples {
        let new_order_ticket = normal_new_order_ticket();
        let new_order = new_order_evidence(&new_order_ticket, 3_001);
        let key = OrderKey::from_parts(
            new_order.warehouse_id,
            new_order.district_id,
            new_order.order_id,
        );
        let customer_id = match new_order_ticket.parameters() {
            TransactionParameters::NewOrder(input) => i32::from(input.customer_id()),
            _ => unreachable!(),
        };
        let delivery_ticket = delivery_ticket(Some(key.warehouse_id()));
        let delivery = delivery_evidence(
            key,
            customer_id,
            TEST_TIMESTAMP,
            new_order.line_amount_bits.clone(),
        );
        let payment_ticket = local_payment_ticket();
        let payment = payment_evidence(
            &payment_ticket,
            true,
            CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            TEST_TIMESTAMP,
            b"ROUNDTRIP-HISTORY",
            b"old-data".to_vec(),
        );

        let mut collector = collector();
        collector
            .offer_terminal(
                &new_order_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::NewOrder(new_order)),
            )
            .unwrap();
        collector
            .offer_terminal(
                &delivery_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Delivery(vec![delivery])),
            )
            .unwrap();
        collector
            .offer_terminal(
                &payment_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(payment)),
            )
            .unwrap();
        collector.seal(&empty_intervals()).unwrap()
    }

    #[test]
    fn canonical_reconstruction_roundtrips_all_rich_domains() {
        let live = all_domain_sealed();
        let restored = reconstruct(canonical_snapshot(&live)).unwrap();
        assert_eq!(restored.warehouses, live.warehouses);
        assert_eq!(restored.run_seed, live.run_seed);
        assert_eq!(restored.raw_size_bytes, live.raw_size_bytes);
        assert_eq!(restored.new_order_commits, live.new_order_commits);
        assert_eq!(restored.delivered_orders, live.delivered_orders);
        assert_eq!(restored.history_rows, live.history_rows);
        assert_eq!(restored.bad_credit_payments, live.bad_credit_payments);
        assert_eq!(restored.new_orders, live.new_orders);
        assert_eq!(restored.deliveries, live.deliveries);
        assert_eq!(restored.bad_credit_customers, live.bad_credit_customers);
        assert_eq!(restored.history_groups, live.history_groups);
    }

    #[test]
    fn canonical_reconstruction_rejects_rank_order_cutoff_and_raw_size_forgery() {
        let live = all_domain_sealed();

        let mut forged_score = canonical_snapshot(&live);
        forged_score.orders[0].score.high ^= 1;
        assert!(matches!(
            reconstruct(forged_score),
            Err(RichRecoveryError::ForgedCanonicalScore { .. })
        ));

        let mut duplicate_order = canonical_snapshot(&live);
        duplicate_order
            .orders
            .push(duplicate_order.orders[0].clone());
        duplicate_order.header.new_order_commits += 1;
        assert!(matches!(
            reconstruct(duplicate_order),
            Err(RichRecoveryError::NonCanonicalOrder { .. })
        ));

        let mut invalid_cutoff = canonical_snapshot(&live);
        let selected = RankedKey {
            score: invalid_cutoff.orders[0].score,
            key: invalid_cutoff.orders[0].key,
        };
        let rejected = (3_002..)
            .map(|order_id| OrderKey::from_parts(1, 1, order_id))
            .map(|key| RankedKey {
                score: order_score(TEST_SEED, key),
                key,
            })
            .find(|candidate| candidate > &selected)
            .unwrap();
        invalid_cutoff.order_rejected =
            Some(CanonicalRichOrderWitness::new(rejected.score, rejected.key));
        invalid_cutoff.header.new_order_commits += 1;
        assert!(matches!(
            reconstruct(invalid_cutoff),
            Err(RichRecoveryError::InvalidWeightedSelection { .. })
        ));

        let mut wrong_size = canonical_snapshot(&live);
        wrong_size.header.raw_size_bytes += 1;
        assert!(matches!(
            reconstruct(wrong_size),
            Err(RichRecoveryError::RawSizeMismatch { .. })
        ));
    }

    #[test]
    fn canonical_reconstruction_rechecks_providers_overlap_and_iterator_caps() {
        let live = all_domain_sealed();

        let mut collision = canonical_snapshot(&live);
        collision.histories[0].setup_collision_multiplicity ^= 1;
        assert!(matches!(
            reconstruct(collision),
            Err(RichRecoveryError::SetupCollisionMismatch { .. })
        ));

        let bad_provider = canonical_snapshot(&live);
        let result = SealedRichRecoverySamples::from_canonical_parts(
            bad_provider.header,
            bad_provider.orders.into_iter(),
            bad_provider.deliveries.into_iter(),
            bad_provider.customers.into_iter(),
            bad_provider.histories.into_iter(),
            bad_provider.order_rejected,
            bad_provider.delivery_rejected,
            bad_provider.customer_rejected,
            bad_provider.history_rejected,
            &empty_intervals(),
            &no_setup_collision,
            &|_| Some(InitialCustomerData::new(*b"GC", Vec::new()).unwrap()),
        );
        assert!(matches!(
            result,
            Err(RichRecoveryError::CustomerCreditFlagMismatch { .. })
        ));

        let mut overlap = canonical_snapshot(&live);
        overlap.deliveries[0].customer_id = if overlap.deliveries[0].customer_id == 1 {
            2
        } else {
            1
        };
        assert!(matches!(
            reconstruct(overlap),
            Err(RichRecoveryError::InvalidEvidence(
                "canonical NewOrder and Delivery samples disagree for one retained order"
            ))
        ));

        let mut bad_count = canonical_snapshot(&live);
        bad_count.customers[0].final_payment_count += 1;
        assert!(matches!(
            reconstruct(bad_count),
            Err(RichRecoveryError::InvalidEvidence(
                "canonical bad-credit final Payment count is not setup-rooted"
            ))
        ));

        let mut bad_suffix = canonical_snapshot(&live);
        bad_suffix.customers[0].payment_suffix[0].amount_cents += 1;
        assert!(matches!(
            reconstruct(bad_suffix),
            Err(RichRecoveryError::InvalidEvidence(
                "bad-credit final data differs from its trusted Payment suffix replay"
            ))
        ));

        let one = canonical_snapshot(&live).orders[0].clone();
        let oversized = std::iter::repeat_n(one, RICH_RECOVERY_SAMPLE_CAPACITY + 1);
        assert!(matches!(
            SealedRichRecoverySamples::from_canonical_parts(
                CanonicalRichRecoveryHeader::new(
                    OFFICIAL_WAREHOUSES,
                    TEST_SEED,
                    RICH_RECOVERY_POLICY_VERSION,
                    64,
                    0,
                    0,
                    0,
                    0,
                ),
                oversized,
                Vec::new().into_iter(),
                Vec::new().into_iter(),
                Vec::new().into_iter(),
                None,
                None,
                None,
                None,
                &empty_intervals(),
                &no_setup_collision,
                &bad_credit_setup,
            ),
            Err(RichRecoveryError::CanonicalCount { .. })
        ));
    }

    #[test]
    fn canonical_nested_iterators_cannot_lie_about_their_bounds() {
        let order_key = OrderKey::from_parts(1, 1, 3_001);
        let order_line = CanonicalRichOrderLine::new(
            1,
            1,
            1,
            Vec::new(),
            MIN_ITEM_QUANTITY,
            1.0_f32.to_bits(),
            vec![b'D'; DISTRICT_INFO_BYTES],
        );
        assert!(matches!(
            CanonicalRichNewOrder::new(
                order_score(TEST_SEED, order_key),
                order_key,
                1,
                TEST_TIMESTAMP.to_vec(),
                0,
                MIN_ORDER_LINES,
                true,
                true,
                LyingExact {
                    inner: std::iter::repeat(order_line),
                    declared: usize::from(MIN_ORDER_LINES),
                },
            ),
            Err(RichRecoveryError::CanonicalCount { .. })
        ));

        let delivery_line =
            CanonicalRichDeliveryLine::new(1, TEST_TIMESTAMP.to_vec(), 1.0_f32.to_bits());
        assert!(matches!(
            CanonicalRichDelivery::new(
                delivery_score(TEST_SEED, order_key),
                order_key,
                1,
                MIN_CARRIER_ID,
                false,
                TEST_TIMESTAMP.to_vec(),
                LyingExact {
                    inner: vec![delivery_line; usize::from(MIN_ORDER_LINES) - 1].into_iter(),
                    declared: usize::from(MIN_ORDER_LINES),
                },
            ),
            Err(RichRecoveryError::CanonicalCount { .. })
        ));

        let prefix = CanonicalRichBadCreditPrefix::new(1, 1, MIN_PAYMENT_CENTS);
        assert!(matches!(
            CanonicalRichBadCreditCustomer::new(
                SampleScore { high: 0, low: 0 },
                CustomerKey {
                    warehouse_id: 1,
                    district_id: 1,
                    customer_id: 1,
                },
                2,
                *b"BC",
                Vec::new(),
                1,
                LyingExact {
                    inner: std::iter::repeat(prefix),
                    declared: 1,
                },
            ),
            Err(RichRecoveryError::CanonicalCount { .. })
        ));

        let live = all_domain_sealed();
        let mut invalid_order = canonical_snapshot(&live);
        invalid_order.orders[0].lines.clear();
        invalid_order.orders[0].line_count = 0;
        assert!(matches!(
            reconstruct(invalid_order),
            Err(RichRecoveryError::CanonicalCount { .. })
        ));
        let mut invalid_delivery = canonical_snapshot(&live);
        invalid_delivery.deliveries[0].lines.clear();
        assert!(matches!(
            reconstruct(invalid_delivery),
            Err(RichRecoveryError::CanonicalCount { .. })
        ));
    }

    #[test]
    fn delivery_arriving_first_still_updates_sampled_new_order_final_state() {
        let new_order_ticket = normal_new_order_ticket();
        let evidence = new_order_evidence(&new_order_ticket, 3_001);
        let key = OrderKey {
            warehouse_id: evidence.warehouse_id,
            district_id: evidence.district_id,
            order_id: evidence.order_id,
        };
        let delivery_ticket = delivery_ticket(Some(key.warehouse_id));
        let carrier = match delivery_ticket.parameters() {
            TransactionParameters::Delivery(input) => input.carrier_id(),
            _ => unreachable!(),
        };
        let customer_id = match new_order_ticket.parameters() {
            TransactionParameters::NewOrder(input) => i32::from(input.customer_id()),
            _ => unreachable!(),
        };
        let delivered = delivery_evidence(
            key,
            customer_id,
            TEST_TIMESTAMP,
            evidence.line_amount_bits.clone(),
        );

        let mut first = collector();
        first
            .offer_terminal(
                &delivery_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Delivery(vec![
                    delivered.clone()
                ])),
            )
            .unwrap();
        first
            .offer_terminal(
                &new_order_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence.clone())),
            )
            .unwrap();

        let mut second = collector();
        second
            .offer_terminal(
                &new_order_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence)),
            )
            .unwrap();
        second
            .offer_terminal(
                &delivery_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Delivery(vec![delivered])),
            )
            .unwrap();

        let intervals = empty_intervals();
        let left = first.seal(&intervals).unwrap();
        let right = second.seal(&intervals).unwrap();
        assert_eq!(left.new_orders(), right.new_orders());
        let sample = &left.new_orders()[0];
        assert_eq!(sample.carrier_id(), carrier);
        assert!(!sample.queue_present());
        assert_eq!(left.deliveries()[0].customer_id(), customer_id);
        assert!(sample
            .lines()
            .iter()
            .all(|line| line.delivery_timestamp() == TEST_TIMESTAMP));
    }

    #[test]
    fn delivery_customer_must_match_new_order_in_both_arrival_orders() {
        let new_order_ticket = normal_new_order_ticket();
        let evidence = new_order_evidence(&new_order_ticket, 3_001);
        let key = OrderKey {
            warehouse_id: evidence.warehouse_id,
            district_id: evidence.district_id,
            order_id: evidence.order_id,
        };
        let expected_customer = match new_order_ticket.parameters() {
            TransactionParameters::NewOrder(input) => i32::from(input.customer_id()),
            _ => unreachable!(),
        };
        let wrong_customer = if expected_customer == i32::from(CUSTOMERS_PER_DISTRICT) {
            1
        } else {
            expected_customer + 1
        };
        let delivery_ticket = delivery_ticket(Some(key.warehouse_id));
        let wrong_delivery = delivery_evidence(
            key,
            wrong_customer,
            TEST_TIMESTAMP,
            evidence.line_amount_bits.clone(),
        );

        let mut origin_first = collector();
        origin_first
            .offer_terminal(
                &new_order_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence.clone())),
            )
            .unwrap();
        assert!(origin_first
            .offer_terminal(
                &delivery_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Delivery(vec![
                    wrong_delivery.clone(),
                ])),
            )
            .is_err());
        assert_eq!(origin_first.delivered_orders, 0);

        let mut delivery_first = collector();
        delivery_first
            .offer_terminal(
                &delivery_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Delivery(vec![wrong_delivery])),
            )
            .unwrap();
        assert!(delivery_first
            .offer_terminal(
                &new_order_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence)),
            )
            .is_err());
        assert_eq!(delivery_first.new_order_commits, 0);
    }

    #[test]
    fn bad_credit_is_independent_nonempty_and_history_counts_setup_collision() {
        let ticket = local_payment_ticket();
        let first = payment_evidence(
            &ticket,
            true,
            CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            TEST_TIMESTAMP,
            b"SAME-HISTORY",
            b"old-data".to_vec(),
        );
        let second = payment_evidence(
            &ticket,
            true,
            first.customer_version_after,
            TEST_TIMESTAMP,
            b"SAME-HISTORY",
            first.customer_data_after.clone(),
        );
        let customer_key = CustomerKey {
            warehouse_id: i32::from(first.customer_warehouse_id),
            district_id: i32::from(first.customer_district_id),
            customer_id: first.customer_id,
        };
        let setup = InitialHistoryRow::new(
            TEST_TIMESTAMP.to_vec(),
            first.amount_bits,
            first.history_data.clone(),
        )
        .unwrap();
        let mut collector = RichRecoveryCollector::new(
            OFFICIAL_WAREHOUSES,
            1,
            TEST_SEED,
            move |key| (key == customer_key).then(|| setup.clone()),
            bad_credit_setup,
        )
        .unwrap();
        collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(first)),
            )
            .unwrap();
        collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(second.clone())),
            )
            .unwrap();

        // The independent interval section is deliberately empty: BC sampling
        // must neither require nor fabricate a key intersection.
        let sealed = collector.seal(&empty_intervals()).unwrap();
        assert_eq!(sealed.bad_credit_payment_count(), 2);
        assert_eq!(sealed.bad_credit_customers().len(), 1);
        assert_eq!(
            sealed.bad_credit_customers()[0].final_data(),
            second.customer_data_after
        );
        let tuples = sealed.history_tuples().collect::<Vec<_>>();
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].1.committed_multiplicity(), 2);
        assert_eq!(tuples[0].1.setup_collision_multiplicity(), 1);
        assert_eq!(tuples[0].1.expected_total_multiplicity().unwrap(), 3);
    }

    #[test]
    fn bad_credit_root_chain_accepts_reverse_and_bridges_at_pending_cap() {
        let ticket = local_payment_ticket();
        let chain = bad_credit_chain(&ticket, 5);
        let final_data = chain.last().unwrap().customer_data_after.clone();
        let final_count = chain.last().unwrap().customer_version_after.payment_count;

        let mut reverse = collector_with_customer(*b"BC", b"old-data", 5);
        for evidence in chain.iter().rev().cloned() {
            reverse
                .offer_terminal(
                    &ticket,
                    &RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)),
                )
                .unwrap();
        }
        assert_eq!(reverse.pending_edges(), 0);
        let reverse = reverse.seal(&empty_intervals()).unwrap();
        assert_eq!(reverse.bad_credit_customers()[0].final_data(), final_data);
        assert_eq!(
            reverse.bad_credit_customers()[0].final_payment_count(),
            final_count
        );
        assert!(
            reverse.bad_credit_customers()[0].payment_suffix().len()
                <= MAX_BAD_CREDIT_SUFFIX_ENTRIES
        );
        assert!(
            reverse.bad_credit_customers()[0].payment_suffix().len()
                < reverse.bad_credit_customers()[0].committed_payment_updates() as usize
        );

        // The composite ACK contract allows at most one unrooted receipt per
        // client. Three successors may wait behind one gap at clients=3, and
        // the bridge drains all three exactly.
        let mut gap = collector_with_customer(*b"BC", b"old-data", 3);
        gap.offer_terminal(
            &ticket,
            &RankedTransactionOutcome::Committed(RankedCommit::Payment(chain[0].clone())),
        )
        .unwrap();
        for evidence in chain.iter().skip(2).take(3).cloned() {
            gap.offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)),
            )
            .unwrap();
        }
        assert_eq!(gap.pending_edges(), 3);
        gap.offer_terminal(
            &ticket,
            &RankedTransactionOutcome::Committed(RankedCommit::Payment(chain[1].clone())),
        )
        .unwrap();
        assert_eq!(gap.pending_edges(), 0);
        let gap = gap.seal(&empty_intervals()).unwrap();
        assert_eq!(gap.bad_credit_customers()[0].final_data(), final_data);
        assert_eq!(
            gap.bad_credit_customers()[0].committed_payment_updates(),
            chain.len() as u64
        );
    }

    #[test]
    fn bad_credit_pending_cap_plus_one_fails_closed() {
        let ticket = local_payment_ticket();
        let chain = bad_credit_chain(&ticket, 6);
        let mut collector = collector_with_customer(*b"BC", b"old-data", 3);
        collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(chain[0].clone())),
            )
            .unwrap();
        for evidence in chain.iter().skip(2).take(3).cloned() {
            collector
                .offer_terminal(
                    &ticket,
                    &RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)),
                )
                .unwrap();
        }
        assert_eq!(collector.pending_edges(), 3);
        assert!(matches!(
            collector.offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(chain[5].clone())),
            ),
            Err(RichRecoveryError::PendingCustomerLimit {
                actual: 4,
                limit: 3
            })
        ));
        assert!(collector.is_poisoned());
        assert_eq!(collector.pending_edges(), 3);
    }

    #[test]
    fn bad_credit_missing_count_cannot_be_hidden_by_an_old_duplicate() {
        let ticket = local_payment_ticket();
        let chain = bad_credit_chain(&ticket, 6);
        let mut collector = collector_with_customer(*b"BC", b"old-data", 5);
        for index in [0, 2, 3, 4, 5] {
            collector
                .offer_terminal(
                    &ticket,
                    &RankedTransactionOutcome::Committed(RankedCommit::Payment(
                        chain[index].clone(),
                    )),
                )
                .unwrap();
        }
        assert_eq!(collector.pending_edges(), 4);
        let history_before = collector.history_rows;
        assert!(collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(chain[0].clone())),
            )
            .is_err());
        assert!(collector.is_poisoned());
        assert_eq!(collector.history_rows, history_before);
        assert_eq!(collector.pending_edges(), 4);
    }

    #[test]
    fn bad_credit_rejects_self_consistent_edge_with_unrelated_before_data_atomically() {
        let ticket = local_payment_ticket();
        let chain = bad_credit_chain(&ticket, 2);
        let mut collector = collector();
        collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(chain[0].clone())),
            )
            .unwrap();
        let retained_history = collector.history_rows;
        let retained_updates = collector
            .customers
            .entries
            .values()
            .next()
            .unwrap()
            .update_count;

        let forged = payment_evidence(
            &ticket,
            true,
            chain[1].customer_version_before,
            TEST_TIMESTAMP,
            b"FORGED-HISTORY",
            b"unrelated-data".to_vec(),
        );
        assert!(collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(forged)),
            )
            .is_err());
        assert!(collector.is_poisoned());
        assert_eq!(collector.history_rows, retained_history);
        assert_eq!(
            collector
                .customers
                .entries
                .values()
                .next()
                .unwrap()
                .update_count,
            retained_updates
        );
    }

    #[test]
    fn payment_credit_flag_must_match_the_generated_setup_row_both_ways() {
        let ticket = local_payment_ticket();
        let claimed_bc = payment_evidence(
            &ticket,
            true,
            CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            TEST_TIMESTAMP,
            b"HISTORY",
            b"old-data".to_vec(),
        );
        let mut generated_gc = collector_with_customer(*b"GC", b"old-data", 1);
        assert!(matches!(
            generated_gc.offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(claimed_bc)),
            ),
            Err(RichRecoveryError::CustomerCreditFlagMismatch { .. })
        ));

        let claimed_gc = payment_evidence(
            &ticket,
            false,
            CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            TEST_TIMESTAMP,
            b"HISTORY",
            b"old-data".to_vec(),
        );
        let mut generated_bc = collector();
        assert!(matches!(
            generated_bc.offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(claimed_gc)),
            ),
            Err(RichRecoveryError::CustomerCreditFlagMismatch { .. })
        ));
    }

    #[test]
    fn bad_credit_oracle_ignores_delivery_count_after_the_final_payment() {
        let ticket = local_payment_ticket();
        let mut chain = bad_credit_chain(&ticket, 2);
        chain[1].customer_version_before.delivery_count = 7;
        chain[1].customer_version_after.delivery_count = 7;
        let mut collector = collector();
        for evidence in chain.iter().cloned() {
            collector
                .offer_terminal(
                    &ticket,
                    &RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)),
                )
                .unwrap();
        }
        let sealed = collector.seal(&empty_intervals()).unwrap();
        let sample = &sealed.bad_credit_customers()[0];
        assert_eq!(sample.final_payment_count(), 3);
        assert_eq!(sample.committed_payment_updates(), 2);
    }

    #[test]
    fn unsafe_character_bytes_are_rejected_before_retention() {
        assert!(matches!(
            validate_bounded_char("quote", b"unsafe'char", 1, 20),
            Err(RichRecoveryError::InvalidChar { .. })
        ));
        assert!(matches!(
            validate_bounded_char("utf8", &[0xff], 1, 20),
            Err(RichRecoveryError::InvalidChar { .. })
        ));

        let ticket = normal_new_order_ticket();
        let mut evidence = new_order_evidence(&ticket, 3_001);
        evidence.entry_timestamp = b"2026-07-29'12:34:56".to_vec();
        let mut collector = collector();
        assert!(collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence)),
            )
            .is_err());
        assert!(collector.orders.entries.is_empty());
    }

    #[test]
    fn malformed_line_timestamp_and_customer_data_poison_atomically() {
        let ticket = normal_new_order_ticket();
        let mut malformed = new_order_evidence(&ticket, 3_001);
        malformed.recovery_lines[0].district_info.pop();
        let mut line_collector = collector();
        assert!(line_collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::NewOrder(malformed)),
            )
            .is_err());
        assert!(line_collector.is_poisoned());
        assert_eq!(line_collector.new_order_commits, 0);
        assert!(line_collector.orders.entries.is_empty());

        let ticket = local_payment_ticket();
        let mut malformed = payment_evidence(
            &ticket,
            true,
            CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            TEST_TIMESTAMP,
            b"HISTORY",
            b"old".to_vec(),
        );
        malformed.customer_data_after.push(b'X');
        let mut data_collector = collector();
        assert!(data_collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(malformed)),
            )
            .is_err());
        assert_eq!(data_collector.history_rows, 0);
        assert!(data_collector.histories.entries.is_empty());

        let ticket = delivery_ticket(None);
        let key = OrderKey {
            warehouse_id: ticket.route().home_warehouse,
            district_id: 1,
            order_id: 1,
        };
        let malformed = delivery_evidence(key, 1, b"bad\0timestamp", vec![1.0_f32.to_bits(); 5]);
        let mut timestamp_collector = collector();
        assert!(timestamp_collector
            .offer_terminal(
                &ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Delivery(vec![malformed])),
            )
            .is_err());
        assert_eq!(timestamp_collector.delivered_orders, 0);
        assert!(timestamp_collector.deliveries.entries.is_empty());
    }

    #[test]
    fn ddl_timestamp_widths_reject_twenty_byte_entry_and_history_but_allow_delivery_30() {
        let new_order_ticket = normal_new_order_ticket();
        let mut new_order = new_order_evidence(&new_order_ticket, 3_001);
        new_order.entry_timestamp = vec![b'E'; 20];
        let mut entry_collector = collector();
        assert!(entry_collector
            .offer_terminal(
                &new_order_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::NewOrder(new_order)),
            )
            .is_err());

        let payment_ticket = local_payment_ticket();
        let payment = payment_evidence(
            &payment_ticket,
            false,
            CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            &[b'H'; 20],
            b"HISTORY",
            Vec::new(),
        );
        let mut history_collector = collector();
        assert!(history_collector
            .offer_terminal(
                &payment_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Payment(payment)),
            )
            .is_err());

        let delivery_ticket = delivery_ticket(None);
        let key = OrderKey {
            warehouse_id: delivery_ticket.route().home_warehouse,
            district_id: 1,
            order_id: 1,
        };
        let delivery = delivery_evidence(
            key,
            1,
            &[b'D'; MAX_DELIVERY_TIMESTAMP_BYTES],
            vec![1.0_f32.to_bits(); 5],
        );
        let mut delivery_collector = collector();
        delivery_collector
            .offer_terminal(
                &delivery_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Delivery(vec![delivery])),
            )
            .unwrap();
        assert_eq!(
            delivery_collector
                .seal(&empty_intervals())
                .unwrap()
                .deliveries()[0]
                .delivery_timestamp()
                .len(),
            MAX_DELIVERY_TIMESTAMP_BYTES
        );
    }

    #[test]
    fn rollback_and_read_only_terminals_create_no_samples() {
        let rollback = rollback_new_order_ticket();
        let order_status = find_ticket(|ticket| ticket.kind() == TransactionKind::OrderStatus);
        let stock_level = find_ticket(|ticket| ticket.kind() == TransactionKind::StockLevel);
        let mut collector = collector();
        collector
            .offer_terminal(&rollback, &RankedTransactionOutcome::ExpectedRollback)
            .unwrap();
        collector
            .offer_terminal(
                &order_status,
                &RankedTransactionOutcome::Committed(RankedCommit::OrderStatus),
            )
            .unwrap();
        collector
            .offer_terminal(
                &stock_level,
                &RankedTransactionOutcome::Committed(RankedCommit::StockLevel {
                    low_stock_count: 0,
                }),
            )
            .unwrap();
        let sealed = collector.seal(&empty_intervals()).unwrap();
        assert!(sealed.new_orders().is_empty());
        assert!(sealed.deliveries().is_empty());
        assert!(sealed.bad_credit_customers().is_empty());
        assert_eq!(sealed.history_tuples().count(), 0);
    }

    #[test]
    fn bottom_k_is_deterministic_at_k_minus_one_k_and_k_plus_one() {
        fn build(order: impl Iterator<Item = u64>, capacity: usize) -> RankedReservoir<u64, u64> {
            let mut reservoir = RankedReservoir::new(capacity);
            for value in order {
                reservoir.ensure(
                    SampleScore {
                        high: value,
                        low: value.rotate_left(17),
                    },
                    value,
                    || value,
                );
            }
            reservoir
        }
        let below = build(0..63, RICH_RECOVERY_SAMPLE_CAPACITY);
        assert_eq!(below.entries.len(), 63);
        assert!(below.rejected.is_none());
        let exact = build(0..64, RICH_RECOVERY_SAMPLE_CAPACITY);
        assert_eq!(exact.entries.len(), 64);
        assert!(exact.rejected.is_none());
        let ascending = build(0..65, RICH_RECOVERY_SAMPLE_CAPACITY);
        let descending = build((0..65).rev(), RICH_RECOVERY_SAMPLE_CAPACITY);
        assert_eq!(
            ascending.entries.keys().collect::<Vec<_>>(),
            descending.entries.keys().collect::<Vec<_>>()
        );
        assert_eq!(ascending.entries.len(), 64);
        assert_eq!(ascending.rejected.as_ref().unwrap().key, 64);

        let history = build(0..3, RICH_HISTORY_SAMPLE_CAPACITY);
        assert_eq!(history.entries.len(), RICH_HISTORY_SAMPLE_CAPACITY);
        assert!(history.rejected.is_some());

        let tied_score = SampleScore { high: 7, low: 11 };
        let mut tied = RankedReservoir::new(2);
        for key in [3, 1, 2] {
            tied.ensure(tied_score, key, || key);
        }
        assert_eq!(
            tied.entries
                .keys()
                .map(|ranked| ranked.key)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(tied.rejected.as_ref().unwrap().key, 3);
    }

    #[test]
    fn real_bad_credit_cutoff_seals_at_k_and_k_plus_one() {
        let exact = distinct_bad_credit_customers(RICH_RECOVERY_SAMPLE_CAPACITY)
            .seal(&empty_intervals())
            .unwrap();
        assert_eq!(
            exact.bad_credit_payment_count(),
            RICH_RECOVERY_SAMPLE_CAPACITY as u64
        );
        assert_eq!(
            exact.bad_credit_customers().len(),
            RICH_RECOVERY_SAMPLE_CAPACITY
        );
        assert!(exact.bad_customer_rejected_witness().is_none());

        let sealed = distinct_bad_credit_customers(RICH_RECOVERY_SAMPLE_CAPACITY + 1)
            .seal(&empty_intervals())
            .unwrap();
        assert_eq!(
            sealed.bad_credit_payment_count(),
            (RICH_RECOVERY_SAMPLE_CAPACITY + 1) as u64
        );
        assert_eq!(
            sealed.bad_credit_customers().len(),
            RICH_RECOVERY_SAMPLE_CAPACITY
        );
        assert!(sealed.bad_customer_rejected_witness().is_some());
    }

    #[test]
    fn one_million_rejected_offers_keep_capacity_and_payloads_stable() {
        let mut reservoir = RankedReservoir::new(RICH_RECOVERY_SAMPLE_CAPACITY);
        for value in 0..RICH_RECOVERY_SAMPLE_CAPACITY as u64 {
            reservoir.ensure(
                SampleScore {
                    high: value,
                    low: 0,
                },
                value,
                || Box::new([value as u8; 1024]),
            );
        }
        let before = reservoir
            .entries
            .values()
            .map(|payload| payload.as_ptr())
            .collect::<Vec<_>>();
        for value in 0..1_000_000_u64 {
            assert!(!reservoir.ensure(
                SampleScore {
                    high: 1_000_000 + value,
                    low: value,
                },
                1_000_000 + value,
                || Box::new([0; 1024]),
            ));
        }
        let after = reservoir
            .entries
            .values()
            .map(|payload| payload.as_ptr())
            .collect::<Vec<_>>();
        assert_eq!(reservoir.entries.len(), RICH_RECOVERY_SAMPLE_CAPACITY);
        assert_eq!(reservoir.by_key.len(), RICH_RECOVERY_SAMPLE_CAPACITY);
        assert_eq!(before, after);
    }

    #[test]
    fn retired_pending_reaches_client_cap_without_eviction() {
        let mut collector = collector_with_customer(*b"BC", b"old-data", 3);
        for customer_id in 100..102 {
            collector.retired_customers.insert(
                CustomerKey {
                    warehouse_id: 1,
                    district_id: 1,
                    customer_id,
                },
                unrooted_customer_state(
                    CustomerKey {
                        warehouse_id: 1,
                        district_id: 1,
                        customer_id,
                    },
                    2,
                ),
            );
        }
        for customer_id in 1..=RICH_RECOVERY_SAMPLE_CAPACITY as i32 {
            let key = CustomerKey {
                warehouse_id: 1,
                district_id: 1,
                customer_id,
            };
            let state = if customer_id == RICH_RECOVERY_SAMPLE_CAPACITY as i32 {
                unrooted_customer_state(key, 2)
            } else {
                CustomerDataState::new(OFFICIAL_WAREHOUSES, key, b"old-data".to_vec())
            };
            collector.customers.ensure(
                SampleScore {
                    high: u64::MAX,
                    low: customer_id as u64,
                },
                key,
                || state,
            );
        }
        collector.pending_customer_edges = 3;
        let candidate_key = CustomerKey {
            warehouse_id: 1,
            district_id: 1,
            customer_id: 3_000,
        };
        let prepared = prepared_bad_customer(candidate_key, CUSTOMER_INITIAL_PAYMENT_COUNT);
        collector.preview_bad_customer(&prepared).unwrap();
        collector.commit_terminal(PreparedRichTerminal::Payment {
            history: HistoryTupleKey {
                group: HistoryGroupKey {
                    customer_id: 3_000,
                    customer_district_id: 1,
                    customer_warehouse_id: 1,
                    home_district_id: 1,
                    home_warehouse_id: 1,
                },
                timestamp: TEST_TIMESTAMP.to_vec(),
                amount_bits: 1.0_f32.to_bits(),
                data: b"RETIRED-CAP".to_vec(),
            },
            bad_customer: Some(prepared),
        });
        assert_eq!(collector.pending_edges(), 3);
        assert_eq!(collector.retired_customers.len(), 3);
        assert!(collector
            .retired_customers
            .values()
            .all(|state| !state.pending.is_empty()));
    }

    #[test]
    fn retired_pending_is_stable_under_one_million_rank_rejections() {
        let mut collector = collector_with_customer(*b"BC", b"old-data", 1);
        let retired_key = CustomerKey {
            warehouse_id: 1,
            district_id: 1,
            customer_id: 100,
        };
        collector
            .retired_customers
            .insert(retired_key, unrooted_customer_state(retired_key, 2));
        collector.pending_customer_edges = 1;
        for customer_id in 1..=RICH_RECOVERY_SAMPLE_CAPACITY as i32 {
            let key = CustomerKey {
                warehouse_id: 1,
                district_id: 1,
                customer_id,
            };
            collector.customers.ensure(
                SampleScore {
                    high: 0,
                    low: customer_id as u64,
                },
                key,
                || CustomerDataState::new(OFFICIAL_WAREHOUSES, key, b"old-data".to_vec()),
            );
        }
        let candidate = prepared_bad_customer(
            CustomerKey {
                warehouse_id: 1,
                district_id: 1,
                customer_id: 3_000,
            },
            CUSTOMER_INITIAL_PAYMENT_COUNT,
        );
        let retired = collector.retired_customers.get(&retired_key).unwrap();
        let endpoint_ptr = retired.endpoint_data.as_ptr();
        let edge = retired.pending.values().next().unwrap();
        let before_ptr = edge.before_data.as_ptr();
        let after_ptr = edge.after_data.as_ptr();
        for _ in 0..1_000_000 {
            collector.preview_bad_customer(&candidate).unwrap();
        }
        let retired = collector.retired_customers.get(&retired_key).unwrap();
        let edge = retired.pending.values().next().unwrap();
        assert_eq!(collector.pending_edges(), 1);
        assert_eq!(collector.retired_customers.len(), 1);
        assert_eq!(retired.endpoint_data.as_ptr(), endpoint_ptr);
        assert_eq!(edge.before_data.as_ptr(), before_ptr);
        assert_eq!(edge.after_data.as_ptr(), after_ptr);
    }

    #[test]
    fn all_retained_fields_fit_the_raw_ceiling_by_construction() {
        assert!(THEORETICAL_MAX_RICH_BYTES <= MAX_RICH_RECOVERY_RAW_BYTES);
        assert_eq!(RICH_HISTORY_SAMPLE_CAPACITY, 2);
        assert_eq!(RICH_RECOVERY_SAMPLE_CAPACITY, 64);

        let orders = (0..RICH_RECOVERY_SAMPLE_CAPACITY)
            .map(|index| SealedNewOrderSample {
                score: SampleScore {
                    high: index as u64,
                    low: 0,
                },
                key: OrderKey {
                    warehouse_id: 1,
                    district_id: 1,
                    order_id: 3_001 + index as i32,
                },
                customer_id: 1,
                entry_timestamp: vec![b'T'; MAX_ENTRY_TIMESTAMP_BYTES],
                carrier_id: MAX_CARRIER_ID,
                line_count: MAX_ORDER_LINES,
                all_local: true,
                queue_present: false,
                lines: (1..=MAX_ORDER_LINES)
                    .map(|number| SealedOrderLine {
                        number,
                        item_id: 1,
                        supply_warehouse: 1,
                        delivery_timestamp: vec![b'T'; MAX_DELIVERY_TIMESTAMP_BYTES],
                        quantity: MAX_ITEM_QUANTITY,
                        amount_bits: 9_999.99_f32.to_bits(),
                        district_info: vec![b'D'; DISTRICT_INFO_BYTES],
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let deliveries = (0..RICH_RECOVERY_SAMPLE_CAPACITY)
            .map(|index| SealedDeliverySample {
                score: SampleScore {
                    high: index as u64,
                    low: 1,
                },
                key: OrderKey {
                    warehouse_id: 1,
                    district_id: 1,
                    order_id: 3_001 + index as i32,
                },
                customer_id: 1,
                carrier_id: MAX_CARRIER_ID,
                queue_present: false,
                delivery_timestamp: vec![b'T'; MAX_DELIVERY_TIMESTAMP_BYTES],
                lines: (1..=MAX_ORDER_LINES)
                    .map(|number| SealedDeliveryLine {
                        number,
                        delivery_timestamp: vec![b'T'; MAX_DELIVERY_TIMESTAMP_BYTES],
                        amount_bits: 9_999.99_f32.to_bits(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();
        let customers = (0..RICH_RECOVERY_SAMPLE_CAPACITY)
            .map(|index| SealedBadCreditCustomerSample {
                score: SampleScore {
                    high: index as u64,
                    low: 2,
                },
                key: CustomerKey {
                    warehouse_id: 1,
                    district_id: 1,
                    customer_id: index as i32 + 1,
                },
                final_payment_count: 2,
                credit: *b"BC",
                data: vec![b'C'; MAX_CUSTOMER_DATA_BYTES],
                committed_payment_updates: 1,
                payment_suffix: vec![
                    SealedBadCreditPaymentPrefix {
                        home_warehouse_id: OFFICIAL_WAREHOUSES,
                        home_district_id: DISTRICTS_PER_WAREHOUSE,
                        amount_cents: MAX_PAYMENT_CENTS,
                    };
                    MAX_BAD_CREDIT_SUFFIX_ENTRIES
                ],
            })
            .collect::<Vec<_>>();
        let histories = (0..RICH_HISTORY_SAMPLE_CAPACITY)
            .map(|index| SealedHistoryGroup {
                key: HistoryGroupKey {
                    customer_id: index as i32 + 1,
                    customer_district_id: 1,
                    customer_warehouse_id: 1,
                    home_district_id: 1,
                    home_warehouse_id: 1,
                },
                tuples: vec![SealedHistoryTuple {
                    score: SampleScore {
                        high: index as u64,
                        low: 3,
                    },
                    timestamp: vec![b'T'; MAX_HISTORY_TIMESTAMP_BYTES],
                    amount_bits: 5_000.0_f32.to_bits(),
                    data: vec![b'H'; MAX_HISTORY_DATA_BYTES],
                    committed_multiplicity: 1,
                    setup_collision_multiplicity: 1,
                }],
            })
            .collect::<Vec<_>>();
        let order_witness = OrderCutoffWitness {
            score: SampleScore {
                high: u64::MAX,
                low: 0,
            },
            key: OrderKey {
                warehouse_id: 50,
                district_id: 10,
                order_id: i32::MAX,
            },
        };
        let customer_witness = CustomerCutoffWitness {
            score: SampleScore {
                high: u64::MAX,
                low: 1,
            },
            key: CustomerKey {
                warehouse_id: 50,
                district_id: 10,
                customer_id: 3_000,
            },
        };
        let history_witness = HistoryCutoffWitness {
            score: SampleScore {
                high: u64::MAX,
                low: 2,
            },
            group: histories[0].key,
            timestamp: vec![b'T'; MAX_HISTORY_TIMESTAMP_BYTES],
            amount_bits: 5_000.0_f32.to_bits(),
            data: vec![b'H'; MAX_HISTORY_DATA_BYTES],
        };
        let actual = sealed_raw_size(
            &orders,
            &deliveries,
            &customers,
            &histories,
            Some(&order_witness),
            Some(&order_witness),
            Some(&customer_witness),
            Some(&history_witness),
        )
        .unwrap();
        assert_eq!(actual, THEORETICAL_MAX_RICH_BYTES);
        assert!(actual < MAX_RICH_RECOVERY_RAW_BYTES);
    }
}
