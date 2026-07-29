//! Fixed-space physical accounting for terminal TPC-C results.
//!
//! This module intentionally retains no terminal events and no per-amount
//! vectors. Every accepted terminal contributes to one of nine semantic
//! classes, one or more of the fixed 50x10 partitions, and one of three
//! per-class exact non-negative FLOAT32 accumulator groups.

use thiserror::Error;

use crate::consistency::{
    sum_f32_as_f64_once, FloatError, NonNegativeF32Accumulator, CUSTOMERS_PER_DISTRICT,
    DISTRICTS_PER_WAREHOUSE, FINAL_WAREHOUSES,
};
use crate::profile::{ITEM_COUNT, OFFICIAL_CLIENTS};
use crate::workload::{
    CustomerSelector, TransactionParameters, TransactionTicket, MAX_CARRIER_ID, MAX_ITEM_QUANTITY,
    MAX_ORDER_LINES, MIN_CARRIER_ID, MIN_ITEM_QUANTITY, MIN_ORDER_LINES,
};

use super::ledger::LedgerClass;
use super::runner::{CustomerVersion, RankedCommit, RankedTransactionOutcome};

pub const LEDGER_CLASS_COUNT: usize = 9;
pub const PHYSICAL_PARTITION_COUNT: usize =
    (FINAL_WAREHOUSES as usize) * (DISTRICTS_PER_WAREHOUSE as usize);

const MAX_DELIVERY_ORDERS: usize = DISTRICTS_PER_WAREHOUSE as usize;

/// Per-class transaction and physical mutation totals.
///
/// `new_order_commits` and `new_orders` deliberately remain separate even
/// though both advance by one for a valid committed NewOrder. The first is the
/// five-family terminal count; the second is the physical order-row delta.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ClassTotals {
    pub new_order_commits: u64,
    pub payment_commits: u64,
    pub order_status_commits: u64,
    pub delivery_commits: u64,
    pub stock_level_commits: u64,
    pub expected_rollbacks: u64,
    pub new_orders: u64,
    pub new_order_lines: u64,
    pub remote_new_order_lines: u64,
    pub stock_quantity_delta: u64,
    pub delivered_orders: u64,
    pub delivered_order_lines: u64,
}

impl ClassTotals {
    fn checked_add(self, other: Self) -> Result<Self, BoundedStatsError> {
        Ok(Self {
            new_order_commits: checked_add(
                self.new_order_commits,
                other.new_order_commits,
                "new_order_commits",
            )?,
            payment_commits: checked_add(
                self.payment_commits,
                other.payment_commits,
                "payment_commits",
            )?,
            order_status_commits: checked_add(
                self.order_status_commits,
                other.order_status_commits,
                "order_status_commits",
            )?,
            delivery_commits: checked_add(
                self.delivery_commits,
                other.delivery_commits,
                "delivery_commits",
            )?,
            stock_level_commits: checked_add(
                self.stock_level_commits,
                other.stock_level_commits,
                "stock_level_commits",
            )?,
            expected_rollbacks: checked_add(
                self.expected_rollbacks,
                other.expected_rollbacks,
                "expected_rollbacks",
            )?,
            new_orders: checked_add(self.new_orders, other.new_orders, "new_orders")?,
            new_order_lines: checked_add(
                self.new_order_lines,
                other.new_order_lines,
                "new_order_lines",
            )?,
            remote_new_order_lines: checked_add(
                self.remote_new_order_lines,
                other.remote_new_order_lines,
                "remote_new_order_lines",
            )?,
            stock_quantity_delta: checked_add(
                self.stock_quantity_delta,
                other.stock_quantity_delta,
                "stock_quantity_delta",
            )?,
            delivered_orders: checked_add(
                self.delivered_orders,
                other.delivered_orders,
                "delivered_orders",
            )?,
            delivered_order_lines: checked_add(
                self.delivered_order_lines,
                other.delivered_order_lines,
                "delivered_order_lines",
            )?,
        })
    }

    fn validate(self) -> Result<(), BoundedStatsError> {
        if self.new_order_commits != self.new_orders {
            return Err(BoundedStatsError::Inconsistent(
                "NewOrder commit and physical order totals differ",
            ));
        }
        validate_line_range(self.new_orders, self.new_order_lines, "committed NewOrder")?;
        if self.remote_new_order_lines > self.new_order_lines {
            return Err(BoundedStatsError::Inconsistent(
                "remote NewOrder lines exceed all NewOrder lines",
            ));
        }
        let maximum_stock_quantity = checked_mul(
            self.new_order_lines,
            u64::from(MAX_ITEM_QUANTITY),
            "stock quantity",
        )?;
        if self.stock_quantity_delta < self.new_order_lines
            || self.stock_quantity_delta > maximum_stock_quantity
        {
            if self.new_order_lines != 0 || self.stock_quantity_delta != 0 {
                return Err(BoundedStatsError::Inconsistent(
                    "stock quantity delta is outside 1..=10 per NewOrder line",
                ));
            }
        }
        validate_line_range(
            self.delivered_orders,
            self.delivered_order_lines,
            "delivered order",
        )?;
        let maximum_delivered_orders = checked_mul(
            self.delivery_commits,
            DISTRICTS_PER_WAREHOUSE as u64,
            "Delivery orders per commit",
        )?;
        if self.delivered_orders > maximum_delivered_orders {
            return Err(BoundedStatsError::Inconsistent(
                "delivered orders exceed ten per Delivery commit",
            ));
        }
        Ok(())
    }
}

/// Fixed-domain physical deltas for one `(warehouse, district)` partition.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PartitionTotals {
    pub new_orders: u64,
    pub new_order_lines: u64,
    pub delivered_orders: u64,
    pub delivered_order_lines: u64,
}

impl PartitionTotals {
    fn checked_add(self, other: Self) -> Result<Self, BoundedStatsError> {
        Ok(Self {
            new_orders: checked_add(self.new_orders, other.new_orders, "partition new_orders")?,
            new_order_lines: checked_add(
                self.new_order_lines,
                other.new_order_lines,
                "partition new_order_lines",
            )?,
            delivered_orders: checked_add(
                self.delivered_orders,
                other.delivered_orders,
                "partition delivered_orders",
            )?,
            delivered_order_lines: checked_add(
                self.delivered_order_lines,
                other.delivered_order_lines,
                "partition delivered_order_lines",
            )?,
        })
    }

    fn validate(self) -> Result<(), BoundedStatsError> {
        validate_line_range(
            self.new_orders,
            self.new_order_lines,
            "partition committed NewOrder",
        )?;
        validate_line_range(
            self.delivered_orders,
            self.delivered_order_lines,
            "partition delivered order",
        )
    }
}

/// Constant-domain terminal accounting suitable for replacing the unbounded
/// aggregate portion of `RunLedger`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundedPhysicalStats {
    classes: [ClassTotals; LEDGER_CLASS_COUNT],
    partitions: [PartitionTotals; PHYSICAL_PARTITION_COUNT],
    payment_history_amounts: [NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
    new_order_line_amounts: [NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
    delivery_customer_amounts: [NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
}

impl Default for BoundedPhysicalStats {
    fn default() -> Self {
        Self {
            classes: [ClassTotals::default(); LEDGER_CLASS_COUNT],
            partitions: [PartitionTotals::default(); PHYSICAL_PARTITION_COUNT],
            payment_history_amounts: std::array::from_fn(|_| NonNegativeF32Accumulator::default()),
            new_order_line_amounts: std::array::from_fn(|_| NonNegativeF32Accumulator::default()),
            delivery_customer_amounts: std::array::from_fn(|_| {
                NonNegativeF32Accumulator::default()
            }),
        }
    }
}

impl BoundedPhysicalStats {
    pub fn class_totals(&self, class: LedgerClass) -> Result<ClassTotals, BoundedStatsError> {
        let index = class_index_without_stage(class)?;
        Ok(self.classes[index])
    }

    pub fn totals(&self) -> Result<ClassTotals, BoundedStatsError> {
        let mut total = ClassTotals::default();
        for class in self.classes {
            total = total.checked_add(class)?;
        }
        Ok(total)
    }

    pub fn partition_totals(
        &self,
        warehouse_id: i32,
        district_id: i32,
    ) -> Result<PartitionTotals, BoundedStatsError> {
        Ok(self.partitions[partition_index(warehouse_id, district_id)?])
    }

    pub fn partition_totals_iter(
        &self,
    ) -> impl ExactSizeIterator<Item = ((i32, i32), PartitionTotals)> + '_ {
        self.partitions
            .iter()
            .copied()
            .enumerate()
            .map(|(index, totals)| {
                let warehouse_id = (index / DISTRICTS_PER_WAREHOUSE as usize) as i32 + 1;
                let district_id = (index % DISTRICTS_PER_WAREHOUSE as usize) as i32 + 1;
                ((warehouse_id, district_id), totals)
            })
    }

    pub fn payment_history_amounts_for(
        &self,
        class: LedgerClass,
    ) -> Result<&NonNegativeF32Accumulator, BoundedStatsError> {
        Ok(&self.payment_history_amounts[class_index_without_stage(class)?])
    }

    pub fn new_order_line_amounts_for(
        &self,
        class: LedgerClass,
    ) -> Result<&NonNegativeF32Accumulator, BoundedStatsError> {
        Ok(&self.new_order_line_amounts[class_index_without_stage(class)?])
    }

    pub fn delivery_customer_amounts_for(
        &self,
        class: LedgerClass,
    ) -> Result<&NonNegativeF32Accumulator, BoundedStatsError> {
        Ok(&self.delivery_customer_amounts[class_index_without_stage(class)?])
    }

    pub fn payment_history_amounts(&self) -> Result<NonNegativeF32Accumulator, BoundedStatsError> {
        merge_accumulator_group(&self.payment_history_amounts, "Payment/history amount")
    }

    pub fn new_order_line_amounts(&self) -> Result<NonNegativeF32Accumulator, BoundedStatsError> {
        merge_accumulator_group(&self.new_order_line_amounts, "NewOrder line amount")
    }

    pub fn delivery_customer_amounts(
        &self,
    ) -> Result<NonNegativeF32Accumulator, BoundedStatsError> {
        merge_accumulator_group(&self.delivery_customer_amounts, "Delivery customer amount")
    }

    /// Validate and atomically account for one confirmed terminal.
    ///
    /// Retryable abort attempts are not terminal outcomes and must not be
    /// offered. Every touched counter, partition, and accumulator is staged
    /// before any field in `self` is changed.
    ///
    /// The caller must offer each terminal exactly once and must pass the
    /// scheduler's in-window or tail disposition. A frozen ticket contains its
    /// stage but cannot encode the instant at which its response crossed the
    /// deadline; bounded aggregate statistics therefore cannot infer that
    /// distinction or retain an unbounded exact duplicate-identity set.
    pub fn offer_terminal(
        &mut self,
        class: LedgerClass,
        ticket: &TransactionTicket,
        outcome: &RankedTransactionOutcome,
    ) -> Result<(), BoundedStatsError> {
        let delta = TerminalDelta::from_terminal(class, ticket, outcome)?;

        let next_class = self.classes[delta.class_index].checked_add(delta.class_totals)?;
        next_class.validate()?;
        // Ensure a caller can always obtain the physical all-class total.
        self.totals()?.checked_add(delta.class_totals)?;

        let mut next_partitions = [PartitionAssignment::default(); MAX_DELIVERY_ORDERS];
        for (slot, update) in delta.partition_updates[..delta.partition_count]
            .iter()
            .copied()
            .enumerate()
        {
            let value = self.partitions[update.index].checked_add(update.delta)?;
            value.validate()?;
            next_partitions[slot] = PartitionAssignment {
                index: update.index,
                value,
            };
        }

        let mut next_payment_history = self.payment_history_amounts[delta.class_index].clone();
        if let Some(bits) = delta.payment_history_amount {
            next_payment_history
                .add_bits(bits)
                .map_err(|source| BoundedStatsError::Float {
                    field: "Payment/history amount",
                    source,
                })?;
        }

        let mut next_new_order_lines = self.new_order_line_amounts[delta.class_index].clone();
        next_new_order_lines
            .extend_bits(delta.new_order_line_amounts.iter().copied())
            .map_err(|source| BoundedStatsError::Float {
                field: "NewOrder line amount",
                source,
            })?;

        let mut next_delivery_customers = self.delivery_customer_amounts[delta.class_index].clone();
        next_delivery_customers
            .extend_bits(
                delta.delivery_customer_amounts[..delta.delivery_amount_count]
                    .iter()
                    .copied(),
            )
            .map_err(|source| BoundedStatsError::Float {
                field: "Delivery customer amount",
                source,
            })?;

        validate_accumulator_replacement(
            &self.payment_history_amounts,
            delta.class_index,
            &next_payment_history,
            "Payment/history amount",
        )?;
        validate_accumulator_replacement(
            &self.new_order_line_amounts,
            delta.class_index,
            &next_new_order_lines,
            "NewOrder line amount",
        )?;
        validate_accumulator_replacement(
            &self.delivery_customer_amounts,
            delta.class_index,
            &next_delivery_customers,
            "Delivery customer amount",
        )?;

        // Commit only after every fallible operation above succeeded.
        self.classes[delta.class_index] = next_class;
        for assignment in next_partitions.iter().copied().take(delta.partition_count) {
            self.partitions[assignment.index] = assignment.value;
        }
        self.payment_history_amounts[delta.class_index] = next_payment_history;
        self.new_order_line_amounts[delta.class_index] = next_new_order_lines;
        self.delivery_customer_amounts[delta.class_index] = next_delivery_customers;
        Ok(())
    }

    /// Checked, atomic and order-independent merge of one bounded worker
    /// summary.
    pub fn merge(&mut self, other: &Self) -> Result<(), BoundedStatsError> {
        self.validate()?;
        other.validate()?;

        let mut next = self.clone();
        for (target, addition) in next.classes.iter_mut().zip(other.classes) {
            *target = target.checked_add(addition)?;
        }
        for (target, addition) in next.partitions.iter_mut().zip(other.partitions) {
            *target = target.checked_add(addition)?;
        }
        for (target, addition) in next
            .payment_history_amounts
            .iter_mut()
            .zip(&other.payment_history_amounts)
        {
            target
                .merge(addition)
                .map_err(|source| BoundedStatsError::Float {
                    field: "Payment/history amount",
                    source,
                })?;
        }
        for (target, addition) in next
            .new_order_line_amounts
            .iter_mut()
            .zip(&other.new_order_line_amounts)
        {
            target
                .merge(addition)
                .map_err(|source| BoundedStatsError::Float {
                    field: "NewOrder line amount",
                    source,
                })?;
        }
        for (target, addition) in next
            .delivery_customer_amounts
            .iter_mut()
            .zip(&other.delivery_customer_amounts)
        {
            target
                .merge(addition)
                .map_err(|source| BoundedStatsError::Float {
                    field: "Delivery customer amount",
                    source,
                })?;
        }
        next.validate()?;
        *self = next;
        Ok(())
    }

    pub fn merge_all<I>(summaries: I) -> Result<Self, BoundedStatsError>
    where
        I: IntoIterator<Item = Self>,
    {
        let mut merged = Self::default();
        for summary in summaries {
            merged.merge(&summary)?;
        }
        Ok(merged)
    }

    pub fn validate(&self) -> Result<(), BoundedStatsError> {
        for (index, class) in self.classes.iter().copied().enumerate() {
            class.validate()?;
            if self.payment_history_amounts[index].term_count() != class.payment_commits {
                return Err(BoundedStatsError::Inconsistent(
                    "Payment/history amount terms differ from Payment commits",
                ));
            }
            if self.new_order_line_amounts[index].term_count() != class.new_order_lines {
                return Err(BoundedStatsError::Inconsistent(
                    "NewOrder amount terms differ from NewOrder lines",
                ));
            }
            if self.delivery_customer_amounts[index].term_count() != class.delivered_orders {
                return Err(BoundedStatsError::Inconsistent(
                    "Delivery amount terms differ from delivered orders",
                ));
            }
        }
        for partition in self.partitions {
            partition.validate()?;
        }

        let total = self.totals()?;
        let mut partition_total = PartitionTotals::default();
        for partition in self.partitions {
            partition_total = partition_total.checked_add(partition)?;
        }
        if partition_total.new_orders != total.new_orders
            || partition_total.new_order_lines != total.new_order_lines
            || partition_total.delivered_orders != total.delivered_orders
            || partition_total.delivered_order_lines != total.delivered_order_lines
        {
            return Err(BoundedStatsError::Inconsistent(
                "partition totals differ from all-class physical totals",
            ));
        }
        self.payment_history_amounts()?;
        self.new_order_line_amounts()?;
        self.delivery_customer_amounts()?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum BoundedStatsError {
    #[error("invalid bounded physical evidence: {0}")]
    InvalidEvidence(&'static str),

    #[error("partition key is outside the final 50x10 domain: ({warehouse_id}, {district_id})")]
    InvalidPartitionKey { warehouse_id: i32, district_id: i32 },

    #[error("bounded physical counter overflow: {0}")]
    Overflow(&'static str),

    #[error("invalid FLOAT32 bits for {field}: 0x{bits:08x}")]
    InvalidFloatBits { field: &'static str, bits: u32 },

    #[error("inconsistent bounded physical evidence: {0}")]
    Inconsistent(&'static str),

    #[error("cannot accumulate {field}: {source}")]
    Float {
        field: &'static str,
        #[source]
        source: FloatError,
    },
}

#[derive(Clone, Copy, Debug, Default)]
struct PartitionIncrement {
    index: usize,
    delta: PartitionTotals,
}

#[derive(Clone, Copy, Debug, Default)]
struct PartitionAssignment {
    index: usize,
    value: PartitionTotals,
}

struct TerminalDelta<'a> {
    class_index: usize,
    class_totals: ClassTotals,
    partition_updates: [PartitionIncrement; MAX_DELIVERY_ORDERS],
    partition_count: usize,
    payment_history_amount: Option<u32>,
    new_order_line_amounts: &'a [u32],
    delivery_customer_amounts: [u32; MAX_DELIVERY_ORDERS],
    delivery_amount_count: usize,
}

impl<'a> TerminalDelta<'a> {
    fn empty(class_index: usize) -> Self {
        Self {
            class_index,
            class_totals: ClassTotals::default(),
            partition_updates: [PartitionIncrement::default(); MAX_DELIVERY_ORDERS],
            partition_count: 0,
            payment_history_amount: None,
            new_order_line_amounts: &[],
            delivery_customer_amounts: [0; MAX_DELIVERY_ORDERS],
            delivery_amount_count: 0,
        }
    }

    fn from_terminal(
        class: LedgerClass,
        ticket: &'a TransactionTicket,
        outcome: &'a RankedTransactionOutcome,
    ) -> Result<Self, BoundedStatsError> {
        let route = ticket.route();
        let class_index = class_index(class, route.stage.value())?;
        if route.client_id >= OFFICIAL_CLIENTS {
            return Err(BoundedStatsError::InvalidEvidence(
                "ticket client_id is outside the final client domain",
            ));
        }
        partition_index(
            i32::from(route.home_warehouse),
            i32::from(route.home_district),
        )?;

        let mut delta = Self::empty(class_index);
        match (ticket.parameters(), outcome) {
            (
                TransactionParameters::NewOrder(input),
                RankedTransactionOutcome::ExpectedRollback,
            ) => {
                if !input.expected_rollback()
                    || input
                        .lines()
                        .last()
                        .is_none_or(|line| !line.is_invalid_item())
                {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "business rollback does not match the frozen invalid-item ticket",
                    ));
                }
                delta.class_totals.expected_rollbacks = 1;
            }
            (
                TransactionParameters::NewOrder(input),
                RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence)),
            ) => {
                if input.expected_rollback() {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "expected-rollback NewOrder cannot be committed",
                    ));
                }
                if evidence.warehouse_id != route.home_warehouse
                    || evidence.district_id != route.home_district
                    || evidence.line_count as usize != input.lines().len()
                    || evidence.line_amount_bits.len() != input.lines().len()
                {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "NewOrder outcome does not match its frozen ticket",
                    ));
                }
                if evidence.order_id <= 0 {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "NewOrder order_id must be positive",
                    ));
                }
                if !(1..=CUSTOMERS_PER_DISTRICT).contains(&i64::from(input.customer_id())) {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "NewOrder customer_id is outside 1..=3000",
                    ));
                }
                if !(usize::from(MIN_ORDER_LINES)..=usize::from(MAX_ORDER_LINES))
                    .contains(&input.lines().len())
                {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "NewOrder line count must be 5..=15",
                    ));
                }

                let mut remote_lines = 0_u64;
                let mut stock_quantity = 0_u64;
                for (index, (line, amount_bits)) in input
                    .lines()
                    .iter()
                    .zip(&evidence.line_amount_bits)
                    .enumerate()
                {
                    if usize::from(line.number()) != index + 1
                        || line.is_invalid_item()
                        || !(1..=ITEM_COUNT).contains(&line.item_id())
                        || !(MIN_ITEM_QUANTITY..=MAX_ITEM_QUANTITY).contains(&line.quantity())
                    {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "committed NewOrder contains an invalid frozen line",
                        ));
                    }
                    partition_index(i32::from(line.supply_warehouse()), 1)?;
                    validate_positive_amount("NewOrder line amount", *amount_bits, 1_000.0)?;
                    if line.supply_warehouse() != route.home_warehouse {
                        remote_lines = checked_add(remote_lines, 1, "NewOrder remote lines")?;
                    }
                    stock_quantity = checked_add(
                        stock_quantity,
                        u64::from(line.quantity()),
                        "NewOrder stock quantity",
                    )?;
                }
                if remote_lines != u64::from(evidence.remote_line_count)
                    || stock_quantity != u64::from(evidence.stock_ytd_delta)
                    || input.all_local() != (remote_lines == 0)
                {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "NewOrder derived totals differ from outcome evidence",
                    ));
                }

                let line_count = input.lines().len() as u64;
                delta.class_totals = ClassTotals {
                    new_order_commits: 1,
                    new_orders: 1,
                    new_order_lines: line_count,
                    remote_new_order_lines: remote_lines,
                    stock_quantity_delta: stock_quantity,
                    ..ClassTotals::default()
                };
                delta.partition_updates[0] = PartitionIncrement {
                    index: partition_index(
                        i32::from(route.home_warehouse),
                        i32::from(route.home_district),
                    )?,
                    delta: PartitionTotals {
                        new_orders: 1,
                        new_order_lines: line_count,
                        ..PartitionTotals::default()
                    },
                };
                delta.partition_count = 1;
                delta.new_order_line_amounts = &evidence.line_amount_bits;
            }
            (
                TransactionParameters::Payment(input),
                RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)),
            ) => {
                if evidence.warehouse_id != route.home_warehouse
                    || evidence.district_id != route.home_district
                    || evidence.customer_warehouse_id != input.customer_warehouse()
                    || evidence.customer_district_id != input.customer_district()
                    || evidence.amount_bits != input.amount_bits()
                {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "Payment outcome does not match its frozen ticket",
                    ));
                }
                partition_index(
                    i32::from(evidence.customer_warehouse_id),
                    i32::from(evidence.customer_district_id),
                )?;
                if evidence.customer_warehouse_id == evidence.warehouse_id
                    && evidence.customer_district_id != evidence.district_id
                {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "local Payment customer district differs from home",
                    ));
                }
                if !(1..=CUSTOMERS_PER_DISTRICT).contains(&i64::from(evidence.customer_id)) {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "Payment customer_id is outside 1..=3000",
                    ));
                }
                if let CustomerSelector::Id(customer_id) = input.customer() {
                    if i32::from(*customer_id) != evidence.customer_id {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "Payment resolved customer differs from frozen id selector",
                        ));
                    }
                }
                validate_positive_amount("Payment amount", evidence.amount_bits, 5_000.0)?;
                validate_relative_add(
                    "Payment warehouse w_ytd",
                    evidence.warehouse_before_bits,
                    evidence.amount_bits,
                    evidence.warehouse_after_bits,
                )?;
                validate_relative_add(
                    "Payment district d_ytd",
                    evidence.district_before_bits,
                    evidence.amount_bits,
                    evidence.district_after_bits,
                )?;
                validate_relative_subtract(
                    "Payment customer c_balance",
                    evidence.customer_balance_before_bits,
                    evidence.amount_bits,
                    evidence.customer_balance_after_bits,
                )?;
                validate_relative_add(
                    "Payment customer c_ytd_payment",
                    evidence.customer_ytd_before_bits,
                    evidence.amount_bits,
                    evidence.customer_ytd_after_bits,
                )?;
                validate_customer_version(
                    evidence.customer_version_before,
                    evidence.customer_version_after,
                    CustomerCounter::Payment,
                )?;

                delta.class_totals.payment_commits = 1;
                delta.payment_history_amount = Some(evidence.amount_bits);
            }
            (
                TransactionParameters::OrderStatus(_),
                RankedTransactionOutcome::Committed(RankedCommit::OrderStatus),
            ) => {
                delta.class_totals.order_status_commits = 1;
            }
            (
                TransactionParameters::Delivery(input),
                RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)),
            ) => {
                if !(MIN_CARRIER_ID..=MAX_CARRIER_ID).contains(&input.carrier_id()) {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "Delivery carrier_id is outside 1..=10",
                    ));
                }
                if orders.len() > MAX_DELIVERY_ORDERS {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "Delivery processed more than one order per district",
                    ));
                }
                let mut seen_districts = [false; MAX_DELIVERY_ORDERS];
                let mut delivered_lines = 0_u64;
                for (index, order) in orders.iter().enumerate() {
                    if order.warehouse_id != route.home_warehouse {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "Delivery outcome warehouse differs from frozen ticket",
                        ));
                    }
                    let partition = partition_index(
                        i32::from(order.warehouse_id),
                        i32::from(order.district_id),
                    )?;
                    let district_slot = usize::from(order.district_id - 1);
                    if seen_districts[district_slot] {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "Delivery processed a district more than once",
                        ));
                    }
                    seen_districts[district_slot] = true;
                    if order.order_id <= 0 {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "Delivery order_id must be positive",
                        ));
                    }
                    if !(1..=CUSTOMERS_PER_DISTRICT).contains(&i64::from(order.customer_id)) {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "Delivery customer_id is outside 1..=3000",
                        ));
                    }
                    if !(MIN_ORDER_LINES..=MAX_ORDER_LINES).contains(&order.line_count) {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "Delivery line count must be 5..=15",
                        ));
                    }
                    if order.line_amount_bits.len() != usize::from(order.line_count) {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "Delivery amount-bit count differs from line_count",
                        ));
                    }
                    for amount_bits in &order.line_amount_bits {
                        validate_positive_amount("Delivery line amount", *amount_bits, 10_000.0)?;
                    }
                    let expected_amount = sum_f32_as_f64_once(
                        order.line_amount_bits.iter().copied(),
                    )
                    .map_err(|source| BoundedStatsError::Float {
                        field: "Delivery line amount sum",
                        source,
                    })?;
                    if expected_amount != order.amount_bits {
                        return Err(BoundedStatsError::InvalidEvidence(
                            "Delivery customer amount differs from exact order-line sum",
                        ));
                    }
                    validate_positive_amount(
                        "Delivery customer amount",
                        order.amount_bits,
                        150_000.0,
                    )?;
                    validate_relative_add(
                        "Delivery customer c_balance",
                        order.customer_balance_before_bits,
                        order.amount_bits,
                        order.customer_balance_after_bits,
                    )?;
                    validate_customer_version(
                        order.customer_version_before,
                        order.customer_version_after,
                        CustomerCounter::Delivery,
                    )?;

                    let line_count = u64::from(order.line_count);
                    delivered_lines =
                        checked_add(delivered_lines, line_count, "Delivery line count")?;
                    delta.partition_updates[index] = PartitionIncrement {
                        index: partition,
                        delta: PartitionTotals {
                            delivered_orders: 1,
                            delivered_order_lines: line_count,
                            ..PartitionTotals::default()
                        },
                    };
                    delta.delivery_customer_amounts[index] = order.amount_bits;
                }
                delta.class_totals = ClassTotals {
                    delivery_commits: 1,
                    delivered_orders: orders.len() as u64,
                    delivered_order_lines: delivered_lines,
                    ..ClassTotals::default()
                };
                delta.partition_count = orders.len();
                delta.delivery_amount_count = orders.len();
            }
            (
                TransactionParameters::StockLevel(_),
                RankedTransactionOutcome::Committed(RankedCommit::StockLevel { low_stock_count }),
            ) => {
                if !(0..=300).contains(low_stock_count) {
                    return Err(BoundedStatsError::InvalidEvidence(
                        "StockLevel low_stock_count must be in 0..=300",
                    ));
                }
                delta.class_totals.stock_level_commits = 1;
            }
            _ => {
                return Err(BoundedStatsError::InvalidEvidence(
                    "terminal outcome kind does not match its frozen ticket",
                ));
            }
        }
        Ok(delta)
    }
}

#[derive(Clone, Copy)]
enum CustomerCounter {
    Payment,
    Delivery,
}

fn validate_customer_version(
    before: CustomerVersion,
    after: CustomerVersion,
    counter: CustomerCounter,
) -> Result<(), BoundedStatsError> {
    if before.payment_count < 0
        || before.delivery_count < 0
        || after.payment_count < 0
        || after.delivery_count < 0
    {
        return Err(BoundedStatsError::InvalidEvidence(
            "customer transaction count must be non-negative",
        ));
    }
    match counter {
        CustomerCounter::Payment => {
            if before.delivery_count != after.delivery_count {
                return Err(BoundedStatsError::Inconsistent(
                    "Payment changed c_delivery_cnt",
                ));
            }
            if before.payment_count.checked_add(1) != Some(after.payment_count) {
                return Err(BoundedStatsError::Inconsistent(
                    "Payment c_payment_cnt is not exactly before + 1",
                ));
            }
        }
        CustomerCounter::Delivery => {
            if before.payment_count != after.payment_count {
                return Err(BoundedStatsError::Inconsistent(
                    "Delivery changed c_payment_cnt",
                ));
            }
            if before.delivery_count.checked_add(1) != Some(after.delivery_count) {
                return Err(BoundedStatsError::Inconsistent(
                    "Delivery c_delivery_cnt is not exactly before + 1",
                ));
            }
        }
    }
    Ok(())
}

fn validate_positive_amount(
    field: &'static str,
    bits: u32,
    maximum: f32,
) -> Result<f32, BoundedStatsError> {
    let value = f32::from_bits(bits);
    if !value.is_finite() || value <= 0.0 || value > maximum {
        Err(BoundedStatsError::InvalidFloatBits { field, bits })
    } else {
        Ok(value)
    }
}

fn validate_finite(field: &'static str, bits: u32) -> Result<f32, BoundedStatsError> {
    let value = f32::from_bits(bits);
    if value.is_finite() {
        Ok(value)
    } else {
        Err(BoundedStatsError::InvalidFloatBits { field, bits })
    }
}

fn validate_relative_add(
    field: &'static str,
    before_bits: u32,
    delta_bits: u32,
    after_bits: u32,
) -> Result<(), BoundedStatsError> {
    let before = validate_finite(field, before_bits)?;
    let delta = validate_finite(field, delta_bits)?;
    validate_finite(field, after_bits)?;
    if (before + delta).to_bits() != after_bits {
        return Err(BoundedStatsError::Inconsistent(
            "relative FLOAT32 addition is not exact RNE",
        ));
    }
    Ok(())
}

fn validate_relative_subtract(
    field: &'static str,
    before_bits: u32,
    delta_bits: u32,
    after_bits: u32,
) -> Result<(), BoundedStatsError> {
    let before = validate_finite(field, before_bits)?;
    let delta = validate_finite(field, delta_bits)?;
    validate_finite(field, after_bits)?;
    if (before - delta).to_bits() != after_bits {
        return Err(BoundedStatsError::Inconsistent(
            "relative FLOAT32 subtraction is not exact RNE",
        ));
    }
    Ok(())
}

fn class_index(class: LedgerClass, stage: u64) -> Result<usize, BoundedStatsError> {
    let index = class_index_without_stage(class)?;
    let valid = match class {
        LedgerClass::Warmup | LedgerClass::WarmupTail => stage == 0,
        LedgerClass::RankedWindow(window) | LedgerClass::RankedTail(window) => {
            window < 3 && stage == u64::from(window) + 1
        }
        LedgerClass::NonRanked => stage > 3,
    };
    if valid {
        Ok(index)
    } else {
        Err(BoundedStatsError::InvalidEvidence(
            "ledger class does not match the frozen ticket stage",
        ))
    }
}

fn class_index_without_stage(class: LedgerClass) -> Result<usize, BoundedStatsError> {
    match class {
        LedgerClass::Warmup => Ok(0),
        LedgerClass::RankedWindow(0) => Ok(1),
        LedgerClass::RankedWindow(1) => Ok(2),
        LedgerClass::RankedWindow(2) => Ok(3),
        LedgerClass::WarmupTail => Ok(4),
        LedgerClass::RankedTail(0) => Ok(5),
        LedgerClass::RankedTail(1) => Ok(6),
        LedgerClass::RankedTail(2) => Ok(7),
        LedgerClass::NonRanked => Ok(8),
        LedgerClass::RankedWindow(_) | LedgerClass::RankedTail(_) => Err(
            BoundedStatsError::InvalidEvidence("ranked ledger class index must be 0..=2"),
        ),
    }
}

fn partition_index(warehouse_id: i32, district_id: i32) -> Result<usize, BoundedStatsError> {
    if !(1..=FINAL_WAREHOUSES).contains(&warehouse_id)
        || !(1..=DISTRICTS_PER_WAREHOUSE).contains(&district_id)
    {
        return Err(BoundedStatsError::InvalidPartitionKey {
            warehouse_id,
            district_id,
        });
    }
    Ok(((warehouse_id - 1) * DISTRICTS_PER_WAREHOUSE + district_id - 1) as usize)
}

fn checked_add(left: u64, right: u64, field: &'static str) -> Result<u64, BoundedStatsError> {
    left.checked_add(right)
        .ok_or(BoundedStatsError::Overflow(field))
}

fn checked_mul(left: u64, right: u64, field: &'static str) -> Result<u64, BoundedStatsError> {
    left.checked_mul(right)
        .ok_or(BoundedStatsError::Overflow(field))
}

fn merge_accumulator_group(
    group: &[NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
    field: &'static str,
) -> Result<NonNegativeF32Accumulator, BoundedStatsError> {
    let mut merged = NonNegativeF32Accumulator::default();
    for accumulator in group {
        merged
            .merge(accumulator)
            .map_err(|source| BoundedStatsError::Float { field, source })?;
    }
    Ok(merged)
}

fn validate_accumulator_replacement(
    group: &[NonNegativeF32Accumulator; LEDGER_CLASS_COUNT],
    replacement_index: usize,
    replacement: &NonNegativeF32Accumulator,
    field: &'static str,
) -> Result<(), BoundedStatsError> {
    let mut merged = NonNegativeF32Accumulator::default();
    for (index, accumulator) in group.iter().enumerate() {
        let selected = if index == replacement_index {
            replacement
        } else {
            accumulator
        };
        merged
            .merge(selected)
            .map_err(|source| BoundedStatsError::Float { field, source })?;
    }
    Ok(())
}

fn validate_line_range(
    orders: u64,
    lines: u64,
    field: &'static str,
) -> Result<(), BoundedStatsError> {
    let minimum = checked_mul(orders, u64::from(MIN_ORDER_LINES), field)?;
    let maximum = checked_mul(orders, u64::from(MAX_ORDER_LINES), field)?;
    if !(minimum..=maximum).contains(&lines) {
        return Err(BoundedStatsError::Inconsistent(
            "order line total is outside 5..=15 per order",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use crate::consistency::large_set_boundary_from_f32;
    use crate::profile::TransactionKind;
    use crate::ranking::ledger::RunLedger;
    use crate::ranking::runner::{DeliveredOrderEvidence, NewOrderEvidence, PaymentEvidence};
    use crate::routing::{ClientSequence, OfficialRouter, StageId, WorkloadSeed};
    use crate::workload::Final2026Workload;

    use super::*;

    fn ticket(
        kind: TransactionKind,
        expected_rollback: Option<bool>,
        stage: StageId,
        client_id: u16,
        seed: u64,
    ) -> TransactionTicket {
        let router = OfficialRouter::new(WorkloadSeed(seed));
        let wheel = router.wheel(stage);
        let workload = Final2026Workload::new(&router, &wheel);
        let mut sequence = ClientSequence::new(client_id).unwrap();
        loop {
            let ticket = workload.select(&mut sequence).unwrap();
            if ticket.kind() != kind {
                continue;
            }
            if let Some(expected) = expected_rollback {
                let TransactionParameters::NewOrder(input) = ticket.parameters() else {
                    unreachable!();
                };
                if input.expected_rollback() != expected {
                    continue;
                }
            }
            return ticket;
        }
    }

    fn new_order(ticket: &TransactionTicket) -> RankedTransactionOutcome {
        let TransactionParameters::NewOrder(input) = ticket.parameters() else {
            panic!("NewOrder ticket");
        };
        let route = ticket.route();
        let line_amount_bits = input
            .lines()
            .iter()
            .enumerate()
            .map(|(index, _)| (index as f32 + 1.0).to_bits())
            .collect();
        RankedTransactionOutcome::Committed(RankedCommit::NewOrder(NewOrderEvidence {
            warehouse_id: route.home_warehouse,
            district_id: route.home_district,
            order_id: 3_001,
            line_count: input.lines().len() as u8,
            remote_line_count: input
                .lines()
                .iter()
                .filter(|line| line.supply_warehouse() != route.home_warehouse)
                .count() as u8,
            stock_ytd_delta: input
                .lines()
                .iter()
                .map(|line| u32::from(line.quantity()))
                .sum(),
            line_amount_bits,
            entry_timestamp: b"2026-07-29 10:20:30".to_vec(),
            recovery_lines: Vec::new(),
        }))
    }

    fn payment(ticket: &TransactionTicket) -> RankedTransactionOutcome {
        let TransactionParameters::Payment(input) = ticket.parameters() else {
            panic!("Payment ticket");
        };
        let customer_id = match input.customer() {
            CustomerSelector::Id(id) => i32::from(*id),
            CustomerSelector::LastName(_) => 42,
        };
        let amount = f32::from_bits(input.amount_bits());
        let warehouse_before = 1_000.0_f32;
        let district_before = 2_000.0_f32;
        let customer_balance_before = -10.0_f32;
        let customer_ytd_before = 10.0_f32;
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
            customer_balance_before_bits: customer_balance_before.to_bits(),
            customer_balance_after_bits: (customer_balance_before - amount).to_bits(),
            customer_ytd_before_bits: customer_ytd_before.to_bits(),
            customer_ytd_after_bits: (customer_ytd_before + amount).to_bits(),
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

    fn delivery(ticket: &TransactionTicket) -> RankedTransactionOutcome {
        let amount = 99.25_f32;
        let customer_balance_before = 10.0_f32;
        RankedTransactionOutcome::Committed(RankedCommit::Delivery(vec![DeliveredOrderEvidence {
            warehouse_id: ticket.route().home_warehouse,
            district_id: 1,
            order_id: 2_101,
            customer_id: 7,
            line_count: 6,
            amount_bits: amount.to_bits(),
            customer_balance_before_bits: customer_balance_before.to_bits(),
            customer_balance_after_bits: (customer_balance_before + amount).to_bits(),
            customer_version_before: CustomerVersion {
                payment_count: 1,
                delivery_count: 0,
            },
            customer_version_after: CustomerVersion {
                payment_count: 1,
                delivery_count: 1,
            },
            delivery_timestamp: b"2026-07-29 10:20:30".to_vec(),
            line_amount_bits: vec![
                10.0_f32.to_bits(),
                20.0_f32.to_bits(),
                30.0_f32.to_bits(),
                19.0_f32.to_bits(),
                10.0_f32.to_bits(),
                10.25_f32.to_bits(),
            ],
        }]))
    }

    fn boundary_signature(accumulator: &NonNegativeF32Accumulator) -> (u64, u32, u32) {
        let boundary = accumulator.boundary().unwrap();
        (
            boundary.term_count,
            boundary.lower_bits,
            boundary.upper_bits,
        )
    }

    #[test]
    fn matches_run_ledger_on_synthetic_nine_class_load() {
        let warmup_new = ticket(
            TransactionKind::NewOrder,
            Some(false),
            StageId::WARMUP,
            0,
            10,
        );
        let ranked_payment = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(0),
            1,
            11,
        );
        let ranked_delivery = ticket(
            TransactionKind::Delivery,
            None,
            StageId::measurement(1),
            2,
            12,
        );
        let ranked_status = ticket(
            TransactionKind::OrderStatus,
            None,
            StageId::measurement(2),
            3,
            13,
        );
        let warmup_rollback = ticket(
            TransactionKind::NewOrder,
            Some(true),
            StageId::WARMUP,
            4,
            14,
        );
        let tail_stock = ticket(
            TransactionKind::StockLevel,
            None,
            StageId::measurement(0),
            5,
            15,
        );
        let tail_payment = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(1),
            6,
            16,
        );
        let tail_new = ticket(
            TransactionKind::NewOrder,
            Some(false),
            StageId::measurement(2),
            7,
            17,
        );
        let nonranked_delivery = ticket(TransactionKind::Delivery, None, StageId::custom(4), 8, 18);

        let cases = vec![
            (
                LedgerClass::Warmup,
                warmup_new.clone(),
                new_order(&warmup_new),
            ),
            (
                LedgerClass::RankedWindow(0),
                ranked_payment.clone(),
                payment(&ranked_payment),
            ),
            (
                LedgerClass::RankedWindow(1),
                ranked_delivery.clone(),
                delivery(&ranked_delivery),
            ),
            (
                LedgerClass::RankedWindow(2),
                ranked_status,
                RankedTransactionOutcome::Committed(RankedCommit::OrderStatus),
            ),
            (
                LedgerClass::WarmupTail,
                warmup_rollback,
                RankedTransactionOutcome::ExpectedRollback,
            ),
            (
                LedgerClass::RankedTail(0),
                tail_stock,
                RankedTransactionOutcome::Committed(RankedCommit::StockLevel {
                    low_stock_count: 7,
                }),
            ),
            (
                LedgerClass::RankedTail(1),
                tail_payment.clone(),
                payment(&tail_payment),
            ),
            (
                LedgerClass::RankedTail(2),
                tail_new.clone(),
                new_order(&tail_new),
            ),
            (
                LedgerClass::NonRanked,
                nonranked_delivery.clone(),
                delivery(&nonranked_delivery),
            ),
        ];

        let mut old = RunLedger::default();
        let mut bounded = BoundedPhysicalStats::default();
        for (class, ticket, outcome) in &cases {
            old.record_as(*class, ticket, outcome).unwrap();
            bounded.offer_terminal(*class, ticket, outcome).unwrap();
        }
        bounded.validate().unwrap();

        let total = bounded.totals().unwrap();
        assert_eq!(total.new_order_commits, old.new_orders() as u64);
        assert_eq!(total.new_orders, old.new_orders() as u64);
        assert_eq!(total.new_order_lines, old.new_order_lines() as u64);
        assert_eq!(
            total.remote_new_order_lines,
            old.remote_new_order_lines() as u64
        );
        assert_eq!(total.stock_quantity_delta, old.stock_ytd_delta() as u64);
        assert_eq!(total.payment_commits, old.payments() as u64);
        assert_eq!(total.delivery_commits, old.delivery_commits() as u64);
        assert_eq!(total.delivered_orders, old.delivered_orders() as u64);
        assert_eq!(
            total.delivered_order_lines,
            old.delivered_order_lines() as u64
        );
        assert_eq!(
            total.order_status_commits,
            old.order_status_commits() as u64
        );
        assert_eq!(total.stock_level_commits, old.stock_level_commits() as u64);
        assert_eq!(total.expected_rollbacks, old.expected_rollbacks() as u64);

        for ((warehouse_id, district_id), actual) in bounded.partition_totals_iter() {
            let expected = old.partition_delta(warehouse_id, district_id).unwrap();
            assert_eq!(actual.new_orders, expected.new_orders as u64);
            assert_eq!(actual.new_order_lines, expected.new_order_lines as u64);
            assert_eq!(actual.delivered_orders, expected.delivered_orders as u64);
            assert_eq!(
                actual.delivered_order_lines,
                expected.delivered_order_lines as u64
            );
        }

        let old_new =
            large_set_boundary_from_f32(old.new_order_line_amount_bits().iter().copied()).unwrap();
        assert_eq!(
            boundary_signature(&bounded.new_order_line_amounts().unwrap()),
            (old_new.term_count, old_new.lower_bits, old_new.upper_bits)
        );
        let old_payment =
            large_set_boundary_from_f32(old.payment_amount_bits().iter().copied()).unwrap();
        assert_eq!(
            boundary_signature(&bounded.payment_history_amounts().unwrap()),
            (
                old_payment.term_count,
                old_payment.lower_bits,
                old_payment.upper_bits
            )
        );
        let old_delivery =
            large_set_boundary_from_f32(old.delivery_customer_amount_bits().iter().copied())
                .unwrap();
        assert_eq!(
            boundary_signature(&bounded.delivery_customer_amounts().unwrap()),
            (
                old_delivery.term_count,
                old_delivery.lower_bits,
                old_delivery.upper_bits
            )
        );

        for class in [
            LedgerClass::Warmup,
            LedgerClass::RankedWindow(0),
            LedgerClass::RankedWindow(1),
            LedgerClass::RankedWindow(2),
            LedgerClass::WarmupTail,
            LedgerClass::RankedTail(0),
            LedgerClass::RankedTail(1),
            LedgerClass::RankedTail(2),
            LedgerClass::NonRanked,
        ] {
            let totals = bounded.class_totals(class).unwrap();
            let terminal_count = totals.new_order_commits
                + totals.payment_commits
                + totals.order_status_commits
                + totals.delivery_commits
                + totals.stock_level_commits
                + totals.expected_rollbacks;
            assert_eq!(terminal_count, 1);
        }
    }

    #[test]
    fn merge_is_canonical_across_worker_permutations() {
        let new_ticket = ticket(
            TransactionKind::NewOrder,
            Some(false),
            StageId::measurement(0),
            0,
            21,
        );
        let payment_ticket = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(1),
            1,
            22,
        );
        let delivery_ticket = ticket(
            TransactionKind::Delivery,
            None,
            StageId::measurement(2),
            2,
            23,
        );

        let mut workers = [
            BoundedPhysicalStats::default(),
            BoundedPhysicalStats::default(),
            BoundedPhysicalStats::default(),
        ];
        workers[0]
            .offer_terminal(
                LedgerClass::RankedWindow(0),
                &new_ticket,
                &new_order(&new_ticket),
            )
            .unwrap();
        workers[1]
            .offer_terminal(
                LedgerClass::RankedWindow(1),
                &payment_ticket,
                &payment(&payment_ticket),
            )
            .unwrap();
        workers[2]
            .offer_terminal(
                LedgerClass::RankedWindow(2),
                &delivery_ticket,
                &delivery(&delivery_ticket),
            )
            .unwrap();

        let permutations = [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];
        let mut expected = None;
        for permutation in permutations {
            let merged =
                BoundedPhysicalStats::merge_all(permutation.map(|index| workers[index].clone()))
                    .unwrap();
            merged.validate().unwrap();
            if let Some(expected) = &expected {
                assert_eq!(&merged, expected);
            } else {
                expected = Some(merged);
            }
        }
    }

    #[test]
    fn validation_and_overflow_failures_are_atomic() {
        let payment_ticket = ticket(TransactionKind::Payment, None, StageId::WARMUP, 0, 31);
        let mut invalid = payment(&payment_ticket);
        let RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)) = &mut invalid
        else {
            unreachable!();
        };
        evidence.warehouse_after_bits ^= 1;

        let mut stats = BoundedPhysicalStats::default();
        let before = stats.clone();
        assert!(stats
            .offer_terminal(
                LedgerClass::RankedWindow(0),
                &payment_ticket,
                &payment(&payment_ticket),
            )
            .is_err());
        assert_eq!(stats, before);
        assert!(stats
            .offer_terminal(LedgerClass::Warmup, &payment_ticket, &invalid,)
            .is_err());
        assert_eq!(stats, before);

        stats.classes[0].payment_commits = u64::MAX;
        let before = stats.clone();
        assert!(matches!(
            stats.offer_terminal(
                LedgerClass::Warmup,
                &payment_ticket,
                &payment(&payment_ticket),
            ),
            Err(BoundedStatsError::Overflow("payment_commits"))
        ));
        assert_eq!(stats, before);

        let mut saturated = BoundedPhysicalStats::default();
        saturated.classes[0].payment_commits = 1_u64 << 53;
        saturated.payment_history_amounts[0]
            .add_repeated_bits(1.0_f32.to_bits(), 1_u64 << 53)
            .unwrap();
        saturated.validate().unwrap();
        let before = saturated.clone();
        assert!(matches!(
            saturated.offer_terminal(
                LedgerClass::Warmup,
                &payment_ticket,
                &payment(&payment_ticket),
            ),
            Err(BoundedStatsError::Float {
                field: "Payment/history amount",
                ..
            })
        ));
        assert_eq!(saturated, before);

        let mut one = BoundedPhysicalStats::default();
        one.offer_terminal(
            LedgerClass::Warmup,
            &payment_ticket,
            &payment(&payment_ticket),
        )
        .unwrap();
        let before = saturated.clone();
        assert!(saturated.merge(&one).is_err());
        assert_eq!(saturated, before);

        let delivery_ticket = ticket(TransactionKind::Delivery, None, StageId::WARMUP, 1, 32);
        let mut empty_delivery = BoundedPhysicalStats::default();
        empty_delivery
            .offer_terminal(
                LedgerClass::Warmup,
                &delivery_ticket,
                &RankedTransactionOutcome::Committed(RankedCommit::Delivery(Vec::new())),
            )
            .unwrap();
        empty_delivery.validate().unwrap();
        let totals = empty_delivery.class_totals(LedgerClass::Warmup).unwrap();
        assert_eq!(totals.delivery_commits, 1);
        assert_eq!(totals.delivered_orders, 0);

        // A worker-local shard may observe deliveries of orders created by a
        // different worker. Queue availability is therefore a final global
        // invariant, not a structural per-shard validation rule.
        PartitionTotals {
            delivered_orders: 901,
            delivered_order_lines: 901 * u64::from(MIN_ORDER_LINES),
            ..PartitionTotals::default()
        }
        .validate()
        .unwrap();
    }

    #[test]
    fn delivery_validates_exact_line_sum_without_a_false_amount_ceiling() {
        let delivery_ticket = ticket(TransactionKind::Delivery, None, StageId::WARMUP, 1, 37);
        let mut high_amount = delivery(&delivery_ticket);
        let RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)) = &mut high_amount
        else {
            unreachable!();
        };
        let order = &mut orders[0];
        order.line_amount_bits = vec![9_999.99_f32.to_bits(); usize::from(order.line_count)];
        order.amount_bits = sum_f32_as_f64_once(order.line_amount_bits.iter().copied()).unwrap();
        assert!(f32::from_bits(order.amount_bits) > 15_000.0);
        order.customer_balance_after_bits = (f32::from_bits(order.customer_balance_before_bits)
            + f32::from_bits(order.amount_bits))
        .to_bits();

        let mut stats = BoundedPhysicalStats::default();
        stats
            .offer_terminal(LedgerClass::Warmup, &delivery_ticket, &high_amount)
            .unwrap();

        let before = stats.clone();
        let mut missing_lines = high_amount.clone();
        let RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)) =
            &mut missing_lines
        else {
            unreachable!();
        };
        orders[0].line_amount_bits.clear();
        assert!(matches!(
            stats.offer_terminal(LedgerClass::Warmup, &delivery_ticket, &missing_lines),
            Err(BoundedStatsError::InvalidEvidence(
                "Delivery amount-bit count differs from line_count"
            ))
        ));
        assert_eq!(stats, before);

        let mut wrong_sum = high_amount;
        let RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)) = &mut wrong_sum
        else {
            unreachable!();
        };
        orders[0].line_amount_bits[0] = 1.0_f32.to_bits();
        assert!(matches!(
            stats.offer_terminal(LedgerClass::Warmup, &delivery_ticket, &wrong_sum),
            Err(BoundedStatsError::InvalidEvidence(
                "Delivery customer amount differs from exact order-line sum"
            ))
        ));
        assert_eq!(stats, before);
    }

    #[test]
    fn million_hot_terminals_keep_the_storage_shape_constant() {
        let payment_ticket = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(0),
            0,
            41,
        );
        let outcome = payment(&payment_ticket);
        let mut stats = BoundedPhysicalStats::default();
        stats
            .offer_terminal(LedgerClass::RankedWindow(0), &payment_ticket, &outcome)
            .unwrap();

        let fixed_bytes = size_of::<BoundedPhysicalStats>();
        let expected_fixed_bytes = size_of::<[ClassTotals; LEDGER_CLASS_COUNT]>()
            + size_of::<[PartitionTotals; PHYSICAL_PARTITION_COUNT]>()
            + 3 * size_of::<[NonNegativeF32Accumulator; LEDGER_CLASS_COUNT]>();
        assert_eq!(fixed_bytes, expected_fixed_bytes);
        assert!(fixed_bytes < 20 * 1024);

        let initial_words = [
            stats
                .payment_history_amounts_for(LedgerClass::RankedWindow(0))
                .unwrap()
                .to_words()
                .1
                .len(),
            stats
                .new_order_line_amounts_for(LedgerClass::RankedWindow(0))
                .unwrap()
                .to_words()
                .1
                .len(),
            stats
                .delivery_customer_amounts_for(LedgerClass::RankedWindow(0))
                .unwrap()
                .to_words()
                .1
                .len(),
        ];
        // Reuse one already-validated shape only to stress bounded storage;
        // production callers must still honor offer_terminal's exactly-once
        // terminal identity contract.
        for _ in 1..1_000_000 {
            stats
                .offer_terminal(LedgerClass::RankedWindow(0), &payment_ticket, &outcome)
                .unwrap();
        }
        stats.validate().unwrap();

        assert_eq!(
            stats
                .class_totals(LedgerClass::RankedWindow(0))
                .unwrap()
                .payment_commits,
            1_000_000
        );
        assert_eq!(
            stats
                .payment_history_amounts_for(LedgerClass::RankedWindow(0),)
                .unwrap()
                .term_count(),
            1_000_000
        );
        assert_eq!(
            [
                stats
                    .payment_history_amounts_for(LedgerClass::RankedWindow(0),)
                    .unwrap()
                    .to_words()
                    .1
                    .len(),
                stats
                    .new_order_line_amounts_for(LedgerClass::RankedWindow(0),)
                    .unwrap()
                    .to_words()
                    .1
                    .len(),
                stats
                    .delivery_customer_amounts_for(LedgerClass::RankedWindow(0),)
                    .unwrap()
                    .to_words()
                    .1
                    .len(),
            ],
            initial_words
        );
    }
}
