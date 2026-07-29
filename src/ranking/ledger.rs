//! Durable, lossless evidence collected from ranked transaction commits.
//!
//! The ledger deliberately stores FLOAT32 values as their raw binary32 bits.
//! It never formats a float as decimal while handing evidence from the ranked
//! phase to the online and recovery consistency phases.

use std::fmt::Write;

use thiserror::Error;

use crate::consistency::{
    CommittedLedger, PartitionKey, CUSTOMERS_PER_DISTRICT, DISTRICTS_PER_WAREHOUSE,
    FINAL_WAREHOUSES, NEW_ORDERS_PER_DISTRICT,
};
use crate::profile::{ITEM_COUNT, OFFICIAL_CLIENTS};
use crate::routing::StageId;
use crate::workload::{
    CustomerSelector, TransactionParameters, TransactionTicket, MAX_CARRIER_ID, MAX_ITEM_QUANTITY,
    MAX_ORDER_LINES, MIN_CARRIER_ID, MIN_ITEM_QUANTITY, MIN_ORDER_LINES,
};

use super::runner::{CustomerVersion, RankedCommit, RankedTransactionOutcome};

const FORMAT_HEADER: &str = "RMDB_TPCC_RUN_LEDGER_V2";
const PARTITION_COUNT: usize = (FINAL_WAREHOUSES as usize) * (DISTRICTS_PER_WAREHOUSE as usize);

const GLOBAL_FIELDS: [&str; 15] = [
    "new_orders",
    "new_order_lines",
    "remote_new_order_lines",
    "stock_ytd_delta",
    "payments",
    "delivery_commits",
    "delivered_orders",
    "delivered_order_lines",
    "order_status_commits",
    "stock_level_commits",
    "expected_rollbacks",
    "new_order_line_amount_bits",
    "payment_amount_bits",
    "delivery_customer_amount_bits",
    "event_count",
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PartitionDelta {
    pub new_orders: i64,
    pub new_order_lines: i64,
    pub delivered_orders: i64,
    pub delivered_order_lines: i64,
}

impl PartitionDelta {
    fn checked_add(self, other: Self) -> Result<Self, LedgerError> {
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

    fn validate(self) -> Result<(), LedgerError> {
        for (name, value) in [
            ("partition new_orders", self.new_orders),
            ("partition new_order_lines", self.new_order_lines),
            ("partition delivered_orders", self.delivered_orders),
            (
                "partition delivered_order_lines",
                self.delivered_order_lines,
            ),
        ] {
            if value < 0 {
                return Err(LedgerError::Inconsistent(format!(
                    "{name} must be non-negative"
                )));
            }
        }

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

/// Accounting class of a confirmed terminal result.
///
/// Every committed class mutates the database and therefore contributes to
/// physical consistency expectations. Only `RankedWindow` contributes to the
/// ranked measurement ledger.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerClass {
    Warmup,
    RankedWindow(u8),
    WarmupTail,
    RankedTail(u8),
    NonRanked,
}

impl LedgerClass {
    pub const fn is_ranked(self) -> bool {
        matches!(self, Self::RankedWindow(_))
    }

    fn normal_for_stage(stage: StageId) -> Self {
        match stage.value() {
            0 => Self::Warmup,
            value @ 1..=3 => Self::RankedWindow((value - 1) as u8),
            _ => Self::NonRanked,
        }
    }

    fn tail_for_stage(stage: StageId) -> Self {
        match stage.value() {
            0 => Self::WarmupTail,
            value @ 1..=3 => Self::RankedTail((value - 1) as u8),
            _ => Self::NonRanked,
        }
    }

    fn validate_stage(self, stage: u64) -> Result<(), LedgerError> {
        let valid = match self {
            Self::Warmup | Self::WarmupTail => stage == StageId::WARMUP.value(),
            Self::RankedWindow(index) | Self::RankedTail(index) => {
                index < 3 && stage == StageId::measurement(index).value()
            }
            Self::NonRanked => stage > StageId::measurement(2).value(),
        };
        if valid {
            Ok(())
        } else {
            Err(LedgerError::InvalidEvidence(
                "ledger class does not match the frozen ticket stage",
            ))
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LedgerEventMeta {
    pub class: LedgerClass,
    pub stage: u64,
    pub client_id: u16,
    pub txn_no: u64,
    pub parameter_fingerprint: u64,
}

impl LedgerEventMeta {
    fn from_ticket(class: LedgerClass, ticket: &TransactionTicket) -> Result<Self, LedgerError> {
        let route = ticket.route();
        class.validate_stage(route.stage.value())?;
        if route.client_id >= OFFICIAL_CLIENTS {
            return Err(LedgerError::InvalidEvidence(
                "ticket client_id is outside the final client domain",
            ));
        }
        Ok(Self {
            class,
            stage: route.stage.value(),
            client_id: route.client_id,
            txn_no: route.txn_no,
            parameter_fingerprint: ticket.parameter_fingerprint(),
        })
    }

    fn validate(self) -> Result<(), LedgerError> {
        self.class.validate_stage(self.stage)?;
        if self.client_id >= OFFICIAL_CLIENTS {
            return Err(LedgerError::InvalidEvidence(
                "ledger event client_id is outside the final client domain",
            ));
        }
        Ok(())
    }

    fn identity(self) -> (u64, u16, u64) {
        (self.stage, self.client_id, self.txn_no)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NewOrderLineDelta {
    pub number: u8,
    pub item_id: u32,
    pub supply_warehouse: u16,
    pub quantity: u8,
    pub amount_bits: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewOrderDelta {
    pub meta: LedgerEventMeta,
    pub warehouse_id: u16,
    pub district_id: u8,
    pub order_id: i32,
    pub customer_id: u16,
    pub lines: Vec<NewOrderLineDelta>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PaymentDelta {
    pub meta: LedgerEventMeta,
    pub warehouse_id: u16,
    pub district_id: u8,
    pub customer_warehouse_id: u16,
    pub customer_district_id: u8,
    pub customer_id: i32,
    pub amount_bits: u32,
    pub warehouse_before_bits: u32,
    pub warehouse_after_bits: u32,
    pub district_before_bits: u32,
    pub district_after_bits: u32,
    pub customer_balance_before_bits: u32,
    pub customer_balance_after_bits: u32,
    pub customer_ytd_before_bits: u32,
    pub customer_ytd_after_bits: u32,
    pub customer_payment_count_before: i32,
    pub customer_payment_count_after: i32,
    pub customer_delivery_count_before: i32,
    pub customer_delivery_count_after: i32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeliveredOrderDelta {
    pub district_id: u8,
    pub order_id: i32,
    pub customer_id: i32,
    pub line_count: u8,
    pub customer_amount_bits: u32,
    pub customer_balance_before_bits: u32,
    pub customer_balance_after_bits: u32,
    pub customer_payment_count_before: i32,
    pub customer_payment_count_after: i32,
    pub customer_delivery_count_before: i32,
    pub customer_delivery_count_after: i32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryDelta {
    pub meta: LedgerEventMeta,
    pub warehouse_id: u16,
    pub carrier_id: u8,
    pub orders: Vec<DeliveredOrderDelta>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LedgerEvent {
    NewOrder(NewOrderDelta),
    Payment(PaymentDelta),
    Delivery(DeliveryDelta),
    OrderStatus {
        meta: LedgerEventMeta,
    },
    StockLevel {
        meta: LedgerEventMeta,
        low_stock_count: i32,
    },
    ExpectedRollback {
        meta: LedgerEventMeta,
        warehouse_id: u16,
        district_id: u8,
    },
}

impl LedgerEvent {
    pub const fn meta(&self) -> LedgerEventMeta {
        match self {
            Self::NewOrder(delta) => delta.meta,
            Self::Payment(delta) => delta.meta,
            Self::Delivery(delta) => delta.meta,
            Self::OrderStatus { meta }
            | Self::StockLevel { meta, .. }
            | Self::ExpectedRollback { meta, .. } => *meta,
        }
    }

    fn validate(&self) -> Result<(), LedgerError> {
        self.meta().validate()?;
        match self {
            Self::NewOrder(delta) => validate_new_order_delta(delta),
            Self::Payment(delta) => validate_payment_delta(delta),
            Self::Delivery(delta) => validate_delivery_delta(delta),
            Self::OrderStatus { .. } => Ok(()),
            Self::StockLevel {
                low_stock_count, ..
            } => {
                if !(0..=300).contains(low_stock_count) {
                    Err(LedgerError::InvalidEvidence(
                        "StockLevel low_stock_count must be in 0..=300",
                    ))
                } else {
                    Ok(())
                }
            }
            Self::ExpectedRollback {
                warehouse_id,
                district_id,
                ..
            } => {
                partition_index(i32::from(*warehouse_id), i32::from(*district_id))?;
                Ok(())
            }
        }
    }

    fn to_outcome(&self) -> RankedTransactionOutcome {
        match self {
            Self::NewOrder(delta) => {
                let remote_line_count = delta
                    .lines
                    .iter()
                    .filter(|line| line.supply_warehouse != delta.warehouse_id)
                    .count() as u8;
                let stock_ytd_delta = delta
                    .lines
                    .iter()
                    .map(|line| u32::from(line.quantity))
                    .sum();
                RankedTransactionOutcome::Committed(RankedCommit::NewOrder(
                    super::runner::NewOrderEvidence {
                        warehouse_id: delta.warehouse_id,
                        district_id: delta.district_id,
                        order_id: delta.order_id,
                        line_count: delta.lines.len() as u8,
                        remote_line_count,
                        stock_ytd_delta,
                        line_amount_bits: delta.lines.iter().map(|line| line.amount_bits).collect(),
                        entry_timestamp: Vec::new(),
                        recovery_lines: Vec::new(),
                    },
                ))
            }
            Self::Payment(delta) => RankedTransactionOutcome::Committed(RankedCommit::Payment(
                super::runner::PaymentEvidence {
                    warehouse_id: delta.warehouse_id,
                    district_id: delta.district_id,
                    customer_warehouse_id: delta.customer_warehouse_id,
                    customer_district_id: delta.customer_district_id,
                    customer_id: delta.customer_id,
                    amount_bits: delta.amount_bits,
                    warehouse_before_bits: delta.warehouse_before_bits,
                    warehouse_after_bits: delta.warehouse_after_bits,
                    district_before_bits: delta.district_before_bits,
                    district_after_bits: delta.district_after_bits,
                    customer_balance_before_bits: delta.customer_balance_before_bits,
                    customer_balance_after_bits: delta.customer_balance_after_bits,
                    customer_ytd_before_bits: delta.customer_ytd_before_bits,
                    customer_ytd_after_bits: delta.customer_ytd_after_bits,
                    customer_version_before: CustomerVersion {
                        payment_count: delta.customer_payment_count_before,
                        delivery_count: delta.customer_delivery_count_before,
                    },
                    customer_version_after: CustomerVersion {
                        payment_count: delta.customer_payment_count_after,
                        delivery_count: delta.customer_delivery_count_after,
                    },
                    history_timestamp: Vec::new(),
                    history_data: Vec::new(),
                    customer_is_bad_credit: false,
                    customer_data_before: Vec::new(),
                    customer_data_after: Vec::new(),
                },
            )),
            Self::Delivery(delta) => RankedTransactionOutcome::Committed(RankedCommit::Delivery(
                delta
                    .orders
                    .iter()
                    .map(|order| super::runner::DeliveredOrderEvidence {
                        warehouse_id: delta.warehouse_id,
                        district_id: order.district_id,
                        order_id: order.order_id,
                        customer_id: order.customer_id,
                        line_count: order.line_count,
                        amount_bits: order.customer_amount_bits,
                        customer_balance_before_bits: order.customer_balance_before_bits,
                        customer_balance_after_bits: order.customer_balance_after_bits,
                        customer_version_before: CustomerVersion {
                            payment_count: order.customer_payment_count_before,
                            delivery_count: order.customer_delivery_count_before,
                        },
                        customer_version_after: CustomerVersion {
                            payment_count: order.customer_payment_count_after,
                            delivery_count: order.customer_delivery_count_after,
                        },
                        delivery_timestamp: Vec::new(),
                        line_amount_bits: vec![0.0_f32.to_bits(); order.line_count as usize],
                    })
                    .collect(),
            )),
            Self::OrderStatus { .. } => {
                RankedTransactionOutcome::Committed(RankedCommit::OrderStatus)
            }
            Self::StockLevel {
                low_stock_count, ..
            } => RankedTransactionOutcome::Committed(RankedCommit::StockLevel {
                low_stock_count: *low_stock_count,
            }),
            Self::ExpectedRollback { .. } => RankedTransactionOutcome::ExpectedRollback,
        }
    }
}

fn event_from_ticket(
    class: LedgerClass,
    ticket: &TransactionTicket,
    outcome: &RankedTransactionOutcome,
) -> Result<LedgerEvent, LedgerError> {
    let meta = LedgerEventMeta::from_ticket(class, ticket)?;
    let route = ticket.route();
    let event = match (ticket.parameters(), outcome) {
        (
            TransactionParameters::NewOrder(input),
            RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence)),
        ) => {
            if input.expected_rollback() {
                return Err(LedgerError::InvalidEvidence(
                    "expected-rollback NewOrder cannot be recorded as committed",
                ));
            }
            if evidence.warehouse_id != route.home_warehouse
                || evidence.district_id != route.home_district
                || evidence.line_count as usize != input.lines().len()
                || evidence.line_amount_bits.len() != input.lines().len()
            {
                return Err(LedgerError::InvalidEvidence(
                    "NewOrder outcome does not match its frozen ticket",
                ));
            }
            let mut remote_line_count = 0_u8;
            let mut stock_ytd_delta = 0_u32;
            let mut lines = Vec::with_capacity(input.lines().len());
            for (line, amount_bits) in input.lines().iter().zip(&evidence.line_amount_bits) {
                if line.is_invalid_item() {
                    return Err(LedgerError::InvalidEvidence(
                        "committed NewOrder ticket contains the invalid item",
                    ));
                }
                if line.supply_warehouse() != route.home_warehouse {
                    remote_line_count = remote_line_count
                        .checked_add(1)
                        .ok_or(LedgerError::Overflow("NewOrder remote line count"))?;
                }
                stock_ytd_delta = stock_ytd_delta
                    .checked_add(u32::from(line.quantity()))
                    .ok_or(LedgerError::Overflow("NewOrder stock_ytd_delta"))?;
                lines.push(NewOrderLineDelta {
                    number: line.number(),
                    item_id: line.item_id(),
                    supply_warehouse: line.supply_warehouse(),
                    quantity: line.quantity(),
                    amount_bits: *amount_bits,
                });
            }
            if remote_line_count != evidence.remote_line_count
                || stock_ytd_delta != evidence.stock_ytd_delta
                || input.all_local() != (remote_line_count == 0)
            {
                return Err(LedgerError::InvalidEvidence(
                    "NewOrder derived line deltas differ from outcome evidence",
                ));
            }
            LedgerEvent::NewOrder(NewOrderDelta {
                meta,
                warehouse_id: route.home_warehouse,
                district_id: route.home_district,
                order_id: evidence.order_id,
                customer_id: input.customer_id(),
                lines,
            })
        }
        (TransactionParameters::NewOrder(input), RankedTransactionOutcome::ExpectedRollback) => {
            if !input.expected_rollback()
                || input
                    .lines()
                    .last()
                    .is_none_or(|line| !line.is_invalid_item())
            {
                return Err(LedgerError::InvalidEvidence(
                    "business rollback does not match the frozen invalid-item ticket",
                ));
            }
            LedgerEvent::ExpectedRollback {
                meta,
                warehouse_id: route.home_warehouse,
                district_id: route.home_district,
            }
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
                return Err(LedgerError::InvalidEvidence(
                    "Payment outcome does not match its frozen ticket",
                ));
            }
            if let CustomerSelector::Id(customer_id) = input.customer() {
                if i32::from(*customer_id) != evidence.customer_id {
                    return Err(LedgerError::InvalidEvidence(
                        "Payment resolved customer differs from frozen id selector",
                    ));
                }
            }
            LedgerEvent::Payment(PaymentDelta {
                meta,
                warehouse_id: evidence.warehouse_id,
                district_id: evidence.district_id,
                customer_warehouse_id: evidence.customer_warehouse_id,
                customer_district_id: evidence.customer_district_id,
                customer_id: evidence.customer_id,
                amount_bits: evidence.amount_bits,
                warehouse_before_bits: evidence.warehouse_before_bits,
                warehouse_after_bits: evidence.warehouse_after_bits,
                district_before_bits: evidence.district_before_bits,
                district_after_bits: evidence.district_after_bits,
                customer_balance_before_bits: evidence.customer_balance_before_bits,
                customer_balance_after_bits: evidence.customer_balance_after_bits,
                customer_ytd_before_bits: evidence.customer_ytd_before_bits,
                customer_ytd_after_bits: evidence.customer_ytd_after_bits,
                customer_payment_count_before: evidence.customer_version_before.payment_count,
                customer_payment_count_after: evidence.customer_version_after.payment_count,
                customer_delivery_count_before: evidence.customer_version_before.delivery_count,
                customer_delivery_count_after: evidence.customer_version_after.delivery_count,
            })
        }
        (
            TransactionParameters::OrderStatus(_),
            RankedTransactionOutcome::Committed(RankedCommit::OrderStatus),
        ) => LedgerEvent::OrderStatus { meta },
        (
            TransactionParameters::Delivery(input),
            RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)),
        ) => {
            let mut deltas = Vec::with_capacity(orders.len());
            for order in orders {
                if order.warehouse_id != route.home_warehouse {
                    return Err(LedgerError::InvalidEvidence(
                        "Delivery outcome warehouse differs from frozen ticket",
                    ));
                }
                deltas.push(DeliveredOrderDelta {
                    district_id: order.district_id,
                    order_id: order.order_id,
                    customer_id: order.customer_id,
                    line_count: order.line_count,
                    customer_amount_bits: order.amount_bits,
                    customer_balance_before_bits: order.customer_balance_before_bits,
                    customer_balance_after_bits: order.customer_balance_after_bits,
                    customer_payment_count_before: order.customer_version_before.payment_count,
                    customer_payment_count_after: order.customer_version_after.payment_count,
                    customer_delivery_count_before: order.customer_version_before.delivery_count,
                    customer_delivery_count_after: order.customer_version_after.delivery_count,
                });
            }
            LedgerEvent::Delivery(DeliveryDelta {
                meta,
                warehouse_id: route.home_warehouse,
                carrier_id: input.carrier_id(),
                orders: deltas,
            })
        }
        (
            TransactionParameters::StockLevel(_),
            RankedTransactionOutcome::Committed(RankedCommit::StockLevel { low_stock_count }),
        ) => LedgerEvent::StockLevel {
            meta,
            low_stock_count: *low_stock_count,
        },
        _ => {
            return Err(LedgerError::InvalidEvidence(
                "terminal outcome kind does not match its frozen ticket",
            ));
        }
    };
    event.validate()?;
    Ok(event)
}

fn validate_new_order_delta(delta: &NewOrderDelta) -> Result<(), LedgerError> {
    partition_index(i32::from(delta.warehouse_id), i32::from(delta.district_id))?;
    if delta.order_id <= 0 {
        return Err(LedgerError::InvalidEvidence(
            "NewOrder order_id must be positive",
        ));
    }
    if !(1..=CUSTOMERS_PER_DISTRICT).contains(&i64::from(delta.customer_id)) {
        return Err(LedgerError::InvalidEvidence(
            "NewOrder customer_id is outside 1..=3000",
        ));
    }
    if !(usize::from(MIN_ORDER_LINES)..=usize::from(MAX_ORDER_LINES)).contains(&delta.lines.len()) {
        return Err(LedgerError::InvalidEvidence(
            "NewOrder line count must be 5..=15",
        ));
    }
    for (index, line) in delta.lines.iter().enumerate() {
        if usize::from(line.number) != index + 1 {
            return Err(LedgerError::InvalidEvidence(
                "NewOrder line numbers must be dense from one",
            ));
        }
        if !(1..=ITEM_COUNT).contains(&line.item_id) {
            return Err(LedgerError::InvalidEvidence(
                "committed NewOrder item_id is outside 1..=100000",
            ));
        }
        partition_index(i32::from(line.supply_warehouse), 1)?;
        if !(MIN_ITEM_QUANTITY..=MAX_ITEM_QUANTITY).contains(&line.quantity) {
            return Err(LedgerError::InvalidEvidence(
                "NewOrder quantity is outside 1..=10",
            ));
        }
        validate_positive_amount_bits("NewOrder line amount", line.amount_bits, 1_000.0)?;
    }
    Ok(())
}

fn validate_payment_delta(delta: &PaymentDelta) -> Result<(), LedgerError> {
    partition_index(i32::from(delta.warehouse_id), i32::from(delta.district_id))?;
    partition_index(
        i32::from(delta.customer_warehouse_id),
        i32::from(delta.customer_district_id),
    )?;
    if delta.customer_warehouse_id == delta.warehouse_id
        && delta.customer_district_id != delta.district_id
    {
        return Err(LedgerError::InvalidEvidence(
            "local Payment customer district differs from the home district",
        ));
    }
    if !(1..=CUSTOMERS_PER_DISTRICT).contains(&i64::from(delta.customer_id)) {
        return Err(LedgerError::InvalidEvidence(
            "Payment customer_id is outside 1..=3000",
        ));
    }
    validate_positive_amount_bits("Payment amount", delta.amount_bits, 5_000.0)?;
    validate_relative_add(
        "Payment warehouse w_ytd",
        delta.warehouse_before_bits,
        delta.amount_bits,
        delta.warehouse_after_bits,
    )?;
    validate_relative_add(
        "Payment district d_ytd",
        delta.district_before_bits,
        delta.amount_bits,
        delta.district_after_bits,
    )?;
    validate_relative_subtract(
        "Payment customer c_balance",
        delta.customer_balance_before_bits,
        delta.amount_bits,
        delta.customer_balance_after_bits,
    )?;
    validate_relative_add(
        "Payment customer c_ytd_payment",
        delta.customer_ytd_before_bits,
        delta.amount_bits,
        delta.customer_ytd_after_bits,
    )?;
    validate_increment(
        "Payment customer c_payment_cnt",
        delta.customer_payment_count_before,
        delta.customer_payment_count_after,
    )?;
    validate_unchanged(
        "Payment customer c_delivery_cnt",
        delta.customer_delivery_count_before,
        delta.customer_delivery_count_after,
    )
}

fn validate_delivery_delta(delta: &DeliveryDelta) -> Result<(), LedgerError> {
    partition_index(i32::from(delta.warehouse_id), 1)?;
    if !(MIN_CARRIER_ID..=MAX_CARRIER_ID).contains(&delta.carrier_id) {
        return Err(LedgerError::InvalidEvidence(
            "Delivery carrier_id is outside 1..=10",
        ));
    }
    if delta.orders.len() > DISTRICTS_PER_WAREHOUSE as usize {
        return Err(LedgerError::InvalidEvidence(
            "Delivery processed more than one order per district",
        ));
    }
    let mut districts = std::collections::BTreeSet::new();
    for order in &delta.orders {
        partition_index(i32::from(delta.warehouse_id), i32::from(order.district_id))?;
        if !districts.insert(order.district_id) {
            return Err(LedgerError::InvalidEvidence(
                "Delivery processed a district more than once",
            ));
        }
        if order.order_id <= 0 {
            return Err(LedgerError::InvalidEvidence(
                "Delivery order_id must be positive",
            ));
        }
        if !(1..=CUSTOMERS_PER_DISTRICT).contains(&i64::from(order.customer_id)) {
            return Err(LedgerError::InvalidEvidence(
                "Delivery customer_id is outside 1..=3000",
            ));
        }
        if !(MIN_ORDER_LINES..=MAX_ORDER_LINES).contains(&order.line_count) {
            return Err(LedgerError::InvalidEvidence(
                "Delivery line_count must be 5..=15",
            ));
        }
        validate_positive_amount_bits(
            "Delivery customer amount",
            order.customer_amount_bits,
            15_000.0,
        )?;
        validate_relative_add(
            "Delivery customer c_balance",
            order.customer_balance_before_bits,
            order.customer_amount_bits,
            order.customer_balance_after_bits,
        )?;
        validate_increment(
            "Delivery customer c_delivery_cnt",
            order.customer_delivery_count_before,
            order.customer_delivery_count_after,
        )?;
        validate_unchanged(
            "Delivery customer c_payment_cnt",
            order.customer_payment_count_before,
            order.customer_payment_count_after,
        )?;
    }
    Ok(())
}

fn validate_positive_amount_bits(field: &str, bits: u32, maximum: f32) -> Result<(), LedgerError> {
    let value = f32::from_bits(bits);
    if !value.is_finite() || value <= 0.0 || value > maximum {
        return Err(LedgerError::InvalidFloatBits {
            field: field.to_owned(),
            bits: format!("{bits:08x}"),
        });
    }
    Ok(())
}

fn validate_finite_bits(field: &str, bits: u32) -> Result<f32, LedgerError> {
    let value = f32::from_bits(bits);
    if !value.is_finite() {
        return Err(LedgerError::InvalidFloatBits {
            field: field.to_owned(),
            bits: format!("{bits:08x}"),
        });
    }
    Ok(value)
}

fn validate_relative_add(
    field: &'static str,
    before_bits: u32,
    delta_bits: u32,
    after_bits: u32,
) -> Result<(), LedgerError> {
    let before = validate_finite_bits(field, before_bits)?;
    let delta = validate_finite_bits(field, delta_bits)?;
    validate_finite_bits(field, after_bits)?;
    if (before + delta).to_bits() != after_bits {
        return Err(LedgerError::Inconsistent(format!(
            "{field} is not exact binary32_RNE(before + delta)"
        )));
    }
    Ok(())
}

fn validate_relative_subtract(
    field: &'static str,
    before_bits: u32,
    delta_bits: u32,
    after_bits: u32,
) -> Result<(), LedgerError> {
    let before = validate_finite_bits(field, before_bits)?;
    let delta = validate_finite_bits(field, delta_bits)?;
    validate_finite_bits(field, after_bits)?;
    if (before - delta).to_bits() != after_bits {
        return Err(LedgerError::Inconsistent(format!(
            "{field} is not exact binary32_RNE(before - delta)"
        )));
    }
    Ok(())
}

fn validate_increment(field: &'static str, before: i32, after: i32) -> Result<(), LedgerError> {
    if before < 0 {
        return Err(LedgerError::InvalidEvidence(
            "customer transaction count must be non-negative",
        ));
    }
    let expected = before
        .checked_add(1)
        .ok_or(LedgerError::Overflow("customer transaction count"))?;
    if after != expected {
        return Err(LedgerError::Inconsistent(format!(
            "{field} is not exactly before + 1"
        )));
    }
    Ok(())
}

fn validate_unchanged(field: &'static str, before: i32, after: i32) -> Result<(), LedgerError> {
    if before < 0 || after < 0 {
        return Err(LedgerError::InvalidEvidence(
            "customer transaction count must be non-negative",
        ));
    }
    if before != after {
        return Err(LedgerError::Inconsistent(format!(
            "{field} changed during the other transaction family"
        )));
    }
    Ok(())
}

/// Lossless committed-work evidence for one ranked run.
///
/// Each worker should own a local `RunLedger` and call [`RunLedger::record`]
/// only after a transaction has reached its terminal result. The coordinator
/// then merges worker ledgers in a stable worker-id order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunLedger {
    new_orders: i64,
    new_order_lines: i64,
    remote_new_order_lines: i64,
    stock_ytd_delta: i64,
    payments: i64,
    delivery_commits: i64,
    delivered_orders: i64,
    delivered_order_lines: i64,
    order_status_commits: i64,
    stock_level_commits: i64,
    expected_rollbacks: i64,
    new_order_line_amount_bits: Vec<u32>,
    payment_amount_bits: Vec<u32>,
    delivery_customer_amount_bits: Vec<u32>,
    partitions: Vec<PartitionDelta>,
    events: Vec<LedgerEvent>,
}

impl Default for RunLedger {
    fn default() -> Self {
        Self {
            new_orders: 0,
            new_order_lines: 0,
            remote_new_order_lines: 0,
            stock_ytd_delta: 0,
            payments: 0,
            delivery_commits: 0,
            delivered_orders: 0,
            delivered_order_lines: 0,
            order_status_commits: 0,
            stock_level_commits: 0,
            expected_rollbacks: 0,
            new_order_line_amount_bits: Vec::new(),
            payment_amount_bits: Vec::new(),
            delivery_customer_amount_bits: Vec::new(),
            partitions: vec![PartitionDelta::default(); PARTITION_COUNT],
            events: Vec::new(),
        }
    }
}

impl RunLedger {
    pub fn new_orders(&self) -> i64 {
        self.new_orders
    }

    pub fn new_order_lines(&self) -> i64 {
        self.new_order_lines
    }

    pub fn remote_new_order_lines(&self) -> i64 {
        self.remote_new_order_lines
    }

    /// Exact sum of the integral quantities bound to committed stock YTD
    /// updates. This can be converted to binary32 without a decimal round trip.
    pub fn stock_ytd_delta(&self) -> i64 {
        self.stock_ytd_delta
    }

    pub fn payments(&self) -> i64 {
        self.payments
    }

    pub fn delivery_commits(&self) -> i64 {
        self.delivery_commits
    }

    pub fn delivered_orders(&self) -> i64 {
        self.delivered_orders
    }

    pub fn delivered_order_lines(&self) -> i64 {
        self.delivered_order_lines
    }

    pub fn order_status_commits(&self) -> i64 {
        self.order_status_commits
    }

    pub fn stock_level_commits(&self) -> i64 {
        self.stock_level_commits
    }

    pub fn expected_rollbacks(&self) -> i64 {
        self.expected_rollbacks
    }

    pub fn new_order_line_amount_bits(&self) -> &[u32] {
        &self.new_order_line_amount_bits
    }

    pub fn payment_amount_bits(&self) -> &[u32] {
        &self.payment_amount_bits
    }

    pub fn delivery_customer_amount_bits(&self) -> &[u32] {
        &self.delivery_customer_amount_bits
    }

    pub fn events(&self) -> &[LedgerEvent] {
        &self.events
    }

    pub fn partition_delta(
        &self,
        warehouse_id: i32,
        district_id: i32,
    ) -> Result<PartitionDelta, LedgerError> {
        let index = partition_index(warehouse_id, district_id)?;
        Ok(self.partitions[index])
    }

    pub fn partition_deltas(
        &self,
    ) -> impl ExactSizeIterator<Item = (PartitionKey, PartitionDelta)> + '_ {
        self.partitions
            .iter()
            .copied()
            .enumerate()
            .map(|(index, delta)| {
                let warehouse_id = (index / DISTRICTS_PER_WAREHOUSE as usize) as i32 + 1;
                let district_id = (index % DISTRICTS_PER_WAREHOUSE as usize) as i32 + 1;
                (
                    PartitionKey {
                        warehouse_id,
                        district_id,
                    },
                    delta,
                )
            })
    }

    /// Record one confirmed terminal using the normal class derived from its
    /// frozen stage.
    ///
    /// Retryable abort attempts must not be passed here. Only the eventual
    /// commit or the transaction's specified expected rollback is durable
    /// workload evidence.
    pub fn record(
        &mut self,
        ticket: &TransactionTicket,
        outcome: &RankedTransactionOutcome,
    ) -> Result<(), LedgerError> {
        let class = LedgerClass::normal_for_stage(ticket.route().stage);
        self.record_as(class, ticket, outcome)
    }

    /// Record a confirmed terminal that arrived after its phase deadline.
    ///
    /// A tail commit never contributes to ranked accounting, but it did mutate
    /// the database and must remain in online and recovery expectations.
    pub fn record_grace_tail(
        &mut self,
        ticket: &TransactionTicket,
        outcome: &RankedTransactionOutcome,
    ) -> Result<(), LedgerError> {
        let class = LedgerClass::tail_for_stage(ticket.route().stage);
        self.record_as(class, ticket, outcome)
    }

    pub fn record_as(
        &mut self,
        class: LedgerClass,
        ticket: &TransactionTicket,
        outcome: &RankedTransactionOutcome,
    ) -> Result<(), LedgerError> {
        let event = event_from_ticket(class, ticket, outcome)?;
        ensure_extend_capacity(&mut self.events, 1, "ledger events")?;
        self.apply_outcome(outcome)?;
        self.events.push(event);
        Ok(())
    }

    fn apply_outcome(&mut self, outcome: &RankedTransactionOutcome) -> Result<(), LedgerError> {
        match outcome {
            RankedTransactionOutcome::ExpectedRollback => {
                self.expected_rollbacks =
                    checked_add(self.expected_rollbacks, 1, "expected_rollbacks")?;
                Ok(())
            }
            RankedTransactionOutcome::Committed(RankedCommit::NewOrder(evidence)) => {
                let index = partition_index(
                    i32::from(evidence.warehouse_id),
                    i32::from(evidence.district_id),
                )?;
                if evidence.order_id <= 0 {
                    return Err(LedgerError::InvalidEvidence(
                        "NewOrder order_id must be positive",
                    ));
                }
                let line_count = i64::from(evidence.line_count);
                if !(5..=15).contains(&line_count) {
                    return Err(LedgerError::InvalidEvidence(
                        "NewOrder line_count must be 5..=15",
                    ));
                }
                let remote_line_count = i64::from(evidence.remote_line_count);
                if remote_line_count > line_count {
                    return Err(LedgerError::InvalidEvidence(
                        "NewOrder remote lines exceed all lines",
                    ));
                }
                if evidence.line_amount_bits.len() != evidence.line_count as usize {
                    return Err(LedgerError::InvalidEvidence(
                        "NewOrder amount-bit count differs from line_count",
                    ));
                }
                validate_amount_bits("NewOrder line amount", &evidence.line_amount_bits)?;

                let stock_delta = i64::from(evidence.stock_ytd_delta);
                let maximum_stock_delta =
                    checked_mul(line_count, 10, "NewOrder maximum stock_ytd_delta")?;
                if !(line_count..=maximum_stock_delta).contains(&stock_delta) {
                    return Err(LedgerError::InvalidEvidence(
                        "NewOrder stock_ytd_delta must equal 1..=10 per line",
                    ));
                }

                let new_orders = checked_add(self.new_orders, 1, "new_orders")?;
                let new_order_lines =
                    checked_add(self.new_order_lines, line_count, "new_order_lines")?;
                let remote_new_order_lines = checked_add(
                    self.remote_new_order_lines,
                    remote_line_count,
                    "remote_new_order_lines",
                )?;
                let stock_ytd_delta =
                    checked_add(self.stock_ytd_delta, stock_delta, "stock_ytd_delta")?;
                let partition = self.partitions[index].checked_add(PartitionDelta {
                    new_orders: 1,
                    new_order_lines: line_count,
                    ..PartitionDelta::default()
                })?;

                ensure_extend_capacity(
                    &mut self.new_order_line_amount_bits,
                    evidence.line_amount_bits.len(),
                    "new_order_line_amount_bits",
                )?;
                self.new_orders = new_orders;
                self.new_order_lines = new_order_lines;
                self.remote_new_order_lines = remote_new_order_lines;
                self.stock_ytd_delta = stock_ytd_delta;
                self.partitions[index] = partition;
                self.new_order_line_amount_bits
                    .extend_from_slice(&evidence.line_amount_bits);
                Ok(())
            }
            RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)) => {
                partition_index(
                    i32::from(evidence.warehouse_id),
                    i32::from(evidence.district_id),
                )?;
                partition_index(
                    i32::from(evidence.customer_warehouse_id),
                    i32::from(evidence.customer_district_id),
                )?;
                if !(1..=CUSTOMERS_PER_DISTRICT).contains(&i64::from(evidence.customer_id)) {
                    return Err(LedgerError::InvalidEvidence(
                        "Payment customer_id is outside 1..=3000",
                    ));
                }
                validate_amount_bits("Payment amount", &[evidence.amount_bits])?;
                let payments = checked_add(self.payments, 1, "payments")?;

                ensure_extend_capacity(&mut self.payment_amount_bits, 1, "payment_amount_bits")?;
                self.payments = payments;
                self.payment_amount_bits.push(evidence.amount_bits);
                Ok(())
            }
            RankedTransactionOutcome::Committed(RankedCommit::OrderStatus) => {
                self.order_status_commits =
                    checked_add(self.order_status_commits, 1, "order_status_commits")?;
                Ok(())
            }
            RankedTransactionOutcome::Committed(RankedCommit::StockLevel { low_stock_count }) => {
                if !(0..=300).contains(low_stock_count) {
                    return Err(LedgerError::InvalidEvidence(
                        "StockLevel low_stock_count must be in 0..=300",
                    ));
                }
                self.stock_level_commits =
                    checked_add(self.stock_level_commits, 1, "stock_level_commits")?;
                Ok(())
            }
            RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)) => {
                if orders.len() > DISTRICTS_PER_WAREHOUSE as usize {
                    return Err(LedgerError::InvalidEvidence(
                        "Delivery processed more than one order per district",
                    ));
                }

                let delivery_commits = checked_add(self.delivery_commits, 1, "delivery_commits")?;
                let mut delivered_orders_delta = 0_i64;
                let mut delivered_lines_delta = 0_i64;
                let mut warehouse_id = None;
                let mut partition_updates = Vec::with_capacity(orders.len());

                for order in orders {
                    if let Some(expected_warehouse_id) = warehouse_id {
                        if order.warehouse_id != expected_warehouse_id {
                            return Err(LedgerError::InvalidEvidence(
                                "one Delivery commit spans multiple warehouses",
                            ));
                        }
                    } else {
                        warehouse_id = Some(order.warehouse_id);
                    }
                    let index = partition_index(
                        i32::from(order.warehouse_id),
                        i32::from(order.district_id),
                    )?;
                    if partition_updates
                        .iter()
                        .any(|(existing_index, _)| *existing_index == index)
                    {
                        return Err(LedgerError::InvalidEvidence(
                            "Delivery processed a district more than once",
                        ));
                    }
                    if order.order_id <= 0 {
                        return Err(LedgerError::InvalidEvidence(
                            "Delivery order_id must be positive",
                        ));
                    }
                    if !(1..=CUSTOMERS_PER_DISTRICT).contains(&i64::from(order.customer_id)) {
                        return Err(LedgerError::InvalidEvidence(
                            "Delivery customer_id is outside 1..=3000",
                        ));
                    }
                    let line_count = i64::from(order.line_count);
                    if !(5..=15).contains(&line_count) {
                        return Err(LedgerError::InvalidEvidence(
                            "Delivery line_count must be 5..=15",
                        ));
                    }
                    validate_amount_bits("Delivery customer amount", &[order.amount_bits])?;

                    delivered_orders_delta =
                        checked_add(delivered_orders_delta, 1, "Delivery order delta")?;
                    delivered_lines_delta =
                        checked_add(delivered_lines_delta, line_count, "Delivery line delta")?;
                    partition_updates.push((
                        index,
                        self.partitions[index].checked_add(PartitionDelta {
                            delivered_orders: 1,
                            delivered_order_lines: line_count,
                            ..PartitionDelta::default()
                        })?,
                    ));
                }

                let delivered_orders = checked_add(
                    self.delivered_orders,
                    delivered_orders_delta,
                    "delivered_orders",
                )?;
                let delivered_order_lines = checked_add(
                    self.delivered_order_lines,
                    delivered_lines_delta,
                    "delivered_order_lines",
                )?;
                ensure_extend_capacity(
                    &mut self.delivery_customer_amount_bits,
                    orders.len(),
                    "delivery_customer_amount_bits",
                )?;

                self.delivery_commits = delivery_commits;
                self.delivered_orders = delivered_orders;
                self.delivered_order_lines = delivered_order_lines;
                for (index, delta) in partition_updates {
                    self.partitions[index] = delta;
                }
                self.delivery_customer_amount_bits
                    .extend(orders.iter().map(|order| order.amount_bits));
                Ok(())
            }
        }
    }

    /// Merge one worker-local ledger. Merge workers in stable worker-id order
    /// when byte-for-byte persistence reproducibility is desired.
    pub fn merge(&mut self, other: &Self) -> Result<(), LedgerError> {
        // All fields are private and every constructor validates its input.
        // Avoid repeatedly scanning an ever-growing FLOAT evidence vector
        // while merging many worker-local ledgers after a ranked window.
        debug_assert!(self.validate().is_ok());
        debug_assert!(other.validate().is_ok());

        let mut identities: std::collections::BTreeSet<_> = self
            .events
            .iter()
            .map(|event| event.meta().identity())
            .collect();
        for event in &other.events {
            if !identities.insert(event.meta().identity()) {
                return Err(LedgerError::Inconsistent(
                    "duplicate terminal event identity across worker ledgers".to_owned(),
                ));
            }
        }

        let new_orders = checked_add(self.new_orders, other.new_orders, "new_orders")?;
        let new_order_lines = checked_add(
            self.new_order_lines,
            other.new_order_lines,
            "new_order_lines",
        )?;
        let remote_new_order_lines = checked_add(
            self.remote_new_order_lines,
            other.remote_new_order_lines,
            "remote_new_order_lines",
        )?;
        let stock_ytd_delta = checked_add(
            self.stock_ytd_delta,
            other.stock_ytd_delta,
            "stock_ytd_delta",
        )?;
        let payments = checked_add(self.payments, other.payments, "payments")?;
        let delivery_commits = checked_add(
            self.delivery_commits,
            other.delivery_commits,
            "delivery_commits",
        )?;
        let delivered_orders = checked_add(
            self.delivered_orders,
            other.delivered_orders,
            "delivered_orders",
        )?;
        let delivered_order_lines = checked_add(
            self.delivered_order_lines,
            other.delivered_order_lines,
            "delivered_order_lines",
        )?;
        let order_status_commits = checked_add(
            self.order_status_commits,
            other.order_status_commits,
            "order_status_commits",
        )?;
        let stock_level_commits = checked_add(
            self.stock_level_commits,
            other.stock_level_commits,
            "stock_level_commits",
        )?;
        let expected_rollbacks = checked_add(
            self.expected_rollbacks,
            other.expected_rollbacks,
            "expected_rollbacks",
        )?;

        let mut partitions = Vec::with_capacity(PARTITION_COUNT);
        for (left, right) in self.partitions.iter().zip(&other.partitions) {
            partitions.push(left.checked_add(*right)?);
        }

        ensure_extend_capacity(
            &mut self.new_order_line_amount_bits,
            other.new_order_line_amount_bits.len(),
            "new_order_line_amount_bits",
        )?;
        ensure_extend_capacity(
            &mut self.payment_amount_bits,
            other.payment_amount_bits.len(),
            "payment_amount_bits",
        )?;
        ensure_extend_capacity(
            &mut self.delivery_customer_amount_bits,
            other.delivery_customer_amount_bits.len(),
            "delivery_customer_amount_bits",
        )?;
        ensure_extend_capacity(&mut self.events, other.events.len(), "ledger events")?;

        self.new_orders = new_orders;
        self.new_order_lines = new_order_lines;
        self.remote_new_order_lines = remote_new_order_lines;
        self.stock_ytd_delta = stock_ytd_delta;
        self.payments = payments;
        self.delivery_commits = delivery_commits;
        self.delivered_orders = delivered_orders;
        self.delivered_order_lines = delivered_order_lines;
        self.order_status_commits = order_status_commits;
        self.stock_level_commits = stock_level_commits;
        self.expected_rollbacks = expected_rollbacks;
        self.partitions = partitions;
        self.new_order_line_amount_bits
            .extend_from_slice(&other.new_order_line_amount_bits);
        self.payment_amount_bits
            .extend_from_slice(&other.payment_amount_bits);
        self.delivery_customer_amount_bits
            .extend_from_slice(&other.delivery_customer_amount_bits);
        self.events.extend_from_slice(&other.events);
        Ok(())
    }

    pub fn merge_all<I>(ledgers: I) -> Result<Self, LedgerError>
    where
        I: IntoIterator<Item = Self>,
    {
        let mut merged = Self::default();
        for ledger in ledgers {
            merged.merge(&ledger)?;
        }
        Ok(merged)
    }

    pub fn to_committed_ledger(&self) -> CommittedLedger {
        CommittedLedger {
            new_orders: self.new_orders,
            new_order_lines: self.new_order_lines,
            remote_new_order_lines: self.remote_new_order_lines,
            payments: self.payments,
            delivered_orders: self.delivered_orders,
            delivered_order_lines: self.delivered_order_lines,
        }
    }

    /// Return a ledger containing only in-deadline formal-window terminals.
    ///
    /// Warmup and tail commits remain in `self` because they changed physical
    /// state, but are intentionally excluded from this ranked projection.
    pub fn ranked_projection(&self) -> Result<Self, LedgerError> {
        let mut ranked = Self::default();
        for event in &self.events {
            if event.meta().class.is_ranked() {
                ensure_extend_capacity(&mut ranked.events, 1, "ranked ledger events")?;
                ranked.apply_outcome(&event.to_outcome())?;
                ranked.events.push(event.clone());
            }
        }
        Ok(ranked)
    }

    pub fn to_ranked_committed_ledger(&self) -> Result<CommittedLedger, LedgerError> {
        Ok(self.ranked_projection()?.to_committed_ledger())
    }

    pub fn ranked_partition_deltas(
        &self,
    ) -> Result<Vec<(PartitionKey, PartitionDelta)>, LedgerError> {
        Ok(self.ranked_projection()?.partition_deltas().collect())
    }

    /// Encode a canonical field order using decimal integers and raw, lower
    /// case, eight-hex-digit binary32 words.
    pub fn encode(&self) -> String {
        debug_assert!(self.validate().is_ok());

        let mut output = String::new();
        let _ = writeln!(output, "{FORMAT_HEADER}");
        let _ = writeln!(output, "new_orders={}", self.new_orders);
        let _ = writeln!(output, "new_order_lines={}", self.new_order_lines);
        let _ = writeln!(
            output,
            "remote_new_order_lines={}",
            self.remote_new_order_lines
        );
        let _ = writeln!(output, "stock_ytd_delta={}", self.stock_ytd_delta);
        let _ = writeln!(output, "payments={}", self.payments);
        let _ = writeln!(output, "delivery_commits={}", self.delivery_commits);
        let _ = writeln!(output, "delivered_orders={}", self.delivered_orders);
        let _ = writeln!(
            output,
            "delivered_order_lines={}",
            self.delivered_order_lines
        );
        let _ = writeln!(output, "order_status_commits={}", self.order_status_commits);
        let _ = writeln!(output, "stock_level_commits={}", self.stock_level_commits);
        let _ = writeln!(output, "expected_rollbacks={}", self.expected_rollbacks);
        let _ = writeln!(output, "event_count={}", self.events.len());

        output.push_str("new_order_line_amount_bits=");
        append_bits(&mut output, &self.new_order_line_amount_bits);
        output.push('\n');
        output.push_str("payment_amount_bits=");
        append_bits(&mut output, &self.payment_amount_bits);
        output.push('\n');
        output.push_str("delivery_customer_amount_bits=");
        append_bits(&mut output, &self.delivery_customer_amount_bits);
        output.push('\n');

        for (key, delta) in self.partition_deltas() {
            let _ = writeln!(
                output,
                "partition.{}.{}={},{},{},{}",
                key.warehouse_id,
                key.district_id,
                delta.new_orders,
                delta.new_order_lines,
                delta.delivered_orders,
                delta.delivered_order_lines
            );
        }
        for (index, event) in self.events.iter().enumerate() {
            let _ = write!(output, "event.{index}=");
            append_event(&mut output, event);
            output.push('\n');
        }
        output
    }

    pub fn decode(input: &str) -> Result<Self, LedgerError> {
        let mut lines = input.split_terminator('\n');
        match lines.next() {
            Some(FORMAT_HEADER) => {}
            Some(header) => {
                return Err(LedgerError::UnsupportedVersion(header.to_owned()));
            }
            None => return Err(LedgerError::Malformed("empty ledger".to_owned())),
        }

        let mut globals = std::collections::BTreeMap::new();
        let mut partitions = vec![None; PARTITION_COUNT];
        let mut encoded_events = std::collections::BTreeMap::new();

        for line in lines {
            if line.is_empty() {
                return Err(LedgerError::Malformed(
                    "empty line is not permitted".to_owned(),
                ));
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| LedgerError::Malformed(format!("missing '=' in {line:?}")))?;
            if key.starts_with("partition.") {
                let index = parse_partition_key(key)?;
                if partitions[index].is_some() {
                    return Err(LedgerError::DuplicateField(key.to_owned()));
                }
                partitions[index] = Some(parse_partition_delta(key, value)?);
                continue;
            }
            if key.starts_with("event.") {
                let index = parse_indexed_key("event", key)?;
                if encoded_events.insert(index, value).is_some() {
                    return Err(LedgerError::DuplicateField(key.to_owned()));
                }
                continue;
            }
            if !GLOBAL_FIELDS.contains(&key) {
                return Err(LedgerError::UnknownField(key.to_owned()));
            }
            if globals.insert(key, value).is_some() {
                return Err(LedgerError::DuplicateField(key.to_owned()));
            }
        }

        for required in GLOBAL_FIELDS {
            if !globals.contains_key(required) {
                return Err(LedgerError::MissingField(required.to_owned()));
            }
        }
        let missing_partition = partitions.iter().position(Option::is_none);
        if let Some(index) = missing_partition {
            let warehouse_id = index / DISTRICTS_PER_WAREHOUSE as usize + 1;
            let district_id = index % DISTRICTS_PER_WAREHOUSE as usize + 1;
            return Err(LedgerError::MissingField(format!(
                "partition.{warehouse_id}.{district_id}"
            )));
        }
        let partitions = partitions
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                LedgerError::Malformed("partition completeness changed during decoding".to_owned())
            })?;

        let new_orders = parse_count("new_orders", globals["new_orders"])?;
        let new_order_lines = parse_count("new_order_lines", globals["new_order_lines"])?;
        let remote_new_order_lines =
            parse_count("remote_new_order_lines", globals["remote_new_order_lines"])?;
        let stock_ytd_delta = parse_count("stock_ytd_delta", globals["stock_ytd_delta"])?;
        let payments = parse_count("payments", globals["payments"])?;
        let delivery_commits = parse_count("delivery_commits", globals["delivery_commits"])?;
        let delivered_orders = parse_count("delivered_orders", globals["delivered_orders"])?;
        let delivered_order_lines =
            parse_count("delivered_order_lines", globals["delivered_order_lines"])?;
        let order_status_commits =
            parse_count("order_status_commits", globals["order_status_commits"])?;
        let stock_level_commits =
            parse_count("stock_level_commits", globals["stock_level_commits"])?;
        let expected_rollbacks = parse_count("expected_rollbacks", globals["expected_rollbacks"])?;
        let event_count = parse_count("event_count", globals["event_count"])?;
        let event_count =
            usize::try_from(event_count).map_err(|_| LedgerError::Overflow("event_count"))?;
        if encoded_events.len() != event_count {
            return Err(LedgerError::Inconsistent(format!(
                "event_count {event_count} differs from {} encoded events",
                encoded_events.len()
            )));
        }
        let mut events = Vec::new();
        events
            .try_reserve(event_count)
            .map_err(|_| LedgerError::Capacity("decoded events"))?;
        for index in 0..event_count {
            let encoded = encoded_events
                .remove(&index)
                .ok_or_else(|| LedgerError::MissingField(format!("event.{index}")))?;
            events.push(parse_event(encoded)?);
        }

        let ledger = Self {
            new_orders,
            new_order_lines,
            remote_new_order_lines,
            stock_ytd_delta,
            payments,
            delivery_commits,
            delivered_orders,
            delivered_order_lines,
            order_status_commits,
            stock_level_commits,
            expected_rollbacks,
            new_order_line_amount_bits: parse_bits_exact(
                "new_order_line_amount_bits",
                globals["new_order_line_amount_bits"],
                new_order_lines,
            )?,
            payment_amount_bits: parse_bits_exact(
                "payment_amount_bits",
                globals["payment_amount_bits"],
                payments,
            )?,
            delivery_customer_amount_bits: parse_bits_exact(
                "delivery_customer_amount_bits",
                globals["delivery_customer_amount_bits"],
                delivered_orders,
            )?,
            partitions,
            events,
        };
        ledger.validate()?;
        Ok(ledger)
    }

    fn validate(&self) -> Result<(), LedgerError> {
        for (name, value) in [
            ("new_orders", self.new_orders),
            ("new_order_lines", self.new_order_lines),
            ("remote_new_order_lines", self.remote_new_order_lines),
            ("stock_ytd_delta", self.stock_ytd_delta),
            ("payments", self.payments),
            ("delivery_commits", self.delivery_commits),
            ("delivered_orders", self.delivered_orders),
            ("delivered_order_lines", self.delivered_order_lines),
            ("order_status_commits", self.order_status_commits),
            ("stock_level_commits", self.stock_level_commits),
            ("expected_rollbacks", self.expected_rollbacks),
        ] {
            if value < 0 {
                return Err(LedgerError::Inconsistent(format!(
                    "{name} must be non-negative"
                )));
            }
        }
        if self.partitions.len() != PARTITION_COUNT {
            return Err(LedgerError::Inconsistent(format!(
                "ledger must contain exactly {PARTITION_COUNT} partitions"
            )));
        }
        if self.remote_new_order_lines > self.new_order_lines {
            return Err(LedgerError::Inconsistent(
                "remote NewOrder lines exceed all NewOrder lines".to_owned(),
            ));
        }
        validate_line_range(self.new_orders, self.new_order_lines, "committed NewOrder")?;
        validate_line_range(
            self.delivered_orders,
            self.delivered_order_lines,
            "delivered order",
        )?;
        let maximum_delivered_orders =
            checked_mul(self.delivery_commits, 10, "maximum delivered orders")?;
        if self.delivered_orders > maximum_delivered_orders {
            return Err(LedgerError::Inconsistent(
                "delivered orders exceed ten per committed Delivery".to_owned(),
            ));
        }

        let maximum_stock_delta = checked_mul(self.new_order_lines, 10, "maximum stock_ytd_delta")?;
        if !(self.new_order_lines..=maximum_stock_delta).contains(&self.stock_ytd_delta) {
            return Err(LedgerError::Inconsistent(
                "stock_ytd_delta must equal 1..=10 per committed NewOrder line".to_owned(),
            ));
        }
        validate_vector_len(
            "new_order_line_amount_bits",
            self.new_order_lines,
            self.new_order_line_amount_bits.len(),
        )?;
        validate_vector_len(
            "payment_amount_bits",
            self.payments,
            self.payment_amount_bits.len(),
        )?;
        validate_vector_len(
            "delivery_customer_amount_bits",
            self.delivered_orders,
            self.delivery_customer_amount_bits.len(),
        )?;
        validate_amount_bits("NewOrder line amount", &self.new_order_line_amount_bits)?;
        validate_amount_bits("Payment amount", &self.payment_amount_bits)?;
        validate_amount_bits(
            "Delivery customer amount",
            &self.delivery_customer_amount_bits,
        )?;

        let mut partition_totals = PartitionDelta::default();
        for delta in &self.partitions {
            delta.validate()?;
            let available_orders = checked_add(
                NEW_ORDERS_PER_DISTRICT,
                delta.new_orders,
                "partition available undelivered orders",
            )?;
            if delta.delivered_orders > available_orders {
                return Err(LedgerError::Inconsistent(
                    "partition deliveries exceed initial plus committed queued orders".to_owned(),
                ));
            }
            partition_totals = partition_totals.checked_add(*delta)?;
        }
        if partition_totals.new_orders != self.new_orders
            || partition_totals.new_order_lines != self.new_order_lines
            || partition_totals.delivered_orders != self.delivered_orders
            || partition_totals.delivered_order_lines != self.delivered_order_lines
        {
            return Err(LedgerError::Inconsistent(
                "partition deltas do not equal global committed counts".to_owned(),
            ));
        }

        let mut identities = std::collections::BTreeSet::new();
        let mut replayed = Self::default();
        for event in &self.events {
            event.validate()?;
            if !identities.insert(event.meta().identity()) {
                return Err(LedgerError::Inconsistent(
                    "duplicate terminal event identity".to_owned(),
                ));
            }
            replayed.apply_outcome(&event.to_outcome())?;
        }
        if !self.same_aggregate_state(&replayed) {
            return Err(LedgerError::Inconsistent(
                "terminal events do not reproduce aggregate ledger fields".to_owned(),
            ));
        }
        Ok(())
    }

    fn same_aggregate_state(&self, other: &Self) -> bool {
        self.new_orders == other.new_orders
            && self.new_order_lines == other.new_order_lines
            && self.remote_new_order_lines == other.remote_new_order_lines
            && self.stock_ytd_delta == other.stock_ytd_delta
            && self.payments == other.payments
            && self.delivery_commits == other.delivery_commits
            && self.delivered_orders == other.delivered_orders
            && self.delivered_order_lines == other.delivered_order_lines
            && self.order_status_commits == other.order_status_commits
            && self.stock_level_commits == other.stock_level_commits
            && self.expected_rollbacks == other.expected_rollbacks
            && self.new_order_line_amount_bits == other.new_order_line_amount_bits
            && self.payment_amount_bits == other.payment_amount_bits
            && self.delivery_customer_amount_bits == other.delivery_customer_amount_bits
            && self.partitions == other.partitions
    }
}

impl From<&RunLedger> for CommittedLedger {
    fn from(ledger: &RunLedger) -> Self {
        ledger.to_committed_ledger()
    }
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum LedgerError {
    #[error("invalid ledger evidence: {0}")]
    InvalidEvidence(&'static str),

    #[error("partition key is outside the final 50x10 domain: ({warehouse_id}, {district_id})")]
    InvalidPartitionKey { warehouse_id: i32, district_id: i32 },

    #[error("ledger counter overflow: {0}")]
    Overflow(&'static str),

    #[error("ledger vector capacity failed: {0}")]
    Capacity(&'static str),

    #[error("unsupported ledger version/header: {0:?}")]
    UnsupportedVersion(String),

    #[error("malformed ledger: {0}")]
    Malformed(String),

    #[error("duplicate ledger field: {0}")]
    DuplicateField(String),

    #[error("unknown ledger field: {0}")]
    UnknownField(String),

    #[error("missing ledger field: {0}")]
    MissingField(String),

    #[error("bad FLOAT32 bits in {field}: {bits:?}")]
    InvalidFloatBits { field: String, bits: String },

    #[error("inconsistent ledger: {0}")]
    Inconsistent(String),
}

fn partition_index(warehouse_id: i32, district_id: i32) -> Result<usize, LedgerError> {
    if !(1..=FINAL_WAREHOUSES).contains(&warehouse_id)
        || !(1..=DISTRICTS_PER_WAREHOUSE).contains(&district_id)
    {
        return Err(LedgerError::InvalidPartitionKey {
            warehouse_id,
            district_id,
        });
    }
    Ok(((warehouse_id - 1) * DISTRICTS_PER_WAREHOUSE + district_id - 1) as usize)
}

fn checked_add(left: i64, right: i64, name: &'static str) -> Result<i64, LedgerError> {
    left.checked_add(right).ok_or(LedgerError::Overflow(name))
}

fn checked_mul(left: i64, right: i64, name: &'static str) -> Result<i64, LedgerError> {
    left.checked_mul(right).ok_or(LedgerError::Overflow(name))
}

fn ensure_extend_capacity<T>(
    values: &mut Vec<T>,
    additional: usize,
    name: &'static str,
) -> Result<(), LedgerError> {
    values
        .len()
        .checked_add(additional)
        .ok_or(LedgerError::Overflow(name))?;
    values
        .try_reserve(additional)
        .map_err(|_| LedgerError::Capacity(name))
}

fn validate_amount_bits(name: &str, bits: &[u32]) -> Result<(), LedgerError> {
    for bits in bits {
        let value = f32::from_bits(*bits);
        if !value.is_finite() || value < 0.0 {
            return Err(LedgerError::InvalidFloatBits {
                field: name.to_owned(),
                bits: format!("{bits:08x}"),
            });
        }
    }
    Ok(())
}

fn validate_line_range(order_count: i64, line_count: i64, name: &str) -> Result<(), LedgerError> {
    let minimum = checked_mul(order_count, 5, "minimum order-line count")?;
    let maximum = checked_mul(order_count, 15, "maximum order-line count")?;
    if !(minimum..=maximum).contains(&line_count) {
        return Err(LedgerError::Inconsistent(format!(
            "{name} lines must be 5..=15 per order"
        )));
    }
    Ok(())
}

fn validate_vector_len(name: &str, expected: i64, actual: usize) -> Result<(), LedgerError> {
    let actual = i64::try_from(actual).map_err(|_| LedgerError::Overflow("vector length"))?;
    if actual != expected {
        return Err(LedgerError::Inconsistent(format!(
            "{name} length {actual} differs from expected {expected}"
        )));
    }
    Ok(())
}

fn append_bits(output: &mut String, bits: &[u32]) {
    for bits in bits {
        let _ = write!(output, "{bits:08x}");
    }
}

fn append_event(output: &mut String, event: &LedgerEvent) {
    match event {
        LedgerEvent::NewOrder(delta) => {
            output.push_str("N|");
            append_meta(output, delta.meta);
            let _ = write!(
                output,
                "|{},{},{},{}|",
                delta.warehouse_id, delta.district_id, delta.order_id, delta.customer_id
            );
            for (index, line) in delta.lines.iter().enumerate() {
                if index > 0 {
                    output.push(';');
                }
                let _ = write!(
                    output,
                    "{},{},{},{},{:08x}",
                    line.number,
                    line.item_id,
                    line.supply_warehouse,
                    line.quantity,
                    line.amount_bits
                );
            }
        }
        LedgerEvent::Payment(delta) => {
            output.push_str("P|");
            append_meta(output, delta.meta);
            let _ = write!(
                output,
                "|{},{},{},{},{},{:08x},{:08x},{:08x},{:08x},{:08x},{:08x},{:08x},{:08x},{:08x},{},{},{},{}",
                delta.warehouse_id,
                delta.district_id,
                delta.customer_warehouse_id,
                delta.customer_district_id,
                delta.customer_id,
                delta.amount_bits,
                delta.warehouse_before_bits,
                delta.warehouse_after_bits,
                delta.district_before_bits,
                delta.district_after_bits,
                delta.customer_balance_before_bits,
                delta.customer_balance_after_bits,
                delta.customer_ytd_before_bits,
                delta.customer_ytd_after_bits,
                delta.customer_payment_count_before,
                delta.customer_delivery_count_before,
                delta.customer_payment_count_after,
                delta.customer_delivery_count_after
            );
        }
        LedgerEvent::Delivery(delta) => {
            output.push_str("D|");
            append_meta(output, delta.meta);
            let _ = write!(output, "|{},{}|", delta.warehouse_id, delta.carrier_id);
            for (index, order) in delta.orders.iter().enumerate() {
                if index > 0 {
                    output.push(';');
                }
                let _ = write!(
                    output,
                    "{},{},{},{},{:08x},{:08x},{:08x},{},{},{},{}",
                    order.district_id,
                    order.order_id,
                    order.customer_id,
                    order.line_count,
                    order.customer_amount_bits,
                    order.customer_balance_before_bits,
                    order.customer_balance_after_bits,
                    order.customer_payment_count_before,
                    order.customer_delivery_count_before,
                    order.customer_payment_count_after,
                    order.customer_delivery_count_after
                );
            }
        }
        LedgerEvent::OrderStatus { meta } => {
            output.push_str("O|");
            append_meta(output, *meta);
        }
        LedgerEvent::StockLevel {
            meta,
            low_stock_count,
        } => {
            output.push_str("S|");
            append_meta(output, *meta);
            let _ = write!(output, "|{low_stock_count}");
        }
        LedgerEvent::ExpectedRollback {
            meta,
            warehouse_id,
            district_id,
        } => {
            output.push_str("R|");
            append_meta(output, *meta);
            let _ = write!(output, "|{warehouse_id},{district_id}");
        }
    }
}

fn append_meta(output: &mut String, meta: LedgerEventMeta) {
    append_class(output, meta.class);
    let _ = write!(
        output,
        ",{},{},{},{:016x}",
        meta.stage, meta.client_id, meta.txn_no, meta.parameter_fingerprint
    );
}

fn append_class(output: &mut String, class: LedgerClass) {
    match class {
        LedgerClass::Warmup => output.push('w'),
        LedgerClass::RankedWindow(index) => {
            let _ = write!(output, "r{index}");
        }
        LedgerClass::WarmupTail => output.push_str("wt"),
        LedgerClass::RankedTail(index) => {
            let _ = write!(output, "t{index}");
        }
        LedgerClass::NonRanked => output.push('n'),
    }
}

fn parse_event(encoded: &str) -> Result<LedgerEvent, LedgerError> {
    let sections: Vec<_> = encoded.split('|').collect();
    let event = match sections.as_slice() {
        ["N", meta, header, lines] => {
            let meta = parse_meta(meta)?;
            let header = parse_csv_exact(header, 4, "NewOrder header")?;
            let warehouse_id = parse_u16("NewOrder warehouse_id", header[0])?;
            let district_id = parse_u8("NewOrder district_id", header[1])?;
            let order_id = parse_i32("NewOrder order_id", header[2])?;
            let customer_id = parse_u16("NewOrder customer_id", header[3])?;
            let mut decoded_lines = Vec::new();
            if !lines.is_empty() {
                for encoded_line in lines.split(';') {
                    let values = parse_csv_exact(encoded_line, 5, "NewOrder line")?;
                    decoded_lines.push(NewOrderLineDelta {
                        number: parse_u8("NewOrder line number", values[0])?,
                        item_id: parse_u32("NewOrder item_id", values[1])?,
                        supply_warehouse: parse_u16("NewOrder supply warehouse", values[2])?,
                        quantity: parse_u8("NewOrder quantity", values[3])?,
                        amount_bits: parse_one_bits("NewOrder line amount", values[4])?,
                    });
                }
            }
            LedgerEvent::NewOrder(NewOrderDelta {
                meta,
                warehouse_id,
                district_id,
                order_id,
                customer_id,
                lines: decoded_lines,
            })
        }
        ["P", meta, body] => {
            let values = parse_csv_exact(body, 18, "Payment")?;
            LedgerEvent::Payment(PaymentDelta {
                meta: parse_meta(meta)?,
                warehouse_id: parse_u16("Payment warehouse_id", values[0])?,
                district_id: parse_u8("Payment district_id", values[1])?,
                customer_warehouse_id: parse_u16("Payment customer warehouse", values[2])?,
                customer_district_id: parse_u8("Payment customer district", values[3])?,
                customer_id: parse_i32("Payment customer_id", values[4])?,
                amount_bits: parse_one_bits("Payment amount", values[5])?,
                warehouse_before_bits: parse_one_bits("Payment warehouse before", values[6])?,
                warehouse_after_bits: parse_one_bits("Payment warehouse after", values[7])?,
                district_before_bits: parse_one_bits("Payment district before", values[8])?,
                district_after_bits: parse_one_bits("Payment district after", values[9])?,
                customer_balance_before_bits: parse_one_bits(
                    "Payment customer balance before",
                    values[10],
                )?,
                customer_balance_after_bits: parse_one_bits(
                    "Payment customer balance after",
                    values[11],
                )?,
                customer_ytd_before_bits: parse_one_bits(
                    "Payment customer ytd before",
                    values[12],
                )?,
                customer_ytd_after_bits: parse_one_bits("Payment customer ytd after", values[13])?,
                customer_payment_count_before: parse_i32(
                    "Payment customer payment count before",
                    values[14],
                )?,
                customer_delivery_count_before: parse_i32(
                    "Payment customer delivery count before",
                    values[15],
                )?,
                customer_payment_count_after: parse_i32(
                    "Payment customer payment count after",
                    values[16],
                )?,
                customer_delivery_count_after: parse_i32(
                    "Payment customer delivery count after",
                    values[17],
                )?,
            })
        }
        ["D", meta, header, orders] => {
            let header = parse_csv_exact(header, 2, "Delivery header")?;
            let mut decoded_orders = Vec::new();
            if !orders.is_empty() {
                for encoded_order in orders.split(';') {
                    let values = parse_csv_exact(encoded_order, 11, "Delivery order")?;
                    decoded_orders.push(DeliveredOrderDelta {
                        district_id: parse_u8("Delivery district_id", values[0])?,
                        order_id: parse_i32("Delivery order_id", values[1])?,
                        customer_id: parse_i32("Delivery customer_id", values[2])?,
                        line_count: parse_u8("Delivery line_count", values[3])?,
                        customer_amount_bits: parse_one_bits(
                            "Delivery customer amount",
                            values[4],
                        )?,
                        customer_balance_before_bits: parse_one_bits(
                            "Delivery customer balance before",
                            values[5],
                        )?,
                        customer_balance_after_bits: parse_one_bits(
                            "Delivery customer balance after",
                            values[6],
                        )?,
                        customer_payment_count_before: parse_i32(
                            "Delivery customer payment count before",
                            values[7],
                        )?,
                        customer_delivery_count_before: parse_i32(
                            "Delivery customer delivery count before",
                            values[8],
                        )?,
                        customer_payment_count_after: parse_i32(
                            "Delivery customer payment count after",
                            values[9],
                        )?,
                        customer_delivery_count_after: parse_i32(
                            "Delivery customer delivery count after",
                            values[10],
                        )?,
                    });
                }
            }
            LedgerEvent::Delivery(DeliveryDelta {
                meta: parse_meta(meta)?,
                warehouse_id: parse_u16("Delivery warehouse_id", header[0])?,
                carrier_id: parse_u8("Delivery carrier_id", header[1])?,
                orders: decoded_orders,
            })
        }
        ["O", meta] => LedgerEvent::OrderStatus {
            meta: parse_meta(meta)?,
        },
        ["S", meta, low_stock_count] => LedgerEvent::StockLevel {
            meta: parse_meta(meta)?,
            low_stock_count: parse_i32("StockLevel low_stock_count", low_stock_count)?,
        },
        ["R", meta, partition] => {
            let partition = parse_csv_exact(partition, 2, "expected rollback partition")?;
            LedgerEvent::ExpectedRollback {
                meta: parse_meta(meta)?,
                warehouse_id: parse_u16("rollback warehouse_id", partition[0])?,
                district_id: parse_u8("rollback district_id", partition[1])?,
            }
        }
        [kind, ..] => {
            return Err(LedgerError::Malformed(format!(
                "unknown or malformed ledger event kind {kind:?}"
            )));
        }
        [] => {
            return Err(LedgerError::Malformed("empty ledger event".to_owned()));
        }
    };
    event.validate()?;
    Ok(event)
}

fn parse_meta(encoded: &str) -> Result<LedgerEventMeta, LedgerError> {
    let values = parse_csv_exact(encoded, 5, "event metadata")?;
    let class = parse_class(values[0])?;
    let stage = parse_u64("event stage", values[1])?;
    let client_id = parse_u16("event client_id", values[2])?;
    let txn_no = parse_u64("event txn_no", values[3])?;
    let parameter_fingerprint = parse_fixed_hex_u64("event parameter_fingerprint", values[4], 16)?;
    let meta = LedgerEventMeta {
        class,
        stage,
        client_id,
        txn_no,
        parameter_fingerprint,
    };
    meta.validate()?;
    Ok(meta)
}

fn parse_class(encoded: &str) -> Result<LedgerClass, LedgerError> {
    match encoded {
        "w" => Ok(LedgerClass::Warmup),
        "wt" => Ok(LedgerClass::WarmupTail),
        "n" => Ok(LedgerClass::NonRanked),
        "r0" => Ok(LedgerClass::RankedWindow(0)),
        "r1" => Ok(LedgerClass::RankedWindow(1)),
        "r2" => Ok(LedgerClass::RankedWindow(2)),
        "t0" => Ok(LedgerClass::RankedTail(0)),
        "t1" => Ok(LedgerClass::RankedTail(1)),
        "t2" => Ok(LedgerClass::RankedTail(2)),
        _ => Err(LedgerError::Malformed(format!(
            "unknown ledger class {encoded:?}"
        ))),
    }
}

fn parse_csv_exact<'a>(
    encoded: &'a str,
    expected: usize,
    field: &str,
) -> Result<Vec<&'a str>, LedgerError> {
    let values: Vec<_> = encoded.split(',').collect();
    if values.len() != expected || values.iter().any(|value| value.is_empty()) {
        return Err(LedgerError::Malformed(format!(
            "{field} must contain exactly {expected} non-empty values"
        )));
    }
    Ok(values)
}

fn parse_indexed_key(prefix: &str, key: &str) -> Result<usize, LedgerError> {
    let encoded = key
        .strip_prefix(prefix)
        .and_then(|suffix| suffix.strip_prefix('.'))
        .ok_or_else(|| LedgerError::UnknownField(key.to_owned()))?;
    let value = parse_u64("indexed field", encoded)?;
    usize::try_from(value).map_err(|_| LedgerError::Overflow("indexed field"))
}

fn parse_bits_exact(
    field: &str,
    encoded: &str,
    expected_count: i64,
) -> Result<Vec<u32>, LedgerError> {
    let expected_count =
        usize::try_from(expected_count).map_err(|_| LedgerError::Overflow("FLOAT32 count"))?;
    let expected_length = expected_count
        .checked_mul(8)
        .ok_or(LedgerError::Overflow("FLOAT32 encoded length"))?;
    if encoded.len() != expected_length {
        return Err(LedgerError::Inconsistent(format!(
            "{field} encoded length {} differs from expected {expected_length}",
            encoded.len()
        )));
    }
    if encoded.len() % 8 != 0
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(LedgerError::InvalidFloatBits {
            field: field.to_owned(),
            bits: abbreviated(encoded),
        });
    }
    let mut result = Vec::new();
    result
        .try_reserve(expected_count)
        .map_err(|_| LedgerError::Capacity("decoded FLOAT32 bits"))?;
    for chunk in encoded.as_bytes().chunks_exact(8) {
        let text = std::str::from_utf8(chunk).map_err(|_| LedgerError::InvalidFloatBits {
            field: field.to_owned(),
            bits: abbreviated(encoded),
        })?;
        let bits = u32::from_str_radix(text, 16).map_err(|_| LedgerError::InvalidFloatBits {
            field: field.to_owned(),
            bits: text.to_owned(),
        })?;
        result.push(bits);
    }
    validate_amount_bits(field, &result)?;
    Ok(result)
}

fn parse_one_bits(field: &str, encoded: &str) -> Result<u32, LedgerError> {
    parse_fixed_hex_u32(field, encoded, 8)
}

fn parse_fixed_hex_u32(field: &str, encoded: &str, width: usize) -> Result<u32, LedgerError> {
    if encoded.len() != width
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(LedgerError::InvalidFloatBits {
            field: field.to_owned(),
            bits: abbreviated(encoded),
        });
    }
    u32::from_str_radix(encoded, 16).map_err(|_| LedgerError::InvalidFloatBits {
        field: field.to_owned(),
        bits: abbreviated(encoded),
    })
}

fn parse_fixed_hex_u64(field: &str, encoded: &str, width: usize) -> Result<u64, LedgerError> {
    if encoded.len() != width
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(LedgerError::Malformed(format!(
            "{field} is not canonical lower-case hexadecimal"
        )));
    }
    u64::from_str_radix(encoded, 16)
        .map_err(|_| LedgerError::Malformed(format!("{field} is outside the supported range")))
}

fn abbreviated(encoded: &str) -> String {
    const LIMIT: usize = 64;
    if encoded.chars().count() <= LIMIT {
        encoded.to_owned()
    } else {
        let prefix: String = encoded.chars().take(LIMIT).collect();
        format!("{prefix}...({} bytes)", encoded.len())
    }
}

fn parse_u64(field: &str, encoded: &str) -> Result<u64, LedgerError> {
    validate_canonical_unsigned(field, encoded)?;
    encoded
        .parse()
        .map_err(|_| LedgerError::Malformed(format!("{field} is outside the supported range")))
}

fn parse_u32(field: &str, encoded: &str) -> Result<u32, LedgerError> {
    let value = parse_u64(field, encoded)?;
    u32::try_from(value)
        .map_err(|_| LedgerError::Malformed(format!("{field} is outside the supported range")))
}

fn parse_u16(field: &str, encoded: &str) -> Result<u16, LedgerError> {
    let value = parse_u64(field, encoded)?;
    u16::try_from(value)
        .map_err(|_| LedgerError::Malformed(format!("{field} is outside the supported range")))
}

fn parse_u8(field: &str, encoded: &str) -> Result<u8, LedgerError> {
    let value = parse_u64(field, encoded)?;
    u8::try_from(value)
        .map_err(|_| LedgerError::Malformed(format!("{field} is outside the supported range")))
}

fn parse_i32(field: &str, encoded: &str) -> Result<i32, LedgerError> {
    validate_canonical_unsigned(field, encoded)?;
    encoded
        .parse()
        .map_err(|_| LedgerError::Malformed(format!("{field} is outside the supported range")))
}

fn validate_canonical_unsigned(field: &str, encoded: &str) -> Result<(), LedgerError> {
    if encoded.is_empty()
        || (encoded.len() > 1 && encoded.starts_with('0'))
        || !encoded.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(LedgerError::Malformed(format!(
            "{field} is not a canonical non-negative integer"
        )));
    }
    Ok(())
}

fn parse_count(field: &str, encoded: &str) -> Result<i64, LedgerError> {
    if encoded.is_empty()
        || (encoded.len() > 1 && encoded.starts_with('0'))
        || !encoded.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(LedgerError::Malformed(format!(
            "{field} is not a canonical non-negative integer"
        )));
    }
    encoded.parse::<i64>().map_err(|_| {
        LedgerError::Malformed(format!("{field} is outside the supported counter range"))
    })
}

fn parse_partition_key(key: &str) -> Result<usize, LedgerError> {
    let mut components = key.split('.');
    if components.next() != Some("partition") {
        return Err(LedgerError::UnknownField(key.to_owned()));
    }
    let warehouse = components
        .next()
        .ok_or_else(|| LedgerError::Malformed(format!("bad partition key {key:?}")))?;
    let district = components
        .next()
        .ok_or_else(|| LedgerError::Malformed(format!("bad partition key {key:?}")))?;
    if components.next().is_some() {
        return Err(LedgerError::Malformed(format!("bad partition key {key:?}")));
    }
    let warehouse_id = parse_partition_component(key, warehouse)?;
    let district_id = parse_partition_component(key, district)?;
    partition_index(warehouse_id, district_id)
}

fn parse_partition_component(key: &str, encoded: &str) -> Result<i32, LedgerError> {
    if encoded.is_empty()
        || (encoded.len() > 1 && encoded.starts_with('0'))
        || !encoded.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(LedgerError::Malformed(format!("bad partition key {key:?}")));
    }
    encoded
        .parse()
        .map_err(|_| LedgerError::Malformed(format!("bad partition key {key:?}")))
}

fn parse_partition_delta(key: &str, encoded: &str) -> Result<PartitionDelta, LedgerError> {
    let mut values = encoded.split(',');
    let new_orders = parse_count(key, values.next().unwrap_or(""))?;
    let new_order_lines = parse_count(key, values.next().unwrap_or(""))?;
    let delivered_orders = parse_count(key, values.next().unwrap_or(""))?;
    let delivered_order_lines = parse_count(key, values.next().unwrap_or(""))?;
    if values.next().is_some() {
        return Err(LedgerError::Malformed(format!(
            "partition delta {key:?} must contain four counters"
        )));
    }
    let delta = PartitionDelta {
        new_orders,
        new_order_lines,
        delivered_orders,
        delivered_order_lines,
    };
    delta.validate()?;
    Ok(delta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::TransactionKind;
    use crate::ranking::runner::{DeliveredOrderEvidence, NewOrderEvidence, PaymentEvidence};
    use crate::routing::{ClientSequence, OfficialRouter, WorkloadSeed};
    use crate::workload::Final2026Workload;

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
        let amount_bits: Vec<_> = input
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
            line_amount_bits: amount_bits,
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

    #[test]
    fn records_exact_ticket_evidence_and_separates_physical_from_ranked() {
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
        let tail_delivery = ticket(
            TransactionKind::Delivery,
            None,
            StageId::measurement(0),
            2,
            12,
        );
        let ranked_order_status = ticket(
            TransactionKind::OrderStatus,
            None,
            StageId::measurement(1),
            3,
            13,
        );
        let ranked_stock = ticket(
            TransactionKind::StockLevel,
            None,
            StageId::measurement(2),
            4,
            14,
        );
        let rollback = ticket(
            TransactionKind::NewOrder,
            Some(true),
            StageId::measurement(0),
            5,
            15,
        );

        let mut ledger = RunLedger::default();
        ledger.record(&warmup_new, &new_order(&warmup_new)).unwrap();
        ledger
            .record(&ranked_payment, &payment(&ranked_payment))
            .unwrap();
        ledger
            .record_grace_tail(&tail_delivery, &delivery(&tail_delivery))
            .unwrap();
        ledger
            .record(
                &ranked_order_status,
                &RankedTransactionOutcome::Committed(RankedCommit::OrderStatus),
            )
            .unwrap();
        ledger
            .record(
                &ranked_stock,
                &RankedTransactionOutcome::Committed(RankedCommit::StockLevel {
                    low_stock_count: 9,
                }),
            )
            .unwrap();
        ledger
            .record(&rollback, &RankedTransactionOutcome::ExpectedRollback)
            .unwrap();

        assert_eq!(ledger.new_orders(), 1);
        let TransactionParameters::NewOrder(new_input) = warmup_new.parameters() else {
            unreachable!();
        };
        assert_eq!(ledger.new_order_lines(), new_input.lines().len() as i64);
        assert_eq!(ledger.payments(), 1);
        assert_eq!(ledger.delivery_commits(), 1);
        assert_eq!(ledger.delivered_orders(), 1);
        assert_eq!(ledger.delivered_order_lines(), 6);
        assert_eq!(ledger.order_status_commits(), 1);
        assert_eq!(ledger.stock_level_commits(), 1);
        assert_eq!(ledger.expected_rollbacks(), 1);
        let TransactionParameters::Payment(payment_input) = ranked_payment.parameters() else {
            unreachable!();
        };
        assert_eq!(ledger.payment_amount_bits(), &[payment_input.amount_bits()]);
        assert_eq!(
            ledger.delivery_customer_amount_bits(),
            &[99.25_f32.to_bits()]
        );
        assert_eq!(ledger.events().len(), 6);
        let LedgerEvent::NewOrder(delta) = &ledger.events()[0] else {
            panic!("NewOrder event");
        };
        assert_eq!(delta.customer_id, new_input.customer_id());
        assert_eq!(delta.lines[0].item_id, new_input.lines()[0].item_id());
        assert_eq!(
            delta.lines[0].supply_warehouse,
            new_input.lines()[0].supply_warehouse()
        );
        assert_eq!(delta.lines[0].quantity, new_input.lines()[0].quantity());

        let ranked = ledger.to_ranked_committed_ledger().unwrap();
        assert_eq!(ranked.new_orders, 0);
        assert_eq!(ranked.payments, 1);
        assert_eq!(ranked.delivered_orders, 0);
        assert_eq!(ledger.partition_deltas().len(), 500);
    }

    #[test]
    fn merge_combines_worker_ledgers_and_consistency_counts() {
        let first_new = ticket(
            TransactionKind::NewOrder,
            Some(false),
            StageId::measurement(0),
            0,
            21,
        );
        let second_new = ticket(
            TransactionKind::NewOrder,
            Some(false),
            StageId::measurement(0),
            1,
            22,
        );
        let payment_ticket = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(0),
            2,
            23,
        );
        let delivery_ticket = ticket(
            TransactionKind::Delivery,
            None,
            StageId::measurement(0),
            3,
            24,
        );
        let mut worker_zero = RunLedger::default();
        worker_zero
            .record(&first_new, &new_order(&first_new))
            .unwrap();
        worker_zero
            .record(&payment_ticket, &payment(&payment_ticket))
            .unwrap();

        let mut worker_one = RunLedger::default();
        worker_one
            .record(&second_new, &new_order(&second_new))
            .unwrap();
        worker_one
            .record(&delivery_ticket, &delivery(&delivery_ticket))
            .unwrap();

        let merged = RunLedger::merge_all([worker_zero, worker_one]).unwrap();
        assert_eq!(merged.new_orders(), 2);
        assert_eq!(merged.payments(), 1);
        assert_eq!(merged.delivered_orders(), 1);
        assert_eq!(merged.events().len(), 4);
        assert_eq!(merged.to_committed_ledger().delivered_order_lines, 6);
    }

    #[test]
    fn merge_rejects_duplicate_terminal_identity_atomically() {
        let payment_ticket = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(0),
            0,
            25,
        );
        let mut first = RunLedger::default();
        first
            .record(&payment_ticket, &payment(&payment_ticket))
            .unwrap();
        let duplicate = first.clone();
        let before = first.clone();
        assert!(matches!(
            first.merge(&duplicate),
            Err(LedgerError::Inconsistent(message))
                if message.contains("duplicate terminal event identity")
        ));
        assert_eq!(first, before);
    }

    #[test]
    fn versioned_text_round_trip_preserves_raw_bits_and_all_partitions() {
        let new_ticket = ticket(
            TransactionKind::NewOrder,
            Some(false),
            StageId::measurement(0),
            0,
            31,
        );
        let payment_ticket = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(1),
            1,
            32,
        );
        let delivery_ticket = ticket(
            TransactionKind::Delivery,
            None,
            StageId::measurement(2),
            2,
            33,
        );
        let mut ledger = RunLedger::default();
        ledger.record(&new_ticket, &new_order(&new_ticket)).unwrap();
        ledger
            .record(&payment_ticket, &payment(&payment_ticket))
            .unwrap();
        ledger
            .record_grace_tail(&delivery_ticket, &delivery(&delivery_ticket))
            .unwrap();

        let encoded = ledger.encode();
        assert!(encoded.starts_with("RMDB_TPCC_RUN_LEDGER_V2\n"));
        assert!(encoded.contains("event_count=3\n"));
        assert!(encoded.contains("event.0=N|"));
        assert!(encoded.contains("event.1=P|"));
        assert!(encoded.contains("event.2=D|"));
        assert_eq!(RunLedger::decode(&encoded).unwrap(), ledger);
        assert_eq!(RunLedger::decode(&encoded).unwrap().encode(), encoded);

        let old_header = encoded.replacen("RMDB_TPCC_RUN_LEDGER_V2", "RMDB_TPCC_RUN_LEDGER_V1", 1);
        assert!(matches!(
            RunLedger::decode(&old_header),
            Err(LedgerError::UnsupportedVersion(header))
                if header == "RMDB_TPCC_RUN_LEDGER_V1"
        ));
    }

    #[test]
    fn decode_rejects_duplicate_unknown_missing_and_bad_bits() {
        let new_ticket = ticket(
            TransactionKind::NewOrder,
            Some(false),
            StageId::measurement(0),
            0,
            41,
        );
        let mut ledger = RunLedger::default();
        ledger.record(&new_ticket, &new_order(&new_ticket)).unwrap();
        let encoded = ledger.encode();

        let duplicate = format!("{encoded}new_orders=1\n");
        assert!(matches!(
            RunLedger::decode(&duplicate),
            Err(LedgerError::DuplicateField(field)) if field == "new_orders"
        ));

        let unknown = encoded.replacen("new_orders=1", "mystery=1", 1);
        assert!(matches!(
            RunLedger::decode(&unknown),
            Err(LedgerError::UnknownField(field)) if field == "mystery"
        ));

        let missing = encoded.replacen("payments=0\n", "", 1);
        assert!(matches!(
            RunLedger::decode(&missing),
            Err(LedgerError::MissingField(field)) if field == "payments"
        ));

        let bad_bits = encoded.replacen(",3f800000", ",7f800000", 1);
        assert!(matches!(
            RunLedger::decode(&bad_bits),
            Err(LedgerError::InvalidFloatBits { .. })
        ));

        let missing_event = encoded
            .lines()
            .filter(|line| !line.starts_with("event.0="))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(RunLedger::decode(&missing_event).is_err());

        let missing_partition = encoded.replacen("partition.50.10=0,0,0,0\n", "", 1);
        assert!(matches!(
            RunLedger::decode(&missing_partition),
            Err(LedgerError::MissingField(field)) if field == "partition.50.10"
        ));
    }

    #[test]
    fn invalid_outcome_is_rejected_without_partial_mutation() {
        let payment_ticket = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(0),
            0,
            51,
        );
        let mut ledger = RunLedger::default();
        let before = ledger.clone();
        let invalid = RankedTransactionOutcome::Committed(RankedCommit::OrderStatus);
        assert!(ledger.record(&payment_ticket, &invalid).is_err());
        assert_eq!(ledger, before);
    }

    #[test]
    fn rejects_inexact_payment_and_delivery_relative_edges() {
        let payment_ticket = ticket(
            TransactionKind::Payment,
            None,
            StageId::measurement(0),
            0,
            52,
        );
        let mut invalid_payment = payment(&payment_ticket);
        let RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)) =
            &mut invalid_payment
        else {
            unreachable!();
        };
        evidence.warehouse_after_bits ^= 1;
        assert!(matches!(
            RunLedger::default().record(&payment_ticket, &invalid_payment),
            Err(LedgerError::Inconsistent(message))
                if message.contains("warehouse w_ytd")
        ));

        let mut invalid_payment_version = payment(&payment_ticket);
        let RankedTransactionOutcome::Committed(RankedCommit::Payment(evidence)) =
            &mut invalid_payment_version
        else {
            unreachable!();
        };
        evidence.customer_version_after.delivery_count += 1;
        assert!(matches!(
            RunLedger::default().record(&payment_ticket, &invalid_payment_version),
            Err(LedgerError::Inconsistent(message))
                if message.contains("c_delivery_cnt")
        ));

        let delivery_ticket = ticket(
            TransactionKind::Delivery,
            None,
            StageId::measurement(0),
            1,
            53,
        );
        let mut invalid_delivery = delivery(&delivery_ticket);
        let RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)) =
            &mut invalid_delivery
        else {
            unreachable!();
        };
        orders[0].customer_version_after.delivery_count = 2;
        assert!(matches!(
            RunLedger::default().record(&delivery_ticket, &invalid_delivery),
            Err(LedgerError::Inconsistent(message))
                if message.contains("c_delivery_cnt")
        ));

        let mut invalid_delivery_version = delivery(&delivery_ticket);
        let RankedTransactionOutcome::Committed(RankedCommit::Delivery(orders)) =
            &mut invalid_delivery_version
        else {
            unreachable!();
        };
        orders[0].customer_version_after.payment_count += 1;
        assert!(matches!(
            RunLedger::default().record(&delivery_ticket, &invalid_delivery_version),
            Err(LedgerError::Inconsistent(message))
                if message.contains("c_payment_cnt")
        ));
    }

    #[test]
    fn counter_and_partition_arithmetic_reject_overflow() {
        assert!(matches!(
            checked_add(i64::MAX, 1, "test"),
            Err(LedgerError::Overflow("test"))
        ));
        assert!(matches!(
            PartitionDelta {
                new_orders: i64::MAX,
                ..PartitionDelta::default()
            }
            .checked_add(PartitionDelta {
                new_orders: 1,
                ..PartitionDelta::default()
            }),
            Err(LedgerError::Overflow("partition new_orders"))
        ));

        let encoded =
            RunLedger::default()
                .encode()
                .replacen("payments=0", "payments=9223372036854775808", 1);
        assert!(matches!(
            RunLedger::decode(&encoded),
            Err(LedgerError::Malformed(message))
                if message.contains("supported counter range")
        ));
    }
}
