//! Immutable parameter selection for the public 2026 final TPC-C workload.
//!
//! This module deliberately owns no mutable RNG. Every field is derived from
//! the routed transaction ticket through an independent random domain. A retry
//! clones the same `Arc`, so it cannot consume another transaction number or
//! silently choose different parameters.

use std::sync::Arc;

use crate::profile::{TransactionKind, ITEM_COUNT};
use crate::routing::{
    ClientSequence, OfficialRouter, RouteError, RoutedTransaction, WarehouseWheel,
};

pub const CUSTOMERS_PER_DISTRICT: u16 = 3_000;
pub const CUSTOMER_LAST_NAMES: u16 = 1_000;
pub const MIN_ORDER_LINES: u8 = 5;
pub const MAX_ORDER_LINES: u8 = 15;
pub const MIN_ITEM_QUANTITY: u8 = 1;
pub const MAX_ITEM_QUANTITY: u8 = 10;
pub const INVALID_ITEM_PERCENT: u8 = 1;
pub const INVALID_ITEM_ID: u32 = ITEM_COUNT + 1;
pub const CUSTOMER_LAST_NAME_PERCENT: u8 = 60;
pub const MIN_PAYMENT_CENTS: u32 = 100;
pub const MAX_PAYMENT_CENTS: u32 = 500_000;
pub const MIN_CARRIER_ID: u8 = 1;
pub const MAX_CARRIER_ID: u8 = 10;
pub const MIN_STOCK_THRESHOLD: u8 = 10;
pub const MAX_STOCK_THRESHOLD: u8 = 20;

const LAST_NAME_SYLLABLES: [&str; 10] = [
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

#[derive(Debug)]
pub struct Final2026Workload<'a> {
    router: &'a OfficialRouter,
    wheel: &'a WarehouseWheel,
}

impl<'a> Final2026Workload<'a> {
    pub fn new(router: &'a OfficialRouter, wheel: &'a WarehouseWheel) -> Self {
        Self { router, wheel }
    }

    /// Selects and freezes one complete transaction input.
    ///
    /// `OfficialRouter::begin_transaction` is the sole point at which the
    /// stage-local sequence advances. Keep the returned ticket for all retries.
    pub fn select(&self, sequence: &mut ClientSequence) -> Result<TransactionTicket, RouteError> {
        let route = self.router.begin_transaction(self.wheel, sequence)?;
        let parameters = parameters_for(&route);
        debug_assert_eq!(route.kind, parameters.kind());
        Ok(TransactionTicket(Arc::new(SelectedTransaction {
            route,
            parameters,
        })))
    }
}

#[derive(Debug, Clone)]
pub struct TransactionTicket(Arc<SelectedTransaction>);

impl TransactionTicket {
    /// Returns a retry ticket backed by the exact same frozen selection.
    pub fn retry(&self) -> Self {
        Self(Arc::clone(&self.0))
    }

    pub fn shares_selection_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn route(&self) -> &RoutedTransaction {
        &self.0.route
    }

    pub fn kind(&self) -> TransactionKind {
        self.0.route.kind
    }

    pub fn parameters(&self) -> &TransactionParameters {
        &self.0.parameters
    }

    /// Stable digest of every routed coordinate and bound parameter.
    ///
    /// The phase scheduler uses this only as an immutable retry identity; it is
    /// not a random seed and is never used to generate later parameters.
    pub fn parameter_fingerprint(&self) -> u64 {
        parameter_fingerprint(&self.0)
    }
}

impl PartialEq for TransactionTicket {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for TransactionTicket {}

#[derive(Debug, PartialEq, Eq)]
struct SelectedTransaction {
    route: RoutedTransaction,
    parameters: TransactionParameters,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionParameters {
    NewOrder(NewOrderInput),
    Payment(PaymentInput),
    OrderStatus(OrderStatusInput),
    Delivery(DeliveryInput),
    StockLevel(StockLevelInput),
}

impl TransactionParameters {
    pub fn kind(&self) -> TransactionKind {
        match self {
            Self::NewOrder(_) => TransactionKind::NewOrder,
            Self::Payment(_) => TransactionKind::Payment,
            Self::OrderStatus(_) => TransactionKind::OrderStatus,
            Self::Delivery(_) => TransactionKind::Delivery,
            Self::StockLevel(_) => TransactionKind::StockLevel,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOrderInput {
    customer_id: u16,
    lines: Box<[NewOrderLineInput]>,
    expected_rollback: bool,
    all_local: bool,
}

impl NewOrderInput {
    pub fn customer_id(&self) -> u16 {
        self.customer_id
    }

    pub fn lines(&self) -> &[NewOrderLineInput] {
        &self.lines
    }

    pub fn expected_rollback(&self) -> bool {
        self.expected_rollback
    }

    pub fn all_local(&self) -> bool {
        self.all_local
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NewOrderLineInput {
    number: u8,
    item_id: u32,
    supply_warehouse: u16,
    quantity: u8,
}

impl NewOrderLineInput {
    pub fn number(&self) -> u8 {
        self.number
    }

    pub fn item_id(&self) -> u32 {
        self.item_id
    }

    pub fn supply_warehouse(&self) -> u16 {
        self.supply_warehouse
    }

    pub fn quantity(&self) -> u8 {
        self.quantity
    }

    pub fn is_invalid_item(&self) -> bool {
        self.item_id == INVALID_ITEM_ID
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentInput {
    customer_warehouse: u16,
    customer_district: u8,
    customer: CustomerSelector,
    amount_cents: u32,
    amount_bits: u32,
}

impl PaymentInput {
    pub fn customer_warehouse(&self) -> u16 {
        self.customer_warehouse
    }

    pub fn customer_district(&self) -> u8 {
        self.customer_district
    }

    pub fn customer(&self) -> &CustomerSelector {
        &self.customer
    }

    pub fn amount_cents(&self) -> u32 {
        self.amount_cents
    }

    /// Returns the already-rounded binary32 value bound by the transaction.
    pub fn amount(&self) -> f32 {
        f32::from_bits(self.amount_bits)
    }

    pub fn amount_bits(&self) -> u32 {
        self.amount_bits
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderStatusInput {
    customer: CustomerSelector,
}

impl OrderStatusInput {
    pub fn customer(&self) -> &CustomerSelector {
        &self.customer
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryInput {
    carrier_id: u8,
}

impl DeliveryInput {
    pub fn carrier_id(&self) -> u8 {
        self.carrier_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StockLevelInput {
    threshold: u8,
}

impl StockLevelInput {
    pub fn threshold(&self) -> u8 {
        self.threshold
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CustomerSelector {
    Id(u16),
    LastName(CustomerLastName),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CustomerLastName {
    number: u16,
    value: String,
}

impl CustomerLastName {
    pub fn number(&self) -> u16 {
        self.number
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

fn parameters_for(route: &RoutedTransaction) -> TransactionParameters {
    match route.kind {
        TransactionKind::NewOrder => TransactionParameters::NewOrder(new_order_input(route)),
        TransactionKind::Payment => TransactionParameters::Payment(payment_input(route)),
        TransactionKind::OrderStatus => TransactionParameters::OrderStatus(OrderStatusInput {
            customer: customer_selector(route, "parameter/order-status/customer"),
        }),
        TransactionKind::Delivery => TransactionParameters::Delivery(DeliveryInput {
            carrier_id: inclusive_u8(
                route,
                "parameter/delivery/carrier-id",
                0,
                MIN_CARRIER_ID,
                MAX_CARRIER_ID,
            ),
        }),
        TransactionKind::StockLevel => TransactionParameters::StockLevel(StockLevelInput {
            threshold: inclusive_u8(
                route,
                "parameter/stock-level/threshold",
                0,
                MIN_STOCK_THRESHOLD,
                MAX_STOCK_THRESHOLD,
            ),
        }),
    }
}

fn new_order_input(route: &RoutedTransaction) -> NewOrderInput {
    let customer_id = route.customer_id("parameter/new-order/customer-id", 0);
    let line_count = inclusive_u8(
        route,
        "parameter/new-order/line-count",
        0,
        MIN_ORDER_LINES,
        MAX_ORDER_LINES,
    );
    let expected_rollback = chance(
        route,
        "parameter/new-order/invalid-item",
        0,
        INVALID_ITEM_PERCENT,
    );

    let mut lines = Vec::with_capacity(usize::from(line_count));
    for number in 1..=line_count {
        let is_invalid_last = expected_rollback && number == line_count;
        lines.push(NewOrderLineInput {
            number,
            item_id: if is_invalid_last {
                INVALID_ITEM_ID
            } else {
                route.item_id(number)
            },
            supply_warehouse: route.new_order_supply_warehouse(number),
            quantity: inclusive_u8(
                route,
                "parameter/new-order/quantity",
                u64::from(number),
                MIN_ITEM_QUANTITY,
                MAX_ITEM_QUANTITY,
            ),
        });
    }
    let all_local = lines
        .iter()
        .all(|line| line.supply_warehouse == route.home_warehouse);

    NewOrderInput {
        customer_id,
        lines: lines.into_boxed_slice(),
        expected_rollback,
        all_local,
    }
}

fn payment_input(route: &RoutedTransaction) -> PaymentInput {
    let customer_warehouse = route.payment_customer_warehouse;
    let customer_district = if customer_warehouse == route.home_warehouse {
        route.home_district
    } else {
        inclusive_u8(
            route,
            "parameter/payment/customer-district",
            0,
            1,
            crate::profile::DISTRICTS_PER_WAREHOUSE,
        )
    };
    let amount_cents = inclusive_u32(
        route,
        "parameter/payment/amount-cents",
        0,
        MIN_PAYMENT_CENTS,
        MAX_PAYMENT_CENTS,
    );
    // amount_cents is exactly representable. This division performs the one
    // required round-to-nearest-even conversion to the bound binary32 amount.
    let amount = amount_cents as f32 / 100.0_f32;

    PaymentInput {
        customer_warehouse,
        customer_district,
        customer: customer_selector(route, "parameter/payment/customer"),
        amount_cents,
        amount_bits: amount.to_bits(),
    }
}

fn customer_selector(route: &RoutedTransaction, domain: &'static str) -> CustomerSelector {
    // TPC-C 5.11: 1% of customer lookups (Payment/OrderStatus) target the
    // load-time constant last name (c_last_load), exercising the hot
    // last-name lookup path.
    if chance(route, domain, 0, 1) {
        let load = route.nurand_constants().c_last_load();
        return CustomerSelector::LastName(CustomerLastName {
            number: load,
            value: last_name(load),
        });
    }
    if chance(route, domain, 0, CUSTOMER_LAST_NAME_PERCENT) {
        let number = route.customer_last_name_number(domain, 1);
        CustomerSelector::LastName(CustomerLastName {
            number,
            value: last_name(number),
        })
    } else {
        CustomerSelector::Id(route.customer_id(domain, 3))
    }
}

fn last_name(number: u16) -> String {
    let number = usize::from(number);
    format!(
        "{}{}{}",
        LAST_NAME_SYLLABLES[number / 100],
        LAST_NAME_SYLLABLES[(number / 10) % 10],
        LAST_NAME_SYLLABLES[number % 10]
    )
}

fn parameter_fingerprint(selected: &SelectedTransaction) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    fn write(state: &mut u64, bytes: &[u8]) {
        for byte in bytes {
            *state ^= u64::from(*byte);
            *state = state.wrapping_mul(PRIME);
        }
    }

    fn write_u64(state: &mut u64, value: u64) {
        write(state, &value.to_be_bytes());
    }

    fn write_selector(state: &mut u64, selector: &CustomerSelector) {
        match selector {
            CustomerSelector::Id(id) => {
                write_u64(state, 0);
                write_u64(state, u64::from(*id));
            }
            CustomerSelector::LastName(last) => {
                write_u64(state, 1);
                write_u64(state, u64::from(last.number));
                write(state, last.value.as_bytes());
            }
        }
    }

    let route = &selected.route;
    let mut state = OFFSET;
    for value in [
        route.stage.value(),
        u64::from(route.client_id),
        route.txn_no,
        u64::from(route.home_warehouse),
        u64::from(route.home_district),
        u64::from(route.payment_customer_warehouse),
        route.kind as u64,
    ] {
        write_u64(&mut state, value);
    }

    match &selected.parameters {
        TransactionParameters::NewOrder(input) => {
            write_u64(&mut state, u64::from(input.customer_id));
            write_u64(&mut state, input.expected_rollback as u64);
            write_u64(&mut state, input.all_local as u64);
            write_u64(&mut state, input.lines.len() as u64);
            for line in input.lines.iter() {
                write_u64(&mut state, u64::from(line.number));
                write_u64(&mut state, u64::from(line.item_id));
                write_u64(&mut state, u64::from(line.supply_warehouse));
                write_u64(&mut state, u64::from(line.quantity));
            }
        }
        TransactionParameters::Payment(input) => {
            write_u64(&mut state, u64::from(input.customer_warehouse));
            write_u64(&mut state, u64::from(input.customer_district));
            write_selector(&mut state, &input.customer);
            write_u64(&mut state, u64::from(input.amount_cents));
            write_u64(&mut state, u64::from(input.amount_bits));
        }
        TransactionParameters::OrderStatus(input) => {
            write_selector(&mut state, &input.customer);
        }
        TransactionParameters::Delivery(input) => {
            write_u64(&mut state, u64::from(input.carrier_id));
        }
        TransactionParameters::StockLevel(input) => {
            write_u64(&mut state, u64::from(input.threshold));
        }
    }
    state
}

fn chance(route: &RoutedTransaction, domain: &'static str, ordinal: u64, percent: u8) -> bool {
    route.parameter_sample(domain, ordinal, 100) < u64::from(percent)
}

fn inclusive_u8(
    route: &RoutedTransaction,
    domain: &'static str,
    ordinal: u64,
    minimum: u8,
    maximum: u8,
) -> u8 {
    minimum + route.parameter_sample(domain, ordinal, u64::from(maximum - minimum) + 1) as u8
}

fn inclusive_u32(
    route: &RoutedTransaction,
    domain: &'static str,
    ordinal: u64,
    minimum: u32,
    maximum: u32,
) -> u32 {
    minimum + route.parameter_sample(domain, ordinal, u64::from(maximum - minimum) + 1) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn customer_last_names_match_the_loaded_public_name_domain() {
        assert_eq!(last_name(0), "BARBARBAR");
        assert_eq!(last_name(255), "ABLEESEESE");
        assert_eq!(last_name(999), "EINGEINGEING");
    }

    #[test]
    fn amount_is_frozen_as_binary32() {
        for amount_cents in [100, 101, 99_999, 500_000] {
            let amount = amount_cents as f32 / 100.0_f32;
            assert_eq!(f32::from_bits(amount.to_bits()).to_bits(), amount.to_bits());
        }
    }

    #[test]
    fn parameter_fingerprint_is_stable_for_the_exact_retry_ticket() {
        let router = OfficialRouter::new(crate::routing::WorkloadSeed(73));
        let wheel = router.wheel(crate::routing::StageId::measurement(1));
        let workload = Final2026Workload::new(&router, &wheel);
        let mut sequence = ClientSequence::new(5).unwrap();

        let first = workload.select(&mut sequence).unwrap();
        let retry = first.retry();
        let second = workload.select(&mut sequence).unwrap();

        assert_eq!(first.parameter_fingerprint(), retry.parameter_fingerprint());
        assert_ne!(
            first.parameter_fingerprint(),
            second.parameter_fingerprint()
        );
    }
}
