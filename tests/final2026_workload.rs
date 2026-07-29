#[path = "../src/profile.rs"]
mod profile;
#[path = "../src/routing.rs"]
mod routing;
#[path = "../src/workload.rs"]
mod workload;

use std::collections::HashMap;

use profile::{TransactionKind, HOT_DISTRICT_PERCENT, HOT_ITEM_PERCENT, NEW_ORDER_REMOTE_PERCENT};
use routing::{ClientSequence, OfficialRouter, StageId, WorkloadSeed};
use workload::{
    CustomerSelector, Final2026Workload, TransactionParameters, CUSTOMERS_PER_DISTRICT,
    CUSTOMER_LAST_NAME_PERCENT, INVALID_ITEM_ID, MAX_CARRIER_ID, MAX_ITEM_QUANTITY,
    MAX_ORDER_LINES, MAX_PAYMENT_CENTS, MAX_STOCK_THRESHOLD, MIN_CARRIER_ID, MIN_ITEM_QUANTITY,
    MIN_ORDER_LINES, MIN_PAYMENT_CENTS, MIN_STOCK_THRESHOLD,
};

#[test]
fn retry_reuses_the_exact_selection_without_consuming_txn_no() {
    let router = OfficialRouter::new(WorkloadSeed(0x2026_0729));
    let wheel = router.wheel(StageId::measurement(0));
    let workload = Final2026Workload::new(&router, &wheel);
    let mut sequence = ClientSequence::new(11).unwrap();

    let selected = workload.select(&mut sequence).unwrap();
    assert_eq!(selected.route().txn_no, 0);
    assert_eq!(sequence.next_txn_no(), 1);

    let retry = selected.retry();
    assert_eq!(sequence.next_txn_no(), 1);
    assert_eq!(selected, retry);
    assert!(selected.shares_selection_with(&retry));

    let next = workload.select(&mut sequence).unwrap();
    assert_eq!(next.route().txn_no, 1);
    assert_eq!(sequence.next_txn_no(), 2);
    assert!(!selected.shares_selection_with(&next));
}

#[test]
fn same_seed_reproduces_every_parameter_bit() {
    let left_router = OfficialRouter::new(WorkloadSeed(91_827_364));
    let right_router = OfficialRouter::new(WorkloadSeed(91_827_364));
    let left_wheel = left_router.wheel(StageId::measurement(2));
    let right_wheel = right_router.wheel(StageId::measurement(2));
    let left = Final2026Workload::new(&left_router, &left_wheel);
    let right = Final2026Workload::new(&right_router, &right_wheel);

    for client_id in 0..profile::OFFICIAL_CLIENTS {
        let mut left_sequence = ClientSequence::new(client_id).unwrap();
        let mut right_sequence = ClientSequence::new(client_id).unwrap();
        for _ in 0..200 {
            assert_eq!(
                left.select(&mut left_sequence).unwrap(),
                right.select(&mut right_sequence).unwrap()
            );
        }
    }
}

#[test]
fn every_generated_parameter_stays_in_the_public_domain() {
    let router = OfficialRouter::new(WorkloadSeed(0xfeed_face_cafe_beef));
    let wheel = router.wheel(StageId::measurement(1));
    let workload = Final2026Workload::new(&router, &wheel);

    for client_id in 0..profile::OFFICIAL_CLIENTS {
        let mut sequence = ClientSequence::new(client_id).unwrap();
        for _ in 0..2_000 {
            let ticket = workload.select(&mut sequence).unwrap();
            let route = ticket.route();
            assert!((1..=profile::OFFICIAL_WAREHOUSES).contains(&route.home_warehouse));
            assert!((1..=profile::DISTRICTS_PER_WAREHOUSE).contains(&route.home_district));
            assert_eq!(ticket.kind(), ticket.parameters().kind());

            match ticket.parameters() {
                TransactionParameters::NewOrder(input) => {
                    assert!((1..=CUSTOMERS_PER_DISTRICT).contains(&input.customer_id()));
                    assert!(
                        (MIN_ORDER_LINES..=MAX_ORDER_LINES).contains(&(input.lines().len() as u8))
                    );
                    assert_eq!(
                        input.all_local(),
                        input
                            .lines()
                            .iter()
                            .all(|line| line.supply_warehouse() == route.home_warehouse)
                    );
                    for (index, line) in input.lines().iter().enumerate() {
                        assert_eq!(usize::from(line.number()), index + 1);
                        assert!(
                            (1..=profile::OFFICIAL_WAREHOUSES).contains(&line.supply_warehouse())
                        );
                        assert!((MIN_ITEM_QUANTITY..=MAX_ITEM_QUANTITY).contains(&line.quantity()));
                        if line.is_invalid_item() {
                            assert!(input.expected_rollback());
                            assert_eq!(line.item_id(), INVALID_ITEM_ID);
                            assert_eq!(index + 1, input.lines().len());
                        } else {
                            assert!((1..=profile::ITEM_COUNT).contains(&line.item_id()));
                        }
                    }
                    assert_eq!(
                        input.expected_rollback(),
                        input
                            .lines()
                            .last()
                            .is_some_and(|line| line.is_invalid_item())
                    );
                }
                TransactionParameters::Payment(input) => {
                    assert!(
                        (1..=profile::OFFICIAL_WAREHOUSES).contains(&input.customer_warehouse())
                    );
                    assert!(
                        (1..=profile::DISTRICTS_PER_WAREHOUSE).contains(&input.customer_district())
                    );
                    if input.customer_warehouse() == route.home_warehouse {
                        assert_eq!(input.customer_district(), route.home_district);
                    }
                    validate_customer(input.customer());
                    assert!((MIN_PAYMENT_CENTS..=MAX_PAYMENT_CENTS).contains(&input.amount_cents()));
                    assert_eq!(
                        input.amount_bits(),
                        (input.amount_cents() as f32 / 100.0_f32).to_bits()
                    );
                    assert!(input.amount().is_finite());
                }
                TransactionParameters::OrderStatus(input) => {
                    validate_customer(input.customer());
                }
                TransactionParameters::Delivery(input) => {
                    assert!((MIN_CARRIER_ID..=MAX_CARRIER_ID).contains(&input.carrier_id()));
                }
                TransactionParameters::StockLevel(input) => {
                    assert!(
                        (MIN_STOCK_THRESHOLD..=MAX_STOCK_THRESHOLD).contains(&input.threshold())
                    );
                }
            }
        }
    }
}

#[test]
fn deterministic_sampling_tracks_the_published_probabilities() {
    let router = OfficialRouter::new(WorkloadSeed(0x5eed_2026_d15c_0a11));
    let wheel = router.wheel(StageId::measurement(0));
    let workload = Final2026Workload::new(&router, &wheel);
    let mut counts = DistributionCounts::default();

    for client_id in 0..profile::OFFICIAL_CLIENTS {
        let mut sequence = ClientSequence::new(client_id).unwrap();
        for _ in 0..10_000 {
            let ticket = workload.select(&mut sequence).unwrap();
            let route = ticket.route();
            *counts.kinds.entry(ticket.kind()).or_default() += 1;

            if let Some(hot_district) = router.hot_district_for(route.home_warehouse) {
                counts.hot_warehouse_transactions += 1;
                counts.hot_district_transactions += u64::from(route.home_district == hot_district);
            }

            match ticket.parameters() {
                TransactionParameters::NewOrder(input) => {
                    counts.new_orders += 1;
                    counts.expected_rollbacks += u64::from(input.expected_rollback());
                    for line in input.lines() {
                        if !line.is_invalid_item() {
                            counts.valid_lines += 1;
                            counts.hot_item_lines +=
                                u64::from(router.hot_items().contains(&line.item_id()));
                        }
                        counts.all_lines += 1;
                        counts.remote_supply_lines +=
                            u64::from(line.supply_warehouse() != route.home_warehouse);
                    }
                }
                TransactionParameters::Payment(input) => {
                    counts.payments += 1;
                    counts.remote_payments +=
                        u64::from(input.customer_warehouse() != route.home_warehouse);
                    counts.payment_last_names +=
                        u64::from(matches!(input.customer(), CustomerSelector::LastName(_)));
                }
                TransactionParameters::OrderStatus(input) => {
                    counts.order_statuses += 1;
                    counts.order_status_last_names +=
                        u64::from(matches!(input.customer(), CustomerSelector::LastName(_)));
                }
                TransactionParameters::Delivery(_) | TransactionParameters::StockLevel(_) => {}
            }
        }
    }

    let total: u64 = counts.kinds.values().sum();
    assert_percent(counts.kinds[&TransactionKind::NewOrder], total, 45.0, 0.35);
    assert_percent(counts.kinds[&TransactionKind::Payment], total, 43.0, 0.35);
    for kind in [
        TransactionKind::OrderStatus,
        TransactionKind::Delivery,
        TransactionKind::StockLevel,
    ] {
        assert_percent(counts.kinds[&kind], total, 4.0, 0.18);
    }
    assert_percent(
        counts.hot_district_transactions,
        counts.hot_warehouse_transactions,
        f64::from(HOT_DISTRICT_PERCENT),
        0.6,
    );
    assert_percent(
        counts.hot_item_lines,
        counts.valid_lines,
        f64::from(HOT_ITEM_PERCENT),
        0.35,
    );
    assert_percent(
        counts.remote_supply_lines,
        counts.all_lines,
        f64::from(NEW_ORDER_REMOTE_PERCENT),
        0.25,
    );
    assert_percent(counts.expected_rollbacks, counts.new_orders, 1.0, 0.12);
    assert_percent(counts.remote_payments, counts.payments, 30.0, 0.55);
    assert_percent(
        counts.payment_last_names,
        counts.payments,
        f64::from(CUSTOMER_LAST_NAME_PERCENT),
        0.55,
    );
    assert_percent(
        counts.order_status_last_names,
        counts.order_statuses,
        f64::from(CUSTOMER_LAST_NAME_PERCENT),
        1.0,
    );
}

fn validate_customer(customer: &CustomerSelector) {
    match customer {
        CustomerSelector::Id(customer_id) => {
            assert!((1..=CUSTOMERS_PER_DISTRICT).contains(customer_id));
        }
        CustomerSelector::LastName(last_name) => {
            assert!(last_name.number() < workload::CUSTOMER_LAST_NAMES);
            assert!(!last_name.value().is_empty());
            assert!(last_name.value().len() <= 16);
        }
    }
}

fn assert_percent(observed: u64, total: u64, expected: f64, tolerance: f64) {
    let actual = observed as f64 * 100.0 / total as f64;
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected:.2}% ± {tolerance:.2}%, observed {actual:.4}% ({observed}/{total})"
    );
}

#[derive(Default)]
struct DistributionCounts {
    kinds: HashMap<TransactionKind, u64>,
    hot_warehouse_transactions: u64,
    hot_district_transactions: u64,
    new_orders: u64,
    expected_rollbacks: u64,
    all_lines: u64,
    valid_lines: u64,
    hot_item_lines: u64,
    remote_supply_lines: u64,
    payments: u64,
    remote_payments: u64,
    payment_last_names: u64,
    order_statuses: u64,
    order_status_last_names: u64,
}
