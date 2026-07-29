#[path = "../src/profile.rs"]
mod profile;
#[path = "../src/routing.rs"]
mod routing;

use profile::{Final2026Profile, MEASUREMENT_WINDOWS};
use routing::{ClientSequence, OfficialRouter, StageId, WorkloadSeed};
use std::collections::HashSet;

#[test]
fn public_profile_and_router_form_a_reproducible_stage_contract() {
    let profile = Final2026Profile::official();
    assert!(profile.is_ranked_configuration());

    let router = OfficialRouter::new(WorkloadSeed(2026));
    for window in 0..MEASUREMENT_WINDOWS {
        let wheel = router.wheel(StageId::measurement(window));
        for client_id in 0..profile.clients {
            let mut sequence = ClientSequence::new(client_id).unwrap();
            let first = router.begin_transaction(&wheel, &mut sequence).unwrap();
            assert_eq!(first.txn_no, 0);
            assert_eq!(sequence.next_txn_no(), 1);
        }
    }
}

#[test]
fn deviation_smoke_routing_is_bounded_reproducible_and_single_warehouse_safe() {
    assert!(OfficialRouter::new_for_warehouses(WorkloadSeed(2026), 0).is_err());
    assert!(OfficialRouter::new_for_warehouses(
        WorkloadSeed(2026),
        profile::OFFICIAL_WAREHOUSES + 1
    )
    .is_err());

    for warehouse_count in [1_u16, 2, 4, 7, 49] {
        let left = OfficialRouter::new_for_warehouses(WorkloadSeed(2026), warehouse_count).unwrap();
        let right =
            OfficialRouter::new_for_warehouses(WorkloadSeed(2026), warehouse_count).unwrap();
        assert_eq!(left.warehouse_count(), warehouse_count);
        assert_eq!(left.hot_warehouses(), right.hot_warehouses());

        let hot: HashSet<_> = left.hot_warehouses().iter().copied().collect();
        assert_eq!(
            hot.len(),
            usize::from(warehouse_count).min(profile::HOT_WAREHOUSES)
        );
        assert!(hot
            .iter()
            .all(|warehouse| (1..=warehouse_count).contains(warehouse)));

        for stage in [StageId::WARMUP, StageId::measurement(0)] {
            let left_wheel = left.wheel(stage);
            let right_wheel = right.wheel(stage);
            assert_eq!(left_wheel, right_wheel);
            assert!(left_wheel
                .slots()
                .iter()
                .all(|warehouse| (1..=warehouse_count).contains(warehouse)));
            assert!(left_wheel
                .extra_cold_warehouses()
                .iter()
                .all(|warehouse| (1..=warehouse_count).contains(warehouse)));

            let hot_slots = left_wheel
                .slots()
                .iter()
                .filter(|warehouse| hot.contains(warehouse))
                .count();
            assert_eq!(
                hot_slots,
                if warehouse_count <= profile::HOT_WAREHOUSES as u16 {
                    profile::ROUTING_SLOTS
                } else {
                    profile::HOT_WAREHOUSES * profile::HOT_SLOTS_PER_WAREHOUSE
                }
            );

            for client_id in 0..profile::OFFICIAL_CLIENTS {
                let mut sequence = ClientSequence::new(client_id).unwrap();
                for _ in 0..64 {
                    let transaction = left.begin_transaction(&left_wheel, &mut sequence).unwrap();
                    assert!((1..=warehouse_count).contains(&transaction.home_warehouse));
                    assert!((1..=warehouse_count).contains(&transaction.payment_customer_warehouse));
                    for line_number in 1..=15 {
                        assert!((1..=warehouse_count)
                            .contains(&transaction.new_order_supply_warehouse(line_number)));
                    }
                    if warehouse_count == 1 {
                        assert_eq!(transaction.payment_customer_warehouse, 1);
                        assert_eq!(transaction.new_order_supply_warehouse(1), 1);
                    }
                }
            }
        }
    }
}

#[test]
fn explicit_fifty_warehouse_router_matches_the_official_constructor() {
    let seed = WorkloadSeed(0x2026_cafe_f00d);
    let official = OfficialRouter::new(seed);
    let explicit = OfficialRouter::new_for_warehouses(seed, profile::OFFICIAL_WAREHOUSES).unwrap();

    assert_eq!(official.hot_warehouses(), explicit.hot_warehouses());
    assert_eq!(official.hot_items(), explicit.hot_items());
    assert_eq!(official.nurand_constants(), explicit.nurand_constants());
    for stage in [StageId::WARMUP, StageId::measurement(0)] {
        assert_eq!(official.wheel(stage), explicit.wheel(stage));
    }
}
