#[path = "../src/consistency.rs"]
mod consistency;

use consistency::{
    add_f32_once, float32_matches, float_aggregate_plan, large_set_boundary,
    large_set_boundary_from_f32, public_online_integer_plan, recovery_partition_audits,
    recovery_plan, setup_plan, sum_f32_as_f64_once, ulp_distance, validate_crash_float_baseline,
    validate_public_float_ledger, validate_relative_add, validate_relative_update_chain,
    CheckScope, CommittedLedger, FloatAggregateId, IdentifierMap, LedgerFloatRule,
    PartitionExpectation, PartitionKey, PublicFloatLedgerEvidence, RecoveryExpectations,
    RelativeUpdateEvidence, ScalarExpectation, SetupExpectations, TypedResult, TypedValue,
    FLOAT_AGGREGATES, PUBLIC_SPEC_NOTICE,
};

fn expectation(plan: &consistency::ConsistencyPlan, id: &str) -> ScalarExpectation {
    plan.queries
        .iter()
        .find(|query| query.id == id)
        .unwrap_or_else(|| panic!("missing check {id}"))
        .expectation
        .clone()
}

#[test]
fn setup_plan_uses_exact_final_counts_and_dynamic_line_answers() {
    let setup = SetupExpectations::final_2026(15_123_456, 4_571_234);
    let plan = setup_plan(setup).unwrap();

    let expected_counts = [
        ("warehouse", 50),
        ("district", 500),
        ("customer", 1_500_000),
        ("history", 1_500_000),
        ("orders", 1_500_000),
        ("new_orders", 450_000),
        ("order_line", 15_123_456),
        ("item", 100_000),
        ("stock", 5_000_000),
    ];
    for (table, count) in expected_counts {
        assert_eq!(
            expectation(&plan, &format!("setup.count.{table}")),
            ScalarExpectation::ExactInt(count)
        );
    }
    assert_eq!(
        expectation(&plan, "setup.orders.sum_o_ol_cnt"),
        ScalarExpectation::ExactInt(15_123_456)
    );
    assert_eq!(
        expectation(&plan, "setup.order_line.undelivered_count"),
        ScalarExpectation::ExactInt(4_571_234)
    );

    let fixed_rows: i64 = expected_counts
        .iter()
        .filter(|(table, _)| *table != "order_line")
        .map(|(_, count)| count)
        .sum();
    assert_eq!(fixed_rows, 10_050_550);
    assert!(plan
        .queries
        .iter()
        .any(|query| query.id == "setup.stock.sum_ytd"));
    assert!(PUBLIC_SPEC_NOTICE.contains("not public"));
}

#[test]
fn setup_plan_rejects_invalid_dynamic_counts() {
    assert!(setup_plan(SetupExpectations::final_2026(-1, 0)).is_err());
    assert!(setup_plan(SetupExpectations::final_2026(1, -1)).is_err());
    assert!(setup_plan(SetupExpectations {
        warehouses: 0,
        order_line_rows: 1,
        undelivered_order_line_rows: 1,
    })
    .is_err());
    assert!(setup_plan(SetupExpectations::final_2026(1_500_000, 9_000_000)).is_err());
}

#[test]
fn recovery_plan_rejects_impossible_committed_ledgers() {
    let setup = SetupExpectations::final_2026(15_000_000, 4_500_000);
    let too_few_lines = RecoveryExpectations {
        setup,
        committed: CommittedLedger {
            new_orders: 2,
            new_order_lines: 9,
            ..CommittedLedger::default()
        },
    };
    assert!(recovery_plan(too_few_lines).is_err());

    let too_many_remote = RecoveryExpectations {
        setup,
        committed: CommittedLedger {
            new_orders: 1,
            new_order_lines: 5,
            remote_new_order_lines: 6,
            ..CommittedLedger::default()
        },
    };
    assert!(recovery_plan(too_many_remote).is_err());

    let too_many_deliveries = RecoveryExpectations {
        setup,
        committed: CommittedLedger {
            delivered_orders: 450_001,
            ..CommittedLedger::default()
        },
    };
    assert!(recovery_plan(too_many_deliveries).is_err());
}

#[test]
fn committed_ledger_drives_public_online_and_recovery_answers() {
    let input = RecoveryExpectations {
        setup: SetupExpectations::final_2026(15_000_000, 4_500_000),
        committed: CommittedLedger {
            new_orders: 10,
            new_order_lines: 100,
            remote_new_order_lines: 4,
            payments: 20,
            delivered_orders: 3,
            delivered_order_lines: 30,
        },
    };

    let online = public_online_integer_plan(input).unwrap();
    assert_eq!(online.queries.len(), 6);
    assert!(online
        .queries
        .iter()
        .all(|query| query.scope == CheckScope::Online));
    assert!(online
        .queries
        .iter()
        .all(|query| query.id.starts_with("online.public.")));
    assert_eq!(
        expectation(&online, "online.public.district_next_sum"),
        ScalarExpectation::ExactInt(1_500_510)
    );
    assert!(online.queries.iter().all(|query| {
        !query.sql.contains("FROM new_orders") && !query.sql.contains("FROM orders")
    }));

    let recovery = recovery_plan(input).unwrap();
    for (id, expected) in [
        ("recovery.count.orders", 1_500_010),
        ("recovery.count.order_line", 15_000_100),
        ("recovery.count.history", 1_500_020),
        ("recovery.count.new_orders", 450_007),
        ("recovery.district.sum_next_order_id", 1_500_510),
        ("recovery.customer.sum_payment_cnt", 1_500_020),
        ("recovery.customer.sum_delivery_cnt", 3),
        ("recovery.stock.sum_order_cnt", 100),
        ("recovery.stock.sum_remote_cnt", 4),
        ("recovery.order_line.empty_delivery_time_count", 4_500_070),
    ] {
        assert_eq!(
            expectation(&recovery, id),
            ScalarExpectation::ExactInt(expected)
        );
    }
}

#[test]
fn logical_sql_can_be_rendered_with_opaque_runtime_identifiers() {
    let plan = setup_plan(SetupExpectations::final_2026(15_000_000, 4_500_000)).unwrap();
    let mut names = IdentifierMap::default();
    names.insert("orders", "t_q7").unwrap();
    names.insert("o_ol_cnt", "c_x2").unwrap();
    names.insert("ol_delivery_d", "c_z9").unwrap();
    let rendered = names.render_plan(&plan);
    let sum = rendered
        .queries
        .iter()
        .find(|query| query.id == "setup.orders.sum_o_ol_cnt")
        .unwrap();
    assert_eq!(sum.sql, "SELECT SUM(c_x2) FROM t_q7");
    let delivery = rendered
        .queries
        .iter()
        .find(|query| query.id == "setup.order_line.undelivered_count")
        .unwrap();
    assert!(delivery.sql.contains("c_z9 = ''"));
    assert!(names.insert("orders", "bad-name").is_err());
}

fn final_partition_expectations() -> Vec<PartitionExpectation> {
    let mut values = Vec::new();
    for warehouse_id in 1..=50 {
        for district_id in 1..=10 {
            values.push(PartitionExpectation {
                key: PartitionKey {
                    warehouse_id,
                    district_id,
                },
                order_count: 3_000,
                order_line_count: 30_000,
                new_order_count: 900,
                empty_delivery_time_count: 9_000,
                carrier_zero_count: 900,
                next_order_id: 3_001,
            });
        }
    }
    values
}

#[test]
fn recovery_audits_every_partition_and_scopes_every_partition_predicate() {
    let audits = recovery_partition_audits(final_partition_expectations()).unwrap();
    assert_eq!(audits.len(), 500);
    assert_eq!(
        audits.first().unwrap().key,
        PartitionKey {
            warehouse_id: 1,
            district_id: 1
        }
    );
    assert_eq!(
        audits.last().unwrap().key,
        PartitionKey {
            warehouse_id: 50,
            district_id: 10
        }
    );

    for audit in &audits {
        assert_eq!(audit.checks.len(), 6);
        let warehouse_fragments = ["_w_id =", "w_id ="];
        let district_fragments = ["_d_id =", "d_id ="];
        for check in &audit.checks {
            assert_eq!(check.scope, CheckScope::Recovery);
            assert!(
                warehouse_fragments
                    .iter()
                    .any(|fragment| check.sql.contains(fragment)),
                "warehouse key missing from {}",
                check.sql
            );
            assert!(
                district_fragments
                    .iter()
                    .any(|fragment| check.sql.contains(fragment)),
                "district key missing from {}",
                check.sql
            );
        }
    }
}

#[test]
fn partition_generator_rejects_incomplete_duplicate_and_out_of_range_ledgers() {
    let mut incomplete = final_partition_expectations();
    incomplete.pop();
    assert!(recovery_partition_audits(incomplete).is_err());

    let mut duplicate = final_partition_expectations();
    duplicate[499] = duplicate[0];
    assert!(recovery_partition_audits(duplicate).is_err());

    let mut out_of_range = final_partition_expectations();
    out_of_range[0].key.warehouse_id = 0;
    assert!(recovery_partition_audits(out_of_range).is_err());

    let mut inconsistent = final_partition_expectations();
    inconsistent[0].carrier_zero_count -= 1;
    assert!(recovery_partition_audits(inconsistent).is_err());
}

#[test]
fn scalar_validation_is_strictly_typed_and_zero_sign_is_ignored() {
    let setup = setup_plan(SetupExpectations::final_2026(15_000_000, 4_500_000)).unwrap();
    let warehouse = setup
        .queries
        .iter()
        .find(|query| query.id == "setup.count.warehouse")
        .unwrap();
    assert!(warehouse
        .validate(&TypedResult::scalar(TypedValue::Int32(50)))
        .is_ok());
    assert!(warehouse
        .validate(&TypedResult::scalar(TypedValue::Float32(
            50.0_f32.to_bits()
        )))
        .is_err());
    assert!(warehouse
        .validate(&TypedResult {
            rows: vec![vec![TypedValue::Int32(50)], vec![TypedValue::Int32(50)]],
        })
        .is_err());

    let stock_ytd = setup
        .queries
        .iter()
        .find(|query| query.id == "setup.stock.sum_ytd")
        .unwrap();
    assert!(stock_ytd
        .validate(&TypedResult::scalar(TypedValue::Float32(
            (-0.0_f32).to_bits()
        )))
        .is_ok());
    assert!(stock_ytd
        .validate(&TypedResult::scalar(TypedValue::Float32(
            f32::NAN.to_bits()
        )))
        .is_err());
}

#[test]
fn float_aggregate_catalog_has_the_published_seven_tolerances() {
    assert_eq!(FLOAT_AGGREGATES.len(), 7);
    assert_eq!(
        FLOAT_AGGREGATES
            .iter()
            .filter(|spec| spec.crash_max_ulps == 0)
            .map(|spec| spec.id)
            .collect::<Vec<_>>(),
        vec![
            FloatAggregateId::WarehouseYtd,
            FloatAggregateId::DistrictYtd,
            FloatAggregateId::StockYtd,
        ]
    );
    assert!(FLOAT_AGGREGATES
        .iter()
        .filter(|spec| spec.crash_max_ulps == 1)
        .all(|spec| matches!(
            spec.id,
            FloatAggregateId::CustomerBalance
                | FloatAggregateId::CustomerYtdPayment
                | FloatAggregateId::HistoryAmount
                | FloatAggregateId::OrderLineAmount
        )));
    assert_eq!(
        FLOAT_AGGREGATES
            .iter()
            .find(|spec| spec.id == FloatAggregateId::HistoryAmount)
            .unwrap()
            .ledger_rule,
        LedgerFloatRule::LargeSetBoundary
    );
    let order_line = FLOAT_AGGREGATES
        .iter()
        .find(|spec| spec.id == FloatAggregateId::OrderLineAmount)
        .unwrap();
    assert_eq!(order_line.sql, "SELECT SUM(ol_amount) FROM order_line");
    assert_eq!(float_aggregate_plan(CheckScope::Online).queries.len(), 7);
}

#[test]
fn ulp_comparison_handles_sign_zero_adjacency_and_non_finite_values() {
    assert_eq!(
        ulp_distance(0.0_f32.to_bits(), (-0.0_f32).to_bits()),
        Some(0)
    );
    let one = 1.0_f32;
    let next = f32::from_bits(one.to_bits() + 1);
    assert_eq!(ulp_distance(one.to_bits(), next.to_bits()), Some(1));
    assert!(float32_matches(one.to_bits(), next.to_bits(), 1));
    assert!(!float32_matches(one.to_bits(), next.to_bits(), 0));

    let minus_one = -1.0_f32;
    let minus_one_toward_zero = f32::from_bits(minus_one.to_bits() - 1);
    assert_eq!(
        ulp_distance(minus_one.to_bits(), minus_one_toward_zero.to_bits()),
        Some(1)
    );
    assert_eq!(ulp_distance(f32::NAN.to_bits(), one.to_bits()), None);
    assert_eq!(ulp_distance(f32::INFINITY.to_bits(), one.to_bits()), None);
    assert_eq!(
        ulp_distance(f32::from_bits(0x8000_0001).to_bits(), 0.0_f32.to_bits()),
        Some(1)
    );
    assert_eq!(
        ulp_distance((-0.0_f32).to_bits(), f32::from_bits(1).to_bits()),
        Some(1)
    );
}

#[test]
fn relative_add_and_sum_round_only_at_the_published_boundaries() {
    let before = 16_777_216.0_f32;
    let amount = 1.0_f32;
    assert_eq!(
        add_f32_once(before.to_bits(), amount.to_bits()).unwrap(),
        before.to_bits()
    );

    let values = [16_777_216.0_f32, 1.0_f32, 1.0_f32];
    let sum_bits = sum_f32_as_f64_once(values.into_iter().map(f32::to_bits)).unwrap();
    assert_eq!(f32::from_bits(sum_bits), 16_777_218.0_f32);
    assert!(sum_f32_as_f64_once([f32::INFINITY.to_bits()]).is_err());
}

#[test]
fn large_set_boundary_accepts_only_endpoint_roundings_and_rejects_non_finite() {
    let midpoint = 1.0_f64 + 2.0_f64.powi(-24);
    let boundary = large_set_boundary(midpoint, 1).unwrap();
    assert_eq!(boundary.lower_bits, 1.0_f32.to_bits());
    assert_eq!(boundary.upper_bits, (1.0_f32.to_bits() + 1));
    assert!(boundary.accepts(boundary.lower_bits));
    assert!(boundary.accepts(boundary.upper_bits));
    assert!(!boundary.accepts(boundary.upper_bits + 1));
    assert!(!boundary.accepts(f32::NAN.to_bits()));

    let zero = large_set_boundary(0.0, 10_000_000).unwrap();
    assert!(zero.accepts(0.0_f32.to_bits()));
    assert!(zero.accepts((-0.0_f32).to_bits()));
    assert!(large_set_boundary(-1.0, 1).is_err());
    assert!(large_set_boundary(f64::INFINITY, 1).is_err());
}

#[test]
fn exact_large_set_boundary_uses_a_superaccumulator_at_midpoints() {
    let half_ulp = 2.0_f32.powi(-24);
    let boundary = large_set_boundary_from_f32([1.0_f32.to_bits(), half_ulp.to_bits()]).unwrap();
    assert_eq!(boundary.sum_for_diagnostics, 1.0_f64 + 2.0_f64.powi(-24));
    assert_eq!(boundary.lower_bits, 1.0_f32.to_bits());
    assert_eq!(boundary.upper_bits, 1.0_f32.to_bits() + 1);

    let subnormal =
        large_set_boundary_from_f32([f32::from_bits(1).to_bits(), f32::from_bits(2).to_bits()])
            .unwrap();
    assert_eq!(subnormal.lower_bits, f32::from_bits(3).to_bits());
    assert_eq!(subnormal.upper_bits, f32::from_bits(3).to_bits());
    assert!(large_set_boundary_from_f32([(-1.0_f32).to_bits()]).is_err());
}

#[test]
fn superaccumulator_matches_exact_integer_ledgers() {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    for _case in 0..100 {
        let mut bits = Vec::new();
        let mut exact_sum = 0_u64;
        for _ in 0..37 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let value = (state % 10_001) as u32;
            exact_sum += u64::from(value);
            bits.push((value as f32).to_bits());
        }
        let exact = large_set_boundary_from_f32(bits).unwrap();
        let reference = large_set_boundary(exact_sum as f64, 37).unwrap();
        assert_eq!(
            (exact.lower_bits, exact.upper_bits),
            (reference.lower_bits, reference.upper_bits)
        );
    }
    assert!(large_set_boundary(1.0, (1_u64 << 53) + 1).is_err());
}

#[test]
fn public_float_ledger_and_crash_baseline_apply_their_distinct_tolerances() {
    let history = large_set_boundary_from_f32([10.0_f32.to_bits(), 2.5_f32.to_bits()]).unwrap();
    let order_line =
        large_set_boundary_from_f32([100.0_f32.to_bits(), 25.0_f32.to_bits()]).unwrap();
    let evidence = PublicFloatLedgerEvidence {
        history_amount: history,
        stock_ytd_bits: 42.0_f32.to_bits(),
        order_line_amount: order_line,
    };
    assert!(validate_public_float_ledger(
        history.lower_bits,
        42.0_f32.to_bits(),
        order_line.upper_bits,
        evidence
    )
    .is_ok());
    assert!(validate_public_float_ledger(
        history.lower_bits + 2,
        42.0_f32.to_bits(),
        order_line.upper_bits,
        evidence
    )
    .is_err());

    let warehouse = FLOAT_AGGREGATES
        .iter()
        .find(|spec| spec.id == FloatAggregateId::WarehouseYtd)
        .copied()
        .unwrap();
    let customer_balance = FLOAT_AGGREGATES
        .iter()
        .find(|spec| spec.id == FloatAggregateId::CustomerBalance)
        .copied()
        .unwrap();
    let one = 1.0_f32.to_bits();
    assert!(validate_crash_float_baseline(warehouse, one, one + 1).is_err());
    assert!(validate_crash_float_baseline(customer_balance, one, one + 1).is_ok());
}

#[test]
fn per_transaction_relative_update_is_bit_exact() {
    let before = 16_777_216.0_f32.to_bits();
    let amount = 1.0_f32.to_bits();
    assert!(validate_relative_add(before, amount, before).is_ok());
    assert!(validate_relative_add(before, amount, before + 1).is_err());
}

#[test]
fn payment_evidence_forms_one_complete_update_chain_to_recovery() {
    let update = |before: f32, amount: f32| RelativeUpdateEvidence {
        before_bits: before.to_bits(),
        bound_amount_bits: amount.to_bits(),
        after_bits: (before + amount).to_bits(),
    };
    let shuffled = [update(105.0, 1.0), update(100.0, 2.0), update(102.0, 3.0)];
    assert!(
        validate_relative_update_chain(100.0_f32.to_bits(), 106.0_f32.to_bits(), &shuffled).is_ok()
    );

    let forked = [update(100.0, 2.0), update(100.0, 3.0)];
    assert!(
        validate_relative_update_chain(100.0_f32.to_bits(), 103.0_f32.to_bits(), &forked).is_err()
    );

    let rounded_self_loop = RelativeUpdateEvidence {
        before_bits: 16_777_216.0_f32.to_bits(),
        bound_amount_bits: 1.0_f32.to_bits(),
        after_bits: 16_777_216.0_f32.to_bits(),
    };
    assert!(validate_relative_update_chain(
        rounded_self_loop.before_bits,
        rounded_self_loop.after_bits,
        &[rounded_self_loop; 3],
    )
    .is_ok());
}
