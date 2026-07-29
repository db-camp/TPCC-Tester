#[path = "../src/consistency.rs"]
mod consistency;

use consistency::{
    add_f32_once, float32_matches, float_aggregate_plan, large_set_boundary,
    large_set_boundary_from_f32, public_online_integer_plan, recovery_partition_audits,
    recovery_plan, setup_plan, sum_f32_as_f64_once, ulp_distance, validate_crash_float_baseline,
    validate_public_float_ledger, validate_public_recovery_integer_gate, validate_relative_add,
    validate_relative_update_chain, CheckScope, CommittedLedger, FloatAggregateId, IdentifierMap,
    LedgerFloatRule, OnlineKeySample, PartitionExpectation, PartitionKey, PlanError,
    PublicFloatLedgerEvidence, RecoveryExpectations, RelativeUpdateEvidence, ScalarExpectation,
    SetupExpectations, TypedResult, TypedValue, FINAL_WAREHOUSES, FLOAT_AGGREGATES,
    PUBLIC_RECOVERY_INTEGER_CHECK_COUNT, PUBLIC_SPEC_NOTICE,
};

fn expectation(plan: &consistency::ConsistencyPlan, id: &str) -> ScalarExpectation {
    plan.queries
        .iter()
        .find(|query| query.id == id)
        .unwrap_or_else(|| panic!("missing check {id}"))
        .expectation
        .clone()
}

fn sql_identifiers(sql: &str) -> Vec<&str> {
    sql.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .collect()
}

fn recovery_fixture() -> RecoveryExpectations {
    RecoveryExpectations {
        setup: SetupExpectations::final_2026(15_000_000, 4_500_000),
        committed: CommittedLedger {
            new_orders: 10,
            new_order_lines: 100,
            remote_new_order_lines: 4,
            stock_ytd_delta: 550,
            payments: 20,
            delivered_orders: 3,
            delivered_order_lines: 30,
        },
    }
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
    let input = recovery_fixture();

    let sample = OnlineKeySample {
        item_id: 72_345,
        customer_warehouse_id: 17,
        customer_district_id: 6,
        customer_id: 2_149,
        stock_warehouse_id: 17,
        stock_item_id: 72_345,
    };
    let online = public_online_integer_plan(input, sample).unwrap();
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
    assert!(online
        .queries
        .iter()
        .find(|query| query.id == "online.public.item_key")
        .unwrap()
        .sql
        .contains("i_id = 72345"));
    assert!(online
        .queries
        .iter()
        .find(|query| query.id == "online.public.customer_key")
        .unwrap()
        .sql
        .contains("c_w_id = 17 AND c_d_id = 6 AND c_id = 2149"));
    assert!(online
        .queries
        .iter()
        .find(|query| query.id == "online.public.stock_key")
        .unwrap()
        .sql
        .contains("s_w_id = 17 AND s_i_id = 72345"));
    assert!(public_online_integer_plan(
        input,
        OnlineKeySample {
            stock_item_id: sample.stock_item_id + 1,
            ..sample
        }
    )
    .is_err());

    let recovery = recovery_plan(input).unwrap();
    for (id, expected) in [
        ("recovery.count.orders", 1_500_010),
        ("recovery.count.order_line", 15_000_100),
        ("recovery.count.history", 1_500_020),
        ("recovery.count.new_orders", 450_007),
        ("recovery.order_line.sum_quantity", 75_000_550),
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
fn recovery_integer_gate_is_exactly_37_typed_and_partition_independent() {
    let plan = recovery_plan(recovery_fixture()).unwrap();
    let expected_ids = [
        "recovery.count.customer",
        "recovery.count.district",
        "recovery.count.history",
        "recovery.count.item",
        "recovery.count.new_orders",
        "recovery.count.order_line",
        "recovery.count.orders",
        "recovery.count.stock",
        "recovery.count.warehouse",
        "recovery.orders.sum_o_ol_cnt",
        "recovery.order_line.sum_quantity",
        "recovery.orders.open_carrier_count",
        "recovery.order_line.empty_delivery_time_count",
        "recovery.district.sum_next_order_id",
        "recovery.customer.sum_payment_cnt",
        "recovery.customer.sum_delivery_cnt",
        "recovery.stock.sum_order_cnt",
        "recovery.stock.sum_remote_cnt",
        "recovery.stock.quantity_range",
        "recovery.orders.line_count_range",
        "recovery.order_line.quantity_range",
        "recovery.orders.carrier_range",
        "recovery.orders.all_local_range",
        "recovery.stock.counter_range",
        "recovery.customer.counter_range",
        "recovery.district.next_order_id_range",
        "recovery.warehouse.key_range",
        "recovery.item.key_range",
        "recovery.history.key_range",
        "recovery.new_orders.key_range",
        "recovery.orders.order_id_range",
        "recovery.order_line.order_key_range",
        "recovery.district.key_range",
        "recovery.customer.key_range",
        "recovery.orders.key_range",
        "recovery.order_line.key_range",
        "recovery.stock.key_range",
    ];

    assert_eq!(PUBLIC_RECOVERY_INTEGER_CHECK_COUNT, 37);
    assert_eq!(
        plan.queries
            .iter()
            .map(|query| query.id.as_str())
            .collect::<Vec<_>>(),
        expected_ids
    );
    validate_public_recovery_integer_gate(&plan).unwrap();
    for query in &plan.queries {
        assert_eq!(query.scope, CheckScope::Recovery);
        assert!(!query.id.starts_with("recovery.partition."));
        assert!(!query.sql.contains(" OR "));
        assert!(!query.sql.contains(';'));
        let expected = match query.expectation {
            ScalarExpectation::ExactInt(value) => i32::try_from(value).unwrap(),
            _ => panic!("{} is not an exact integer check", query.id),
        };
        assert!(query
            .validate(&TypedResult::scalar(TypedValue::Int32(expected)))
            .is_ok());
        assert!(query
            .validate(&TypedResult::scalar(TypedValue::Float32(
                (expected as f32).to_bits()
            )))
            .is_err());
    }
}

#[test]
fn recovery_integer_gate_shape_and_int32_bounds_fail_closed() {
    let plan = recovery_plan(recovery_fixture()).unwrap();

    let mut missing = plan.clone();
    missing.queries.pop();
    assert!(matches!(
        validate_public_recovery_integer_gate(&missing),
        Err(PlanError::InvalidRecoveryIntegerCheckCount {
            expected: 37,
            actual: 36
        })
    ));

    let mut duplicate = plan.clone();
    duplicate.queries[36] = duplicate.queries[0].clone();
    assert!(matches!(
        validate_public_recovery_integer_gate(&duplicate),
        Err(PlanError::DuplicateRecoveryCheckId(_))
    ));

    let mut wrong_type = plan.clone();
    wrong_type.queries[0].expectation = ScalarExpectation::FiniteFloat32;
    assert!(matches!(
        validate_public_recovery_integer_gate(&wrong_type),
        Err(PlanError::InvalidRecoveryIntegerCheck(_))
    ));

    let mut too_large = plan;
    too_large.queries[0].expectation = ScalarExpectation::ExactInt(i64::from(i32::MAX) + 1);
    assert!(matches!(
        validate_public_recovery_integer_gate(&too_large),
        Err(PlanError::IntegerExpectationOutOfRange { .. })
    ));
}

#[test]
fn recovery_quantity_evidence_is_exact_and_rejects_invalid_or_unsafe_ledgers() {
    let mut invalid = recovery_fixture();
    invalid.committed.stock_ytd_delta = invalid.committed.new_order_lines - 1;
    assert!(matches!(
        recovery_plan(invalid),
        Err(PlanError::InconsistentLedger(
            "committed stock YTD delta must equal 1..=10 per committed NewOrder line"
        ))
    ));

    let unsafe_int32 = RecoveryExpectations {
        setup: SetupExpectations::final_2026(15_000_000, 4_500_000),
        committed: CommittedLedger {
            new_orders: 300_000_000,
            new_order_lines: 1_500_000_000,
            remote_new_order_lines: 0,
            stock_ytd_delta: 15_000_000_000,
            ..CommittedLedger::default()
        },
    };
    assert!(matches!(
        recovery_plan(unsafe_int32),
        Err(PlanError::IntegerExpectationOutOfRange { .. })
    ));
}

#[test]
fn recovery_integer_gate_covers_sf1_and_sf50_dynamic_line_extremes() {
    let sf1 = recovery_plan(RecoveryExpectations {
        setup: SetupExpectations {
            warehouses: 1,
            order_line_rows: 150_000,
            undelivered_order_line_rows: 45_000,
        },
        committed: CommittedLedger::default(),
    })
    .unwrap();
    assert_eq!(sf1.queries.len(), PUBLIC_RECOVERY_INTEGER_CHECK_COUNT);
    assert_eq!(
        expectation(&sf1, "recovery.count.stock"),
        ScalarExpectation::ExactInt(100_000)
    );
    assert_eq!(
        expectation(&sf1, "recovery.order_line.sum_quantity"),
        ScalarExpectation::ExactInt(750_000)
    );

    let sf50_max_lines = recovery_plan(RecoveryExpectations {
        setup: SetupExpectations::final_2026(22_500_000, 6_750_000),
        committed: CommittedLedger::default(),
    })
    .unwrap();
    assert_eq!(
        sf50_max_lines.queries.len(),
        PUBLIC_RECOVERY_INTEGER_CHECK_COUNT
    );
    assert_eq!(
        expectation(&sf50_max_lines, "recovery.order_line.sum_quantity"),
        ScalarExpectation::ExactInt(112_500_000)
    );

    let new_order_keys = sf1
        .queries
        .iter()
        .find(|query| query.id == "recovery.new_orders.key_range")
        .unwrap();
    assert!(new_order_keys.sql.contains("no_o_id >= 2101"));

    assert!(matches!(
        recovery_plan(RecoveryExpectations {
            setup: SetupExpectations {
                warehouses: FINAL_WAREHOUSES + 1,
                order_line_rows: 7_650_000,
                undelivered_order_line_rows: 2_295_000,
            },
            committed: CommittedLedger::default(),
        }),
        Err(PlanError::WarehouseCountExceedsPublicMaximum {
            actual: 51,
            maximum: 50
        })
    ));
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

#[test]
fn all_37_recovery_queries_render_every_logical_identifier_opaquely() {
    let plan = recovery_plan(recovery_fixture()).unwrap();
    let logical_names = [
        "warehouse",
        "district",
        "customer",
        "history",
        "orders",
        "new_orders",
        "order_line",
        "item",
        "stock",
        "w_id",
        "d_w_id",
        "d_id",
        "d_next_o_id",
        "c_w_id",
        "c_d_id",
        "c_id",
        "c_payment_cnt",
        "c_delivery_cnt",
        "h_c_id",
        "h_c_d_id",
        "h_c_w_id",
        "h_d_id",
        "h_w_id",
        "o_w_id",
        "o_d_id",
        "o_id",
        "o_c_id",
        "o_carrier_id",
        "o_ol_cnt",
        "o_all_local",
        "no_w_id",
        "no_d_id",
        "no_o_id",
        "ol_w_id",
        "ol_d_id",
        "ol_o_id",
        "ol_number",
        "ol_i_id",
        "ol_supply_w_id",
        "ol_delivery_d",
        "ol_quantity",
        "i_id",
        "s_w_id",
        "s_i_id",
        "s_quantity",
        "s_order_cnt",
        "s_remote_cnt",
    ];
    let mut names = IdentifierMap::default();
    let mappings = logical_names
        .iter()
        .enumerate()
        .map(|(index, logical)| {
            let runtime = format!("z_{index}");
            names.insert(*logical, runtime.clone()).unwrap();
            (*logical, runtime)
        })
        .collect::<Vec<_>>();
    let rendered = names.render_plan(&plan);

    for (logical, opaque) in plan.queries.iter().zip(&rendered.queries) {
        assert_ne!(logical.sql, opaque.sql, "{} was not rendered", logical.id);
        let before = sql_identifiers(&logical.sql);
        let after = sql_identifiers(&opaque.sql);
        for (canonical, runtime) in &mappings {
            if before.contains(canonical) {
                assert!(
                    !after.contains(canonical),
                    "{} leaked canonical identifier {canonical}",
                    logical.id
                );
                assert!(
                    after.contains(&runtime.as_str()),
                    "{} did not render {canonical} as {runtime}",
                    logical.id
                );
            }
        }
    }
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
