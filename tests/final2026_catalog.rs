#[path = "../src/connection/mod.rs"]
mod connection;
#[path = "../src/consistency.rs"]
mod consistency;
#[path = "../src/data_gen.rs"]
mod data_gen;
#[path = "../src/error.rs"]
mod error;
#[path = "../src/model.rs"]
mod model;
#[path = "../src/profile.rs"]
mod profile;
#[path = "../src/ranking/mod.rs"]
mod ranking;
#[path = "../src/routing.rs"]
mod routing;
#[path = "../src/transaction/mod.rs"]
mod transaction;
#[path = "../src/workload.rs"]
mod workload;

use std::collections::BTreeSet;

use connection::prepared::{Statement, StatementKind};
use connection::wire::SqlType;
use ranking::catalog::{
    final2026_catalog, validate_catalog, Multiplicity, StatementId, DELIVERY_EMPTY_STAGE,
    DELIVERY_STAGES, NEW_ORDER_EXPECTED_ROLLBACK_STAGE, NEW_ORDER_STAGES, ORDER_STATUS_STAGES,
    PAYMENT_STAGES, STOCK_LEVEL_STAGES, UNDELIVERED_CARRIER_ID, UNDELIVERED_DATE,
};

fn find(catalog: &[Statement], id: StatementId) -> &Statement {
    catalog
        .iter()
        .find(|statement| statement.id == id.wire_id())
        .expect("catalogue statement")
}

#[test]
fn catalogue_has_unique_bounded_ids_and_dense_typed_markers() {
    let catalog = final2026_catalog();
    validate_catalog(&catalog).expect("valid public final catalogue");

    let ids: BTreeSet<_> = catalog.iter().map(|statement| statement.id).collect();
    assert_eq!(ids.len(), catalog.len());
    assert!(ids.iter().all(|id| (1..=256).contains(id)));
    assert!(catalog
        .iter()
        .all(|statement| !statement.sql.as_bytes().contains(&0)));

    let mut invalid = catalog.clone();
    find_mut(&mut invalid, StatementId::NewOrderInsertLine)
        .param_types
        .pop();
    assert!(
        validate_catalog(&invalid)
            .unwrap_err()
            .to_string()
            .contains("dense schema"),
        "a missing type declaration must reject the $1..$10 SQL"
    );
}

#[test]
fn query_schemas_use_fixed_aliases_and_exact_wire_types() {
    let catalog = final2026_catalog();

    let home = find(&catalog, StatementId::NewOrderHome);
    assert_eq!(
        home.param_types,
        vec![SqlType::Int32, SqlType::Int32, SqlType::Int32]
    );
    assert_query_columns(
        home,
        &[
            ("c_discount", SqlType::Float32),
            ("c_last", SqlType::Char),
            ("c_credit", SqlType::Char),
            ("w_tax", SqlType::Float32),
            ("d_next_o_id", SqlType::Int32),
            ("d_tax", SqlType::Float32),
        ],
    );

    let stock = find(&catalog, StatementId::NewOrderStock);
    let StatementKind::Query { columns } = &stock.kind else {
        panic!("stock lookup must be a query");
    };
    assert_eq!(columns.len(), 12);
    assert_eq!(columns[0].sql_type, SqlType::Int32);
    assert!(columns[1..]
        .iter()
        .all(|column| column.sql_type == SqlType::Char));

    let payment_by_last = find(&catalog, StatementId::PaymentCustomerByLast);
    assert_eq!(
        payment_by_last.param_types,
        vec![SqlType::Int32, SqlType::Int32, SqlType::Char]
    );
    assert!(payment_by_last
        .sql
        .contains("ORDER BY customer.c_first ASC, customer.c_id ASC"));

    let order_status_by_last = find(&catalog, StatementId::OrderStatusCustomerByLast);
    assert_eq!(
        order_status_by_last.param_types,
        vec![SqlType::Int32, SqlType::Int32, SqlType::Char]
    );
    assert!(order_status_by_last
        .sql
        .contains("ORDER BY c_first ASC, c_id ASC"));
}

#[test]
fn ranked_updates_keep_float32_arithmetic_inside_relative_sql() {
    let catalog = final2026_catalog();

    for id in [
        StatementId::PaymentUpdateWarehouse,
        StatementId::PaymentUpdateDistrict,
        StatementId::PaymentUpdateGoodCustomer,
        StatementId::PaymentUpdateBadCustomer,
        StatementId::DeliveryUpdateCustomer,
        StatementId::NewOrderUpdateStockNormal,
        StatementId::NewOrderUpdateStockWrapped,
    ] {
        let statement = find(&catalog, id);
        assert_eq!(
            statement.param_types[0],
            if matches!(
                id,
                StatementId::NewOrderUpdateStockNormal | StatementId::NewOrderUpdateStockWrapped
            ) {
                SqlType::Int32
            } else {
                SqlType::Float32
            }
        );
        assert!(
            statement.sql.contains(" = ")
                && (statement.sql.contains(" + $") || statement.sql.contains(" - $")),
            "{id:?} must perform a relative update"
        );
    }

    for id in [
        StatementId::NewOrderUpdateStockNormal,
        StatementId::NewOrderUpdateStockWrapped,
    ] {
        let statement = find(&catalog, id);
        assert_eq!(statement.param_types[1], SqlType::Float32);
        assert!(statement.sql.contains("s_ytd = s_ytd + $2"));
        assert!(statement.sql.contains("s_remote_cnt = s_remote_cnt + $3"));
    }
}

#[test]
fn stock_level_is_one_server_side_distinct_join_with_complete_keys() {
    let catalog = final2026_catalog();
    let statement = find(&catalog, StatementId::StockLevelCount);

    assert_eq!(
        statement.param_types,
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32
        ]
    );
    assert!(statement
        .sql
        .contains("COUNT(DISTINCT order_line.ol_i_id) AS low_stock_count"));
    assert!(statement.sql.contains("stock.s_quantity < $4"));
    assert!(!statement.sql.contains("stock.s_quantity <= $4"));
    assert!(statement.sql.contains("order_line.ol_w_id = $1"));
    assert!(statement.sql.contains("order_line.ol_d_id = $2"));
    assert!(statement.sql.contains("order_line.ol_o_id < $3"));
    assert!(statement.sql.contains("order_line.ol_o_id >= $3 - 20"));
    assert!(statement.sql.contains("stock.s_w_id = order_line.ol_w_id"));
    assert!(statement.sql.contains("stock.s_i_id = order_line.ol_i_id"));
    assert_query_columns(statement, &[("low_stock_count", SqlType::Int32)]);
}

#[test]
fn delivery_reads_rows_and_sum_with_the_full_partition_key() {
    let catalog = final2026_catalog();
    let customer = find(&catalog, StatementId::DeliveryCustomer);
    assert!(customer.sql.contains("orders.o_w_id = $1"));
    assert!(customer.sql.contains("orders.o_d_id = $2"));
    assert!(customer.sql.contains("orders.o_id = $3"));
    assert!(customer.sql.contains("customer.c_id = orders.o_c_id"));

    for id in [StatementId::DeliveryLineRows, StatementId::DeliveryLineSum] {
        let statement = find(&catalog, id);
        assert!(statement.sql.contains("ol_w_id = $1"));
        assert!(statement.sql.contains("ol_d_id = $2"));
        assert!(statement.sql.contains("ol_o_id = $3"));
    }
    assert_query_columns(
        find(&catalog, StatementId::DeliveryLineSum),
        &[("ol_amount_sum", SqlType::Float32)],
    );
}

#[test]
fn delivery_reads_customer_after_its_relative_update() {
    let catalogue = final2026_catalog();
    let after = find(&catalogue, StatementId::DeliveryCustomerAfter);
    assert_eq!(
        after.param_types,
        vec![SqlType::Int32, SqlType::Int32, SqlType::Int32]
    );
    assert!(after.sql.contains("c_w_id = $1"));
    assert!(after.sql.contains("c_d_id = $2"));
    assert!(after.sql.contains("c_id = $3"));

    let final_stage = DELIVERY_STAGES.last().unwrap();
    assert!(final_stage.steps.iter().any(|step| {
        step.alternatives == [StatementId::DeliveryCustomerAfter]
            && step.multiplicity == Multiplicity::PerClaimedDistrict
    }));
}

#[test]
fn stage_templates_preserve_the_ranked_round_trip_shapes() {
    assert_eq!(
        NEW_ORDER_STAGES
            .iter()
            .map(|stage| stage.round_trip)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        PAYMENT_STAGES
            .iter()
            .map(|stage| stage.round_trip)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        ORDER_STATUS_STAGES
            .iter()
            .map(|stage| stage.round_trip)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        DELIVERY_STAGES
            .iter()
            .map(|stage| stage.round_trip)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        STOCK_LEVEL_STAGES
            .iter()
            .map(|stage| stage.round_trip)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    assert!(NEW_ORDER_STAGES[0].steps.iter().any(|step| {
        step.multiplicity == Multiplicity::SortedUniqueStock
            && step.alternatives == &[StatementId::NewOrderLockStock]
    }));
    assert!(DELIVERY_STAGES[0].steps.iter().any(|step| {
        step.multiplicity == Multiplicity::TenDistricts
            && step.alternatives == &[StatementId::DeliveryOldestOrder]
    }));
    assert_eq!(
        NEW_ORDER_EXPECTED_ROLLBACK_STAGE.steps[0].alternatives,
        &[StatementId::Abort]
    );
    assert_eq!(
        DELIVERY_EMPTY_STAGE.steps[0].alternatives,
        &[StatementId::Commit]
    );
}

#[test]
fn catalogue_never_changes_output_mode_or_writes_null_sentinels() {
    let catalog = final2026_catalog();
    for statement in &catalog {
        let upper = statement.sql.to_ascii_uppercase();
        assert!(!upper.contains("OUTPUT_FILE"));
        assert!(!upper.contains("SET OUTPUT"));
        assert!(!upper.contains("NULL"));
        assert!(
            !upper.starts_with("INSERT INTO ") || !upper.contains(") VALUES"),
            "RMDB INSERT grammar uses table-order VALUES without a column list"
        );
    }
    assert_eq!(UNDELIVERED_CARRIER_ID, 0);
    assert_eq!(UNDELIVERED_DATE, "");
}

fn assert_query_columns(statement: &Statement, expected: &[(&str, SqlType)]) {
    let StatementKind::Query { columns } = &statement.kind else {
        panic!("statement {} must be a query", statement.id);
    };
    let actual: Vec<_> = columns
        .iter()
        .map(|column| (column.name.as_str(), column.sql_type))
        .collect();
    assert_eq!(actual, expected);
    for (name, _) in expected {
        assert!(
            statement
                .sql
                .to_ascii_uppercase()
                .contains(&format!(" AS {}", name.to_ascii_uppercase())),
            "statement {} must use fixed alias {name}",
            statement.id
        );
    }
}

fn find_mut(catalog: &mut [Statement], id: StatementId) -> &mut Statement {
    catalog
        .iter_mut()
        .find(|statement| statement.id == id.wire_id())
        .expect("catalogue statement")
}
