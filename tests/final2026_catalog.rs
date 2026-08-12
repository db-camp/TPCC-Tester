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
#[path = "../src/runtime_schema.rs"]
mod runtime_schema;
#[path = "../src/transaction/mod.rs"]
mod transaction;
#[path = "../src/workload.rs"]
mod workload;

use std::collections::BTreeSet;

use connection::prepared::{Statement, StatementKind};
use connection::wire::SqlType;
use ranking::catalog::{
    final2026_catalog, new_order_stock_statement, validate_catalog, Multiplicity, StatementId,
    DELIVERY_EMPTY_STAGE, DELIVERY_STAGES, NEW_ORDER_EXPECTED_ROLLBACK_STAGE, NEW_ORDER_STAGES,
    ORDER_STATUS_STAGES, PAYMENT_STAGES, STOCK_LEVEL_STAGES, UNDELIVERED_CARRIER_ID,
    UNDELIVERED_DATE,
};
use runtime_schema::{
    RuntimeSchema, FINAL2026_STATEMENT_KEYS, FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS,
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
    assert_eq!(catalog.len(), 56);
    assert_eq!(StatementId::BASE.len(), 42);
    assert_eq!(StatementId::SUPPLEMENTAL.len(), 14);
    assert_eq!(StatementId::ALL.len(), 56);
    assert_eq!(
        StatementId::BASE.map(StatementId::key),
        FINAL2026_STATEMENT_KEYS
    );
    assert_eq!(
        StatementId::SUPPLEMENTAL.map(StatementId::key),
        FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS
    );
    assert_eq!(
        StatementId::SUPPLEMENTAL.map(StatementId::wire_id),
        [82, 83, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102]
    );

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
fn supplemental_catalogue_preserves_the_persisted_42_statement_cache() {
    let schema = RuntimeSchema::opaque(2026).unwrap();
    assert_eq!(schema.fingerprint(), 0x45b1_0a2a_a625_dea4);
    let encoded = schema.encode();
    assert_eq!(
        encoded
            .lines()
            .filter(|line| line.starts_with("statement="))
            .count(),
        42
    );
    assert!(FINAL2026_SUPPLEMENTAL_STATEMENT_KEYS
        .iter()
        .all(|key| !encoded.contains(*key)));

    let decoded = RuntimeSchema::decode(&encoded).unwrap();
    assert_eq!(decoded.encode(), encoded);
    for id in StatementId::SUPPLEMENTAL {
        assert_eq!(
            decoded.statements().id(id.key()).unwrap(),
            schema.statements().id(id.key()).unwrap()
        );
    }
}

#[test]
fn query_schemas_use_declared_names_and_exact_wire_types() {
    let catalog = final2026_catalog();
    assert!(catalog.iter().all(|statement| {
        !matches!(&statement.kind, StatementKind::Query { .. })
            || !statement.sql.to_ascii_uppercase().contains(" AS ")
    }));

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
        ],
    );
    assert_eq!(home.sql.matches("FROM customer, warehouse").count(), 1);
    assert!(!home.sql.contains("district"));

    assert_query_columns(
        find(&catalog, StatementId::NewOrderItem),
        &[
            ("i_id", SqlType::Int32),
            ("i_price", SqlType::Float32),
            ("i_name", SqlType::Char),
            ("i_data", SqlType::Char),
        ],
    );

    assert_query_columns(
        find(&catalog, StatementId::StockLevelNextOrder),
        &[
            ("d_next_o_id", SqlType::Int32),
            ("d_tax", SqlType::Float32),
        ],
    );

    for district_id in 1..=10 {
        let id = new_order_stock_statement(district_id).unwrap();
        let stock = find(&catalog, id);
        let StatementKind::Query { columns } = &stock.kind else {
            panic!("district {district_id} stock lookup must be a query");
        };
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "s_quantity");
        assert_eq!(columns[0].sql_type, SqlType::Int32);
        assert_eq!(columns[1].name, format!("s_dist_{district_id:02}"));
        assert_eq!(columns[1].sql_type, SqlType::Char);
        assert!(stock.sql.starts_with(&format!(
            "SELECT s_quantity, s_dist_{district_id:02} FROM stock"
        )));
        assert!(!stock.sql.contains("s_ytd"));
        assert!(!stock.sql.contains("s_order_cnt"));
        assert!(!stock.sql.contains("s_remote_cnt"));
        assert!(!stock.sql.contains("s_data"));
    }

    assert_query_columns(
        find(&catalog, StatementId::PreflightNewOrderStockVersion),
        &[
            ("s_quantity", SqlType::Int32),
            ("s_ytd", SqlType::Float32),
            ("s_order_cnt", SqlType::Int32),
            ("s_remote_cnt", SqlType::Int32),
        ],
    );

    let payment_by_last = find(&catalog, StatementId::PaymentCustomerByLast);
    assert_eq!(
        payment_by_last.param_types,
        vec![SqlType::Int32, SqlType::Int32, SqlType::Char]
    );
    assert!(payment_by_last
        .sql
        .ends_with("WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3;"));
    assert!(!payment_by_last.sql.contains("ORDER BY"));

    let order_status_by_last = find(&catalog, StatementId::OrderStatusCustomerByLast);
    assert_eq!(
        order_status_by_last.param_types,
        vec![SqlType::Int32, SqlType::Int32, SqlType::Char]
    );
    assert!(order_status_by_last
        .sql
        .ends_with("WHERE c_w_id = $1 AND c_d_id = $2 AND c_last = $3;"));
    assert!(!order_status_by_last.sql.contains("ORDER BY"));
}

#[test]
fn payment_before_reads_only_fields_consumed_by_the_runner() {
    let catalog = final2026_catalog();
    let warehouse_before = find(&catalog, StatementId::PaymentWarehouse);
    let warehouse_after = find(&catalog, StatementId::PaymentWarehouseAfter);
    let district_before = find(&catalog, StatementId::PaymentDistrict);
    let district_after = find(&catalog, StatementId::PaymentDistrictAfter);

    assert_query_columns(
        warehouse_before,
        &[
            ("w_ytd", SqlType::Float32),
            ("w_name", SqlType::Char),
        ],
    );
    assert_query_columns(warehouse_after, &[("w_ytd", SqlType::Float32)]);
    assert_eq!(warehouse_after.param_types, vec![SqlType::Int32]);
    assert_eq!(warehouse_after.sql, "SELECT w_ytd FROM warehouse WHERE w_id = $1;");

    assert_query_columns(
        district_before,
        &[
            ("d_ytd", SqlType::Float32),
            ("d_name", SqlType::Char),
        ],
    );
    assert_query_columns(district_after, &[("d_ytd", SqlType::Float32)]);
    assert_eq!(district_after.param_types, vec![SqlType::Int32; 2]);
    assert_eq!(
        district_after.sql,
        "SELECT d_ytd FROM district WHERE d_w_id = $1 AND d_id = $2;"
    );

    for id in [
        StatementId::PaymentCustomerById,
        StatementId::PaymentCustomerByLast,
    ] {
        assert_query_columns(
            find(&catalog, id),
            &[
                ("c_id", SqlType::Int32),
                ("c_first", SqlType::Char),
                ("c_last", SqlType::Char),
                ("c_credit", SqlType::Char),
                ("c_balance", SqlType::Float32),
                ("c_ytd_payment", SqlType::Float32),
                ("c_payment_cnt", SqlType::Int32),
                ("c_delivery_cnt", SqlType::Int32),
                ("c_data", SqlType::Char),
            ],
        );
    }
    let stage_one = PAYMENT_STAGES[0].steps;
    assert_eq!(stage_one[1].alternatives, &[StatementId::PaymentWarehouse]);
    assert_eq!(
        stage_one[3].alternatives,
        &[StatementId::PaymentWarehouseAfter]
    );
    assert_eq!(stage_one[4].alternatives, &[StatementId::PaymentDistrict]);
    assert_eq!(
        stage_one[6].alternatives,
        &[StatementId::PaymentDistrictAfter]
    );
}

#[test]
fn order_status_latest_lookup_returns_the_order_header_once() {
    let catalog = final2026_catalog();
    let latest = find(&catalog, StatementId::OrderStatusLatestOrder);
    assert_query_columns(
        latest,
        &[
            ("o_id", SqlType::Int32),
            ("o_entry_d", SqlType::Char),
            ("o_carrier_id", SqlType::Int32),
        ],
    );
    assert!(latest.sql.contains("o_c_id = $3"));
    assert!(latest.sql.contains("ORDER BY o_id DESC LIMIT 1"));

    assert_eq!(
        ORDER_STATUS_STAGES[1].steps[0].alternatives,
        &[StatementId::OrderStatusLatestOrder]
    );
    assert_eq!(ORDER_STATUS_STAGES[2].steps.len(), 2);
    assert_eq!(
        ORDER_STATUS_STAGES[2].steps[0].alternatives,
        &[StatementId::OrderStatusLines]
    );
    assert_eq!(
        ORDER_STATUS_STAGES[2].steps[1].alternatives,
        &[StatementId::Commit]
    );

    assert_query_columns(
        find(&catalog, StatementId::OrderStatusOrder),
        &[
            ("o_id", SqlType::Int32),
            ("o_entry_d", SqlType::Char),
            ("o_carrier_id", SqlType::Int32),
        ],
    );
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

    let normal = find(&catalog, StatementId::NewOrderUpdateStockNormal);
    assert_eq!(normal.param_types, vec![SqlType::Int32; 5]);
    assert!(normal.sql.contains("s_quantity = s_quantity - $1"));
    assert!(normal.sql.contains("s_ytd = s_ytd + $1"));
    assert!(normal.sql.contains("s_remote_cnt = s_remote_cnt + $2"));
    assert!(normal.sql.contains("s_w_id = $3"));
    assert!(normal.sql.contains("s_i_id = $4"));
    assert!(normal.sql.contains("s_quantity >= $5"));

    let wrapped = find(&catalog, StatementId::NewOrderUpdateStockWrapped);
    assert_eq!(wrapped.param_types, vec![SqlType::Int32; 5]);
    assert!(wrapped
        .sql
        .contains("s_quantity = s_quantity - $1 + 91"));
    assert!(wrapped.sql.contains("s_ytd = s_ytd + $1"));
    assert!(wrapped.sql.contains("s_remote_cnt = s_remote_cnt + $2"));
    assert!(wrapped.sql.contains("s_w_id = $3"));
    assert!(wrapped.sql.contains("s_i_id = $4"));
    assert!(wrapped.sql.contains("s_quantity < $5"));
}

#[test]
fn stock_level_matches_the_official_distinct_join_shape() {
    let catalog = final2026_catalog();
    let statement = find(&catalog, StatementId::StockLevelCount);

    assert_eq!(
        statement.param_types,
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32
        ]
    );
    assert_eq!(
        statement.sql,
        "SELECT COUNT(DISTINCT (stock.s_i_id)) FROM order_line, stock \
         WHERE order_line.ol_w_id = $1 AND order_line.ol_d_id = $2 \
         AND order_line.ol_o_id < $4 AND order_line.ol_o_id >= $3 \
         AND stock.s_w_id = $1 AND stock.s_i_id = order_line.ol_i_id \
         AND stock.s_quantity < $5;"
    );
    assert!(statement.sql.contains("stock.s_quantity < $5"));
    assert!(!statement.sql.contains("stock.s_quantity <= $5"));
    assert!(statement.sql.contains("order_line.ol_w_id = $1"));
    assert!(statement.sql.contains("order_line.ol_d_id = $2"));
    assert!(statement.sql.contains("order_line.ol_o_id >= $3"));
    assert!(statement.sql.contains("order_line.ol_o_id < $4"));
    assert!(statement.sql.contains("stock.s_w_id = $1"));
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

    for (id, predicate, column) in [
        (
            StatementId::DeliveryEarlierQueueCount,
            "no_o_id < $3",
            "earlier_queue_count",
        ),
        (
            StatementId::DeliveryExactQueueCount,
            "no_o_id = $3",
            "exact_queue_count",
        ),
    ] {
        let statement = find(&catalog, id);
        assert_eq!(statement.param_types, vec![SqlType::Int32; 3]);
        assert!(statement.sql.contains("COUNT(*)"));
        assert!(statement.sql.contains("no_w_id = $1"));
        assert!(statement.sql.contains("no_d_id = $2"));
        assert!(statement.sql.contains(predicate));
        assert_query_columns(statement, &[(column, SqlType::Int32)]);
    }
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
    assert!(NEW_ORDER_STAGES[0].steps.iter().any(|step| {
        step.multiplicity == Multiplicity::Once
            && step.alternatives == &[StatementId::StockLevelNextOrder]
    }));
    let district_stock_statements = (1..=10)
        .map(|district_id| new_order_stock_statement(district_id).unwrap())
        .collect::<Vec<_>>();
    assert!(NEW_ORDER_STAGES[0].steps.iter().any(|step| {
        step.multiplicity == Multiplicity::PerOrderLine
            && step.alternatives == district_stock_statements.as_slice()
    }));
    assert!(!NEW_ORDER_STAGES[1].steps.iter().any(|step| {
        step.alternatives == &[StatementId::NewOrderStock]
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
}

fn find_mut(catalog: &mut [Statement], id: StatementId) -> &mut Statement {
    catalog
        .iter_mut()
        .find(|statement| statement.id == id.wire_id())
        .expect("catalogue statement")
}
