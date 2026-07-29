//! Bounded typed recovery probes backed only by sealed terminal evidence.
//!
//! Every probe is one anonymous-schema `EXEC_STREAM` query. Point-query rows
//! are folded into a fixed expected set, so neither a large response nor a
//! server diagnostic can become part of the tester's retained state or error
//! output.

use crate::connection::client::RmdbClient;
use crate::connection::wire::{
    Column, FoldStreamResponse, SqlType, WireError, WireResult, WireValue,
};
use crate::error::TpccError;
use crate::ranking::rich_recovery_samples::{SealedNewOrderSample, SealedRichRecoverySamples};
use crate::runtime_schema::RuntimeSchema;

const MAX_EXACT_POINT_ROWS: usize = 15;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SampleDomain {
    NewOrder,
    Delivery,
    Stock,
    Customer,
    BadCredit,
    History,
}

impl SampleDomain {
    const fn label(self) -> &'static str {
        match self {
            Self::NewOrder => "new-order",
            Self::Delivery => "delivery",
            Self::Stock => "stock",
            Self::Customer => "customer",
            Self::BadCredit => "bad-credit",
            Self::History => "history",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueryScope {
    domain: SampleDomain,
    ordinal: usize,
}

impl QueryScope {
    const fn new(domain: SampleDomain, ordinal: usize) -> Self {
        Self { domain, ordinal }
    }

    fn message(self, category: &str) -> String {
        format!(
            "recovery {} sample query {} {category}",
            self.domain.label(),
            self.ordinal
        )
    }
}

#[derive(Debug)]
struct ExpectedPointQuery {
    scope: QueryScope,
    logical_sql: String,
    column_types: Vec<SqlType>,
    expected_rows: Vec<Vec<WireValue>>,
}

impl ExpectedPointQuery {
    fn new(
        scope: QueryScope,
        logical_sql: String,
        column_types: Vec<SqlType>,
        expected_rows: Vec<Vec<WireValue>>,
    ) -> Result<Self, TpccError> {
        if column_types.is_empty() || expected_rows.len() > MAX_EXACT_POINT_ROWS {
            return Err(TpccError::Protocol(
                scope.message("has invalid bounded expectations"),
            ));
        }
        for (index, row) in expected_rows.iter().enumerate() {
            if row.len() != column_types.len()
                || row
                    .iter()
                    .zip(&column_types)
                    .any(|(value, sql_type)| !wire_value_has_type(value, *sql_type))
            {
                return Err(TpccError::Protocol(
                    scope.message("has invalid typed expectations"),
                ));
            }
            if expected_rows[..index].contains(row) {
                return Err(TpccError::Protocol(
                    scope.message("has duplicate expected rows"),
                ));
            }
        }
        Ok(Self {
            scope,
            logical_sql,
            column_types,
            expected_rows,
        })
    }
}

#[derive(Debug)]
struct ExactRowsFold {
    expected_types: Vec<SqlType>,
    unmatched_rows: Vec<Vec<WireValue>>,
    expected_row_count: u64,
    saw_meta: bool,
}

impl ExactRowsFold {
    fn new(query: &ExpectedPointQuery) -> Self {
        Self {
            expected_types: query.column_types.clone(),
            unmatched_rows: query.expected_rows.clone(),
            expected_row_count: query.expected_rows.len() as u64,
            saw_meta: false,
        }
    }

    fn accept_meta(&mut self, columns: &[Column]) -> WireResult<()> {
        if columns.len() != self.expected_types.len()
            || columns
                .iter()
                .zip(&self.expected_types)
                .any(|(column, expected)| column.sql_type != *expected)
        {
            return Err(WireError::Protocol(
                "recovery sample metadata mismatch".to_owned(),
            ));
        }
        self.saw_meta = true;
        Ok(())
    }

    fn accept_row(&mut self, row: Vec<WireValue>) -> WireResult<()> {
        if !self.saw_meta
            || row.len() != self.expected_types.len()
            || row
                .iter()
                .zip(&self.expected_types)
                .any(|(value, expected)| !wire_value_has_type(value, *expected))
        {
            return Err(WireError::Protocol(
                "recovery sample row type mismatch".to_owned(),
            ));
        }
        let Some(index) = self
            .unmatched_rows
            .iter()
            .position(|expected| expected == &row)
        else {
            return Err(WireError::Protocol(
                "recovery sample row mismatch".to_owned(),
            ));
        };
        self.unmatched_rows.swap_remove(index);
        Ok(())
    }

    fn finish(self, row_count: u64) -> WireResult<()> {
        if !self.saw_meta || row_count != self.expected_row_count || !self.unmatched_rows.is_empty()
        {
            return Err(WireError::Protocol(
                "recovery sample result cardinality mismatch".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
struct NewOrderLineExpected {
    number: i32,
    item_id: i32,
    supply_warehouse: i32,
    delivery_timestamp: Vec<u8>,
    quantity: i32,
    amount_bits: u32,
    district_info: Vec<u8>,
}

#[derive(Debug, Eq, PartialEq)]
struct NewOrderExpected {
    warehouse_id: i32,
    district_id: i32,
    order_id: i32,
    customer_id: i32,
    entry_timestamp: Vec<u8>,
    carrier_id: i32,
    line_count: i32,
    all_local: i32,
    queue_present: bool,
    lines: Vec<NewOrderLineExpected>,
}

impl NewOrderExpected {
    fn from_sealed(sample: &SealedNewOrderSample, ordinal: usize) -> Result<Self, TpccError> {
        let scope = QueryScope::new(SampleDomain::NewOrder, ordinal);
        let key = sample.key();
        let lines = sample
            .lines()
            .iter()
            .map(|line| {
                Ok(NewOrderLineExpected {
                    number: i32::from(line.number()),
                    item_id: i32::try_from(line.item_id()).map_err(|_| {
                        TpccError::Protocol(scope.message("has invalid sealed evidence"))
                    })?,
                    supply_warehouse: i32::from(line.supply_warehouse()),
                    delivery_timestamp: line.delivery_timestamp().to_vec(),
                    quantity: i32::from(line.quantity()),
                    amount_bits: line.amount_bits(),
                    district_info: line.district_info().to_vec(),
                })
            })
            .collect::<Result<Vec<_>, TpccError>>()?;
        if lines.len() != usize::from(sample.line_count()) {
            return Err(TpccError::Protocol(
                scope.message("has invalid sealed evidence"),
            ));
        }
        Ok(Self {
            warehouse_id: i32::from(key.warehouse_id()),
            district_id: i32::from(key.district_id()),
            order_id: key.order_id(),
            customer_id: i32::from(sample.customer_id()),
            entry_timestamp: sample.entry_timestamp().to_vec(),
            carrier_id: i32::from(sample.carrier_id()),
            line_count: i32::from(sample.line_count()),
            all_local: i32::from(sample.all_local()),
            queue_present: sample.queue_present(),
            lines,
        })
    }
}

async fn check_new_order_samples(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    rich: &SealedRichRecoverySamples,
) -> Result<(), TpccError> {
    require_nonempty_samples(
        SampleDomain::NewOrder,
        rich.new_order_commit_count(),
        rich.new_orders().len(),
    )?;
    for (sample_index, sample) in rich.new_orders().iter().enumerate() {
        let base_ordinal = checked_query_ordinal(SampleDomain::NewOrder, sample_index, 3)?;
        let expected = NewOrderExpected::from_sealed(sample, base_ordinal)?;
        for query in new_order_point_queries(&expected, base_ordinal)? {
            execute_exact_point_query(client, schema, query).await?;
        }
    }
    Ok(())
}

fn new_order_point_queries(
    sample: &NewOrderExpected,
    base_ordinal: usize,
) -> Result<Vec<ExpectedPointQuery>, TpccError> {
    let orders_scope = QueryScope::new(SampleDomain::NewOrder, base_ordinal);
    let queue_scope = QueryScope::new(SampleDomain::NewOrder, base_ordinal + 1);
    let lines_scope = QueryScope::new(SampleDomain::NewOrder, base_ordinal + 2);
    let key_predicate = format!(
        "orders.o_w_id = {} AND orders.o_d_id = {} AND orders.o_id = {}",
        sample.warehouse_id, sample.district_id, sample.order_id
    );
    let order = ExpectedPointQuery::new(
        orders_scope,
        format!(
            "SELECT orders.o_id, orders.o_d_id, orders.o_w_id, orders.o_c_id, orders.o_entry_d, orders.o_carrier_id, orders.o_ol_cnt, orders.o_all_local FROM orders WHERE {key_predicate}"
        ),
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
        ],
        vec![vec![
            WireValue::Int32(sample.order_id),
            WireValue::Int32(sample.district_id),
            WireValue::Int32(sample.warehouse_id),
            WireValue::Int32(sample.customer_id),
            WireValue::Char(sample.entry_timestamp.clone()),
            WireValue::Int32(sample.carrier_id),
            WireValue::Int32(sample.line_count),
            WireValue::Int32(sample.all_local),
        ]],
    )?;
    let queue_rows = sample
        .queue_present
        .then(|| {
            vec![
                WireValue::Int32(sample.order_id),
                WireValue::Int32(sample.district_id),
                WireValue::Int32(sample.warehouse_id),
            ]
        })
        .into_iter()
        .collect();
    let queue = ExpectedPointQuery::new(
        queue_scope,
        format!(
            "SELECT new_orders.no_o_id, new_orders.no_d_id, new_orders.no_w_id FROM new_orders WHERE new_orders.no_w_id = {} AND new_orders.no_d_id = {} AND new_orders.no_o_id = {}",
            sample.warehouse_id, sample.district_id, sample.order_id
        ),
        vec![SqlType::Int32, SqlType::Int32, SqlType::Int32],
        queue_rows,
    )?;
    let line_rows = sample
        .lines
        .iter()
        .map(|line| {
            vec![
                WireValue::Int32(sample.order_id),
                WireValue::Int32(sample.district_id),
                WireValue::Int32(sample.warehouse_id),
                WireValue::Int32(line.number),
                WireValue::Int32(line.item_id),
                WireValue::Int32(line.supply_warehouse),
                WireValue::Char(line.delivery_timestamp.clone()),
                WireValue::Int32(line.quantity),
                WireValue::Float32(line.amount_bits),
                WireValue::Char(line.district_info.clone()),
            ]
        })
        .collect();
    let lines = ExpectedPointQuery::new(
        lines_scope,
        format!(
            "SELECT order_line.ol_o_id, order_line.ol_d_id, order_line.ol_w_id, order_line.ol_number, order_line.ol_i_id, order_line.ol_supply_w_id, order_line.ol_delivery_d, order_line.ol_quantity, order_line.ol_amount, order_line.ol_dist_info FROM order_line WHERE order_line.ol_w_id = {} AND order_line.ol_d_id = {} AND order_line.ol_o_id = {}",
            sample.warehouse_id, sample.district_id, sample.order_id
        ),
        vec![
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Int32,
            SqlType::Char,
            SqlType::Int32,
            SqlType::Float32,
            SqlType::Char,
        ],
        line_rows,
    )?;
    Ok(vec![order, queue, lines])
}

async fn execute_exact_point_query(
    client: &mut RmdbClient,
    schema: &RuntimeSchema,
    query: ExpectedPointQuery,
) -> Result<(), TpccError> {
    let scope = query.scope;
    let rendered = render_and_only_point_sql(schema, scope, &query.logical_sql)?;
    let state = ExactRowsFold::new(&query);
    let response = client
        .exec_stream_fold(
            &rendered,
            state,
            |columns, state| state.accept_meta(columns),
            |_, row, state| state.accept_row(row),
        )
        .await
        .map_err(|error| sanitize_exec_error(scope, error))?;
    finish_exact_response(scope, response)
}

fn finish_exact_response(
    scope: QueryScope,
    response: FoldStreamResponse<ExactRowsFold>,
) -> Result<(), TpccError> {
    match response {
        FoldStreamResponse::Query {
            row_count, state, ..
        } => state
            .finish(row_count)
            .map_err(|error| sanitize_exec_error(scope, TpccError::Protocol(error.to_string()))),
        FoldStreamResponse::CommandOk => Err(TpccError::Protocol(
            scope.message("returned a non-query terminal"),
        )),
        FoldStreamResponse::TransactionAbort { diagnostic: _ } => {
            Err(TpccError::Abort(scope.message("aborted")))
        }
        FoldStreamResponse::Error { diagnostic: _ } => {
            Err(TpccError::QueryError(scope.message("failed")))
        }
    }
}

fn render_and_only_point_sql(
    schema: &RuntimeSchema,
    scope: QueryScope,
    logical_sql: &str,
) -> Result<String, TpccError> {
    let tokens = logical_sql
        .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_uppercase())
        .collect::<Vec<_>>();
    let valid_shape = tokens.first().is_some_and(|token| token == "SELECT")
        && tokens.iter().any(|token| token == "WHERE")
        && logical_sql.contains('=')
        && !tokens
            .iter()
            .any(|token| matches!(token.as_str(), "OR" | "UNION" | "LIMIT"));
    if !valid_shape || logical_sql.as_bytes().contains(&0) {
        return Err(TpccError::Protocol(
            scope.message("has invalid point-query shape"),
        ));
    }
    Ok(terminate_sql(&schema.render_sql(logical_sql)))
}

fn sanitize_exec_error(scope: QueryScope, error: TpccError) -> TpccError {
    match error {
        TpccError::Connection(_) | TpccError::Io(_) => {
            TpccError::Connection(scope.message("transport failed"))
        }
        TpccError::Abort(_) => TpccError::Abort(scope.message("aborted")),
        TpccError::QueryError(_) => TpccError::QueryError(scope.message("failed")),
        TpccError::ParseError(_) | TpccError::Protocol(_) => {
            TpccError::Protocol(scope.message("protocol failed"))
        }
        TpccError::Timeout { .. } => TpccError::Timeout {
            context: scope.message("response deadline expired"),
        },
    }
}

fn wire_value_has_type(value: &WireValue, sql_type: SqlType) -> bool {
    matches!(
        (value, sql_type),
        (WireValue::Int32(_), SqlType::Int32)
            | (WireValue::Float32(_), SqlType::Float32)
            | (WireValue::Char(_), SqlType::Char)
    )
}

fn terminate_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.ends_with(';') {
        trimmed.to_owned()
    } else {
        format!("{trimmed};")
    }
}

fn require_nonempty_samples(
    domain: SampleDomain,
    observed_count: u64,
    sample_count: usize,
) -> Result<(), TpccError> {
    if observed_count == 0 || sample_count == 0 {
        Err(TpccError::Protocol(format!(
            "recovery {} sample evidence is empty",
            domain.label()
        )))
    } else {
        Ok(())
    }
}

fn checked_query_ordinal(
    domain: SampleDomain,
    sample_index: usize,
    queries_per_sample: usize,
) -> Result<usize, TpccError> {
    sample_index
        .checked_mul(queries_per_sample)
        .and_then(|ordinal| ordinal.checked_add(1))
        .ok_or_else(|| {
            TpccError::Protocol(format!(
                "recovery {} sample query ordinal overflow",
                domain.label()
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_schema::{LogicalTable, SchemaMode};

    fn columns(types: &[SqlType]) -> Vec<Column> {
        types
            .iter()
            .enumerate()
            .map(|(index, sql_type)| Column {
                name: format!("ignored_{index}"),
                sql_type: *sql_type,
            })
            .collect()
    }

    fn point_query(rows: Vec<Vec<WireValue>>) -> ExpectedPointQuery {
        ExpectedPointQuery::new(
            QueryScope::new(SampleDomain::Stock, 3),
            "SELECT stock.s_w_id, stock.s_i_id FROM stock WHERE stock.s_w_id = 1 AND stock.s_i_id = 2"
                .to_owned(),
            vec![SqlType::Int32, SqlType::Int32],
            rows,
        )
        .unwrap()
    }

    fn delivered_new_order_expected() -> NewOrderExpected {
        NewOrderExpected {
            warehouse_id: 2,
            district_id: 3,
            order_id: 4_001,
            customer_id: 37,
            entry_timestamp: b"2026-07-29 12:34:56".to_vec(),
            carrier_id: 7,
            line_count: 2,
            all_local: 0,
            queue_present: false,
            lines: vec![
                NewOrderLineExpected {
                    number: 1,
                    item_id: 91,
                    supply_warehouse: 2,
                    delivery_timestamp: b"2026-07-29 12:35:00".to_vec(),
                    quantity: 5,
                    amount_bits: (-0.0_f32).to_bits(),
                    district_info: b"ABCDEFGHIJKLMNOPQRSTUVWX".to_vec(),
                },
                NewOrderLineExpected {
                    number: 2,
                    item_id: 92,
                    supply_warehouse: 1,
                    delivery_timestamp: b"2026-07-29 12:35:00".to_vec(),
                    quantity: 6,
                    amount_bits: 0x3f80_0001,
                    district_info: b"abcdefghijklmnopqrstuvwx".to_vec(),
                },
            ],
        }
    }

    #[test]
    fn exact_fold_accepts_reordered_rows_and_ignores_column_labels() {
        let query = point_query(vec![
            vec![WireValue::Int32(1), WireValue::Int32(2)],
            vec![WireValue::Int32(3), WireValue::Int32(4)],
        ]);
        let mut state = ExactRowsFold::new(&query);
        state
            .accept_meta(&columns(&[SqlType::Int32, SqlType::Int32]))
            .unwrap();
        state
            .accept_row(vec![WireValue::Int32(3), WireValue::Int32(4)])
            .unwrap();
        state
            .accept_row(vec![WireValue::Int32(1), WireValue::Int32(2)])
            .unwrap();
        state.finish(2).unwrap();
    }

    #[test]
    fn exact_fold_rejects_duplicate_missing_extra_wrong_type_and_null() {
        let query = point_query(vec![vec![WireValue::Int32(1), WireValue::Int32(2)]]);
        for bad_row in [
            vec![WireValue::Int32(1), WireValue::Int32(2)],
            vec![WireValue::Int32(9), WireValue::Int32(2)],
            vec![WireValue::Float32(1), WireValue::Int32(2)],
            vec![WireValue::Null, WireValue::Int32(2)],
        ] {
            let mut state = ExactRowsFold::new(&query);
            state
                .accept_meta(&columns(&[SqlType::Int32, SqlType::Int32]))
                .unwrap();
            state
                .accept_row(vec![WireValue::Int32(1), WireValue::Int32(2)])
                .unwrap();
            assert!(state.accept_row(bad_row).is_err());
        }

        let mut missing = ExactRowsFold::new(&query);
        missing
            .accept_meta(&columns(&[SqlType::Int32, SqlType::Int32]))
            .unwrap();
        assert!(missing.finish(0).is_err());
        assert!(ExpectedPointQuery::new(
            QueryScope::new(SampleDomain::Stock, 4),
            "SELECT stock.s_w_id FROM stock WHERE stock.s_w_id = 1".to_owned(),
            vec![SqlType::Int32],
            vec![vec![WireValue::Null]],
        )
        .is_err());
    }

    #[test]
    fn metadata_requires_exact_column_types_but_not_names() {
        let query = point_query(Vec::new());
        let mut wrong_count = ExactRowsFold::new(&query);
        assert!(wrong_count
            .accept_meta(&columns(&[SqlType::Int32]))
            .is_err());

        let mut wrong_type = ExactRowsFold::new(&query);
        assert!(wrong_type
            .accept_meta(&columns(&[SqlType::Int32, SqlType::Float32]))
            .is_err());
    }

    #[test]
    fn point_renderer_is_opaque_terminated_and_forbids_wide_shapes() {
        let schema = RuntimeSchema::opaque(0x1234).unwrap();
        assert_eq!(schema.mode(), SchemaMode::LocalSeedOpaqueV1);
        let query = point_query(Vec::new());
        let rendered = render_and_only_point_sql(&schema, query.scope, &query.logical_sql).unwrap();
        assert!(rendered.ends_with(';'));
        for logical in [LogicalTable::Stock.canonical(), "s_w_id", "s_i_id"] {
            assert!(
                !rendered
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || character == '_')
                    })
                    .any(|token| token == logical),
                "logical identifier leaked: {logical}"
            );
        }
        for sql in [
            "SELECT a FROM t WHERE a = 1 OR b = 2",
            "SELECT a FROM t WHERE a = 1 LIMIT 1",
            "SELECT a FROM t UNION SELECT a FROM u WHERE a = 1",
        ] {
            assert!(render_and_only_point_sql(&schema, query.scope, sql).is_err());
        }
    }

    #[test]
    fn all_error_paths_strip_sql_keys_answers_and_server_diagnostics() {
        let scope = QueryScope::new(SampleDomain::Customer, 17);
        let secret = "secret_table key=417 expected=9 actual=10";
        let errors = [
            sanitize_exec_error(scope, TpccError::Connection(secret.to_owned())),
            sanitize_exec_error(scope, TpccError::Abort(secret.to_owned())),
            sanitize_exec_error(scope, TpccError::QueryError(secret.to_owned())),
            sanitize_exec_error(scope, TpccError::ParseError(secret.to_owned())),
            sanitize_exec_error(scope, TpccError::Protocol(secret.to_owned())),
            sanitize_exec_error(
                scope,
                TpccError::Timeout {
                    context: secret.to_owned(),
                },
            ),
            finish_exact_response(
                scope,
                FoldStreamResponse::TransactionAbort {
                    diagnostic: secret.to_owned(),
                },
            )
            .unwrap_err(),
            finish_exact_response(
                scope,
                FoldStreamResponse::Error {
                    diagnostic: secret.to_owned(),
                },
            )
            .unwrap_err(),
        ];
        for error in errors {
            let rendered = error.to_string();
            assert!(!rendered.contains(secret), "{rendered}");
            assert!(!rendered.contains("417"), "{rendered}");
            assert!(rendered.contains("customer"), "{rendered}");
            assert!(rendered.contains("17"), "{rendered}");
        }
    }

    #[test]
    fn every_domain_has_a_fixed_nonsecret_label() {
        assert_eq!(SampleDomain::NewOrder.label(), "new-order");
        assert_eq!(SampleDomain::Delivery.label(), "delivery");
        assert_eq!(SampleDomain::Stock.label(), "stock");
        assert_eq!(SampleDomain::Customer.label(), "customer");
        assert_eq!(SampleDomain::BadCredit.label(), "bad-credit");
        assert_eq!(SampleDomain::History.label(), "history");
    }

    #[test]
    fn new_order_queries_cover_full_rows_and_later_delivery_overlay() {
        let expected = delivered_new_order_expected();
        let queries = new_order_point_queries(&expected, 10).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(
            queries[0].scope,
            QueryScope::new(SampleDomain::NewOrder, 10)
        );
        assert_eq!(
            queries[1].scope,
            QueryScope::new(SampleDomain::NewOrder, 11)
        );
        assert_eq!(
            queries[2].scope,
            QueryScope::new(SampleDomain::NewOrder, 12)
        );
        assert_eq!(
            queries[0].expected_rows,
            vec![vec![
                WireValue::Int32(4_001),
                WireValue::Int32(3),
                WireValue::Int32(2),
                WireValue::Int32(37),
                WireValue::Char(b"2026-07-29 12:34:56".to_vec()),
                WireValue::Int32(7),
                WireValue::Int32(2),
                WireValue::Int32(0),
            ]]
        );
        assert!(queries[1].expected_rows.is_empty());
        assert_eq!(queries[2].expected_rows.len(), 2);
        assert_eq!(
            queries[2].expected_rows[0][8],
            WireValue::Float32((-0.0_f32).to_bits())
        );
        assert_eq!(
            queries[2].expected_rows[1][8],
            WireValue::Float32(0x3f80_0001)
        );
        assert_eq!(
            queries[2].expected_rows[0][6],
            WireValue::Char(b"2026-07-29 12:35:00".to_vec())
        );
    }

    #[test]
    fn new_order_queries_are_opaque_and_and_only_points() {
        let schema = RuntimeSchema::opaque(0x5678).unwrap();
        let queries = new_order_point_queries(&delivered_new_order_expected(), 1).unwrap();
        for query in queries {
            let tokens = query
                .logical_sql
                .split(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .filter(|token| !token.is_empty())
                .map(str::to_ascii_uppercase)
                .collect::<Vec<_>>();
            assert!(tokens.iter().any(|token| token == "WHERE"));
            assert!(!tokens
                .iter()
                .any(|token| matches!(token.as_str(), "OR" | "UNION" | "LIMIT")));

            let rendered =
                render_and_only_point_sql(&schema, query.scope, &query.logical_sql).unwrap();
            for logical in [
                "orders",
                "new_orders",
                "order_line",
                "o_id",
                "no_o_id",
                "ol_o_id",
            ] {
                assert!(!rendered
                    .split(|character: char| {
                        !(character.is_ascii_alphanumeric() || character == '_')
                    })
                    .any(|token| token == logical));
            }
        }
    }

    #[test]
    fn new_order_nonempty_gate_fails_closed() {
        assert!(require_nonempty_samples(SampleDomain::NewOrder, 0, 0).is_err());
        assert!(require_nonempty_samples(SampleDomain::NewOrder, 1, 0).is_err());
        assert!(require_nonempty_samples(SampleDomain::NewOrder, 1, 1).is_ok());
        assert_eq!(
            checked_query_ordinal(SampleDomain::NewOrder, 2, 3).unwrap(),
            7
        );
    }
}
