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
}
