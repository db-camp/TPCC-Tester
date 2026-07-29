//! Typed Wire-v3 executor for public-spec consistency plans.

use std::collections::BTreeMap;

use tracing::{info, warn};

use crate::connection::client::RmdbClient;
use crate::connection::wire::{StreamResponse, WireValue};
use crate::consistency::{
    float_aggregate_plan, setup_plan, CheckQuery, CheckScope, ConsistencyPlan, FloatAggregateId,
    SetupExpectations, TypedResult, TypedValue, FLOAT_AGGREGATES, PUBLIC_SPEC_NOTICE,
};
use crate::error::TpccError;
use crate::run_state::DatasetState;

pub async fn run_setup(client: &mut RmdbClient, dataset: &DatasetState) -> Result<(), TpccError> {
    let plan = setup_plan(SetupExpectations {
        warehouses: dataset.warehouses,
        order_line_rows: dataset.order_line_rows,
        undelivered_order_line_rows: dataset.undelivered_order_line_rows,
    })
    .map_err(|error| TpccError::Protocol(format!("invalid setup plan: {error}")))?;
    run_plan(client, &plan).await
}

pub async fn run_plan(client: &mut RmdbClient, plan: &ConsistencyPlan) -> Result<(), TpccError> {
    warn!("{PUBLIC_SPEC_NOTICE}");
    for query in &plan.queries {
        let result = execute_query(client, query).await?;
        query.validate(&result).map_err(|error| {
            TpccError::QueryError(format!(
                "consistency check {} ({}) failed: {error}",
                query.id, query.description
            ))
        })?;
        info!("consistency PASS: {} — {}", query.id, query.description);
    }
    Ok(())
}

pub async fn read_float_aggregates(
    client: &mut RmdbClient,
    scope: CheckScope,
) -> Result<BTreeMap<FloatAggregateId, u32>, TpccError> {
    let plan = float_aggregate_plan(scope);
    let mut values = BTreeMap::new();
    for (spec, query) in FLOAT_AGGREGATES.iter().zip(&plan.queries) {
        let result = execute_query(client, query).await?;
        query.validate(&result).map_err(|error| {
            TpccError::QueryError(format!(
                "FLOAT32 consistency check {} failed: {error}",
                query.id
            ))
        })?;
        let bits = match result.rows.as_slice() {
            [row] => match row.as_slice() {
                [TypedValue::Float32(bits)] => *bits,
                _ => unreachable!("validated FLOAT32 scalar shape"),
            },
            _ => unreachable!("validated FLOAT32 scalar shape"),
        };
        values.insert(spec.id, bits);
    }
    Ok(values)
}

async fn execute_query(
    client: &mut RmdbClient,
    query: &CheckQuery,
) -> Result<TypedResult, TpccError> {
    let sql = terminated_sql(&query.sql);
    match client.exec_stream(&sql).await? {
        StreamResponse::Query { rows, .. } => Ok(TypedResult {
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(typed_value).collect())
                .collect(),
        }),
        StreamResponse::CommandOk => Err(TpccError::Protocol(format!(
            "consistency query {} returned COMMAND_OK",
            query.id
        ))),
        StreamResponse::TransactionAbort { diagnostic } => Err(TpccError::Abort(format!(
            "consistency query {} aborted: {diagnostic}",
            query.id
        ))),
        StreamResponse::Error { diagnostic } => Err(TpccError::QueryError(format!(
            "consistency query {} failed: {diagnostic}",
            query.id
        ))),
    }
}

fn typed_value(value: WireValue) -> TypedValue {
    match value {
        WireValue::Null => TypedValue::Null,
        WireValue::Int32(value) => TypedValue::Int32(value),
        WireValue::Float32(bits) => TypedValue::Float32(bits),
        WireValue::Char(bytes) => TypedValue::Char(bytes),
    }
}

fn terminated_sql(sql: &str) -> String {
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

    #[test]
    fn sql_is_terminated_once_and_float_bits_are_not_formatted() {
        assert_eq!(
            terminated_sql(" SELECT COUNT(*) FROM item "),
            "SELECT COUNT(*) FROM item;"
        );
        assert_eq!(terminated_sql("show tables;"), "show tables;");
        assert_eq!(
            typed_value(WireValue::Float32(0x7f7f_ffff)),
            TypedValue::Float32(0x7f7f_ffff)
        );
    }
}
