use tracing::trace;

use crate::connection::client::RmdbClient;
use crate::connection::wire::{StreamResponse, WireValue};
use crate::error::TpccError;

/// SQL parameter value for substitution on the general EXEC_STREAM path.
#[derive(Debug, Clone)]
pub enum SqlParam {
    Int(i64),
    Float(f64),
    Str(String),
    Null,
}

impl SqlParam {
    fn to_sql_literal(&self) -> Result<String, TpccError> {
        match self {
            SqlParam::Int(value) => Ok(value.to_string()),
            SqlParam::Float(value) => {
                let narrowed = *value as f32;
                if !narrowed.is_finite() {
                    return Err(TpccError::QueryError(
                        "FLOAT 参数必须能表示为有限 binary32".to_owned(),
                    ));
                }
                Ok(narrowed.to_string())
            }
            SqlParam::Str(value) => {
                if value.as_bytes().contains(&0) {
                    return Err(TpccError::QueryError(
                        "CHAR 参数不得包含 NUL 字节".to_owned(),
                    ));
                }
                Ok(format!("'{}'", value.replace('\'', "''")))
            }
            SqlParam::Null => Ok("NULL".to_owned()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

impl QueryResult {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }
}

pub struct RmdbCursor {
    client: RmdbClient,
}

impl RmdbCursor {
    pub fn new(client: RmdbClient) -> Self {
        Self { client }
    }

    /// Substitute only placeholders outside SQL string literals.
    fn build_query(
        sql: &str,
        params: &[SqlParam],
        append_semicolon: bool,
    ) -> Result<String, TpccError> {
        let mut query = String::with_capacity(sql.len() + params.len() * 8 + 1);
        let mut params = params.iter();
        let mut chars = sql.chars().peekable();
        let mut quoted = false;

        while let Some(ch) = chars.next() {
            if ch == '\'' {
                query.push(ch);
                if quoted && chars.peek() == Some(&'\'') {
                    query.push(chars.next().expect("peeked quote"));
                } else {
                    quoted = !quoted;
                }
            } else if ch == '?' && !quoted {
                let param = params.next().ok_or_else(|| {
                    TpccError::QueryError("SQL 占位符数量多于参数数量".to_owned())
                })?;
                query.push_str(&param.to_sql_literal()?);
            } else {
                query.push(ch);
            }
        }

        if quoted {
            return Err(TpccError::QueryError(
                "SQL 包含未闭合的字符串字面量".to_owned(),
            ));
        }
        if params.next().is_some() {
            return Err(TpccError::QueryError(
                "SQL 参数数量多于占位符数量".to_owned(),
            ));
        }
        if append_semicolon && !query.trim_end().ends_with(';') {
            query.push(';');
        }
        Ok(query)
    }

    async fn execute_update_query(&mut self, query: String) -> Result<(), TpccError> {
        trace!("执行更新: {query}");

        match self.client.exec_stream(&query).await? {
            StreamResponse::CommandOk => Ok(()),
            StreamResponse::TransactionAbort { diagnostic } => Err(TpccError::Abort(diagnostic)),
            StreamResponse::Error { diagnostic } => Err(TpccError::QueryError(diagnostic)),
            StreamResponse::Query { .. } => {
                Err(TpccError::Protocol("更新语句意外返回查询结果".to_owned()))
            }
        }
    }

    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<QueryResult, TpccError> {
        let query = Self::build_query(sql, params, true)?;
        trace!("执行 SQL: {query}");

        match self.client.exec_stream(&query).await? {
            StreamResponse::Query { columns, rows } => Ok(QueryResult {
                columns: columns.into_iter().map(|column| column.name).collect(),
                rows: rows
                    .into_iter()
                    .map(|row| row.into_iter().map(wire_value_text).collect())
                    .collect(),
            }),
            StreamResponse::CommandOk => Err(TpccError::Protocol(
                "查询语句意外返回 COMMAND_OK".to_owned(),
            )),
            StreamResponse::TransactionAbort { diagnostic } => Err(TpccError::Abort(diagnostic)),
            StreamResponse::Error { diagnostic } => Err(TpccError::QueryError(diagnostic)),
        }
    }

    pub async fn execute_update(
        &mut self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<(), TpccError> {
        let query = Self::build_query(sql, params, true)?;
        self.execute_update_query(query).await
    }

    pub async fn execute_update_raw(
        &mut self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<(), TpccError> {
        let query = Self::build_query(sql, params, false)?;
        self.execute_update_query(query).await
    }

    pub fn into_client(self) -> RmdbClient {
        self.client
    }

    pub fn client_mut(&mut self) -> &mut RmdbClient {
        &mut self.client
    }
}

fn wire_value_text(value: WireValue) -> String {
    match value {
        WireValue::Null => "NULL".to_owned(),
        WireValue::Int32(value) => value.to_string(),
        WireValue::Float32(bits) => f32::from_bits(bits).to_string(),
        WireValue::Char(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_only_unquoted_placeholders_and_escapes_char() {
        let query = RmdbCursor::build_query(
            "SELECT '?' AS marker FROM t WHERE id=? AND name=?",
            &[SqlParam::Int(7), SqlParam::Str("O'Brien".to_owned())],
            true,
        )
        .unwrap();

        assert_eq!(
            query,
            "SELECT '?' AS marker FROM t WHERE id=7 AND name='O''Brien';"
        );
    }

    #[test]
    fn float_literal_round_trips_through_binary32() {
        let source = 1.00000006_f64;
        let literal = SqlParam::Float(source).to_sql_literal().unwrap();
        let parsed = literal.parse::<f32>().unwrap();

        assert_eq!(parsed.to_bits(), (source as f32).to_bits());
    }

    #[test]
    fn rejects_placeholder_mismatch_and_non_finite_float() {
        assert!(RmdbCursor::build_query("SELECT ?", &[], true).is_err());
        assert!(RmdbCursor::build_query("SELECT 1", &[SqlParam::Int(1)], true).is_err());
        assert!(SqlParam::Float(f64::INFINITY).to_sql_literal().is_err());
    }
}
