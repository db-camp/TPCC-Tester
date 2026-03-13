use tracing::{trace, warn};

use crate::connection::client::RmdbClient;
use crate::error::TpccError;

/// SQL parameter value for substitution.
#[derive(Debug, Clone)]
pub enum SqlParam {
    Int(i64),
    Float(f64),
    Str(String),
    Null,
}

impl SqlParam {
    fn to_sql_literal(&self) -> String {
        match self {
            SqlParam::Int(v) => v.to_string(),
            SqlParam::Float(v) => format!("{v}"),
            SqlParam::Str(v) => {
                let escaped = v.replace('\'', "''");
                format!("'{escaped}'")
            }
            SqlParam::Null => "NULL".to_string(),
        }
    }
}

/// Parsed query result: column names + data rows.
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

/// Cursor-like interface over RMDB TCP client.
pub struct RmdbCursor {
    client: RmdbClient,
}

impl RmdbCursor {
    pub fn new(client: RmdbClient) -> Self {
        Self { client }
    }

    /// Build final SQL string with parameter substitution.
    fn build_query(sql: &str, params: &[SqlParam]) -> String {
        let mut query = format!("{sql};");
        for param in params {
            let literal = param.to_sql_literal();
            // Replace first occurrence of ? with the literal value
            if let Some(pos) = query.find('?') {
                query = format!("{}{}{}", &query[..pos], literal, &query[pos + 1..]);
            }
        }
        query
    }

    /// Execute a SQL statement and return parsed results.
    pub async fn execute(
        &mut self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<QueryResult, TpccError> {
        let query = Self::build_query(sql, params);
        trace!("执行 SQL: {query}");

        let response = self.client.send_cmd(&query).await?;

        // Check for abort
        if response.starts_with("abort") {
            return Err(TpccError::Abort(response.trim().to_string()));
        }

        // Check for error or empty
        if response.starts_with("Error") || response.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            });
        }

        // Parse pipe-delimited response
        Self::parse_response(&response)
    }

    /// Execute a statement that doesn't return rows (BEGIN, COMMIT, ROLLBACK, INSERT, UPDATE, DELETE).
    pub async fn execute_update(
        &mut self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<(), TpccError> {
        let query = Self::build_query(sql, params);
        trace!("执行更新: {query}");

        let response = self.client.send_cmd(&query).await?;

        if response.starts_with("abort") {
            return Err(TpccError::Abort(response.trim().to_string()));
        }

        Ok(())
    }

    fn parse_response(response: &str) -> Result<QueryResult, TpccError> {
        let lines: Vec<&str> = response.trim().split('\n').collect();
        if lines.is_empty() {
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
            });
        }

        // Find header line (starts with |)
        let mut header_idx = None;
        for (i, line) in lines.iter().enumerate() {
            if line.starts_with('|') {
                header_idx = Some(i);
                break;
            }
        }

        let header_idx = match header_idx {
            Some(idx) => idx,
            None => {
                return Ok(QueryResult {
                    columns: vec![],
                    rows: vec![],
                })
            }
        };

        // Parse column names from header
        let columns: Vec<String> = lines[header_idx]
            .split('|')
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().to_string())
            .collect();

        // Parse data rows
        let mut rows = Vec::new();
        for line in &lines[header_idx + 1..] {
            if line.starts_with('|') {
                let values: Vec<String> = line
                    .split('|')
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| s.trim().to_string())
                    .collect();
                if values.len() == columns.len() {
                    rows.push(values);
                } else {
                    warn!(
                        "行列数不匹配: 期望 {} 列, 实际 {} 列, 原始行: {line}",
                        columns.len(),
                        values.len()
                    );
                }
            }
        }

        Ok(QueryResult { columns, rows })
    }

    /// Convenience: execute and return the underlying client for reuse.
    pub fn into_client(self) -> RmdbClient {
        self.client
    }

    /// Get mutable reference to client (for ping, etc.).
    pub fn client_mut(&mut self) -> &mut RmdbClient {
        &mut self.client
    }
}
