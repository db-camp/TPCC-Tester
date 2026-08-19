use tracing::{trace, warn};

use crate::connection::client::{ExecOutcome, ProtocolMode, RmdbClient};
use crate::connection::protocol::{ColumnDef, Value};
use crate::error::TpccError;

/// SQL parameter value for substitution.
#[derive(Debug, Clone)]
pub enum SqlParam {
    Int(i64),
    Float(f64),
    Str(String),
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
        }
    }
}

/// 查询结果：列定义 + 类型化数据行。
///
/// Wire 模式下 cell 为协议解码出的 `Value`（INT32 / FLOAT32 / CHAR / NULL）；
/// legacy 文本协议下 cell 统一为 `Value::Char`，columns 为空。
pub struct QueryResult {
    /// 查询列 schema；后续在线一致性检查（#15）按列类型断言时使用
    #[allow(dead_code)]
    pub columns: Vec<ColumnDef>,
    pub rows: Vec<Vec<Value>>,
}

impl QueryResult {
    fn empty() -> Self {
        Self {
            columns: vec![],
            rows: vec![],
        }
    }

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

        match self.client.mode() {
            ProtocolMode::Wire => match self.client.exec_stream(&query).await? {
                ExecOutcome::Query { columns, rows } => Ok(QueryResult { columns, rows }),
                ExecOutcome::Command => Ok(QueryResult::empty()),
            },
            ProtocolMode::Legacy => self.execute_legacy(&query).await,
        }
    }

    /// Execute a statement that doesn't return rows (BEGIN, COMMIT, ROLLBACK, INSERT, UPDATE, DELETE).
    pub async fn execute_update(
        &mut self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<(), TpccError> {
        let query = Self::build_query(sql, params);
        trace!("执行更新: {query}");

        match self.client.mode() {
            ProtocolMode::Wire => {
                match self.client.exec_stream(&query).await? {
                    ExecOutcome::Command => {}
                    ExecOutcome::Query { .. } => {
                        // 协议规定非查询只返回 COMMAND_OK；结果流已读完，容忍但提示
                        warn!("非查询语句返回了查询结果 (已忽略): {query}");
                    }
                }
                Ok(())
            }
            ProtocolMode::Legacy => {
                let response = self.client.send_cmd(&query).await?;
                if response.starts_with("abort") {
                    return Err(TpccError::Abort(response.trim().to_string()));
                }
                Ok(())
            }
        }
    }

    async fn execute_legacy(&mut self, query: &str) -> Result<QueryResult, TpccError> {
        let response = self.client.send_cmd(query).await?;

        // Check for abort
        if response.starts_with("abort") {
            return Err(TpccError::Abort(response.trim().to_string()));
        }

        // Check for error or empty
        if response.starts_with("Error") || response.is_empty() {
            return Ok(QueryResult::empty());
        }

        // Parse pipe-delimited response
        Ok(Self::parse_legacy_response(&response))
    }

    fn parse_legacy_response(response: &str) -> QueryResult {
        let lines: Vec<&str> = response.trim().split('\n').collect();
        if lines.is_empty() {
            return QueryResult::empty();
        }

        let mut header_idx = None;
        for (i, line) in lines.iter().enumerate() {
            if line.starts_with('|') {
                header_idx = Some(i);
                break;
            }
        }

        let header_idx = match header_idx {
            Some(idx) => idx,
            None => return QueryResult::empty(),
        };

        let col_count = lines[header_idx]
            .split('|')
            .filter(|s| !s.trim().is_empty())
            .count();

        let mut rows = Vec::new();
        for line in &lines[header_idx + 1..] {
            if line.starts_with('|') {
                let values: Vec<Value> = line
                    .split('|')
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| Value::Char(s.trim().to_string()))
                    .collect();
                if values.len() == col_count {
                    rows.push(values);
                } else {
                    warn!(
                        "行列数不匹配: 期望 {} 列, 实际 {} 列, 原始行: {line}",
                        col_count,
                        values.len()
                    );
                }
            }
        }

        QueryResult {
            columns: vec![],
            rows,
        }
    }

    pub async fn close(self) {
        self.client.close().await;
    }

    /// Get mutable reference to client (for ping, etc.).
    pub fn client_mut(&mut self) -> &mut RmdbClient {
        &mut self.client
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_query_substitutes_params_in_order() {
        let q = RmdbCursor::build_query(
            "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?",
            &[
                SqlParam::Int(1),
                SqlParam::Str("x'y".to_string()),
                SqlParam::Float(2.5),
            ],
        );
        assert_eq!(q, "SELECT * FROM t WHERE a = 1 AND b = 'x''y' AND c = 2.5;");
    }

    #[test]
    fn parse_legacy_response_wraps_cells_as_char() {
        let resp = "| c1 | c2 |\n| 10 | ab |\n| 20 | cd |\n";
        let r = RmdbCursor::parse_legacy_response(resp);
        assert_eq!(r.len(), 2);
        assert_eq!(r.rows[0][0].as_i32(), Some(10));
        assert_eq!(r.rows[1][1].as_str(), "cd");
    }
}
