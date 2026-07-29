//! Ranked-session setup in the exact final-2026 order.

use std::time::Duration;

use crate::connection::client::RmdbClient;
use crate::connection::prepared::PrepareResponse;
use crate::connection::wire::StreamResponse;
use crate::error::TpccError;

use super::catalog::{final2026_catalog, validate_catalog};

pub const SNAPSHOT_ISOLATION_SQL: &str = "SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;";

/// Open, negotiate, configure, and prepare one connection before the timing
/// barrier. The caller must retain this connection across warmup and all three
/// formal windows.
pub async fn open_ranked_session(
    host: &str,
    port: u16,
    response_timeout: Duration,
) -> Result<RmdbClient, TpccError> {
    let mut client = RmdbClient::connect_with_timeout(host, port, response_timeout).await?;
    configure_snapshot_isolation(&mut client).await?;

    let catalog = final2026_catalog();
    validate_catalog(&catalog)
        .map_err(|error| TpccError::Protocol(format!("invalid ranked catalogue: {error}")))?;
    match client.prepare_set(&catalog).await? {
        PrepareResponse::Installed => Ok(client),
        PrepareResponse::Error { diagnostic } => Err(TpccError::QueryError(format!(
            "PREPARE_SET failed: {diagnostic}"
        ))),
    }
}

async fn configure_snapshot_isolation(client: &mut RmdbClient) -> Result<(), TpccError> {
    match client.exec_stream(SNAPSHOT_ISOLATION_SQL).await? {
        StreamResponse::CommandOk => Ok(()),
        StreamResponse::TransactionAbort { diagnostic } => Err(TpccError::Abort(format!(
            "setting SNAPSHOT ISOLATION aborted: {diagnostic}"
        ))),
        StreamResponse::Error { diagnostic } => Err(TpccError::QueryError(format!(
            "setting SNAPSHOT ISOLATION failed: {diagnostic}"
        ))),
        StreamResponse::Query { .. } => Err(TpccError::Protocol(
            "SET SNAPSHOT ISOLATION returned a query result".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranked_setup_sql_is_exact_and_does_not_change_output_mode() {
        assert_eq!(
            SNAPSHOT_ISOLATION_SQL,
            "SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;"
        );
        assert!(!SNAPSHOT_ISOLATION_SQL.contains("OUTPUT"));
        let catalog = final2026_catalog();
        validate_catalog(&catalog).unwrap();
        assert!(!catalog
            .iter()
            .any(|statement| statement.sql.contains("OUTPUT_FILE")));
    }
}
