use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{debug, trace};

use crate::connection::prepared::{BatchResponse, Operation, PrepareResponse, Statement};
use crate::connection::wire::{StreamResponse, WireConnection, WireError, WireValue};
use crate::error::TpccError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(300);

pub struct RmdbClient {
    connection: WireConnection<TcpStream>,
    response_timeout: Duration,
}

impl RmdbClient {
    /// Run the exact readiness request without imposing a client-local
    /// connect or response timeout.
    ///
    /// The workflow supervisor owns the single monotonic deadline that also
    /// covers server launch, process registration, and listener ownership.
    /// Keeping this path unbounded internally lets an already connected
    /// fragmented response consume all of that remaining shared budget.
    pub async fn probe_readiness(host: &str, port: u16) -> Result<(), TpccError> {
        let addr = format!("{host}:{port}");
        debug!("正在连接 RMDB readiness endpoint: {addr}");
        let mut connection = WireConnection::connect(&addr)
            .await
            .map_err(map_wire_error)?;
        let response = connection
            .exec_stream("show tables;")
            .await
            .map_err(|error| map_exec_wire_error(error, "show tables;"))?;
        validate_readiness_response(response)
    }

    pub async fn connect(host: &str, port: u16) -> Result<Self, TpccError> {
        Self::connect_with_timeout(host, port, DEFAULT_RESPONSE_TIMEOUT).await
    }

    pub async fn connect_with_timeout(
        host: &str,
        port: u16,
        response_timeout: Duration,
    ) -> Result<Self, TpccError> {
        let addr = format!("{host}:{port}");
        debug!("正在连接 RMDB: {addr}");

        let connection = tokio::time::timeout(CONNECT_TIMEOUT, WireConnection::connect(&addr))
            .await
            .map_err(|_| TpccError::Timeout {
                context: format!("连接及 Wire v3 握手 {addr} 超时 ({CONNECT_TIMEOUT:?})"),
            })?
            .map_err(map_wire_error)?;

        debug!("已连接到 RMDB 且完成 Wire v3 握手: {addr}");
        Ok(Self {
            connection,
            response_timeout,
        })
    }

    pub async fn exec_stream(&mut self, cmd: &str) -> Result<StreamResponse, TpccError> {
        trace!("发送 SQL: {cmd}");

        self.connection
            .exec_stream_with_timeout(cmd, self.response_timeout)
            .await
            .map_err(|error| map_exec_wire_error(error, cmd))
    }

    pub async fn prepare_set(
        &mut self,
        statements: &[Statement],
    ) -> Result<PrepareResponse, TpccError> {
        self.connection
            .prepare_set_with_timeout(statements, self.response_timeout)
            .await
            .map_err(map_wire_error)
    }

    pub async fn exec_batch(
        &mut self,
        operations: &[Operation],
    ) -> Result<BatchResponse, TpccError> {
        self.connection
            .exec_batch_with_timeout(operations, self.response_timeout)
            .await
            .map_err(map_wire_error)
    }

    /// Transitional text adapter for legacy callers.
    ///
    /// New code should use `exec_stream` so FLOAT32 raw bits and terminal
    /// status are never flattened into text.
    pub async fn send_cmd(&mut self, cmd: &str) -> Result<String, TpccError> {
        Ok(response_as_legacy_text(self.exec_stream(cmd).await?))
    }

    pub async fn close(self) {
        let mut stream = self.connection.into_inner();
        let _ = stream.shutdown().await;
        debug!("RMDB 连接已关闭");
    }

    /// Official readiness is a complete framed execution of this exact SQL,
    /// not merely a successful TCP connect.
    pub async fn ping(&mut self) -> Result<(), TpccError> {
        validate_readiness_response(self.exec_stream("show tables;").await?)
    }
}

fn validate_readiness_response(response: StreamResponse) -> Result<(), TpccError> {
    match response {
        StreamResponse::CommandOk | StreamResponse::Query { .. } => Ok(()),
        StreamResponse::TransactionAbort { diagnostic } => Err(TpccError::Abort(diagnostic)),
        StreamResponse::Error { diagnostic } => Err(TpccError::QueryError(diagnostic)),
    }
}

fn map_wire_error(error: WireError) -> TpccError {
    match error {
        WireError::Io(error) => TpccError::Connection(error.to_string()),
        WireError::Protocol(message) => TpccError::Protocol(message),
        WireError::Timeout {
            request,
            phase,
            timeout,
        } => TpccError::Timeout {
            context: format!("{request} Wire {phase} timeout ({timeout:?})"),
        },
    }
}

fn map_exec_wire_error(error: WireError, sql: &str) -> TpccError {
    match error {
        WireError::Timeout {
            request,
            phase,
            timeout,
        } => TpccError::Timeout {
            context: format!("{request} Wire {phase} timeout ({timeout:?}), last sent SQL: {sql}"),
        },
        other => map_wire_error(other),
    }
}

fn response_as_legacy_text(response: StreamResponse) -> String {
    match response {
        StreamResponse::CommandOk => "OK".to_owned(),
        StreamResponse::TransactionAbort { diagnostic } => format!("abort {diagnostic}"),
        StreamResponse::Error { diagnostic } => format!("Error {diagnostic}"),
        StreamResponse::Query { columns, rows } => {
            let mut output = String::new();
            output.push('|');
            for column in columns {
                output.push_str(&column.name);
                output.push('|');
            }
            output.push('\n');
            for row in rows {
                output.push('|');
                for value in row {
                    output.push_str(&wire_value_text(value));
                    output.push('|');
                }
                output.push('\n');
            }
            output
        }
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
    use crate::connection::wire::{WireTimeoutPhase, HANDSHAKE};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::time::Instant;

    #[test]
    fn readiness_accepts_both_published_success_terminals() {
        assert!(validate_readiness_response(StreamResponse::CommandOk).is_ok());
        assert!(validate_readiness_response(StreamResponse::Query {
            columns: Vec::new(),
            rows: Vec::new(),
        })
        .is_ok());
    }

    #[test]
    fn readiness_rejects_abort_and_error_terminals() {
        assert!(
            validate_readiness_response(StreamResponse::TransactionAbort {
                diagnostic: "conflict".to_owned(),
            })
            .is_err()
        );
        assert!(validate_readiness_response(StreamResponse::Error {
            diagnostic: "failed".to_owned(),
        })
        .is_err());
    }

    #[test]
    fn timeout_mapping_distinguishes_send_and_response_read() {
        let send = map_wire_error(WireError::Timeout {
            request: "EXEC_BATCH",
            phase: WireTimeoutPhase::RequestSend,
            timeout: Duration::from_secs(1),
        })
        .to_string();
        assert!(send.contains("request send timeout"));

        let read = map_exec_wire_error(
            WireError::Timeout {
                request: "EXEC_STREAM",
                phase: WireTimeoutPhase::ResponseRead,
                timeout: Duration::from_secs(2),
            },
            "show tables;",
        )
        .to_string();
        assert!(read.contains("response read timeout"));
        assert!(read.contains("show tables;"));
    }

    #[tokio::test]
    async fn readiness_allows_fragmented_terminal_after_two_seconds() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0_u8; HANDSHAKE.len()];
            socket.read_exact(&mut handshake).await.unwrap();
            assert_eq!(handshake, HANDSHAKE);
            socket.write_all(&HANDSHAKE).await.unwrap();

            let mut header = [0_u8; 8];
            socket.read_exact(&mut header).await.unwrap();
            assert_eq!(header[4], 0x20);
            let payload_bytes =
                u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            let mut payload = vec![0_u8; payload_bytes];
            socket.read_exact(&mut payload).await.unwrap();
            assert_eq!(payload, b"show tables;");

            let command_ok = [0_u8, 0, 0, 0, 0x10, 0, 0, 0];
            socket.write_all(&command_ok[..4]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(2_100)).await;
            socket.write_all(&command_ok[4..]).await.unwrap();
        });

        let started = Instant::now();
        tokio::time::timeout(
            Duration::from_secs(4),
            RmdbClient::probe_readiness("127.0.0.1", port),
        )
        .await
        .expect("test safety timeout elapsed")
        .unwrap();
        assert!(started.elapsed() >= Duration::from_secs(2));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn readiness_reports_eof_for_supervisor_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0_u8; HANDSHAKE.len()];
            socket.read_exact(&mut handshake).await.unwrap();
        });

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            RmdbClient::probe_readiness("127.0.0.1", port),
        )
        .await
        .expect("readiness EOF did not fail promptly")
        .unwrap_err()
        .to_string();
        assert!(error.contains("连接失败"), "{error}");
        server.await.unwrap();
    }
}
