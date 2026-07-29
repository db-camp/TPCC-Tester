use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tracing::{debug, trace};

use crate::connection::prepared::{BatchResponse, Operation, PrepareResponse, Statement};
use crate::connection::wire::{
    Column, FoldStreamResponse, StreamResponse, WireConnection, WireError, WireResult, WireValue,
};
use crate::error::TpccError;

const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(300);

pub struct RmdbClient {
    connection: WireConnection<TcpStream>,
    response_timeout: Duration,
}

impl RmdbClient {
    /// Run the exact readiness request under one client-local deadline.
    ///
    /// The workflow supervisor owns the single monotonic deadline that also
    /// covers server launch, process registration, and listener ownership,
    /// then passes this process the remaining portion. One Tokio deadline
    /// covers connect, handshake, request send, and the complete terminal.
    pub async fn probe_readiness(
        host: &str,
        port: u16,
        shared_budget: Duration,
    ) -> Result<(), TpccError> {
        let ip = host.parse::<IpAddr>().map_err(|_| {
            TpccError::Connection(format!(
                "readiness host must be a numeric IPv4 or IPv6 address: {host}"
            ))
        })?;
        let addr = SocketAddr::new(ip, port);
        debug!("正在连接 RMDB readiness endpoint: {addr}");
        let probe = async {
            let mut connection = WireConnection::connect(addr)
                .await
                .map_err(map_wire_error)?;
            let response = connection
                .exec_stream("show tables;")
                .await
                .map_err(|error| map_exec_wire_error(error, "show tables;"))?;
            validate_readiness_response(response)
        };
        let local_deadline = tokio::time::Instant::now()
            .checked_add(shared_budget)
            .ok_or_else(|| TpccError::Timeout {
                context: "shared readiness budget cannot be represented".to_owned(),
            })?;
        tokio::time::timeout_at(local_deadline, probe)
            .await
            .map_err(|_| TpccError::Timeout {
                context: format!(
                    "shared readiness deadline exhausted after remaining {shared_budget:?}"
                ),
            })?
    }

    pub async fn connect(host: &str, port: u16) -> Result<Self, TpccError> {
        Self::connect_with_timeout(host, port, DEFAULT_RESPONSE_TIMEOUT).await
    }

    /// Open and negotiate one session under the caller's local I/O deadline.
    ///
    /// The same duration is then reused independently for every complete Wire
    /// request. It is never accumulated across a multi-batch transaction.
    pub async fn connect_with_timeout(
        host: &str,
        port: u16,
        response_timeout: Duration,
    ) -> Result<Self, TpccError> {
        let addr = display_endpoint(host, port);
        debug!("正在连接 RMDB: {addr}");

        let connection =
            tokio::time::timeout(response_timeout, WireConnection::connect((host, port)))
                .await
                .map_err(|_| TpccError::Timeout {
                    context: format!("连接及 Wire v3 握手 {addr} 超时 ({response_timeout:?})"),
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

    /// Execute one typed streaming query while retaining only caller-defined
    /// fold state. SQL text is deliberately omitted from logs and error
    /// mapping so recovery sample keys cannot leak through diagnostics.
    pub async fn exec_stream_fold<T, M, R>(
        &mut self,
        sql: &str,
        state: T,
        on_meta: M,
        on_row: R,
    ) -> Result<FoldStreamResponse<T>, TpccError>
    where
        M: FnMut(&[Column], &mut T) -> WireResult<()>,
        R: FnMut(&[Column], Vec<WireValue>, &mut T) -> WireResult<()>,
    {
        trace!("发送有界流式折叠 SQL");
        self.connection
            .exec_stream_fold_with_timeout(sql, self.response_timeout, state, on_meta, on_row)
            .await
            .map_err(map_wire_error)
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

fn display_endpoint(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
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
    async fn folded_stream_timeout_does_not_disclose_sql() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0_u8; HANDSHAKE.len()];
            socket.read_exact(&mut handshake).await.unwrap();
            socket.write_all(&handshake).await.unwrap();

            let mut header = [0_u8; 8];
            socket.read_exact(&mut header).await.unwrap();
            let payload_bytes = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            let mut payload = vec![0_u8; payload_bytes];
            socket.read_exact(&mut payload).await.unwrap();
            assert!(String::from_utf8(payload)
                .unwrap()
                .contains("sample_secret_417"));
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let mut client =
            RmdbClient::connect_with_timeout("127.0.0.1", port, Duration::from_millis(100))
                .await
                .unwrap();
        let error = client
            .exec_stream_fold(
                "SELECT sample_secret_417;",
                (),
                |_, _| Ok(()),
                |_, _, _| Ok(()),
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("response read timeout"), "{error}");
        assert!(!error.contains("sample_secret_417"), "{error}");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn configured_timeout_covers_connect_and_complete_handshake() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0_u8; HANDSHAKE.len()];
            socket.read_exact(&mut handshake).await.unwrap();
            assert_eq!(handshake, HANDSHAKE);
            tokio::time::sleep(Duration::from_millis(300)).await;
        });

        let timeout = Duration::from_millis(100);
        let started = Instant::now();
        let error = match RmdbClient::connect_with_timeout("127.0.0.1", port, timeout).await {
            Ok(_) => panic!("stalled handshake unexpectedly completed"),
            Err(error) => error,
        };
        assert!(matches!(error, TpccError::Timeout { .. }));
        let message = error.to_string();
        assert!(message.contains("连接及 Wire v3 握手"), "{message}");
        assert!(message.contains("100ms"), "{message}");
        assert!(started.elapsed() < Duration::from_secs(1));
        server.await.unwrap();
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
            let payload_bytes = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
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
            RmdbClient::probe_readiness("127.0.0.1", port, Duration::from_secs(4)),
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
            RmdbClient::probe_readiness("127.0.0.1", port, Duration::from_secs(1)),
        )
        .await
        .expect("readiness EOF did not fail promptly")
        .unwrap_err()
        .to_string();
        assert!(error.contains("连接失败"), "{error}");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn readiness_shared_budget_covers_handshake_and_terminal() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0_u8; HANDSHAKE.len()];
            socket.read_exact(&mut handshake).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            socket.write_all(&handshake).await.unwrap();

            let mut header = [0_u8; 8];
            socket.read_exact(&mut header).await.unwrap();
            let payload_bytes = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            let mut payload = vec![0_u8; payload_bytes];
            socket.read_exact(&mut payload).await.unwrap();
            tokio::time::sleep(Duration::from_millis(60)).await;
            let _ = socket.write_all(&[0_u8, 0, 0, 0, 0x10, 0, 0, 0]).await;
        });

        let started = Instant::now();
        let error = RmdbClient::probe_readiness("127.0.0.1", port, Duration::from_millis(100))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("shared readiness deadline"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
        server.await.unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0_u8; HANDSHAKE.len()];
            socket.read_exact(&mut handshake).await.unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            socket.write_all(&handshake).await.unwrap();

            let mut header = [0_u8; 8];
            socket.read_exact(&mut header).await.unwrap();
            let payload_bytes = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            let mut payload = vec![0_u8; payload_bytes];
            socket.read_exact(&mut payload).await.unwrap();
            tokio::time::sleep(Duration::from_millis(40)).await;
            socket
                .write_all(&[0_u8, 0, 0, 0, 0x10, 0, 0, 0])
                .await
                .unwrap();
        });

        RmdbClient::probe_readiness("127.0.0.1", port, Duration::from_millis(200))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn readiness_reports_refused_connect_for_supervisor_retry() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);

        let started = Instant::now();
        let error = RmdbClient::probe_readiness("127.0.0.1", port, Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("连接失败"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn readiness_accepts_numeric_ipv6_endpoint() {
        let listener = match TcpListener::bind("[::1]:0").await {
            Ok(listener) => listener,
            Err(_) => return,
        };
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut handshake = [0_u8; HANDSHAKE.len()];
            socket.read_exact(&mut handshake).await.unwrap();
            socket.write_all(&handshake).await.unwrap();
            let mut header = [0_u8; 8];
            socket.read_exact(&mut header).await.unwrap();
            let payload_bytes = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
            let mut payload = vec![0_u8; payload_bytes];
            socket.read_exact(&mut payload).await.unwrap();
            socket
                .write_all(&[0_u8, 0, 0, 0, 0x10, 0, 0, 0])
                .await
                .unwrap();
        });

        RmdbClient::probe_readiness("::1", port, Duration::from_secs(1))
            .await
            .unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn readiness_rejects_non_numeric_host_without_dns() {
        let error = RmdbClient::probe_readiness("localhost", 8765, Duration::from_secs(1))
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("numeric IPv4 or IPv6"), "{error}");
    }
}
