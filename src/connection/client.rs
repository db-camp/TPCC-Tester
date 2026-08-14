//! rmdb 客户端连接层。
//!
//! 默认使用 2026 决赛 RMDB Wire Protocol v3（握手 + 8 字节 frame + EXEC_STREAM，
//! 见赛题附件 A §1–4）；`--legacy-protocol` 保留 2025 及以前的文本协议。

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, trace, warn};

use crate::connection::protocol::{
    self, tag, ColumnDef, FrameHeader, Value, FRAME_HEADER_LEN, HANDSHAKE,
};
use crate::error::TpccError;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(300);

/// 客户端协议模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolMode {
    /// 2026 决赛 Wire Protocol v3（默认）。
    Wire,
    /// 2025 及以前的文本 SQL 协议。
    Legacy,
}

impl ProtocolMode {
    pub fn name(&self) -> &'static str {
        match self {
            ProtocolMode::Wire => "Wire Protocol v3",
            ProtocolMode::Legacy => "legacy 文本协议",
        }
    }
}

fn map_read_err(e: std::io::Error) -> TpccError {
    match e.kind() {
        std::io::ErrorKind::UnexpectedEof => TpccError::Connection(
            "连接已断开 - rmdb 可能已关闭连接，请检查服务状态".to_string(),
        ),
        _ => TpccError::Connection(format!("读取响应失败: {e}")),
    }
}

/// 一次 EXEC_STREAM 的完整结果。
#[derive(Debug)]
pub enum ExecOutcome {
    /// 非查询成功（COMMAND_OK）。
    Command,
    /// 查询成功（META → ROW* → RESULT_END，row_count 已校验）。
    Query {
        columns: Vec<ColumnDef>,
        rows: Vec<Vec<Value>>,
    },
}

#[derive(Debug)]
pub struct RmdbClient {
    stream: TcpStream,
    mode: ProtocolMode,
    /// wire 模式复用的 payload 缓冲。
    payload_buf: Vec<u8>,
    /// legacy 文本协议的读缓冲。
    legacy_buf: Vec<u8>,
}

impl RmdbClient {
    pub async fn connect(
        host: &str,
        port: u16,
        mode: ProtocolMode,
    ) -> Result<Self, TpccError> {
        let addr = format!("{host}:{port}");
        debug!("正在连接 RMDB: {addr} ({})", mode.name());

        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(&addr))
            .await
            .map_err(|_| TpccError::Timeout {
                context: format!("连接 {addr} 超时 (30s)"),
            })?
            .map_err(|e| {
                let msg = match e.kind() {
                    std::io::ErrorKind::ConnectionRefused => {
                        format!("连接被拒绝 - 请确认 rmdb 服务已启动并监听在 {addr}")
                    }
                    std::io::ErrorKind::AddrNotAvailable => {
                        format!("地址不可用 - 请检查地址 {addr} 是否正确")
                    }
                    _ => format!("连接失败: {e}"),
                };
                TpccError::Connection(msg)
            })?;

        let mut client = Self {
            stream,
            mode,
            payload_buf: Vec::new(),
            legacy_buf: vec![0u8; 8192],
        };

        if mode == ProtocolMode::Wire {
            client.handshake().await?;
        }

        debug!("已连接到 RMDB: {addr}");
        Ok(client)
    }

    pub fn mode(&self) -> ProtocolMode {
        self.mode
    }

    /// 8 字节握手：发送 "RMDB"+3+0，服务端必须原样回送后才能进入 frame 交互。
    async fn handshake(&mut self) -> Result<(), TpccError> {
        self.stream
            .write_all(&HANDSHAKE)
            .await
            .map_err(|e| TpccError::Connection(format!("发送握手失败: {e}")))?;

        let mut echo = [0u8; 8];
        tokio::time::timeout(CONNECT_TIMEOUT, self.stream.read_exact(&mut echo))
            .await
            .map_err(|_| TpccError::Timeout {
                context: "等待握手回送超时 (30s)".to_string(),
            })?
            .map_err(|e| match e.kind() {
                // 规范：握手不被支持时服务端应关闭连接（表现为 EOF 或 RST）
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset => {
                    TpccError::Protocol(
                        "服务端在握手阶段关闭连接 - 可能不支持 Wire Protocol v3，\
                         旧文本协议 rmdb 请使用 --legacy-protocol"
                            .to_string(),
                    )
                }
                _ => TpccError::Connection(format!("读取握手回送失败: {e}")),
            })?;

        if echo != HANDSHAKE {
            return Err(TpccError::Protocol(format!(
                "握手回送不匹配: 期望 {HANDSHAKE:02x?}, 实际 {echo:02x?} - \
                 可能不支持 Wire Protocol v3，旧文本协议 rmdb 请使用 --legacy-protocol"
            )));
        }
        trace!("握手完成 (RMDB v3.0)");
        Ok(())
    }

    /// 循环读取一个完整 frame（header + payload），payload 留在 `self.payload_buf`。
    async fn read_frame(&mut self) -> Result<FrameHeader, TpccError> {
        let mut header = [0u8; FRAME_HEADER_LEN];
        tokio::time::timeout(RESPONSE_TIMEOUT, self.stream.read_exact(&mut header))
            .await
            .map_err(|_| TpccError::Timeout {
                context: "等待响应 frame header 超时 (300s)".to_string(),
            })?
            .map_err(map_read_err)?;

        // decode 会先校验 payload 长度上限，再进行分配
        let frame = FrameHeader::decode(&header)?;
        self.payload_buf.resize(frame.payload_len, 0);
        tokio::time::timeout(
            RESPONSE_TIMEOUT,
            self.stream.read_exact(&mut self.payload_buf[..frame.payload_len]),
        )
        .await
        .map_err(|_| TpccError::Timeout {
            context: "等待响应 frame payload 超时 (300s)".to_string(),
        })?
        .map_err(map_read_err)?;

        Ok(frame)
    }

    /// 通过 EXEC_STREAM 执行一条 SQL，驱动 META/ROW/RESULT_END/COMMAND_OK 状态机。
    pub async fn exec_stream(&mut self, sql: &str) -> Result<ExecOutcome, TpccError> {
        if self.mode != ProtocolMode::Wire {
            return Err(TpccError::Protocol(
                "exec_stream 仅在 Wire Protocol 模式下可用".to_string(),
            ));
        }
        trace!("EXEC_STREAM: {sql}");

        let frame = protocol::encode_exec_stream(sql)?;
        self.stream
            .write_all(&frame)
            .await
            .map_err(|e| TpccError::Connection(format!("发送失败: {e}")))?;

        let mut columns: Option<Vec<ColumnDef>> = None;
        let mut rows: Vec<Vec<Value>> = Vec::new();

        loop {
            let frame = self.read_frame().await?;
            if frame.flags != 0 {
                return Err(TpccError::Protocol(format!(
                    "服务端响应 flags 必须为 0, 实际 {:#04x} (tag={})",
                    frame.flags,
                    tag::name(frame.tag)
                )));
            }
            let payload = &self.payload_buf[..frame.payload_len];

            match frame.tag {
                tag::META => {
                    if columns.is_some() {
                        return Err(TpccError::Protocol("同一查询收到重复 META".to_string()));
                    }
                    columns = Some(protocol::parse_meta(payload)?);
                }
                tag::ROW => {
                    let cols = columns.as_ref().ok_or_else(|| {
                        TpccError::Protocol("META 之前收到 ROW".to_string())
                    })?;
                    rows.push(protocol::parse_row(payload, cols)?);
                }
                tag::RESULT_END => {
                    let cols = columns.take().ok_or_else(|| {
                        TpccError::Protocol("META 之前收到 RESULT_END".to_string())
                    })?;
                    let row_count = protocol::parse_result_end(payload)?;
                    if row_count != rows.len() as u64 {
                        return Err(TpccError::Protocol(format!(
                            "RESULT_END row_count={row_count} 与实际 ROW 数 {} 不一致",
                            rows.len()
                        )));
                    }
                    trace!("查询完成: {} 行", rows.len());
                    return Ok(ExecOutcome::Query {
                        columns: cols,
                        rows,
                    });
                }
                tag::COMMAND_OK => {
                    if columns.is_some() {
                        return Err(TpccError::Protocol(
                            "查询结果流中出现 COMMAND_OK (查询只能以 RESULT_END 终结)"
                                .to_string(),
                        ));
                    }
                    if !payload.is_empty() {
                        return Err(TpccError::Protocol(format!(
                            "COMMAND_OK payload 必须为空, 实际 {} 字节",
                            payload.len()
                        )));
                    }
                    return Ok(ExecOutcome::Command);
                }
                tag::TRANSACTION_ABORT => {
                    // 流式结果中途失败：丢弃已收到的 META/ROW
                    let diag = protocol::parse_diagnostic(payload)?;
                    debug!("TRANSACTION_ABORT: {diag}");
                    return Err(TpccError::Abort(diag));
                }
                tag::ERROR => {
                    let diag = protocol::parse_diagnostic(payload)?;
                    debug!("ERROR: {diag}");
                    return Err(TpccError::Server(diag));
                }
                other => {
                    return Err(TpccError::Protocol(format!(
                        "EXEC_STREAM 响应中出现非法 tag {other:#04x} ({})",
                        tag::name(other)
                    )));
                }
            }
        }
    }

    /// legacy 文本协议：发送 SQL 文本并读取一段文本响应。
    pub async fn send_cmd(&mut self, cmd: &str) -> Result<String, TpccError> {
        if self.mode != ProtocolMode::Legacy {
            return Err(TpccError::Protocol(
                "send_cmd 仅在 legacy 文本协议模式下可用".to_string(),
            ));
        }
        trace!("发送 SQL: {cmd}");

        self.stream
            .write_all(cmd.as_bytes())
            .await
            .map_err(|e| TpccError::Connection(format!("发送失败: {e}")))?;

        let n = tokio::time::timeout(RESPONSE_TIMEOUT, self.stream.read(&mut self.legacy_buf))
            .await
            .map_err(|_| TpccError::Timeout {
                context: format!("等待响应超时 (300s), 最后发送的 SQL: {cmd}"),
            })?
            .map_err(|e| TpccError::Connection(format!("读取响应失败: {e}")))?;

        if n == 0 {
            return Err(TpccError::Connection(
                "连接已断开 - rmdb 可能已关闭连接，请检查服务状态".to_string(),
            ));
        }

        let response = String::from_utf8_lossy(&self.legacy_buf[..n]).to_string();
        trace!("收到响应 ({n} bytes): {response}");
        Ok(response)
    }

    /// 连接测活。wire 模式按赛题规范执行精确语句 `show tables;`，
    /// COMMAND_OK 或 META…RESULT_END（含 0 行）均视为就绪。
    pub async fn ping(&mut self) -> Result<(), TpccError> {
        match self.mode {
            ProtocolMode::Wire => {
                match self.exec_stream("show tables;").await? {
                    ExecOutcome::Command => trace!("测活 show tables; 返回 COMMAND_OK"),
                    ExecOutcome::Query { rows, .. } => {
                        trace!("测活 show tables; 返回 {} 行", rows.len())
                    }
                }
                Ok(())
            }
            ProtocolMode::Legacy => {
                let resp = self.send_cmd("BEGIN;").await?;
                if resp.starts_with("abort") || resp.starts_with("Error") {
                    warn!("Ping 响应异常: {resp}");
                }
                let resp = self.send_cmd("COMMIT;").await?;
                if resp.starts_with("abort") || resp.starts_with("Error") {
                    warn!("Ping COMMIT 响应异常: {resp}");
                }
                Ok(())
            }
        }
    }

    pub async fn close(mut self) {
        let _ = self.stream.shutdown().await;
        debug!("RMDB 连接已关闭");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::protocol::SqlType;
    use tokio::net::TcpListener;

    /// 组装一个响应 frame。
    fn frame(t: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.push(t);
        buf.push(flags);
        buf.extend_from_slice(&[0, 0]);
        buf.extend_from_slice(payload);
        buf
    }

    fn meta(cols: &[(&str, u8)]) -> Vec<u8> {
        let mut p = Vec::new();
        p.extend_from_slice(&(cols.len() as u16).to_be_bytes());
        for (name, t) in cols {
            p.extend_from_slice(&(name.len() as u16).to_be_bytes());
            p.extend_from_slice(name.as_bytes());
            p.push(*t);
        }
        frame(tag::META, 0, &p)
    }

    fn result_end(row_count: u64) -> Vec<u8> {
        frame(tag::RESULT_END, 0, &row_count.to_be_bytes())
    }

    async fn read_request(s: &mut TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 8];
        s.read_exact(&mut header).await.unwrap();
        let len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let mut payload = vec![0u8; len];
        s.read_exact(&mut payload).await.unwrap();
        (header[4], payload)
    }

    /// 启动单连接 mock 服务端：正确回送握手，读取一个请求，然后按脚本回写字节。
    /// 返回 (port, 服务端收到的请求)。
    async fn spawn_server(response: Vec<u8>) -> (u16, tokio::task::JoinHandle<(u8, Vec<u8>)>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut hs = [0u8; 8];
            s.read_exact(&mut hs).await.unwrap();
            s.write_all(&hs).await.unwrap();
            let req = read_request(&mut s).await;
            s.write_all(&response).await.unwrap();
            s.flush().await.unwrap();
            req
        });
        (port, handle)
    }

    async fn connect(port: u16) -> RmdbClient {
        RmdbClient::connect("127.0.0.1", port, ProtocolMode::Wire)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn command_ok_roundtrip() {
        let (port, server) = spawn_server(frame(tag::COMMAND_OK, 0, &[])).await;
        let mut c = connect(port).await;
        let outcome = c.exec_stream("BEGIN;").await.unwrap();
        assert!(matches!(outcome, ExecOutcome::Command));

        // 服务端应收到合法的 EXEC_STREAM 请求
        let (req_tag, req_payload) = server.await.unwrap();
        assert_eq!(req_tag, tag::EXEC_STREAM);
        assert_eq!(req_payload, b"BEGIN;");
    }

    #[tokio::test]
    async fn handshake_mismatch_is_rejected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut hs = [0u8; 8];
            s.read_exact(&mut hs).await.unwrap();
            hs[0] = b'X'; // 回送被篡改的握手
            s.write_all(&hs).await.unwrap();
        });
        let err = RmdbClient::connect("127.0.0.1", port, ProtocolMode::Wire)
            .await
            .unwrap_err();
        assert!(matches!(err, TpccError::Protocol(_)), "实际: {err}");
        assert!(err.to_string().contains("--legacy-protocol"));
    }

    #[tokio::test]
    async fn handshake_eof_hints_legacy() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            // 规范行为：不支持握手的服务端读取后直接关闭连接，不回送
            let (mut s, _) = listener.accept().await.unwrap();
            let mut hs = [0u8; 8];
            s.read_exact(&mut hs).await.unwrap();
            drop(s);
        });
        let err = RmdbClient::connect("127.0.0.1", port, ProtocolMode::Wire)
            .await
            .unwrap_err();
        assert!(matches!(err, TpccError::Protocol(_)), "实际: {err}");
    }

    #[tokio::test]
    async fn typed_query_roundtrip() {
        let mut resp = meta(&[("o_id", 0x01), ("amount", 0x02), ("c_credit", 0x03)]);
        // row 1: 3001, 12.5, "BC"
        let mut r1 = Vec::new();
        r1.push(1);
        r1.extend_from_slice(&3001i32.to_be_bytes());
        r1.push(1);
        r1.extend_from_slice(&12.5f32.to_bits().to_be_bytes());
        r1.push(1);
        r1.extend_from_slice(&2u32.to_be_bytes());
        r1.extend_from_slice(b"BC");
        resp.extend_from_slice(&frame(tag::ROW, 0, &r1));
        // row 2: NULL, -0.0, ""
        let mut r2 = Vec::new();
        r2.push(0);
        r2.push(1);
        r2.extend_from_slice(&(-0.0f32).to_bits().to_be_bytes());
        r2.push(1);
        r2.extend_from_slice(&0u32.to_be_bytes());
        resp.extend_from_slice(&frame(tag::ROW, 0, &r2));
        resp.extend_from_slice(&result_end(2));

        let (port, _server) = spawn_server(resp).await;
        let mut c = connect(port).await;
        match c.exec_stream("SELECT ...;").await.unwrap() {
            ExecOutcome::Query { columns, rows } => {
                assert_eq!(columns.len(), 3);
                assert_eq!(columns[1].sql_type, SqlType::Float32);
                assert_eq!(rows.len(), 2);
                assert_eq!(rows[0][0], Value::Int(3001));
                assert_eq!(rows[0][1], Value::Float(12.5));
                assert_eq!(rows[0][2], Value::Char("BC".to_string()));
                assert!(rows[1][0].is_null());
                match rows[1][1] {
                    Value::Float(f) => assert_eq!(f.to_bits(), (-0.0f32).to_bits()),
                    _ => panic!("期望 Float"),
                }
                assert_eq!(rows[1][2], Value::Char(String::new()));
            }
            other => panic!("期望 Query, 实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_result_roundtrip() {
        let mut resp = meta(&[("c", 0x01)]);
        resp.extend_from_slice(&result_end(0));
        let (port, _server) = spawn_server(resp).await;
        let mut c = connect(port).await;
        match c.exec_stream("SELECT ...;").await.unwrap() {
            ExecOutcome::Query { rows, .. } => assert!(rows.is_empty()),
            other => panic!("期望 Query, 实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn error_frame_maps_to_server_error() {
        let (port, _server) = spawn_server(frame(tag::ERROR, 0, "syntax error".as_bytes())).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("bad sql;").await.unwrap_err();
        match err {
            TpccError::Server(diag) => assert_eq!(diag, "syntax error"),
            other => panic!("期望 Server, 实际 {other}"),
        }
    }

    #[tokio::test]
    async fn transaction_abort_maps_to_abort() {
        let (port, _server) =
            spawn_server(frame(tag::TRANSACTION_ABORT, 0, "write conflict".as_bytes())).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("UPDATE ...;").await.unwrap_err();
        match err {
            TpccError::Abort(diag) => assert_eq!(diag, "write conflict"),
            other => panic!("期望 Abort, 实际 {other}"),
        }
    }

    #[tokio::test]
    async fn mid_stream_abort_discards_partial_rows() {
        let mut resp = meta(&[("c", 0x01)]);
        let mut r1 = Vec::new();
        r1.push(1);
        r1.extend_from_slice(&1i32.to_be_bytes());
        resp.extend_from_slice(&frame(tag::ROW, 0, &r1));
        resp.extend_from_slice(&frame(tag::TRANSACTION_ABORT, 0, b"conflict"));
        let (port, _server) = spawn_server(resp).await;
        let mut c = connect(port).await;
        assert!(matches!(
            c.exec_stream("SELECT ...;").await.unwrap_err(),
            TpccError::Abort(_)
        ));
    }

    #[tokio::test]
    async fn unknown_tag_is_rejected() {
        let (port, _server) = spawn_server(frame(0x7f, 0, &[])).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("SELECT 1;").await.unwrap_err();
        assert!(matches!(err, TpccError::Protocol(_)), "实际: {err}");
    }

    #[tokio::test]
    async fn nonzero_reserved_is_rejected() {
        let mut resp = frame(tag::COMMAND_OK, 0, &[]);
        resp[7] = 1; // reserved 低位非 0
        let (port, _server) = spawn_server(resp).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("BEGIN;").await.unwrap_err();
        assert!(matches!(err, TpccError::Protocol(_)), "实际: {err}");
    }

    #[tokio::test]
    async fn nonzero_response_flags_are_rejected() {
        let (port, _server) = spawn_server(frame(tag::COMMAND_OK, 0x01, &[])).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("BEGIN;").await.unwrap_err();
        assert!(matches!(err, TpccError::Protocol(_)), "实际: {err}");
    }

    #[tokio::test]
    async fn oversized_payload_is_rejected_before_allocation() {
        let mut resp = Vec::new();
        resp.extend_from_slice(&(2u32 * 1024 * 1024).to_be_bytes()); // 声明 2 MiB
        resp.push(tag::ERROR);
        resp.push(0);
        resp.extend_from_slice(&[0, 0]);
        let (port, _server) = spawn_server(resp).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("SELECT 1;").await.unwrap_err();
        match err {
            TpccError::Protocol(msg) => assert!(msg.contains("1 MiB"), "实际: {msg}"),
            other => panic!("期望 Protocol, 实际 {other}"),
        }
    }

    #[tokio::test]
    async fn row_count_mismatch_is_rejected() {
        let mut resp = meta(&[("c", 0x01)]);
        resp.extend_from_slice(&result_end(3)); // 实际 0 行，声明 3 行
        let (port, _server) = spawn_server(resp).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("SELECT ...;").await.unwrap_err();
        match err {
            TpccError::Protocol(msg) => assert!(msg.contains("row_count"), "实际: {msg}"),
            other => panic!("期望 Protocol, 实际 {other}"),
        }
    }

    #[tokio::test]
    async fn row_before_meta_is_rejected() {
        let mut r = Vec::new();
        r.push(1);
        r.extend_from_slice(&1i32.to_be_bytes());
        let (port, _server) = spawn_server(frame(tag::ROW, 0, &r)).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("SELECT 1;").await.unwrap_err();
        assert!(matches!(err, TpccError::Protocol(_)), "实际: {err}");
    }

    #[tokio::test]
    async fn command_ok_after_meta_is_rejected() {
        let mut resp = meta(&[("c", 0x01)]);
        resp.extend_from_slice(&frame(tag::COMMAND_OK, 0, &[]));
        let (port, _server) = spawn_server(resp).await;
        let mut c = connect(port).await;
        let err = c.exec_stream("SELECT 1;").await.unwrap_err();
        assert!(matches!(err, TpccError::Protocol(_)), "实际: {err}");
    }

    #[tokio::test]
    async fn short_reads_are_reassembled() {
        // 服务端逐小块发送，验证客户端 read_exact 循环正确拼装
        let mut resp = meta(&[("c", 0x01), ("s", 0x03)]);
        let mut r1 = Vec::new();
        r1.push(1);
        r1.extend_from_slice(&42i32.to_be_bytes());
        r1.push(1);
        r1.extend_from_slice(&5u32.to_be_bytes());
        r1.extend_from_slice(b"hello");
        resp.extend_from_slice(&frame(tag::ROW, 0, &r1));
        resp.extend_from_slice(&result_end(1));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let mut hs = [0u8; 8];
            s.read_exact(&mut hs).await.unwrap();
            s.write_all(&hs).await.unwrap();
            read_request(&mut s).await;
            s.set_nodelay(true).unwrap();
            for chunk in resp.chunks(3) {
                s.write_all(chunk).await.unwrap();
                s.flush().await.unwrap();
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        });

        let mut c = connect(port).await;
        match c.exec_stream("SELECT ...;").await.unwrap() {
            ExecOutcome::Query { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], Value::Int(42));
                assert_eq!(rows[0][1], Value::Char("hello".to_string()));
            }
            other => panic!("期望 Query, 实际 {other:?}"),
        }
    }

    #[tokio::test]
    async fn ping_accepts_command_ok_and_query() {
        let (port, _s) = spawn_server(frame(tag::COMMAND_OK, 0, &[])).await;
        let mut c = connect(port).await;
        c.ping().await.unwrap();

        let mut resp = meta(&[("table_name", 0x03)]);
        resp.extend_from_slice(&result_end(0));
        let (port, server) = spawn_server(resp).await;
        let mut c = connect(port).await;
        c.ping().await.unwrap();
        // 测活必须发送精确语句 show tables;
        let (_, payload) = server.await.unwrap();
        assert_eq!(payload, b"show tables;");
    }
}
