use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, trace, warn};

use crate::error::TpccError;

pub struct RmdbClient {
    stream: TcpStream,
    buf: Vec<u8>,
}

impl RmdbClient {
    pub async fn connect(host: &str, port: u16) -> Result<Self, TpccError> {
        let addr = format!("{host}:{port}");
        debug!("正在连接 RMDB: {addr}");

        let stream = tokio::time::timeout(
            Duration::from_secs(30),
            TcpStream::connect(&addr),
        )
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

        debug!("已连接到 RMDB: {addr}");
        Ok(Self {
            stream,
            buf: vec![0u8; 8192],
        })
    }

    pub async fn send_cmd(&mut self, cmd: &str) -> Result<String, TpccError> {
        trace!("发送 SQL: {cmd}");

        self.stream
            .write_all(cmd.as_bytes())
            .await
            .map_err(|e| TpccError::Connection(format!("发送失败: {e}")))?;

        let n = tokio::time::timeout(
            Duration::from_secs(300),
            self.stream.read(&mut self.buf),
        )
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

        let response = String::from_utf8_lossy(&self.buf[..n]).to_string();
        trace!("收到响应 ({n} bytes): {response}");
        Ok(response)
    }

    pub async fn close(mut self) {
        let _ = self.stream.shutdown().await;
        debug!("RMDB 连接已关闭");
    }

    pub async fn ping(&mut self) -> Result<(), TpccError> {
        // Try a simple BEGIN/COMMIT to check the connection
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
