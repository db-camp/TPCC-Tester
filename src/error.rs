use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum TpccError {
    #[error("连接失败: {0}")]
    Connection(String),

    #[error("事务被中止: {0}")]
    Abort(String),

    #[error("查询错误: {0}")]
    QueryError(String),

    #[error("响应解析失败: {0}")]
    ParseError(String),

    #[error("Wire 协议错误: {0}")]
    Protocol(String),

    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("操作超时: {context}")]
    Timeout { context: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlState {
    Success,
    Abort,
}

impl fmt::Display for SqlState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlState::Success => write!(f, "SUCCESS"),
            SqlState::Abort => write!(f, "ABORT"),
        }
    }
}
