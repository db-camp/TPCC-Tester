#[derive(Debug, thiserror::Error)]
pub enum TpccError {
    #[error("连接失败: {0}")]
    Connection(String),

    #[error("事务被中止: {0}")]
    Abort(String),

    #[error("IO 错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("操作超时: {context}")]
    Timeout { context: String },
}
