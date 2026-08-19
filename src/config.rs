use clap::Parser;

use crate::connection::client::ProtocolMode;

#[derive(Parser, Debug, Clone)]
#[command(name = "tpcc-tester", about = "TPC-C Benchmark Tool for RMDB")]
pub struct Config {
    /// Scale factor / 仓库数量
    #[arg(short = 's', long = "scale", default_value_t = 1)]
    pub scale_factor: i32,

    /// RMDB 服务地址
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// RMDB 服务端口
    #[arg(long, default_value_t = 8765)]
    pub port: u16,

    /// 初始化数据库（建表+加载数据）
    #[arg(long, requires = "csv_path")]
    pub init: bool,

    /// 运行一致性检查
    #[arg(long)]
    pub check: bool,

    /// 显示各表行数统计
    #[arg(long)]
    pub stats: bool,

    /// 运行并发基准测试
    #[arg(long)]
    pub benchmark: bool,

    /// 并发线程数
    #[arg(long, default_value_t = 1)]
    pub threads: usize,

    /// 每线程事务数
    #[arg(long, default_value_t = 100)]
    pub transactions: usize,

    /// 读写比例 0.0-1.0
    #[arg(long = "rw-ratio", default_value_t = 1.0)]
    pub rw_ratio: f64,

    /// 事务概率 [NewOrder Payment Delivery OrderStatus StockLevel]
    #[arg(long = "txn-probs", num_args = 5, default_values_t = vec![0.45, 0.43, 0.04, 0.04, 0.04])]
    pub txn_probs: Vec<f64>,

    /// CSV 文件路径 (rmdb 服务端可访问), --init 时必须指定
    #[arg(long = "csv-path", requires = "init")]
    pub csv_path: Option<String>,

    /// 运行数据库兼容性诊断
    #[arg(long)]
    pub diagnose: bool,

    /// 使用 2025 及以前的旧文本协议（默认使用 2026 Wire Protocol v3）
    #[arg(long = "legacy-protocol")]
    pub legacy_protocol: bool,

    /// 详细日志 (-v=DEBUG, -vv=TRACE)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}

impl Config {
    pub fn protocol_mode(&self) -> ProtocolMode {
        if self.legacy_protocol {
            ProtocolMode::Legacy
        } else {
            ProtocolMode::Wire
        }
    }
}
