use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "tpcc-tester", about = "TPC-C Benchmark Tool for RMDB")]
pub struct Config {
    /// Scale factor / 仓库数量
    #[arg(short = 's', long = "scale", default_value_t = 50)]
    pub scale_factor: i32,

    /// RMDB 服务地址
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// RMDB 服务端口
    #[arg(long, default_value_t = 8765)]
    pub port: u16,

    /// 创建 TPC-C 表和索引
    #[arg(long = "create-schema")]
    pub create_schema: bool,

    /// 加载 TPC-C 初始数据
    #[arg(long)]
    pub init: bool,

    /// 保持 RMDB output.txt 写入开启
    #[arg(long = "keep-output-file")]
    pub keep_output_file: bool,

    /// 运行一致性检查
    #[arg(long)]
    pub check: bool,

    /// 一致性检查期望已提交 NewOrder 数
    #[arg(long = "expected-new-orders")]
    pub expected_new_orders: Option<i64>,

    /// 显示各表行数统计
    #[arg(long)]
    pub stats: bool,

    /// 运行并发基准测试
    #[arg(long)]
    pub benchmark: bool,

    /// 并发线程数
    #[arg(long, default_value_t = 16)]
    pub threads: usize,

    /// 每线程事务数
    #[arg(long, default_value_t = 100)]
    pub transactions: usize,

    /// 读写比例 0.0-1.0
    #[arg(long = "rw-ratio", default_value_t = 0.9130434782608695)]
    pub rw_ratio: f64,

    /// 事务概率 [NewOrder Payment OrderStatus Delivery StockLevel]
    #[arg(long = "txn-probs", num_args = 5, default_values_t = vec![10.0, 10.0, 1.0, 1.0, 1.0])]
    pub txn_probs: Vec<f64>,

    /// 运行数据库兼容性诊断
    #[arg(long)]
    pub diagnose: bool,

    /// 详细日志 (-v=DEBUG, -vv=TRACE)
    #[arg(short, long, action = clap::ArgAction::Count)]
    pub verbose: u8,
}
