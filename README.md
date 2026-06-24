# TPCC-Tester

本项目为 2023-2025 全国大学生计算机系统能力大赛-数据库管理系统设计赛决赛模拟评测脚本，目前由 [db-camp](https://github.com/db-camp) 社区开发和维护。

使用 Rust 编写，基于 tokio 异步模型，提供高性能并发基准测试和友好的诊断输出。

欢迎未来的数据库大赛参赛选手使用和PR。

## 环境配置

本项目使用 Rust 编写，请确保已安装 Rust 工具链。

1. **安装 Rust** (如果尚未安装):
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   ```

2. **编译**:
   ```bash
   cargo build --release
   ```

3. **运行**:
   ```bash
   # 创建表和索引
   ./target/release/tpcc-tester --create-schema -s 50

   # 加载初始数据
   ./target/release/tpcc-tester --init -s 50

   # 运行一致性检查
   ./target/release/tpcc-tester --check -s 50

   # 查看各表行数统计
   ./target/release/tpcc-tester --stats

   # 运行并发基准测试
   ./target/release/tpcc-tester --benchmark --threads 16 --transactions 100 -s 50

   # 运行数据库兼容性诊断
   ./target/release/tpcc-tester --diagnose
   ```

## 命令行参数

```
Options:
  -s, --scale <N>          Scale factor / 仓库数量 [default: 50]
      --host <HOST>        RMDB 服务地址 [default: 127.0.0.1]
      --port <PORT>        RMDB 服务端口 [default: 8765]
      --create-schema      创建 TPC-C 表和索引
      --init               加载 TPC-C 初始数据
      --check              运行一致性检查
      --stats              显示各表行数统计
      --benchmark          运行并发基准测试
      --threads <N>        并发线程数 [default: 16]
      --transactions <N>   每线程事务数 [default: 100]
      --rw-ratio <F>       读写比例 0.0-1.0 [default: 0.9130434782608695]
      --txn-probs <F F F F F>  事务概率 [NewOrder Payment OrderStatus Delivery StockLevel] [default: 10 10 1 1 1]
      --diagnose           运行数据库兼容性诊断
  -v, --verbose            详细日志 (-v=DEBUG, -vv=TRACE)
```

默认 scale=50，对应初始数据规模：

- warehouse: 50
- district: 500
- customer: 1500000
- history: 1500000
- new_orders: 450000
- orders: 1500000
- order_line: 15000000
- item: 100000
- stock: 5000000

## 日志级别

- 默认 (`INFO`): 阶段进度、最终结果、检查通过/失败
- `-v` (`DEBUG`): 每条 SQL 语句、响应摘要、事务步骤
- `-vv` (`TRACE`): 完整原始响应、参数替换细节

## 支持项目

如果你觉得这个项目对你有帮助，请考虑给它一个 Star 支持！

[![Star History Chart](https://api.star-history.com/svg?repos=db-camp/TPCC-Tester&type=Date)](https://star-history.com/#db-camp/TPCC-Tester&Date)

[![Visitors](https://api.visitorbadge.io/api/visitors?path=https://github.com/db-camp/TPCC-Tester&label=visitors&countColor=%23263759)](https://visitorbadge.io/status?path=https://github.com/db-camp/TPCC-Tester)
