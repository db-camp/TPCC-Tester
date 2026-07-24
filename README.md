# TPCC-Tester

## **注意**：2026年 初/决赛评测逻辑和脚本大改，该repo版本对应为2025年及之前的评测规则
## 欢迎参赛选手贡献最新评测代码，欢迎 Pr ！！！

本项目为 2023-2026 全国大学生计算机系统能力大赛-数据库管理系统设计赛决赛模拟评测脚本，目前由 [db-camp](https://github.com/db-camp) 社区开发和维护。

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
   # 初始化数据库（生成 CSV + LOAD 批量导入）
   ./target/release/tpcc-tester --init --csv-path /path/to/csv -s 1

   # 运行一致性检查
   ./target/release/tpcc-tester --check -s 1

   # 查看各表行数统计
   ./target/release/tpcc-tester --stats

   # 运行并发基准测试
   ./target/release/tpcc-tester --benchmark --threads 4 --transactions 100 -s 1

   # 运行数据库兼容性诊断
   ./target/release/tpcc-tester --diagnose
   ```

## 数据加载方式

`--init` 使用 CSV + LOAD 方式导入数据：

1. 生成 TPC-C 标准数据到 `--csv-path` 指定的目录（9 个 CSV 文件）
2. 通过 rmdb 的 `load <file> into <table>` 命令批量导入

`--csv-path` 必须是 **rmdb 服务端能访问到的路径**（tpcc-tester 和 rmdb 需在同一台机器或共享文件系统）。

```bash
# 示例：scale_factor=5，CSV 生成到 /tmp/tpcc_data/
./target/release/tpcc-tester --init --csv-path /tmp/tpcc_data -s 5
```

## 命令行参数

```
Options:
  -s, --scale <N>          Scale factor / 仓库数量 [default: 1]
      --host <HOST>        RMDB 服务地址 [default: 127.0.0.1]
      --port <PORT>        RMDB 服务端口 [default: 8765]
      --init               初始化数据库（建表+加载数据）
      --csv-path <PATH>    CSV 文件路径 (rmdb 服务端可访问), --init 时必须指定
      --check              运行一致性检查
      --stats              显示各表行数统计
      --benchmark          运行并发基准测试
      --threads <N>        并发线程数 [default: 1]
      --transactions <N>   每线程事务数 [default: 100]
      --rw-ratio <F>       读写比例 0.0-1.0 [default: 1.0]
      --txn-probs <F F F F F>  事务概率 [default: 0.45 0.43 0.04 0.04 0.04]
      --diagnose           运行数据库兼容性诊断
  -v, --verbose            详细日志 (-v=DEBUG, -vv=TRACE)
```

## 日志级别

- 默认 (`INFO`): 阶段进度、最终结果、检查通过/失败
- `-v` (`DEBUG`): 每条 SQL 语句、响应摘要、事务步骤
- `-vv` (`TRACE`): 完整原始响应、参数替换细节

## 支持项目

如果你觉得这个项目对你有帮助，请考虑给它一个 Star 支持！

[![Star History Chart](https://api.star-history.com/svg?repos=db-camp/TPCC-Tester&type=Date)](https://star-history.com/#db-camp/TPCC-Tester&Date)

[![Visitors](https://api.visitorbadge.io/api/visitors?path=https://github.com/db-camp/TPCC-Tester&label=visitors&countColor=%23263759)](https://visitorbadge.io/status?path=https://github.com/db-camp/TPCC-Tester)
