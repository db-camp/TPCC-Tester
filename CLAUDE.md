# CLAUDE.md

## 项目概述

TPC-C 基准测试工具，用于测试 rmdb 数据库。Rust 实现，通过自定义 TCP 协议连接 rmdb。

## 构建与运行

```bash
cargo build --release

# 初始化：生成 CSV 并 LOAD 导入 rmdb
./target/release/tpcc-tester --init --csv-path /path/to/csv -s 1

# 一致性检查
./target/release/tpcc-tester --check -s 1

# 基准测试
./target/release/tpcc-tester --benchmark -s 1 -c 4 -d 60
```

## 代码结构

- `config.rs` — clap 命令行参数定义
- `connection/` — rmdb TCP 客户端和 cursor（自定义协议，非 SQL 标准驱动）
- `model.rs` — TPC-C 9 张表的结构体 + CSV 序列化 (`ToCsvRow`)
- `data_gen.rs` — 数据生成器，**不会改动**，生成的字段值不含逗号/换行/引号
- `loader.rs` — 建表、生成 CSV、通过 `load <file> into <table>` 批量导入
- `checker.rs` — TPC-C 一致性检查
- `transaction/` — 5 种 TPC-C 事务实现
- `executor.rs` — 并发基准测试执行器
- `report.rs` — 测试报告输出
- `sql/` — 建表和建索引 SQL

## 注意事项

- CSV 文件会被性能测试反复使用，不要在代码中清理
- rmdb 的 LOAD 命令格式：`load <csv_path> into <table_name>`，csv_path 必须是 rmdb 服务端可访问的路径
