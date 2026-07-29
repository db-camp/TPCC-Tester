# TPCC-Tester — final2026 `public_spec_aligned`

这是 RMDB 2026 决赛公开 TPC-C 契约的本地实现。它覆盖公开的数据规模、并发与计时、事务混合、热点路由、Wire v3、排序门槛及崩溃恢复流程。

本项目不是官方评测客户端的副本。官方隐藏 seed、精确校验 SQL 与答案、运行/连接标识符，以及未公开的 socket response deadline 均未包含在公开赛题中，也不会在这里猜测或硬编码。因此，本实现的准确定位是 `public_spec_aligned`，不是 hidden-grader clone。

## 公开 final2026 契约

| 项目 | 实现 |
| --- | --- |
| 数据规模 | SF=50，50 个 warehouse；固定表共 10,050,550 行，另有每个初始订单 5–15 行的动态 `order_line` |
| 并发 | 32 个持续施压客户端，无 think time |
| setup | 9 张表、10 个索引、9 次装载、计数与公开完整性检查，共用一个不超过 900 秒的绝对预算 |
| 时间线 | 一次 30 秒预热，紧接连续 3 个 150 秒正式窗口；窗口之间不重新预热 |
| 事务混合 | NewOrder / Payment / OrderStatus / Delivery / StockLevel = `45 / 43 / 4 / 4 / 4`；派生写事务比例为 92%，不做二次缩放 |
| 排名值 | 三个正式窗口各自的 NewOrder/min 的中位数 |
| 恢复 | 在线校验通过后 `SIGKILL`，原数据库重启，90 秒内通过精确 Wire `show tables;` readiness，再执行恢复校验 |

正式窗口只将截止前完成的提交和业务预期回滚计入完成样本。重试 abort、放弃请求和 grace-tail 响应不进入排名样本。每个窗口还要求五类事务均至少提交一次、Delivery 至少实际处理一个队列项，并满足公开的 warehouse 覆盖门槛。

### Wire v3 与事务语义

- 每个连接先交换 `RMDB` Wire 3.0 handshake；帧使用 8 字节大端序头，payload 上限为 1 MiB。
- ranked `PREPARE_SET` / `EXEC_BATCH` 路径将 `INT32`、原始位模式
  `FLOAT32` 和 `CHAR` 按类型编码；该路径的浮点 bind/result 不经十进制
  文本或 `f64` 往返。
- 32 个连接在计时屏障前完成 `SET TRANSACTION ISOLATION LEVEL SNAPSHOT ISOLATION;`，随后用 `PREPARE_SET` 安装连接本地语句集，并贯穿预热及三个正式窗口。
- 事务使用有界 `EXEC_BATCH`，始终设置 `AUTO_ABORT`。响应帧顺序、行数、类型、statement/operation id 和 terminal 均严格校验。
- 只有 `TRANSACTION_ABORT` 可按原始、完全冻结的输入重试；协议错误和 SQL `ERROR` 立即失败。Snapshot Isolation 下的 stale write-write 必须 abort，不能覆盖更新后的版本。
- 普通 setup、校验和 readiness 使用 `EXEC_STREAM`。框架不切换 `OUTPUT_FILE`，也不读取 `output.txt`。

### 热点轮盘与参数冻结

每个阶段使用一个 160 槽 warehouse 轮盘：

- 4 个 hot warehouse 的身份整次运行固定，每个占 26 槽，共 104 槽；
- 其余 46 个 cold warehouse 各占 1 槽，再从 cold 集合加入 10 个额外槽；
- 每个阶段独立洗牌，但不改变 hot warehouse 身份；
- 客户端取槽公式为
  `(client_id + 32 * (txn_no mod 5) + 13 * floor(txn_no / 5)) mod 160`。

`txn_no` 在每个阶段从零开始。一次参数选择消耗一个编号；重试不消耗新编号并复用全部参数；最终放弃仍消费原编号。路由和各参数域使用相互隔离的确定性随机域。公开热点还包括 hot warehouse 内 65% hot district、24 个 hot item 且 NewOrder 行 25% 命中、NewOrder 8% remote supply，以及 Payment 30% remote customer warehouse。

### FLOAT32 与一致性状态

装载、绑定和相对更新遵循 IEEE-754 binary32 round-to-nearest-even。CSV 装载使用可往返同一 binary32 位模式的十进制文本；ranked typed Wire 路径则直接传 raw bits。Payment 和 Delivery 的逐事务相对更新保留更新前后 raw bits，并按 0 ULP 检查。跨阶段的版本化 `state-dir` 保存装载形状、已确认提交 ledger 和崩溃前基线，供在线与恢复校验复用。

公开赛题只给出校验规模与语义边界，没有公开官方的具体 SQL、答案或采样标识。本框架的校验计划验证同一类公开不变量，但不能声称与隐藏查询逐字相同。

## 构建

需要 Rust stable。请在本目录执行：

```bash
cargo build --release --locked
cargo test --locked
```

RMDB 服务端必须支持上述 Wire v3 契约。推荐通过下节的工作流启动服务端；直接运行 tester 时，当前工作目录必须是 TPCC-Tester 根目录，因为 setup 会读取 `sql/` 下的 DDL。

## 推荐：完整崩溃生命周期

从 RMDB 仓库根目录执行：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --db-name tpcc_final2026_local \
  --seed 2026
```

工作流依次完成 build、setup、一次原生三窗口测量、在线校验、对本次登记 RMDB PID 执行 `SIGKILL`、原数据库重启和恢复校验。`2026` 只是可复现的本地 seed，不是官方隐藏 seed。结果写入：

```text
performance_test_record/<UTC-run-id>_final2026/
```

完整参数、安全约束及拆分运行方式见 [`perf_workflow/README.md`](perf_workflow/README.md)。

## 直接运行 tester

以下示例假定 RMDB 已在 `127.0.0.1:8765` 运行，并且状态目录属于这一数据库与 seed：

```bash
mkdir -p /tmp/tpcc-final2026-state

RMDB_TPCC_RUN_ID=local-final2026-01 \
./target/release/tpcc-tester \
  --create-schema --init --check --check-scope setup \
  --profile final2026 \
  --seed 2026 \
  --state-dir /tmp/tpcc-final2026-state

RMDB_TPCC_RUN_ID=local-final2026-01 \
./target/release/tpcc-tester \
  --benchmark \
  --profile final2026 \
  --seed 2026 \
  --state-dir /tmp/tpcc-final2026-state

RMDB_TPCC_RUN_ID=local-final2026-01 \
./target/release/tpcc-tester \
  --check --check-scope online \
  --profile final2026 \
  --seed 2026 \
  --state-dir /tmp/tpcc-final2026-state
```

崩溃与同库重启应交给 `run_workflow.sh --mode all`，避免误杀其他进程或恢复到错误数据库。readiness 可单独探测：

```bash
./target/release/tpcc-tester \
  --probe-ready \
  --host 127.0.0.1 \
  --port 8765
```

`--probe-ready` 必须单独使用，它会完成 Wire v3 handshake 并完整执行 `show tables;`，而不是只测试 TCP connect。

## 明确标记的本地 smoke

任何 scale、客户端数或时间覆盖都必须同时给出 `--allow-deviation`。本地
smoke 支持 `scale=1..50`、`clients=1..32`；更大的值不属于当前热点路由
实现的有效域。例如：

```bash
mkdir -p /tmp/tpcc-final2026-smoke-state

RMDB_TPCC_RUN_ID=local-final2026-smoke \
./target/release/tpcc-tester \
  --create-schema --init --check --check-scope setup --benchmark \
  --profile final2026 \
  --seed 7 \
  --state-dir /tmp/tpcc-final2026-smoke-state \
  --allow-deviation \
  --scale 1 \
  --clients 2 \
  --warmup-seconds 1 \
  --window-seconds 3
```

smoke 仍走同一三窗口状态机、Wire 路径与语义检查，但会明确输出 `NON-RANKED`，其结果不能与正式 final2026 排名比较。`--threads` 是 `--clients` 的兼容别名。

`--response-timeout-seconds`（本地默认 30 秒）与 `--phase-tail-grace-seconds`（本地默认 5 秒）是为了避免本地测试永久阻塞的安全值。官方对应 deadline 未公开，这两个默认值不得被描述为官方参数。
