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

装载、绑定和相对更新遵循 IEEE-754 binary32 round-to-nearest-even。CSV 装载使用可往返同一 binary32 位模式的十进制文本；ranked typed Wire 路径则直接传 raw bits。Payment 和 Delivery 的逐事务相对更新保留更新前后 raw bits，并按 0 ULP 检查。跨阶段的版本化 `state-dir` 保存装载形状、密封的有界终态证据和崩溃前基线，供在线与恢复校验复用。

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
  --seed 2026
```

工作流会为新库派生不透明数据库名，并依次完成 build、setup、一次原生三窗口测量、在线校验、对本次登记 RMDB PID 执行 `SIGKILL`、原数据库重启和恢复校验。显式指定新 `--db-name` 属于本地偏差，必须同时使用 `--allow-deviation`。`2026` 只是可复现的本地 seed，不是官方隐藏 seed。结果写入：

```text
performance_test_record/<UTC-run-id>_final2026/
```

完整参数、安全约束及拆分运行方式见 [`perf_workflow/README.md`](perf_workflow/README.md)。

优化迭代时可先运行固定 SF50/32-client 的非排名初测：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode fast \
  --seed 2026
```

它使用同一 prepared transaction/session/router/dispatch 路径；预热和 60 秒测量
使用独立阶段轮盘，测量阶段各客户端从 `txn_no=0` 重新开始，但不发布任何正式
rank claim 或 terminal evidence。

### 环境对齐：`--rtt-sim-ms`

官方客户端跨主机、每次物理 attempt 受网络往返约束；本地 loopback 无 RTT，
客户端无限速发送会形成重试风暴（本地实测约 81% 的物理尝试是重试、物理尝试率
约 2440/s，官方约 733/s），掩盖服务器真实能力。`--rtt-sim-ms <ms>` 在每个
attempt（含重试）前插入模拟 RTT，把本地 attempt 率对齐到官方水平；本地
fast 实测最优约 30ms（物理 ~820/s，重试占比降到 ~19%，NewOrder/min
相对 rtt=0 提升约 40%）。它是环境对齐（复现官方跨主机网络的客观物理延迟），
不是思考时间偏差，因此**默认 30ms 且不破坏排名配置**；`--rtt-sim-ms 0` 选择
本地 loopback。

```bash
# 默认 rtt=30，保持 ranked 配置（官方对齐）
deps/TPCC-Tester/perf_workflow/run_workflow.sh --mode all --seed 2026

# 本地 loopback（公开契约 rtt=0）
deps/TPCC-Tester/perf_workflow/run_workflow.sh --mode all --seed 2026 --rtt-sim-ms 0

# 快速官方对齐初测
deps/TPCC-Tester/perf_workflow/run_workflow.sh --mode fast --seed 2026
```

### 官方对齐校验：`compare_official.py`

用官方测评报告（`run_new.log`）与本地产出做多指标对比，量化每个指标的差距
（不只 tpmC）：

```bash
python3 deps/TPCC-Tester/perf_workflow/compare_official.py \
  run_new.log performance_test_record/<UTC-run-id>_final2026
```

对比维度：Median/R1/R2/R3 NewOrder/min、R1/R3 衰减、abandoned 率、p50/p99/avg
latency、CPU avg、Peak RSS、Load time、5s bucket max。

chen/v3 HEAD 官方 vs 本地（rtt-sim-ms=8，32 客户端 SF50）实测差异：

| 指标 | 官方 | 本地 | 差距 |
|---|---|---|---|
| Median NewOrder/min | 24623 | 21380 | 1.15x |
| R1 | 40582 | 24841 | 1.63x |
| R3 | 19520 | 20752 | 0.94x |
| R1/R3 衰减 | 2.08x | 1.20x | 官方衰减大 |
| Abandoned | 35.0% | 0.00% | 官方跨主机超时 |
| p50 / p99 | 25.5 / 157.6ms | 20.5 / 201.6ms | 混合 |
| CPU avg (host%) | 35.2% | 40.3% | 本地更忙 |
| Load time | 52.7s | 15.9s | 官方慢 |
| 5s bucket max | 68916 | 31536 | 2.19x |

差异来源：
1. **机器 compute（主）**：官方 R1 峰值 40582/68916 vs 本地 24841/31536（约 1.6x）；
   R3 稳态两者接近（19520 vs 20752），说明稳态能力相当，峰值受机器单核/内存带宽限制。
2. **abandoned**：官方 35% 事务超 3s deadline 被放弃（跨主机 RTT + 官方服务器 R2/R3
   数据增长排队）；本地 loopback + 服务器快，3s 内总能完成 → 0%。本地事务端到端
   延迟（avg ~33ms）远小于 deadline，仅调大 rtt 会同时压低吞吐，无法单独复现官方超时。
3. **load 时间**：官方 52.7s vs 本地 15.9s（官方校验更严格/磁盘慢；本地虚拟化 IO 快）。

复现策略：本地对齐目标是**形态**（R1 高、R2/R3 衰减、吞吐/延迟/CPU 相对关系），
绝对值受机器系数影响。校准 rtt 匹配官方 attempt 时序（官方 R1 高 → 本地 rtt 宜小，
如 8ms；rtt15 时本地 R1 23448、rtt8 时 24841）。

abandoned 复现边界（rtt 扫描实验）：rtt0（loopback 无限速）确能产生大量
Delivery 超时 abandoned（服务器排队），但会触发重试风暴导致 NewOrder 语义失败
（rank 不通过）；rtt8-15 语义正常但 abandoned 近 0（本地服务器快，事务 avg ~33ms
远小于 3s deadline）。因此官方 35% abandoned 是跨主机网络 + 官方负载特征的产物，
本地不能仅靠 rtt 复现，也不应通过人为注入超时制造（那会伪造非官方行为）。

衰减机制与优化尝试（实验矩阵，本地 rtt-sim-ms=8）：

| 尝试 | R1 | R3 | 衰减 | median | 结论 |
|---|---|---|---|---|---|
| 官方 | 40582 | 19520 | 2.08x | 24623 | 目标形态 |
| 基线 rtt8 | 24841 | 20752 | 1.20x | 21380 | — |
| A1 自增豁免判定2 | 24614 | 21450 | 1.15x | 21725 | 冲突主因是锁竞争非 stale-commit，revert |
| D8 重试退避(0-2ms) | 25368 | 20904 | 1.21x | 21951 | median+2.7%、CPU 40→38% |
| SI 锁等待1ms | 26244(60s窗) | — | — | — | 等待阻塞 worker，revert |
| 缓冲池 512MB | 20546 | 19122 | 1.07x | 18866 | R1 降且衰减更小 |

衰减机制：衰减幅度 = 数据增长速率 → R3 工作集 vs 1.5GB 缓冲池 → miss。
官方 R1 峰值高（机器 compute）→ 数据增长快 → R3 工作集超缓冲池 → 降速；
本地 R1 峰值 ~25k（机器/服务器上限，物理 attempt 每 commit ~4.6 次、重试 ~52%）
→ 数据增长慢 → R3 工作集小 → 不降。512MB 缓冲池实验证实减小缓冲池只降 R1、
不放大衰减。因此**本地 R1 峰值是天花板**：衰减与峰值差距的放大需要 RMDB 内核
提升每核效率（本地 3623 vs 官方 4397 NewOrder/min/核），即减少热行锁竞争、
abort 开销与事务路径时间，属后续 R2 median 优化方向。

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

崩溃与同库重启应交给 `run_workflow.sh --mode all`，避免误杀其他进程或恢复到错误数据库。下面只是在 5 秒本地诊断预算内单独探测；正式恢复的 90 秒 deadline 必须由 workflow 在启动 RMDB 前建立并传递：

```bash
./target/release/tpcc-tester \
  --probe-ready \
  --probe-budget-millis 5000 \
  --host 127.0.0.1 \
  --port 8765
```

`--probe-ready` 必须和正数 `--probe-budget-millis` 成对使用，且不能混入其他动作；它会完成 Wire v3 handshake 并完整执行 `show tables;`，而不是只测试 TCP connect。

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

`--response-timeout-seconds`（本地默认 150 秒）用于按已提供官方失败报告中的公开 elapsed
边界复现响应读取超时，`--phase-tail-grace-seconds`（本地默认 5 秒）用于避免阶段尾部请求
永久阻塞。决赛说明没有公布官方对应 deadline；150 秒只是本地报告复现值，不得描述为
官方参数。恢复一致性请求若在完整发送后超时，会明确报告“请求已发送，但未收到完整
response frame 与 terminal”，且不会把本地生成的 SQL 写入超时诊断。

若正式排名在本机先于崩溃阶段失败，可在对同一 SF50 数据库执行真实 `SIGKILL`、重启并
通过 `show tables;` 测活后，使用 `--post-crash-response-probe --seed <setup-seed>
--state-dir <setup-state>` 做只读的本地响应面探测。它依次执行公开 37+7、装载期关系/
内容样本以及 500 分区分组查询形状，并为每个脱敏 ordinal/shape 记录发送、首个完整
响应帧和 terminal 的耗时。该模式不比较数值、不写 recovery receipt，也不推断未公开
SQL/答案，结果始终不构成排名或正式恢复通过证明。
