# final2026 `public_spec_aligned` 工作流

`run_workflow.sh` 是 RMDB 与 Rust `tpcc-tester` 的安全生命周期封装。Shell 只负责构建、数据库目录、进程、日志、`SIGKILL` 和同库重启；事务选择、32 个持久连接、Wire v3、预热、连续三窗口、重试、语义门槛和排名都由一次 Rust benchmark 调用完成。

这是公开决赛契约的本地等价实现，不是官方隐藏客户端。官方 seed、精确校验 SQL/答案、运行与连接标识符，以及未公开的 socket response deadline 无法从公开赛题复刻。脚本默认 `--seed 2026` 仅用于本地可复现性。只有完整 `all` 流程通过全部必需证明后，最终结果才会标记为 `public_spec_aligned`；计划、运行中状态和拆分模式不会预先宣称该结论。

## 默认契约

- SF=50，32 个无 think time 的饱和客户端；
- setup 创建 9 张表与 10 个索引，装载 9 张表并执行公开检查，共用一个不超过 900 秒的绝对预算；
- 一次 30 秒预热，随后无间隔地连续执行 3 个 150 秒正式窗口；
- NewOrder / Payment / OrderStatus / Delivery / StockLevel 为 `45 / 43 / 4 / 4 / 4`；
- 每个连接先设置 Snapshot Isolation，再以 Wire v3 `PREPARE_SET` 安装语句，并通过带 `AUTO_ABORT` 的 `EXEC_BATCH` 执行事务；
- 公开的 160 槽热点轮盘、参数冻结、逐窗口事务/Delivery/warehouse 覆盖门槛，以及三个 NewOrder/min 的中位数排名；
- 在线校验通过后，向本次工作流登记的 RMDB PID 发送 `SIGKILL`；
- 以同一数据库目录重启，最多 90 秒内通过完整 Wire `show tables;` readiness，随后执行恢复校验。
- 从每次 RMDB 进程登记到退出，以固定 1 秒本地采样周期观察完整进程树 RSS、数据库目录占用和 CPU；这些资源值只用于诊断，不参与排名。

脚本不提供事务数、事务混合、输出文件、窗口数量或窗口内 timeout 选项，避免 Shell 与 Rust 产生两套时间线。官方未公开的 response deadline 和 phase-tail grace 使用 Rust 的本地安全默认值，不能视作官方参数。

## 完整流程

从 RMDB 仓库根目录执行：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --seed 2026
```

在 Linux 上，非排名 I/O observation 使用 `strace -c -f` 和 `/proc/<pid>`
计数器。在 macOS 上无需容器或虚拟机：工作流会改用系统自带的
`fs_usage -w -f filesys <RMDB_PID>`，并通过 `libproc.proc_pid_rusage`
采集进程累计读写字节、page-in、CPU 和 wakeup。`fs_usage` 需要管理员权限，
但不要求关闭 SIP；在启动工作流的同一个终端先刷新一次 sudo ticket：

```bash
sudo -v

deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --seed 2026
```

工作流只以 root 身份启动 `/usr/bin/fs_usage`，RMDB、tester 和其余生命周期
仍使用当前用户运行；它使用 `sudo -n`，不会在无人值守运行中卡在密码提示。
sudo ticket 缺失时 diagnostic 明确标记为 `unavailable`，排名和正式语义结果
不受影响。macOS 结果的 backend 为 `darwin_fs_usage`，输出
`diagnostic_fs_usage_summary.txt`、`diagnostic_fs_usage_metrics.json` 以及
`diagnostic_proc_{before,after,delta}.json`。`fs_usage` 是原生文件系统事件流，
不是 Linux `strace` 的逐系统调用等价物；manifest 会保留这一差异，禁止混用
两套耗时百分比作跨平台排名比较。

默认数据库名不是公开固定名称。工作流使用带域分隔的
`SHA-256(run_id, seed)` 为本次 setup 确定性生成安全的不透明名称，并把该名称和
数据库目录身份密封到状态工件中。对新库显式指定 `--db-name` 属于本地偏差，必须
同时指定 `--allow-deviation`，结果不会标记为排名配置。

`all` 是唯一执行完整 crash transition 的模式，顺序固定为：

1. 校验路径和端口，以锁定依赖构建 release tester，并以
   `Release -O2 -DNDEBUG -g0` 构建 RMDB；x86-64 主机还固定使用
   `-march=x86-64 -mtune=generic`，非 x86-64 本地主机会在计划中明确标记平台差异；
2. 创建一个此前不存在的数据库并登记所有权；
3. 通过 Wire v3 handshake 与完整 `show tables;` 确认 readiness；
4. 在同一个 900 秒 setup 预算内建表、建索引、装载并校验；
5. 启动一次 Rust benchmark，完成一次 30 秒预热和连续 `3 × 150` 秒正式窗口；
6. 执行在线一致性与 FLOAT32 语义校验并落盘崩溃基线；
7. 只对本次登记的 RMDB PID 执行 `SIGKILL`；
8. 原数据库目录重启，并在 90 秒内通过同一个精确 readiness probe；
9. 载入同一个 `state-dir` 执行恢复校验；
10. 汇总两代 RMDB 的最大 RSS、数据库峰值/最终占用，并按 Rust 发布的正式三窗口边界计算 server 进程树 CPU；
11. 由 Rust tester 只读重放并校验 dataset/contract、setup、rank receipt、有界终态证据、online baseline、四个 crash transition 和 recovery receipt 的完整状态链；
12. 写出结果；成功时默认仅清理本次创建且所有权标记匹配的数据库。

需要保留成功后的数据库时添加：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --seed 2026 \
  --keep-db-artifacts
```

失败的数据库始终保留供诊断。`--clean-db-on-exit` 可要求其他模式在成功后清理本次创建的数据库；它不会接管或删除已有数据库。

## `state-dir` 与拆分运行

`state-dir` 是数据库状态的一部分，保存版本化的装载形状、密封的有界终态证据、崩溃前校验基线和 `database.identity`。setup 完成后，
`database.identity` 会同时绑定 dataset run id、seed、不透明库名、数据库路径指纹、
文件系统 device/inode、完整 `dataset.state` SHA-256 和 runtime schema 指纹；数据库
目录内保存一份字节一致的 marker。它必须与同一个数据库一起保留，不能编辑、
跨数据库复用或在 rank/recovery 之间删除。

- `all`、`init`，以及带 `--init-db` 的 `rank` 默认创建
  `<result-dir>/state`；
- 对已有数据库执行 `rank` 或 `recovery` 时，必须用
  `--state-dir` 指向原 setup 的现有真实目录；
- 显式状态目录必须预先存在；脚本会通过 `pwd -P` 将目录链接规范化为真实目录。

用于分阶段诊断的真实命令如下：

```bash
STATE_DIR=/tmp/tpcc-final2026-state
mkdir -p "${STATE_DIR}"

deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode init \
  --seed 2026 \
  --state-dir "${STATE_DIR}"

deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode rank \
  --seed 2026 \
  --state-dir "${STATE_DIR}"

deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode recovery \
  --seed 2026 \
  --state-dir "${STATE_DIR}"
```

`rank` 和 `recovery` 从 setup 的 `database.identity` 复用库名，不会根据当前运行重新
推导。为兼容旧的调用包装，可以再次传入 `--db-name`，但它只作为断言使用，必须与
状态中的名称完全一致；不一致会在启动 RMDB 前失败。

这里 `rank` 执行正式测量与在线检查后会正常停止服务，`recovery` 只启动已有数据库并运行恢复检查；这三条拆分命令不等价于 `all` 中相邻的在线检查 → `SIGKILL` → 同库恢复链路。需要验证公开 crash lifecycle 时必须使用 `--mode all`。拆分模式即使各自成功，`ranking_eligible` 也始终为 `false`。

`--mode benchmark` 是 `rank` 的兼容别名。`--mode rank --init-db` 可在一次诊断调用中先创建/装载新数据库，再执行 rank 与在线检查。

## 环境对齐与工具链要求

本地测试与官方测评存在机器/客户端因素差异，以下项已在测试端对齐（服务器
侧不包含任何网络特殊处理，与官方服务器行为一致）：

- **机器核数**：官方测评机为 16 核。本地机器核数更多时，用 `taskset` 把
  整个工作流（服务器 + 32 客户端）固定在 16 个核上，以对齐官方 CPU 规模：

  ```bash
  taskset -c 0-15 deps/TPCC-Tester/perf_workflow/run_workflow.sh \
    --mode preliminary --seed 2026
  ```

- **客户端网络**：本地 tester 在连接建立时对每个 socket 设置 `TCP_NODELAY`
  （`WireConnection::connect`），与官方隐藏客户端一致；本地不再依赖服务器
  侧的 NODELAY/QUICKACK（官方客户端自带 NODELAY，服务器侧这些设置在官方
  无收益，故不包含）。

- **响应 deadline 放弃**：官方未公开 socket response deadline，从官方轮次
  数据（均匀 22-27% abandoned、Delivery p99 ~2.2s）校准为 3s 端到端事务
  预算（`LOCAL_RESPONSE_TIMEOUT_SECONDS`）；超时尝试（含写事务）被放弃并
  重建会话（关闭旧连接 → 服务器回滚在途事务），与官方 abandoned 行为一致。
  一致性查询使用独立 300s 超时（大 SUM 扫描不是排名流量）。

- **Rust 工具链**：tester 的 `Cargo.lock` 为 lockfile v4，需要 cargo ≥ 1.78
  （建议 rustup stable ≥ 1.97，走 rsproxy.cn 镜像安装）。系统包管理器自带
  的旧 cargo（如 Ubuntu 的 1.75）无法解析 lockfile v4，会导致 tester 构建
  失败、conformance 校验不通过。确保 `cargo --version` 返回 ≥ 1.78 后再
  运行工作流；若默认 PATH 仍是旧版本，把 rustup 的 cargo/rustc 软链到
  `/usr/local/bin`（位于 `/usr/bin` 之前）。

## 明确非排名的 smoke

性能优化迭代可使用固定的 preliminary 流程。它仍新建并装载完整 SF50
数据集，使用 32 个持久 prepared 客户端和正式事务路由，但只执行连续
`30s warmup + 1×60s measurement`。预热与测量使用独立阶段轮盘，每个客户端
在测量阶段重新从 `txn_no=0` 开始，重试仍只复用原阶段的冻结参数：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode preliminary \
  --seed 2026
```

该模式拒绝 scale、clients、warmup 和 window 覆盖，结果始终为
`NON-RANKED`。它只写 `preliminary.log`、非排名 manifest 和资源 observation，
不会创建 `rank.started`、`terminal_evidence.state`，也不会执行在线校验、
崩溃恢复或正式状态证明。正式结论仍必须来自完整 `--mode all`。

任何规模、并发或时间覆盖都需要 `--allow-deviation`，否则脚本直接拒绝。
本地 smoke 的有效范围为 `scale=1..50`、`clients=1..32`：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --db-name tpcc_final2026_smoke \
  --seed 7 \
  --allow-deviation \
  --scale 1 \
  --clients 2 \
  --warmup-seconds 1 \
  --window-seconds 3
```

smoke 保留三窗口、Wire、事务与校验路径，但 profile 和结果会明确标记 `NON-RANKED`。`--threads` 是 `--clients` 的别名，`--measure-seconds` 是 `--window-seconds` 的别名。它们只为兼容旧调用保留。

## 模式与常用检查

| 模式 | 行为 |
| --- | --- |
| `all` | 新建/装载、rank、在线检查、`SIGKILL`、同库重启、恢复检查 |
| `preliminary` | 新建/装载后执行 NON-RANKED `30s + 1×60s` 性能初测 |
| `init` | 新建并装载数据库，成功后保留 |
| `rank` | 对已有数据库 rank 并执行在线检查；`--init-db` 可先新建/装载 |
| `recovery` | 启动已有数据库并执行恢复检查 |
| `tools` | 仅记录可用工具和系统信息 |

查看解析后的绝对路径和命令，不构建、不启动、不发送信号、不创建结果目录：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh --plan-only
```

检查全部参数：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh --help
```

使用已有二进制时，两条路径都必须指向现有普通文件：

```bash
deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --skip-build \
  --server-bin /absolute/path/to/rmdb \
  --tpcc-bin /absolute/path/to/tpcc-tester
```

`--tpcc-bin`、`--skip-build` 或外部 TPCC-Tester 源码根只适合本地诊断。
任意已有 tester 都可能伪造 formal attestation 的退出状态，因此这三种路径不会
获得 `trusted_tester_binary` 必需证明，也绝不会产生排名资格。正式路径必须由
当前工作流源码根在本次调用中 fresh build tester；工作流会密封其
device/inode/size/SHA-256，并在每次执行前复核。

若 RMDB 分支需要复用预先构建的本机兼容二进制，可使用
`--skip-rmdb-build --server-bin /absolute/path/to/rmdb`。该选项只跳过
RMDB 的 CMake 构建，tester 仍由当前工作流 fresh build 并完成上述信任封存，
因此它不会像 `--skip-build` 一样取消 tester 的排名资格。

## 安全边界

- 默认 RMDB 根目录是 `perf_workflow/` 向上三级；可用 `--target-dir` 显式覆盖。
- 默认库名由本次 run id 和 seed 派生。新库显式 `--db-name` 必须和
  `--allow-deviation` 一起使用；所有库名、`--label` 和 `--build-dir` 都必须是安全的
  单一路径组件，数据库路径不能逃逸 RMDB 根目录。
- 已存在的数据库绝不会被自动替换。新库清理需要当前 run 的精确所有权 token，符号链接会被拒绝。
- 已有数据库在启动前、readiness 后和每个状态阶段入口都会校验同一份 sealed
  identity；库名、路径、device/inode、dataset 或 DB 内 marker 任一变化都会
  fail closed，而不会对新目录继续执行恢复校验。
- CSV 只生成在 `<RMDB>/.tpcc-workflow/<run-id>/csv`，工作流结束时按本次所有权清理，不触碰源码树中的 CSV。
- 端口被占用时 fail closed。脚本不按端口发现或杀进程，只向本次登记的 server/probe PID 发送信号。
- 发现指向其他源码目录的旧 CMake cache 时直接失败，不删除也不静默改写 cache。
- readiness 使用 tester 的 `--probe-ready`：Wire v3 handshake 后完整执行 `show tables;`。每次 RMDB 启动都在启动命令之前建立唯一、纳秒精度的 monotonic 绝对 deadline，进程登记、listener ownership、TCP connect、握手和完整 terminal 共用其剩余预算；Shell 在 exec probe 前传入向上取整的剩余毫秒，Rust 用一次 deadline 覆盖完整 Wire future，Shell supervisor 仍按原始绝对 deadline 执行最终边界，不另设更短或重置的 connect/response 截止。首次 setup/rank 启动使用本地 `--startup-ready-timeout-seconds` 安全预算；只有崩溃后的同库重启及独立 recovery 模式使用公开 90 秒 `--ready-timeout-seconds` 契约。仅 TCP connect 不算 ready。显式覆盖公开恢复预算会使生命周期偏离正式配置；本地首次启动预算不属于公开评分参数。
- 脚本兼容 macOS Bash 3.2，不依赖 `ss`、`nproc` 或 GNU `timeout`。

## 结果与日志

默认结果目录：

```text
<RMDB>/performance_test_record/<UTC-run-id>_<label>/
```

可用 `--record-root` 覆盖。目录包含：

- `manifest.json`（唯一权威结果）、`tool_status.txt`、`system_info.txt`；
- tester/RMDB 构建日志；
- `server.log`、`ready_probe.log` 和登记过的 `server.pid`；
- `setup.log`、`rank.log`、`check_online.log`、`check_recovery.log`（按所选模式生成）；
- `resource_segment_<n>.json`（每代服务）、`rank_timeline.state`、`rank_completion.json` 和汇总后的 `resource_metrics.json`；
- `state/`（未指定外部 `--state-dir` 时，其中含 sealed
  `database.identity`）；
- 成功后的 `summary.md`。

`manifest.json` 使用通用 required-attestations 列表。只有公开配置精确匹配、
本次 fresh build 的可信 tester 二进制通过 provenance 密封、数据库身份为
opaque + sealed、五个正式阶段均通过，并且 Rust 对完整正式状态链的只读验证
成功，且工作流最终状态为 `success` 时，才同时给出
`conformance=public_spec_aligned` 和 `ranking_eligible=true`。缺失、损坏或
符号链接状态工件会使正式 attestation 失败；唯一排名证据文件固定为
`${STATE_DIR}/terminal_evidence.state`。工作流和 summary 都通过状态目录 fd 与
`O_NOFOLLOW` 打开它，要求 regular file，按 16 MiB canonical hex + 128-byte
binding headroom + 4 KiB StateStore envelope 得到 16,781,440-byte 上限，并把
精确路径、size 和 SHA-256 绑定进 manifest。正式链 V2 还按固定顺序绑定
`dataset.state` 到 `recovery_check.passed` 的 16 个 mandatory 工件、状态目录
device/inode 和每个工件的原始字节；四个 diagnostic claim/receipt 可缺失、完整
或只留下 claim，均不进入该摘要。除 `database.identity` 和这四个可选诊断工件
外，未知或 unsafe 状态目录项都会 fail-closed。

任何形态的 `${STATE_DIR}/run_ledger.state`，以及严格匹配
`.run_ledger.state.<canonical-pid>.<canonical-counter>.tmp` 的旧原子发布残留，
都只通过目录枚举和 no-follow metadata 检测，绝不读取、删除或修改。Rust 在同
一个状态目录锁内输出唯一 ASCII `FORMAL_STATE_V2` receipt，绑定 terminal
size/SHA-256、formal-chain SHA-256 和目录 device/inode；其他 tracing 行可以是
任意字节。工作流要求 pre-inspect、Rust receipt 和 final inspect 精确一致，
阻断 ABA、late legacy temp 和后续 recovery receipt 篡改。最终 manifest 使用
`state_directory_fd_flock_v2`：对与 StateStore 同源的状态目录 fd 加独占锁，
在锁内重算完整 V2 chain、原子替换 manifest，并在释放前 fsync 结果目录。
显式偏差、`init`/`rank`/`recovery` 拆分模式和 `tools` 始终非排名。
`summary.md` 只从 `manifest.json` 生成，不读取旧式文本 manifest；只有
size/SHA-256 与 manifest 绑定一致的 `rank.log` 可以提供吞吐，profiling 日志
始终只是 observation。manifest 缺失、损坏、自相矛盾或状态非成功时不会抽取
吞吐。

最终排名指标只取 `rank.log` 中按序且各出现一次的三个
`windowN: new_order_per_min=` 以及唯一
`ranked_new_order_per_min_median=`。最终 manifest 发布前已在状态目录锁内
重读绑定的 `rank.log`，用 Decimal 重算中位数，并写入 canonical 三窗和 median
descriptor；summary 再次核对 descriptor、文件 SHA-256 和三值中位数后明确
输出 `NewOrder/min`。旧式 `Throughput`/`tpmC` 文本不作为排名依据。

资源工件始终标记 `ranked=false`、`score_effect=none`。RSS 是登记 RMDB
进程树在固定周期采样时的总和峰值；磁盘占用使用 `lstat`/allocated blocks，
按 inode 去重硬链接并拒绝目录内符号链接；CPU 同时给出“单核为 100%”和主机
逻辑 CPU 占比。采样器覆盖到服务进程组确认退出并回收 root，且清理信号只会
在连续两次确认 helper 的启动身份和直接父子关系后发送。任一 generation
缺失、身份不匹配、区间重叠、采样缺口、时钟
跳变或工件损坏都会降级为 `partial`/`unavailable`，不会改变 workflow、语义
门禁或排名结果。公开赛题没有披露官方资源采样器的精确周期，因此这里不会把
本地 1 秒峰值宣称为官方隐藏采样值。

资源或诊断采集的 `partial`、`unavailable`、`failed` 只会作为
`ranking effect: none` 的 WARN 写入 manifest/summary，不参与 required
attestation，也不会推翻已经通过的正式语义结果。

日志和本地状态只用于复现与诊断，不能据此推断官方隐藏 seed、SQL、答案、标识符或未公开 deadline。
