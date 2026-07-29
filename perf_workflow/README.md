# RMDB final2026 TPC-C workflow

`run_workflow.sh` is a lifecycle wrapper for the Rust `tpcc-tester`. It builds
the two binaries, starts only the RMDB process it registered, delegates one
complete benchmark invocation to Rust, performs the prescribed crash/restart,
and collects logs.

The shell does **not** implement transaction selection, warmup, measurement
windows, retry deadlines, semantic checks, or ranking. Those are one coherent
`--profile final2026` contract in the Rust tester.

## Official profile

The default `final2026` profile is expected to enforce:

- 50 warehouses and 32 clients;
- one 30-second warmup followed by three consecutive 150-second windows;
- transaction mix `45 / 43 / 4 / 4 / 4`;
- the deterministic 160-slot warehouse routing wheel;
- per-window coverage and transaction-family gates;
- online checks, then SIGKILL, a 90-second exact `show tables;` readiness
  budget, and the full recovery checks.

The public seed defaults to `2026` for reproducible local runs. It replaces the
grader's hidden seed; it does not claim to reveal that seed.

## Common commands

Run the complete official lifecycle:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --db-name tpcc_final2026 \
  --seed 2026
```

Create and retain a database, then rank it in another invocation:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode init \
  --db-name tpcc_final2026

./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode rank \
  --db-name tpcc_final2026
```

Run recovery checks against an existing database:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode recovery \
  --db-name tpcc_final2026
```

Inspect the resolved paths without building, starting, killing, or deleting
anything:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh --plan-only
```

For a deliberately short local smoke test, deviations must be explicit:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --allow-deviation \
  --scale 1 \
  --clients 2 \
  --warmup-seconds 1 \
  --window-seconds 3
```

`benchmark` remains an alias for `rank`, and `--threads` /
`--measure-seconds` remain aliases for `--clients` /
`--window-seconds`.

## Safety contract

- The default RMDB root is exactly three levels above this directory:
  `<RMDB>/deps/TPCC-Tester/perf_workflow/../../..`.
- `--db-name`, `--label`, and `--build-dir` must be safe single path
  components. A database path can therefore never escape the RMDB root.
- An existing database is never replaced automatically. A successful `all`
  run cleans its new database by default; `init`, `rank`, and `recovery` retain
  databases. `--keep-db-artifacts` and `--clean-db-on-exit` make this explicit.
  A failed run retains its database for diagnosis.
- Database cleanup requires an ownership marker whose token exactly matches
  the current run. Symlinks are rejected.
- Generated CSV files live under the current run's
  `<RMDB>/.tpcc-workflow/<run-id>/csv` directory. The workflow never removes
  source-tree CSV files.
- Port conflicts fail closed. The script never discovers or kills a process by
  port; shutdown signals target only the PID registered by this invocation.
- A pre-existing CMake cache whose `CMAKE_HOME_DIRECTORY` points elsewhere
  causes a clear failure. The workflow never deletes or silently rewrites that
  cache.
- Readiness is the Rust tester's `--probe-ready`, which executes the exact
  `show tables;` protocol check. Each probe subprocess has a portable two-second
  watchdog inside the overall 90-second budget. A successful TCP connection
  alone is not readiness.
- The script is compatible with macOS Bash 3.2 and has no dependency on
  `ss`, `nproc`, or GNU `timeout`.

Each invocation writes its manifest, tool status, server log, probe log, tester
logs, and summary under:

```text
<RMDB>/performance_test_record/<UTC-run-id>_<label>/
```

Use `--record-root` to place these artifacts elsewhere.
