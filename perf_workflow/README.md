# RMDB Performance Workflow

This directory contains a reusable local workflow for RMDB performance work.

## Files

- `install_optional_tools.sh`: installs user-local helpers under `~/.local` without `sudo`.
- `run_workflow.sh`: runs build, optional TPC-C init/check, benchmark, `perf`, `callgrind`, and `heaptrack`.
- `summarize_perf_run.py`: generates a short Markdown summary for each run directory.

## Output layout

Each run writes to:

```text
RMDB/performance_test_record/<timestamp>_<label>/
```

Typical artifacts include:

- `manifest.txt`
- `system_info.txt`
- `tool_status.txt`
- `server.log`
- `benchmark.log`
- `perf/perf_stat.csv`
- `perf/perf.data`
- `perf/perf.svg`
- `callgrind/callgrind.out`
- `callgrind/callgrind_annotate.txt`
- `heaptrack/heaptrack.data.gz`
- `heaptrack/heaptrack_print.txt`
- `summary.md`

## Common commands

Install or relink optional tools:

```bash
./deps/TPCC-Tester/perf_workflow/install_optional_tools.sh
```

Run the full workflow:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode all \
  --label sf50_t16_official \
  --db-name tpcc_sf50 \
  --init-db \
  --check
```

Run only throughput + latency benchmark with the fixed official-style window:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode benchmark \
  --label official_like \
  --db-name tpcc_sf50 \
  --init-db \
  --check
```

Run only `perf`:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode perf \
  --label perf_only \
  --db-name tpcc_sf50 \
  --perf-record-seconds 30
```

## GitHub Actions CI

The same TPC-C flow is available as a manual GitHub Actions workflow:

```text
TPCC Performance
```

It starts the configured Azure self-hosted runner VM, runs on the `rmdb/azure/westus3/d32alds-v7/nvme` runner labels, and deallocates the VM in an `always()` cleanup job. The default dispatch profile is a scale-50, 16-client, timed official-style run:

- target_ref: empty, which means the selected workflow branch
- tpcc_branch: `chen`
- scale: `50`
- threads: `16`
- transactions: `1000000`
- warmup_seconds: `30`
- measure_seconds: `60`
- rw_ratio: `0.9130434782608695`
- txn_probs: `10 10 1 1 1`
- mode: `benchmark`
- perf_record_seconds: follows `measure_seconds`, so the CI default is `60`
- skip_perf_record: `false`
- callgrind_transactions: `100`
- heaptrack_transactions: `100`
- timeout_minutes: `20`
- server_start_timeout_seconds: `120`
- build flags: `-O2 -g -fno-omit-frame-pointer`

The default scale is 50 warehouses, which generates the official-sized initial data set:

- warehouse: 50
- district: 500
- customer: 1500000
- history: 1500000
- new_orders: 450000
- orders: 1500000
- order_line: 15000000
- item: 100000
- stock: 5000000

Set `mode` to `perf`, `callgrind`, `heaptrack`, or `all` to collect profiling artifacts. Profiling modes use the same scale, thread count, warmup, transaction mix, and read-write ratio as the benchmark inputs. `perf` runs a timed warmup, records `perf stat` for the full measurement window, and uses the same duration for `perf record` unless `--perf-record-seconds` is set explicitly. The GitHub Actions workflow defaults `callgrind` and `heaptrack` to 100 transactions so `all` mode fits comfortably within the 20-minute CI budget.

`perf stat` starts from a broad candidate event list, probes event support on the runner, writes the selected list to `perf/perf_stat_events.txt`, and writes rejected events to `perf_unsupported_events.txt`. This keeps the workflow portable across runner kernel and hardware configurations while preserving the available counters.

Each run writes `summary.md` during normal completion and during cleanup after a failure. The GitHub Actions summary step publishes the file whenever it exists, so failed runs still retain the manifest, logs, partial profiling artifacts, and the generated summary.

Use `target_ref` to benchmark a different RMDB branch, tag, or commit SHA. `tpcc_branch` supplies the TPCC-Tester checkout under `deps/TPCC-Tester`, including the reusable performance workflow scripts; leave it as `chen` unless you are testing TPCC-Tester workflow changes. The CI checks out both revisions: `target_ref` supplies the RMDB code under test, while `tpcc_branch` supplies the TPCC tester and benchmark workflow.

Each CI run creates a clean temporary workspace under the self-hosted runner temp directory:

```text
${{ runner.temp }}/rmdb-tpcc-<run_id>-<attempt>/
```

The workflow copies the checked-out commit and its `deps/TPCC-Tester` submodule into that directory, then builds RMDB, builds the tester, creates the TPCC database, and writes performance artifacts there. It does not reuse the manual remote working directory.

The lifecycle jobs use Azure OIDC login and these repository variables:

- `AZURE_CLIENT_ID`
- `AZURE_TENANT_ID`
- `AZURE_SUBSCRIPTION_ID`
- `AZURE_RUNNER_RESOURCE_GROUP`
- `AZURE_RUNNER_VM`

The runner keeps its persistent service configuration under `/opt/actions-runner/rmdb-azure` on the OS disk. Its GitHub Actions work directory is `/mnt/nvme/actions-work`, backed by the VM-local NVMe RAID0 workspace. The VM also has `rmdb-nvme.service`, which recreates or remounts `/mnt/nvme` before the runner service starts.

Profiling support is configured on the Azure runner VM:

- `azureuser` has passwordless sudo.
- `perf`, `valgrind`, `callgrind_annotate`, `heaptrack`, and `heaptrack_print` are installed system-wide.
- FlameGraph is checked out at `/home/azureuser/FlameGraph`, with `flamegraph.pl` and `stackcollapse-perf.pl` linked under `/home/azureuser/.local/bin`.
- `/etc/sysctl.d/90-rmdb-profiling.conf` sets `kernel.perf_event_paranoid=1`, `kernel.kptr_restrict=0`, and `kernel.perf_event_mlock_kb=2048`.

## Environment notes

- `TPCC-Tester` defaults to the submodule checkout at `<workspace>/deps/TPCC-Tester`. The script initializes and builds it if missing.
- `perf` hardware counters are not available on this WSL2 host; the workflow uses software events and `cpu-clock` sampling.
- The installer prefers already-installed system tools.
- If `~/FlameGraph` exists, the workflow prefers that checkout and symlinks `flamegraph.pl` and `stackcollapse-perf.pl` from it.
- `heaptrack_gui` and `hotspot` are expected to be available from the system install.
- `trace_processor` and `traceconv` are installed as official Perfetto wrappers and can be used later for trace analysis.
- `run_workflow.sh` cleans the temporary database directory by default after each run, so `performance_test_record/` remains the only persistent result store. Use `--keep-db-artifacts` only when you explicitly need the database files.
