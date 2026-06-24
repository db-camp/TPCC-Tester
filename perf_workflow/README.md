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
  --label sf1_t16_official \
  --db-name tpcc_sf1 \
  --init-db \
  --check
```

Run only throughput + latency benchmark with the fixed official-style window:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode benchmark \
  --label official_like \
  --db-name tpcc_sf1 \
  --init-db \
  --check
```

Run only `perf`:

```bash
./deps/TPCC-Tester/perf_workflow/run_workflow.sh \
  --mode perf \
  --label perf_only \
  --db-name tpcc_sf1 \
  --perf-record-seconds 30
```

## GitHub Actions CI

The same TPC-C flow is available as a manual GitHub Actions workflow:

```text
TPCC Performance
```

It starts the configured Azure self-hosted runner VM, runs on the `rmdb/azure/westus3/d32alds-v7/nvme` runner labels, and deallocates the VM in an `always()` cleanup job. It defaults to the current quick official-style test:

- target_ref: empty, which means the selected workflow branch
- tpcc_branch: `chen`
- scale: `1`
- threads: `16`
- transactions: `1000000`
- warmup_seconds: `30`
- measure_seconds: `60`
- rw_ratio: `0.9130434782608695`
- txn_probs: `10 10 1 1 1`
- mode: `benchmark`
- perf_record_seconds: `20`
- skip_perf_record: `false`
- callgrind_transactions: `60`
- heaptrack_transactions: `60`
- build flags: `-O2 -g -fno-omit-frame-pointer`

Set `mode` to `perf`, `callgrind`, `heaptrack`, or `all` to collect profiling artifacts. The workflow validates the required tools before running each profiling mode and uploads perf data, flamegraphs, callgrind outputs, heaptrack captures, stdout, and stderr files with the normal TPCC artifacts.

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
- The installer now prefers already-installed system tools.
- If `~/FlameGraph` exists, the workflow prefers that checkout and symlinks `flamegraph.pl` and `stackcollapse-perf.pl` from it.
- `heaptrack_gui` and `hotspot` are now expected to be available from the system install.
- `trace_processor` and `traceconv` are installed as official Perfetto wrappers and can be used later for trace analysis.
- `run_workflow.sh` now cleans the temporary database directory by default after each run, so `performance_test_record/` remains the only persistent result store. Use `--keep-db-artifacts` only when you explicitly need the database files.
