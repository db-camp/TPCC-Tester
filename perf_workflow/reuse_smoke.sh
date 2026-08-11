#!/bin/bash
# reuse_smoke.sh — kernel-commit-keyed database reuse for smoke measurements.
#
# The loaded database (schema + seed data) is independent of the tester's
# runtime SQL shapes (projections, line counts, LockStock, write-first),
# so tester commits share one database. The database DOES depend on the
# RMDB kernel (page format, catalog, ...), so the reuse cache is keyed by
# the kernel repo's git commit: a different kernel commit forces a fresh
# load, tester changes do not.
#
# Cache layout (persisted on the machine, across sessions):
#   <rmdb-dir>/.reuse-db/<kernel-commit>/{db/, state/}
# Each measurement restores a fresh copy of the cached database into the
# DB root (tmpfs when configured) before starting RMDB, so every run sees
# an identical database (no drift from previous runs).
#
# Works for every RMDB kernel directory; nothing is hardcoded to a
# particular branch or machine path.
#
# Mode support: the loaded database is independent of the tester's runtime
# SQL shapes, so every ranked/measurement mode can reuse the same cached
# database. Both tpcc-tester --preliminary and --benchmark bind the
# database through the same load_bound_state()/--state-dir path:
#   --mode preliminary  non-ranked 30s warmup + 1x60s window (default)
#   --mode rank         ranked 30s warmup + 3x150s windows (official shape)
# Recovery-mode reuse stays in run_workflow.sh (--mode recovery --state-dir).
#
# Usage:
#   reuse_smoke.sh <rmdb-dir> [options]
#     <rmdb-dir>   RMDB kernel directory containing build-perf/bin/rmdb
#                  (or build/bin/rmdb) and deps/TPCC-Tester; its git HEAD
#                  is the reuse cache key
#   Options:
#     --mode <name>    measurement mode: preliminary (default) or rank
#     --seed <n>       workload seed (default: 2026)
#     --warmup <sec>   warmup seconds (default: 30)
#     --window <sec>   measurement window seconds (default: 60 for
#                      preliminary; 150 for rank)
#     --db-root <dir>  database root (default: $RMDB_DB_ROOT or <rmdb-dir>);
#                      should be a fast filesystem (tmpfs) for measurement
#     --no-measure     only ensure the cached database for this kernel
#                      commit exists (load if missing), skip the run
#     --refresh        drop the cached database for this kernel commit and
#                      force a fresh load on the next run
#
# Environment honored: RMDB_DB_ROOT, RMDB_DISABLE_TCMALLOC.
set -u

SEED=2026
MODE=preliminary
WARMUP=30
# Window default depends on the mode: 60s for preliminary, 150s for rank.
# Empty until parse_args resolves the mode so an explicit --window always
# wins and the official ranked shape stays deviation-free.
WINDOW=""
DB_ROOT="${RMDB_DB_ROOT:-}"
MEASURE=1
REFRESH=0

usage() {
  sed -n '2,44p' "$0" | sed 's/^# \{0,1\}//'
}

parse_args() {
  local arg
  while [[ $# -gt 0 ]]; do
    arg="$1"
    case "$arg" in
      --seed) SEED="$2"; shift 2 ;;
      --mode) MODE="$2"; shift 2 ;;
      --warmup) WARMUP="$2"; shift 2 ;;
      --window) WINDOW="$2"; shift 2 ;;
      --db-root) DB_ROOT="$2"; shift 2 ;;
      --no-measure) MEASURE=0; shift ;;
      --refresh) REFRESH=1; shift ;;
      -h|--help) usage; exit 0 ;;
      *) RMDB_DIR="$arg"; shift ;;
    esac
  done
}

main() {
  parse_args "$@"
  [[ -n "${RMDB_DIR:-}" ]] || { usage; echo "error: <rmdb-dir> is required" >&2; exit 1; }
  # Resolve the mode-dependent window default after argument parsing.
  if [[ -z "$WINDOW" ]]; then
    case "$MODE" in
      preliminary) WINDOW=60 ;;
      rank) WINDOW=150 ;;
      *) echo "error: unknown --mode '$MODE' (expected 'preliminary' or 'rank')" >&2; exit 2 ;;
    esac
  fi

  # locate build artifacts
  SERVER_BIN=""
  for candidate in "$RMDB_DIR/build-perf/bin/rmdb" "$RMDB_DIR/build/bin/rmdb"; do
    [[ -x "$candidate" ]] && { SERVER_BIN="$candidate"; break; }
  done
  [[ -n "$SERVER_BIN" ]] || { echo "error: rmdb binary not found under $RMDB_DIR" >&2; exit 1; }
  TESTER_DIR="$RMDB_DIR/deps/TPCC-Tester"
  TESTER_BIN="$TESTER_DIR/target/release/tpcc-tester"
  [[ -x "$TESTER_BIN" ]] || { echo "error: tpcc-tester not built at $TESTER_BIN (run the workflow once or cargo build --release)" >&2; exit 1; }

  # reuse key: kernel SOURCE tree hash (tester commits deliberately excluded).
  # The parent repo HEAD also tracks the deps/TPCC-Tester submodule pointer,
  # so a tester change would otherwise invalidate the database cache even
  # though the database is independent of the tester's runtime SQL shapes.
  KERNEL_COMMIT="$(git -C "$RMDB_DIR" rev-parse --short=12 HEAD:src 2>/dev/null || echo unknown)"
  CACHE_DIR="$RMDB_DIR/.reuse-db/$KERNEL_COMMIT"
  CACHE_DB="$CACHE_DIR/db"
  CACHE_STATE="$CACHE_DIR/state"
  CACHE_IDENTITY="$CACHE_STATE/database.identity"
  # .git may be a directory (normal repo) or a file (git worktree)
  if [[ ! -d "$RMDB_DIR/.git" && ! -f "$RMDB_DIR/.git" ]]; then
    echo "warning: $RMDB_DIR is not a git repo; reuse key is 'unknown'" >&2
  fi

  if [[ "$REFRESH" == "1" ]]; then
    echo "[reuse] --refresh: dropping cached database for kernel $KERNEL_COMMIT"
    rm -rf "$CACHE_DIR"
  fi

  DB_ROOT="${DB_ROOT:-$RMDB_DIR}"
  WORKFLOW="$TESTER_DIR/perf_workflow/run_workflow.sh"

  # load once per kernel commit (rank --init-db contract; online check may
  # fail on short-line variants — the database and state are already made).
  # Load with the OFFICIAL default profile (30s warmup, 3x150s windows) so the
  # sealed run contract matches --benchmark reuse; the preliminary mode binds
  # the same dataset.state through load_dataset() which does not compare the
  # contract. Explicit --warmup/--window only shape rank measurement runs.
  if [[ ! -f "$CACHE_IDENTITY" ]]; then
    echo "[reuse] kernel $KERNEL_COMMIT: loading database (one-time, ~8 min) into $DB_ROOT ..."
    mkdir -p "$(dirname "$CACHE_DIR/.load.log")"
    ( cd "$RMDB_DIR" && \
      RMDB_DB_ROOT="$DB_ROOT" ${RMDB_DISABLE_TCMALLOC:+RMDB_DISABLE_TCMALLOC=1} \
      "$WORKFLOW" --mode rank --init-db --skip-build --keep-db-artifacts \
        --seed "$SEED" ) > "$CACHE_DIR/.load.log" 2>&1 \
      || true
    # capture produced state (latest run's state dir)
    local latest_state
    latest_state="$(ls -td "$RMDB_DIR"/performance_test_record/*/state 2>/dev/null | head -1)"
    if [[ -n "$latest_state" && -f "$latest_state/database.identity" ]]; then
      mkdir -p "$CACHE_STATE"
      cp "$latest_state"/* "$CACHE_STATE"/ 2>/dev/null || true
      rm -f "$CACHE_STATE"/rank.started "$CACHE_STATE"/rank_completion* 2>/dev/null || true
    fi
    # persist the database files in the cache (disk), leaving the DB root clean
    local db_name
    if [[ -f "$CACHE_IDENTITY" ]]; then
      db_name="$(sed -n 's/^db_name=//p' "$CACHE_IDENTITY" | head -1)"
      if [[ -n "$db_name" && -d "$DB_ROOT/$db_name" ]]; then
        mkdir -p "$CACHE_DB"
        # copy with trailing dot so dotfiles (.rmdb_storage_format) are kept
        cp -a "$DB_ROOT/$db_name/." "$CACHE_DB/"
        rm -rf "$DB_ROOT/$db_name"
        echo "[reuse] kernel $KERNEL_COMMIT: database cached at $CACHE_DB"
      fi
    fi
  fi
  [[ -f "$CACHE_IDENTITY" ]] || { echo "error: cached database identity missing after load; see $CACHE_DIR/.load.log" >&2; exit 1; }

  DB_NAME="$(sed -n 's/^db_name=//p' "$CACHE_IDENTITY" | head -1)"
  echo "[reuse] kernel $KERNEL_COMMIT: cached database $DB_NAME"

  if [[ "$MEASURE" == "0" ]]; then
    echo "[reuse] cached database ready; skipping measurement"
    exit 0
  fi
  [[ -d "$CACHE_DB" ]] || { echo "error: cached database files missing at $CACHE_DB" >&2; exit 1; }

  # fresh state copy for this run (write-once rank artifacts excluded)
  local run_id="reuse_$(date +%s)"
  local state_copy="/tmp/reuse-states/$run_id"
  rm -rf "$state_copy"; mkdir -p "$state_copy"
  cp "$CACHE_STATE"/* "$state_copy"/ 2>/dev/null || true
  rm -f "$state_copy"/rank.started "$state_copy"/rank_completion* 2>/dev/null || true

  # restore a fresh database copy into the DB root (fast filesystem).
  # Remove any prior copy of the same database name first (the DB root may
  # hold a leftover or the master itself), then verify the copy completed —
  # a short tmpfs silently truncates the copy and RMDB then rejects the
  # database as "unsupported format".
  rm -rf "$DB_ROOT/$DB_NAME"
  mkdir -p "$DB_ROOT/$DB_NAME"
  # copy with trailing dot so dotfiles (.rmdb_storage_format) are kept
  cp -a "$CACHE_DB/." "$DB_ROOT/$DB_NAME/" || { echo "error: failed to restore database copy" >&2; exit 1; }
  # verify the copy is complete (a short tmpfs silently truncates the copy
  # and RMDB then rejects the database as "unsupported format")
  if ! diff -rq "$CACHE_DB" "$DB_ROOT/$DB_NAME" >/dev/null 2>&1; then
    echo "error: restored database differs from cached copy (DB root too small?)" >&2
    rm -rf "$DB_ROOT/$DB_NAME"
    exit 1
  fi
  echo "[reuse] restored fresh database copy to $DB_ROOT/$DB_NAME"

  # start RMDB against the restored database
  pkill -x rmdb 2>/dev/null || true
  for i in 1 2 3 4 5 6; do ss -tlnp 2>/dev/null | grep -q 8765 || break; sleep 3; done
  ( cd "$DB_ROOT" && ulimit -c 0 2>/dev/null || true; exec "$SERVER_BIN" "$DB_NAME" ) \
    > "/tmp/${run_id}_rmdb.log" 2>&1 &
  # a freshly restored 5GB database takes a while to open on cold page
  # cache; allow up to 240s for readiness
  for i in $(seq 1 120); do ss -tlnp 2>/dev/null | grep -q 8765 && break; sleep 2; done
  if ! ss -tlnp 2>/dev/null | grep -q 8765; then
    echo "error: RMDB did not become ready; see /tmp/${run_id}_rmdb.log" >&2
    tail -5 "/tmp/${run_id}_rmdb.log" >&2
    pkill -x rmdb 2>/dev/null || true
    exit 1
  fi

  # run the smoke measurement directly. Both modes bind the database through
  # --state-dir (same load_bound_state path); rank additionally clears the
  # write-once rank artifacts above so begin_rank() can re-claim the run.
  local -a measure_args
  case "$MODE" in
    preliminary)
      measure_args=(--preliminary)
      ;;
    rank)
      # Official ranked shape: 30s warmup + 3x150s windows. Passing the
      # official values through smoke overrides is not a deviation, so no
      # --allow-deviation is needed.
      measure_args=(--benchmark --warmup-seconds "$WARMUP" --window-seconds "$WINDOW")
      ;;
    *)
      echo "error: unknown --mode '$MODE' (expected 'preliminary' or 'rank')" >&2
      pkill -x rmdb 2>/dev/null || true
      exit 2
      ;;
  esac
  ( cd "$TESTER_DIR" && "$TESTER_BIN" "${measure_args[@]}" \
      --seed "$SEED" --rtt-sim-ms 0 --state-dir "$state_copy" ) \
    > "/tmp/${run_id}_tester.log" 2>&1
  local rc=$?

  pkill -x rmdb 2>/dev/null || true
  # wait for graceful shutdown (rmdb may rewrite files while closing) before
  # removing the restored copy
  for i in $(seq 1 10); do pgrep -x rmdb >/dev/null || break; sleep 2; done
  rm -rf "$DB_ROOT/$DB_NAME" 2>/dev/null || true

  echo "[reuse] exit=$rc"
  grep -E 'new_order_per_min|ranked_new_order_per_min_median|ERROR|执行失败' "/tmp/${run_id}_tester.log" | tail -5
  exit $rc
}

main "$@"
