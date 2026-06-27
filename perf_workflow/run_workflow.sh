#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_RMDB_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RMDB_DIR="${RMDB_DIR_OVERRIDE:-${DEFAULT_RMDB_DIR}}"
WORK_ROOT="$(cd "${RMDB_DIR}/.." && pwd)"
RECORD_ROOT_DEFAULT="${DEFAULT_RMDB_DIR}/performance_test_record"
RECORD_ROOT="${RECORD_ROOT_OVERRIDE:-${RECORD_ROOT_DEFAULT}}"
LOCAL_BIN="${HOME}/.local/bin"

export PATH="${LOCAL_BIN}:${PATH}"

MODE="all"
LABEL="manual"
BUILD_DIR="build-perf"
BUILD_TYPE="RelWithDebInfo"
DB_NAME="tpcc_sf50"
HOST="127.0.0.1"
PORT="8765"
SCALE="50"
THREADS="16"
TRANSACTIONS="1000000"
RW_RATIO="0.9130434782608695"
TXN_PROBS="10 10 1 1 1"
TPCC_DIR="${TPCC_TESTER_DIR:-}"
TPCC_BIN=""
TARGET_NAME="$(basename "${RMDB_DIR}")"
INIT_DB=0
RUN_CHECK=0
RUN_DIAGNOSE=0
SKIP_BUILD=0
SKIP_PERF_RECORD=0
PERF_RECORD_SECONDS=""
PERF_STAT_EVENTS="task-clock,cpu-clock,context-switches,cpu-migrations,page-faults,minor-faults,major-faults,cycles,instructions,branches,branch-misses,cache-references,cache-misses,LLC-loads,LLC-load-misses,LLC-stores,LLC-store-misses,dTLB-loads,dTLB-load-misses,iTLB-loads,iTLB-load-misses"
CALLGRIND_TRANSACTIONS="500"
HEAPTRACK_TRANSACTIONS="500"
WARMUP_TRANSACTIONS="40"
WARMUP_SECONDS="30"
MEASURE_SECONDS="60"
TIMED_TRANSACTIONS_CAP="1000000"
TIMED_SHUTDOWN_GRACE="10m"
TPCC_TIMEOUT="5m"
SERVER_START_TIMEOUT_SECONDS="120"
DETACH=0
KEEP_DB_ARTIFACTS=0
RESULT_DIR=""
SERVER_PID=""
SERVER_LOG=""

usage() {
  cat <<'EOF'
Usage:
  run_workflow.sh [options]

Options:
  --mode <all|benchmark|perf|callgrind|heaptrack|tools>
  --label <name>
  --db-name <name>
  --build-dir <dir>
  --target-dir <dir>
  --record-root <dir>
  --host <host>
  --port <port>
  --scale <n>
  --threads <n>
  --transactions <n>
  --rw-ratio <f>
  --txn-probs "<new_order payment order_status delivery stock_level>"
  --tpcc-dir <path>
  --init-db
  --check
  --diagnose
  --skip-build
  --skip-perf-record
  --perf-record-seconds <n>
  --perf-stat-events <events>
  --callgrind-transactions <n>
  --heaptrack-transactions <n>
  --warmup-transactions <n>
  --warmup-seconds <n>
  --measure-seconds <n>
  --server-start-timeout-seconds <n>
  --timed-shutdown-grace <duration>
  --tpcc-timeout <duration>
  --detach
  --keep-db-artifacts
  --help
EOF
}

log() {
  printf '[run_workflow] %s\n' "$*"
}

die() {
  printf '[run_workflow] ERROR: %s\n' "$*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode) MODE="$2"; shift 2 ;;
    --label) LABEL="$2"; shift 2 ;;
    --db-name) DB_NAME="$2"; shift 2 ;;
    --build-dir) BUILD_DIR="$2"; shift 2 ;;
    --target-dir) RMDB_DIR="$2"; WORK_ROOT="$(cd "${RMDB_DIR}/.." && pwd)"; TARGET_NAME="$(basename "${RMDB_DIR}")"; shift 2 ;;
    --record-root) RECORD_ROOT="$2"; shift 2 ;;
    --host) HOST="$2"; shift 2 ;;
    --port) PORT="$2"; shift 2 ;;
    --scale) SCALE="$2"; shift 2 ;;
    --threads) THREADS="$2"; shift 2 ;;
    --transactions) TRANSACTIONS="$2"; shift 2 ;;
    --rw-ratio) RW_RATIO="$2"; shift 2 ;;
    --txn-probs) TXN_PROBS="$2"; shift 2 ;;
    --tpcc-dir) TPCC_DIR="$2"; shift 2 ;;
    --init-db) INIT_DB=1; shift ;;
    --check) RUN_CHECK=1; shift ;;
    --diagnose) RUN_DIAGNOSE=1; shift ;;
    --skip-build) SKIP_BUILD=1; shift ;;
    --skip-perf-record) SKIP_PERF_RECORD=1; shift ;;
    --perf-record-seconds) PERF_RECORD_SECONDS="$2"; shift 2 ;;
    --perf-stat-events) PERF_STAT_EVENTS="$2"; shift 2 ;;
    --callgrind-transactions) CALLGRIND_TRANSACTIONS="$2"; shift 2 ;;
    --heaptrack-transactions) HEAPTRACK_TRANSACTIONS="$2"; shift 2 ;;
    --warmup-transactions) WARMUP_TRANSACTIONS="$2"; shift 2 ;;
    --warmup-seconds) WARMUP_SECONDS="$2"; shift 2 ;;
    --measure-seconds) MEASURE_SECONDS="$2"; shift 2 ;;
    --server-start-timeout-seconds) SERVER_START_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --timed-shutdown-grace) TIMED_SHUTDOWN_GRACE="$2"; shift 2 ;;
    --tpcc-timeout) TPCC_TIMEOUT="$2"; shift 2 ;;
    --detach) DETACH=1; shift ;;
    --keep-db-artifacts) KEEP_DB_ARTIFACTS=1; shift ;;
    --help) usage; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

validate_seconds_option() {
  local name="$1"
  local value="$2"
  local allow_zero="$3"
  if [[ ! "${value}" =~ ^[0-9]+$ ]]; then
    die "${name} must be an integer number of seconds"
  fi
  local numeric=$((10#${value}))
  if [[ "${allow_zero}" == "1" ]]; then
    if (( numeric < 0 )); then
      die "${name} must be non-negative"
    fi
  elif (( numeric <= 0 )); then
    die "${name} must be positive"
  fi
}

if [[ -n "${WARMUP_SECONDS}" ]]; then
  validate_seconds_option "--warmup-seconds" "${WARMUP_SECONDS}" 1
fi
if [[ -n "${MEASURE_SECONDS}" ]]; then
  validate_seconds_option "--measure-seconds" "${MEASURE_SECONDS}" 0
fi
if [[ -z "${PERF_RECORD_SECONDS}" ]]; then
  PERF_RECORD_SECONDS="${MEASURE_SECONDS:-60}"
fi
validate_seconds_option "--perf-record-seconds" "${PERF_RECORD_SECONDS}" 0
validate_seconds_option "--server-start-timeout-seconds" "${SERVER_START_TIMEOUT_SECONDS}" 0
if [[ -z "${TPCC_DIR}" ]]; then
  TPCC_DIR="${RMDB_DIR}/deps/TPCC-Tester"
fi

RUN_TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
RESULT_DIR="${RECORD_ROOT}/${RUN_TIMESTAMP}_${LABEL}"
mkdir -p "${RESULT_DIR}"
SERVER_LOG="${RESULT_DIR}/server.log"

cleanup_server() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -INT "${SERVER_PID}" 2>/dev/null || true
    for _ in $(seq 1 40); do
      kill -0 "${SERVER_PID}" 2>/dev/null || break
      sleep 0.25
    done
    if kill -0 "${SERVER_PID}" 2>/dev/null; then
      kill -TERM "${SERVER_PID}" 2>/dev/null || true
      sleep 1
      kill -KILL "${SERVER_PID}" 2>/dev/null || true
    fi
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  while read -r pid; do
    [[ -z "${pid}" ]] && continue
    kill -TERM "${pid}" 2>/dev/null || true
  done < <(port_listener_pids)
  sleep 0.25
  SERVER_PID=""
}

cleanup() {
  cleanup_server
  if declare -F generate_summary >/dev/null 2>&1; then
    generate_summary || true
  fi
  if [[ "${DETACH}" == "0" && "${KEEP_DB_ARTIFACTS}" == "0" ]]; then
    cleanup_database_artifacts
  fi
}
trap cleanup EXIT

write_file() {
  local path="$1"
  shift
  printf '%s\n' "$@" >"${path}"
}

append_file() {
  local path="$1"
  shift
  printf '%s\n' "$@" >>"${path}"
}

tool_path() {
  which "$1" 2>/dev/null | head -n 1 || true
}

record_tool_status() {
  local status_file="${RESULT_DIR}/tool_status.txt"
  {
    echo "perf=$(tool_path perf)"
    echo "valgrind=$(tool_path valgrind)"
    echo "callgrind_control=$(tool_path callgrind_control)"
    echo "callgrind_annotate=$(tool_path callgrind_annotate)"
    echo "flamegraph.pl=$(tool_path flamegraph.pl)"
    echo "stackcollapse-perf.pl=$(tool_path stackcollapse-perf.pl)"
    echo "heaptrack=$(tool_path heaptrack)"
    echo "heaptrack_print=$(tool_path heaptrack_print)"
    echo "heaptrack_gui=$(tool_path heaptrack_gui)"
    echo "hotspot=$(tool_path hotspot)"
    echo "trace_processor=$(tool_path trace_processor)"
    echo "traceconv=$(tool_path traceconv)"
    echo "home_flamegraph=${HOME}/FlameGraph"
    if [[ -r /proc/sys/kernel/yama/ptrace_scope ]]; then
      echo "kernel.yama.ptrace_scope=$(< /proc/sys/kernel/yama/ptrace_scope)"
    else
      echo "kernel.yama.ptrace_scope=unavailable"
    fi
  } >"${status_file}"
  if [[ -x "$(tool_path hotspot)" ]]; then
    {
      echo
      echo "[ldd hotspot]"
      ldd "$(tool_path hotspot)" || true
    } >>"${status_file}"
  fi
  if [[ -x "$(tool_path heaptrack_gui)" ]]; then
    {
      echo
      echo "[ldd heaptrack_gui]"
      ldd "$(tool_path heaptrack_gui)" || true
    } >>"${status_file}"
  fi
}

ensure_heaptrack_attach_allowed() {
  local out_dir="$1"
  local ptrace_scope_file="/proc/sys/kernel/yama/ptrace_scope"
  local ptrace_log="${out_dir}/heaptrack_ptrace_scope.txt"

  if [[ ! -e "${ptrace_scope_file}" ]]; then
    echo "kernel.yama.ptrace_scope=unavailable" >"${ptrace_log}"
    return 0
  fi
  if [[ ! -r "${ptrace_scope_file}" ]]; then
    echo "kernel.yama.ptrace_scope=unreadable" >"${ptrace_log}"
    return 0
  fi

  local current_scope
  current_scope="$(< "${ptrace_scope_file}")"
  echo "kernel.yama.ptrace_scope.before=${current_scope}" >"${ptrace_log}"
  if [[ "${current_scope}" == "0" ]]; then
    echo "kernel.yama.ptrace_scope.after=${current_scope}" >>"${ptrace_log}"
    return 0
  fi

  if ! command -v sudo >/dev/null 2>&1 || ! sudo -n true >/dev/null 2>&1; then
    die "heaptrack attach requires kernel.yama.ptrace_scope=0 or passwordless sudo"
  fi

  sudo -n sysctl -w kernel.yama.ptrace_scope=0 >>"${ptrace_log}" 2>&1 \
    || die "failed to set kernel.yama.ptrace_scope=0; see ${ptrace_log}"
  echo "kernel.yama.ptrace_scope.after=$(< "${ptrace_scope_file}")" >>"${ptrace_log}"
}

record_system_info() {
  {
    echo "date=$(date -Is 2>/dev/null || date '+%Y-%m-%dT%H:%M:%S%z')"
    echo "cwd=${WORK_ROOT}"
    echo "rmdb_dir=${RMDB_DIR}"
    echo "result_dir=${RESULT_DIR}"
    echo "kernel=$(uname -a)"
    echo "os_release="
    if [[ -r /etc/os-release ]]; then
      cat /etc/os-release
    else
      sw_vers 2>/dev/null || true
    fi
  } >"${RESULT_DIR}/system_info.txt"
}

record_manifest() {
  {
    echo "mode=${MODE}"
    echo "label=${LABEL}"
    echo "db_name=${DB_NAME}"
    echo "target_name=${TARGET_NAME}"
    echo "target_dir=${RMDB_DIR}"
    echo "build_dir=${BUILD_DIR}"
    echo "record_root=${RECORD_ROOT}"
    echo "host=${HOST}"
    echo "port=${PORT}"
    echo "scale=${SCALE}"
    echo "threads=${THREADS}"
    echo "transactions=${TRANSACTIONS}"
    echo "rw_ratio=${RW_RATIO}"
    echo "txn_probs=${TXN_PROBS}"
    echo "tpcc_dir=${TPCC_DIR}"
    echo "init_db=${INIT_DB}"
    echo "run_check=${RUN_CHECK}"
    echo "run_diagnose=${RUN_DIAGNOSE}"
    echo "skip_build=${SKIP_BUILD}"
    echo "skip_perf_record=${SKIP_PERF_RECORD}"
    echo "perf_record_seconds=${PERF_RECORD_SECONDS}"
    echo "perf_stat_events=${PERF_STAT_EVENTS}"
    echo "callgrind_transactions=${CALLGRIND_TRANSACTIONS}"
    echo "heaptrack_transactions=${HEAPTRACK_TRANSACTIONS}"
    echo "warmup_transactions=${WARMUP_TRANSACTIONS}"
    echo "warmup_seconds=${WARMUP_SECONDS}"
    echo "measure_seconds=${MEASURE_SECONDS}"
    echo "timed_transactions_cap=${TIMED_TRANSACTIONS_CAP}"
    echo "timed_shutdown_grace=${TIMED_SHUTDOWN_GRACE}"
    echo "tpcc_timeout=${TPCC_TIMEOUT}"
    echo "server_start_timeout_seconds=${SERVER_START_TIMEOUT_SECONDS}"
    echo "keep_db_artifacts=${KEEP_DB_ARTIFACTS}"
  } >"${RESULT_DIR}/manifest.txt"
}

ensure_tpcc_tester() {
  if [[ ! -f "${TPCC_DIR}/Cargo.toml" ]]; then
    if [[ -d "${RMDB_DIR}/.git" || -f "${RMDB_DIR}/.git" ]]; then
      log "initializing TPCC-Tester submodule"
      git -C "${RMDB_DIR}" submodule update --init --recursive deps/TPCC-Tester \
        >"${RESULT_DIR}/tpcc_submodule_update.log" 2>&1 || true
    fi
  fi
  [[ -f "${TPCC_DIR}/Cargo.toml" ]] \
    || die "TPCC-Tester submodule not found at ${TPCC_DIR}; run: git submodule update --init --recursive deps/TPCC-Tester"
  log "building TPCC-Tester"
  cargo build --release --manifest-path "${TPCC_DIR}/Cargo.toml" \
    >"${RESULT_DIR}/tpcc_build.log" 2>&1
  TPCC_BIN="${TPCC_DIR}/target/release/tpcc-tester"
  [[ -x "${TPCC_BIN}" ]] || die "tpcc-tester binary not found at ${TPCC_BIN}"
}

build_rmdb() {
  [[ "${SKIP_BUILD}" == "1" ]] && return 0
  log "configuring RMDB into ${BUILD_DIR}"
  cmake -S "${RMDB_DIR}" -B "${RMDB_DIR}/${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O2 -g -fno-omit-frame-pointer" \
    >"${RESULT_DIR}/cmake_configure.log" 2>&1
  log "building RMDB"
  cmake --build "${RMDB_DIR}/${BUILD_DIR}" --target rmdb -j"$(nproc)" \
    >"${RESULT_DIR}/cmake_build.log" 2>&1
}

server_bin() {
  printf '%s\n' "${RMDB_DIR}/${BUILD_DIR}/bin/rmdb"
}

database_path() {
  printf '%s\n' "${RMDB_DIR}/${DB_NAME}"
}

reset_database_dir() {
  local db_path
  db_path="$(database_path)"
  if [[ -e "${db_path}" ]]; then
    log "removing existing database directory ${db_path}"
    rm -rf "${db_path}"
  fi
}

cleanup_database_artifacts() {
  local db_path
  db_path="$(database_path)"
  if [[ -e "${db_path}" ]]; then
    log "cleaning database directory ${db_path}"
    rm -rf "${db_path}"
  fi
  local table_data_dir="${RMDB_DIR}/src/test/performance_test/table_data"
  if [[ -e "${table_data_dir}" ]]; then
    log "cleaning generated CSV files in ${table_data_dir}"
    rm -f "${table_data_dir}"/*.csv
  fi
}

port_listener_pids() {
  ss -ltnpH "( sport = :${PORT} )" 2>/dev/null \
    | grep -o 'pid=[0-9]\+' \
    | cut -d= -f2 \
    | sort -u
}

describe_port_listeners() {
  local found=0
  while read -r pid; do
    [[ -z "${pid}" ]] && continue
    found=1
    ps -p "${pid}" -o pid=,ppid=,cmd= || true
  done < <(port_listener_pids)
  if [[ "${found}" == "0" ]]; then
    echo "(none)"
  fi
}

ensure_port_available() {
  local listeners
  listeners="$(port_listener_pids || true)"
  [[ -z "${listeners}" ]] && return 0
  {
    echo "port ${HOST}:${PORT} is already in use before startup"
    echo
    echo "[ss]"
    ss -ltnp "( sport = :${PORT} )" || true
    echo
    echo "[ps]"
    describe_port_listeners
  } >"${RESULT_DIR}/port_conflict.txt"
  die "port ${HOST}:${PORT} is already in use; stop the existing listener first. See ${RESULT_DIR}/port_conflict.txt"
}

wait_for_server() {
  local timeout_seconds="${1:-20}"
  local retries=$((timeout_seconds * 4))
  while (( retries > 0 )); do
    if [[ -z "${SERVER_PID}" ]] || ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      return 1
    fi
    if [[ -z "$(port_listener_pids)" ]]; then
      sleep 0.25
      retries=$((retries - 1))
      continue
    fi
    if python3 - <<PY
import socket
s = socket.socket()
s.settimeout(0.2)
try:
    s.connect(("${HOST}", int("${PORT}")))
except OSError:
    raise SystemExit(1)
else:
    s.close()
    raise SystemExit(0)
PY
    then
      return 0
    fi
    sleep 0.25
    retries=$((retries - 1))
  done
  return 1
}

start_server() {
  local mode_name="$1"
  shift || true
  cleanup_server
  ensure_port_available
  : >"${SERVER_LOG}"
  log "starting RMDB server (${mode_name})"
  (
    cd "${RMDB_DIR}"
    RMDB_PORT="${PORT}" "$@" "$(server_bin)" "${DB_NAME}"
  ) >"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  local start_timeout="${SERVER_START_TIMEOUT_SECONDS}"
  wait_for_server "${start_timeout}" || {
    {
      echo
      echo "[ss after failed startup]"
      ss -ltnp "( sport = :${PORT} )" || true
      echo
      echo "[ps after failed startup]"
      describe_port_listeners
    } >>"${SERVER_LOG}"
    die "RMDB server failed to listen on ${HOST}:${PORT}; see ${SERVER_LOG}"
  }
  echo "${SERVER_PID}" >"${RESULT_DIR}/server.pid"
}

run_tpcc() {
  local log_path="$1"
  shift
  (
    cd "${TPCC_DIR}"
    timeout "${TPCC_TIMEOUT}" "${TPCC_BIN}" --host "${HOST}" --port "${PORT}" -s "${SCALE}" "$@"
  ) >"${log_path}" 2>&1
}

run_tpcc_for_seconds() {
  local log_path="$1"
  local seconds="$2"
  shift 2
  local rc=0
  (
    cd "${TPCC_DIR}"
    timeout -s INT -k "${TIMED_SHUTDOWN_GRACE}" "${seconds}s" "${TPCC_BIN}" --host "${HOST}" --port "${PORT}" -s "${SCALE}" "$@"
  ) >"${log_path}" 2>&1 || rc=$?

  # GNU timeout exits 124 after sending SIGINT even when tpcc-tester catches it
  # and prints the final benchmark report. Treat that as a successful timed run.
  if [[ "${rc}" == "124" ]] && grep -q "TPC-C CONCURRENT BENCHMARK RESULTS" "${log_path}"; then
    return 0
  fi
  return "${rc}"
}

maybe_append_txn_probs() {
  local -n cmd_ref=$1
  if [[ -n "${TXN_PROBS}" ]]; then
    cmd_ref+=(--txn-probs)
    IFS=' ' read -r -a probs <<<"${TXN_PROBS}"
    cmd_ref+=("${probs[@]}")
  fi
}

prepare_database() {
  if [[ "${INIT_DB}" == "1" ]]; then
    reset_database_dir
  fi
  start_server "prepare"
  if [[ "${RUN_DIAGNOSE}" == "1" ]]; then
    log "running TPCC diagnose -> ${RESULT_DIR}/diagnose.log"
    run_tpcc "${RESULT_DIR}/diagnose.log" --diagnose \
      || die "TPCC diagnose failed; see ${RESULT_DIR}/diagnose.log"
  fi
  if [[ "${INIT_DB}" == "1" ]]; then
    log "running TPCC schema setup -> ${RESULT_DIR}/schema.log"
    run_tpcc "${RESULT_DIR}/schema.log" --create-schema \
      || die "TPCC schema setup failed; see ${RESULT_DIR}/schema.log"
    log "running TPCC init -> ${RESULT_DIR}/init.log"
    run_tpcc "${RESULT_DIR}/init.log" --init \
      || die "TPCC init failed; see ${RESULT_DIR}/init.log"
  fi
  if [[ "${RUN_CHECK}" == "1" ]]; then
    log "running TPCC check -> ${RESULT_DIR}/check.log"
    run_tpcc "${RESULT_DIR}/check.log" --check --expected-new-orders 0 \
      || die "TPCC check failed; see ${RESULT_DIR}/check.log"
  fi
  cleanup_server
}

run_benchmark_phase() {
  local phase_name="$1"
  local transactions="$2"
  local log_path="${RESULT_DIR}/${phase_name}.log"
  local benchmark_transactions="${transactions}"
  if [[ -n "${MEASURE_SECONDS}" ]]; then
    benchmark_transactions="${TIMED_TRANSACTIONS_CAP}"
  fi
  local cmd=(--benchmark --threads "${THREADS}" --transactions "${benchmark_transactions}" --rw-ratio "${RW_RATIO}")
  maybe_append_txn_probs cmd
  start_server "${phase_name}"
  if [[ -n "${WARMUP_SECONDS}" ]]; then
    if [[ "${WARMUP_SECONDS}" != "0" ]]; then
      local warmup_log="${RESULT_DIR}/${phase_name}_warmup.log"
      local warmup_cmd=(--benchmark --threads "${THREADS}" --transactions "${TIMED_TRANSACTIONS_CAP}" --rw-ratio "${RW_RATIO}")
      maybe_append_txn_probs warmup_cmd
      log "running TPCC warmup (${phase_name}, ${WARMUP_SECONDS}s) -> ${warmup_log}"
      run_tpcc_for_seconds "${warmup_log}" "${WARMUP_SECONDS}" "${warmup_cmd[@]}" \
        || die "TPCC warmup failed; see ${warmup_log}"
    fi
  elif [[ "${WARMUP_TRANSACTIONS}" != "0" ]]; then
    local warmup_log="${RESULT_DIR}/${phase_name}_warmup.log"
    local warmup_cmd=(--benchmark --threads "${THREADS}" --transactions "${WARMUP_TRANSACTIONS}" --rw-ratio "${RW_RATIO}")
    maybe_append_txn_probs warmup_cmd
    log "running TPCC warmup (${phase_name}) -> ${warmup_log}"
    run_tpcc "${warmup_log}" "${warmup_cmd[@]}" \
      || die "TPCC warmup failed; see ${warmup_log}"
  fi
  if [[ -n "${MEASURE_SECONDS}" ]]; then
    log "running TPCC benchmark (${phase_name}, ${MEASURE_SECONDS}s) -> ${log_path}"
    run_tpcc_for_seconds "${log_path}" "${MEASURE_SECONDS}" "${cmd[@]}" \
      || die "TPCC benchmark failed; see ${log_path}"
  else
    log "running TPCC benchmark (${phase_name}) -> ${log_path}"
    run_tpcc "${log_path}" "${cmd[@]}" \
      || die "TPCC benchmark failed; see ${log_path}"
  fi
  cleanup_server
}

run_timed_warmup_phase() {
  local phase_name="$1"
  local out_dir="$2"
  if [[ -n "${WARMUP_SECONDS}" ]]; then
    if [[ "${WARMUP_SECONDS}" == "0" ]]; then
      return 0
    fi
    local warmup_log="${out_dir}/${phase_name}_warmup.log"
    local warmup_cmd=(--benchmark --threads "${THREADS}" --transactions "${TIMED_TRANSACTIONS_CAP}" --rw-ratio "${RW_RATIO}")
    maybe_append_txn_probs warmup_cmd
    log "running TPCC warmup (${phase_name}, ${WARMUP_SECONDS}s) -> ${warmup_log}"
    run_tpcc_for_seconds "${warmup_log}" "${WARMUP_SECONDS}" "${warmup_cmd[@]}"
    return $?
  fi
  if [[ "${WARMUP_TRANSACTIONS}" != "0" ]]; then
    local warmup_log="${out_dir}/${phase_name}_warmup.log"
    local warmup_cmd=(--benchmark --threads "${THREADS}" --transactions "${WARMUP_TRANSACTIONS}" --rw-ratio "${RW_RATIO}")
    maybe_append_txn_probs warmup_cmd
    log "running TPCC warmup (${phase_name}) -> ${warmup_log}"
    run_tpcc "${warmup_log}" "${warmup_cmd[@]}"
  fi
}

supported_perf_events() {
  local events_csv="$1"
  local supported=()
  IFS=',' read -r -a candidates <<<"${events_csv}"
  for event in "${candidates[@]}"; do
    [[ -n "${event}" ]] || continue
    if perf stat -x, -e "${event}" -- sleep 0.01 >/dev/null 2>&1; then
      supported+=("${event}")
    else
      echo "${event}" >>"${RESULT_DIR}/perf_unsupported_events.txt"
    fi
  done
  local joined
  joined="$(IFS=,; echo "${supported[*]}")"
  if [[ -z "${joined}" ]]; then
    joined="task-clock,context-switches,cpu-migrations,page-faults"
  fi
  printf '%s\n' "${joined}"
}

run_perf_phase() {
  local perf_dir="${RESULT_DIR}/perf"
  mkdir -p "${perf_dir}"
  local bench_log="${perf_dir}/benchmark.log"
  local perf_stat="${perf_dir}/perf_stat.csv"
  local perf_data="${perf_dir}/perf.data"
  local perf_script_out="${perf_dir}/perf.script"
  local perf_folded="${perf_dir}/perf.folded"
  local perf_svg="${perf_dir}/perf.svg"
  local bench_cmd=(--benchmark --threads "${THREADS}" --transactions "${TRANSACTIONS}" --rw-ratio "${RW_RATIO}")
  maybe_append_txn_probs bench_cmd

  start_server "perf"
  run_timed_warmup_phase "perf" "${perf_dir}" || true
  local perf_event_list
  perf_event_list="$(supported_perf_events "${PERF_STAT_EVENTS}")"
  echo "${perf_event_list}" >"${perf_dir}/perf_stat_events.txt"

  log "running perf stat"
  "${TPCC_BIN}" --host "${HOST}" --port "${PORT}" -s "${SCALE}" "${bench_cmd[@]}" \
    >"${bench_log}" 2>&1 &
  local bench_pid=$!
  sleep 1
  perf stat -x, -o "${perf_stat}" \
    -e "${perf_event_list}" \
    -p "${SERVER_PID}" -- sleep "${PERF_RECORD_SECONDS}" \
    >"${perf_dir}/perf_stat.stdout" 2>"${perf_dir}/perf_stat.stderr" || true
  kill -INT "${bench_pid}" 2>/dev/null || true
  wait "${bench_pid}" 2>/dev/null || true

  if [[ "${SKIP_PERF_RECORD}" == "0" ]]; then
    run_timed_warmup_phase "perf_record" "${perf_dir}" || true
    log "running perf record"
    "${TPCC_BIN}" --host "${HOST}" --port "${PORT}" -s "${SCALE}" "${bench_cmd[@]}" \
      >"${perf_dir}/benchmark_record.log" 2>&1 &
    bench_pid=$!
    sleep 1
    perf record -F 99 -g -e cpu-clock -o "${perf_data}" -p "${SERVER_PID}" -- sleep "${PERF_RECORD_SECONDS}" \
      >"${perf_dir}/perf_record.stdout" 2>"${perf_dir}/perf_record.stderr" || true
    kill -INT "${bench_pid}" 2>/dev/null || true
    wait "${bench_pid}" 2>/dev/null || true

    if [[ -s "${perf_data}" ]] && command -v stackcollapse-perf.pl >/dev/null 2>&1 && command -v flamegraph.pl >/dev/null 2>&1; then
      perf script -i "${perf_data}" >"${perf_script_out}" 2>"${perf_dir}/perf_script.stderr" || true
      stackcollapse-perf.pl "${perf_script_out}" >"${perf_folded}" 2>"${perf_dir}/stackcollapse.stderr" || true
      flamegraph.pl "${perf_folded}" >"${perf_svg}" 2>"${perf_dir}/flamegraph.stderr" || true
    fi
  fi
  cleanup_server
}

run_callgrind_phase() {
  local out_dir="${RESULT_DIR}/callgrind"
  mkdir -p "${out_dir}"
  if ! command -v valgrind >/dev/null 2>&1; then
    log "skipping callgrind: valgrind not found"
    echo "skipped: valgrind not found" >"${out_dir}/skipped.txt"
    return 0
  fi
  if ! command -v callgrind_control >/dev/null 2>&1; then
    log "skipping callgrind: callgrind_control not found"
    echo "skipped: callgrind_control not found" >"${out_dir}/skipped.txt"
    return 0
  fi
  local callgrind_out="${out_dir}/callgrind.out"
  local callgrind_online_out="${out_dir}/callgrind.online.out"
  local callgrind_shutdown_out="${out_dir}/callgrind.shutdown.out"
  local control_log="${out_dir}/callgrind_control.log"
  start_server "callgrind" valgrind --tool=callgrind --instr-atstart=no --callgrind-out-file="${callgrind_out}"
  run_timed_warmup_phase "callgrind" "${out_dir}" || true
  log "starting callgrind counters after RMDB recovery"
  {
    echo "[callgrind_control -i on ${SERVER_PID}]"
    callgrind_control -i on "${SERVER_PID}"
    echo
    echo "[callgrind_control -z ${SERVER_PID}]"
    callgrind_control -z "${SERVER_PID}"
  } >"${control_log}" 2>&1 || die "callgrind_control failed; see ${control_log}"
  local tpcc_rc=0
  local bench_cmd=(--benchmark --threads "${THREADS}" --transactions "${CALLGRIND_TRANSACTIONS}" --rw-ratio "${RW_RATIO}")
  maybe_append_txn_probs bench_cmd
  if [[ -n "${MEASURE_SECONDS}" && "${MEASURE_SECONDS}" != "0" ]]; then
    log "running callgrind benchmark (${MEASURE_SECONDS}s) -> ${out_dir}/benchmark.log"
    run_tpcc_for_seconds "${out_dir}/benchmark.log" "${MEASURE_SECONDS}" "${bench_cmd[@]}" || tpcc_rc=$?
  else
    log "running callgrind benchmark -> ${out_dir}/benchmark.log"
    run_tpcc "${out_dir}/benchmark.log" "${bench_cmd[@]}" || tpcc_rc=$?
  fi
  log "dumping callgrind counters before RMDB shutdown"
  {
    echo
    echo "[callgrind_control -d ${SERVER_PID}]"
    callgrind_control -d "${SERVER_PID}"
    echo
    callgrind_dump=""
    for candidate in "${out_dir}"/callgrind.out*; do
      [[ -s "${candidate}" ]] || continue
      callgrind_dump="${candidate}"
    done
    if [[ -n "${callgrind_dump}" ]]; then
      echo "[copy online callgrind dump]"
      echo "source=${callgrind_dump}"
      cp "${callgrind_dump}" "${callgrind_online_out}"
      ls -l "${callgrind_online_out}"
    else
      echo "[copy online callgrind dump]"
      echo "missing callgrind dump matching ${out_dir}/callgrind.out*"
    fi
    echo
    echo "[callgrind_control -i off ${SERVER_PID}]"
    callgrind_control -i off "${SERVER_PID}"
    echo
    echo "[callgrind_control -z ${SERVER_PID}]"
    callgrind_control -z "${SERVER_PID}"
  } >>"${control_log}" 2>&1 || {
    cleanup_server
    die "callgrind final dump failed; see ${control_log}"
  }
  cleanup_server
  if [[ -s "${callgrind_out}" ]]; then
    cp "${callgrind_out}" "${callgrind_shutdown_out}" 2>/dev/null || true
  fi
  if [[ -s "${callgrind_online_out}" ]]; then
    cp "${callgrind_online_out}" "${callgrind_out}"
  fi
  if [[ "${tpcc_rc}" != "0" ]]; then
    return "${tpcc_rc}"
  fi
  if [[ -s "${callgrind_out}" ]]; then
    callgrind_annotate --inclusive=yes "${callgrind_out}" >"${out_dir}/callgrind_annotate.txt" 2>"${out_dir}/callgrind_annotate.stderr" || true
  fi
}

run_heaptrack_phase() {
  local out_dir="${RESULT_DIR}/heaptrack"
  mkdir -p "${out_dir}"
  if ! command -v heaptrack >/dev/null 2>&1; then
    log "skipping heaptrack: heaptrack not found"
    echo "skipped: heaptrack not found" >"${out_dir}/skipped.txt"
    return 0
  fi
  local heaptrack_cmd_log="${out_dir}/heaptrack_command.txt"
  start_server "heaptrack"
  run_timed_warmup_phase "heaptrack" "${out_dir}" || true
  log "attaching heaptrack after RMDB recovery"
  ensure_heaptrack_attach_allowed "${out_dir}"
  local -a heaptrack_cmd=(heaptrack --pid "${SERVER_PID}")
  printf '%q ' "${heaptrack_cmd[@]}" >"${heaptrack_cmd_log}"
  printf '\n' >>"${heaptrack_cmd_log}"
  (
    cd "${out_dir}"
    "${heaptrack_cmd[@]}"
  ) \
    >"${out_dir}/heaptrack.stdout" 2>"${out_dir}/heaptrack.stderr" &
  local heaptrack_pid=$!
  sleep 2
  if ! kill -0 "${heaptrack_pid}" 2>/dev/null; then
    wait "${heaptrack_pid}" 2>/dev/null || true
    cleanup_server
    die "heaptrack attach failed; see ${out_dir}/heaptrack.stderr"
  fi
  local tpcc_rc=0
  local bench_cmd=(--benchmark --threads "${THREADS}" --transactions "${HEAPTRACK_TRANSACTIONS}" --rw-ratio "${RW_RATIO}")
  maybe_append_txn_probs bench_cmd
  if [[ -n "${MEASURE_SECONDS}" && "${MEASURE_SECONDS}" != "0" ]]; then
    log "running heaptrack benchmark (${MEASURE_SECONDS}s) -> ${out_dir}/benchmark.log"
    run_tpcc_for_seconds "${out_dir}/benchmark.log" "${MEASURE_SECONDS}" "${bench_cmd[@]}" || tpcc_rc=$?
  else
    log "running heaptrack benchmark -> ${out_dir}/benchmark.log"
    run_tpcc "${out_dir}/benchmark.log" "${bench_cmd[@]}" || tpcc_rc=$?
  fi
  kill -INT "${heaptrack_pid}" 2>/dev/null || true
  for _ in $(seq 1 40); do
    kill -0 "${heaptrack_pid}" 2>/dev/null || break
    sleep 0.25
  done
  if kill -0 "${heaptrack_pid}" 2>/dev/null; then
    kill -TERM "${heaptrack_pid}" 2>/dev/null || true
  fi
  wait "${heaptrack_pid}" 2>/dev/null || true
  cleanup_server
  if [[ "${tpcc_rc}" != "0" ]]; then
    return "${tpcc_rc}"
  fi
  local heaptrack_result=""
  while IFS= read -r -d '' candidate; do
    heaptrack_result="${candidate}"
    break
  done < <(find "${out_dir}" -maxdepth 1 -type f \
    \( -name 'heaptrack*.gz' -o -name 'heaptrack*.zst' \) -print0 | sort -z)
  if [[ -s "${heaptrack_result}" ]] && command -v heaptrack_print >/dev/null 2>&1; then
    heaptrack_print "${heaptrack_result}" >"${out_dir}/heaptrack_print.txt" 2>"${out_dir}/heaptrack_print.stderr" || true
  fi
}

generate_summary() {
  python3 "${SCRIPT_DIR}/summarize_perf_run.py" "${RESULT_DIR}" >"${RESULT_DIR}/summary.md"
}

run_detached() {
  local status_file="${RESULT_DIR}/detach_status.txt"
  local script_copy="${RESULT_DIR}/run_detached_inner.sh"
  cat >"${script_copy}" <<EOF
#!/usr/bin/env bash
set -euo pipefail
trap '' HUP
$(printf 'cd %q\n' "${WORK_ROOT}")
$(printf 'export PATH=%q:"$PATH"\n' "${LOCAL_BIN}")
  $(printf 'bash %q --mode %q --label %q --db-name %q --build-dir %q --target-dir %q --record-root %q --host %q --port %q --scale %q --threads %q --transactions %q --rw-ratio %q --perf-record-seconds %q --perf-stat-events %q --callgrind-transactions %q --heaptrack-transactions %q --warmup-transactions %q --server-start-timeout-seconds %q --timed-shutdown-grace %q %s %s %s %s %s %s %s %s\n' \
  "${SCRIPT_DIR}/run_workflow.sh" "${MODE}" "${LABEL}" "${DB_NAME}" "${BUILD_DIR}" "${RMDB_DIR}" "${RECORD_ROOT}" "${HOST}" "${PORT}" "${SCALE}" "${THREADS}" "${TRANSACTIONS}" "${RW_RATIO}" "${PERF_RECORD_SECONDS}" "${PERF_STAT_EVENTS}" "${CALLGRIND_TRANSACTIONS}" "${HEAPTRACK_TRANSACTIONS}" "${WARMUP_TRANSACTIONS}" \
  "${SERVER_START_TIMEOUT_SECONDS}" "${TIMED_SHUTDOWN_GRACE}" \
  "$( [[ -n "${WARMUP_SECONDS}" ]] && echo "--warmup-seconds ${WARMUP_SECONDS}" )" \
  "$( [[ -n "${MEASURE_SECONDS}" ]] && echo "--measure-seconds ${MEASURE_SECONDS}" )" \
  "$( [[ "${INIT_DB}" == "1" ]] && echo --init-db )" \
  "$( [[ "${RUN_CHECK}" == "1" ]] && echo --check )" \
  "$( [[ "${RUN_DIAGNOSE}" == "1" ]] && echo --diagnose )" \
  "$( [[ "${SKIP_BUILD}" == "1" ]] && echo --skip-build )" \
  "$( [[ "${SKIP_PERF_RECORD}" == "1" ]] && echo --skip-perf-record )" \
  "$( [[ "${KEEP_DB_ARTIFACTS}" == "1" ]] && echo --keep-db-artifacts )" )
EOF
  chmod +x "${script_copy}"
  nohup "${script_copy}" >"${RESULT_DIR}/detach_stdout.log" 2>"${RESULT_DIR}/detach_stderr.log" &
  local detached_pid=$!
  {
    echo "pid=${detached_pid}"
    echo "result_dir=${RESULT_DIR}"
  } >"${status_file}"
  echo "${status_file}"
}

record_system_info
record_manifest
record_tool_status

if [[ "${DETACH}" == "1" ]]; then
  run_detached
  exit 0
fi

if [[ "${MODE}" == "tools" ]]; then
  generate_summary
  log "results written to ${RESULT_DIR}"
  exit 0
fi

ensure_tpcc_tester
build_rmdb
prepare_database

case "${MODE}" in
  benchmark)
    run_benchmark_phase "benchmark" "${TRANSACTIONS}"
    ;;
  perf)
    run_perf_phase
    ;;
  callgrind)
    run_callgrind_phase
    ;;
  heaptrack)
    run_heaptrack_phase
    ;;
  all)
    run_benchmark_phase "benchmark" "${TRANSACTIONS}"
    run_perf_phase
    run_callgrind_phase
    run_heaptrack_phase
    ;;
  *)
    die "unsupported mode: ${MODE}"
    ;;
esac

generate_summary
log "results written to ${RESULT_DIR}"
