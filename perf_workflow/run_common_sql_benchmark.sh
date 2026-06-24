#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_RMDB_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
TARGET_DIR="${1:?target dir required}"
LABEL="${2:?label required}"
BUILD_DIR="${3:-build-perf}"
DB_NAME="${4:-compat_bench}"
RECORD_ROOT="${DEFAULT_RMDB_DIR}/performance_test_record"
RESULT_DIR="${RECORD_ROOT}/$(date +%Y%m%d_%H%M%S)_${LABEL}"
SERVER_LOG="${RESULT_DIR}/server.log"
SERVER_PID=""

mkdir -p "${RESULT_DIR}"

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -INT "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

status_file="${RESULT_DIR}/status.txt"
echo "starting" >"${status_file}"

cmake -S "${TARGET_DIR}" -B "${TARGET_DIR}/${BUILD_DIR}" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O2 -g -fno-omit-frame-pointer" \
  >"${RESULT_DIR}/cmake_configure.log" 2>&1
cmake --build "${TARGET_DIR}/${BUILD_DIR}" --target rmdb -j"$(nproc)" \
  >"${RESULT_DIR}/cmake_build.log" 2>&1

(
  cd "${TARGET_DIR}"
  "${TARGET_DIR}/${BUILD_DIR}/bin/rmdb" "${DB_NAME}"
) >"${SERVER_LOG}" 2>&1 &
SERVER_PID=$!

python3 - <<'PY'
import socket, time
for _ in range(100):
    s = socket.socket()
    s.settimeout(0.2)
    try:
        s.connect(("127.0.0.1", 8765))
    except OSError:
        time.sleep(0.1)
    else:
        s.close()
        raise SystemExit(0)
raise SystemExit(1)
PY

python3 "${SCRIPT_DIR}/common_sql_benchmark.py" \
  --host 127.0.0.1 \
  --port 8765 \
  --result-dir "${RESULT_DIR}" \
  >"${RESULT_DIR}/benchmark_path.txt" 2>"${RESULT_DIR}/benchmark.stderr"

echo "success" >"${status_file}"

echo "${RESULT_DIR}"
