#!/bin/bash
# reuse_smoke.sh — thin wrapper around run_workflow.sh database reuse.
#
# Database-reuse acceleration now lives inside run_workflow.sh and is the
# default for every mode: all, preliminary, rank --init-db, and init. The
# loaded database is cached once per kernel source tree hash (HEAD:src) with
# a static checkpoint and a truncated WAL, then restored as a fresh,
# byte-for-byte-verified copy for each measurement.
#
# This script forwards its arguments to run_workflow.sh so existing smoke
# invocations keep working:
#   reuse_smoke.sh <rmdb-dir> [--mode preliminary|rank] [--seed <n>]
#                  [--warmup <sec>] [--window <sec>] [--db-root <dir>]
#                  [--no-measure] [--refresh]
#
#   --no-measure  is forwarded as --mode init (load/cache only)
#   --refresh     drops the cached database for this kernel before running
#   --db-root     is exported as RMDB_DB_ROOT
#   --warmup/--window are forwarded as --warmup-seconds/--window-seconds
set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
WORKFLOW="${SCRIPT_DIR}/run_workflow.sh"

MODE=preliminary
SEED=2026
WARMUP=""
WINDOW=""
DB_ROOT=""
NO_MEASURE=0
REFRESH=0
RMDB_DIR=""

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
      --no-measure) NO_MEASURE=1; shift ;;
      --refresh) REFRESH=1; shift ;;
      -h|--help)
        sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'
        exit 0
        ;;
      *) RMDB_DIR="$arg"; shift ;;
    esac
  done
}

main() {
  parse_args "$@"
  [[ -n "${RMDB_DIR}" ]] || { echo "error: <rmdb-dir> is required" >&2; exit 1; }

  if [[ "${NO_MEASURE}" == "1" ]]; then
    MODE=init
  fi

  local -a command
  command=("${WORKFLOW}" --mode "${MODE}" --seed "${SEED}" \
    --target-dir "${RMDB_DIR}" --allow-deviation --clean-db-on-exit)
  [[ -z "${WARMUP}" ]] || command+=(--warmup-seconds "${WARMUP}")
  [[ -z "${WINDOW}" ]] || command+=(--window-seconds "${WINDOW}")
  if [[ -n "${DB_ROOT}" ]]; then
    export RMDB_DB_ROOT="${DB_ROOT}"
  fi

  if [[ "${REFRESH}" == "1" ]]; then
    local key=""
    if [[ -d "${RMDB_DIR}/.git" || -f "${RMDB_DIR}/.git" ]]; then
      key="$(git -C "${RMDB_DIR}" rev-parse --short=12 HEAD:src 2>/dev/null || echo unknown)"
    else
      key="unknown"
    fi
    echo "[reuse] --refresh: dropping cached database for kernel ${key}"
    rm -rf "${RMDB_DIR}/.reuse-db/${key}"
  fi

  exec env "${command[@]}"
}

main "$@"
