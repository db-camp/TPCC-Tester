#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
LOCAL_BIN="${HOME}/.local/bin"
LOCAL_OPT="${HOME}/.local/opt"
TMP_DIR="${TMPDIR:-/tmp}/rmdb_perf_tools"
HOME_FLAMEGRAPH_DIR="${HOME}/FlameGraph"

mkdir -p "${LOCAL_BIN}" "${LOCAL_OPT}" "${TMP_DIR}"

log() {
  printf '[install_optional_tools] %s\n' "$*"
}

link_if_exists() {
  local src="$1"
  local dst="$2"
  if [[ -e "${src}" ]]; then
    ln -sf "${src}" "${dst}"
  fi
}

install_flamegraph() {
  local repo_dir=""
  if [[ -x "${HOME_FLAMEGRAPH_DIR}/flamegraph.pl" ]]; then
    repo_dir="${HOME_FLAMEGRAPH_DIR}"
    log "using existing FlameGraph checkout at ${repo_dir}"
  else
    repo_dir="${LOCAL_OPT}/FlameGraph"
    if [[ ! -d "${repo_dir}/.git" ]]; then
      log "cloning FlameGraph into ${repo_dir}"
      rm -rf "${repo_dir}"
      git clone --depth 1 https://github.com/brendangregg/FlameGraph "${repo_dir}"
    else
      log "FlameGraph already present at ${repo_dir}"
    fi
  fi
  link_if_exists "${repo_dir}/flamegraph.pl" "${LOCAL_BIN}/flamegraph.pl"
  link_if_exists "${repo_dir}/stackcollapse-perf.pl" "${LOCAL_BIN}/stackcollapse-perf.pl"
}

download_and_extract_deb() {
  local package_name="$1"
  local target_dir="$2"
  local deb_path

  mkdir -p "${target_dir}"
  pushd "${TMP_DIR}" >/dev/null
  rm -f "${package_name}"_*.deb
  if ! apt download "${package_name}" >/dev/null 2>"${TMP_DIR}/${package_name}.err"; then
    log "apt download failed for ${package_name}; see ${TMP_DIR}/${package_name}.err"
    popd >/dev/null
    return 1
  fi
  deb_path="$(find "${TMP_DIR}" -maxdepth 1 -name "${package_name}_*.deb" | head -n 1)"
  if [[ -z "${deb_path}" ]]; then
    log "downloaded package for ${package_name} not found"
    popd >/dev/null
    return 1
  fi
  rm -rf "${target_dir}"
  mkdir -p "${target_dir}"
  dpkg-deb -x "${deb_path}" "${target_dir}"
  popd >/dev/null
}

install_heaptrack() {
  if command -v heaptrack >/dev/null 2>&1 && command -v heaptrack_print >/dev/null 2>&1; then
    log "using system heaptrack at $(command -v heaptrack)"
    return 0
  fi
  local target_dir="${LOCAL_OPT}/heaptrack"
  if download_and_extract_deb "heaptrack" "${target_dir}"; then
    link_if_exists "${target_dir}/usr/bin/heaptrack" "${LOCAL_BIN}/heaptrack"
    link_if_exists "${target_dir}/usr/bin/heaptrack_print" "${LOCAL_BIN}/heaptrack_print"
    log "heaptrack installed under ${target_dir}"
  fi
}

install_hotspot() {
  if command -v hotspot >/dev/null 2>&1; then
    log "using system hotspot at $(command -v hotspot)"
    return 0
  fi
  local target_dir="${LOCAL_OPT}/hotspot"
  if download_and_extract_deb "hotspot" "${target_dir}"; then
    link_if_exists "${target_dir}/usr/bin/hotspot" "${LOCAL_BIN}/hotspot"
    log "hotspot unpacked under ${target_dir}"
  fi
}

install_perfetto_wrappers() {
  local trace_processor="${LOCAL_BIN}/trace_processor"
  local traceconv="${LOCAL_BIN}/traceconv"

  if [[ ! -x "${trace_processor}" ]]; then
    log "downloading Perfetto trace_processor wrapper"
    curl -fsSL https://get.perfetto.dev/trace_processor -o "${trace_processor}"
    chmod +x "${trace_processor}"
  fi
  if [[ ! -x "${traceconv}" ]]; then
    log "downloading Perfetto traceconv wrapper"
    curl -fsSL https://get.perfetto.dev/traceconv -o "${traceconv}"
    chmod +x "${traceconv}"
  fi
}

print_status() {
  log "tool locations"
  printf '  %-22s %s\n' "perf" "$(command -v perf || echo missing)"
  printf '  %-22s %s\n' "valgrind" "$(command -v valgrind || echo missing)"
  printf '  %-22s %s\n' "callgrind_annotate" "$(command -v callgrind_annotate || echo missing)"
  printf '  %-22s %s\n' "flamegraph.pl" "$(command -v flamegraph.pl || echo missing)"
  printf '  %-22s %s\n' "stackcollapse-perf.pl" "$(command -v stackcollapse-perf.pl || echo missing)"
  printf '  %-22s %s\n' "heaptrack" "$(command -v heaptrack || echo missing)"
  printf '  %-22s %s\n' "heaptrack_print" "$(command -v heaptrack_print || echo missing)"
  printf '  %-22s %s\n' "heaptrack_gui" "$(command -v heaptrack_gui || echo missing)"
  printf '  %-22s %s\n' "hotspot" "$(command -v hotspot || echo missing)"
  printf '  %-22s %s\n' "trace_processor" "$(command -v trace_processor || echo missing)"
  printf '  %-22s %s\n' "traceconv" "$(command -v traceconv || echo missing)"
}

install_flamegraph
install_heaptrack
install_hotspot
install_perfetto_wrappers
print_status
