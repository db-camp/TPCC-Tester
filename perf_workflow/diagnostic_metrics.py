#!/usr/bin/env python3
"""Collect non-ranked process diagnostics for the final2026 workflow."""

import argparse
import ctypes
import json
import os
from pathlib import Path
import re
import sys
import tempfile
import time


SCHEMA_VERSION = 1
PROC_IO_FIELDS = (
    "rchar",
    "wchar",
    "syscr",
    "syscw",
    "read_bytes",
    "write_bytes",
    "cancelled_write_bytes",
)
PROC_STAT_FIELDS = {
    "minflt": 7,
    "cminflt": 8,
    "majflt": 9,
    "cmajflt": 10,
    "utime_ticks": 11,
    "stime_ticks": 12,
    "cutime_ticks": 13,
    "cstime_ticks": 14,
}
PROC_STAT_STARTTIME_INDEX = 19
PROC_STATUS_FIELDS = (
    "voluntary_ctxt_switches",
    "nonvoluntary_ctxt_switches",
)
SYSCALL_GROUPS = {
    "read": {
        "read",
        "pread64",
        "readv",
        "preadv",
        "preadv2",
    },
    "write": {
        "write",
        "pwrite64",
        "writev",
        "pwritev",
        "pwritev2",
    },
    "open_close": {
        "open",
        "openat",
        "openat2",
        "creat",
        "close",
        "close_range",
    },
    "truncate_allocate": {
        "truncate",
        "ftruncate",
        "fallocate",
    },
    "sync": {
        "fsync",
        "fdatasync",
        "sync",
        "syncfs",
        "sync_file_range",
        "msync",
    },
}
STRACE_ROW = re.compile(
    r"^\s*"
    r"(?P<percent>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<seconds>[0-9]+(?:\.[0-9]+)?)\s+"
    r"(?P<usecs>[0-9]+|\?)\s+"
    r"(?P<calls>[0-9]+)"
    r"(?:\s+(?P<errors>[0-9]+))?\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*)\s*$"
)
FS_USAGE_ROW = re.compile(
    r"^(?P<timestamp>[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+)\s+"
    r"(?P<name>[A-Za-z_][A-Za-z0-9_]*(?:\[[A-Z]\])?)"
    r"(?P<body>.*?)\s+"
    r"(?P<seconds>[0-9]+\.[0-9]+)"
    r"(?:\s+W)?\s+"
    r"(?P<process>.+?)\s*$"
)
FS_USAGE_BYTES = re.compile(r"(?:^|\s)B=0x(?P<bytes>[0-9A-Fa-f]+)(?:\s|$)")
FS_USAGE_ERRNO = re.compile(r"(?:^|\s)\[\s*(?P<errno>[0-9]+)\](?:\s|$)")
FS_USAGE_GROUPS = {
    "read": {"read", "pread", "readv", "preadv"},
    "write": {"write", "pwrite", "writev", "pwritev"},
    "open_close": {"open", "openat", "creat", "close"},
    "truncate_allocate": {"truncate", "ftruncate"},
    "sync": {"fsync", "msync", "sync"},
    "physical_read": {"RdData", "RdMeta", "PgIn"},
    "physical_write": {"WrData", "WrMeta", "PgOut"},
}


class DarwinRusageInfoV4(ctypes.Structure):
    _fields_ = [
        ("ri_uuid", ctypes.c_uint8 * 16),
        ("ri_user_time", ctypes.c_uint64),
        ("ri_system_time", ctypes.c_uint64),
        ("ri_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_interrupt_wkups", ctypes.c_uint64),
        ("ri_pageins", ctypes.c_uint64),
        ("ri_wired_size", ctypes.c_uint64),
        ("ri_resident_size", ctypes.c_uint64),
        ("ri_phys_footprint", ctypes.c_uint64),
        ("ri_proc_start_abstime", ctypes.c_uint64),
        ("ri_proc_exit_abstime", ctypes.c_uint64),
        ("ri_child_user_time", ctypes.c_uint64),
        ("ri_child_system_time", ctypes.c_uint64),
        ("ri_child_pkg_idle_wkups", ctypes.c_uint64),
        ("ri_child_interrupt_wkups", ctypes.c_uint64),
        ("ri_child_pageins", ctypes.c_uint64),
        ("ri_child_elapsed_abstime", ctypes.c_uint64),
        ("ri_diskio_bytesread", ctypes.c_uint64),
        ("ri_diskio_byteswritten", ctypes.c_uint64),
        ("ri_cpu_time_qos_default", ctypes.c_uint64),
        ("ri_cpu_time_qos_maintenance", ctypes.c_uint64),
        ("ri_cpu_time_qos_background", ctypes.c_uint64),
        ("ri_cpu_time_qos_utility", ctypes.c_uint64),
        ("ri_cpu_time_qos_legacy", ctypes.c_uint64),
        ("ri_cpu_time_qos_user_initiated", ctypes.c_uint64),
        ("ri_cpu_time_qos_user_interactive", ctypes.c_uint64),
        ("ri_billed_system_time", ctypes.c_uint64),
        ("ri_serviced_system_time", ctypes.c_uint64),
        ("ri_logical_writes", ctypes.c_uint64),
        ("ri_lifetime_max_phys_footprint", ctypes.c_uint64),
        ("ri_instructions", ctypes.c_uint64),
        ("ri_cycles", ctypes.c_uint64),
        ("ri_billed_energy", ctypes.c_uint64),
        ("ri_serviced_energy", ctypes.c_uint64),
        ("ri_interval_max_phys_footprint", ctypes.c_uint64),
        ("ri_runnable_time", ctypes.c_uint64),
    ]


class MetricError(Exception):
    """A diagnostic artifact could not be parsed safely."""


def atomic_write_json(path, payload):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def parse_key_value_file(path, wanted):
    values = {}
    with path.open("r", encoding="utf-8") as stream:
        for raw_line in stream:
            key, separator, raw_value = raw_line.partition(":")
            if not separator or key not in wanted:
                continue
            token = raw_value.strip().split(maxsplit=1)[0]
            values[key] = int(token)
    missing = sorted(set(wanted) - set(values))
    if missing:
        raise MetricError(f"{path} is missing counters: {', '.join(missing)}")
    return values


def parse_proc_stat(path, expected_pid):
    text = path.read_text(encoding="utf-8").strip()
    closing_parenthesis = text.rfind(")")
    opening_parenthesis = text.find("(")
    if opening_parenthesis <= 0 or closing_parenthesis <= opening_parenthesis:
        raise MetricError(f"{path} has an invalid stat record")
    try:
        actual_pid = int(text[:opening_parenthesis].strip())
    except ValueError as error:
        raise MetricError(f"{path} has an invalid pid") from error
    if actual_pid != expected_pid:
        raise MetricError(
            f"{path} belongs to pid {actual_pid}, expected {expected_pid}"
        )
    fields = text[closing_parenthesis + 1 :].split()
    if len(fields) <= max(
        max(PROC_STAT_FIELDS.values()),
        PROC_STAT_STARTTIME_INDEX,
    ):
        raise MetricError(f"{path} has a truncated stat record")
    values = {}
    for name, index in PROC_STAT_FIELDS.items():
        try:
            values[name] = int(fields[index])
        except ValueError as error:
            raise MetricError(f"{path} has a non-integer {name}") from error
    try:
        starttime_ticks = int(fields[PROC_STAT_STARTTIME_INDEX])
    except ValueError as error:
        raise MetricError(f"{path} has a non-integer starttime") from error
    return values, starttime_ticks


def capture_proc_snapshot(pid, output, proc_root):
    captured_at_ns = time.time_ns()
    process_root = Path(proc_root) / str(pid)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "proc_snapshot",
        "status": "unavailable",
        "pid": pid,
        "captured_at_unix_ns": captured_at_ns,
        "source": str(process_root),
    }
    if not Path(proc_root).is_dir():
        payload["reason"] = f"procfs is unavailable at {proc_root}"
        atomic_write_json(output, payload)
        return False

    try:
        io_values = parse_key_value_file(
            process_root / "io",
            PROC_IO_FIELDS,
        )
        stat_values, starttime_ticks = parse_proc_stat(
            process_root / "stat",
            pid,
        )
        status_values = parse_key_value_file(
            process_root / "status",
            PROC_STATUS_FIELDS,
        )
    except (OSError, MetricError, ValueError) as error:
        payload["reason"] = str(error)
        atomic_write_json(output, payload)
        return False

    payload.update(
        {
            "status": "available",
            "metrics": {
                "io": io_values,
                "stat": stat_values,
                "status": status_values,
            },
            "identity": {
                "starttime_ticks": starttime_ticks,
            },
        }
    )
    atomic_write_json(output, payload)
    return True


def capture_darwin_snapshot(pid, output):
    captured_at_ns = time.time_ns()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "proc_snapshot",
        "status": "unavailable",
        "pid": pid,
        "captured_at_unix_ns": captured_at_ns,
        "source": f"libproc:{pid}",
        "backend": "darwin_libproc_rusage_v4",
    }
    if sys.platform != "darwin":
        payload["reason"] = "Darwin libproc is unavailable on this platform"
        atomic_write_json(output, payload)
        return False
    try:
        libproc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError as error:
        payload["reason"] = f"cannot load macOS libproc: {error}"
        atomic_write_json(output, payload)
        return False
    libproc.proc_pid_rusage.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
    ]
    libproc.proc_pid_rusage.restype = ctypes.c_int
    usage = DarwinRusageInfoV4()
    if libproc.proc_pid_rusage(pid, 4, ctypes.byref(usage)) != 0:
        error_number = ctypes.get_errno()
        payload["reason"] = (
            f"proc_pid_rusage failed for pid {pid}"
            + (f": errno {error_number}" if error_number else "")
        )
        atomic_write_json(output, payload)
        return False
    payload.update(
        {
            "status": "available",
            "metrics": {
                "io": {
                    "diskio_bytesread": usage.ri_diskio_bytesread,
                    "diskio_byteswritten": usage.ri_diskio_byteswritten,
                    "logical_writes": usage.ri_logical_writes,
                },
                "stat": {
                    "pageins": usage.ri_pageins,
                    "child_pageins": usage.ri_child_pageins,
                    "user_time_ns": usage.ri_user_time,
                    "system_time_ns": usage.ri_system_time,
                    "child_user_time_ns": usage.ri_child_user_time,
                    "child_system_time_ns": usage.ri_child_system_time,
                    "instructions": usage.ri_instructions,
                    "cycles": usage.ri_cycles,
                },
                "status": {
                    "pkg_idle_wakeups": usage.ri_pkg_idle_wkups,
                    "interrupt_wakeups": usage.ri_interrupt_wkups,
                    "child_pkg_idle_wakeups": usage.ri_child_pkg_idle_wkups,
                    "child_interrupt_wakeups": usage.ri_child_interrupt_wkups,
                },
            },
            "identity": {
                "start_abstime": usage.ri_proc_start_abstime,
            },
            "instantaneous": {
                "resident_bytes": usage.ri_resident_size,
                "physical_footprint_bytes": usage.ri_phys_footprint,
                "lifetime_max_physical_footprint_bytes": (
                    usage.ri_lifetime_max_phys_footprint
                ),
            },
        }
    )
    atomic_write_json(output, payload)
    return True


def load_json(path):
    try:
        with Path(path).open("r", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        raise MetricError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise MetricError(f"{path} does not contain a JSON object")
    return value


def calculate_proc_delta(before_path, after_path, output):
    before = load_json(before_path)
    after = load_json(after_path)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "proc_delta",
        "status": "unavailable",
        "before": str(Path(before_path)),
        "after": str(Path(after_path)),
    }
    if before.get("status") != "available" or after.get("status") != "available":
        payload["reason"] = "one or both proc snapshots are unavailable"
        payload["snapshot_status"] = {
            "before": before.get("status", "invalid"),
            "after": after.get("status", "invalid"),
        }
        atomic_write_json(output, payload)
        return False
    if before.get("pid") != after.get("pid"):
        raise MetricError("proc snapshots belong to different pids")
    before_identity = before.get("identity")
    after_identity = after.get("identity")
    if not isinstance(before_identity, dict) or not isinstance(
        after_identity,
        dict,
    ):
        raise MetricError("proc snapshot process identity is missing")
    if before_identity.get("starttime_ticks") != after_identity.get(
        "starttime_ticks"
    ):
        raise MetricError("proc snapshots belong to different process instances")

    before_metrics = before.get("metrics")
    after_metrics = after.get("metrics")
    if not isinstance(before_metrics, dict) or not isinstance(after_metrics, dict):
        raise MetricError("proc snapshot metrics are missing")

    deltas = {}
    decreased = []
    for section in ("io", "stat", "status"):
        before_section = before_metrics.get(section)
        after_section = after_metrics.get(section)
        if not isinstance(before_section, dict) or not isinstance(after_section, dict):
            raise MetricError(f"proc snapshot section {section} is missing")
        if set(before_section) != set(after_section):
            raise MetricError(f"proc snapshot section {section} changed shape")
        section_delta = {}
        for name, before_value in before_section.items():
            after_value = after_section[name]
            if not isinstance(before_value, int) or not isinstance(after_value, int):
                raise MetricError(f"proc counter {section}.{name} is not an integer")
            raw_delta = after_value - before_value
            if raw_delta < 0:
                decreased.append(f"{section}.{name}")
            section_delta[name] = max(0, raw_delta)
        deltas[section] = section_delta

    before_time = before.get("captured_at_unix_ns")
    after_time = after.get("captured_at_unix_ns")
    if not isinstance(before_time, int) or not isinstance(after_time, int):
        raise MetricError("proc snapshot timestamps are invalid")
    payload.update(
        {
            "status": "available",
            "pid": before["pid"],
            "elapsed_ns": max(0, after_time - before_time),
            "metrics": deltas,
            "decreased_counters": decreased,
        }
    )
    atomic_write_json(output, payload)
    return True


def parse_strace_summary(input_path, output):
    path = Path(input_path)
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as error:
        raise MetricError(f"cannot read {path}: {error}") from error
    if not text.strip():
        raise MetricError(f"{path} is empty")

    rows = []
    total = None
    saw_header = False
    saw_separator = False
    for line in text.splitlines():
        stripped = line.strip()
        if "% time" in stripped and "calls" in stripped and "syscall" in stripped:
            saw_header = True
            continue
        if stripped.startswith("------"):
            saw_separator = True
            continue
        tokens = stripped.split()
        if tokens and tokens[-1] == "total":
            if len(tokens) not in (4, 5):
                raise MetricError(f"{path} has an invalid strace total row")
            try:
                total = {
                    "percent_time": float(tokens[0]),
                    "seconds": float(tokens[1]),
                    "calls": int(tokens[2]),
                    "errors": int(tokens[3]) if len(tokens) == 5 else 0,
                }
            except ValueError as error:
                raise MetricError(
                    f"{path} has a non-numeric strace total row"
                ) from error
            continue
        match = STRACE_ROW.match(line)
        if match is None:
            continue
        row = {
            "name": match.group("name"),
            "percent_time": float(match.group("percent")),
            "seconds": float(match.group("seconds")),
            "microseconds_per_call": (
                None
                if match.group("usecs") == "?"
                else int(match.group("usecs"))
            ),
            "calls": int(match.group("calls")),
            "errors": int(match.group("errors") or 0),
        }
        rows.append(row)
    if not saw_header or not saw_separator or total is None:
        raise MetricError(f"{path} has an incomplete strace -c summary")
    if not rows:
        raise MetricError(f"{path} contains no parseable strace -c syscall rows")
    parsed_calls = sum(row["calls"] for row in rows)
    parsed_errors = sum(row["errors"] for row in rows)
    if parsed_calls != total["calls"] or parsed_errors != total["errors"]:
        raise MetricError(
            f"{path} strace total does not match the parsed syscall rows"
        )

    derived = {}
    for group_name, syscall_names in SYSCALL_GROUPS.items():
        matched = [row for row in rows if row["name"] in syscall_names]
        derived[group_name] = {
            "matched_syscalls": sorted(row["name"] for row in matched),
            "calls": sum(row["calls"] for row in matched),
            "errors": sum(row["errors"] for row in matched),
            "seconds": round(sum(row["seconds"] for row in matched), 9),
            "percent_time": round(
                sum(row["percent_time"] for row in matched),
                6,
            ),
        }

    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "strace_summary",
        "status": "available",
        "source": str(path),
        "syscalls": rows,
        "totals": total,
        "derived": derived,
    }
    atomic_write_json(output, payload)
    return True


def parse_fs_usage(input_path, output):
    path = Path(input_path)
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as error:
        raise MetricError(f"cannot read {path}: {error}") from error
    rows_by_name = {}
    for line in text.splitlines():
        match = FS_USAGE_ROW.match(line)
        if match is None:
            continue
        raw_name = match.group("name")
        name = raw_name.split("[", 1)[0]
        body = match.group("body")
        bytes_match = FS_USAGE_BYTES.search(body)
        errno_match = FS_USAGE_ERRNO.search(body)
        row = rows_by_name.setdefault(
            name,
            {
                "name": name,
                "calls": 0,
                "errors": 0,
                "seconds": 0.0,
                "bytes": 0,
            },
        )
        row["calls"] += 1
        row["errors"] += int(errno_match is not None)
        row["seconds"] += float(match.group("seconds"))
        if bytes_match is not None:
            row["bytes"] += int(bytes_match.group("bytes"), 16)
    if not rows_by_name:
        raise MetricError(f"{path} contains no parseable fs_usage rows")
    rows = sorted(rows_by_name.values(), key=lambda row: row["name"])
    total_seconds = sum(row["seconds"] for row in rows)
    for row in rows:
        row["seconds"] = round(row["seconds"], 9)
        row["microseconds_per_call"] = round(
            row["seconds"] * 1_000_000 / row["calls"],
            3,
        )
        row["percent_time"] = round(
            100.0 * row["seconds"] / total_seconds if total_seconds else 0.0,
            6,
        )
    derived = {}
    for group_name, event_names in FS_USAGE_GROUPS.items():
        matched = [row for row in rows if row["name"] in event_names]
        derived[group_name] = {
            "matched_events": sorted(row["name"] for row in matched),
            "calls": sum(row["calls"] for row in matched),
            "errors": sum(row["errors"] for row in matched),
            "seconds": round(sum(row["seconds"] for row in matched), 9),
            "bytes": sum(row["bytes"] for row in matched),
            "percent_time": round(
                sum(row["percent_time"] for row in matched),
                6,
            ),
        }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "fs_usage_summary",
        "status": "available",
        "source": str(path),
        "backend": "darwin_fs_usage",
        "official_strace_equivalent": False,
        "events": rows,
        "totals": {
            "calls": sum(row["calls"] for row in rows),
            "errors": sum(row["errors"] for row in rows),
            "seconds": round(total_seconds, 9),
            "bytes": sum(row["bytes"] for row in rows),
        },
        "derived": derived,
    }
    atomic_write_json(output, payload)
    return True


def positive_pid(value):
    try:
        pid = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("pid must be an integer") from error
    if pid <= 0:
        raise argparse.ArgumentTypeError("pid must be positive")
    return pid


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture = subparsers.add_parser("capture", help="capture /proc counters")
    capture.add_argument("--pid", required=True, type=positive_pid)
    capture.add_argument("--output", required=True, type=Path)
    capture.add_argument("--proc-root", default=Path("/proc"), type=Path)
    capture.add_argument("--require-available", action="store_true")

    delta = subparsers.add_parser("delta", help="calculate snapshot deltas")
    delta.add_argument("--before", required=True, type=Path)
    delta.add_argument("--after", required=True, type=Path)
    delta.add_argument("--output", required=True, type=Path)
    delta.add_argument("--require-available", action="store_true")

    strace = subparsers.add_parser("strace", help="parse a strace -c summary")
    strace.add_argument("--input", required=True, type=Path)
    strace.add_argument("--output", required=True, type=Path)
    fs_usage = subparsers.add_parser(
        "fs-usage",
        help="parse a macOS fs_usage event stream",
    )
    fs_usage.add_argument("--input", required=True, type=Path)
    fs_usage.add_argument("--output", required=True, type=Path)
    return parser


def main():
    arguments = build_parser().parse_args()
    try:
        if arguments.command == "capture":
            if arguments.proc_root.is_dir() or sys.platform != "darwin":
                available = capture_proc_snapshot(
                    arguments.pid,
                    arguments.output,
                    arguments.proc_root,
                )
            elif arguments.proc_root == Path("/proc"):
                available = capture_darwin_snapshot(
                    arguments.pid,
                    arguments.output,
                )
            else:
                available = capture_proc_snapshot(
                    arguments.pid,
                    arguments.output,
                    arguments.proc_root,
                )
        elif arguments.command == "delta":
            available = calculate_proc_delta(
                arguments.before,
                arguments.after,
                arguments.output,
            )
        elif arguments.command == "strace":
            available = parse_strace_summary(arguments.input, arguments.output)
        else:
            available = parse_fs_usage(arguments.input, arguments.output)
    except (MetricError, OSError, ValueError) as error:
        print(f"diagnostic_metrics: {error}", file=sys.stderr)
        return 2
    if getattr(arguments, "require_available", False) and not available:
        print("diagnostic_metrics: requested metrics are unavailable", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
