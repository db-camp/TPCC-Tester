#!/usr/bin/env python3
"""Collect non-ranked process diagnostics for the final2026 workflow."""

import argparse
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
    return parser


def main():
    arguments = build_parser().parse_args()
    try:
        if arguments.command == "capture":
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
        else:
            available = parse_strace_summary(arguments.input, arguments.output)
    except (MetricError, OSError, ValueError) as error:
        print(f"diagnostic_metrics: {error}", file=sys.stderr)
        return 2
    if getattr(arguments, "require_available", False) and not available:
        print("diagnostic_metrics: requested metrics are unavailable", file=sys.stderr)
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
