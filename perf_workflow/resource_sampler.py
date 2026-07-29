#!/usr/bin/env python3
"""Collect observation-only RMDB resource metrics for the final2026 workflow."""

import argparse
import ctypes
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time


SCHEMA_VERSION = 1
DEFAULT_INTERVAL_MS = 1000
TIMELINE_KEYS = {
    "schema_version",
    "kind",
    "origin_unix_ns",
    "warmup_ns",
    "measurement_windows",
    "measurement_window_ns",
}


class ResourceError(Exception):
    """A resource sample or artifact could not be validated."""


class RootAbsent(ResourceError):
    """The registered root process is no longer present."""


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


def load_json(path):
    try:
        with Path(path).open("r", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as error:
        raise ResourceError(f"cannot read {path}: {error}") from error
    if not isinstance(value, dict):
        raise ResourceError(f"{path} does not contain a JSON object")
    return value


def online_cpu_count():
    try:
        affinity = os.sched_getaffinity(0)
    except (AttributeError, OSError):
        affinity = None
    count = len(affinity) if affinity else os.cpu_count()
    if not isinstance(count, int) or count <= 0:
        raise ResourceError("the online logical CPU count is unavailable")
    return count


def descendants(table, root_pid):
    owned = {root_pid}
    changed = True
    while changed:
        changed = False
        for pid, item in table.items():
            if pid not in owned and item["ppid"] in owned:
                owned.add(pid)
                changed = True
    return owned


def parse_linux_stat(text, expected_pid, clock_ticks, page_size):
    closing = text.rfind(")")
    opening = text.find("(")
    if opening <= 0 or closing <= opening:
        raise ResourceError("invalid Linux proc stat record")
    try:
        pid = int(text[:opening].strip())
    except ValueError as error:
        raise ResourceError("invalid Linux proc stat pid") from error
    if pid != expected_pid:
        raise ResourceError(f"proc stat belongs to pid {pid}, expected {expected_pid}")
    fields = text[closing + 1 :].split()
    if len(fields) <= 21:
        raise ResourceError("truncated Linux proc stat record")
    try:
        self_and_waited_ticks = sum(int(fields[index]) for index in (11, 12, 13, 14))
        return {
            "pid": pid,
            "state": fields[0],
            "ppid": int(fields[1]),
            "pgid": int(fields[2]),
            "identity": f"linux:{fields[19]}",
            "cpu_ns": (self_and_waited_ticks * 1_000_000_000) // clock_ticks,
            "rss_bytes": max(0, int(fields[21])) * page_size,
        }
    except ValueError as error:
        raise ResourceError("non-integer Linux proc stat field") from error


class LinuxProcBackend:
    name = "linux_proc"

    def __init__(self, proc_root):
        self.proc_root = Path(proc_root)
        try:
            self.clock_ticks = int(os.sysconf("SC_CLK_TCK"))
            self.page_size = int(os.sysconf("SC_PAGE_SIZE"))
        except (OSError, ValueError) as error:
            raise ResourceError(f"cannot read Linux clock/page units: {error}") from error
        if self.clock_ticks <= 0 or self.page_size <= 0:
            raise ResourceError("Linux clock/page units must be positive")
        try:
            self.boot_id = (
                self.proc_root / "sys/kernel/random/boot_id"
            ).read_text(encoding="ascii").strip()
        except OSError:
            self.boot_id = "unavailable"

    def process_table(self):
        table = {}
        try:
            entries = list(self.proc_root.iterdir())
        except OSError as error:
            raise ResourceError(f"cannot scan {self.proc_root}: {error}") from error
        for entry in entries:
            if not entry.name.isdigit():
                continue
            pid = int(entry.name)
            try:
                text = (entry / "stat").read_text(encoding="ascii")
                item = parse_linux_stat(
                    text,
                    pid,
                    self.clock_ticks,
                    self.page_size,
                )
            except (OSError, ResourceError):
                continue
            table[pid] = item
        return table

    def metadata(self):
        return {
            "backend": self.name,
            "clock_ticks_per_second": self.clock_ticks,
            "page_size_bytes": self.page_size,
            "boot_id": self.boot_id,
        }


class ProcBsdInfo(ctypes.Structure):
    _fields_ = [
        ("pbi_flags", ctypes.c_uint32),
        ("pbi_status", ctypes.c_uint32),
        ("pbi_xstatus", ctypes.c_uint32),
        ("pbi_pid", ctypes.c_uint32),
        ("pbi_ppid", ctypes.c_uint32),
        ("pbi_uid", ctypes.c_uint32),
        ("pbi_gid", ctypes.c_uint32),
        ("pbi_ruid", ctypes.c_uint32),
        ("pbi_rgid", ctypes.c_uint32),
        ("pbi_svuid", ctypes.c_uint32),
        ("pbi_svgid", ctypes.c_uint32),
        ("rfu_1", ctypes.c_uint32),
        ("pbi_comm", ctypes.c_char * 16),
        ("pbi_name", ctypes.c_char * 32),
        ("pbi_nfiles", ctypes.c_uint32),
        ("pbi_pgid", ctypes.c_uint32),
        ("pbi_pjobc", ctypes.c_uint32),
        ("e_tdev", ctypes.c_uint32),
        ("e_tpgid", ctypes.c_uint32),
        ("pbi_nice", ctypes.c_int32),
        ("pbi_start_tvsec", ctypes.c_uint64),
        ("pbi_start_tvusec", ctypes.c_uint64),
    ]


class DarwinLibprocBackend:
    name = "darwin_libproc"
    PROC_PIDTBSDINFO = 3
    RUSAGE_INFO_V4 = 4
    RUSAGE_BUFFER_BYTES = 512

    def __init__(self):
        try:
            self.libproc = ctypes.CDLL("/usr/lib/libproc.dylib")
        except OSError as error:
            raise ResourceError(f"cannot load macOS libproc: {error}") from error
        self.libproc.proc_listallpids.argtypes = [ctypes.c_void_p, ctypes.c_int]
        self.libproc.proc_listallpids.restype = ctypes.c_int
        self.libproc.proc_pidinfo.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_uint64,
            ctypes.c_void_p,
            ctypes.c_int,
        ]
        self.libproc.proc_pidinfo.restype = ctypes.c_int
        self.libproc.proc_pid_rusage.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_void_p,
        ]
        self.libproc.proc_pid_rusage.restype = ctypes.c_int

    def all_pids(self):
        count = self.libproc.proc_listallpids(None, 0)
        if count <= 0:
            raise ResourceError("macOS libproc returned no process capacity")
        buffer = (ctypes.c_int * (count + 128))()
        returned = self.libproc.proc_listallpids(buffer, ctypes.sizeof(buffer))
        if returned <= 0:
            raise ResourceError("macOS libproc could not list processes")
        return [pid for pid in buffer[:returned] if pid > 0]

    def process_table(self):
        table = {}
        for pid in self.all_pids():
            bsd = ProcBsdInfo()
            received = self.libproc.proc_pidinfo(
                pid,
                self.PROC_PIDTBSDINFO,
                0,
                ctypes.byref(bsd),
                ctypes.sizeof(bsd),
            )
            if received != ctypes.sizeof(bsd):
                continue
            rusage = ctypes.create_string_buffer(self.RUSAGE_BUFFER_BYTES)
            if (
                self.libproc.proc_pid_rusage(
                    pid,
                    self.RUSAGE_INFO_V4,
                    ctypes.byref(rusage),
                )
                != 0
            ):
                continue
            user_ns = ctypes.c_uint64.from_buffer(rusage, 16).value
            system_ns = ctypes.c_uint64.from_buffer(rusage, 24).value
            resident_bytes = ctypes.c_uint64.from_buffer(rusage, 64).value
            start_abstime = ctypes.c_uint64.from_buffer(rusage, 80).value
            child_user_ns = ctypes.c_uint64.from_buffer(rusage, 96).value
            child_system_ns = ctypes.c_uint64.from_buffer(rusage, 104).value
            table[pid] = {
                "pid": pid,
                "state": str(bsd.pbi_status),
                "ppid": int(bsd.pbi_ppid),
                "pgid": int(bsd.pbi_pgid),
                "identity": f"darwin:{start_abstime}",
                "cpu_ns": user_ns + system_ns + child_user_ns + child_system_ns,
                "rss_bytes": resident_bytes,
            }
        return table

    def metadata(self):
        return {
            "backend": self.name,
            "cpu_unit": "nanoseconds",
            "rss_unit": "bytes",
        }


def select_backend(proc_root):
    proc_root = Path(proc_root)
    if (proc_root / "self/stat").is_file():
        return LinuxProcBackend(proc_root)
    if sys.platform == "darwin":
        return DarwinLibprocBackend()
    raise ResourceError("neither Linux procfs nor macOS libproc is available")


def collect_tree_sample(backend, root_pid, expected_pgid, root_identity):
    table = backend.process_table()
    root = table.get(root_pid)
    if root is None:
        raise RootAbsent(f"registered RMDB pid {root_pid} is absent")
    if root_identity is not None and root["identity"] != root_identity:
        raise ResourceError(
            f"registered RMDB pid {root_pid} identity changed from "
            f"{root_identity} to {root['identity']}"
        )
    if root["pgid"] != expected_pgid:
        raise ResourceError(
            f"registered RMDB pid {root_pid} moved from process group "
            f"{expected_pgid} to {root['pgid']}"
        )
    owned = descendants(table, root_pid)
    group_members = {
        pid
        for pid, item in table.items()
        if item["pgid"] == expected_pgid
    }
    if not group_members.issubset(owned):
        raise ResourceError("RMDB process group contains a process outside the root tree")
    live_owned = [table[pid] for pid in owned if pid in table]
    if any(item["pgid"] != expected_pgid for item in live_owned):
        raise ResourceError("an RMDB descendant escaped the registered process group")
    return {
        "identity": root["identity"],
        "process_count": len(live_owned),
        "cpu_ns": sum(item["cpu_ns"] for item in live_owned),
        "rss_bytes": sum(item["rss_bytes"] for item in live_owned),
    }


def directory_usage(path):
    root = Path(path)
    try:
        root_stat = root.lstat()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ResourceError(f"cannot stat database directory {root}: {error}") from error
    if root.is_symlink() or not root.is_dir():
        raise ResourceError(f"database path is not a real directory: {root}")

    allocated = 0
    apparent = 0
    seen = set()
    stack = [root]
    while stack:
        current = stack.pop()
        try:
            item_stat = current.lstat()
        except OSError as error:
            raise ResourceError(f"cannot stat database entry {current}: {error}") from error
        identity = (item_stat.st_dev, item_stat.st_ino)
        if identity in seen:
            continue
        seen.add(identity)
        allocated += max(0, item_stat.st_blocks) * 512
        apparent += max(0, item_stat.st_size)
        if not current.is_dir() or current.is_symlink():
            continue
        try:
            with os.scandir(current) as entries:
                stack.extend(Path(entry.path) for entry in entries)
        except OSError as error:
            raise ResourceError(f"cannot scan database directory {current}: {error}") from error
    return {
        "allocated_bytes": allocated,
        "apparent_bytes": apparent,
        "inode_count": len(seen),
        "root_identity": {
            "device": root_stat.st_dev,
            "inode": root_stat.st_ino,
        },
    }


def append_warning(warnings, code, detail):
    item = {"code": code, "detail": str(detail)}
    if item not in warnings:
        warnings.append(item)


def sample_segment(arguments):
    output = Path(arguments.output)
    started_unix_ns = time.time_ns()
    started_monotonic_ns = time.monotonic_ns()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "rmdb_resource_segment",
        "status": "collecting",
        "ranked": False,
        "score_effect": "none",
        "root_pid": arguments.root_pid,
        "process_group": arguments.process_group,
        "database_path": str(Path(arguments.database_path)),
        "sample_interval_ms": arguments.interval_ms,
        "started_unix_ns": started_unix_ns,
        "started_monotonic_ns": started_monotonic_ns,
        "warnings": [],
    }
    atomic_write_json(output, payload)

    try:
        backend = select_backend(arguments.proc_root)
        logical_cpus = online_cpu_count()
    except ResourceError as error:
        payload.update(
            {
                "status": "unavailable",
                "reason": str(error),
                "completed_unix_ns": time.time_ns(),
            }
        )
        atomic_write_json(output, payload)
        return 0

    stop_event = threading.Event()

    def request_stop(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    expected_identity = None
    previous = None
    intervals = []
    correlations = []
    process_samples = 0
    disk_samples = 0
    missed_deadlines = 0
    max_rss_bytes = 0
    max_disk_allocated = 0
    max_disk_apparent = 0
    final_disk = None
    root_observed_exit = False
    interval_ns = arguments.interval_ms * 1_000_000
    deadline = time.monotonic_ns()

    while True:
        monotonic_before = time.monotonic_ns()
        unix_ns = time.time_ns()
        monotonic_after = time.monotonic_ns()
        monotonic_ns = (monotonic_before + monotonic_after) // 2
        try:
            tree = collect_tree_sample(
                backend,
                arguments.root_pid,
                arguments.process_group,
                expected_identity,
            )
            if expected_identity is None:
                expected_identity = tree["identity"]
            process_samples += 1
            max_rss_bytes = max(max_rss_bytes, tree["rss_bytes"])
            correlations.append(
                {
                    "monotonic_ns": monotonic_ns,
                    "unix_ns": unix_ns,
                }
            )
            if previous is not None:
                elapsed_ns = monotonic_ns - previous["monotonic_ns"]
                cpu_delta_ns = tree["cpu_ns"] - previous["cpu_ns"]
                if elapsed_ns <= 0:
                    append_warning(
                        payload["warnings"],
                        "nonpositive_sample_interval",
                        elapsed_ns,
                    )
                elif cpu_delta_ns < 0:
                    append_warning(
                        payload["warnings"],
                        "process_tree_cpu_regressed",
                        cpu_delta_ns,
                    )
                else:
                    intervals.append(
                        {
                            "start_monotonic_ns": previous["monotonic_ns"],
                            "end_monotonic_ns": monotonic_ns,
                            "cpu_delta_ns": cpu_delta_ns,
                        }
                    )
            previous = {
                "monotonic_ns": monotonic_ns,
                "cpu_ns": tree["cpu_ns"],
            }
        except RootAbsent:
            if expected_identity is not None:
                root_observed_exit = True
        except ResourceError as error:
            append_warning(payload["warnings"], "process_sample_failed", error)

        try:
            disk = directory_usage(arguments.database_path)
            if disk is not None:
                disk_samples += 1
                final_disk = disk
                max_disk_allocated = max(
                    max_disk_allocated,
                    disk["allocated_bytes"],
                )
                max_disk_apparent = max(
                    max_disk_apparent,
                    disk["apparent_bytes"],
                )
        except ResourceError as error:
            append_warning(payload["warnings"], "disk_sample_failed", error)

        if stop_event.is_set():
            break
        deadline += interval_ns
        now = time.monotonic_ns()
        if now > deadline:
            missed = ((now - deadline) // interval_ns) + 1
            missed_deadlines += missed
            deadline += missed * interval_ns
        stop_event.wait(max(0.0, (deadline - time.monotonic_ns()) / 1_000_000_000))

    if missed_deadlines:
        append_warning(
            payload["warnings"],
            "sample_deadlines_missed",
            missed_deadlines,
        )
    if process_samples == 0:
        status = "unavailable"
    elif disk_samples == 0 or payload["warnings"]:
        status = "partial"
    else:
        status = "available"
    payload.update(
        {
            "status": status,
            "backend": backend.metadata(),
            "logical_cpus": logical_cpus,
            "root_identity": expected_identity,
            "root_observed_exit": root_observed_exit,
            "process_samples": process_samples,
            "disk_samples": disk_samples,
            "missed_deadlines": missed_deadlines,
            "max_rss_bytes": max_rss_bytes,
            "max_disk_allocated_bytes": max_disk_allocated,
            "max_disk_apparent_bytes": max_disk_apparent,
            "final_disk": final_disk,
            "cpu_intervals": intervals,
            "clock_correlations": correlations,
            "completed_unix_ns": time.time_ns(),
            "completed_monotonic_ns": time.monotonic_ns(),
        }
    )
    atomic_write_json(output, payload)
    return 0


def parse_timeline(path):
    try:
        text = Path(path).read_text(encoding="ascii")
    except OSError as error:
        raise ResourceError(f"cannot read rank timeline {path}: {error}") from error
    values = {}
    for raw_line in text.splitlines():
        key, separator, value = raw_line.partition("=")
        if not separator or key in values:
            raise ResourceError("rank timeline has malformed or duplicate fields")
        values[key] = value
    if set(values) != TIMELINE_KEYS:
        raise ResourceError("rank timeline fields do not match schema version 1")
    if values["schema_version"] != "1":
        raise ResourceError("unsupported rank timeline schema")
    if values["kind"] != "final2026_rank_timeline":
        raise ResourceError("rank timeline kind is invalid")
    try:
        parsed = {
            "origin_unix_ns": int(values["origin_unix_ns"]),
            "warmup_ns": int(values["warmup_ns"]),
            "measurement_windows": int(values["measurement_windows"]),
            "measurement_window_ns": int(values["measurement_window_ns"]),
        }
    except ValueError as error:
        raise ResourceError("rank timeline contains a non-integer duration") from error
    if (
        parsed["origin_unix_ns"] <= 0
        or parsed["warmup_ns"] < 0
        or parsed["measurement_windows"] != 3
        or parsed["measurement_window_ns"] <= 0
    ):
        raise ResourceError("rank timeline values are outside the final2026 contract")
    return parsed


def overlap(left_start, left_end, right_start, right_end):
    return max(0, min(left_end, right_end) - max(left_start, right_start))


def calculate_rank_cpu(segments, timeline, rank_complete):
    if not rank_complete:
        return {
            "status": "unavailable",
            "reason": "rank completion marker is missing",
        }
    correlations = [
        correlation
        for segment in segments
        for correlation in segment.get("clock_correlations", [])
        if isinstance(correlation, dict)
    ]
    if not correlations:
        return {
            "status": "unavailable",
            "reason": "no process sample can correlate the rank timeline",
        }
    correlation = min(
        correlations,
        key=lambda item: abs(item["unix_ns"] - timeline["origin_unix_ns"]),
    )
    origin_monotonic_ns = (
        correlation["monotonic_ns"]
        + timeline["origin_unix_ns"]
        - correlation["unix_ns"]
    )
    formal_start = origin_monotonic_ns + timeline["warmup_ns"]
    window_ns = timeline["measurement_window_ns"]
    formal_end = formal_start + timeline["measurement_windows"] * window_ns
    intervals = sorted(
        (
            interval
            for segment in segments
            for interval in segment.get("cpu_intervals", [])
            if isinstance(interval, dict)
        ),
        key=lambda item: item["start_monotonic_ns"],
    )
    logical_counts = {
        segment.get("logical_cpus")
        for segment in segments
        if isinstance(segment.get("logical_cpus"), int)
        and segment["logical_cpus"] > 0
    }
    if len(logical_counts) != 1:
        return {
            "status": "unavailable",
            "reason": "online logical CPU count changed or is missing",
        }
    logical_cpus = logical_counts.pop()

    windows = []
    complete = True
    combined_cpu_ns = 0.0
    combined_coverage_ns = 0
    combined_peak = 0.0
    for index in range(timeline["measurement_windows"]):
        start = formal_start + index * window_ns
        end = start + window_ns
        cpu_ns = 0.0
        coverage_ns = 0
        peak = 0.0
        interval_count = 0
        for interval in intervals:
            interval_start = interval["start_monotonic_ns"]
            interval_end = interval["end_monotonic_ns"]
            elapsed = interval_end - interval_start
            shared = overlap(interval_start, interval_end, start, end)
            if shared <= 0 or elapsed <= 0:
                continue
            delta = interval["cpu_delta_ns"]
            cpu_ns += delta * (shared / elapsed)
            coverage_ns += shared
            interval_count += 1
            peak = max(peak, 100.0 * delta / elapsed)
        coverage_ratio = min(1.0, coverage_ns / window_ns)
        window_complete = coverage_ratio >= 0.999
        complete = complete and window_complete
        average = 100.0 * cpu_ns / window_ns if window_ns else 0.0
        windows.append(
            {
                "window": index + 1,
                "start_monotonic_ns": start,
                "end_monotonic_ns": end,
                "coverage_ratio": round(coverage_ratio, 9),
                "intervals": interval_count,
                "average_single_core_percent": round(average, 6),
                "peak_single_core_percent": round(peak, 6),
                "average_host_percent": round(average / logical_cpus, 6),
                "peak_host_percent": round(peak / logical_cpus, 6),
            }
        )
        combined_cpu_ns += cpu_ns
        combined_coverage_ns += coverage_ns
        combined_peak = max(combined_peak, peak)
    formal_duration = formal_end - formal_start
    combined_average = 100.0 * combined_cpu_ns / formal_duration
    status = "available" if complete else "partial"
    return {
        "status": status,
        "clock_basis": "scheduler_barrier_correlated_to_monotonic_samples",
        "origin_unix_ns": timeline["origin_unix_ns"],
        "origin_monotonic_ns": origin_monotonic_ns,
        "formal_start_monotonic_ns": formal_start,
        "formal_end_monotonic_ns": formal_end,
        "formal_duration_ns": formal_duration,
        "logical_cpus": logical_cpus,
        "combined": {
            "coverage_ratio": round(
                min(1.0, combined_coverage_ns / formal_duration),
                9,
            ),
            "average_single_core_percent": round(combined_average, 6),
            "peak_single_core_percent": round(combined_peak, 6),
            "average_host_percent": round(combined_average / logical_cpus, 6),
            "peak_host_percent": round(combined_peak / logical_cpus, 6),
        },
        "windows": windows,
    }


def validate_segment(path):
    segment = load_json(path)
    if segment.get("schema_version") != SCHEMA_VERSION:
        raise ResourceError(f"{path} has an unsupported resource schema")
    if segment.get("kind") != "rmdb_resource_segment":
        raise ResourceError(f"{path} is not an RMDB resource segment")
    if segment.get("status") not in {
        "available",
        "partial",
        "unavailable",
        "failed",
    }:
        raise ResourceError(f"{path} has an invalid resource status")
    return segment


def aggregate_segments(arguments):
    warnings = []
    segments = []
    for path in arguments.segment:
        try:
            segments.append(validate_segment(path))
        except ResourceError as error:
            append_warning(warnings, "invalid_resource_segment", error)

    rss_values = [
        segment.get("max_rss_bytes")
        for segment in segments
        if isinstance(segment.get("max_rss_bytes"), int)
        and segment["max_rss_bytes"] > 0
    ]
    disk_peak_values = [
        segment.get("max_disk_allocated_bytes")
        for segment in segments
        if isinstance(segment.get("max_disk_allocated_bytes"), int)
        and segment["max_disk_allocated_bytes"] >= 0
        and segment.get("disk_samples", 0) > 0
    ]
    apparent_peak_values = [
        segment.get("max_disk_apparent_bytes")
        for segment in segments
        if isinstance(segment.get("max_disk_apparent_bytes"), int)
        and segment["max_disk_apparent_bytes"] >= 0
        and segment.get("disk_samples", 0) > 0
    ]
    final_candidates = [
        (
            segment.get("completed_monotonic_ns", 0),
            segment.get("final_disk"),
        )
        for segment in segments
        if isinstance(segment.get("final_disk"), dict)
    ]
    final_disk = max(final_candidates, default=(0, None))[1]

    if arguments.mode in {"all", "rank"}:
        try:
            timeline = parse_timeline(arguments.timeline)
            rank_cpu = calculate_rank_cpu(
                segments,
                timeline,
                Path(arguments.rank_complete).is_file(),
            )
        except ResourceError as error:
            rank_cpu = {"status": "unavailable", "reason": str(error)}
    else:
        rank_cpu = {
            "status": "not_applicable",
            "reason": f"mode {arguments.mode} has no ranked measurement",
        }

    for segment in segments:
        for warning in segment.get("warnings", []):
            if isinstance(warning, dict) and warning not in warnings:
                warnings.append(warning)

    rss_status = "available" if rss_values else "unavailable"
    disk_status = "available" if disk_peak_values and final_disk else "unavailable"
    required_statuses = [rss_status, disk_status]
    if rank_cpu["status"] != "not_applicable":
        required_statuses.append(rank_cpu["status"])
    if required_statuses and all(status == "available" for status in required_statuses):
        status = "available"
    elif any(status in {"available", "partial"} for status in required_statuses):
        status = "partial"
    else:
        status = "unavailable"
    if any(segment.get("status") in {"partial", "failed"} for segment in segments):
        status = "partial" if status == "available" else status

    max_rss = max(rss_values, default=0)
    disk_peak = max(disk_peak_values, default=0)
    apparent_peak = max(apparent_peak_values, default=0)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "rmdb_resource_metrics",
        "status": status,
        "ranked": False,
        "score_effect": "none",
        "scope": (
            "full_workflow_server_lifetimes"
            if arguments.mode == "all"
            else "selected_mode_server_lifetimes"
        ),
        "sample_interval_ms": arguments.interval_ms,
        "server_generations": len(segments),
        "max_rss": {
            "status": rss_status,
            "bytes": max_rss,
            "gb_decimal": round(max_rss / 1_000_000_000, 9),
            "aggregation": "maximum_of_sampled_process_tree_rss_sums",
        },
        "database_disk": {
            "status": disk_status,
            "peak_allocated_bytes": disk_peak,
            "peak_apparent_bytes": apparent_peak,
            "disk_usage_gb_decimal": round(disk_peak / 1_000_000_000, 9),
            "final": final_disk,
            "aggregation": "lstat_st_blocks_512_hardlinks_deduplicated",
        },
        "rank_cpu": rank_cpu,
        "warnings": warnings,
        "segments": [str(Path(path).name) for path in arguments.segment],
    }
    atomic_write_json(arguments.output, payload)
    return 0


def positive_integer(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be an integer") from error
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample = subparsers.add_parser("sample", help="sample one RMDB process generation")
    sample.add_argument("--root-pid", required=True, type=positive_integer)
    sample.add_argument("--process-group", required=True, type=positive_integer)
    sample.add_argument("--database-path", required=True, type=Path)
    sample.add_argument("--output", required=True, type=Path)
    sample.add_argument("--interval-ms", type=positive_integer, default=DEFAULT_INTERVAL_MS)
    sample.add_argument("--proc-root", type=Path, default=Path("/proc"))

    aggregate = subparsers.add_parser("aggregate", help="merge resource segments")
    aggregate.add_argument("--segment", action="append", default=[], type=Path)
    aggregate.add_argument("--timeline", required=True, type=Path)
    aggregate.add_argument("--rank-complete", required=True, type=Path)
    aggregate.add_argument(
        "--mode",
        required=True,
        choices=("all", "init", "rank", "recovery"),
    )
    aggregate.add_argument("--output", required=True, type=Path)
    aggregate.add_argument(
        "--interval-ms",
        type=positive_integer,
        default=DEFAULT_INTERVAL_MS,
    )
    return parser


def main():
    arguments = build_parser().parse_args()
    try:
        if arguments.command == "sample":
            return sample_segment(arguments)
        return aggregate_segments(arguments)
    except (OSError, ResourceError, ValueError, OverflowError) as error:
        try:
            atomic_write_json(
                arguments.output,
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": (
                        "rmdb_resource_segment"
                        if arguments.command == "sample"
                        else "rmdb_resource_metrics"
                    ),
                    "status": "failed",
                    "ranked": False,
                    "score_effect": "none",
                    "reason": str(error),
                },
            )
        except OSError:
            pass
        print(f"resource_sampler: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
