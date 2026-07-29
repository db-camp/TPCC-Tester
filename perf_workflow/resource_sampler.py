#!/usr/bin/env python3
"""Collect observation-only RMDB resource metrics for the final2026 workflow."""

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import sys
import tempfile
import threading
import time


SCHEMA_VERSION = 1
DEFAULT_INTERVAL_MS = 1000
CLOCK_OFFSET_TOLERANCE_NS = 10_000_000
MAX_SAMPLE_GAP_NUMERATOR = 3
MAX_SAMPLE_GAP_DENOMINATOR = 2
TIMELINE_KEYS = {
    "schema_version",
    "kind",
    "origin_unix_ns",
    "warmup_ns",
    "measurement_windows",
    "measurement_window_ns",
}
SEGMENT_FIELDS = {
    "schema_version",
    "kind",
    "status",
    "ranked",
    "score_effect",
    "run_id",
    "generation",
    "root_pid",
    "root_identity_expected",
    "root_identity_observed",
    "root_strong_identity",
    "root_observed_exit",
    "process_group",
    "database_path",
    "database_identity_expected",
    "database_identity_observed",
    "sample_interval_ms",
    "started_unix_ns",
    "started_monotonic_ns",
    "completed_unix_ns",
    "completed_monotonic_ns",
    "backend",
    "logical_cpus",
    "process_samples",
    "disk_samples",
    "missed_deadlines",
    "max_sample_collection_span_ns",
    "clock_offset_spread_ns",
    "max_rss_bytes",
    "max_disk_allocated_bytes",
    "max_disk_apparent_bytes",
    "final_disk",
    "cpu_intervals",
    "clock_correlations",
    "warnings",
}
INTERVAL_FIELDS = {
    "start_monotonic_ns",
    "end_monotonic_ns",
    "cpu_delta_ns",
    "start_collection_span_ns",
    "end_collection_span_ns",
}
CORRELATION_FIELDS = {
    "monotonic_before_ns",
    "monotonic_after_ns",
    "monotonic_ns",
    "unix_ns",
    "offset_lower_ns",
    "offset_upper_ns",
    "collection_span_ns",
}
COMPLETION_FIELDS = {
    "schema_version",
    "kind",
    "run_id",
    "timeline_sha256",
    "timeline_size_bytes",
    "timeline_mtime_ns",
    "completed_unix_ns",
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


def require_exact_fields(value, fields, label):
    actual = set(value)
    if actual != fields:
        missing = sorted(fields - actual)
        unknown = sorted(actual - fields)
        raise ResourceError(
            f"{label} fields do not match schema version {SCHEMA_VERSION}; "
            f"missing={missing}, unknown={unknown}"
        )


def parse_database_identity(value):
    if value == "auto":
        return None
    device, separator, inode = value.partition(":")
    if not separator:
        raise argparse.ArgumentTypeError(
            "database identity must be auto or DEVICE:INODE"
        )
    try:
        parsed = (int(device), int(inode))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "database identity must contain integers"
        ) from error
    if parsed[0] < 0 or parsed[1] <= 0:
        raise argparse.ArgumentTypeError(
            "database identity requires a non-negative device and positive inode"
        )
    return parsed


def encode_database_identity(identity):
    if identity is None:
        return None
    return {
        "device": identity[0],
        "inode": identity[1],
    }


def decode_database_identity(value, label):
    if not isinstance(value, dict) or set(value) != {"device", "inode"}:
        raise ResourceError(f"{label} is not a device/inode identity")
    device = value["device"]
    inode = value["inode"]
    if (
        not isinstance(device, int)
        or isinstance(device, bool)
        or device < 0
        or not isinstance(inode, int)
        or isinstance(inode, bool)
        or inode <= 0
    ):
        raise ResourceError(f"{label} has invalid device/inode values")
    return (device, inode)


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
                "identity": (
                    f"darwin:{bsd.pbi_start_tvsec}:{bsd.pbi_start_tvusec}"
                ),
                "strong_identity": f"darwin-abstime:{start_abstime}",
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
        "strong_identity": root.get("strong_identity", root["identity"]),
        "process_count": len(live_owned),
        "cpu_ns": sum(item["cpu_ns"] for item in live_owned),
        "rss_bytes": sum(item["rss_bytes"] for item in live_owned),
    }


def directory_usage(path, expected_identity=None):
    root = Path(path)
    try:
        root_stat = root.lstat()
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ResourceError(f"cannot stat database directory {root}: {error}") from error
    if stat.S_ISLNK(root_stat.st_mode) or not stat.S_ISDIR(root_stat.st_mode):
        raise ResourceError(f"database path is not a real directory: {root}")
    root_identity = (root_stat.st_dev, root_stat.st_ino)
    if expected_identity is not None and root_identity != expected_identity:
        raise ResourceError(
            "database directory identity changed from "
            f"{expected_identity[0]}:{expected_identity[1]} to "
            f"{root_identity[0]}:{root_identity[1]}"
        )

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
        if stat.S_ISLNK(item_stat.st_mode):
            raise ResourceError(
                f"database directory contains a symbolic link: {current}"
            )
        identity = (item_stat.st_dev, item_stat.st_ino)
        if identity in seen:
            continue
        seen.add(identity)
        allocated += max(0, item_stat.st_blocks) * 512
        apparent += max(0, item_stat.st_size)
        if not stat.S_ISDIR(item_stat.st_mode):
            continue
        try:
            with os.scandir(current) as entries:
                stack.extend(Path(entry.path) for entry in entries)
        except OSError as error:
            raise ResourceError(f"cannot scan database directory {current}: {error}") from error
    try:
        final_root_stat = root.lstat()
    except OSError as error:
        raise ResourceError(
            f"cannot re-stat database directory {root}: {error}"
        ) from error
    final_identity = (final_root_stat.st_dev, final_root_stat.st_ino)
    if (
        stat.S_ISLNK(final_root_stat.st_mode)
        or not stat.S_ISDIR(final_root_stat.st_mode)
        or final_identity != root_identity
    ):
        raise ResourceError("database directory changed while it was sampled")
    return {
        "allocated_bytes": allocated,
        "apparent_bytes": apparent,
        "inode_count": len(seen),
        "root_identity": {
            "device": root_identity[0],
            "inode": root_identity[1],
        },
    }


def append_warning(warnings, code, detail):
    item = {"code": code, "detail": str(detail)}
    if item not in warnings:
        warnings.append(item)


def sample_segment(arguments):
    output = Path(arguments.output)
    stop_event = threading.Event()

    def request_stop(_signum, _frame):
        stop_event.set()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    started_unix_ns = time.time_ns()
    started_monotonic_ns = time.monotonic_ns()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "rmdb_resource_segment",
        "status": "collecting",
        "ranked": False,
        "score_effect": "none",
        "run_id": arguments.run_id,
        "generation": arguments.generation,
        "root_pid": arguments.root_pid,
        "root_identity_expected": arguments.root_identity,
        "root_identity_observed": None,
        "root_strong_identity": None,
        "root_observed_exit": False,
        "process_group": arguments.process_group,
        "database_path": str(Path(arguments.database_path)),
        "database_identity_expected": encode_database_identity(
            arguments.database_identity
        ),
        "database_identity_observed": None,
        "sample_interval_ms": arguments.interval_ms,
        "started_unix_ns": started_unix_ns,
        "started_monotonic_ns": started_monotonic_ns,
        "completed_unix_ns": None,
        "completed_monotonic_ns": None,
        "backend": None,
        "logical_cpus": None,
        "process_samples": 0,
        "disk_samples": 0,
        "missed_deadlines": 0,
        "max_sample_collection_span_ns": 0,
        "clock_offset_spread_ns": None,
        "max_rss_bytes": 0,
        "max_disk_allocated_bytes": 0,
        "max_disk_apparent_bytes": 0,
        "final_disk": None,
        "cpu_intervals": [],
        "clock_correlations": [],
        "warnings": [],
    }
    atomic_write_json(output, payload)

    try:
        backend = select_backend(arguments.proc_root)
        logical_cpus = online_cpu_count()
    except ResourceError as error:
        append_warning(
            payload["warnings"],
            "resource_backend_unavailable",
            error,
        )
        payload["status"] = "unavailable"
        payload["completed_unix_ns"] = time.time_ns()
        payload["completed_monotonic_ns"] = time.monotonic_ns()
        atomic_write_json(output, payload)
        return 0

    payload["backend"] = backend.metadata()
    payload["logical_cpus"] = logical_cpus
    expected_identity = arguments.root_identity
    observed_identity = None
    strong_identity = None
    database_identity = arguments.database_identity
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
        unix_before_ns = time.time_ns()
        monotonic_before_ns = time.monotonic_ns()
        stop_after_sample = False
        try:
            tree = collect_tree_sample(
                backend,
                arguments.root_pid,
                arguments.process_group,
                expected_identity,
            )
            monotonic_after_ns = time.monotonic_ns()
            unix_after_ns = time.time_ns()
            monotonic_ns = (
                monotonic_before_ns + monotonic_after_ns
            ) // 2
            unix_ns = (unix_before_ns + unix_after_ns) // 2
            collection_span_ns = monotonic_after_ns - monotonic_before_ns
            offset_first = unix_before_ns - monotonic_before_ns
            offset_second = unix_after_ns - monotonic_after_ns
            offset_lower_ns = min(offset_first, offset_second)
            offset_upper_ns = max(offset_first, offset_second)
            if offset_first - offset_second > CLOCK_OFFSET_TOLERANCE_NS:
                append_warning(
                    payload["warnings"],
                    "wall_clock_regressed_during_sample",
                    offset_first - offset_second,
                )
            if collection_span_ns > interval_ns // 2:
                append_warning(
                    payload["warnings"],
                    "process_sample_collection_too_slow",
                    collection_span_ns,
                )
            if observed_identity is None:
                observed_identity = tree["identity"]
                strong_identity = tree["strong_identity"]
            elif tree["strong_identity"] != strong_identity:
                raise ResourceError("registered RMDB strong identity changed")
            process_samples += 1
            max_rss_bytes = max(max_rss_bytes, tree["rss_bytes"])
            correlations.append(
                {
                    "monotonic_before_ns": monotonic_before_ns,
                    "monotonic_after_ns": monotonic_after_ns,
                    "monotonic_ns": monotonic_ns,
                    "unix_ns": unix_ns,
                    "offset_lower_ns": offset_lower_ns,
                    "offset_upper_ns": offset_upper_ns,
                    "collection_span_ns": collection_span_ns,
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
                    if (
                        elapsed_ns * MAX_SAMPLE_GAP_DENOMINATOR
                        > interval_ns * MAX_SAMPLE_GAP_NUMERATOR
                    ):
                        append_warning(
                            payload["warnings"],
                            "process_sample_gap_exceeded",
                            elapsed_ns,
                        )
                    intervals.append(
                        {
                            "start_monotonic_ns": previous["monotonic_ns"],
                            "end_monotonic_ns": monotonic_ns,
                            "cpu_delta_ns": cpu_delta_ns,
                            "start_collection_span_ns": previous[
                                "collection_span_ns"
                            ],
                            "end_collection_span_ns": collection_span_ns,
                        }
                    )
            previous = {
                "monotonic_ns": monotonic_ns,
                "cpu_ns": tree["cpu_ns"],
                "collection_span_ns": collection_span_ns,
            }
        except RootAbsent as error:
            root_observed_exit = True
            if process_samples == 0:
                append_warning(
                    payload["warnings"],
                    "root_absent_before_first_sample",
                    error,
                )
            stop_after_sample = True
        except ResourceError as error:
            append_warning(payload["warnings"], "process_sample_failed", error)

        try:
            disk = directory_usage(
                arguments.database_path,
                database_identity,
            )
            if disk is not None:
                sampled_identity = decode_database_identity(
                    disk["root_identity"],
                    "sampled database identity",
                )
                if database_identity is None:
                    database_identity = sampled_identity
                elif sampled_identity != database_identity:
                    raise ResourceError("sampled database identity changed")
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

        if stop_after_sample:
            break
        if stop_event.is_set():
            append_warning(
                payload["warnings"],
                "sampler_stopped_before_root_exit",
                "registered RMDB process was still present",
            )
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
    if correlations:
        offset_lower = min(
            item["offset_lower_ns"] for item in correlations
        )
        offset_upper = max(
            item["offset_upper_ns"] for item in correlations
        )
        clock_offset_spread_ns = offset_upper - offset_lower
        if clock_offset_spread_ns > CLOCK_OFFSET_TOLERANCE_NS:
            append_warning(
                payload["warnings"],
                "wall_monotonic_offset_drift",
                clock_offset_spread_ns,
            )
        max_collection_span_ns = max(
            item["collection_span_ns"] for item in correlations
        )
    else:
        clock_offset_spread_ns = None
        max_collection_span_ns = 0

    if process_samples == 0 and disk_samples == 0:
        status = "unavailable"
    elif (
        process_samples == 0
        or disk_samples == 0
        or payload["warnings"]
    ):
        status = "partial"
    else:
        status = "available"
    payload.update(
        {
            "status": status,
            "root_identity_observed": observed_identity,
            "root_strong_identity": strong_identity,
            "root_observed_exit": root_observed_exit,
            "database_identity_observed": encode_database_identity(
                database_identity
            ),
            "process_samples": process_samples,
            "disk_samples": disk_samples,
            "missed_deadlines": missed_deadlines,
            "max_sample_collection_span_ns": max_collection_span_ns,
            "clock_offset_spread_ns": clock_offset_spread_ns,
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


def read_stable_file(path, label):
    path = Path(path)
    try:
        before = path.stat()
        content = path.read_bytes()
        after = path.stat()
    except OSError as error:
        raise ResourceError(f"cannot read {label} {path}: {error}") from error
    before_identity = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    if before_identity != after_identity or len(content) != after.st_size:
        raise ResourceError(f"{label} changed while it was read")
    return content, after


def parse_timeline_bytes(content):
    try:
        text = content.decode("ascii")
    except UnicodeDecodeError as error:
        raise ResourceError("rank timeline is not ASCII") from error
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


def parse_timeline(path):
    content, _metadata = read_stable_file(path, "rank timeline")
    return parse_timeline_bytes(content)


def complete_rank(arguments):
    content, metadata = read_stable_file(arguments.timeline, "rank timeline")
    parse_timeline_bytes(content)
    completed_unix_ns = time.time_ns()
    if metadata.st_mtime_ns > completed_unix_ns:
        raise ResourceError("rank timeline modification time is in the future")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "rmdb_rank_completion",
        "run_id": arguments.run_id,
        "timeline_sha256": hashlib.sha256(content).hexdigest(),
        "timeline_size_bytes": len(content),
        "timeline_mtime_ns": metadata.st_mtime_ns,
        "completed_unix_ns": completed_unix_ns,
    }
    atomic_write_json(arguments.output, payload)
    return 0


def validate_rank_completion(path, run_id, timeline_content, timeline_metadata):
    marker = load_json(path)
    require_exact_fields(marker, COMPLETION_FIELDS, "rank completion marker")
    if marker["schema_version"] != SCHEMA_VERSION:
        raise ResourceError("rank completion marker schema is unsupported")
    if marker["kind"] != "rmdb_rank_completion":
        raise ResourceError("rank completion marker kind is invalid")
    if marker["run_id"] != run_id:
        raise ResourceError("rank completion marker belongs to another run")
    expected_sha256 = hashlib.sha256(timeline_content).hexdigest()
    if marker["timeline_sha256"] != expected_sha256:
        raise ResourceError("rank completion marker does not bind this timeline")
    if marker["timeline_size_bytes"] != len(timeline_content):
        raise ResourceError("rank completion marker timeline size is stale")
    if marker["timeline_mtime_ns"] != timeline_metadata.st_mtime_ns:
        raise ResourceError("rank completion marker timeline time is stale")
    completed_unix_ns = marker["completed_unix_ns"]
    if (
        not isinstance(completed_unix_ns, int)
        or isinstance(completed_unix_ns, bool)
        or completed_unix_ns < timeline_metadata.st_mtime_ns
        or completed_unix_ns > time.time_ns() + 1_000_000_000
    ):
        raise ResourceError("rank completion marker time is invalid")
    return marker


def overlap(left_start, left_end, right_start, right_end):
    return max(0, min(left_end, right_end) - max(left_start, right_start))


def calculate_rank_cpu(segments, timeline, completion):
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
    earliest_start = min(segment["started_unix_ns"] for segment in segments)
    if (
        timeline["origin_unix_ns"] < earliest_start - 1_000_000_000
        or timeline["origin_unix_ns"] > completion["completed_unix_ns"]
    ):
        return {
            "status": "unavailable",
            "reason": "rank timeline is outside the bound server generations",
        }
    expected_formal_end_unix_ns = (
        timeline["origin_unix_ns"]
        + timeline["warmup_ns"]
        + timeline["measurement_windows"]
        * timeline["measurement_window_ns"]
    )
    if (
        completion["completed_unix_ns"] + CLOCK_OFFSET_TOLERANCE_NS
        < expected_formal_end_unix_ns
    ):
        return {
            "status": "unavailable",
            "reason": "rank completion predates the formal measurement end",
        }
    correlation = min(
        correlations,
        key=lambda item: abs(item["unix_ns"] - timeline["origin_unix_ns"]),
    )
    chosen_offset_ns = (
        correlation["offset_lower_ns"] + correlation["offset_upper_ns"]
    ) // 2
    origin_monotonic_ns = timeline["origin_unix_ns"] - chosen_offset_ns
    clock_offset_lower_ns = min(
        item["offset_lower_ns"] for item in correlations
    )
    clock_offset_upper_ns = max(
        item["offset_upper_ns"] for item in correlations
    )
    clock_offset_spread_ns = (
        clock_offset_upper_ns - clock_offset_lower_ns
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
    previous_end = None
    for interval in intervals:
        if (
            previous_end is not None
            and interval["start_monotonic_ns"] < previous_end
        ):
            return {
                "status": "unavailable",
                "reason": "resource segments contain overlapping CPU intervals",
            }
        previous_end = interval["end_monotonic_ns"]
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
    complete = clock_offset_spread_ns <= CLOCK_OFFSET_TOLERANCE_NS
    combined_cpu_ns = 0.0
    combined_coverage_ns = 0
    combined_peak = 0.0
    boundaries = [
        formal_start + index * window_ns
        for index in range(timeline["measurement_windows"] + 1)
    ]
    boundary_uncertain = any(
        item["monotonic_before_ns"] <= boundary <= item["monotonic_after_ns"]
        for item in correlations
        for boundary in boundaries
    )
    complete = complete and not boundary_uncertain
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
        coverage_ratio = coverage_ns / window_ns
        if coverage_ratio > 1.000000001:
            return {
                "status": "unavailable",
                "reason": "rank CPU coverage exceeds one formal window",
            }
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
        "clock_offset_spread_ns": clock_offset_spread_ns,
        "boundary_sample_uncertain": boundary_uncertain,
        "origin_unix_ns": timeline["origin_unix_ns"],
        "origin_monotonic_ns": origin_monotonic_ns,
        "formal_start_monotonic_ns": formal_start,
        "formal_end_monotonic_ns": formal_end,
        "formal_duration_ns": formal_duration,
        "logical_cpus": logical_cpus,
        "combined": {
            "coverage_ratio": round(
                combined_coverage_ns / formal_duration,
                9,
            ),
            "average_single_core_percent": round(combined_average, 6),
            "peak_single_core_percent": round(combined_peak, 6),
            "average_host_percent": round(combined_average / logical_cpus, 6),
            "peak_host_percent": round(combined_peak / logical_cpus, 6),
        },
        "windows": windows,
    }


def require_integer(value, label, minimum=0):
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < minimum
    ):
        raise ResourceError(f"{label} must be an integer >= {minimum}")
    return value


def validate_disk_sample(value, label):
    if not isinstance(value, dict):
        raise ResourceError(f"{label} is not an object")
    require_exact_fields(
        value,
        {
            "allocated_bytes",
            "apparent_bytes",
            "inode_count",
            "root_identity",
        },
        label,
    )
    require_integer(value["allocated_bytes"], f"{label}.allocated_bytes")
    require_integer(value["apparent_bytes"], f"{label}.apparent_bytes")
    require_integer(value["inode_count"], f"{label}.inode_count", 1)
    return decode_database_identity(
        value["root_identity"],
        f"{label}.root_identity",
    )


def validate_segment(path):
    segment = load_json(path)
    require_exact_fields(segment, SEGMENT_FIELDS, f"resource segment {path}")
    if segment["schema_version"] != SCHEMA_VERSION:
        raise ResourceError(f"{path} has an unsupported resource schema")
    if segment["kind"] != "rmdb_resource_segment":
        raise ResourceError(f"{path} is not an RMDB resource segment")
    if segment["status"] not in {
        "available",
        "partial",
        "unavailable",
        "failed",
    }:
        raise ResourceError(f"{path} has an invalid resource status")
    if segment["ranked"] is not False or segment["score_effect"] != "none":
        raise ResourceError(f"{path} incorrectly marks resources as ranked")
    if not isinstance(segment["run_id"], str) or not segment["run_id"]:
        raise ResourceError(f"{path} has an invalid run id")
    require_integer(segment["generation"], f"{path}.generation", 1)
    require_integer(segment["root_pid"], f"{path}.root_pid", 1)
    require_integer(segment["process_group"], f"{path}.process_group", 1)
    if (
        not isinstance(segment["root_identity_expected"], str)
        or not segment["root_identity_expected"]
    ):
        raise ResourceError(f"{path} has no expected root identity")
    if not isinstance(segment["database_path"], str) or not segment["database_path"]:
        raise ResourceError(f"{path} has no database path")
    expected_database_identity = None
    if segment["database_identity_expected"] is not None:
        expected_database_identity = decode_database_identity(
            segment["database_identity_expected"],
            f"{path}.database_identity_expected",
        )
    observed_database_identity = None
    if segment["database_identity_observed"] is not None:
        observed_database_identity = decode_database_identity(
            segment["database_identity_observed"],
            f"{path}.database_identity_observed",
        )
    if (
        expected_database_identity is not None
        and observed_database_identity is not None
        and expected_database_identity != observed_database_identity
    ):
        raise ResourceError(f"{path} observed a different database identity")
    interval_ms = require_integer(
        segment["sample_interval_ms"],
        f"{path}.sample_interval_ms",
        1,
    )
    started_unix_ns = require_integer(
        segment["started_unix_ns"],
        f"{path}.started_unix_ns",
        1,
    )
    started_monotonic_ns = require_integer(
        segment["started_monotonic_ns"],
        f"{path}.started_monotonic_ns",
        1,
    )
    completed_unix_ns = require_integer(
        segment["completed_unix_ns"],
        f"{path}.completed_unix_ns",
        started_unix_ns,
    )
    completed_monotonic_ns = require_integer(
        segment["completed_monotonic_ns"],
        f"{path}.completed_monotonic_ns",
        started_monotonic_ns,
    )
    process_samples = require_integer(
        segment["process_samples"],
        f"{path}.process_samples",
    )
    disk_samples = require_integer(
        segment["disk_samples"],
        f"{path}.disk_samples",
    )
    require_integer(segment["missed_deadlines"], f"{path}.missed_deadlines")
    require_integer(
        segment["max_sample_collection_span_ns"],
        f"{path}.max_sample_collection_span_ns",
    )
    require_integer(segment["max_rss_bytes"], f"{path}.max_rss_bytes")
    require_integer(
        segment["max_disk_allocated_bytes"],
        f"{path}.max_disk_allocated_bytes",
    )
    require_integer(
        segment["max_disk_apparent_bytes"],
        f"{path}.max_disk_apparent_bytes",
    )
    if not isinstance(segment["root_observed_exit"], bool):
        raise ResourceError(f"{path}.root_observed_exit is not boolean")
    if process_samples:
        if segment["root_identity_observed"] != segment["root_identity_expected"]:
            raise ResourceError(f"{path} root identity is not launcher-bound")
        if (
            not isinstance(segment["root_strong_identity"], str)
            or not segment["root_strong_identity"]
        ):
            raise ResourceError(f"{path} has no strong root identity")
        if not isinstance(segment["backend"], dict):
            raise ResourceError(f"{path} has no process backend metadata")
        require_integer(segment["logical_cpus"], f"{path}.logical_cpus", 1)
    elif (
        segment["root_identity_observed"] is not None
        or segment["root_strong_identity"] is not None
    ):
        raise ResourceError(f"{path} has root identity without process samples")
    if disk_samples:
        if segment["final_disk"] is None:
            raise ResourceError(f"{path} has disk samples without final disk")
        final_identity = validate_disk_sample(
            segment["final_disk"],
            f"{path}.final_disk",
        )
        if final_identity != observed_database_identity:
            raise ResourceError(f"{path} final database identity changed")
    elif segment["final_disk"] is not None:
        raise ResourceError(f"{path} has final disk without disk samples")

    warnings = segment["warnings"]
    if not isinstance(warnings, list):
        raise ResourceError(f"{path}.warnings is not a list")
    for index, warning in enumerate(warnings):
        if not isinstance(warning, dict):
            raise ResourceError(f"{path}.warnings[{index}] is not an object")
        require_exact_fields(
            warning,
            {"code", "detail"},
            f"{path}.warnings[{index}]",
        )
        if not all(isinstance(warning[key], str) for key in warning):
            raise ResourceError(f"{path}.warnings[{index}] is not textual")

    correlations = segment["clock_correlations"]
    if not isinstance(correlations, list):
        raise ResourceError(f"{path}.clock_correlations is not a list")
    previous_monotonic = None
    for index, item in enumerate(correlations):
        if not isinstance(item, dict):
            raise ResourceError(f"{path}.clock_correlations[{index}] is invalid")
        require_exact_fields(
            item,
            CORRELATION_FIELDS,
            f"{path}.clock_correlations[{index}]",
        )
        before = require_integer(
            item["monotonic_before_ns"],
            f"{path}.clock_correlations[{index}].monotonic_before_ns",
            started_monotonic_ns,
        )
        after = require_integer(
            item["monotonic_after_ns"],
            f"{path}.clock_correlations[{index}].monotonic_after_ns",
            before,
        )
        midpoint = require_integer(
            item["monotonic_ns"],
            f"{path}.clock_correlations[{index}].monotonic_ns",
            before,
        )
        if midpoint > after or item["collection_span_ns"] != after - before:
            raise ResourceError(f"{path} has an invalid process sample bracket")
        require_integer(item["unix_ns"], f"{path}.clock_correlations[{index}].unix_ns", 1)
        lower = require_integer(
            item["offset_lower_ns"],
            f"{path}.clock_correlations[{index}].offset_lower_ns",
        )
        upper = require_integer(
            item["offset_upper_ns"],
            f"{path}.clock_correlations[{index}].offset_upper_ns",
            lower,
        )
        if previous_monotonic is not None and midpoint <= previous_monotonic:
            raise ResourceError(f"{path} clock correlations are not monotonic")
        previous_monotonic = midpoint
    if len(correlations) != process_samples:
        raise ResourceError(f"{path} process sample count is inconsistent")
    calculated_spread = (
        max(item["offset_upper_ns"] for item in correlations)
        - min(item["offset_lower_ns"] for item in correlations)
        if correlations
        else None
    )
    if segment["clock_offset_spread_ns"] != calculated_spread:
        raise ResourceError(f"{path} clock offset spread is inconsistent")
    calculated_max_span = max(
        (item["collection_span_ns"] for item in correlations),
        default=0,
    )
    if segment["max_sample_collection_span_ns"] != calculated_max_span:
        raise ResourceError(f"{path} process collection span is inconsistent")

    intervals = segment["cpu_intervals"]
    if not isinstance(intervals, list):
        raise ResourceError(f"{path}.cpu_intervals is not a list")
    previous_end = None
    for index, item in enumerate(intervals):
        if not isinstance(item, dict):
            raise ResourceError(f"{path}.cpu_intervals[{index}] is invalid")
        require_exact_fields(
            item,
            INTERVAL_FIELDS,
            f"{path}.cpu_intervals[{index}]",
        )
        start = require_integer(
            item["start_monotonic_ns"],
            f"{path}.cpu_intervals[{index}].start_monotonic_ns",
            started_monotonic_ns,
        )
        end = require_integer(
            item["end_monotonic_ns"],
            f"{path}.cpu_intervals[{index}].end_monotonic_ns",
            start + 1,
        )
        if end > completed_monotonic_ns:
            raise ResourceError(f"{path} CPU interval exceeds segment lifetime")
        require_integer(
            item["cpu_delta_ns"],
            f"{path}.cpu_intervals[{index}].cpu_delta_ns",
        )
        require_integer(
            item["start_collection_span_ns"],
            f"{path}.cpu_intervals[{index}].start_collection_span_ns",
        )
        require_integer(
            item["end_collection_span_ns"],
            f"{path}.cpu_intervals[{index}].end_collection_span_ns",
        )
        if previous_end is not None and start < previous_end:
            raise ResourceError(f"{path} contains overlapping CPU intervals")
        previous_end = end
    if len(intervals) > max(0, process_samples - 1):
        raise ResourceError(f"{path} has too many CPU intervals")
    if segment["status"] == "available" and (
        warnings
        or process_samples == 0
        or disk_samples == 0
        or segment["clock_offset_spread_ns"] > CLOCK_OFFSET_TOLERANCE_NS
    ):
        raise ResourceError(f"{path} overclaims available resource coverage")
    if interval_ms <= 0:
        raise ResourceError(f"{path} has an invalid sample interval")
    return segment


def aggregate_segments(arguments):
    warnings = []
    segments = []
    requested_paths = []
    seen_paths = set()
    for path in arguments.segment:
        canonical = str(Path(path).resolve(strict=False))
        requested_paths.append(canonical)
        if canonical in seen_paths:
            append_warning(
                warnings,
                "duplicate_resource_segment",
                canonical,
            )
            continue
        seen_paths.add(canonical)
        try:
            segment = validate_segment(path)
            if segment["run_id"] != arguments.run_id:
                raise ResourceError(f"{path} belongs to another workflow run")
            if segment["sample_interval_ms"] != arguments.interval_ms:
                raise ResourceError(f"{path} uses a different sample interval")
            if (
                str(Path(segment["database_path"]).resolve(strict=False))
                != str(Path(arguments.database_path).resolve(strict=False))
            ):
                raise ResourceError(f"{path} belongs to another database path")
            observed_identity = segment["database_identity_observed"]
            if (
                observed_identity is None
                or decode_database_identity(
                    observed_identity,
                    f"{path}.database_identity_observed",
                )
                != arguments.database_identity
            ):
                raise ResourceError(f"{path} belongs to another database inode")
            segments.append(segment)
        except ResourceError as error:
            append_warning(warnings, "invalid_resource_segment", error)

    generations = {}
    for segment in segments:
        generation = segment["generation"]
        if generation in generations:
            append_warning(
                warnings,
                "duplicate_resource_generation",
                generation,
            )
            continue
        generations[generation] = segment
    expected_generation_ids = set(range(1, arguments.expected_generations + 1))
    actual_generation_ids = set(generations)
    if actual_generation_ids != expected_generation_ids:
        append_warning(
            warnings,
            "resource_generation_set_mismatch",
            (
                f"expected={sorted(expected_generation_ids)}, "
                f"actual={sorted(actual_generation_ids)}"
            ),
        )
    segments = [
        generations[generation]
        for generation in sorted(actual_generation_ids & expected_generation_ids)
    ]

    backend_identities = {
        (
            segment["backend"].get("backend"),
            segment["backend"].get("boot_id"),
        )
        for segment in segments
        if isinstance(segment["backend"], dict)
    }
    if len(backend_identities) > 1:
        append_warning(
            warnings,
            "resource_backend_identity_changed",
            sorted(str(value) for value in backend_identities),
        )

    all_intervals = sorted(
        (
            (interval["start_monotonic_ns"], interval["end_monotonic_ns"])
            for segment in segments
            for interval in segment["cpu_intervals"]
        )
    )
    intervals_overlap = any(
        current[0] < previous[1]
        for previous, current in zip(all_intervals, all_intervals[1:])
    )
    if intervals_overlap:
        append_warning(
            warnings,
            "resource_segments_overlap",
            "CPU intervals from distinct generations overlap",
        )

    generation_set_complete = (
        len(requested_paths) == arguments.expected_generations
        and len(seen_paths) == arguments.expected_generations
        and actual_generation_ids == expected_generation_ids
        and len(segments) == arguments.expected_generations
        and not intervals_overlap
        and len(backend_identities) <= 1
    )
    all_generations_available = (
        generation_set_complete
        and all(segment["status"] == "available" for segment in segments)
    )

    rss_values = [
        segment["max_rss_bytes"]
        for segment in segments
        if segment["process_samples"] > 0 and segment["max_rss_bytes"] > 0
    ]
    disk_peak_values = [
        segment["max_disk_allocated_bytes"]
        for segment in segments
        if segment["disk_samples"] > 0
    ]
    apparent_peak_values = [
        segment["max_disk_apparent_bytes"]
        for segment in segments
        if segment["disk_samples"] > 0
    ]
    final_candidates = [
        (
            segment["completed_monotonic_ns"],
            segment["final_disk"],
        )
        for segment in segments
        if segment["final_disk"] is not None
    ]
    final_disk = max(final_candidates, default=(0, None))[1]

    if arguments.mode in {"all", "rank"}:
        try:
            timeline_content, timeline_metadata = read_stable_file(
                arguments.timeline,
                "rank timeline",
            )
            timeline = parse_timeline_bytes(timeline_content)
            if (
                timeline["warmup_ns"]
                != arguments.expected_warmup_seconds * 1_000_000_000
                or timeline["measurement_window_ns"]
                != arguments.expected_window_seconds * 1_000_000_000
            ):
                raise ResourceError(
                    "rank timeline durations do not match workflow configuration"
                )
            completion = validate_rank_completion(
                arguments.rank_complete,
                arguments.run_id,
                timeline_content,
                timeline_metadata,
            )
            rank_cpu = calculate_rank_cpu(
                segments,
                timeline,
                completion,
            )
        except ResourceError as error:
            rank_cpu = {"status": "unavailable", "reason": str(error)}
    else:
        rank_cpu = {
            "status": "not_applicable",
            "reason": f"mode {arguments.mode} has no ranked measurement",
        }

    for segment in segments:
        for warning in segment["warnings"]:
            if warning not in warnings:
                warnings.append(warning)

    if rss_values:
        rss_status = "available" if all_generations_available else "partial"
    else:
        rss_status = "unavailable"
    if disk_peak_values and final_disk:
        disk_status = "available" if all_generations_available else "partial"
    else:
        disk_status = "unavailable"
    required_statuses = [rss_status, disk_status]
    if rank_cpu["status"] != "not_applicable":
        required_statuses.append(rank_cpu["status"])
    if required_statuses and all(status == "available" for status in required_statuses):
        status = "available"
    elif any(status in {"available", "partial"} for status in required_statuses):
        status = "partial"
    else:
        status = "unavailable"
    if not generation_set_complete and status == "available":
        status = "partial"
    if warnings and status == "available":
        status = "partial"

    max_rss = max(rss_values, default=0)
    disk_peak = max(disk_peak_values, default=0)
    apparent_peak = max(apparent_peak_values, default=0)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "kind": "rmdb_resource_metrics",
        "status": status,
        "ranked": False,
        "score_effect": "none",
        "run_id": arguments.run_id,
        "scope": (
            "full_workflow_server_lifetimes"
            if arguments.mode == "all"
            else "selected_mode_server_lifetimes"
        ),
        "database_path": str(Path(arguments.database_path)),
        "database_identity": encode_database_identity(
            arguments.database_identity
        ),
        "sample_interval_ms": arguments.interval_ms,
        "expected_server_generations": arguments.expected_generations,
        "requested_server_generations": len(requested_paths),
        "valid_server_generations": len(segments),
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
        "segments": [
            {
                "generation": segment["generation"],
                "status": segment["status"],
                "root_pid": segment["root_pid"],
                "root_identity": segment["root_identity_observed"],
                "root_observed_exit": segment["root_observed_exit"],
            }
            for segment in segments
        ],
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


def nonnegative_integer(value):
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("value must be an integer") from error
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be non-negative")
    return parsed


def nonempty_text(value):
    if not value or "\0" in value:
        raise argparse.ArgumentTypeError("value must be non-empty text")
    return value


def required_database_identity(value):
    parsed = parse_database_identity(value)
    if parsed is None:
        raise argparse.ArgumentTypeError(
            "aggregate requires an exact DEVICE:INODE database identity"
        )
    return parsed


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sample = subparsers.add_parser("sample", help="sample one RMDB process generation")
    sample.add_argument("--run-id", required=True, type=nonempty_text)
    sample.add_argument("--generation", required=True, type=positive_integer)
    sample.add_argument("--root-pid", required=True, type=positive_integer)
    sample.add_argument("--root-identity", required=True, type=nonempty_text)
    sample.add_argument("--process-group", required=True, type=positive_integer)
    sample.add_argument("--database-path", required=True, type=Path)
    sample.add_argument(
        "--database-identity",
        required=True,
        type=parse_database_identity,
        metavar="auto|DEVICE:INODE",
    )
    sample.add_argument("--output", required=True, type=Path)
    sample.add_argument("--interval-ms", type=positive_integer, default=DEFAULT_INTERVAL_MS)
    sample.add_argument("--proc-root", type=Path, default=Path("/proc"))

    complete = subparsers.add_parser(
        "complete-rank",
        help="bind a successful rank to its scheduler timeline",
    )
    complete.add_argument("--run-id", required=True, type=nonempty_text)
    complete.add_argument("--timeline", required=True, type=Path)
    complete.add_argument("--output", required=True, type=Path)

    aggregate = subparsers.add_parser("aggregate", help="merge resource segments")
    aggregate.add_argument("--segment", action="append", default=[], type=Path)
    aggregate.add_argument("--run-id", required=True, type=nonempty_text)
    aggregate.add_argument(
        "--expected-generations",
        required=True,
        type=positive_integer,
    )
    aggregate.add_argument("--database-path", required=True, type=Path)
    aggregate.add_argument(
        "--database-identity",
        required=True,
        type=required_database_identity,
        metavar="DEVICE:INODE",
    )
    aggregate.add_argument("--timeline", required=True, type=Path)
    aggregate.add_argument("--rank-complete", required=True, type=Path)
    aggregate.add_argument(
        "--expected-warmup-seconds",
        required=True,
        type=nonnegative_integer,
    )
    aggregate.add_argument(
        "--expected-window-seconds",
        required=True,
        type=positive_integer,
    )
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
        if arguments.command == "complete-rank":
            return complete_rank(arguments)
        return aggregate_segments(arguments)
    except (
        OSError,
        ResourceError,
        ValueError,
        OverflowError,
        TypeError,
        KeyError,
    ) as error:
        try:
            atomic_write_json(
                arguments.output,
                {
                    "schema_version": SCHEMA_VERSION,
                    "kind": (
                        "rmdb_resource_segment"
                        if arguments.command == "sample"
                        else (
                            "rmdb_rank_completion"
                            if arguments.command == "complete-rank"
                            else "rmdb_resource_metrics"
                        )
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
