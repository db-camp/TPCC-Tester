#!/usr/bin/env python3
import argparse
from copy import deepcopy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "run_workflow.sh"
METRICS_HELPER = Path(__file__).resolve().parents[1] / "diagnostic_metrics.py"
RESOURCE_HELPER = Path(__file__).resolve().parents[1] / "resource_sampler.py"
RESOURCE_SPEC = importlib.util.spec_from_file_location(
    "workflow_resource_sampler",
    RESOURCE_HELPER,
)
RESOURCE_SAMPLER = importlib.util.module_from_spec(RESOURCE_SPEC)
RESOURCE_SPEC.loader.exec_module(RESOURCE_SAMPLER)


class WorkflowSafetyTests(unittest.TestCase):
    def kill_process_session(self, session_id):
        for requested_signal in (signal.SIGTERM, signal.SIGKILL):
            result = subprocess.run(
                ["ps", "-axo", "pid=,sess="],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            members = []
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    fields = line.split()
                    if len(fields) != 2:
                        continue
                    try:
                        pid, candidate_session = map(int, fields)
                    except ValueError:
                        continue
                    if candidate_session == session_id and pid != os.getpid():
                        members.append(pid)
            for pid in reversed(members):
                try:
                    os.kill(pid, requested_signal)
                except ProcessLookupError:
                    pass
            time.sleep(0.05)

    def run_script(self, *args, script=SCRIPT, env=None):
        command = ["bash", str(script), *map(str, args)]
        process = subprocess.Popen(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            start_new_session=True,
        )
        try:
            stdout, stderr = process.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            self.kill_process_session(process.pid)
            process.communicate()
            raise
        return subprocess.CompletedProcess(
            command,
            process.returncode,
            stdout,
            stderr,
        )

    def make_root(self, parent):
        root = Path(parent) / "rmdb"
        (root / "deps" / "TPCC-Tester").mkdir(parents=True)
        return root

    def make_executable(self, path, body):
        path.write_text("#!/usr/bin/env bash\n" + body, encoding="utf-8")
        path.chmod(0o755)
        return path

    def make_python_executable(self, path, body):
        path.write_text("#!/usr/bin/env python3\n" + body, encoding="utf-8")
        path.chmod(0o755)
        return path

    def reserve_port(self):
        reservation = socket.socket()
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
        reservation.close()
        return port

    def make_lifecycle_fakes(self, temp_path, root):
        calls = temp_path / "tester-calls"
        server_events = temp_path / "server-events"
        server = self.make_python_executable(
            temp_path / "fake-rmdb",
            """
import os
import signal
import socket
import sys
import time

database_existed = os.path.isdir(sys.argv[1])
os.makedirs(sys.argv[1], exist_ok=True)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("0.0.0.0", int(os.environ["RMDB_PORT"])))
listener.listen(8)
with open(os.environ["FAKE_SERVER_EVENTS"], "a", encoding="utf-8") as output:
    output.write(f"start {os.getpid()} {int(database_existed)}\\n")
def stop(signum, _frame):
    with open(
        os.environ["FAKE_SERVER_EVENTS"], "a", encoding="utf-8"
    ) as output:
        output.write(f"graceful {os.getpid()} {signum}\\n")
    listener.close()
    os._exit(0)
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
while True:
    time.sleep(0.02)
""",
        )
        tester = self.make_python_executable(
            temp_path / "fake-tpcc",
            """
import os
import sys

with open(os.environ["FAKE_TPCC_CALLS"], "a", encoding="utf-8") as output:
    output.write("\\t".join(sys.argv[1:]) + "\\n")
""",
        )
        env = os.environ.copy()
        env["FAKE_TPCC_CALLS"] = str(calls)
        env["FAKE_SERVER_EVENTS"] = str(server_events)
        return server, tester, calls, server_events, env, self.reserve_port()

    def wait_for_path(self, path, timeout=3.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if path.exists():
                return True
            time.sleep(0.01)
        return path.exists()

    def assert_pid_gone(self, pid):
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            result = subprocess.run(
                ["ps", "-p", str(pid)],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            if result.returncode != 0:
                return
            time.sleep(0.02)
        self.fail(f"process {pid} is still alive")

    def make_resource_segment(self, database_path, generation=1, shift=0):
        origin_monotonic = 1_000_000_000 + shift
        origin_unix = time.time_ns() - 5_000_000_000 + shift
        offset = origin_unix - origin_monotonic
        midpoints = [
            origin_monotonic - 500_000_000 + index * 1_000_000_000
            for index in range(5)
        ]
        correlations = [
            {
                "monotonic_before_ns": midpoint - 1_000,
                "monotonic_after_ns": midpoint + 1_000,
                "monotonic_ns": midpoint,
                "unix_ns": midpoint + offset,
                "offset_lower_ns": offset - 100,
                "offset_upper_ns": offset + 100,
                "collection_span_ns": 2_000,
            }
            for midpoint in midpoints
        ]
        intervals = [
            {
                "start_monotonic_ns": midpoints[index],
                "end_monotonic_ns": midpoints[index + 1],
                "cpu_delta_ns": 1_000_000_000,
                "start_collection_span_ns": 2_000,
                "end_collection_span_ns": 2_000,
            }
            for index in range(4)
        ]
        return {
            "schema_version": 1,
            "kind": "rmdb_resource_segment",
            "status": "available",
            "ranked": False,
            "score_effect": "none",
            "run_id": "resource-run",
            "generation": generation,
            "root_pid": 100 + generation,
            "root_identity_expected": f"linux:{generation}",
            "root_identity_observed": f"linux:{generation}",
            "root_strong_identity": f"linux:{generation}",
            "root_observed_exit": False,
            "process_group": 100 + generation,
            "database_path": str(database_path),
            "database_identity_expected": {"device": 7, "inode": 9},
            "database_identity_observed": {"device": 7, "inode": 9},
            "sample_interval_ms": 1000,
            "started_unix_ns": origin_unix - 600_000_000,
            "started_monotonic_ns": origin_monotonic - 600_000_000,
            "completed_unix_ns": origin_unix + 4_600_000_000,
            "completed_monotonic_ns": origin_monotonic + 4_600_000_000,
            "backend": {"backend": "linux_proc", "boot_id": "boot"},
            "logical_cpus": 4,
            "process_samples": 5,
            "disk_samples": 5,
            "missed_deadlines": 0,
            "max_sample_collection_span_ns": 2_000,
            "clock_offset_spread_ns": 200,
            "max_rss_bytes": 1_000,
            "max_disk_allocated_bytes": 2_000,
            "max_disk_apparent_bytes": 3_000,
            "final_disk": {
                "allocated_bytes": 2_000,
                "apparent_bytes": 3_000,
                "inode_count": 2,
                "root_identity": {"device": 7, "inode": 9},
            },
            "cpu_intervals": intervals,
            "clock_correlations": correlations,
            "warnings": [],
        }, origin_unix

    def test_bash_syntax(self):
        result = subprocess.run(
            ["bash", "-n", str(SCRIPT)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_resource_sampler_tracks_tree_and_rejects_symlinked_disk_entries(self):
        class FakeBackend:
            def process_table(self):
                return {
                    10: {
                        "ppid": 1,
                        "pgid": 10,
                        "identity": "linux:10",
                        "cpu_ns": 100,
                        "rss_bytes": 1_000,
                    },
                    11: {
                        "ppid": 10,
                        "pgid": 10,
                        "identity": "linux:11",
                        "cpu_ns": 200,
                        "rss_bytes": 2_000,
                    },
                    12: {
                        "ppid": 11,
                        "pgid": 10,
                        "identity": "linux:12",
                        "cpu_ns": 300,
                        "rss_bytes": 3_000,
                    },
                    99: {
                        "ppid": 1,
                        "pgid": 99,
                        "identity": "linux:99",
                        "cpu_ns": 9_999,
                        "rss_bytes": 9_999,
                    },
                }

        sample = RESOURCE_SAMPLER.collect_tree_sample(
            FakeBackend(),
            10,
            10,
            "linux:10",
        )
        self.assertEqual(sample["process_count"], 3)
        self.assertEqual(sample["cpu_ns"], 600)
        self.assertEqual(sample["rss_bytes"], 6_000)

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "database"
            root.mkdir()
            page = root / "page"
            page.write_bytes(b"x" * 8_192)
            os.link(page, root / "same-page")
            usage = RESOURCE_SAMPLER.directory_usage(root)
            self.assertEqual(usage["inode_count"], 2)
            outside = Path(temp) / "outside"
            outside.write_text("outside", encoding="utf-8")
            (root / "escape").symlink_to(outside)
            with self.assertRaises(RESOURCE_SAMPLER.ResourceError):
                RESOURCE_SAMPLER.directory_usage(root)

    def test_resource_sampler_treats_verified_root_exit_as_complete(self):
        class ExitingBackend:
            def __init__(self):
                self.calls = 0

            def metadata(self):
                return {"backend": "test"}

            def process_table(self):
                self.calls += 1
                if self.calls == 1:
                    return {
                        10: {
                            "ppid": 1,
                            "pgid": 10,
                            "identity": "linux:10",
                            "strong_identity": "linux-strong:10",
                            "cpu_ns": 100,
                            "rss_bytes": 1_000,
                        }
                    }
                return {}

        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            database = temp_path / "database"
            database.mkdir()
            database_stat = database.stat()
            output = temp_path / "segment.json"
            old_sigint = signal.getsignal(signal.SIGINT)
            old_sigterm = signal.getsignal(signal.SIGTERM)
            try:
                with mock.patch.object(
                    RESOURCE_SAMPLER,
                    "select_backend",
                    return_value=ExitingBackend(),
                ), mock.patch.object(
                    RESOURCE_SAMPLER,
                    "online_cpu_count",
                    return_value=4,
                ):
                    result = RESOURCE_SAMPLER.sample_segment(
                        argparse.Namespace(
                            output=output,
                            run_id="root-exit-run",
                            generation=1,
                            root_pid=10,
                            root_identity="linux:10",
                            process_group=10,
                            database_path=database,
                            database_identity=(
                                database_stat.st_dev,
                                database_stat.st_ino,
                            ),
                            interval_ms=100,
                            proc_root=Path("/unused"),
                        )
                    )
            finally:
                signal.signal(signal.SIGINT, old_sigint)
                signal.signal(signal.SIGTERM, old_sigterm)

            self.assertEqual(result, 0)
            segment = RESOURCE_SAMPLER.validate_segment(output)
            self.assertEqual(segment["status"], "available")
            self.assertTrue(segment["root_observed_exit"])
            self.assertEqual(segment["process_samples"], 1)
            self.assertEqual(segment["root_identity_observed"], "linux:10")
            self.assertEqual(segment["warnings"], [])

            class InterruptedBackend:
                def metadata(self):
                    return {"backend": "test"}

                def process_table(self):
                    os.kill(os.getpid(), signal.SIGINT)
                    return {
                        10: {
                            "ppid": 1,
                            "pgid": 10,
                            "identity": "linux:10",
                            "strong_identity": "linux-strong:10",
                            "cpu_ns": 100,
                            "rss_bytes": 1_000,
                        }
                    }

            interrupted_output = temp_path / "interrupted.json"
            old_sigint = signal.getsignal(signal.SIGINT)
            old_sigterm = signal.getsignal(signal.SIGTERM)
            try:
                with mock.patch.object(
                    RESOURCE_SAMPLER,
                    "select_backend",
                    return_value=InterruptedBackend(),
                ), mock.patch.object(
                    RESOURCE_SAMPLER,
                    "online_cpu_count",
                    return_value=4,
                ):
                    RESOURCE_SAMPLER.sample_segment(
                        argparse.Namespace(
                            output=interrupted_output,
                            run_id="early-stop-run",
                            generation=1,
                            root_pid=10,
                            root_identity="linux:10",
                            process_group=10,
                            database_path=database,
                            database_identity=(
                                database_stat.st_dev,
                                database_stat.st_ino,
                            ),
                            interval_ms=100,
                            proc_root=Path("/unused"),
                        )
                    )
            finally:
                signal.signal(signal.SIGINT, old_sigint)
                signal.signal(signal.SIGTERM, old_sigterm)
            interrupted = RESOURCE_SAMPLER.validate_segment(
                interrupted_output
            )
            self.assertEqual(interrupted["status"], "partial")
            self.assertIn(
                "sampler_stopped_before_root_exit",
                {warning["code"] for warning in interrupted["warnings"]},
            )

    def test_resource_aggregate_fails_closed_on_incomplete_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            database = temp_path / "database"
            database.mkdir()
            first, origin = self.make_resource_segment(database)
            first_path = temp_path / "first.json"
            RESOURCE_SAMPLER.atomic_write_json(first_path, first)
            RESOURCE_SAMPLER.validate_segment(first_path)
            timeline = temp_path / "timeline.state"
            timeline.write_text(
                "schema_version=1\n"
                "kind=final2026_rank_timeline\n"
                f"origin_unix_ns={origin}\n"
                "warmup_ns=0\n"
                "measurement_windows=3\n"
                "measurement_window_ns=1000000000\n",
                encoding="ascii",
            )
            completion = temp_path / "rank-complete.json"
            RESOURCE_SAMPLER.complete_rank(
                argparse.Namespace(
                    timeline=timeline,
                    run_id="resource-run",
                    output=completion,
                )
            )
            output = temp_path / "metrics.json"

            def aggregate(
                paths,
                expected,
                identity=(7, 9),
                expected_window_seconds=1,
            ):
                RESOURCE_SAMPLER.aggregate_segments(
                    argparse.Namespace(
                        segment=paths,
                        run_id="resource-run",
                        expected_generations=expected,
                        database_path=database,
                        database_identity=identity,
                        timeline=timeline,
                        rank_complete=completion,
                        expected_warmup_seconds=0,
                        expected_window_seconds=expected_window_seconds,
                        mode="all",
                        output=output,
                        interval_ms=1000,
                    )
                )
                return json.loads(output.read_text(encoding="utf-8"))

            baseline = aggregate([first_path], 1)
            self.assertEqual(baseline["status"], "available")
            self.assertEqual(
                baseline["rank_cpu"]["combined"][
                    "average_single_core_percent"
                ],
                100.0,
            )
            mismatched_schedule = aggregate(
                [first_path],
                1,
                expected_window_seconds=2,
            )
            self.assertEqual(
                mismatched_schedule["rank_cpu"]["status"],
                "unavailable",
            )

            self.assertEqual(aggregate([first_path], 2)["status"], "partial")
            duplicate = aggregate([first_path, first_path], 2)
            self.assertEqual(duplicate["status"], "partial")
            self.assertIn(
                "duplicate_resource_segment",
                {warning["code"] for warning in duplicate["warnings"]},
            )

            second, _ = self.make_resource_segment(
                database,
                generation=2,
                shift=10_000_000_000,
            )
            second["status"] = "unavailable"
            second_path = temp_path / "second.json"
            RESOURCE_SAMPLER.atomic_write_json(second_path, second)
            self.assertEqual(
                aggregate([first_path, second_path], 2)["status"],
                "partial",
            )

            overlapping, _ = self.make_resource_segment(
                database,
                generation=2,
                shift=100_000_000,
            )
            overlapping_path = temp_path / "overlapping.json"
            RESOURCE_SAMPLER.atomic_write_json(overlapping_path, overlapping)
            overlap_result = aggregate([first_path, overlapping_path], 2)
            self.assertEqual(overlap_result["status"], "partial")
            self.assertEqual(
                overlap_result["rank_cpu"]["status"],
                "unavailable",
            )

            self.assertNotEqual(
                aggregate([first_path], 1, identity=(7, 10))["status"],
                "available",
            )
            unknown = deepcopy(first)
            unknown["unexpected"] = True
            unknown_path = temp_path / "unknown.json"
            RESOURCE_SAMPLER.atomic_write_json(unknown_path, unknown)
            with self.assertRaises(RESOURCE_SAMPLER.ResourceError):
                RESOURCE_SAMPLER.validate_segment(unknown_path)

            gap = deepcopy(first)
            gap["status"] = "partial"
            gap["warnings"] = [
                {
                    "code": "process_sample_gap_exceeded",
                    "detail": "2000000000",
                }
            ]
            gap_path = temp_path / "gap.json"
            RESOURCE_SAMPLER.atomic_write_json(gap_path, gap)
            self.assertEqual(aggregate([gap_path], 1)["status"], "partial")

            stale = json.loads(completion.read_text(encoding="utf-8"))
            stale["run_id"] = "stale-run"
            RESOURCE_SAMPLER.atomic_write_json(completion, stale)
            self.assertEqual(
                aggregate([first_path], 1)["rank_cpu"]["status"],
                "unavailable",
            )

    def test_resource_monitor_refuses_a_reused_pid_identity(self):
        script_text = SCRIPT.read_text(encoding="utf-8")
        monotonic_start = script_text.index("monotonic_millis() {")
        monotonic_end = script_text.index(
            "\n}\n\nprocess_identity() {",
            monotonic_start,
        ) + 3
        helper_start = script_text.index(
            "resource_monitor_process_helper() {"
        )
        helper_end = script_text.index(
            "\n}\n\nresource_monitor_job_running() {",
            helper_start,
        ) + 3
        with tempfile.TemporaryDirectory() as temp:
            harness = self.make_executable(
                Path(temp) / "resource-identity-test",
                "set -euo pipefail\n"
                + script_text[monotonic_start:monotonic_end]
                + "\n"
                + script_text[helper_start:helper_end]
                + """
python3 -c 'import time; time.sleep(30)' &
victim_pid=$!
trap 'kill -KILL "${victim_pid}" 2>/dev/null || true' EXIT
identity="$(
  resource_monitor_process_helper capture "${victim_pid}" "" "$$"
)"
if resource_monitor_process_helper signal "${victim_pid}" \
    "stale:${identity}" "$$" TERM; then
  exit 91
fi
kill -0 "${victim_pid}"
resource_monitor_process_helper signal "${victim_pid}" \
  "${identity}" "$$" TERM
wait "${victim_pid}" 2>/dev/null || true
trap - EXIT
""",
            )
            result = subprocess.run(
                ["bash", str(harness)],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=5,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(
        sys.platform == "darwin",
        "Darwin ps uses -E to expose a process environment",
    )
    def test_darwin_cleanup_owner_probe_reads_process_environment(self):
        token = f"workflow-owner-test-{os.getpid()}"
        process = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            env={
                **os.environ,
                "RMDB_WORKFLOW_PROCESS_OWNER": token,
            },
        )
        try:
            result = subprocess.run(
                [
                    "ps",
                    "-E",
                    "-ww",
                    "-o",
                    "command=",
                    "-p",
                    str(process.pid),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=2,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(
                f"RMDB_WORKFLOW_PROCESS_OWNER={token}",
                result.stdout.split(),
            )
            script_text = SCRIPT.read_text(encoding="utf-8")
            self.assertIn(
                '["ps", "-E", "-ww", "-o", "command=", "-p", str(pid)]',
                script_text,
            )
            self.assertNotIn(
                '["ps", "eww", "-o", "command=", "-p", str(pid)]',
                script_text,
            )
        finally:
            process.terminate()
            process.wait(timeout=2)

    def test_cleanup_identity_proof_brackets_owner_and_group_checks(self):
        script_text = SCRIPT.read_text(encoding="utf-8")
        function_start = script_text.index("establish_cleanup_identity() {")
        function_end = script_text.index(
            "\n}\n\nserver_process_helper() {",
            function_start,
        )
        function_text = script_text[function_start:function_end]
        first_identity = function_text.index(
            'cleanup_identity_before="$(\n'
            '    process_identity "${pid}" "${cleanup_deadline}"'
        )
        owner_proof = function_text.index(
            'process_owner_matches "${pid}" "${cleanup_deadline}"'
        )
        group_proof = function_text.index(
            'cleanup_pgid="$(python3 - "${pid}"'
        )
        second_identity = function_text.index(
            'cleanup_identity_after="$(\n'
            '    process_identity "${pid}" "${cleanup_deadline}"'
        )
        equality_proof = function_text.index(
            '[[ "${cleanup_identity_before}" == '
            '"${cleanup_identity_after}" ]]'
        )
        self.assertLess(first_identity, owner_proof)
        self.assertLess(owner_proof, group_proof)
        self.assertLess(group_proof, second_identity)
        self.assertLess(second_identity, equality_proof)

    def test_registration_timeout_kills_stalled_pre_group_launcher(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, tester, _, _, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            stalled_pid = temp_path / "stalled-launcher.pid"
            fake_tools = temp_path / "fake-tools"
            fake_tools.mkdir()
            self.make_executable(
                fake_tools / "python3",
                """
if [[ "${1-}" == "-c" \
  && "${2-}" == *"os.setpgid(0, 0)"* ]]; then
  exec "${REAL_WORKFLOW_PYTHON}" -c \
    'import os
from pathlib import Path
import time
Path(os.environ["STALLED_LAUNCHER_PID"]).write_text(
    str(os.getpid()), encoding="utf-8"
)
time.sleep(60)'
fi
exec "${REAL_WORKFLOW_PYTHON}" "$@"
""",
            )
            env["PATH"] = f"{fake_tools}{os.pathsep}{env['PATH']}"
            env["REAL_WORKFLOW_PYTHON"] = sys.executable
            env["STALLED_LAUNCHER_PID"] = str(stalled_pid)

            started = time.monotonic()
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                temp_path / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                "--allow-deviation",
                "--ready-timeout-seconds",
                "1",
                env=env,
            )
            elapsed = time.monotonic() - started

            self.assertNotEqual(result.returncode, 0)
            self.assertIn(
                "process registration exceeded the shared readiness budget",
                result.stderr,
            )
            self.assertTrue(self.wait_for_path(stalled_pid), result.stderr)
            self.assert_pid_gone(
                int(stalled_pid.read_text(encoding="utf-8"))
            )
            listener_check = socket.socket()
            try:
                listener_check.bind(("127.0.0.1", port))
            finally:
                listener_check.close()
            self.assertLess(elapsed, 6.0)

    def test_diagnostic_metrics_collect_delta_and_parse_strace(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            proc_root = temp_path / "proc"
            pid = 4242
            process_root = proc_root / str(pid)
            process_root.mkdir(parents=True)

            def write_proc(
                read_bytes,
                write_bytes,
                cancelled_write_bytes,
                minflt,
                voluntary_switches,
            ):
                (process_root / "io").write_text(
                    "\n".join(
                        (
                            "rchar: 100",
                            "wchar: 200",
                            "syscr: 10",
                            "syscw: 20",
                            f"read_bytes: {read_bytes}",
                            f"write_bytes: {write_bytes}",
                            f"cancelled_write_bytes: {cancelled_write_bytes}",
                        )
                    )
                    + "\n",
                    encoding="utf-8",
                )
                fields = ["S"] + ["0"] * 29
                fields[7] = str(minflt)
                fields[8] = "2"
                fields[9] = "3"
                fields[10] = "4"
                fields[11] = "5"
                fields[12] = "6"
                fields[13] = "7"
                fields[14] = "8"
                fields[17] = "4"
                fields[19] = "999"
                (process_root / "stat").write_text(
                    f"{pid} (fake rmdb worker) {' '.join(fields)}\n",
                    encoding="utf-8",
                )
                (process_root / "status").write_text(
                    "\n".join(
                        (
                            "Name:\tfake-rmdb",
                            f"voluntary_ctxt_switches:\t{voluntary_switches}",
                            "nonvoluntary_ctxt_switches:\t12",
                        )
                    )
                    + "\n",
                    encoding="utf-8",
                )

            before = temp_path / "before.json"
            after = temp_path / "after.json"
            delta = temp_path / "delta.json"
            write_proc(1000, 2000, 4, 11, 20)
            captured_before = subprocess.run(
                [
                    "python3",
                    str(METRICS_HELPER),
                    "capture",
                    "--pid",
                    str(pid),
                    "--proc-root",
                    str(proc_root),
                    "--output",
                    str(before),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertEqual(captured_before.returncode, 0, captured_before.stderr)

            write_proc(1128, 2512, 3, 18, 27)
            captured_after = subprocess.run(
                [
                    "python3",
                    str(METRICS_HELPER),
                    "capture",
                    "--pid",
                    str(pid),
                    "--proc-root",
                    str(proc_root),
                    "--output",
                    str(after),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertEqual(captured_after.returncode, 0, captured_after.stderr)
            calculated = subprocess.run(
                [
                    "python3",
                    str(METRICS_HELPER),
                    "delta",
                    "--before",
                    str(before),
                    "--after",
                    str(after),
                    "--output",
                    str(delta),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertEqual(calculated.returncode, 0, calculated.stderr)
            delta_payload = json.loads(delta.read_text(encoding="utf-8"))
            self.assertEqual(delta_payload["status"], "available")
            self.assertEqual(delta_payload["metrics"]["io"]["read_bytes"], 128)
            self.assertEqual(delta_payload["metrics"]["io"]["write_bytes"], 512)
            self.assertEqual(delta_payload["metrics"]["stat"]["minflt"], 7)
            self.assertEqual(
                delta_payload["metrics"]["status"]["voluntary_ctxt_switches"],
                7,
            )
            self.assertEqual(
                delta_payload["metrics"]["io"]["cancelled_write_bytes"],
                0,
            )
            self.assertIn(
                "io.cancelled_write_bytes",
                delta_payload["decreased_counters"],
            )

            unavailable = temp_path / "unavailable.json"
            missing_proc = subprocess.run(
                [
                    "python3",
                    str(METRICS_HELPER),
                    "capture",
                    "--pid",
                    str(pid),
                    "--proc-root",
                    str(temp_path / "no-proc"),
                    "--output",
                    str(unavailable),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertEqual(missing_proc.returncode, 0, missing_proc.stderr)
            self.assertEqual(
                json.loads(unavailable.read_text(encoding="utf-8"))["status"],
                "unavailable",
            )
            required_proc = subprocess.run(
                [
                    "python3",
                    str(METRICS_HELPER),
                    "capture",
                    "--pid",
                    str(pid),
                    "--proc-root",
                    str(temp_path / "no-proc"),
                    "--output",
                    str(unavailable),
                    "--require-available",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertNotEqual(required_proc.returncode, 0)
            self.assertEqual(
                json.loads(unavailable.read_text(encoding="utf-8"))["status"],
                "unavailable",
            )

            summary = temp_path / "strace.txt"
            metrics = temp_path / "strace.json"
            summary.write_text(
                """% time     seconds  usecs/call     calls    errors syscall
 35.00    0.035000          35      100           pread64
 25.00    0.025000          25       40         2 pwrite64
 15.00    0.015000          15       10           openat
 10.00    0.010000          10       10           close
 10.00    0.010000          10        5           fdatasync
  5.00    0.005000           5        2         1 fallocate
------ ----------- ----------- --------- --------- ----------------
100.00    0.100000                   167         3 total
""",
                encoding="utf-8",
            )
            parsed = subprocess.run(
                [
                    "python3",
                    str(METRICS_HELPER),
                    "strace",
                    "--input",
                    str(summary),
                    "--output",
                    str(metrics),
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            self.assertEqual(parsed.returncode, 0, parsed.stderr)
            strace_payload = json.loads(metrics.read_text(encoding="utf-8"))
            self.assertEqual(strace_payload["status"], "available")
            self.assertEqual(strace_payload["derived"]["read"]["calls"], 100)
            self.assertEqual(strace_payload["derived"]["write"]["calls"], 40)
            self.assertEqual(
                strace_payload["derived"]["open_close"]["calls"],
                20,
            )
            self.assertEqual(
                strace_payload["derived"]["truncate_allocate"]["errors"],
                1,
            )
            self.assertEqual(strace_payload["derived"]["sync"]["calls"], 5)

    def test_default_root_is_three_levels_above_workflow(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            workflow = (
                root / "deps" / "TPCC-Tester" / "perf_workflow"
            )
            workflow.mkdir()
            copied_script = workflow / "run_workflow.sh"
            shutil.copy2(SCRIPT, copied_script)

            result = self.run_script("--plan-only", script=copied_script)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn(f"rmdb_dir={root.resolve()}\n", result.stdout)

    def test_rejects_dangerous_database_names(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            for unsafe_name in ("", ".", "..", "/", "a/b", "a b", "*"):
                with self.subTest(name=unsafe_name):
                    result = self.run_script(
                        "--plan-only",
                        "--target-dir",
                        root,
                        "--db-name",
                        unsafe_name,
                    )
                    self.assertNotEqual(result.returncode, 0)

    def test_diagnostics_requires_full_lifecycle(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            for mode in ("init", "rank", "recovery", "tools"):
                with self.subTest(mode=mode):
                    result = self.run_script(
                        "--plan-only",
                        "--mode",
                        mode,
                        "--diagnostics",
                        "--target-dir",
                        root,
                    )
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("requires --mode all", result.stderr)

    def test_readiness_deviation_requires_explicit_opt_in(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            rejected = self.run_script(
                "--plan-only",
                "--target-dir",
                root,
                "--ready-timeout-seconds",
                "5",
            )
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("require --allow-deviation", rejected.stderr)

            accepted = self.run_script(
                "--plan-only",
                "--target-dir",
                root,
                "--ready-timeout-seconds",
                "5",
                "--allow-deviation",
            )
            self.assertEqual(accepted.returncode, 0, accepted.stderr)
            self.assertIn("conformance=non_ranked_deviation", accepted.stdout)
            self.assertIn("ranked_configuration=0", accepted.stdout)
            self.assertIn(
                "recovery_ready_budget_seconds=5\n",
                accepted.stdout,
            )

    def test_non_ranked_all_does_not_run_fixed_final_diagnostics(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            result = self.run_script(
                "--plan-only",
                "--mode",
                "all",
                "--target-dir",
                root,
                "--allow-deviation",
                "--scale",
                "1",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("diagnostics_requested=0\n", result.stdout)
            self.assertIn(
                "diagnostics_phase=not_applicable_non_ranked\n",
                result.stdout,
            )

    def test_scale_only_all_skips_diagnostics_and_uses_sigkill_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            (
                server,
                tester,
                calls,
                server_events,
                env,
                port,
            ) = self.make_lifecycle_fakes(temp_path, root)
            records = temp_path / "records"
            result = self.run_script(
                "--mode",
                "all",
                "--target-dir",
                root,
                "--record-root",
                records,
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                "--allow-deviation",
                "--scale",
                "1",
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            invocations = [
                line.split("\t")
                for line in calls.read_text(encoding="utf-8").splitlines()
            ]
            self.assertFalse(
                any(
                    "--diagnostic-workload-seconds" in args
                    for args in invocations
                )
            )
            result_dir = next(records.iterdir())
            manifest = json.loads(
                (result_dir / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                manifest["phases"]["diagnostics"],
                "not_applicable_non_ranked",
            )
            events = server_events.read_text(encoding="utf-8").splitlines()
            starts = [line.split() for line in events if line.startswith("start ")]
            graceful = [
                line.split() for line in events if line.startswith("graceful ")
            ]
            self.assertEqual(len(starts), 2, events)
            self.assertEqual([row[2] for row in starts], ["0", "1"])
            self.assertEqual(
                [row[1] for row in graceful],
                [starts[1][1]],
                "the pre-recovery server must die by SIGKILL, not graceful stop",
            )
            for row in starts:
                self.assert_pid_gone(int(row[1]))
            rebound = socket.socket()
            try:
                rebound.bind(("127.0.0.1", port))
            finally:
                rebound.close()

    def test_custom_recovery_budget_reaches_every_stateful_tester_call(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, tester, calls, _, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                temp_path / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                "--allow-deviation",
                "--ready-timeout-seconds",
                "5",
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            invocations = [
                line.split("\t")
                for line in calls.read_text(encoding="utf-8").splitlines()
            ]
            stateful = [
                args for args in invocations if "--probe-ready" not in args
            ]
            self.assertGreaterEqual(len(stateful), 1)
            self.assertTrue(
                all(
                    args[
                        args.index("--recovery-ready-budget-seconds") + 1
                    ]
                    == "5"
                    for args in stateful
                )
            )

    def test_existing_database_modes_reuse_dataset_run_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            state = Path(temp) / "state"
            state.mkdir()
            (state / "dataset.state").write_text(
                "version=2\nrun_id=original.dataset:42\n",
                encoding="utf-8",
            )

            for mode in ("rank", "recovery"):
                with self.subTest(mode=mode):
                    result = self.run_script(
                        "--plan-only",
                        "--mode",
                        mode,
                        "--target-dir",
                        root,
                        "--state-dir",
                        state,
                    )
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(
                        "dataset_run_id=original.dataset:42\n",
                        result.stdout,
                    )

    def test_tools_mode_preserves_existing_database_and_source_csv(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            database_file = root / "existing_db" / "keep.data"
            source_csv = (
                root
                / "src"
                / "test"
                / "performance_test"
                / "table_data"
                / "tracked.csv"
            )
            database_file.parent.mkdir()
            source_csv.parent.mkdir(parents=True)
            database_file.write_text("database", encoding="utf-8")
            source_csv.write_text("tracked csv", encoding="utf-8")
            minimal_env = os.environ.copy()
            minimal_env["PATH"] = os.defpath

            result = self.run_script(
                "--mode",
                "tools",
                "--target-dir",
                root,
                "--db-name",
                "existing_db",
                "--record-root",
                Path(temp) / "records",
                env=minimal_env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(database_file.read_text(), "database")
            self.assertEqual(source_csv.read_text(), "tracked csv")
            result_dir = next((Path(temp) / "records").iterdir())
            manifest = json.loads(
                (result_dir / "manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                manifest["resources"]["status"],
                "not_applicable",
            )
            self.assertEqual(
                manifest["resources"]["artifact"]["status"],
                "missing",
            )
            self.assertFalse((result_dir / "resource_metrics.json").exists())

    def test_resource_helper_failure_is_warning_only(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, tester, _, _, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            broken_helper = self.make_python_executable(
                temp_path / "broken-resource-helper",
                "raise SystemExit(19)\n",
            )
            env["RMDB_TPCC_RESOURCE_HELPER_OVERRIDE"] = str(broken_helper)
            records = temp_path / "records"
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                records,
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("resource", result.stderr.lower())
            manifest = json.loads(
                (next(records.iterdir()) / "manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(manifest["status"], "success")
            self.assertEqual(manifest["phases"]["setup"], "passed")
            self.assertEqual(manifest["resources"]["status"], "failed")
            self.assertFalse(manifest["resources"]["ranked"])

    def test_rank_cpu_uses_the_published_three_window_timeline(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            database = root / "tpcc_final2026"
            database.mkdir()
            (database / "page").write_bytes(b"x" * 8_192)
            state = temp_path / "state"
            state.mkdir()
            (state / "dataset.state").write_text(
                "version=2\nrun_id=resource.timeline.test\n",
                encoding="utf-8",
            )
            server = self.make_python_executable(
                temp_path / "busy-rmdb",
                """
import os
import signal
import socket
import sys

listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("0.0.0.0", int(os.environ["RMDB_PORT"])))
listener.listen(4)
running = True
def stop(_signum, _frame):
    global running
    running = False
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
value = 0
while running:
    value = (value + 1) % 1000003
listener.close()
""",
            )
            tester = self.make_python_executable(
                temp_path / "timeline-tpcc",
                """
import os
from pathlib import Path
import sys
import time

if "--probe-ready" in sys.argv:
    time.sleep(0.2)
if "--benchmark" in sys.argv:
    time.sleep(0.2)
    output = Path(os.environ["RMDB_TPCC_RESOURCE_TIMELINE_FILE"])
    origin = time.time_ns()
    output.write_text(
        "schema_version=1\\n"
        "kind=final2026_rank_timeline\\n"
        f"origin_unix_ns={origin}\\n"
        "warmup_ns=0\\n"
        "measurement_windows=3\\n"
        "measurement_window_ns=1000000000\\n",
        encoding="ascii",
    )
    time.sleep(3.4)
""",
            )
            records = temp_path / "records"
            result = self.run_script(
                "--mode",
                "rank",
                "--target-dir",
                root,
                "--record-root",
                records,
                "--state-dir",
                state,
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                self.reserve_port(),
                "--allow-deviation",
                "--warmup-seconds",
                "0",
                "--window-seconds",
                "1",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            result_dir = next(records.iterdir())
            metrics = json.loads(
                (result_dir / "resource_metrics.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(metrics["status"], "available")
            self.assertEqual(metrics["rank_cpu"]["status"], "available")
            self.assertEqual(len(metrics["rank_cpu"]["windows"]), 3)
            self.assertTrue(
                all(
                    window["coverage_ratio"] >= 0.999
                    for window in metrics["rank_cpu"]["windows"]
                )
            )
            self.assertGreater(
                metrics["rank_cpu"]["combined"][
                    "average_single_core_percent"
                ],
                1.0,
            )
            self.assertGreater(
                metrics["rank_cpu"]["combined"]["peak_host_percent"],
                0.0,
            )

    def test_refuses_to_adopt_symlinked_workflow_state_root(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            victim = Path(temp) / "victim"
            victim.mkdir()
            sentinel = victim / "sentinel"
            sentinel.write_text("keep", encoding="utf-8")
            os.symlink(victim, root / ".tpcc-workflow")

            result = self.run_script(
                "--mode",
                "tools",
                "--target-dir",
                root,
                "--record-root",
                Path(temp) / "records",
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("not a safe directory", result.stderr)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep")

    def test_occupied_port_is_reported_without_killing_owner(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            invoked = Path(temp) / "server-invoked"
            server = self.make_executable(
                Path(temp) / "fake-rmdb",
                f"touch {invoked!s}\nexit 99\n",
            )
            tester = self.make_executable(
                Path(temp) / "fake-tpcc",
                "exit 0\n",
            )
            listener = socket.socket()
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            port = listener.getsockname()[1]
            try:
                result = self.run_script(
                    "--mode",
                    "init",
                    "--target-dir",
                    root,
                    "--record-root",
                    Path(temp) / "records",
                    "--skip-build",
                    "--server-bin",
                    server,
                    "--tpcc-bin",
                    tester,
                    "--port",
                    port,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("will not kill its owner", result.stderr)
                self.assertEqual(listener.getsockname()[1], port)
                self.assertFalse(invoked.exists())
            finally:
                listener.close()

    def test_readiness_probe_can_use_more_than_two_seconds_of_shared_budget(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, _, _, _, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            tester = self.make_python_executable(
                temp_path / "three-second-tpcc",
                """
import sys
import time

if "--probe-ready" in sys.argv:
    time.sleep(3)
""",
            )
            started = time.monotonic()
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                temp_path / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                "--allow-deviation",
                "--ready-timeout-seconds",
                "5",
                env=env,
            )
            elapsed = time.monotonic() - started
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertGreaterEqual(elapsed, 3.0)
            self.assertLess(elapsed, 5.0)

    def test_readiness_probe_is_bounded_by_shared_absolute_deadline(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            server = self.make_python_executable(
                Path(temp) / "fake-rmdb",
                """
import os
import signal
import socket
import sys
import time

os.makedirs(sys.argv[1], exist_ok=True)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(os.environ["RMDB_PORT"])))
listener.listen(4)
running = True
def stop(_signum, _frame):
    global running
    running = False
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
while running:
    time.sleep(0.02)
listener.close()
""",
            )
            tester = self.make_executable(
                Path(temp) / "slow-tpcc",
                """
if [[ " $* " == *" --probe-ready "* ]]; then
  sleep 5
fi
exit 0
""",
            )
            reservation = socket.socket()
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
            reservation.close()

            started = time.monotonic()
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                Path(temp) / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                "--allow-deviation",
                "--ready-timeout-seconds",
                "1",
            )
            elapsed = time.monotonic() - started
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("within 1s", result.stderr)
            self.assertLess(
                elapsed,
                3.0,
                "readiness probe exceeded the shared absolute deadline",
            )

    def test_probe_response_timeout_can_retry_before_shared_deadline(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, _, calls, _, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            probe_count = temp_path / "probe-count"
            tester = self.make_python_executable(
                temp_path / "retry-tpcc",
                """
import os
from pathlib import Path
import sys
import time

with open(os.environ["FAKE_TPCC_CALLS"], "a", encoding="utf-8") as output:
    output.write("\\t".join(sys.argv[1:]) + "\\n")
if "--probe-ready" in sys.argv:
    counter = Path(os.environ["FAKE_PROBE_COUNT"])
    count = int(counter.read_text(encoding="utf-8")) if counter.exists() else 0
    counter.write_text(str(count + 1), encoding="utf-8")
    if count == 0:
        time.sleep(0.25)
        raise SystemExit(124)
""",
            )
            env["FAKE_PROBE_COUNT"] = str(probe_count)
            started = time.monotonic()
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                temp_path / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                "--allow-deviation",
                "--ready-timeout-seconds",
                "5",
                env=env,
            )
            elapsed = time.monotonic() - started
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertGreaterEqual(
                int(probe_count.read_text(encoding="utf-8")),
                2,
            )
            self.assertLess(elapsed, 5.0)
            invocations = [
                line.split("\t")
                for line in calls.read_text(encoding="utf-8").splitlines()
            ]
            probes = [
                args for args in invocations if "--probe-ready" in args
            ]
            self.assertGreaterEqual(len(probes), 2)

    def test_hung_probe_exec_path_respects_deadline_and_leaves_no_processes(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, _, _, _, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            probe_pid_file = temp_path / "hung-exec-probe.pid"
            child_pid_file = temp_path / "hung-exec-child.pid"
            tester = self.make_python_executable(
                temp_path / "hung-exec-tpcc",
                """
import os
from pathlib import Path
import subprocess
import sys
import time

Path(os.environ["HUNG_EXEC_PROBE_PID"]).write_text(
    str(os.getpid()),
    encoding="utf-8",
)
child = subprocess.Popen(
    [sys.executable, "-c", "import time; time.sleep(60)"]
)
Path(os.environ["HUNG_EXEC_CHILD_PID"]).write_text(
    str(child.pid),
    encoding="utf-8",
)
time.sleep(60)
""",
            )
            env["HUNG_EXEC_PROBE_PID"] = str(probe_pid_file)
            env["HUNG_EXEC_CHILD_PID"] = str(child_pid_file)

            started = time.monotonic()
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                temp_path / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                "--allow-deviation",
                "--ready-timeout-seconds",
                "3",
                env=env,
            )
            elapsed = time.monotonic() - started

            self.assertNotEqual(result.returncode, 0)
            self.assertIn("within 3s", result.stderr)
            self.assertTrue(self.wait_for_path(probe_pid_file), result.stderr)
            self.assertTrue(self.wait_for_path(child_pid_file), result.stderr)
            self.assert_pid_gone(
                int(probe_pid_file.read_text(encoding="utf-8"))
            )
            self.assert_pid_gone(
                int(child_pid_file.read_text(encoding="utf-8"))
            )
            listener_check = socket.socket()
            try:
                listener_check.bind(("127.0.0.1", port))
            finally:
                listener_check.close()
            self.assertLess(elapsed, 5.0)

    @unittest.skipIf(
        Path("/proc/self/stat").is_file(),
        "lsof readiness backend is used on macOS, not Linux",
    )
    def test_hung_lsof_cannot_exceed_readiness_deadline(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, tester, _, _, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            fake_tools = temp_path / "fake-tools"
            fake_tools.mkdir()
            self.make_executable(fake_tools / "lsof", "sleep 30\n")
            env["PATH"] = f"{fake_tools}{os.pathsep}{env['PATH']}"
            started = time.monotonic()
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                temp_path / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--port",
                port,
                "--allow-deviation",
                "--ready-timeout-seconds",
                "1",
                env=env,
            )
            elapsed = time.monotonic() - started
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("inspection exceeded readiness deadline", result.stderr)
            self.assertLess(elapsed, 3.0)

    def test_term_during_hung_probe_cleans_probe_server_and_port(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, _, _, server_events, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            probe_pid_file = temp_path / "probe-pid"
            tester = self.make_python_executable(
                temp_path / "hung-tpcc",
                """
import os
from pathlib import Path
import sys
import time

if "--probe-ready" in sys.argv:
    Path(os.environ["FAKE_PROBE_PID"]).write_text(
        str(os.getpid()),
        encoding="utf-8",
    )
    time.sleep(60)
""",
            )
            env["FAKE_PROBE_PID"] = str(probe_pid_file)
            command = [
                "bash",
                str(SCRIPT),
                "--mode",
                "init",
                "--target-dir",
                str(root),
                "--record-root",
                str(temp_path / "records"),
                "--skip-build",
                "--server-bin",
                str(server),
                "--tpcc-bin",
                str(tester),
                "--port",
                str(port),
            ]
            process = subprocess.Popen(
                command,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env,
                start_new_session=True,
            )
            try:
                self.assertTrue(self.wait_for_path(probe_pid_file))
                server_pid = int(
                    server_events.read_text(encoding="utf-8").split()[1]
                )
                probe_pid = int(probe_pid_file.read_text(encoding="utf-8"))
                started = time.monotonic()
                process.terminate()
                stdout, stderr = process.communicate(timeout=4)
                self.assertLess(time.monotonic() - started, 4.0)
                self.assertNotEqual(
                    process.returncode,
                    0,
                    (stdout, stderr),
                )
                self.assert_pid_gone(probe_pid)
                self.assert_pid_gone(server_pid)
                rebound = socket.socket()
                try:
                    rebound.bind(("127.0.0.1", port))
                finally:
                    rebound.close()
            finally:
                if process.poll() is None:
                    self.kill_process_session(process.pid)
                    process.communicate()

    def test_foreign_racing_listener_is_rejected_and_not_killed(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            start_marker = temp_path / "server-started"
            listener_ready = temp_path / "foreign-listener-ready"
            server = self.make_python_executable(
                temp_path / "fake-rmdb",
                """
import os
import signal
import sys
import time

os.makedirs(sys.argv[1], exist_ok=True)
open(os.environ["FAKE_START_MARKER"], "w", encoding="utf-8").close()
running = True
def stop(_signum, _frame):
    global running
    running = False
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
while running:
    time.sleep(0.02)
""",
            )
            tester = self.make_executable(
                temp_path / "fake-tpcc",
                "exit 0\n",
            )
            reservation = socket.socket()
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
            reservation.close()
            foreign = subprocess.Popen(
                [
                    "python3",
                    "-c",
                    """
import os
from pathlib import Path
import signal
import socket
import sys
import time

marker, ready, port = Path(sys.argv[1]), Path(sys.argv[2]), int(sys.argv[3])
while not marker.exists():
    time.sleep(0.01)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", port))
listener.listen(4)
ready.write_text(str(os.getpid()), encoding="utf-8")
running = True
def stop(_signum, _frame):
    global running
    running = False
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
while running:
    time.sleep(0.02)
listener.close()
""",
                    str(start_marker),
                    str(listener_ready),
                    str(port),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            env = os.environ.copy()
            env["FAKE_START_MARKER"] = str(start_marker)
            try:
                result = self.run_script(
                    "--mode",
                    "init",
                    "--target-dir",
                    root,
                    "--record-root",
                    temp_path / "records",
                    "--skip-build",
                    "--server-bin",
                    server,
                    "--tpcc-bin",
                    tester,
                    "--port",
                    port,
                    "--allow-deviation",
                    "--ready-timeout-seconds",
                    "1",
                    env=env,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(listener_ready.exists(), result.stderr)
                self.assertIn(
                    "outside the registered RMDB process tree",
                    result.stderr,
                )
                self.assertIsNone(
                    foreign.poll(),
                    "workflow killed a listener outside its registered tree",
                )
            finally:
                if foreign.poll() is None:
                    foreign.terminate()
                foreign.wait(timeout=3)

    def test_listener_owner_check_ignores_nonmatching_ipv6_address(self):
        if not socket.has_ipv6:
            self.skipTest("IPv6 is unavailable")
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, tester, _, _, env, _ = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            foreign = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            try:
                foreign.setsockopt(
                    socket.IPPROTO_IPV6,
                    socket.IPV6_V6ONLY,
                    1,
                )
                foreign.bind(("::1", 0))
                foreign.listen(2)
                port = foreign.getsockname()[1]
                ipv4_probe = socket.socket()
                try:
                    ipv4_probe.bind(("127.0.0.1", port))
                except OSError:
                    self.skipTest("IPv4 and IPv6 listeners cannot coexist")
                finally:
                    ipv4_probe.close()

                result = self.run_script(
                    "--mode",
                    "init",
                    "--target-dir",
                    root,
                    "--record-root",
                    temp_path / "records",
                    "--skip-build",
                    "--server-bin",
                    server,
                    "--tpcc-bin",
                    tester,
                    "--port",
                    port,
                    env=env,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(foreign.getsockname()[1], port)
            finally:
                foreign.close()

    def test_ipv4_wildcard_listener_belongs_to_ipv4_loopback_host(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            server, tester, _, _, env, port = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                temp_path / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--host",
                "127.0.0.1",
                "--port",
                port,
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)

    def test_owner_check_ignores_foreign_same_family_address(self):
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            _, tester, _, _, env, _ = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            server = self.make_python_executable(
                temp_path / "fake-rmdb-exact",
                """
import os
import signal
import socket
import sys
import time

os.makedirs(sys.argv[1], exist_ok=True)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(os.environ["RMDB_PORT"])))
listener.listen(4)
def stop(_signum, _frame):
    listener.close()
    os._exit(0)
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
while True:
    time.sleep(0.02)
""",
            )
            foreign = socket.socket()
            try:
                try:
                    foreign.bind(("127.0.0.2", 0))
                except OSError:
                    self.skipTest("127.0.0.2 is not configured locally")
                foreign.listen(2)
                port = foreign.getsockname()[1]
                availability = socket.socket()
                try:
                    availability.bind(("127.0.0.1", port))
                except OSError:
                    self.skipTest("distinct IPv4 loopback listeners cannot coexist")
                finally:
                    availability.close()
                result = self.run_script(
                    "--mode",
                    "init",
                    "--target-dir",
                    root,
                    "--record-root",
                    temp_path / "records",
                    "--skip-build",
                    "--server-bin",
                    server,
                    "--tpcc-bin",
                    tester,
                    "--host",
                    "127.0.0.1",
                    "--port",
                    port,
                    env=env,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(foreign.getsockname()[1], port)
            finally:
                foreign.close()

    def test_ipv6_wildcard_listener_belongs_to_ipv6_loopback_host(self):
        if not socket.has_ipv6:
            self.skipTest("IPv6 is unavailable")
        with tempfile.TemporaryDirectory() as temp:
            temp_path = Path(temp)
            root = self.make_root(temp)
            _, tester, _, server_events, env, _ = self.make_lifecycle_fakes(
                temp_path,
                root,
            )
            server = self.make_python_executable(
                temp_path / "fake-rmdb-v6",
                """
import os
import signal
import socket
import sys
import time

os.makedirs(sys.argv[1], exist_ok=True)
listener = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
listener.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("::", int(os.environ["RMDB_PORT"])))
listener.listen(4)
with open(os.environ["FAKE_SERVER_EVENTS"], "a", encoding="utf-8") as output:
    output.write(f"start {os.getpid()} 0\\n")
def stop(signum, _frame):
    with open(
        os.environ["FAKE_SERVER_EVENTS"], "a", encoding="utf-8"
    ) as output:
        output.write(f"graceful {os.getpid()} {signum}\\n")
    listener.close()
    os._exit(0)
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
while True:
    time.sleep(0.02)
""",
            )
            reservation = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            reservation.setsockopt(
                socket.IPPROTO_IPV6,
                socket.IPV6_V6ONLY,
                1,
            )
            reservation.bind(("::1", 0))
            port = reservation.getsockname()[1]
            reservation.close()
            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                temp_path / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--host",
                "::1",
                "--port",
                port,
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            events = server_events.read_text(encoding="utf-8").splitlines()
            self.assertTrue(any(line.startswith("start ") for line in events))
            self.assertTrue(any(line.startswith("graceful ") for line in events))

    def test_stale_cmake_cache_fails_without_deleting_it(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            cache = root / "build-perf" / "CMakeCache.txt"
            cache.parent.mkdir()
            content = "CMAKE_HOME_DIRECTORY:INTERNAL=/different/source\n"
            cache.write_text(content, encoding="utf-8")
            server = self.make_executable(
                Path(temp) / "fake-rmdb",
                "exit 99\n",
            )
            tester = self.make_executable(
                Path(temp) / "fake-tpcc",
                "exit 0\n",
            )

            result = self.run_script(
                "--mode",
                "init",
                "--target-dir",
                root,
                "--record-root",
                Path(temp) / "records",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("stale CMake cache", result.stderr)
            self.assertEqual(cache.read_text(encoding="utf-8"), content)

    def test_all_delegates_exactly_one_rank_run_and_cleans_owned_db(self):
        with tempfile.TemporaryDirectory() as temp:
            root = self.make_root(temp)
            source_csv = (
                root
                / "src"
                / "test"
                / "performance_test"
                / "table_data"
                / "tracked.csv"
            )
            source_csv.parent.mkdir(parents=True)
            source_csv.write_text("tracked csv", encoding="utf-8")
            calls = Path(temp) / "tester-calls"
            server_children = Path(temp) / "server-children"
            server = self.make_python_executable(
                Path(temp) / "fake-rmdb",
                """
import os
import signal
import socket
import subprocess
import sys
import time

os.makedirs(sys.argv[1], exist_ok=True)
listener = socket.socket()
listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
listener.bind(("127.0.0.1", int(os.environ["RMDB_PORT"])))
listener.listen(8)
worker = subprocess.Popen(
    [
        sys.executable,
        "-c",
        "import time\\nwhile True: time.sleep(0.05)",
    ]
)
with open(os.environ["FAKE_SERVER_CHILDREN"], "a", encoding="utf-8") as output:
    output.write(f"{worker.pid}\\n")
running = True
def stop(_signum, _frame):
    global running
    running = False
signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)
try:
    while running:
        time.sleep(0.02)
finally:
    listener.close()
    if worker.poll() is None:
        worker.terminate()
    worker.wait()
""",
            )
            tester = self.make_python_executable(
                Path(temp) / "fake-tpcc",
                """
import os
import sys

with open(os.environ["FAKE_TPCC_CALLS"], "a", encoding="utf-8") as output:
    output.write("\\t".join(sys.argv[1:]) + "\\n")
if "--probe-ready" in sys.argv and not os.path.isdir(os.environ["FAKE_DB_PATH"]):
    raise SystemExit(1)
if "--check-scope" in sys.argv:
    scope = sys.argv[sys.argv.index("--check-scope") + 1]
    if scope == os.environ.get("FAKE_TPCC_FAIL_SCOPE"):
        raise SystemExit(12)
""",
            )
            fake_tools = Path(temp) / "fake-tools"
            fake_tools.mkdir()
            self.make_executable(
                fake_tools / "strace",
                """
output=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) output="$2"; shift 2 ;;
    -p) shift 2 ;;
    *) shift ;;
  esac
done
: >"${output}"
if [[ "${FAKE_STRACE_DIE_EARLY:-0}" == "1" ]]; then
  exit 23
fi
write_summary() {
  if [[ "${FAKE_STRACE_EMPTY:-0}" != "1" ]]; then
    printf '%s\n' \
      '% time     seconds  usecs/call     calls    errors syscall' \
      ' 50.00    0.050000          50       20           pread64' \
      ' 30.00    0.030000          30       10         1 pwrite64' \
      ' 20.00    0.020000          20        5           fdatasync' \
      '------ ----------- ----------- --------- --------- ----------------' \
      '100.00    0.100000                    35         1 total' \
      >"${output}"
  fi
  exit 0
}
trap write_summary INT TERM
while :; do sleep 0.05; done
""",
            )
            env = os.environ.copy()
            env["FAKE_TPCC_CALLS"] = str(calls)
            env["FAKE_DB_PATH"] = str(root / "tpcc_final2026")
            env["FAKE_SERVER_CHILDREN"] = str(server_children)
            env["PATH"] = f"{fake_tools}{os.pathsep}{env['PATH']}"

            def recorded_child_pids():
                if not server_children.exists():
                    return []
                return [
                    int(pid)
                    for pid in server_children.read_text(
                        encoding="utf-8"
                    ).splitlines()
                ]

            def assert_cleanup_since(previous_count):
                new_pids = recorded_child_pids()[previous_count:]
                self.assertGreaterEqual(len(new_pids), 2)
                for child_pid in new_pids:
                    self.assert_pid_gone(child_pid)
                rebound = socket.socket()
                try:
                    rebound.bind(("127.0.0.1", 8765))
                finally:
                    rebound.close()

            result = self.run_script(
                "--mode",
                "all",
                "--target-dir",
                root,
                "--record-root",
                Path(temp) / "records",
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                env=env,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            invocations = [
                line.split("\t")
                for line in calls.read_text(encoding="utf-8").splitlines()
            ]
            ranks = [args for args in invocations if "--benchmark" in args]
            self.assertEqual(
                len(ranks), 1, (invocations, result.stdout, result.stderr)
            )
            self.assertFalse(
                any("--diagnose" in args for args in invocations),
                "10s+60s diagnostics must not reuse compatibility diagnose mode",
            )
            diagnostic_runs = [
                args for args in invocations if "--diagnostic-workload-seconds" in args
            ]
            self.assertEqual(
                [
                    args[args.index("--diagnostic-workload-seconds") + 1]
                    for args in diagnostic_runs
                ],
                ["10", "60"],
            )
            self.assertEqual(
                [
                    args[args.index("--diagnostic-segment") + 1]
                    for args in diagnostic_runs
                ],
                ["warmup", "observation"],
            )
            self.assertIn("--profile", ranks[0])
            self.assertIn("final2026", ranks[0])
            for forbidden in (
                "--scale",
                "--clients",
                "--warmup-seconds",
                "--window-seconds",
            ):
                self.assertNotIn(forbidden, ranks[0])
            scopes = [
                args[args.index("--check-scope") + 1]
                for args in invocations
                if "--check-scope" in args
            ]
            self.assertEqual(scopes, ["setup", "online", "recovery"])
            state_dirs = [
                args[args.index("--state-dir") + 1]
                for args in invocations
                if "--state-dir" in args
            ]
            self.assertGreaterEqual(len(state_dirs), 6)
            self.assertEqual(len(set(state_dirs)), 1)
            probes = [
                args for args in invocations if "--probe-ready" in args
            ]
            self.assertGreaterEqual(len(probes), 2)
            self.assertTrue(
                all(
                    args
                    == [
                        "--probe-ready",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        "8765",
                    ]
                    for args in probes
                )
            )
            stateful_invocations = [
                args for args in invocations if "--probe-ready" not in args
            ]
            self.assertTrue(
                all(
                    args[
                        args.index("--recovery-ready-budget-seconds") + 1
                    ]
                    == "90"
                    for args in stateful_invocations
                )
            )
            result_dirs = list((Path(temp) / "records").iterdir())
            self.assertEqual(len(result_dirs), 1)
            manifest = json.loads(
                (result_dirs[0] / "manifest.json").read_text(encoding="utf-8")
            )
            resource_metrics = json.loads(
                (result_dirs[0] / "resource_metrics.json").read_text(
                    encoding="utf-8"
                )
            )
            proc_delta = json.loads(
                (result_dirs[0] / "diagnostic_proc_delta.json").read_text(
                    encoding="utf-8"
                )
            )
            expected_diagnostics = (
                "passed"
                if proc_delta["status"] == "available"
                else "failed"
            )
            self.assertEqual(manifest["conformance"], "public_spec_aligned")
            self.assertFalse(manifest["embeds_unpublished_official_values"])
            self.assertEqual(manifest["status"], "success")
            self.assertEqual(resource_metrics["status"], "partial")
            self.assertFalse(resource_metrics["ranked"])
            self.assertEqual(resource_metrics["score_effect"], "none")
            self.assertEqual(
                resource_metrics["expected_server_generations"],
                2,
            )
            self.assertEqual(
                resource_metrics["valid_server_generations"],
                2,
            )
            self.assertTrue(
                all(
                    segment["root_observed_exit"]
                    for segment in resource_metrics["segments"]
                )
            )
            self.assertTrue(
                all(
                    segment["status"] == "available"
                    for segment in resource_metrics["segments"]
                )
            )
            self.assertEqual(
                resource_metrics["max_rss"]["status"],
                "available",
            )
            self.assertGreater(resource_metrics["max_rss"]["bytes"], 0)
            self.assertEqual(
                resource_metrics["database_disk"]["status"],
                "available",
            )
            self.assertEqual(
                resource_metrics["rank_cpu"]["status"],
                "unavailable",
            )
            self.assertEqual(
                manifest["resources"]["status"],
                resource_metrics["status"],
            )
            self.assertEqual(
                manifest["resources"]["artifact"],
                {
                    "path": "resource_metrics.json",
                    "status": "partial",
                },
            )
            self.assertFalse(manifest["resources"]["ranked"])
            self.assertEqual(
                manifest["resources"]["score_effect"],
                "none",
            )
            self.assertFalse(
                manifest["resources"]["sampling"][
                    "official_hidden_sampler_reproduced"
                ]
            )
            self.assertTrue(manifest["ranked_configuration"])
            self.assertEqual(
                manifest["seed"],
                {
                    "value": 2026,
                    "caller_supplied": False,
                    "source": "local_workflow_default_not_official",
                },
            )
            self.assertEqual(
                {
                    key: manifest["effective"][key]
                    for key in (
                        "warehouses",
                        "clients",
                        "warmup_seconds",
                        "measurement_windows",
                        "window_seconds",
                    )
                },
                {
                    "warehouses": 50,
                    "clients": 32,
                    "warmup_seconds": 30,
                    "measurement_windows": 3,
                    "window_seconds": 150,
                },
            )
            self.assertEqual(
                manifest["phases"],
                {
                    "setup": "passed",
                    "rank": "passed",
                    "online": "passed",
                    "crash_restart": "passed",
                    "recovery": "passed",
                    "diagnostics": manifest["diagnostics"]["status"],
                },
            )
            self.assertEqual(
                manifest["diagnostics"]["status"],
                expected_diagnostics,
            )
            self.assertEqual(
                {
                    key: manifest["diagnostics"][key]
                    for key in (
                        "requested",
                        "ranked",
                        "public_warmup_seconds",
                        "public_observation_seconds",
                        "native_single_observation_supported",
                    )
                },
                {
                    "requested": True,
                    "ranked": False,
                    "public_warmup_seconds": 10,
                    "public_observation_seconds": 60,
                    "native_single_observation_supported": True,
                },
            )
            self.assertEqual(
                manifest["diagnostics"]["artifacts"],
                {
                    "proc_before": {
                        "path": "diagnostic_proc_before.json",
                        "status": proc_delta["status"],
                    },
                    "proc_after": {
                        "path": "diagnostic_proc_after.json",
                        "status": proc_delta["status"],
                    },
                    "proc_delta": {
                        "path": "diagnostic_proc_delta.json",
                        "status": proc_delta["status"],
                    },
                    "strace_summary": {
                        "path": "diagnostic_strace_summary.txt",
                        "status": "present",
                    },
                    "strace_metrics": {
                        "path": "diagnostic_strace_metrics.json",
                        "status": "available",
                    },
                },
            )
            self.assertEqual(manifest["paths"]["result"], str(result_dirs[0]))
            self.assertEqual(
                manifest["paths"]["state"],
                str(result_dirs[0] / "state"),
            )
            self.assertEqual(
                manifest["source"]["rmdb_sha"],
                "unavailable",
            )
            self.assertRegex(
                manifest["source"]["tpcc_tester_sha"],
                r"^[0-9a-f]{40}$",
            )
            self.assertTrue(
                (result_dirs[0] / "diagnostic_strace_summary.txt").is_file()
            )
            strace_metrics = json.loads(
                (
                    result_dirs[0] / "diagnostic_strace_metrics.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(strace_metrics["status"], "available")
            self.assertEqual(strace_metrics["derived"]["read"]["calls"], 20)
            self.assertEqual(strace_metrics["derived"]["write"]["errors"], 1)
            for artifact in (
                "diagnostic_proc_before.json",
                "diagnostic_proc_after.json",
                "diagnostic_proc_delta.json",
            ):
                self.assertTrue((result_dirs[0] / artifact).is_file())
            self.assertIn(proc_delta["status"], ("available", "unavailable"))
            server_log = (result_dirs[0] / "server.log").read_text(
                encoding="utf-8"
            )
            self.assertIn("[server start: new database setup]", server_log)
            self.assertIn("[server start: existing database]", server_log)
            self.assertFalse((root / "tpcc_final2026").exists())
            self.assertEqual(source_csv.read_text(encoding="utf-8"), "tracked csv")
            for sampler_pid in (
                result_dirs[0] / "resource_sampler.pids"
            ).read_text(encoding="utf-8").splitlines():
                self.assert_pid_gone(int(sampler_pid))
            assert_cleanup_since(0)

            for failure_variable, diagnostic_status in (
                ("FAKE_STRACE_EMPTY", "failed"),
                ("FAKE_STRACE_DIE_EARLY", "unavailable"),
            ):
                with self.subTest(diagnostic_failure=failure_variable):
                    prior_child_count = len(recorded_child_pids())
                    env[failure_variable] = "1"
                    diagnostic_failure_records = (
                        Path(temp) / f"{failure_variable}-records"
                    )
                    diagnostic_failure = self.run_script(
                        "--mode",
                        "all",
                        "--target-dir",
                        root,
                        "--record-root",
                        diagnostic_failure_records,
                        "--skip-build",
                        "--server-bin",
                        server,
                        "--tpcc-bin",
                        tester,
                        env=env,
                    )
                    self.assertEqual(
                        diagnostic_failure.returncode,
                        0,
                        diagnostic_failure.stderr,
                    )
                    diagnostic_failure_dir = next(
                        diagnostic_failure_records.iterdir()
                    )
                    diagnostic_failure_manifest = json.loads(
                        (diagnostic_failure_dir / "manifest.json").read_text(
                            encoding="utf-8"
                        )
                    )
                    self.assertEqual(
                        diagnostic_failure_manifest["status"],
                        "success",
                    )
                    self.assertEqual(
                        diagnostic_failure_manifest["phases"]["rank"],
                        "passed",
                    )
                    self.assertEqual(
                        diagnostic_failure_manifest["phases"]["diagnostics"],
                        diagnostic_status,
                    )
                    assert_cleanup_since(prior_child_count)
                    env.pop(failure_variable)

            prior_child_count = len(recorded_child_pids())
            env["FAKE_TPCC_FAIL_SCOPE"] = "recovery"
            failed_records = Path(temp) / "failed-records"
            failed = self.run_script(
                "--mode",
                "all",
                "--target-dir",
                root,
                "--record-root",
                failed_records,
                "--skip-build",
                "--server-bin",
                server,
                "--tpcc-bin",
                tester,
                "--seed",
                "7331",
                env=env,
            )
            self.assertNotEqual(failed.returncode, 0)
            failed_result_dirs = list(failed_records.iterdir())
            self.assertEqual(len(failed_result_dirs), 1)
            failed_manifest = json.loads(
                (failed_result_dirs[0] / "manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(failed_manifest["status"], "failed")
            self.assertEqual(
                failed_manifest["seed"],
                {
                    "value": 7331,
                    "caller_supplied": True,
                    "source": "caller",
                },
            )
            self.assertEqual(
                failed_manifest["phases"],
                {
                    "setup": "passed",
                    "rank": "passed",
                    "online": "passed",
                    "crash_restart": "passed",
                    "recovery": "failed",
                    "diagnostics": "skipped_due_to_failure",
                },
            )
            self.assertIn(
                failed_manifest["resources"]["status"],
                {"partial", "unavailable", "failed"},
            )
            self.assertNotIn(
                failed_manifest["resources"]["status"],
                {"pending", "collecting", "available"},
            )
            for sampler_pid in (
                failed_result_dirs[0] / "resource_sampler.pids"
            ).read_text(encoding="utf-8").splitlines():
                self.assert_pid_gone(int(sampler_pid))
            assert_cleanup_since(prior_child_count)


if __name__ == "__main__":
    unittest.main()
