#!/usr/bin/env python3
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run_workflow.sh"
METRICS_HELPER = Path(__file__).resolve().parents[1] / "diagnostic_metrics.py"


class WorkflowSafetyTests(unittest.TestCase):
    def run_script(self, *args, script=SCRIPT, env=None):
        return subprocess.run(
            ["bash", str(script), *map(str, args)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=15,
            env=env,
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

    def test_bash_syntax(self):
        result = subprocess.run(
            ["bash", "-n", str(SCRIPT)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

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
                "--clients",
                "1",
                "--warmup-seconds",
                "0",
                "--window-seconds",
                "1",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("diagnostics_requested=0\n", result.stdout)
            self.assertIn(
                "diagnostics_phase=not_applicable_non_ranked\n",
                result.stdout,
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

    def test_readiness_probe_is_bounded_by_monotonic_absolute_deadline(self):
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
                "a single readiness probe exceeded the remaining deadline",
            )

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
            child_pids = [
                int(pid)
                for pid in server_children.read_text(encoding="utf-8").splitlines()
            ]
            deadline = time.monotonic() + 2.0
            while time.monotonic() < deadline:
                if all(
                    subprocess.run(
                        ["ps", "-p", str(pid)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    ).returncode
                    != 0
                    for pid in child_pids
                ):
                    break
                time.sleep(0.02)
            self.assertTrue(
                all(
                    subprocess.run(
                        ["ps", "-p", str(pid)],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    ).returncode
                    != 0
                    for pid in child_pids
                ),
                f"registered RMDB descendants survived cleanup: {child_pids}",
            )

            for failure_variable, diagnostic_status in (
                ("FAKE_STRACE_EMPTY", "failed"),
                ("FAKE_STRACE_DIE_EARLY", "unavailable"),
            ):
                with self.subTest(diagnostic_failure=failure_variable):
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
                    env.pop(failure_variable)

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


if __name__ == "__main__":
    unittest.main()
