#!/usr/bin/env python3
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "run_workflow.sh"


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
            server = self.make_python_executable(
                Path(temp) / "fake-rmdb",
                """
import os
import signal
import sys
import time

os.makedirs(sys.argv[1], exist_ok=True)
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
            env = os.environ.copy()
            env["FAKE_TPCC_CALLS"] = str(calls)
            env["FAKE_DB_PATH"] = str(root / "tpcc_final2026")

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
                "--ready-timeout-seconds",
                "5",
                "--diagnostics",
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
                "unsupported 10s+60s diagnostics must not reuse another mode",
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
            self.assertGreaterEqual(len(state_dirs), 4)
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
            result_dirs = list((Path(temp) / "records").iterdir())
            self.assertEqual(len(result_dirs), 1)
            manifest = json.loads(
                (result_dirs[0] / "manifest.json").read_text(encoding="utf-8")
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
            self.assertIn(
                manifest["diagnostics"]["status"],
                {"unavailable", "unsupported"},
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
                    "native_single_observation_supported": False,
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
            self.assertIn("WARN:", result.stderr)
            server_log = (result_dirs[0] / "server.log").read_text(
                encoding="utf-8"
            )
            self.assertIn("[server start: new database setup]", server_log)
            self.assertIn("[server start: existing database]", server_log)
            self.assertFalse((root / "tpcc_final2026").exists())
            self.assertEqual(source_csv.read_text(encoding="utf-8"), "tracked csv")

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
                "--ready-timeout-seconds",
                "5",
                "--diagnostics",
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
