#!/usr/bin/env python3
from __future__ import annotations

import csv
import pathlib
import re
import sys


def read_text(path: pathlib.Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def extract_report_metrics(text: str) -> list[str]:
  patterns = [
      r"Total Transactions:\s+.+",
      r"Successful:\s+.+",
      r"Failed \(retried\):\s+.+",
      r"Success Rate:\s+.+",
      r"Total Duration:\s+.+",
      r"Average Response:\s+.+",
      r"Throughput:\s+.+",
      r"tpmC:\s+.+",
  ]
  lines: list[str] = []
  for pattern in patterns:
      m = re.search(pattern, text)
      if m:
          lines.append(m.group(0))
  return lines


def summarize_perf_stat(path: pathlib.Path) -> list[str]:
    if not path.exists():
        return []
    rows = []
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 3:
                continue
            value = row[0].strip()
            metric = row[2].strip()
            if value and metric:
                rows.append(f"{metric}: {value}")
    return rows


def summarize_exists(path: pathlib.Path, label: str) -> str:
    return f"- {label}: {'yes' if path.exists() else 'no'}"


def summarize_glob(directory: pathlib.Path, patterns: list[str], label: str) -> str:
    matches: list[pathlib.Path] = []
    for pattern in patterns:
        matches.extend(path for path in directory.glob(pattern) if path.is_file() and path.stat().st_size > 0)
    if not matches:
        return f"- {label}: no"
    names = ", ".join(path.name for path in sorted(matches))
    return f"- {label}: yes ({names})"


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: summarize_perf_run.py <result-dir>", file=sys.stderr)
        return 1

    result_dir = pathlib.Path(sys.argv[1]).resolve()
    summary: list[str] = []
    summary.append("# RMDB Performance Run Summary")
    summary.append("")
    summary.append(f"- result_dir: `{result_dir}`")

    manifest = read_text(result_dir / "manifest.txt").strip()
    if manifest:
        summary.append("")
        summary.append("## Manifest")
        for line in manifest.splitlines():
            summary.append(f"- {line}")

    for name in ["benchmark.log", "perf/benchmark.log", "perf/benchmark_record.log", "callgrind/benchmark.log", "heaptrack/benchmark.log"]:
        log_path = result_dir / name
        metrics = extract_report_metrics(read_text(log_path))
        if metrics:
            summary.append("")
            summary.append(f"## Metrics From `{name}`")
            for line in metrics:
                summary.append(f"- {line}")

    perf_metrics = summarize_perf_stat(result_dir / "perf" / "perf_stat.csv")
    if perf_metrics:
        summary.append("")
        summary.append("## perf stat")
        for line in perf_metrics:
            summary.append(f"- {line}")

    summary.append("")
    summary.append("## Artifacts")
    summary.append(summarize_exists(result_dir / "server.log", "server.log"))
    summary.append(summarize_exists(result_dir / "perf" / "perf.data", "perf.data"))
    summary.append(summarize_exists(result_dir / "perf" / "perf.svg", "perf flamegraph"))
    summary.append(summarize_exists(result_dir / "callgrind" / "callgrind.out", "callgrind.out"))
    summary.append(summarize_exists(result_dir / "callgrind" / "callgrind_annotate.txt", "callgrind annotate"))
    summary.append(summarize_glob(result_dir / "heaptrack", ["heaptrack*.gz", "heaptrack*.zst"], "heaptrack data"))
    summary.append(summarize_exists(result_dir / "heaptrack" / "heaptrack_print.txt", "heaptrack report"))

    tool_status = read_text(result_dir / "tool_status.txt").strip()
    if tool_status:
        summary.append("")
        summary.append("## Tool Status")
        for line in tool_status.splitlines():
            summary.append(f"- {line}")

    print("\n".join(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
