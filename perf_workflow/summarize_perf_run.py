#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import os
import pathlib
import re
import stat
import sys
from typing import Any


MAX_MANIFEST_BYTES = 1024 * 1024
MAX_TEXT_ARTIFACT_BYTES = 64 * 1024 * 1024


class ManifestError(ValueError):
    pass


def read_regular_text(path: pathlib.Path, limit: int) -> str:
    try:
        path_metadata = path.lstat()
    except FileNotFoundError:
        return ""
    if (
        stat.S_ISLNK(path_metadata.st_mode)
        or not stat.S_ISREG(path_metadata.st_mode)
        or path_metadata.st_size > limit
    ):
        raise ManifestError(f"unsafe or oversized artifact: {path.name}")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        opened_metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened_metadata.st_mode)
            or opened_metadata.st_dev != path_metadata.st_dev
            or opened_metadata.st_ino != path_metadata.st_ino
            or opened_metadata.st_size != path_metadata.st_size
            or opened_metadata.st_size > limit
        ):
            raise ManifestError(f"artifact changed while opening: {path.name}")
        with os.fdopen(descriptor, "r", encoding="utf-8", errors="replace") as stream:
            descriptor = -1
            return stream.read(limit + 1)
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def load_authoritative_manifest(result_dir: pathlib.Path) -> dict[str, Any]:
    text = read_regular_text(result_dir / "manifest.json", MAX_MANIFEST_BYTES)
    if not text:
        raise ManifestError("manifest.json is missing or empty")
    try:
        manifest = json.loads(text)
    except json.JSONDecodeError as error:
        raise ManifestError(f"manifest.json is invalid JSON: {error.msg}") from error
    if not isinstance(manifest, dict):
        raise ManifestError("manifest.json root must be an object")
    if manifest.get("schema_version") != 3:
        raise ManifestError("manifest.json has an unsupported schema version")
    if manifest.get("authority") != "manifest.json":
        raise ManifestError("manifest.json does not declare itself authoritative")

    status = manifest.get("status")
    conformance = manifest.get("conformance")
    ranking_eligible = manifest.get("ranking_eligible")
    if status not in {"running", "success", "failed"}:
        raise ManifestError("manifest.json has an invalid workflow status")
    if conformance not in {
        "public_spec_aligned",
        "non_ranked_deviation",
        "not_public_spec_aligned",
    }:
        raise ManifestError("manifest.json has an invalid conformance")
    if not isinstance(ranking_eligible, bool):
        raise ManifestError("manifest.json has an invalid ranking_eligible flag")

    attestations = manifest.get("attestations")
    if not isinstance(attestations, dict):
        raise ManifestError("manifest.json is missing attestations")
    if attestations.get("policy") != "all_required_must_be_verified":
        raise ManifestError("manifest.json has an unknown attestation policy")
    required = attestations.get("required")
    if not isinstance(required, list) or not required:
        raise ManifestError("manifest.json has no required attestations")
    names: set[str] = set()
    all_required_verified = True
    for item in required:
        if not isinstance(item, dict):
            raise ManifestError("manifest.json has a malformed attestation")
        name = item.get("name")
        item_status = item.get("status")
        required_for_ranking = item.get("required_for_ranking")
        if not isinstance(name, str) or not name or name in names:
            raise ManifestError("manifest.json has an invalid attestation name")
        names.add(name)
        if item_status not in {
            "pending",
            "verified",
            "failed",
            "not_applicable",
        }:
            raise ManifestError(f"manifest.json has an invalid {name} attestation")
        if not isinstance(required_for_ranking, bool):
            raise ManifestError(f"manifest.json has an invalid {name} requirement")
        if required_for_ranking and item_status != "verified":
            all_required_verified = False

    eligible_shape = (
        status == "success"
        and manifest.get("mode") == "all"
        and manifest.get("profile") == "final2026"
        and manifest.get("ranked_configuration") is True
        and all_required_verified
    )
    if ranking_eligible != eligible_shape:
        raise ManifestError("manifest.json ranking eligibility contradicts attestations")
    if (conformance == "public_spec_aligned") != ranking_eligible:
        raise ManifestError("manifest.json public conformance contradicts eligibility")
    return manifest


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
        match = re.search(pattern, text)
        if match:
            lines.append(match.group(0))
    return lines


def summarize_perf_stat(path: pathlib.Path) -> list[str]:
    text = read_regular_text(path, MAX_TEXT_ARTIFACT_BYTES)
    if not text:
        return []
    rows = []
    reader = csv.reader(text.splitlines())
    for row in reader:
        if len(row) < 3:
            continue
        value = row[0].strip()
        metric = row[2].strip()
        if value and metric:
            rows.append(f"{metric}: {value}")
    return rows


def summarize_exists(path: pathlib.Path, label: str) -> str:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return f"- {label}: no"
    if stat.S_ISLNK(metadata.st_mode):
        return f"- {label}: unsafe symlink"
    return f"- {label}: {'yes' if stat.S_ISREG(metadata.st_mode) else 'no'}"


def summarize_glob(directory: pathlib.Path, patterns: list[str], label: str) -> str:
    matches: list[pathlib.Path] = []
    for pattern in patterns:
        matches.extend(
            path
            for path in directory.glob(pattern)
            if not path.is_symlink() and path.is_file() and path.stat().st_size > 0
        )
    if not matches:
        return f"- {label}: no"
    names = ", ".join(path.name for path in sorted(matches))
    return f"- {label}: yes ({names})"


def render_manifest(summary: list[str], manifest: dict[str, Any]) -> None:
    summary.extend(
        [
            "",
            "## Authoritative Manifest",
            f"- status: {manifest['status']}",
            f"- mode: {manifest.get('mode', 'invalid')}",
            f"- conformance: {manifest['conformance']}",
            f"- ranking_eligible: {str(manifest['ranking_eligible']).lower()}",
        ]
    )
    for item in manifest["attestations"]["required"]:
        summary.append(f"- attestation.{item['name']}: {item['status']}")
    for warning in manifest.get("warnings", []):
        if isinstance(warning, dict):
            summary.append(
                f"- WARN {warning.get('kind', 'observation')}: "
                f"{warning.get('status', 'unknown')} (ranking effect: none)"
            )


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: summarize_perf_run.py <result-dir>", file=sys.stderr)
        return 1

    requested_dir = pathlib.Path(sys.argv[1]).absolute()
    try:
        metadata = requested_dir.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise ManifestError("result directory is missing or unsafe")
        result_dir = requested_dir.resolve(strict=True)
        manifest = load_authoritative_manifest(result_dir)
    except (FileNotFoundError, OSError, ManifestError) as error:
        print("# RMDB Performance Run Summary")
        print()
        print(f"- result_dir: `{requested_dir}`")
        print("- manifest: invalid or untrusted")
        print("- metrics: suppressed")
        print(f"- reason: {error}")
        return 2

    summary = [
        "# RMDB Performance Run Summary",
        "",
        f"- result_dir: `{result_dir}`",
    ]
    render_manifest(summary, manifest)

    if manifest["status"] == "success":
        metric_heading = (
            "Ranked Metrics"
            if manifest["ranking_eligible"]
            else "Non-ranked Metrics"
        )
        for name in [
            "rank.log",
            "benchmark.log",
            "perf/benchmark.log",
            "perf/benchmark_record.log",
            "callgrind/benchmark.log",
            "heaptrack/benchmark.log",
        ]:
            log_path = result_dir / name
            metrics = extract_report_metrics(
                read_regular_text(log_path, MAX_TEXT_ARTIFACT_BYTES)
            )
            if metrics:
                summary.extend(["", f"## {metric_heading} From `{name}`"])
                summary.extend(f"- {line}" for line in metrics)

        perf_metrics = summarize_perf_stat(result_dir / "perf" / "perf_stat.csv")
        if perf_metrics:
            summary.extend(["", f"## {metric_heading}: perf stat"])
            summary.extend(f"- {line}" for line in perf_metrics)
    else:
        summary.extend(
            [
                "",
                "## Metrics",
                "- suppressed because the authoritative workflow status is not success",
            ]
        )

    summary.extend(
        [
            "",
            "## Artifacts",
            summarize_exists(result_dir / "server.log", "server.log"),
            summarize_exists(result_dir / "perf" / "perf.data", "perf.data"),
            summarize_exists(result_dir / "perf" / "perf.svg", "perf flamegraph"),
            summarize_exists(
                result_dir / "callgrind" / "callgrind.out",
                "callgrind.out",
            ),
            summarize_exists(
                result_dir / "callgrind" / "callgrind_annotate.txt",
                "callgrind annotate",
            ),
            summarize_glob(
                result_dir / "heaptrack",
                ["heaptrack*.gz", "heaptrack*.zst"],
                "heaptrack data",
            ),
            summarize_exists(
                result_dir / "heaptrack" / "heaptrack_print.txt",
                "heaptrack report",
            ),
        ]
    )

    try:
        tool_status = read_regular_text(
            result_dir / "tool_status.txt",
            MAX_TEXT_ARTIFACT_BYTES,
        ).strip()
    except ManifestError:
        tool_status = "unsafe artifact"
    if tool_status:
        summary.extend(["", "## Tool Status"])
        summary.extend(f"- {line}" for line in tool_status.splitlines())

    print("\n".join(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
