#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import os
import pathlib
import re
import stat
import sys
from typing import Any


MAX_MANIFEST_BYTES = 1024 * 1024
MAX_TEXT_ARTIFACT_BYTES = 64 * 1024 * 1024
CORE_RANKING_ATTESTATIONS = {
    "public_configuration",
    "opaque_sealed_database",
    "formal_workflow_phases",
    "formal_state_chain",
}
CORE_ATTESTATION_VALIDATORS = {
    "public_configuration": "workflow_exact_public_profile_and_mode",
    "opaque_sealed_database": "database_identity_v2",
    "formal_workflow_phases": "shell_phase_receipts_v1",
    "formal_state_chain": "tpcc_tester_read_only_state_attestation_v1",
}
PUBLIC_EFFECTIVE_CONFIGURATION = {
    "warehouses": 50,
    "clients": 32,
    "warmup_seconds": 30,
    "measurement_windows": 3,
    "window_seconds": 150,
    "recovery_ready_budget_seconds": 90,
}
PUBLIC_TRANSACTION_MIX = {
    "new_order": 45,
    "payment": 43,
    "order_status": 4,
    "delivery": 4,
    "stock_level": 4,
}
HEX_16 = re.compile(r"^[0-9a-f]{16}$")
HEX_64 = re.compile(r"^[0-9a-f]{64}$")


class ManifestError(ValueError):
    pass


def is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def read_regular_bytes(
    result_dir: pathlib.Path,
    relative_name: str,
    limit: int,
) -> bytes:
    relative = pathlib.PurePosixPath(relative_name)
    if (
        relative.is_absolute()
        or not relative.parts
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise ManifestError(f"unsafe artifact path: {relative_name}")

    descriptors: list[int] = []
    try:
        directory_flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY"):
            directory_flags |= os.O_DIRECTORY
        if hasattr(os, "O_NOFOLLOW"):
            directory_flags |= os.O_NOFOLLOW
        current = os.open(result_dir, directory_flags)
        descriptors.append(current)
        if not stat.S_ISDIR(os.fstat(current).st_mode):
            raise ManifestError("result directory changed while opening")

        for component in relative.parts[:-1]:
            current = os.open(component, directory_flags, dir_fd=current)
            descriptors.append(current)
            if not stat.S_ISDIR(os.fstat(current).st_mode):
                raise ManifestError(
                    f"unsafe artifact ancestor: {relative_name}"
                )

        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(relative.parts[-1], flags, dir_fd=current)
        descriptors.append(descriptor)
        opened_metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened_metadata.st_mode)
            or opened_metadata.st_size > limit
        ):
            raise ManifestError(
                f"unsafe or oversized artifact: {relative_name}"
            )

        chunks: list[bytes] = []
        remaining = opened_metadata.st_size
        while remaining:
            chunk = os.read(descriptor, min(1024 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        final_metadata = os.fstat(descriptor)
        if (
            final_metadata.st_dev != opened_metadata.st_dev
            or final_metadata.st_ino != opened_metadata.st_ino
            or final_metadata.st_size != opened_metadata.st_size
            or final_metadata.st_mtime_ns != opened_metadata.st_mtime_ns
            or len(data) != opened_metadata.st_size
        ):
            raise ManifestError(
                f"artifact changed while reading: {relative_name}"
            )
        return data
    except FileNotFoundError:
        return b""
    except OSError as error:
        raise ManifestError(
            f"unsafe artifact path {relative_name}: {error.strerror}"
        ) from error
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def read_regular_text(
    result_dir: pathlib.Path,
    relative_name: str,
    limit: int,
) -> str:
    return read_regular_bytes(result_dir, relative_name, limit).decode(
        "utf-8",
        errors="replace",
    )


def validate_effective_configuration(manifest: dict[str, Any]) -> bool:
    effective = manifest.get("effective")
    if not isinstance(effective, dict):
        raise ManifestError("manifest.json is missing effective configuration")
    for name in PUBLIC_EFFECTIVE_CONFIGURATION:
        value = effective.get(name)
        if not is_int(value) or value < 0:
            raise ManifestError(
                f"manifest.json has an invalid effective {name}"
            )
    if any(
        effective[name] <= 0
        for name in (
            "warehouses",
            "clients",
            "measurement_windows",
            "window_seconds",
            "recovery_ready_budget_seconds",
        )
    ):
        raise ManifestError("manifest.json has a non-positive public setting")
    if effective.get("transaction_mix_percent") != PUBLIC_TRANSACTION_MIX:
        raise ManifestError("manifest.json has an invalid transaction mix")
    if effective.get("derived_write_ratio") != 0.92:
        raise ManifestError("manifest.json has an invalid write ratio")
    if not isinstance(effective.get("deviation_opt_in"), bool) or not isinstance(
        effective.get("deviation_active"),
        bool,
    ):
        raise ManifestError("manifest.json has invalid deviation flags")
    return (
        all(
            effective[name] == expected
            for name, expected in PUBLIC_EFFECTIVE_CONFIGURATION.items()
        )
        and effective["deviation_active"] is False
    )


def validate_database_identity(
    manifest: dict[str, Any],
    result_dir: pathlib.Path,
) -> bool:
    paths = manifest.get("paths")
    if not isinstance(paths, dict):
        raise ManifestError("manifest.json is missing paths")
    if paths.get("result") != str(result_dir):
        raise ManifestError("manifest.json is not bound to this result directory")
    for name in ("database", "state"):
        if not isinstance(paths.get(name), str) or not paths[name]:
            raise ManifestError(f"manifest.json has an invalid {name} path")

    seed = manifest.get("seed")
    if (
        not isinstance(seed, dict)
        or not is_int(seed.get("value"))
        or seed["value"] < 0
        or not isinstance(seed.get("caller_supplied"), bool)
        or not isinstance(seed.get("source"), str)
    ):
        raise ManifestError("manifest.json has invalid seed metadata")

    identity = manifest.get("database_identity")
    if not isinstance(identity, dict):
        raise ManifestError("manifest.json is missing database identity")
    if identity.get("status") not in {
        "pending",
        "verified",
        "failed",
        "not_applicable",
    }:
        raise ManifestError("manifest.json has an invalid database status")
    if identity.get("binding_status") not in {
        "provisioned",
        "sealed",
        "failed",
        "not_applicable",
    }:
        raise ManifestError("manifest.json has an invalid database binding")
    for name in (
        "opaque_name",
        "caller_supplied_this_invocation",
        "deviation_active",
    ):
        if not isinstance(identity.get(name), bool):
            raise ManifestError(
                f"manifest.json has an invalid database {name}"
            )
    for name in ("name", "path_basename", "name_source", "name_algorithm"):
        if not isinstance(identity.get(name), str) or not identity[name]:
            raise ManifestError(
                f"manifest.json has an invalid database {name}"
            )
    if identity["path_basename"] != pathlib.Path(paths["database"]).name:
        raise ManifestError("manifest.json database path identity disagrees")

    filesystem = identity.get("filesystem")
    binding = identity.get("dataset_binding")
    if not isinstance(filesystem, dict) or not isinstance(binding, dict):
        raise ManifestError("manifest.json has incomplete database bindings")
    if binding.get("dataset_run_id") != manifest.get("dataset_run_id"):
        raise ManifestError("manifest.json dataset run binding disagrees")
    if binding.get("seed") != seed["value"]:
        raise ManifestError("manifest.json dataset seed binding disagrees")

    verified = (
        identity["status"] == "verified"
        and identity["binding_status"] == "sealed"
        and identity["opaque_name"] is True
        and identity["name"] == identity["path_basename"]
        and identity["name_source"] == "derived_opaque"
        and identity["name_algorithm"] == "sha256_domain_run_id_seed_v1"
        and identity["caller_supplied_this_invocation"] is False
        and identity["deviation_active"] is False
        and is_int(filesystem.get("device"))
        and filesystem["device"] > 0
        and is_int(filesystem.get("inode"))
        and filesystem["inode"] > 0
        and isinstance(filesystem.get("path_fingerprint"), str)
        and HEX_64.fullmatch(filesystem["path_fingerprint"]) is not None
        and isinstance(binding.get("runtime_schema_fingerprint"), str)
        and HEX_16.fullmatch(binding["runtime_schema_fingerprint"]) is not None
        and isinstance(binding.get("dataset_state_fingerprint"), str)
        and HEX_64.fullmatch(binding["dataset_state_fingerprint"]) is not None
        and isinstance(identity.get("identity_fingerprint"), str)
        and HEX_64.fullmatch(identity["identity_fingerprint"]) is not None
        and identity.get("state_artifact") == "database.identity"
        and identity.get("database_marker")
        == ".tpcc-workflow-database-identity"
    )
    if (
        identity["status"] == "verified"
        or identity["binding_status"] == "sealed"
    ) and not verified:
        raise ManifestError("manifest.json has an invalid sealed database identity")
    return verified


def validate_observation_metadata(manifest: dict[str, Any]) -> None:
    phases = manifest.get("phases")
    diagnostics = manifest.get("diagnostics")
    resources = manifest.get("resources")
    if not isinstance(phases, dict) or not isinstance(diagnostics, dict):
        raise ManifestError("manifest.json is missing phase metadata")
    if not isinstance(resources, dict):
        raise ManifestError("manifest.json is missing resource metadata")
    if (
        diagnostics.get("ranked") is not False
        or diagnostics.get("status") != phases.get("diagnostics")
    ):
        raise ManifestError("manifest.json ranks or contradicts diagnostics")
    if (
        resources.get("observation_only") is not True
        or resources.get("ranked") is not False
        or resources.get("score_effect") != "none"
    ):
        raise ManifestError("manifest.json ranks resource observations")
    sampling = resources.get("sampling")
    if (
        not isinstance(sampling, dict)
        or sampling.get("official_hidden_sampler_reproduced") is not False
    ):
        raise ManifestError("manifest.json overclaims resource sampling")
    warnings = manifest.get("warnings")
    if not isinstance(warnings, list):
        raise ManifestError("manifest.json has invalid warnings")
    for warning in warnings:
        if (
            not isinstance(warning, dict)
            or warning.get("ranking_effect") != "none"
        ):
            raise ManifestError("manifest.json has a ranked warning")


def validate_rank_result(manifest: dict[str, Any]) -> dict[str, Any]:
    rank_result = manifest.get("rank_result")
    if not isinstance(rank_result, dict):
        raise ManifestError("manifest.json is missing the rank result binding")
    if rank_result.get("path") != "rank.log":
        raise ManifestError("manifest.json has an invalid rank result path")
    status = rank_result.get("status")
    if status not in {
        "missing",
        "unsafe",
        "empty",
        "oversized",
        "changed",
        "verified",
    }:
        raise ManifestError("manifest.json has an invalid rank result status")
    if status == "verified":
        size = rank_result.get("size_bytes")
        digest = rank_result.get("sha256")
        if (
            not is_int(size)
            or size <= 0
            or not isinstance(digest, str)
            or HEX_64.fullmatch(digest) is None
        ):
            raise ManifestError("manifest.json has an invalid rank result digest")
    elif (
        rank_result.get("size_bytes") is not None
        or rank_result.get("sha256") is not None
    ):
        raise ManifestError("manifest.json binds an unavailable rank result")
    return rank_result


def load_authoritative_manifest(
    result_dir: pathlib.Path,
) -> tuple[dict[str, Any], str]:
    text = read_regular_text(
        result_dir,
        "manifest.json",
        MAX_MANIFEST_BYTES,
    )
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
    if manifest.get("mode") not in {
        "all",
        "init",
        "rank",
        "recovery",
        "tools",
    }:
        raise ManifestError("manifest.json has an invalid workflow mode")
    if manifest.get("profile") != "final2026":
        raise ManifestError("manifest.json has an invalid workflow profile")
    if not isinstance(manifest.get("ranked_configuration"), bool):
        raise ManifestError("manifest.json has an invalid ranked configuration")
    for name in ("run_id", "dataset_run_id"):
        if not isinstance(manifest.get(name), str) or not manifest[name]:
            raise ManifestError(f"manifest.json has an invalid {name}")

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
        validator = item.get("validator")
        if not isinstance(validator, str) or not validator:
            raise ManifestError(f"manifest.json has an invalid {name} validator")
        if (
            name in CORE_ATTESTATION_VALIDATORS
            and validator != CORE_ATTESTATION_VALIDATORS[name]
        ):
            raise ManifestError(
                f"manifest.json has an invalid {name} validator"
            )
        if required_for_ranking and item_status != "verified":
            all_required_verified = False
    if not CORE_RANKING_ATTESTATIONS.issubset(names):
        raise ManifestError("manifest.json is missing a core ranking attestation")
    for item in required:
        if (
            item["name"] in CORE_RANKING_ATTESTATIONS
            and item["required_for_ranking"] is not True
        ):
            raise ManifestError(
                f"manifest.json makes core attestation {item['name']} optional"
            )

    phases = manifest.get("phases")
    if not isinstance(phases, dict):
        raise ManifestError("manifest.json is missing workflow phases")
    formal_phase_names = (
        "setup",
        "rank",
        "online",
        "crash_restart",
        "recovery",
    )
    for name in formal_phase_names:
        if phases.get(name) not in {
            "pending",
            "running",
            "passed",
            "failed",
            "not_applicable",
            "skipped_due_to_failure",
        }:
            raise ManifestError(f"manifest.json has an invalid {name} phase")
    if not isinstance(phases.get("diagnostics"), str):
        raise ManifestError("manifest.json has an invalid diagnostics phase")

    exact_public_configuration = validate_effective_configuration(manifest)
    identity_verified = validate_database_identity(manifest, result_dir)
    validate_observation_metadata(manifest)
    rank_result = validate_rank_result(manifest)

    ranked_configuration = manifest["ranked_configuration"]
    if ranked_configuration != exact_public_configuration:
        raise ManifestError(
            "manifest.json ranked configuration contradicts effective values"
        )
    mode = manifest["mode"]
    formal_phases_complete = all(
        phases[name] == "passed" for name in formal_phase_names
    )
    by_name = {item["name"]: item for item in required}
    expected_configuration_status = (
        "not_applicable"
        if mode != "all"
        else "verified"
        if ranked_configuration
        else "pending"
        if status == "running"
        else "failed"
    )
    expected_identity_status = (
        "not_applicable"
        if mode != "all"
        else "verified"
        if identity_verified
        else "pending"
        if (
            status == "running"
            and manifest["database_identity"]["status"]
            in {"pending", "verified"}
        )
        else "failed"
    )
    expected_phase_status = (
        "not_applicable"
        if mode != "all"
        else "verified"
        if formal_phases_complete and rank_result["status"] == "verified"
        else "pending"
        if (
            status == "running"
            and any(
                phases[name] in {"pending", "running"}
                for name in formal_phase_names
            )
        )
        else "failed"
    )
    for name, expected in (
        ("public_configuration", expected_configuration_status),
        ("opaque_sealed_database", expected_identity_status),
        ("formal_workflow_phases", expected_phase_status),
    ):
        if by_name[name]["status"] != expected:
            raise ManifestError(
                f"manifest.json {name} contradicts its evidence"
            )
    if (
        mode != "all"
        and by_name["formal_state_chain"]["status"] != "not_applicable"
    ):
        raise ManifestError(
            "manifest.json split mode claims a formal state attestation"
        )

    eligible_shape = (
        status == "success"
        and mode == "all"
        and ranked_configuration
        and all_required_verified
    )
    if ranking_eligible != eligible_shape:
        raise ManifestError("manifest.json ranking eligibility contradicts attestations")
    expected_conformance = (
        "public_spec_aligned"
        if ranking_eligible
        else "non_ranked_deviation"
        if not ranked_configuration
        else "not_public_spec_aligned"
    )
    if conformance != expected_conformance:
        raise ManifestError("manifest.json conformance contradicts eligibility")

    rank_text = ""
    if (
        status == "success"
        and phases["rank"] == "passed"
        and rank_result["status"] == "verified"
    ):
        rank_bytes = read_regular_bytes(
            result_dir,
            rank_result["path"],
            MAX_TEXT_ARTIFACT_BYTES,
        )
        if (
            len(rank_bytes) != rank_result["size_bytes"]
            or hashlib.sha256(rank_bytes).hexdigest() != rank_result["sha256"]
        ):
            raise ManifestError("rank.log does not match its manifest binding")
        rank_text = rank_bytes.decode("utf-8", errors="replace")
    elif ranking_eligible:
        raise ManifestError("manifest.json does not bind its ranked result")
    return manifest, rank_text


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


def summarize_perf_stat(result_dir: pathlib.Path) -> list[str]:
    text = read_regular_text(
        result_dir,
        "perf/perf_stat.csv",
        MAX_TEXT_ARTIFACT_BYTES,
    )
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


def path_has_symlink(
    result_dir: pathlib.Path,
    relative_name: str,
) -> bool:
    current = result_dir
    for component in pathlib.PurePosixPath(relative_name).parts:
        current = current / component
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            return False
        if stat.S_ISLNK(metadata.st_mode):
            return True
    return False


def summarize_exists(
    result_dir: pathlib.Path,
    relative_name: str,
    label: str,
) -> str:
    if path_has_symlink(result_dir, relative_name):
        return f"- {label}: unsafe symlink"
    path = result_dir / relative_name
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return f"- {label}: no"
    return f"- {label}: {'yes' if stat.S_ISREG(metadata.st_mode) else 'no'}"


def summarize_glob(
    result_dir: pathlib.Path,
    directory_name: str,
    patterns: list[str],
    label: str,
) -> str:
    if path_has_symlink(result_dir, directory_name):
        return f"- {label}: unsafe symlink"
    directory = result_dir / directory_name
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
        manifest, rank_text = load_authoritative_manifest(result_dir)
    except (FileNotFoundError, OSError, ManifestError) as error:
        print("# RMDB Performance Run Summary")
        print()
        print(f"- result_dir: `{requested_dir}`")
        print("- manifest: invalid or untrusted")
        print("- metrics: suppressed")
        print(f"- reason: {error}")
        print(f"summary rejected untrusted result: {error}", file=sys.stderr)
        return 2

    summary = [
        "# RMDB Performance Run Summary",
        "",
        f"- result_dir: `{result_dir}`",
    ]
    render_manifest(summary, manifest)

    if manifest["status"] == "success" and rank_text:
        metric_heading = (
            "Ranked Metrics"
            if manifest["ranking_eligible"]
            else "Non-ranked Metrics"
        )
        metrics = extract_report_metrics(rank_text)
        if metrics:
            summary.extend(["", f"## {metric_heading} From `rank.log`"])
            summary.extend(f"- {line}" for line in metrics)
    elif manifest["status"] != "success":
        summary.extend(
            [
                "",
                "## Metrics",
                "- suppressed because the authoritative workflow status is not success",
            ]
        )

    try:
        perf_metrics = summarize_perf_stat(result_dir)
    except ManifestError:
        perf_metrics = []
    if perf_metrics:
        summary.extend(["", "## Observation-only Diagnostics: perf stat"])
        summary.extend(f"- {line}" for line in perf_metrics)

    summary.extend(
        [
            "",
            "## Artifacts",
            summarize_exists(result_dir, "server.log", "server.log"),
            summarize_exists(result_dir, "perf/perf.data", "perf.data"),
            summarize_exists(
                result_dir,
                "perf/perf.svg",
                "perf flamegraph",
            ),
            summarize_exists(
                result_dir,
                "callgrind/callgrind.out",
                "callgrind.out",
            ),
            summarize_exists(
                result_dir,
                "callgrind/callgrind_annotate.txt",
                "callgrind annotate",
            ),
            summarize_glob(
                result_dir,
                "heaptrack",
                ["heaptrack*.gz", "heaptrack*.zst"],
                "heaptrack data",
            ),
            summarize_exists(
                result_dir,
                "heaptrack/heaptrack_print.txt",
                "heaptrack report",
            ),
        ]
    )

    try:
        tool_status = read_regular_text(
            result_dir,
            "tool_status.txt",
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
