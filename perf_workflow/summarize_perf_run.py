#!/usr/bin/env python3
from __future__ import annotations

import csv
from decimal import Decimal
import hashlib
import importlib.util
import json
import os
import pathlib
import re
import stat
import sys
from typing import Any


FORMAL_CHAIN_SPEC = importlib.util.spec_from_file_location(
    "workflow_formal_state_chain",
    pathlib.Path(__file__).resolve().with_name("formal_state_chain.py"),
)
if FORMAL_CHAIN_SPEC is None or FORMAL_CHAIN_SPEC.loader is None:
    raise RuntimeError("could not load formal state chain helper")
FORMAL_STATE_CHAIN = importlib.util.module_from_spec(FORMAL_CHAIN_SPEC)
FORMAL_CHAIN_SPEC.loader.exec_module(FORMAL_STATE_CHAIN)


MAX_MANIFEST_BYTES = 1024 * 1024
MAX_TEXT_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_TERMINAL_EVIDENCE_STATE_BYTES = (
    16 * 1024 * 1024 + 128 + 4 * 1024
)
TERMINAL_EVIDENCE_FILE = "terminal_evidence.state"
LEGACY_RUN_LEDGER_FILE = "run_ledger.state"
RANK_METRIC_POLICY = "new_order_three_window_decimal_median_v1"
CORE_RANKING_ATTESTATIONS = {
    "public_configuration",
    "trusted_tester_binary",
    "opaque_sealed_database",
    "formal_workflow_phases",
    "formal_state_chain",
}
CORE_ATTESTATION_VALIDATORS = {
    "public_configuration": "workflow_exact_public_profile_and_mode",
    "trusted_tester_binary": "fresh_workflow_source_binary_sha256_v1",
    "opaque_sealed_database": "database_identity_v2",
    "formal_workflow_phases": "shell_phase_receipts_v1",
    "formal_state_chain": "tpcc_tester_terminal_evidence_attestation_v2",
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


def validate_tester_binary(manifest: dict[str, Any]) -> bool:
    tester = manifest.get("tester_binary")
    if not isinstance(tester, dict):
        raise ManifestError("manifest.json is missing tester binary provenance")
    status = tester.get("status")
    if status not in {
        "pending_fresh_build",
        "verified_fresh_build",
        "untrusted_binary_override",
        "unverified_prebuilt_binary",
        "external_source_root",
    }:
        raise ManifestError("manifest.json has invalid tester binary provenance")
    if not isinstance(tester.get("path"), str) or not tester["path"]:
        raise ManifestError("manifest.json has an invalid tester binary path")
    paths = manifest.get("paths")
    source_root = paths.get("tpcc_tester") if isinstance(paths, dict) else None
    if not isinstance(source_root, str) or not source_root:
        raise ManifestError("manifest.json has no tester source path")
    for name in (
        "source_matches_workflow",
        "built_this_invocation",
        "binary_override",
        "skip_build",
    ):
        if not isinstance(tester.get(name), bool):
            raise ManifestError(
                f"manifest.json has an invalid tester binary {name}"
            )
    filesystem = tester.get("filesystem")
    if not isinstance(filesystem, dict):
        raise ManifestError("manifest.json has incomplete tester binary identity")
    digest = tester.get("sha256")
    has_identity = (
        isinstance(digest, str)
        and HEX_64.fullmatch(digest) is not None
        and is_int(filesystem.get("device"))
        and filesystem["device"] > 0
        and is_int(filesystem.get("inode"))
        and filesystem["inode"] > 0
        and is_int(filesystem.get("size_bytes"))
        and filesystem["size_bytes"] > 0
    )
    trusted = (
        status == "verified_fresh_build"
        and has_identity
        and tester["source_matches_workflow"] is True
        and tester["built_this_invocation"] is True
        and tester["binary_override"] is False
        and tester["skip_build"] is False
    )
    if status == "verified_fresh_build" and not trusted:
        raise ManifestError("manifest.json has invalid trusted tester provenance")
    if trusted and tester["path"] != str(
        pathlib.Path(source_root) / "target" / "release" / "tpcc-tester"
    ):
        raise ManifestError("manifest.json tester path contradicts its source")
    if status == "pending_fresh_build":
        if (
            digest is not None
            or any(
                filesystem.get(name) is not None
                for name in ("device", "inode", "size_bytes")
            )
            or tester["source_matches_workflow"] is not True
            or tester["built_this_invocation"] is not False
            or tester["binary_override"] is not False
            or tester["skip_build"] is not False
        ):
            raise ManifestError("manifest.json has invalid pending tester provenance")
    elif not has_identity:
        raise ManifestError("manifest.json has unbound tester binary provenance")
    return trusted


def validate_rank_result(manifest: dict[str, Any]) -> dict[str, Any]:
    rank_result = manifest.get("rank_result")
    if (
        not isinstance(rank_result, dict)
        or set(rank_result)
        != {"path", "status", "size_bytes", "sha256", "metrics"}
    ):
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
        "invalid_metrics",
        "verified",
    }:
        raise ManifestError("manifest.json has an invalid rank result status")
    metrics = rank_result.get("metrics")
    if (
        not isinstance(metrics, dict)
        or set(metrics)
        != {"policy", "status", "window_values", "median"}
        or metrics.get("policy") != RANK_METRIC_POLICY
        or metrics.get("status") not in {"unavailable", "invalid", "verified"}
    ):
        raise ManifestError("manifest.json has invalid ranked metric metadata")
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
        windows = metrics.get("window_values")
        median = metrics.get("median")
        canonical_rate = re.compile(r"(?:0|[1-9][0-9]*)\.[0-9]{3}")
        if (
            metrics.get("status") != "verified"
            or not isinstance(windows, list)
            or len(windows) != 3
            or any(
                not isinstance(value, str)
                or canonical_rate.fullmatch(value) is None
                for value in windows
            )
            or not isinstance(median, str)
            or canonical_rate.fullmatch(median) is None
            or Decimal(median) != sorted(Decimal(value) for value in windows)[1]
        ):
            raise ManifestError(
                "manifest.json has invalid ranked NewOrder/min metrics"
            )
    elif (
        rank_result.get("size_bytes") is not None
        or rank_result.get("sha256") is not None
    ):
        raise ManifestError("manifest.json binds an unavailable rank result")
    elif (
        metrics.get("status")
        != ("invalid" if status == "invalid_metrics" else "unavailable")
        or metrics.get("window_values") is not None
        or metrics.get("median") is not None
    ):
        raise ManifestError(
            "manifest.json claims metrics for an unavailable rank result"
        )
    return rank_result


def inspect_terminal_evidence(
    state_dir: pathlib.Path,
) -> tuple[int, str]:
    if not state_dir.is_absolute():
        raise ManifestError("manifest.json state path is not absolute")
    if not hasattr(os, "O_DIRECTORY") or not hasattr(os, "O_NOFOLLOW"):
        raise ManifestError("platform cannot safely inspect terminal evidence")
    directory_flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        directory_flags |= os.O_CLOEXEC
    try:
        state_descriptor = os.open(state_dir, directory_flags)
    except OSError as error:
        raise ManifestError(
            "manifest.json state directory is missing or unsafe"
        ) from error
    try:
        try:
            os.stat(
                LEGACY_RUN_LEDGER_FILE,
                dir_fd=state_descriptor,
                follow_symlinks=False,
            )
        except FileNotFoundError:
            pass
        except OSError as error:
            raise ManifestError(
                "could not inspect forbidden run_ledger.state"
            ) from error
        else:
            raise ManifestError(
                "forbidden run_ledger.state is present"
            )

        try:
            before = os.stat(
                TERMINAL_EVIDENCE_FILE,
                dir_fd=state_descriptor,
                follow_symlinks=False,
            )
        except OSError as error:
            raise ManifestError(
                "terminal_evidence.state is missing or unsafe"
            ) from error
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size <= 0
            or before.st_size > MAX_TERMINAL_EVIDENCE_STATE_BYTES
        ):
            raise ManifestError(
                "terminal_evidence.state is empty, oversized, or unsafe"
            )

        file_flags = os.O_RDONLY | os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            file_flags |= os.O_CLOEXEC
        try:
            descriptor = os.open(
                TERMINAL_EVIDENCE_FILE,
                file_flags,
                dir_fd=state_descriptor,
            )
        except OSError as error:
            raise ManifestError(
                "terminal_evidence.state could not be opened safely"
            ) from error
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_dev != before.st_dev
                or opened.st_ino != before.st_ino
                or opened.st_size != before.st_size
                or opened.st_mtime_ns != before.st_mtime_ns
            ):
                raise ManifestError(
                    "terminal_evidence.state changed while opening"
                )
            digest = hashlib.sha256()
            remaining = opened.st_size
            while remaining:
                chunk = os.read(
                    descriptor,
                    min(1024 * 1024, remaining),
                )
                if not chunk:
                    break
                digest.update(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
            try:
                current = os.stat(
                    TERMINAL_EVIDENCE_FILE,
                    dir_fd=state_descriptor,
                    follow_symlinks=False,
                )
            except OSError as error:
                raise ManifestError(
                    "terminal_evidence.state changed while hashing"
                ) from error
            if (
                remaining != 0
                or after.st_dev != opened.st_dev
                or after.st_ino != opened.st_ino
                or after.st_size != opened.st_size
                or after.st_mtime_ns != opened.st_mtime_ns
                or current.st_dev != opened.st_dev
                or current.st_ino != opened.st_ino
                or current.st_size != opened.st_size
                or current.st_mtime_ns != opened.st_mtime_ns
            ):
                raise ManifestError(
                    "terminal_evidence.state changed while hashing"
                )
            try:
                os.stat(
                    LEGACY_RUN_LEDGER_FILE,
                    dir_fd=state_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                pass
            except OSError as error:
                raise ManifestError(
                    "could not re-inspect forbidden run_ledger.state"
                ) from error
            else:
                raise ManifestError(
                    "forbidden run_ledger.state appeared while hashing"
                )
            return opened.st_size, digest.hexdigest()
        finally:
            os.close(descriptor)
    finally:
        os.close(state_descriptor)


def validate_formal_state(
    manifest: dict[str, Any],
    attestation_status: str,
) -> bool:
    formal = manifest.get("formal_state")
    if (
        not isinstance(formal, dict)
        or set(formal)
        != {
            "status",
            "publication_policy",
            "terminal_evidence",
            "legacy_run_ledger",
            "formal_chain",
        }
    ):
        raise ManifestError("manifest.json has invalid formal-state metadata")
    terminal = formal.get("terminal_evidence")
    legacy = formal.get("legacy_run_ledger")
    chain = formal.get("formal_chain")
    if (
        not isinstance(terminal, dict)
        or set(terminal)
        != {
            "path",
            "status",
            "file_type",
            "open_policy",
            "size_bytes",
            "max_size_bytes",
            "sha256",
        }
        or not isinstance(legacy, dict)
        or set(legacy)
        != {"path", "status", "inspection_policy"}
        or not isinstance(chain, dict)
        or set(chain)
        != {
            "status",
            "policy",
            "domain",
            "encoding",
            "state_directory",
            "file_count",
            "files",
            "sha256",
        }
    ):
        raise ManifestError(
            "manifest.json has invalid terminal evidence descriptors"
        )
    paths = manifest.get("paths")
    if not isinstance(paths, dict) or not isinstance(paths.get("state"), str):
        raise ManifestError("manifest.json has no formal-state directory")
    state_dir = pathlib.Path(paths["state"])
    if terminal.get("path") != str(state_dir / TERMINAL_EVIDENCE_FILE):
        raise ManifestError(
            "manifest.json terminal evidence path is not exact"
        )
    if legacy.get("path") != str(state_dir / LEGACY_RUN_LEDGER_FILE):
        raise ManifestError(
            "manifest.json legacy ledger path is not exact"
        )
    if (
        formal.get("publication_policy")
        != "state_directory_fd_flock_v2"
        or terminal.get("open_policy")
        != "state_dir_fd_o_nofollow_sha256_v1"
        or terminal.get("max_size_bytes")
        != MAX_TERMINAL_EVIDENCE_STATE_BYTES
        or legacy.get("inspection_policy")
        != "state_dir_fd_exact_target_and_canonical_temps_v2"
        or chain.get("policy") != "formal_state_chain_v2"
        or chain.get("domain") != "RMDB_TPCC_FORMAL_CHAIN_V2\\0"
        or chain.get("encoding")
        != (
            "domain_dev_u64be_ino_u64be_count_u32be_"
            "name_len_u32be_name_content_len_u64be_"
            "content_content_len_u64be"
        )
        or chain.get("file_count")
        != len(FORMAL_STATE_CHAIN.FORMAL_CHAIN_FILES)
        or chain.get("files")
        != list(FORMAL_STATE_CHAIN.FORMAL_CHAIN_FILES)
    ):
        raise ManifestError(
            "manifest.json has an invalid formal-state inspection policy"
        )
    valid_terminal_statuses = {
        "pending",
        "verified",
        "missing",
        "unsafe",
        "empty",
        "oversized",
        "changed",
        "inspection_failed",
        "legacy_present",
        "not_applicable",
    }
    valid_legacy_statuses = {
        "pending",
        "absent",
        "present",
        "inspection_failed",
        "not_applicable",
    }
    if (
        formal.get("status") != attestation_status
        or terminal.get("status") not in valid_terminal_statuses
        or legacy.get("status") not in valid_legacy_statuses
        or chain.get("status")
        not in {"pending", "verified", "failed", "not_applicable"}
        or chain.get("status") != formal.get("status")
    ):
        raise ManifestError(
            "manifest.json formal-state status contradicts its attestation"
        )

    mode = manifest.get("mode")
    if mode != "all":
        if (
            formal["status"] != "not_applicable"
            or terminal["status"] != "not_applicable"
            or legacy["status"] != "not_applicable"
            or terminal.get("file_type") is not None
            or terminal.get("size_bytes") is not None
            or terminal.get("sha256") is not None
            or chain.get("state_directory")
            != {"device": None, "inode": None}
            or chain.get("sha256") is not None
        ):
            raise ManifestError(
                "manifest.json split mode claims terminal evidence"
            )
        return False

    state_identity = chain.get("state_directory")
    if (
        not isinstance(state_identity, dict)
        or set(state_identity) != {"device", "inode"}
    ):
        raise ManifestError(
            "manifest.json has invalid formal state directory identity"
        )
    if chain["status"] != "verified" and (
        state_identity != {"device": None, "inode": None}
        or chain.get("sha256") is not None
    ):
        raise ManifestError(
            "manifest.json binds an unavailable formal state chain"
        )

    if terminal["status"] != "verified":
        if (
            terminal.get("file_type") is not None
            or terminal.get("size_bytes") is not None
            or terminal.get("sha256") is not None
        ):
            raise ManifestError(
                "manifest.json binds unavailable terminal evidence"
            )
        if formal["status"] == "verified":
            raise ManifestError(
                "manifest.json verifies an unavailable terminal artifact"
            )
        return False

    size = terminal.get("size_bytes")
    digest = terminal.get("sha256")
    if (
        terminal.get("file_type") != "regular"
        or not is_int(size)
        or size <= 0
        or size > MAX_TERMINAL_EVIDENCE_STATE_BYTES
        or not isinstance(digest, str)
        or HEX_64.fullmatch(digest) is None
        or legacy.get("status") != "absent"
    ):
        raise ManifestError(
            "manifest.json has an invalid terminal evidence binding"
        )
    if formal["status"] != "verified":
        return False
    state_device = state_identity.get("device")
    state_inode = state_identity.get("inode")
    chain_digest = chain.get("sha256")
    if (
        not is_int(state_device)
        or state_device < 0
        or not is_int(state_inode)
        or state_inode < 0
        or not isinstance(chain_digest, str)
        or HEX_64.fullmatch(chain_digest) is None
    ):
        raise ManifestError(
            "manifest.json has an invalid formal state chain binding"
        )
    try:
        actual = FORMAL_STATE_CHAIN.inspect_formal_state_path(state_dir)
    except FORMAL_STATE_CHAIN.FormalStateError as error:
        raise ManifestError(
            f"formal state chain revalidation failed: {error}"
        ) from error
    if (
        size != actual["terminal_evidence_size"]
        or digest != actual["terminal_evidence_sha256"]
        or state_device != actual["state_device"]
        or state_inode != actual["state_inode"]
        or chain_digest != actual["formal_chain_sha256"]
        or actual["legacy_run_ledger_status"] != "absent"
    ):
        raise ManifestError(
            "formal state chain does not match its manifest binding"
        )
    return True


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
    tester_binary_verified = validate_tester_binary(manifest)
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
    formal_state_verified = validate_formal_state(
        manifest,
        by_name["formal_state_chain"]["status"],
    )
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
    expected_tester_status = (
        "not_applicable"
        if mode != "all"
        else "verified"
        if tester_binary_verified
        else "pending"
        if (
            status == "running"
            and manifest["tester_binary"]["status"] == "pending_fresh_build"
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
        ("trusted_tester_binary", expected_tester_status),
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
        and formal_state_verified
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
        try:
            rank_text = rank_bytes.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ManifestError("rank.log is not valid UTF-8") from error
        window_values, median = parse_ranked_new_order_metrics(rank_text)
        if rank_result["metrics"] != {
            "policy": RANK_METRIC_POLICY,
            "status": "verified",
            "window_values": window_values,
            "median": median,
        }:
            raise ManifestError(
                "rank.log metrics do not match their manifest binding"
            )
    elif ranking_eligible:
        raise ManifestError("manifest.json does not bind its ranked result")
    return manifest, rank_text


def parse_ranked_new_order_metrics(text: str) -> tuple[list[str], str]:
    rate = r"(?:0|[1-9][0-9]*)\.[0-9]{3}"
    window_pattern = re.compile(
        rf"^window([1-3]): new_order_per_min=({rate}),"
    )
    median_pattern = re.compile(
        rf"^ranked_new_order_per_min_median=({rate})$"
    )
    windows: list[tuple[int, str, Decimal, int]] = []
    medians: list[tuple[str, Decimal, int]] = []
    for line_number, line in enumerate(text.splitlines()):
        window = window_pattern.match(line)
        if window is not None:
            number = int(window.group(1))
            value = window.group(2)
            windows.append(
                (number, value, Decimal(value), line_number)
            )
            continue
        if line.startswith("window") and "new_order_per_min" in line:
            raise ManifestError(
                "rank.log has a malformed NewOrder/min window"
            )
        median = median_pattern.fullmatch(line)
        if median is not None:
            value = median.group(1)
            medians.append((value, Decimal(value), line_number))
            continue
        if line.startswith("ranked_new_order_per_min_median"):
            raise ManifestError(
                "rank.log has a malformed NewOrder/min median"
            )
    if [item[0] for item in windows] != [1, 2, 3]:
        raise ManifestError(
            "rank.log must contain exactly ordered windows 1, 2, and 3"
        )
    if len(medians) != 1 or medians[0][2] <= windows[-1][3]:
        raise ManifestError(
            "rank.log must contain one median after all three windows"
        )
    expected_median = sorted(item[2] for item in windows)[1]
    if medians[0][1] != expected_median:
        raise ManifestError(
            "rank.log NewOrder/min median disagrees with its windows"
        )
    return [item[1] for item in windows], medians[0][0]


def extract_ranked_new_order_metrics(text: str) -> list[str]:
    windows, median = parse_ranked_new_order_metrics(text)
    return [
        *(
            f"NewOrder/min window{number}: {value}"
            for number, value in enumerate(windows, start=1)
        ),
        f"NewOrder/min median: {median}",
    ]


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
    formal = manifest["formal_state"]
    terminal = formal["terminal_evidence"]
    legacy = formal["legacy_run_ledger"]
    summary.extend(
        [
            f"- formal_state.publication_policy: {formal['publication_policy']}",
            f"- terminal_evidence.path: `{terminal['path']}`",
            f"- terminal_evidence.status: {terminal['status']}",
            f"- terminal_evidence.file_type: {terminal['file_type']}",
            f"- terminal_evidence.open_policy: {terminal['open_policy']}",
            f"- terminal_evidence.size_bytes: {terminal['size_bytes']}",
            (
                "- terminal_evidence.max_size_bytes: "
                f"{terminal['max_size_bytes']}"
            ),
            f"- terminal_evidence.sha256: {terminal['sha256']}",
            f"- legacy_run_ledger.path: `{legacy['path']}`",
            f"- legacy_run_ledger.status: {legacy['status']}",
            (
                "- legacy_run_ledger.inspection_policy: "
                f"{legacy['inspection_policy']}"
            ),
        ]
    )
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
        metrics = extract_ranked_new_order_metrics(rank_text)
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
