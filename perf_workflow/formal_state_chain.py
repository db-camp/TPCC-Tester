#!/usr/bin/env python3
"""Cross-language final2026 formal-state chain inspection."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
from typing import Iterable


FORMAL_CHAIN_DOMAIN = b"RMDB_TPCC_FORMAL_CHAIN_V2\0"
FORMAL_CHAIN_FILES = (
    "dataset.state",
    "setup.started",
    "setup.execution.started",
    "run_contract.state",
    "setup_check.started",
    "setup_check.passed",
    "rank.started",
    "terminal_evidence.state",
    "online_check.started",
    "float_baseline.state",
    "crash.intent",
    "crash.killed",
    "restart.started",
    "restart.ready",
    "recovery_check.started",
    "recovery_check.passed",
)
TERMINAL_EVIDENCE_FILE = "terminal_evidence.state"
LEGACY_RUN_LEDGER_FILE = "run_ledger.state"
LEGACY_RUN_LEDGER_TEMP = re.compile(
    r"^\.run_ledger\.state\."
    r"(?:0|[1-9][0-9]*)\."
    r"(?:0|[1-9][0-9]*)\.tmp$"
)
DATABASE_IDENTITY_FILE = "database.identity"
DIAGNOSTIC_STATE_FILES = (
    "diagnostic_warmup.started",
    "diagnostic_warmup.passed",
    "diagnostic_observation.started",
    "diagnostic_observation.passed",
)
ALLOWED_STATE_FILES = frozenset(
    (*FORMAL_CHAIN_FILES, DATABASE_IDENTITY_FILE, *DIAGNOSTIC_STATE_FILES)
)
MAX_TERMINAL_EVIDENCE_BYTES = 16 * 1024 * 1024 + 128 + 4 * 1024
MAX_FORMAL_CHAIN_CONTENT_BYTES = 32 * 1024 * 1024


class FormalStateError(RuntimeError):
    """The state directory cannot prove one immutable formal chain."""


def _u32(value: int) -> bytes:
    if value < 0 or value > (1 << 32) - 1:
        raise FormalStateError("formal chain u32 value is out of range")
    return struct.pack(">I", value)


def _u64(value: int) -> bytes:
    if value < 0 or value > (1 << 64) - 1:
        raise FormalStateError("formal chain u64 value is out of range")
    return struct.pack(">Q", value)


def compute_formal_chain_digest(
    state_device: int,
    state_inode: int,
    entries: Iterable[tuple[str, bytes]],
) -> str:
    canonical_entries = tuple(entries)
    if tuple(name for name, _ in canonical_entries) != FORMAL_CHAIN_FILES:
        raise FormalStateError("formal chain files are not in canonical order")
    digest = hashlib.sha256()
    digest.update(FORMAL_CHAIN_DOMAIN)
    digest.update(_u64(state_device))
    digest.update(_u64(state_inode))
    digest.update(_u32(len(canonical_entries)))
    for name, content in canonical_entries:
        try:
            encoded_name = name.encode("ascii")
        except UnicodeEncodeError as error:
            raise FormalStateError(
                "formal chain artifact name is not ASCII"
            ) from error
        if not isinstance(content, bytes):
            raise FormalStateError("formal chain artifact content is not bytes")
        digest.update(_u32(len(encoded_name)))
        digest.update(encoded_name)
        digest.update(_u64(len(content)))
        digest.update(content)
        digest.update(_u64(len(content)))
    return digest.hexdigest()


def _same_file(left: os.stat_result, right: os.stat_result) -> bool:
    return (
        left.st_dev == right.st_dev
        and left.st_ino == right.st_ino
        and left.st_mode == right.st_mode
        and left.st_size == right.st_size
        and left.st_mtime_ns == right.st_mtime_ns
    )


def _validate_directory_entries(directory_descriptor: int) -> None:
    try:
        entries = os.listdir(directory_descriptor)
    except OSError as error:
        raise FormalStateError("could not enumerate the formal state directory") from error
    for name in entries:
        if name == LEGACY_RUN_LEDGER_FILE or LEGACY_RUN_LEDGER_TEMP.fullmatch(name):
            raise FormalStateError(f"forbidden legacy state entry is present: {name}")
        if name not in ALLOWED_STATE_FILES:
            raise FormalStateError(f"unknown formal state entry is present: {name}")
        try:
            metadata = os.stat(
                name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except OSError as error:
            raise FormalStateError(
                f"could not inspect formal state entry: {name}"
            ) from error
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 0:
            raise FormalStateError(f"formal state entry is unsafe: {name}")
        if metadata.st_size > MAX_FORMAL_CHAIN_CONTENT_BYTES:
            raise FormalStateError(f"formal state entry is oversized: {name}")
    if DATABASE_IDENTITY_FILE not in entries:
        raise FormalStateError("database identity is missing from formal state")


def _read_regular_file(directory_descriptor: int, name: str) -> bytes:
    try:
        before = os.stat(
            name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
    except OSError as error:
        raise FormalStateError(f"required formal artifact is unavailable: {name}") from error
    if not stat.S_ISREG(before.st_mode) or before.st_size <= 0:
        raise FormalStateError(
            f"required formal artifact is not a non-empty regular file: {name}"
        )
    if before.st_size > MAX_FORMAL_CHAIN_CONTENT_BYTES:
        raise FormalStateError(f"required formal artifact is oversized: {name}")
    if (
        name == TERMINAL_EVIDENCE_FILE
        and before.st_size > MAX_TERMINAL_EVIDENCE_BYTES
    ):
        raise FormalStateError("terminal evidence is oversized")

    flags = os.O_RDONLY | os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(name, flags, dir_fd=directory_descriptor)
    except OSError as error:
        raise FormalStateError(f"could not safely open formal artifact: {name}") from error
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or not _same_file(before, opened):
            raise FormalStateError(f"formal artifact changed while opening: {name}")
        remaining = opened.st_size
        chunks: list[bytes] = []
        while remaining:
            try:
                chunk = os.read(descriptor, min(1024 * 1024, remaining))
            except OSError as error:
                raise FormalStateError(
                    f"could not read formal artifact: {name}"
                ) from error
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        try:
            current = os.stat(
                name,
                dir_fd=directory_descriptor,
                follow_symlinks=False,
            )
        except OSError as error:
            raise FormalStateError(
                f"formal artifact disappeared while reading: {name}"
            ) from error
        if (
            remaining != 0
            or not _same_file(opened, after)
            or not _same_file(opened, current)
        ):
            raise FormalStateError(f"formal artifact changed while reading: {name}")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


def inspect_formal_state_fd(
    directory_descriptor: int,
    *,
    expected_path: str | os.PathLike[str] | None = None,
) -> dict[str, object]:
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise FormalStateError("safe no-follow directory inspection is unavailable")
    before = os.fstat(directory_descriptor)
    if not stat.S_ISDIR(before.st_mode):
        raise FormalStateError("formal state descriptor is not a directory")
    if before.st_dev < 0 or before.st_ino < 0:
        raise FormalStateError("formal state directory identity is invalid")
    if expected_path is not None:
        try:
            path_before = os.stat(expected_path, follow_symlinks=False)
        except OSError as error:
            raise FormalStateError("formal state path is unavailable") from error
        if (
            not stat.S_ISDIR(path_before.st_mode)
            or path_before.st_dev != before.st_dev
            or path_before.st_ino != before.st_ino
        ):
            raise FormalStateError("formal state path changed while opening")

    _validate_directory_entries(directory_descriptor)
    entries: list[tuple[str, bytes]] = []
    total_size = 0
    for name in FORMAL_CHAIN_FILES:
        content = _read_regular_file(directory_descriptor, name)
        total_size += len(content)
        if total_size > MAX_FORMAL_CHAIN_CONTENT_BYTES:
            raise FormalStateError("formal state chain content is oversized")
        entries.append((name, content))
    _validate_directory_entries(directory_descriptor)

    after = os.fstat(directory_descriptor)
    if after.st_dev != before.st_dev or after.st_ino != before.st_ino:
        raise FormalStateError("formal state directory identity changed")
    if expected_path is not None:
        try:
            path_after = os.stat(expected_path, follow_symlinks=False)
        except OSError as error:
            raise FormalStateError("formal state path disappeared") from error
        if (
            not stat.S_ISDIR(path_after.st_mode)
            or path_after.st_dev != before.st_dev
            or path_after.st_ino != before.st_ino
        ):
            raise FormalStateError("formal state path changed while hashing")

    by_name = dict(entries)
    terminal = by_name[TERMINAL_EVIDENCE_FILE]
    return {
        "status": "verified",
        "policy": "formal_state_chain_v2",
        "state_device": before.st_dev,
        "state_inode": before.st_ino,
        "file_count": len(entries),
        "files": list(FORMAL_CHAIN_FILES),
        "total_content_bytes": total_size,
        "terminal_evidence_size": len(terminal),
        "terminal_evidence_sha256": hashlib.sha256(terminal).hexdigest(),
        "formal_chain_sha256": compute_formal_chain_digest(
            before.st_dev,
            before.st_ino,
            entries,
        ),
        "legacy_run_ledger_status": "absent",
    }


def inspect_formal_state_path(
    state_dir: str | os.PathLike[str],
    *,
    lock_operation: int = fcntl.LOCK_SH,
) -> dict[str, object]:
    path = Path(state_dir)
    if not path.is_absolute():
        raise FormalStateError("formal state path must be absolute")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise FormalStateError("could not safely open formal state directory") from error
    try:
        fcntl.flock(descriptor, lock_operation)
        try:
            return inspect_formal_state_fd(
                descriptor,
                expected_path=path,
            )
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--state-dir", required=True)
    arguments = parser.parse_args()
    try:
        inspection = inspect_formal_state_path(arguments.state_dir)
    except FormalStateError as error:
        print(f"formal state inspection failed: {error}", file=os.sys.stderr)
        return 2
    json.dump(inspection, os.sys.stdout, sort_keys=True)
    os.sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
