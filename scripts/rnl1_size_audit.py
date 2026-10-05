#!/usr/bin/env python3
"""Read-only, bounded-memory inventory of Runnel stream-log record sizes.

The report contains lengths and validation outcomes only. It never emits stream
keys, request IDs, payload bytes, or absolute source paths. Incomplete final
records are reported without repairing them.
"""

from __future__ import annotations

import argparse
import errno
import json
import os
import platform
import stat
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO


LEGACY_MAGIC = b"RNL1"
VERSIONED_MAGIC = b"RNL2"
REQUEST_ID_MAGIC = b"RNL3"
LEGACY_HEADER_LEN = 28
VERSIONED_HEADER_LEN = 44
REQUEST_ID_HEADER_LEN = 48
VERSIONED_FORMAT_VERSION = 1
REQUEST_ID_FORMAT_VERSION = 1
VERSIONED_MAX_KEY_LEN = 128
VERSIONED_MAX_BODY_LEN = 64 * 1024 * 1024
REQUEST_ID_MAX_LEN = 1024
REQUEST_ID_MAX_KEY_LEN = 128
REQUEST_ID_MAX_BODY_LEN = 64 * 1024 * 1024
SCRATCH_BUFFER_BYTES = 64 * 1024
MAX_SIZE_HISTOGRAM_BIN = 34
FORMAT_NAMES = ("RNL1", "RNL2", "RNL3")
SIZE_FIELDS = ("key", "request_id", "payload", "record")


class AuditError(Exception):
    """A malformed or changing file prevents a trustworthy complete scan."""

    def __init__(self, reason: str, offset: int, *, changed: bool = False):
        super().__init__(reason)
        self.reason = reason
        self.offset = offset
        self.changed = changed


@dataclass(frozen=True)
class ParsedRecordSizes:
    format_name: str
    offset: int
    key_len: int
    request_id_len: int
    payload_len: int
    record_len: int


def _make_crc32c_table() -> tuple[int, ...]:
    table: list[int] = []
    for index in range(256):
        value = index
        for _ in range(8):
            value = (value >> 1) ^ (0x82F63B78 if value & 1 else 0)
        table.append(value)
    return tuple(table)


CRC32C_TABLE = _make_crc32c_table()


def _crc32c_update(checksum: int, data: bytes | bytearray | memoryview) -> int:
    for byte in data:
        checksum = (checksum >> 8) ^ CRC32C_TABLE[(checksum ^ byte) & 0xFF]
    return checksum


def _validate_utf8_chunk(state: list[int], data: memoryview) -> bool:
    """Validate UTF-8 bytes without constructing a decoded string.

    State is [continuation_bytes, codepoint, minimum_codepoint] and is retained
    across the fixed-size reads used for large legacy keys.
    """
    remaining, codepoint, minimum = state
    for byte in data:
        if remaining == 0:
            if byte <= 0x7F:
                continue
            if 0xC2 <= byte <= 0xDF:
                remaining, codepoint, minimum = 1, byte & 0x1F, 0x80
            elif 0xE0 <= byte <= 0xEF:
                remaining, codepoint, minimum = 2, byte & 0x0F, 0x800
            elif 0xF0 <= byte <= 0xF4:
                remaining, codepoint, minimum = 3, byte & 0x07, 0x10000
            else:
                return False
            continue

        if not 0x80 <= byte <= 0xBF:
            return False
        codepoint = (codepoint << 6) | (byte & 0x3F)
        remaining -= 1
        if remaining == 0 and (
            codepoint < minimum
            or codepoint > 0x10FFFF
            or 0xD800 <= codepoint <= 0xDFFF
        ):
            return False

    state[:] = [remaining, codepoint, minimum]
    return True


def _read_header(file: BinaryIO, size: int, offset: int, length: int) -> bytes:
    """Read a small fixed-format header after its bytes were checked present."""
    file.seek(offset)
    pieces = bytearray()
    while len(pieces) < length:
        chunk = file.read(length - len(pieces))
        if not chunk:
            raise AuditError("file changed during inspection", offset, changed=True)
        pieces.extend(chunk)
    if offset + length > size:
        raise AuditError("file changed during inspection", offset, changed=True)
    return bytes(pieces)


def _consume_field(
    file: BinaryIO,
    length: int,
    offset: int,
    scratch: bytearray,
    checksum: int | None,
    utf8_state: list[int] | None = None,
) -> int | None:
    remaining = length
    view = memoryview(scratch)
    while remaining:
        wanted = min(remaining, len(scratch))
        read = file.readinto(view[:wanted])
        if not read:
            raise AuditError("file changed during inspection", offset, changed=True)
        chunk = view[:read]
        if utf8_state is not None and not _validate_utf8_chunk(utf8_state, chunk):
            raise AuditError("record contains invalid UTF-8", offset)
        if checksum is not None:
            checksum = _crc32c_update(checksum, chunk)
        remaining -= read
        offset += read
    return checksum


def _finish_utf8(state: list[int], offset: int) -> None:
    if state[0] != 0:
        raise AuditError("record contains invalid UTF-8", offset)


def _size_bin(length: int) -> int:
    if length == 0:
        return 0
    return length.bit_length()


@dataclass
class SizeStats:
    records: int = 0
    key_bytes: int = 0
    request_id_bytes: int = 0
    payload_bytes: int = 0
    record_bytes: int = 0
    max_bytes: dict[str, int] = field(
        default_factory=lambda: {name: 0 for name in SIZE_FIELDS}
    )
    histogram_log2: dict[str, list[int]] = field(
        default_factory=lambda: {
            name: [0] * (MAX_SIZE_HISTOGRAM_BIN + 1) for name in SIZE_FIELDS
        }
    )

    def add(
        self, key_len: int, request_id_len: int, payload_len: int, record_len: int
    ) -> None:
        self.records += 1
        values = {
            "key": key_len,
            "request_id": request_id_len,
            "payload": payload_len,
            "record": record_len,
        }
        for name, value in values.items():
            total_field = f"{name}_bytes"
            setattr(self, total_field, getattr(self, total_field) + value)
            self.max_bytes[name] = max(self.max_bytes[name], value)
            self.histogram_log2[name][_size_bin(value)] += 1

    def to_dict(self, *, include_histogram: bool) -> dict[str, object]:
        result: dict[str, object] = {
            "records": self.records,
            "total_bytes": {
                "key": self.key_bytes,
                "request_id": self.request_id_bytes,
                "payload": self.payload_bytes,
                "record": self.record_bytes,
            },
            "max_bytes": dict(self.max_bytes),
        }
        if include_histogram:
            result["histogram_log2"] = {
                name: counts for name, counts in self.histogram_log2.items()
            }
        return result


@dataclass
class FileStats:
    name: str
    file_bytes: int
    formats: dict[str, SizeStats] = field(
        default_factory=lambda: {name: SizeStats() for name in FORMAT_NAMES}
    )
    state: str = "complete"
    trailing_bytes: int = 0
    tail_kind: str | None = None
    error: dict[str, object] | None = None
    file_bytes_after: int | None = None

    @property
    def records(self) -> int:
        return sum(stats.records for stats in self.formats.values())

    def to_dict(self) -> dict[str, object]:
        return {
            "file": self.name,
            "file_bytes_at_open": self.file_bytes,
            "file_bytes_after_scan": self.file_bytes_after,
            "state": self.state,
            "records": self.records,
            "records_by_format": {
                name: stats.records for name, stats in self.formats.items()
            },
            "max_bytes_by_format": {
                name: dict(stats.max_bytes) for name, stats in self.formats.items()
            },
            "incomplete_tail": (
                {
                    "kind": self.tail_kind,
                    "file_offset": self.file_bytes - self.trailing_bytes,
                    "bytes": self.trailing_bytes,
                }
                if self.state == "incomplete_tail"
                else None
            ),
            "error": self.error,
        }


def _same_file_snapshot(before: os.stat_result, after: os.stat_result) -> bool:
    return (
        before.st_dev == after.st_dev
        and before.st_ino == after.st_ino
        and before.st_size == after.st_size
        and before.st_atime_ns == after.st_atime_ns
        and before.st_mtime_ns == after.st_mtime_ns
        and before.st_ctime_ns == after.st_ctime_ns
    )


def _incomplete_tail(stats: FileStats, kind: str, remaining: int) -> None:
    stats.state = "incomplete_tail"
    stats.tail_kind = kind
    stats.trailing_bytes = remaining


def _scan_legacy_record(
    file: BinaryIO,
    header: bytes,
    cursor: int,
    remaining: int,
    scratch: bytearray,
) -> ParsedRecordSizes | None:
    offset = int.from_bytes(header[4:12], "little")
    key_len = int.from_bytes(header[20:24], "little")
    payload_len = int.from_bytes(header[24:28], "little")
    record_len = LEGACY_HEADER_LEN + key_len + payload_len
    if remaining < record_len:
        return None

    key_state = [0, 0, 0]
    file.seek(cursor + LEGACY_HEADER_LEN)
    _consume_field(
        file,
        key_len,
        cursor + LEGACY_HEADER_LEN,
        scratch,
        checksum=None,
        utf8_state=key_state,
    )
    _finish_utf8(key_state, cursor + LEGACY_HEADER_LEN + key_len)
    file.seek(cursor + record_len)
    return ParsedRecordSizes("RNL1", offset, key_len, 0, payload_len, record_len)


def _scan_versioned_record(
    file: BinaryIO,
    header: bytes,
    cursor: int,
    remaining: int,
    scratch: bytearray,
) -> ParsedRecordSizes | None:
    if header[4] != VERSIONED_FORMAT_VERSION:
        raise AuditError("unsupported versioned record version", cursor)
    if header[5] != 0:
        raise AuditError("unsupported versioned record flags", cursor)
    if int.from_bytes(header[6:8], "little") != VERSIONED_HEADER_LEN:
        raise AuditError("invalid versioned record header length", cursor)

    stored_len = int.from_bytes(header[8:12], "little")
    logical_len = int.from_bytes(header[12:16], "little")
    offset = int.from_bytes(header[16:24], "little")
    key_len = int.from_bytes(header[32:36], "little")
    if stored_len > VERSIONED_MAX_BODY_LEN or logical_len > VERSIONED_MAX_BODY_LEN:
        raise AuditError("versioned record exceeds storage limit", cursor)
    if logical_len != stored_len:
        raise AuditError("compressed versioned records are not supported", cursor)
    if key_len > VERSIONED_MAX_KEY_LEN:
        raise AuditError("versioned record key exceeds storage limit", cursor)
    if header[36] != 0:
        raise AuditError("unsupported versioned record encoding", cursor)
    if header[37] != 0:
        raise AuditError("unsupported versioned record compression", cursor)
    if int.from_bytes(header[38:40], "little") != 0:
        raise AuditError("unsupported versioned record header fields", cursor)

    record_len = VERSIONED_HEADER_LEN + key_len + stored_len
    if remaining < record_len:
        return None

    expected_checksum = int.from_bytes(header[40:44], "little")
    checksum_header = bytearray(header)
    checksum_header[40:44] = b"\0" * 4
    checksum = _crc32c_update(0xFFFFFFFF, checksum_header)
    file.seek(cursor + VERSIONED_HEADER_LEN)
    key_state = [0, 0, 0]
    checksum = _consume_field(
        file,
        key_len,
        cursor + VERSIONED_HEADER_LEN,
        scratch,
        checksum,
        key_state,
    )
    _finish_utf8(key_state, cursor + VERSIONED_HEADER_LEN + key_len)
    checksum = _consume_field(
        file,
        stored_len,
        cursor + VERSIONED_HEADER_LEN + key_len,
        scratch,
        checksum,
    )
    if (~checksum & 0xFFFFFFFF) != expected_checksum:
        raise AuditError("versioned record checksum mismatch", cursor)
    return ParsedRecordSizes("RNL2", offset, key_len, 0, stored_len, record_len)


def _scan_request_id_record(
    file: BinaryIO,
    header: bytes,
    cursor: int,
    remaining: int,
    scratch: bytearray,
) -> ParsedRecordSizes | None:
    if header[4] != REQUEST_ID_FORMAT_VERSION:
        raise AuditError("unsupported request-aware record version", cursor)
    if header[5] != 0:
        raise AuditError("unsupported request-aware record flags", cursor)
    if int.from_bytes(header[6:8], "little") != REQUEST_ID_HEADER_LEN:
        raise AuditError("invalid request-aware record header length", cursor)

    stored_len = int.from_bytes(header[8:12], "little")
    logical_len = int.from_bytes(header[12:16], "little")
    offset = int.from_bytes(header[16:24], "little")
    key_len = int.from_bytes(header[32:36], "little")
    request_id_len = int.from_bytes(header[36:40], "little")
    if key_len > REQUEST_ID_MAX_KEY_LEN:
        raise AuditError("request-aware record key exceeds storage limit", cursor)
    if request_id_len > REQUEST_ID_MAX_LEN:
        raise AuditError("request-aware record ID exceeds storage limit", cursor)
    if stored_len > REQUEST_ID_MAX_BODY_LEN or logical_len > REQUEST_ID_MAX_BODY_LEN:
        raise AuditError("request-aware record exceeds storage limit", cursor)
    if logical_len != stored_len:
        raise AuditError("compressed request-aware records are not supported", cursor)
    if header[40:44] != b"\0" * 4:
        raise AuditError("unsupported request-aware record header fields", cursor)

    record_len = REQUEST_ID_HEADER_LEN + key_len + request_id_len + stored_len
    if remaining < record_len:
        return None

    expected_checksum = int.from_bytes(header[44:48], "little")
    checksum_header = bytearray(header)
    checksum_header[44:48] = b"\0" * 4
    checksum = _crc32c_update(0xFFFFFFFF, checksum_header)
    file.seek(cursor + REQUEST_ID_HEADER_LEN)
    key_state = [0, 0, 0]
    checksum = _consume_field(
        file,
        key_len,
        cursor + REQUEST_ID_HEADER_LEN,
        scratch,
        checksum,
        key_state,
    )
    _finish_utf8(key_state, cursor + REQUEST_ID_HEADER_LEN + key_len)
    request_id_state = [0, 0, 0]
    checksum = _consume_field(
        file,
        request_id_len,
        cursor + REQUEST_ID_HEADER_LEN + key_len,
        scratch,
        checksum,
        request_id_state,
    )
    _finish_utf8(
        request_id_state,
        cursor + REQUEST_ID_HEADER_LEN + key_len + request_id_len,
    )
    checksum = _consume_field(
        file,
        stored_len,
        cursor + REQUEST_ID_HEADER_LEN + key_len + request_id_len,
        scratch,
        checksum,
    )
    if (~checksum & 0xFFFFFFFF) != expected_checksum:
        raise AuditError("request-aware record checksum mismatch", cursor)
    return ParsedRecordSizes("RNL3", offset, key_len, request_id_len, stored_len, record_len)


def _scan_open_file(file: BinaryIO, stats: FileStats) -> None:
    size = stats.file_bytes
    cursor = 0
    expected_offset = 0
    scratch = bytearray(SCRATCH_BUFFER_BYTES)

    while cursor < size:
        remaining = size - cursor
        if remaining < 4:
            _incomplete_tail(stats, "incomplete_magic", remaining)
            return

        magic = _read_header(file, size, cursor, 4)
        if magic == LEGACY_MAGIC:
            header_len, scanner = LEGACY_HEADER_LEN, _scan_legacy_record
        elif magic == VERSIONED_MAGIC:
            header_len, scanner = VERSIONED_HEADER_LEN, _scan_versioned_record
        elif magic == REQUEST_ID_MAGIC:
            header_len, scanner = REQUEST_ID_HEADER_LEN, _scan_request_id_record
        else:
            raise AuditError("unsupported record magic", cursor)

        if remaining < header_len:
            _incomplete_tail(stats, "incomplete_header", remaining)
            return

        header = _read_header(file, size, cursor, header_len)
        record = scanner(file, header, cursor, remaining, scratch)
        if record is None:
            _incomplete_tail(stats, "incomplete_record", remaining)
            return
        if record.offset != expected_offset:
            raise AuditError("record offsets are not contiguous", cursor)
        if expected_offset == 0xFFFFFFFFFFFFFFFF:
            raise AuditError("record offset exceeds u64 range", cursor)

        stats.formats[record.format_name].add(
            record.key_len,
            record.request_id_len,
            record.payload_len,
            record.record_len,
        )
        expected_offset += 1
        cursor += record.record_len


def _no_mutation_flags(*, directory: bool = False) -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    noatime = getattr(os, "O_NOATIME", None)
    directory_flag = getattr(os, "O_DIRECTORY", None) if directory else 0
    if nofollow is None or noatime is None or directory_flag is None:
        raise OSError(
            errno.ENOTSUP,
            "this platform cannot guarantee no-atime, no-follow inspection",
        )
    return os.O_RDONLY | nofollow | noatime | directory_flag


def _open_stream_directory(path: Path) -> int:
    return os.open(path, _no_mutation_flags(directory=True))


def _open_regular_read_only(name: str, directory_fd: int) -> BinaryIO:
    metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
    if stat.S_ISLNK(metadata.st_mode):
        raise OSError(errno.ELOOP, "symbolic link refused")
    descriptor = os.open(
        name,
        _no_mutation_flags(),
        dir_fd=directory_fd,
    )
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise OSError(errno.EINVAL, "not a regular file")
        return os.fdopen(descriptor, "rb", buffering=0)
    except BaseException:
        os.close(descriptor)
        raise


def _audit_log(name: str, directory_fd: int) -> FileStats:
    try:
        with _open_regular_read_only(name, directory_fd) as file:
            before = os.fstat(file.fileno())
            stats = FileStats(name, before.st_size)
            try:
                _scan_open_file(file, stats)
            except AuditError as error:
                stats.state = "changed_during_scan" if error.changed else "malformed"
                stats.error = {
                    "file_offset": error.offset,
                    "reason": error.reason,
                }
            after = os.fstat(file.fileno())
            stats.file_bytes_after = after.st_size
            if not _same_file_snapshot(before, after):
                stats.state = "changed_during_scan"
                stats.error = {
                    "file_offset": (
                        stats.error["file_offset"] if stats.error else None
                    ),
                    "reason": "file metadata changed during inspection",
                }
            return stats
    except OSError as error:
        return FileStats(
            name=name,
        file_bytes=0,
        state="unreadable",
        error={
            "file_offset": None,
            "reason": f"could not open/read regular log ({error.strerror})",
        },
        )


def audit_data_directory(data_directory: Path) -> dict[str, object]:
    """Audit direct `streams/*.log` files without changing the source tree."""
    streams_directory = data_directory / "streams"
    directory_fd = _open_stream_directory(streams_directory)
    try:
        directory_before = os.fstat(directory_fd)
        names: list[str] = []
        with os.scandir(directory_fd) as entries:
            for entry in entries:
                if Path(entry.name).suffix == ".log":
                    names.append(entry.name)
        names.sort()
        file_results = [_audit_log(name, directory_fd) for name in names]
        directory_after = os.fstat(directory_fd)
        directory_changed = not _same_file_snapshot(directory_before, directory_after)
    finally:
        os.close(directory_fd)
    totals = {name: SizeStats() for name in FORMAT_NAMES}
    for result in file_results:
        for name in FORMAT_NAMES:
            source = result.formats[name]
            target = totals[name]
            target.records += source.records
            target.key_bytes += source.key_bytes
            target.request_id_bytes += source.request_id_bytes
            target.payload_bytes += source.payload_bytes
            target.record_bytes += source.record_bytes
            for field_name in SIZE_FIELDS:
                target.max_bytes[field_name] = max(
                    target.max_bytes[field_name], source.max_bytes[field_name]
                )
                target.histogram_log2[field_name] = [
                    left + right
                    for left, right in zip(
                        target.histogram_log2[field_name],
                        source.histogram_log2[field_name],
                    )
                ]

    state_counts: dict[str, int] = {}
    for result in file_results:
        state_counts[result.state] = state_counts.get(result.state, 0) + 1
    report_state = (
        "malformed"
        if state_counts.get("malformed", 0)
        else "unreadable"
        if state_counts.get("unreadable", 0)
        else "changed_during_scan"
        if directory_changed or state_counts.get("changed_during_scan", 0)
        else "incomplete_tails"
        if state_counts.get("incomplete_tail", 0)
        else "complete"
    )
    return {
        "tool": "runnel-record-size-audit",
        "schema_version": 1,
        "scanner_runtime": {
            "python_version": platform.python_version(),
            "platform": sys.platform,
        },
        "report_state": report_state,
        "read_only": True,
        "open_flags": ["O_RDONLY", "O_NOFOLLOW", "O_NOATIME"],
        "directory_changed_during_scan": directory_changed,
        "scratch_buffer_bytes": SCRATCH_BUFFER_BYTES,
        "validated": [
            "format headers and current version-specific limits",
            "record completeness against file size captured at open",
            "contiguous offsets starting at zero",
            "UTF-8 keys and RNL3 request IDs",
            "RNL2 and RNL3 CRC32C checksums",
        ],
        "not_validated": [
            "RNL1 payload contents (no checksum; recovery does not inspect them)"
        ],
        "totals_scope": (
            "summaries include only complete records validated before each file's "
            "reported tail, malformed record, or change during inspection"
        ),
        "histogram_definition": (
            "histogram_log2 index 0 counts length 0; index 1 counts length 1; "
            "index n >= 2 counts [2^(n-1), 2^n-1]"
        ),
        "files": [result.to_dict() for result in file_results],
        "file_states": state_counts,
        "totals_by_format": {
            name: stats.to_dict(include_histogram=True)
            for name, stats in totals.items()
        },
        "scan_completed_utc": datetime.now(timezone.utc).isoformat(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "data_directory",
        type=Path,
        help="Runnel data directory containing streams/*.log",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        report = audit_data_directory(args.data_directory)
    except (OSError, ValueError) as error:
        print(f"runnel-record-size-audit: {error}", file=sys.stderr)
        return 2

    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["report_state"] == "complete" else 1


if __name__ == "__main__":
    raise SystemExit(main())
