import io
import json
import struct
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR.parent))

import rnl1_size_audit as audit  # noqa: E402


def legacy_record(offset: int, key: bytes, payload: bytes) -> bytes:
    return (
        b"RNL1"
        + struct.pack("<QQII", offset, 1234, len(key), len(payload))
        + key
        + payload
    )


def versioned_record(offset: int, key: bytes, payload: bytes) -> bytes:
    header = bytearray(audit.VERSIONED_HEADER_LEN)
    header[:4] = audit.VERSIONED_MAGIC
    header[4] = audit.VERSIONED_FORMAT_VERSION
    header[6:8] = struct.pack("<H", audit.VERSIONED_HEADER_LEN)
    header[8:12] = struct.pack("<I", len(payload))
    header[12:16] = struct.pack("<I", len(payload))
    header[16:24] = struct.pack("<Q", offset)
    header[24:32] = struct.pack("<Q", 5678)
    header[32:36] = struct.pack("<I", len(key))
    checksum = audit._crc32c_update(0xFFFFFFFF, header)
    checksum = audit._crc32c_update(checksum, key)
    checksum = audit._crc32c_update(checksum, payload)
    header[40:44] = struct.pack("<I", ~checksum & 0xFFFFFFFF)
    return bytes(header) + key + payload


def request_id_record(
    offset: int, key: bytes, request_id: bytes, payload: bytes
) -> bytes:
    header = bytearray(audit.REQUEST_ID_HEADER_LEN)
    header[:4] = audit.REQUEST_ID_MAGIC
    header[4] = audit.REQUEST_ID_FORMAT_VERSION
    header[6:8] = struct.pack("<H", audit.REQUEST_ID_HEADER_LEN)
    header[8:12] = struct.pack("<I", len(payload))
    header[12:16] = struct.pack("<I", len(payload))
    header[16:24] = struct.pack("<Q", offset)
    header[24:32] = struct.pack("<Q", 9012)
    header[32:36] = struct.pack("<I", len(key))
    header[36:40] = struct.pack("<I", len(request_id))
    checksum = audit._crc32c_update(0xFFFFFFFF, header)
    checksum = audit._crc32c_update(checksum, key)
    checksum = audit._crc32c_update(checksum, request_id)
    checksum = audit._crc32c_update(checksum, payload)
    header[44:48] = struct.pack("<I", ~checksum & 0xFFFFFFFF)
    return bytes(header) + key + request_id + payload


def data_directory(parent: Path) -> Path:
    data = parent / "data"
    (data / "streams").mkdir(parents=True)
    return data


class Rnl1SizeAuditTests(unittest.TestCase):
    def test_crc32c_uses_the_castagnoli_check_value(self) -> None:
        checksum = audit._crc32c_update(0xFFFFFFFF, b"123456789")
        self.assertEqual(~checksum & 0xFFFFFFFF, 0xE3069283)

    def test_cli_emits_json_and_returns_nonzero_for_incomplete_tail(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data = data_directory(Path(temporary))
            log = data / "streams" / "events.log"
            log.write_bytes(legacy_record(0, b"key", b"payload"))
            output = io.StringIO()

            with redirect_stdout(output):
                complete_exit = audit.main([str(data)])

            self.assertEqual(complete_exit, 0)
            self.assertEqual(json.loads(output.getvalue())["report_state"], "complete")

            with log.open("ab") as file:
                file.write(b"RNL")
            output = io.StringIO()
            with redirect_stdout(output):
                incomplete_exit = audit.main([str(data)])

            self.assertEqual(incomplete_exit, 1)
            report = json.loads(output.getvalue())
            self.assertEqual(report["report_state"], "incomplete_tails")
            self.assertEqual(
                report["files"][0]["incomplete_tail"]["kind"], "incomplete_magic"
            )

    def test_mixed_history_reports_lengths_without_contents_or_mutation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data = data_directory(Path(temporary))
            log = data / "streams" / "events.log"
            original = b"".join(
                [
                    legacy_record(0, "hëllo".encode(), b"legacy-secret"),
                    versioned_record(1, b"rnl2-key", b"rnl2-payload-secret"),
                    request_id_record(
                        2,
                        b"rnl3-key",
                        b"request-id-secret",
                        b"rnl3-payload-secret",
                    ),
                ]
            )
            log.write_bytes(original)
            file_before = log.stat()
            directory_before = (data / "streams").stat()

            report = audit.audit_data_directory(data)
            file_after = log.stat()
            directory_after = (data / "streams").stat()

            self.assertEqual(report["report_state"], "complete")
            self.assertTrue(report["read_only"])
            self.assertEqual(
                {
                    name: report["totals_by_format"][name]["records"]
                    for name in audit.FORMAT_NAMES
                },
                {"RNL1": 1, "RNL2": 1, "RNL3": 1},
            )
            self.assertEqual(
                report["totals_by_format"]["RNL1"]["max_bytes"]["key"],
                len("hëllo".encode()),
            )
            self.assertEqual(
                report["totals_by_format"]["RNL3"]["max_bytes"]["request_id"],
                len(b"request-id-secret"),
            )
            encoded_report = json.dumps(report)
            for secret in (
                "hëllo",
                "legacy-secret",
                "rnl2-key",
                "rnl2-payload-secret",
                "rnl3-key",
                "request-id-secret",
                "rnl3-payload-secret",
            ):
                self.assertNotIn(secret, encoded_report)
            for before, after in (
                (file_before, file_after),
                (directory_before, directory_after),
            ):
                self.assertEqual(after.st_atime_ns, before.st_atime_ns)
                self.assertEqual(after.st_mtime_ns, before.st_mtime_ns)
                self.assertEqual(after.st_ctime_ns, before.st_ctime_ns)
            self.assertEqual(log.read_bytes(), original)

    def test_symbolic_link_is_reported_without_following_it(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data = data_directory(Path(temporary))
            target = data / "streams" / "real.log"
            link = data / "streams" / "linked.log"
            original = legacy_record(0, b"key", b"payload")
            target.write_bytes(original)
            link.symlink_to(target.name)
            target_before = target.stat()
            link_before = link.lstat()
            directory_before = (data / "streams").stat()

            report = audit.audit_data_directory(data)

            target_after = target.stat()
            link_after = link.lstat()
            directory_after = (data / "streams").stat()
            by_name = {entry["file"]: entry for entry in report["files"]}
            self.assertEqual(by_name["real.log"]["state"], "complete")
            self.assertEqual(by_name["linked.log"]["state"], "unreadable")
            self.assertIn("symbolic link refused", by_name["linked.log"]["error"]["reason"])
            for before, after in (
                (target_before, target_after),
                (link_before, link_after),
                (directory_before, directory_after),
            ):
                self.assertEqual(after.st_atime_ns, before.st_atime_ns)
                self.assertEqual(after.st_mtime_ns, before.st_mtime_ns)
                self.assertEqual(after.st_ctime_ns, before.st_ctime_ns)
            self.assertEqual(target.read_bytes(), original)

    def test_file_changed_during_scan_is_flagged(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data = data_directory(Path(temporary))
            log = data / "streams" / "events.log"
            log.write_bytes(legacy_record(0, b"key", b"payload"))
            scanner = audit._scan_open_file

            def append_after_snapshot(file, stats):
                scanner(file, stats)
                with log.open("ab") as output:
                    output.write(b"external-append")

            with patch.object(audit, "_scan_open_file", side_effect=append_after_snapshot):
                report = audit.audit_data_directory(data)

            result = report["files"][0]
            self.assertEqual(report["report_state"], "changed_during_scan")
            self.assertEqual(result["state"], "changed_during_scan")
            self.assertGreater(
                result["file_bytes_after_scan"], result["file_bytes_at_open"]
            )
            self.assertTrue(log.read_bytes().endswith(b"external-append"))

    def test_complete_malformed_legacy_key_is_reported_and_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data = data_directory(Path(temporary))
            log = data / "streams" / "legacy.log"
            original = legacy_record(0, b"\xff", b"not-emitted")
            log.write_bytes(original)

            report = audit.audit_data_directory(data)

            self.assertEqual(report["report_state"], "malformed")
            result = report["files"][0]
            self.assertEqual(result["state"], "malformed")
            self.assertEqual(result["error"]["file_offset"], audit.LEGACY_HEADER_LEN)
            self.assertEqual(result["error"]["reason"], "record contains invalid UTF-8")
            self.assertNotIn("not-emitted", json.dumps(report))
            self.assertEqual(log.read_bytes(), original)

    def test_current_format_checksum_mismatch_is_malformed_and_preserved(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data = data_directory(Path(temporary))
            legacy = legacy_record(0, b"ok", b"legacy")
            versioned = bytearray(versioned_record(1, b"key", b"body"))
            request_id = bytearray(request_id_record(1, b"key", b"id", b"body"))
            versioned[-1] ^= 1
            request_id[-1] ^= 1
            files = {
                "versioned.log": bytes(versioned),
                "request-aware.log": bytes(request_id),
                "legacy.log": legacy,
            }
            for name, contents in files.items():
                (data / "streams" / name).write_bytes(contents)

            report = audit.audit_data_directory(data)

            by_name = {entry["file"]: entry for entry in report["files"]}
            self.assertEqual(by_name["versioned.log"]["state"], "malformed")
            self.assertEqual(
                by_name["versioned.log"]["error"]["reason"],
                "versioned record checksum mismatch",
            )
            self.assertEqual(by_name["request-aware.log"]["state"], "malformed")
            self.assertEqual(
                by_name["request-aware.log"]["error"]["reason"],
                "request-aware record checksum mismatch",
            )
            self.assertEqual(by_name["legacy.log"]["state"], "complete")
            for name, contents in files.items():
                self.assertEqual((data / "streams" / name).read_bytes(), contents)

    def test_incomplete_headers_and_frames_are_reported_without_repair(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data = data_directory(Path(temporary))
            huge_legacy_tail = (
                b"RNL1"
                + struct.pack("<QQII", 0, 1234, 0xFFFFFFFF, 0xFFFFFFFF)
            )
            valid_legacy = legacy_record(0, b"", b"abc")
            valid_versioned = versioned_record(0, b"k", b"body")
            valid_request = request_id_record(0, b"k", b"id", b"body")
            tails = {
                "short_magic.log": b"RNL",
                "legacy_header.log": b"RNL1partial",
                "legacy_huge_declared.log": huge_legacy_tail,
                "versioned_header.log": b"RNL2partial",
                "versioned_body.log": valid_versioned[:-1],
                "request_header.log": b"RNL3partial",
                "request_body.log": valid_request[:-1],
                "legacy_after_record.log": valid_legacy + b"RNL1tail",
            }
            for name, contents in tails.items():
                (data / "streams" / name).write_bytes(contents)

            before = {
                name: (data / "streams" / name).read_bytes() for name in tails
            }
            report = audit.audit_data_directory(data)

            self.assertEqual(report["report_state"], "incomplete_tails")
            self.assertEqual(
                report["file_states"], {"incomplete_tail": len(tails)}
            )
            by_name = {entry["file"]: entry for entry in report["files"]}
            self.assertEqual(
                by_name["legacy_huge_declared.log"]["records"], 0
            )
            self.assertEqual(
                by_name["legacy_huge_declared.log"]["incomplete_tail"]["kind"],
                "incomplete_record",
            )
            for name, contents in before.items():
                self.assertEqual((data / "streams" / name).read_bytes(), contents)

    def test_format_limits_are_validated_before_incomplete_body_classification(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            data = data_directory(Path(temporary))
            header = bytearray(audit.VERSIONED_HEADER_LEN)
            header[:4] = audit.VERSIONED_MAGIC
            header[4] = audit.VERSIONED_FORMAT_VERSION
            header[6:8] = struct.pack("<H", audit.VERSIONED_HEADER_LEN)
            header[8:12] = struct.pack("<I", audit.VERSIONED_MAX_BODY_LEN + 1)
            header[12:16] = header[8:12]
            path = data / "streams" / "invalid-limit.log"
            path.write_bytes(header)

            report = audit.audit_data_directory(data)

            self.assertEqual(report["files"][0]["state"], "malformed")
            self.assertEqual(
                report["files"][0]["error"]["reason"],
                "versioned record exceeds storage limit",
            )
            self.assertEqual(path.read_bytes(), bytes(header))


if __name__ == "__main__":
    unittest.main()
