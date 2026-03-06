# tests/test_idempotency.py
"""
Tests สำหรับ idempotency check ใน utils/hdfs.py
- compute_file_checksum
- hdfs_write_done / hdfs_read_done
- is_already_processed
"""

import json
import pytest


# ── Mock helpers: import จาก conftest ────────────────────────────
from conftest import make_hdfs_sc as make_sc


# ── Tests: compute_file_checksum ──────────────────────────────────

class TestComputeFileChecksum:

    def test_returns_sha256_hex(self):
        import hashlib
        from utils.hdfs import compute_file_checksum

        content = b"hello world finance data"
        path = "hdfs://namenode:8020/datalake/raw/test.csv"
        sc, fs, files, written = make_sc({path: content})

        result = compute_file_checksum(sc, path)

        expected = hashlib.sha256(content).hexdigest()
        assert result == expected

    def test_raises_if_file_not_found(self):
        from utils.hdfs import compute_file_checksum

        sc, fs, files, written = make_sc({})
        with pytest.raises(FileNotFoundError):
            compute_file_checksum(sc, "hdfs://namenode:8020/nonexistent.csv")

    def test_same_content_same_checksum(self):
        from utils.hdfs import compute_file_checksum

        content = b"budget data 2024"
        path = "hdfs://namenode:8020/file.csv"
        sc, _, _, _ = make_sc({path: content})

        r1 = compute_file_checksum(sc, path)
        r2 = compute_file_checksum(sc, path)
        assert r1 == r2

    def test_different_content_different_checksum(self):
        from utils.hdfs import compute_file_checksum

        path = "hdfs://namenode:8020/file.csv"
        sc1, _, _, _ = make_sc({path: b"content A"})
        sc2, _, _, _ = make_sc({path: b"content B"})

        assert compute_file_checksum(sc1, path) != compute_file_checksum(sc2, path)


# ── Tests: hdfs_write_done / hdfs_read_done ───────────────────────

class TestDoneMarker:

    def test_write_then_read_checksum(self):
        from utils.hdfs import hdfs_write_done, hdfs_read_done

        csv_path = "hdfs://namenode:8020/datalake/raw/finance_2024.csv"
        checksum = "abc123def456"
        sc, fs, files, written = make_sc({})

        hdfs_write_done(sc, csv_path, checksum)
        result = hdfs_read_done(sc, csv_path)

        assert result == checksum

    def test_done_file_contains_source_name(self):
        from utils.hdfs import hdfs_write_done

        csv_path = "hdfs://namenode:8020/datalake/raw/finance_2024.csv"
        sc, fs, files, written = make_sc({})

        hdfs_write_done(sc, csv_path, "somechecksum")

        done_content = written.get(csv_path + ".done", b"")
        data = json.loads(done_content.decode("utf-8"))
        assert data["source"] == "finance_2024.csv"
        assert data["checksum"] == "somechecksum"

    def test_read_empty_done_returns_none(self):
        """Legacy .done ที่เป็นไฟล์เปล่า → return None"""
        from utils.hdfs import hdfs_read_done

        csv_path = "hdfs://namenode:8020/datalake/raw/finance_2024.csv"
        sc, fs, files, written = make_sc({csv_path + ".done": b""})

        result = hdfs_read_done(sc, csv_path)
        assert result is None

    def test_read_missing_done_returns_none(self):
        from utils.hdfs import hdfs_read_done

        csv_path = "hdfs://namenode:8020/datalake/raw/finance_2024.csv"
        sc, fs, files, written = make_sc({})

        result = hdfs_read_done(sc, csv_path)
        assert result is None


# ── Tests: is_already_processed ──────────────────────────────────

class TestIsAlreadyProcessed:

    def test_no_done_file_returns_false(self):
        from utils.hdfs import is_already_processed

        csv_path = "hdfs://namenode:8020/raw/file.csv"
        sc, _, _, _ = make_sc({csv_path: b"data"})

        assert is_already_processed(sc, csv_path) is False

    def test_checksum_match_returns_true(self):
        import hashlib
        from utils.hdfs import is_already_processed, hdfs_write_done

        csv_path = "hdfs://namenode:8020/raw/file.csv"
        content = b"budget 2024"
        sc, fs, files, written = make_sc({csv_path: content})

        checksum = hashlib.sha256(content).hexdigest()
        hdfs_write_done(sc, csv_path, checksum)

        assert is_already_processed(sc, csv_path) is True

    def test_checksum_mismatch_returns_false(self):
        from utils.hdfs import is_already_processed, hdfs_write_done

        csv_path = "hdfs://namenode:8020/raw/file.csv"
        sc, fs, files, written = make_sc({csv_path: b"new content"})

        # เขียน .done ด้วย checksum เก่า
        hdfs_write_done(sc, csv_path, "old_checksum_that_does_not_match")

        assert is_already_processed(sc, csv_path) is False

    def test_legacy_empty_done_returns_true(self):
        """.done แบบเก่า (เปล่า) → backward compat ถือว่า processed"""
        from utils.hdfs import is_already_processed

        csv_path = "hdfs://namenode:8020/raw/file.csv"
        sc, _, _, _ = make_sc({
            csv_path: b"data",
            csv_path + ".done": b"",  # legacy empty marker
        })

        assert is_already_processed(sc, csv_path) is True

    def test_csv_not_found_with_done_returns_true(self):
        """ไฟล์ต้นทางหายไปแล้ว แต่มี .done → skip"""
        from utils.hdfs import is_already_processed, hdfs_write_done

        csv_path = "hdfs://namenode:8020/raw/file.csv"
        sc, fs, files, written = make_sc({})

        # เขียน .done แต่ไม่มี csv จริง
        hdfs_write_done(sc, csv_path, "somechecksum")

        assert is_already_processed(sc, csv_path) is True