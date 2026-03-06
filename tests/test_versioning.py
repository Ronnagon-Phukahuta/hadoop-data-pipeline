# tests/test_versioning.py
"""
ทดสอบ versioning module:
1. create_version — สร้าง snapshot + metadata
2. list_versions — เรียงจากใหม่ไปเก่า
3. cleanup_old_versions — เก็บแค่ N version ล่าสุด
4. restore_version — restore ผ่าน atomic write
"""

import json
import pytest
from unittest.mock import MagicMock, patch


# ── Helpers ───────────────────────────────────────────────────────
from conftest import make_mock_sc as _make_mock_sc
from utils.versioning import compute_schema_hash


def make_mock_sc():
    """versioning ต้องการ fs.getFileChecksum และ fs.exists=False เป็น default"""
    sc, fs, _ = _make_mock_sc()
    fs.exists.return_value = False
    fs.getFileChecksum.return_value.toString.return_value = "mock_checksum_abc123"
    return sc, fs


def make_mock_df(row_count=100, columns=None):
    df = MagicMock()
    df.count.return_value = row_count
    df.columns = columns or ["date", "details", "year", "total_amount"]
    df.sparkSession.sparkContext = make_mock_sc()[0]
    df.write.mode.return_value.parquet = MagicMock()
    return df


def make_version_metadata(version_id: str, timestamp: str) -> dict:
    return {
        "version": version_id,
        "source_file": "finance_2024.csv",
        "year": 2024,
        "timestamp": timestamp,
        "row_count": 100,
        "checksum": "abc123",
        "columns": ["date", "details", "year"],
        "keep_versions": 5,
    }


# ── Test 1: create_version เขียน parquet + metadata ──────────────
def test_create_version_writes_parquet_and_metadata():
    sc, fs = make_mock_sc()
    df = make_mock_df()

    written_metadata = {}

    def mock_create(path):
        stream = MagicMock()
        def capture_write(data):
            written_metadata.update(json.loads(data.decode("utf-8")))
        stream.write.side_effect = capture_write
        return stream

    fs.create.side_effect = mock_create

    with patch("utils.versioning.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "v_20260301_120000"
        mock_dt.now.return_value.isoformat.return_value = "2026-03-01T12:00:00"

        from utils.versioning import create_version
        version_id = create_version(sc, df, "/raw/finance_2024.csv", year=2024)

    assert version_id == "v_20260301_120000"
    assert written_metadata["year"] == 2024
    assert written_metadata["row_count"] == 100
    assert written_metadata["source_file"] == "finance_2024.csv"


# ── Test 2: list_versions เรียงจากใหม่ไปเก่า ──────────────────────
def test_list_versions_sorted_newest_first():
    sc, fs = make_mock_sc()

    meta_v1 = make_version_metadata("v_20260101_000000", "2026-01-01T00:00:00")
    meta_v2 = make_version_metadata("v_20260201_000000", "2026-02-01T00:00:00")
    meta_v3 = make_version_metadata("v_20260301_000000", "2026-03-01T00:00:00")

    with patch("utils.hdfs.hdfs_ls_recursive") as mock_ls, \
         patch("utils.versioning._read_file") as mock_read:

        mock_ls.return_value = [
            "/versions/year=2024/v_20260101_000000/_version.json",
            "/versions/year=2024/v_20260201_000000/_version.json",
            "/versions/year=2024/v_20260301_000000/_version.json",
        ]
        mock_read.side_effect = [
            json.dumps(meta_v1),
            json.dumps(meta_v2),
            json.dumps(meta_v3),
        ]

        from utils.versioning import list_versions
        versions = list_versions(sc, year=2024)

    assert versions[0]["version"] == "v_20260301_000000"  # ใหม่สุดก่อน
    assert versions[1]["version"] == "v_20260201_000000"
    assert versions[2]["version"] == "v_20260101_000000"


# ── Test 3: cleanup เก็บแค่ N version ล่าสุด ─────────────────────
def test_cleanup_keeps_only_n_versions():
    sc, fs = make_mock_sc()
    fs.exists.return_value = True

    versions = [
        make_version_metadata(f"v_2026030{i}_000000", f"2026-03-0{i}T00:00:00")
        for i in range(1, 8)  # 7 versions
    ]
    # เรียงจากใหม่ไปเก่า
    versions_sorted = list(reversed(versions))

    with patch("utils.versioning.list_versions", return_value=versions_sorted), \
         patch("utils.versioning.safe_delete") as mock_delete:

        from utils.versioning import cleanup_old_versions
        cleanup_old_versions(sc, year=2024, keep=5)

    # ลบแค่ 2 อัน (7 - 5 = 2)
    assert mock_delete.call_count == 2


# ── Test 4: cleanup ไม่ลบถ้า version น้อยกว่า N ──────────────────
def test_cleanup_no_delete_when_under_limit():
    sc, _ = make_mock_sc()

    versions = [
        make_version_metadata(f"v_2026030{i}_000000", f"2026-03-0{i}T00:00:00")
        for i in range(1, 4)  # 3 versions
    ]

    with patch("utils.versioning.list_versions", return_value=versions), \
         patch("utils.versioning.safe_delete") as mock_delete:

        from utils.versioning import cleanup_old_versions
        cleanup_old_versions(sc, year=2024, keep=5)

    mock_delete.assert_not_called()


# ── Test 5: restore_version ใช้ atomic_write_table ────────────────
def test_restore_version_uses_atomic_write():
    sc, _ = make_mock_sc()
    spark = MagicMock()
    spark.sparkContext = sc
    spark.read.parquet.return_value = make_mock_df()

    with patch("utils.versioning.atomic_write_table") as mock_atomic:
        from utils.versioning import restore_version
        restore_version(
            spark,
            version_id="v_20260215_090000",
            year=2024,
            target_table="finance_itsc_wide",
            target_path="hdfs://namenode:8020/datalake/staging/finance_itsc_wide",
        )

    mock_atomic.assert_called_once()
    call_kwargs = mock_atomic.call_args[1]  # index 1 = kwargs dict
    assert call_kwargs["table_name"] == "finance_itsc_wide"
    assert call_kwargs["partition_value"] == 2024


# ── Test 6: restore_version อ่าน parquet จาก path ที่ถูกต้อง ──────
def test_restore_version_reads_correct_path():
    sc, _ = make_mock_sc()
    spark = MagicMock()
    spark.sparkContext = sc
    spark.read.parquet.return_value = make_mock_df()

    with patch("utils.versioning.atomic_write_table"):
        from utils.versioning import restore_version
        restore_version(
            spark,
            version_id="v_20260215_090000",
            year=2024,
            target_table="finance_itsc_wide",
            target_path="hdfs://namenode:8020/datalake/staging/finance_itsc_wide",
        )

    expected_path = "hdfs://namenode:8020/datalake/versions/finance_itsc/year=2024/v_20260215_090000"
    spark.read.parquet.assert_called_once_with(expected_path)

# ── Test 7: compute_schema_hash ───────────────────────────────────
def test_schema_hash_is_deterministic():
    """columns เดิม → hash เดิมเสมอ ไม่ว่าจะเรียกกี่ครั้ง"""
    from utils.versioning import compute_schema_hash

    cols = ["date", "details", "year", "compensation_budget", "expense_budget"]
    assert compute_schema_hash(cols) == compute_schema_hash(cols)


def test_schema_hash_order_independent():
    """column order ต่างกัน → hash เดิม เพราะ sorted() ก่อน hash"""
    from utils.versioning import compute_schema_hash

    cols_a = ["date", "details", "compensation_budget", "expense_budget"]
    cols_b = ["expense_budget", "compensation_budget", "date", "details"]
    assert compute_schema_hash(cols_a) == compute_schema_hash(cols_b)


def test_schema_hash_changes_when_column_added():
    """เพิ่ม column → hash เปลี่ยน"""
    from utils.versioning import compute_schema_hash

    cols_v1 = ["date", "details", "compensation_budget"]
    cols_v2 = ["date", "details", "compensation_budget", "new_fund_column"]
    assert compute_schema_hash(cols_v1) != compute_schema_hash(cols_v2)


def test_schema_hash_changes_when_column_removed():
    """ลบ column → hash เปลี่ยน"""
    from utils.versioning import compute_schema_hash

    cols_v1 = ["date", "details", "compensation_budget", "expense_budget"]
    cols_v2 = ["date", "details", "compensation_budget"]
    assert compute_schema_hash(cols_v1) != compute_schema_hash(cols_v2)


def test_schema_hash_stored_in_version_metadata():
    """create_version ต้องเขียน schema_hash ลงใน _version.json"""
    sc, fs = make_mock_sc()
    df = make_mock_df(columns=["date", "details", "year", "compensation_budget"])

    written_metadata = {}

    def mock_create(path):
        stream = MagicMock()
        def capture_write(data):
            written_metadata.update(json.loads(data.decode("utf-8")))
        stream.write.side_effect = capture_write
        return stream

    fs.create.side_effect = mock_create

    with patch("utils.versioning.datetime") as mock_dt:
        mock_dt.now.return_value.strftime.return_value = "v_20260306_120000"
        mock_dt.now.return_value.isoformat.return_value = "2026-03-06T12:00:00"

        from utils.versioning import create_version, compute_schema_hash
        create_version(sc, df, "/raw/finance_2024.csv", year=2024)

    assert "schema_hash" in written_metadata
    expected_hash = compute_schema_hash(["date", "details", "year", "compensation_budget"])
    assert written_metadata["schema_hash"] == expected_hash


def test_schema_hash_is_12_char_hex():
    """schema_hash ต้องเป็น hex string ที่ถูกต้อง"""
    from utils.versioning import compute_schema_hash
    import re

    h = compute_schema_hash(["date", "details", "compensation_budget"])
    # SHA256 hex = 64 chars
    assert len(h) == 64
    assert re.match(r"^[0-9a-f]+$", h)


# ── Tests: diff_versions ──────────────────────────────────────────

def test_diff_versions_no_schema_change():
    """columns เหมือนกันทุกตัว → schema_changed=False"""
    sc, fs = make_mock_sc()
    cols = ["date", "details", "year", "compensation_budget"]

    meta_a = {**make_version_metadata("v_20260301_000000", "2026-03-01T00:00:00"),
              "columns": cols, "schema_hash": compute_schema_hash(cols), "row_count": 100}
    meta_b = {**make_version_metadata("v_20260306_000000", "2026-03-06T00:00:00"),
              "columns": cols, "schema_hash": compute_schema_hash(cols), "row_count": 120}

    with patch("utils.versioning._read_file") as mock_read:
        mock_read.side_effect = [json.dumps(meta_a), json.dumps(meta_b)]
        from utils.versioning import diff_versions
        result = diff_versions(sc, "v_20260301_000000", "v_20260306_000000", year=2024)

    assert result["schema_changed"] is False
    assert result["added_columns"] == []
    assert result["removed_columns"] == []


def test_diff_versions_column_added():
    """เพิ่ม column → schema_changed=True, added_columns มี column ใหม่"""
    sc, fs = make_mock_sc()
    cols_a = ["date", "details", "compensation_budget"]
    cols_b = ["date", "details", "compensation_budget", "new_fund"]

    meta_a = {**make_version_metadata("v_20260301_000000", "2026-03-01T00:00:00"),
              "columns": cols_a, "schema_hash": compute_schema_hash(cols_a), "row_count": 100}
    meta_b = {**make_version_metadata("v_20260306_000000", "2026-03-06T00:00:00"),
              "columns": cols_b, "schema_hash": compute_schema_hash(cols_b), "row_count": 100}

    with patch("utils.versioning._read_file") as mock_read:
        mock_read.side_effect = [json.dumps(meta_a), json.dumps(meta_b)]
        from utils.versioning import diff_versions
        result = diff_versions(sc, "v_20260301_000000", "v_20260306_000000", year=2024)

    assert result["schema_changed"] is True
    assert result["added_columns"] == ["new_fund"]
    assert result["removed_columns"] == []


def test_diff_versions_column_removed():
    """ลบ column → schema_changed=True, removed_columns มี column ที่หาย"""
    sc, fs = make_mock_sc()
    cols_a = ["date", "details", "compensation_budget", "old_column"]
    cols_b = ["date", "details", "compensation_budget"]

    meta_a = {**make_version_metadata("v_20260301_000000", "2026-03-01T00:00:00"),
              "columns": cols_a, "schema_hash": compute_schema_hash(cols_a), "row_count": 100}
    meta_b = {**make_version_metadata("v_20260306_000000", "2026-03-06T00:00:00"),
              "columns": cols_b, "schema_hash": compute_schema_hash(cols_b), "row_count": 100}

    with patch("utils.versioning._read_file") as mock_read:
        mock_read.side_effect = [json.dumps(meta_a), json.dumps(meta_b)]
        from utils.versioning import diff_versions
        result = diff_versions(sc, "v_20260301_000000", "v_20260306_000000", year=2024)

    assert result["schema_changed"] is True
    assert result["added_columns"] == []
    assert result["removed_columns"] == ["old_column"]


def test_diff_versions_row_diff():
    """row_diff คำนวณถูกต้อง"""
    sc, fs = make_mock_sc()
    cols = ["date", "details", "compensation_budget"]

    meta_a = {**make_version_metadata("v_20260301_000000", "2026-03-01T00:00:00"),
              "columns": cols, "schema_hash": compute_schema_hash(cols), "row_count": 100}
    meta_b = {**make_version_metadata("v_20260306_000000", "2026-03-06T00:00:00"),
              "columns": cols, "schema_hash": compute_schema_hash(cols), "row_count": 125}

    with patch("utils.versioning._read_file") as mock_read:
        mock_read.side_effect = [json.dumps(meta_a), json.dumps(meta_b)]
        from utils.versioning import diff_versions
        result = diff_versions(sc, "v_20260301_000000", "v_20260306_000000", year=2024)

    assert result["row_diff"] == 25
    assert result["row_count_a"] == 100
    assert result["row_count_b"] == 125


def test_diff_versions_missing_metadata_raises():
    """version ที่ไม่มี metadata → raise FileNotFoundError"""
    sc, fs = make_mock_sc()

    with patch("utils.versioning._read_file", return_value=None):
        from utils.versioning import diff_versions
        with pytest.raises(FileNotFoundError):
            diff_versions(sc, "v_nonexistent", "v_20260306_000000", year=2024)


def test_diff_versions_legacy_no_schema_hash():
    """version เก่าไม่มี schema_hash → คำนวณจาก columns แทน"""
    sc, fs = make_mock_sc()
    cols = ["date", "details", "compensation_budget"]

    # ไม่มี schema_hash ใน metadata (version เก่า)
    meta_a = {**make_version_metadata("v_20260201_000000", "2026-02-01T00:00:00"),
              "columns": cols, "row_count": 100}
    meta_b = {**make_version_metadata("v_20260306_000000", "2026-03-06T00:00:00"),
              "columns": cols, "schema_hash": compute_schema_hash(cols), "row_count": 100}

    with patch("utils.versioning._read_file") as mock_read:
        mock_read.side_effect = [json.dumps(meta_a), json.dumps(meta_b)]
        from utils.versioning import diff_versions
        result = diff_versions(sc, "v_20260201_000000", "v_20260306_000000", year=2024)

    # columns เหมือนกัน → schema_changed=False แม้ version เก่าไม่มี schema_hash
    assert result["schema_changed"] is False