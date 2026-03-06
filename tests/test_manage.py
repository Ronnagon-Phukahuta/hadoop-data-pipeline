# tests/test_manage.py
"""
ทดสอบ manage.py CLI commands
- cmd_versions — แสดง versions
- cmd_diff — เปรียบเทียบ 2 versions
- cmd_restore — restore version
- cmd_trash — แสดง trash
- cmd_cleanup — cleanup old versions
"""

from unittest.mock import MagicMock, patch
from types import SimpleNamespace

from conftest import make_mock_sc as _make_mock_sc
from utils.versioning import compute_schema_hash


# ── Helpers ───────────────────────────────────────────────────────

def make_mock_sc():
    sc, fs, _ = _make_mock_sc()
    return sc, fs


def make_args(**kwargs):
    """สร้าง mock argparse.Namespace"""
    defaults = {"year": 2024, "yes": True}
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def make_version(version_id, timestamp, rows=25, cols=None):
    cols = cols or ["date", "details", "year", "compensation_budget"]
    return {
        "version": version_id,
        "source_file": "finance_2024.csv",
        "year": 2024,
        "timestamp": timestamp,
        "row_count": rows,
        "columns": cols,
        "schema_hash": compute_schema_hash(cols),
        "keep_versions": 5,
    }


# ── Tests: cmd_versions ───────────────────────────────────────────

def test_cmd_versions_prints_table(capsys):
    sc, _ = make_mock_sc()
    versions = [
        make_version("v_20260306_110000", "2026-03-06T11:00:00", rows=25),
        make_version("v_20260305_090000", "2026-03-05T09:00:00", rows=20),
    ]

    with patch("utils.versioning.list_versions", return_value=versions):
        import sys
        sys.path.insert(0, ".")
        from manage import cmd_versions
        cmd_versions(sc, make_args(year=2024))

    out = capsys.readouterr().out
    assert "v_20260306_110000" in out
    assert "v_20260305_090000" in out
    assert "25" in out
    assert "20" in out


def test_cmd_versions_empty(capsys):
    sc, _ = make_mock_sc()

    with patch("utils.versioning.list_versions", return_value=[]):
        from manage import cmd_versions
        cmd_versions(sc, make_args(year=2024))

    out = capsys.readouterr().out
    assert "ไม่มี version" in out


# ── Tests: cmd_diff ───────────────────────────────────────────────

def test_cmd_diff_no_schema_change(capsys):
    sc, _ = make_mock_sc()
    diff_result = {
        "version_a": "v_20260301_000000",
        "version_b": "v_20260306_000000",
        "year": 2024,
        "schema_changed": False,
        "added_columns": [],
        "removed_columns": [],
        "row_count_a": 25,
        "row_count_b": 25,
        "row_diff": 0,
        "source_a": "finance_2024.csv",
        "source_b": "finance_2024.csv",
        "same_source": True,
        "timestamp_a": "2026-03-01T00:00:00",
        "timestamp_b": "2026-03-06T00:00:00",
    }

    with patch("utils.versioning.diff_versions", return_value=diff_result):
        from manage import cmd_diff
        cmd_diff(sc, make_args(
            year=2024,
            version_a="v_20260301_000000",
            version_b="v_20260306_000000",
            json=False,
        ))

    out = capsys.readouterr().out
    assert "schema_changed" in out
    assert "False" in out


def test_cmd_diff_with_json_flag(capsys):
    sc, _ = make_mock_sc()
    diff_result = {
        "version_a": "v_a", "version_b": "v_b", "year": 2024,
        "schema_changed": True,
        "added_columns": ["new_col"], "removed_columns": [],
        "row_count_a": 10, "row_count_b": 15, "row_diff": 5,
        "source_a": "a.csv", "source_b": "b.csv", "same_source": False,
        "timestamp_a": "2026-03-01T00:00:00", "timestamp_b": "2026-03-06T00:00:00",
    }

    with patch("utils.versioning.diff_versions", return_value=diff_result):
        from manage import cmd_diff
        cmd_diff(sc, make_args(
            year=2024,
            version_a="v_a",
            version_b="v_b",
            json=True,
        ))

    out = capsys.readouterr().out
    # ต้องมี JSON output
    assert '"schema_changed": true' in out
    assert '"added_columns"' in out


# ── Tests: cmd_restore ────────────────────────────────────────────

def test_cmd_restore_with_yes_flag(capsys):
    sc, _ = make_mock_sc()
    spark = MagicMock()
    spark.sparkContext = sc

    with patch("utils.versioning.restore_version") as mock_restore:
        from manage import cmd_restore
        cmd_restore(sc, spark, make_args(
            year=2024,
            version_id="v_20260301_120000",
            yes=True,
        ))

    mock_restore.assert_called_once()
    call_kwargs = mock_restore.call_args[1]
    assert call_kwargs["version_id"] == "v_20260301_120000"
    assert call_kwargs["year"] == 2024
    assert call_kwargs["target_table"] == "finance_itsc_wide"


def test_cmd_restore_without_yes_prints_hint(capsys):
    sc, _ = make_mock_sc()
    spark = MagicMock()

    with patch("utils.versioning.restore_version") as mock_restore:
        from manage import cmd_restore
        cmd_restore(sc, spark, make_args(
            year=2024,
            version_id="v_20260301_120000",
            yes=False,
        ))

    mock_restore.assert_not_called()
    out = capsys.readouterr().out
    assert "--yes" in out


# ── Tests: cmd_cleanup ────────────────────────────────────────────

def test_cmd_cleanup_deletes_old_versions(capsys):
    sc, _ = make_mock_sc()
    versions = [
        make_version(f"v_2026030{i}_000000", f"2026-03-0{i}T00:00:00")
        for i in range(1, 8)  # 7 versions
    ]
    versions_sorted = list(reversed(versions))

    with patch("utils.versioning.list_versions", return_value=versions_sorted), \
         patch("utils.versioning.cleanup_old_versions") as mock_cleanup:
        from manage import cmd_cleanup
        cmd_cleanup(sc, make_args(year=2024, keep=5, yes=True))

    mock_cleanup.assert_called_once_with(sc, year=2024, keep=5)


def test_cmd_cleanup_no_delete_when_under_limit(capsys):
    sc, _ = make_mock_sc()
    versions = [
        make_version(f"v_2026030{i}_000000", f"2026-03-0{i}T00:00:00")
        for i in range(1, 4)  # 3 versions
    ]

    with patch("utils.versioning.list_versions", return_value=versions), \
         patch("utils.versioning.cleanup_old_versions") as mock_cleanup:
        from manage import cmd_cleanup
        cmd_cleanup(sc, make_args(year=2024, keep=5, yes=True))

    mock_cleanup.assert_not_called()
    out = capsys.readouterr().out
    assert "ไม่มี version ที่ต้องลบ" in out


def test_cmd_cleanup_without_yes_prints_hint(capsys):
    sc, _ = make_mock_sc()
    versions = [
        make_version(f"v_2026030{i}_000000", f"2026-03-0{i}T00:00:00")
        for i in range(1, 8)
    ]
    versions_sorted = list(reversed(versions))

    with patch("utils.versioning.list_versions", return_value=versions_sorted), \
         patch("utils.versioning.cleanup_old_versions") as mock_cleanup:
        from manage import cmd_cleanup
        cmd_cleanup(sc, make_args(year=2024, keep=5, yes=False))

    mock_cleanup.assert_not_called()
    out = capsys.readouterr().out
    assert "--yes" in out