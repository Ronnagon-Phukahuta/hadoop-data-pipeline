# tests/test_soft_delete.py
"""
Tests สำหรับ soft_delete.py — ใช้ mock HDFS (ไม่ต้องการ Spark)
"""

import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


# ── Helpers: สร้าง mock sc ที่จำลอง HDFS filesystem ──────────────

def make_sc(existing_paths=None):
    """
    สร้าง mock SparkContext ที่มี _jvm ครบ
    existing_paths: set ของ path ที่ "มีอยู่" ใน HDFS
    """
    existing = set(existing_paths or [])
    renamed = {}  # src -> dst

    sc = MagicMock()

    # mock Path objects คืน string เพื่อง่ายต่อการ assert
    def make_path(p):
        path_mock = MagicMock()
        path_mock.toString.return_value = p
        path_mock.getName.return_value = p.split("/")[-1]
        path_mock.getParent.side_effect = lambda: make_path("/".join(p.split("/")[:-1]))
        return path_mock

    sc._jvm.org.apache.hadoop.fs.Path.side_effect = make_path

    # mock FileSystem
    fs = MagicMock()

    def exists(path_mock):
        p = path_mock.toString().rstrip("/")
        # match exact หรือ เป็น prefix ของ path ที่มีอยู่ (intermediate dir)
        return p in existing or any(e.startswith(p + "/") for e in existing)

    def rename(src_mock, dst_mock):
        src = src_mock.toString()
        dst = dst_mock.toString()
        if src in existing:
            existing.discard(src)
            existing.add(dst)
            renamed[src] = dst
            return True
        return False

    def delete(path_mock, recursive):
        existing.discard(path_mock.toString())

    def mkdirs(path_mock):
        pass

    def listStatus(path_mock):
        prefix = path_mock.toString().rstrip("/")
        out = []
        seen_dirs = set()
        for p in list(existing):  # snapshot กัน set change ระหว่าง iterate
            if not p.startswith(prefix + "/"):
                continue
            rest = p[len(prefix) + 1:]
            child_name = rest.split("/")[0]
            child_path = f"{prefix}/{child_name}"
            if child_path not in seen_dirs:
                seen_dirs.add(child_path)
                status = MagicMock()
                status.getPath.return_value = make_path(child_path)
                out.append(status)
        return out

    fs.exists.side_effect = exists
    fs.rename.side_effect = rename
    fs.delete.side_effect = delete
    fs.mkdirs.side_effect = mkdirs
    fs.listStatus.side_effect = listStatus

    sc._jvm.java.net.URI.create.return_value = MagicMock()
    sc._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs

    return sc, fs, existing, renamed


# ── Tests: safe_delete ─────────────────────────────────────────────

class TestSafeDelete:

    def test_moves_existing_path_to_trash(self):
        from utils.soft_delete import safe_delete, TRASH_BASE_PATH

        path = "hdfs://namenode:8020/datalake/versions/finance_itsc/year=2024/v_old"
        sc, fs, existing, renamed = make_sc(existing_paths={path})

        with patch("utils.soft_delete.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 4, 10, 0, 0)
            trash_path = safe_delete(sc, path)

        assert path not in existing
        assert trash_path.startswith(f"{TRASH_BASE_PATH}/20260304/")
        assert path in renamed

    def test_skips_nonexistent_path(self):
        from utils.soft_delete import safe_delete

        sc, fs, existing, renamed = make_sc(existing_paths=set())
        result = safe_delete(sc, "hdfs://namenode:8020/nonexistent")

        assert result == ""
        assert len(renamed) == 0

    def test_adds_timestamp_suffix_on_collision(self):
        from utils.soft_delete import safe_delete, TRASH_BASE_PATH

        path = "hdfs://namenode:8020/datalake/versions/finance_itsc/year=2024/v_old"
        escaped = path.replace("hdfs://namenode:8020", "").replace("/", "__").strip("_")

        with patch("utils.soft_delete.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 4, 10, 0, 0)
            # pre-populate trash ให้มี collision
            collision_path = f"{TRASH_BASE_PATH}/20260304/{escaped}"
            sc, fs, existing, renamed = make_sc(
                existing_paths={path, collision_path}
            )
            result = safe_delete(sc, path)

        # ต้องต่อ timestamp suffix
        assert result != collision_path
        assert "100000" in result  # HH MM SS = 10:00:00


# ── Tests: list_trash ──────────────────────────────────────────────

class TestListTrash:

    def test_returns_empty_when_no_trash(self):
        from utils.soft_delete import list_trash

        sc, fs, existing, _ = make_sc(existing_paths=set())
        result = list_trash(sc)

        assert result == []

    def test_lists_all_dates(self):
        from utils.soft_delete import list_trash, TRASH_BASE_PATH

        items = {
            f"{TRASH_BASE_PATH}/20260304/item_a",
            f"{TRASH_BASE_PATH}/20260304/item_b",
            f"{TRASH_BASE_PATH}/20260305/item_c",
        }
        sc, fs, existing, _ = make_sc(existing_paths=items)

        result = list_trash(sc)
        names = {r["name"] for r in result}

        assert "item_a" in names
        assert "item_b" in names
        assert "item_c" in names

    def test_filters_by_date(self):
        from utils.soft_delete import list_trash, TRASH_BASE_PATH

        items = {
            f"{TRASH_BASE_PATH}/20260304/item_a",
            f"{TRASH_BASE_PATH}/20260305/item_b",
        }
        sc, fs, existing, _ = make_sc(existing_paths=items)

        result = list_trash(sc, date_str="20260304")
        names = {r["name"] for r in result}

        assert "item_a" in names
        assert "item_b" not in names


# ── Tests: restore_from_trash ──────────────────────────────────────

class TestRestoreFromTrash:

    def test_restores_to_decoded_path(self):
        from utils.soft_delete import restore_from_trash, TRASH_BASE_PATH

        name = "__datalake__versions__finance_itsc__year=2024__v_20260301_120000"
        trash_item = f"{TRASH_BASE_PATH}/20260304/{name}"
        sc, fs, existing, renamed = make_sc(existing_paths={trash_item})

        result = restore_from_trash(sc, "20260304", name)

        assert trash_item not in existing
        assert result.startswith("hdfs://namenode:8020/")

    def test_restores_to_explicit_path(self):
        from utils.soft_delete import restore_from_trash, TRASH_BASE_PATH

        name = "some_item"
        trash_item = f"{TRASH_BASE_PATH}/20260304/{name}"
        target = "hdfs://namenode:8020/datalake/restored/item"
        sc, fs, existing, renamed = make_sc(existing_paths={trash_item})

        result = restore_from_trash(sc, "20260304", name, restore_to=target)

        assert result == target
        assert trash_item not in existing

    def test_raises_if_trash_item_missing(self):
        from utils.soft_delete import restore_from_trash

        sc, fs, existing, _ = make_sc(existing_paths=set())

        with pytest.raises(FileNotFoundError):
            restore_from_trash(sc, "20260304", "nonexistent")

    def test_raises_if_destination_exists(self):
        from utils.soft_delete import restore_from_trash, TRASH_BASE_PATH

        name = "item"
        trash_item = f"{TRASH_BASE_PATH}/20260304/{name}"
        target = "hdfs://namenode:8020/datalake/already/exists"
        sc, fs, existing, _ = make_sc(existing_paths={trash_item, target})

        with pytest.raises(RuntimeError, match="already exists"):
            restore_from_trash(sc, "20260304", name, restore_to=target)


# ── Tests: purge_old_trash ─────────────────────────────────────────

class TestPurgeOldTrash:

    def test_purges_old_dates(self):
        from utils.soft_delete import purge_old_trash, TRASH_BASE_PATH

        items = {
            f"{TRASH_BASE_PATH}/20260101/old_item",    # เก่า → ลบ
            f"{TRASH_BASE_PATH}/20260201/old_item2",   # เก่า → ลบ
            f"{TRASH_BASE_PATH}/20260303/recent_item", # ใหม่ → เก็บ
        }
        sc, fs, existing, _ = make_sc(existing_paths=items)

        # patch เฉพาะ datetime.now ไม่แตะ timedelta
        with patch("utils.soft_delete.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 4)
            # cutoff = 2026-03-04 - 30d = 2026-02-02 → "20260202"
            # 20260101 และ 20260201 ≤ 20260202 → ลบ
            # 20260303 > 20260202 → เก็บ
            purge_old_trash(sc, keep_days=30)

        deleted_paths = {str(c.args[0].toString()) for c in fs.delete.call_args_list}
        assert any("20260101" in p for p in deleted_paths)
        assert any("20260201" in p for p in deleted_paths)
        assert not any("20260303" in p for p in deleted_paths)

    def test_no_error_when_trash_empty(self):
        from utils.soft_delete import purge_old_trash

        sc, fs, existing, _ = make_sc(existing_paths=set())
        # ไม่ควร raise
        purge_old_trash(sc, keep_days=30)