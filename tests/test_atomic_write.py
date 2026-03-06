# tests/test_atomic_write.py
"""
ทดสอบ atomic_write / swap pattern ว่า:
1. สำเร็จปกติ — partition ใหม่เข้า, ปีอื่นไม่หาย
2. crash step 1 (write _tmp fail) — partition เดิมยังอยู่
3. crash step 2 (backup fail) — partition เดิมยังอยู่
4. crash step 3 (rename fail) — rollback จาก _old ได้
"""

import pytest
from unittest.mock import patch


# ── Helpers: import จาก conftest ─────────────────────────────────
from conftest import make_hdfs_fs, make_mock_sc as _make_mock_sc


def make_mock_fs(existing_paths):
    fs, existing = make_hdfs_fs(existing_paths)
    fs._existing = existing  # expose existing set สำหรับ assert
    return fs


def make_mock_sc(fs):
    sc, _, _ = _make_mock_sc(fs=fs)
    return sc


# ── Test 1: สำเร็จปกติ ────────────────────────────────────────────
def test_swap_success():
    """swap สำเร็จ — backup, swap, แล้ว safe_delete _old ไป trash"""
    fs = make_mock_fs(["/data/year=2024"])  # ไม่มี _old ค้างอยู่
    existing = fs._existing
    sc = make_mock_sc(fs)

    from utils.retry import _hdfs_swap
    _hdfs_swap(sc, "/data/year=2024_tmp", "/data/year=2024")

    # ต้อง backup ก่อน
    fs.rename.assert_any_call("/data/year=2024", "/data/year=2024_old")
    # แล้วค่อย swap
    fs.rename.assert_any_call("/data/year=2024_tmp", "/data/year=2024")
    # step 3: safe_delete ย้าย _old → trash
    # ตรวจจาก existing set — หลัง swap สำเร็จ trash ต้องมี item ใหม่
    assert any("trash" in p for p in existing), (
        "expected _old to be in trash, existing=" + str(existing)
    )


# ── Test 2: ไม่มี partition เดิม (first time write) ───────────────
def test_swap_first_time():
    """ถ้ายังไม่มี partition เดิม — ข้าม backup ไปเลย"""
    fs = make_mock_fs([])  # ไม่มีอะไรอยู่เลย
    sc = make_mock_sc(fs)

    from utils.retry import _hdfs_swap
    _hdfs_swap(sc, "/data/year=2024_tmp", "/data/year=2024")

    # ไม่ backup เพราะไม่มีของเดิม
    calls = [str(c) for c in fs.rename.call_args_list]
    assert not any("_old" in c for c in calls)
    # แต่ต้อง swap _tmp → partition
    fs.rename.assert_called_with("/data/year=2024_tmp", "/data/year=2024")


# ── Test 3: crash step 3 (rename _tmp → partition fail) ───────────
def test_swap_rename_fail_rollback():
    """rename _tmp → partition fail — rollback จาก _old ได้"""
    # ไม่มี _old ตอนเริ่มต้น → safe_delete stale branch ไม่ถูกเรียก
    # call sequence: 1=backup(dst→old) 2=swap(_tmp→dst)←fail → rollback(old→dst)
    existing = ["/data/year=2024"]
    fs = make_mock_fs(existing)
    sc = make_mock_sc(fs)

    call_count = {"n": 0}
    def rename_side_effect(src, dst):
        call_count["n"] += 1
        if call_count["n"] == 2:  # swap call → fail
            return False
        return True

    fs.rename.side_effect = rename_side_effect
    # _old มีอยู่หลังจาก backup สำเร็จ (call 1) เพื่อให้ rollback branch ทำงาน
    call_exists = {"n": 0}
    def exists_side_effect(p):
        call_exists["n"] += 1
        # ครั้งแรก: check dst ("/data/year=2024") → True
        # ครั้งที่ 2: check old ("/data/year=2024_old") → False (ไม่มี stale)
        # ครั้งที่ 3: check old หลัง swap fail → True (rollback ได้)
        if str(p) == "/data/year=2024_old":
            return call_exists["n"] >= 3
        return str(p) in existing

    fs.exists.side_effect = exists_side_effect

    from utils.retry import _hdfs_swap
    with pytest.raises(RuntimeError, match="HDFS rename failed"):
        _hdfs_swap(sc, "/data/year=2024_tmp", "/data/year=2024")

    # ต้อง rollback — rename _old กลับ
    fs.rename.assert_any_call("/data/year=2024_old", "/data/year=2024")


# ── Test 4: crash step 1 (backup fail) ────────────────────────────
def test_swap_backup_fail():
    """backup fail — raise error ทันที partition เดิมยังอยู่"""
    existing = ["/data/year=2024"]
    fs = make_mock_fs(existing)
    sc = make_mock_sc(fs)

    # override side_effect ให้ rename fail เสมอ
    fs.rename.side_effect = lambda src, dst: False

    from utils.retry import _hdfs_swap
    with pytest.raises(RuntimeError, match="HDFS backup failed"):
        _hdfs_swap(sc, "/data/year=2024_tmp", "/data/year=2024")

    # ไม่ควร swap ต่อถ้า backup fail
    assert fs.rename.call_count == 1


# ── Test 5: ปีอื่นไม่โดนแตะ ─────────────────────────────────────
def test_swap_other_partitions_untouched():
    """swap year=2024 — year=2023 และ year=2025 ไม่โดนแตะ"""
    existing = ["/data/year=2023", "/data/year=2024", "/data/year=2025"]
    fs = make_mock_fs(existing)
    sc = make_mock_sc(fs)

    from utils.retry import _hdfs_swap
    _hdfs_swap(sc, "/data/year=2024_tmp", "/data/year=2024")

    # ตรวจว่าไม่มี call ที่แตะ year=2023 หรือ year=2025
    all_calls = str(fs.rename.call_args_list) + str(fs.delete.call_args_list)
    assert "year=2023" not in all_calls
    assert "year=2025" not in all_calls


# ── Test 6: with_retry exponential backoff ────────────────────────
def test_with_retry_exponential_backoff():
    """retry 3 ครั้ง wait 5 → 10 วินาที"""
    call_count = {"n": 0}

    def flaky_fn():
        call_count["n"] += 1
        if call_count["n"] < 3:
            raise ConnectionError("HDFS timeout")
        return "ok"

    with patch("utils.retry.time.sleep") as mock_sleep:
        from utils.retry import with_retry
        result = with_retry(flaky_fn, label="test", max_retries=3, delay=5)

    assert result == "ok"
    assert call_count["n"] == 3
    mock_sleep.assert_any_call(5)
    mock_sleep.assert_any_call(10)


# ── Test 7: with_retry หมด retry raise error ──────────────────────
def test_with_retry_exhausted():
    """หมด retry — raise error ตัวสุดท้าย"""
    from utils.retry import with_retry

    with patch("utils.retry.time.sleep"):
        with pytest.raises(RuntimeError, match="always fails"):
            with_retry(
                lambda: (_ for _ in ()).throw(RuntimeError("always fails")),
                label="test",
                max_retries=3,
                delay=1,
            )