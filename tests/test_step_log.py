# tests/test_step_log.py
"""
Tests สำหรับ step_log context manager ใน logger.py
ไม่ต้องการ Spark
"""

import pytest


from conftest import make_mock_log


def get_kwargs(call_obj):
    """Python 3.7 compatible — ดึง kwargs จาก mock call"""
    # Python 3.8+: call.kwargs เป็น dict
    # Python 3.7:  call[1] เป็น dict
    kw = call_obj[1]
    if isinstance(kw, dict):
        return kw
    return dict(call_obj[1])



class TestStepLog:

    def test_logs_start_and_success(self):
        from logger import step_log

        log = make_mock_log()
        with step_log(log, "transform", dataset="finance", year=2024):
            pass

        # START ถูก log
        start_calls = [str(c) for c in log.info.call_args_list]
        assert any("START" in c for c in start_calls)

        # SUCCESS ถูก log
        assert any("SUCCESS" in c for c in start_calls)

    def test_success_includes_duration_ms(self):
        from logger import step_log

        log = make_mock_log()
        with step_log(log, "read_csv", dataset="finance", year=2024):
            pass

        # หา SUCCESS call และตรวจ duration_ms
        for c in log.info.call_args_list:
            if "SUCCESS" in str(c):
                kwargs = get_kwargs(c)
                assert "duration_ms" in kwargs
                assert isinstance(kwargs["duration_ms"], int)
                assert kwargs["duration_ms"] >= 0
                break
        else:
            pytest.fail("No SUCCESS log found")

    def test_success_includes_extra_fields(self):
        from logger import step_log

        log = make_mock_log()
        with step_log(log, "atomic_write", dataset="finance", year=2024):
            pass

        for c in log.info.call_args_list:
            if "SUCCESS" in str(c):
                kwargs = get_kwargs(c)
                assert kwargs.get("dataset") == "finance"
                assert kwargs.get("year") == 2024
                assert kwargs.get("step") == "atomic_write"
                break

    def test_ctx_fields_appear_in_success_log(self):
        from logger import step_log

        log = make_mock_log()
        with step_log(log, "transform", dataset="finance", year=2024) as ctx:
            ctx["rows"] = 1500
            ctx["version"] = "v_20260306_120000"

        for c in log.info.call_args_list:
            if "SUCCESS" in str(c):
                kwargs = get_kwargs(c)
                assert kwargs.get("rows") == 1500
                assert kwargs.get("version") == "v_20260306_120000"
                break
        else:
            pytest.fail("No SUCCESS log found")

    def test_logs_error_on_exception(self):
        from logger import step_log

        log = make_mock_log()
        with pytest.raises(ValueError):
            with step_log(log, "read_csv", dataset="finance", year=2024):
                raise ValueError("HDFS connection timeout")

        # FAILED ถูก log ผ่าน log.error
        assert log.error.called
        error_calls = [str(c) for c in log.error.call_args_list]
        assert any("FAILED" in c for c in error_calls)

    def test_error_log_includes_error_message(self):
        from logger import step_log

        log = make_mock_log()
        with pytest.raises(RuntimeError):
            with step_log(log, "atomic_write", dataset="finance", year=2024):
                raise RuntimeError("disk full")

        for c in log.error.call_args_list:
            kwargs = get_kwargs(c)
            assert "error" in kwargs
            assert "disk full" in kwargs["error"]
            assert "duration_ms" in kwargs
            break

    def test_exception_propagates(self):
        from logger import step_log

        log = make_mock_log()
        with pytest.raises(ValueError, match="bad data"):
            with step_log(log, "transform", dataset="finance"):
                raise ValueError("bad data")

    def test_ctx_available_in_error_log(self):
        from logger import step_log

        log = make_mock_log()
        with pytest.raises(RuntimeError):
            with step_log(log, "transform", dataset="finance", year=2024) as ctx:
                ctx["rows_processed"] = 500  # บางส่วนทำไปแล้วก่อน fail
                raise RuntimeError("OOM")

        for c in log.error.call_args_list:
            kwargs = get_kwargs(c)
            assert kwargs.get("rows_processed") == 500
            break

    def test_step_and_dataset_in_start_log(self):
        from logger import step_log

        log = make_mock_log()
        with step_log(log, "versioning", dataset="finance"):
            pass

        start_call = log.info.call_args_list[0]
        kwargs = get_kwargs(start_call)
        assert kwargs.get("step") == "versioning"
        assert kwargs.get("dataset") == "finance"