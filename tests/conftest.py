# tests/conftest.py
import sys
import os
import pytest
from unittest.mock import MagicMock

# ─── Spark on Windows: ต้องการ winutils.exe ───
if sys.platform == "win32":
    hadoop_home = os.environ.get("HADOOP_HOME", r"D:\hadoop_new")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))


# ══════════════════════════════════════════════════════════════════
# HDFS Mock Fixtures
# ══════════════════════════════════════════════════════════════════

def make_hdfs_fs(existing_paths=None):
    """
    สร้าง mock HDFS FileSystem ที่ rename/exists update state จริง
    ใช้สำหรับ test ที่ต้องการ fs โดยตรง (atomic_write, soft_delete)

    Returns: (fs, existing_set)
    """
    existing = set(existing_paths or [])
    fs = MagicMock()

    def exists(p):
        s = str(p)
        return s in existing or any(e.startswith(s + "/") for e in existing)

    def rename(src, dst):
        s, d = str(src), str(dst)
        if s in existing:
            existing.discard(s)
            existing.add(d)
        return True

    def delete(path_mock, recursive=True):
        existing.discard(str(path_mock))

    def mkdirs(p):
        pass

    def listStatus(path_mock):
        prefix = str(path_mock).rstrip("/")
        seen, out = set(), []
        for p in list(existing):
            if not p.startswith(prefix + "/"):
                continue
            child = f"{prefix}/{p[len(prefix)+1:].split('/')[0]}"
            if child not in seen:
                seen.add(child)
                status = MagicMock()
                status.getPath.return_value = _make_path_mock(child)
                out.append(status)
        return out

    fs.exists.side_effect = exists
    fs.rename.side_effect = rename
    fs.delete.side_effect = delete
    fs.mkdirs.side_effect = mkdirs
    fs.listStatus.side_effect = listStatus
    fs.create.return_value = MagicMock()

    return fs, existing


def _make_path_mock(p: str):
    """สร้าง mock Hadoop Path object จาก string"""
    m = MagicMock()
    m.toString.return_value = p
    m.getName.return_value = p.split("/")[-1]
    m.getParent.side_effect = lambda: _make_path_mock("/".join(p.split("/")[:-1]))
    return m


def make_mock_sc(fs=None, existing_paths=None):
    """
    สร้าง mock SparkContext พร้อม _jvm ครบ
    ถ้าไม่ส่ง fs มา จะสร้าง make_hdfs_fs ให้อัตโนมัติ

    Usage:
        sc, fs, existing = make_mock_sc()
        sc, fs, existing = make_mock_sc(existing_paths=["/data/year=2024"])
        sc, fs, existing = make_mock_sc(fs=my_fs)
    """
    if fs is None:
        fs, existing = make_hdfs_fs(existing_paths)
    else:
        existing = set(existing_paths or [])

    sc = MagicMock()
    sc._jvm.java.net.URI.create.return_value = MagicMock()
    sc._jsc.hadoopConfiguration.return_value = MagicMock()
    sc._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs
    sc._jvm.org.apache.hadoop.fs.Path.side_effect = lambda p: p  # path เป็น string
    return sc, fs, existing


def make_hdfs_sc(files: dict = None, existing_paths=None):
    """
    สร้าง mock sc ที่รองรับ read/write file content ด้วย
    ใช้สำหรับ test idempotency (compute_file_checksum, hdfs_write_done ฯลฯ)

    files: {path: bytes_content}
    Returns: (sc, fs, files_dict, written_dict)
    """
    files = dict(files or {})
    written = {}

    sc = MagicMock()

    def make_path(p):
        m = MagicMock()
        m.toString.return_value = p
        return m

    sc._jvm.org.apache.hadoop.fs.Path.side_effect = make_path
    sc._jvm.java.net.URI.create.return_value = MagicMock()
    sc._jsc.hadoopConfiguration.return_value = MagicMock()

    fs = MagicMock()

    def _to_str(p):
        return p.toString() if hasattr(p, "toString") else str(p)

    def exists(path_mock):
        return _to_str(path_mock) in files or _to_str(path_mock) in written

    def fs_open(path_mock):
        p = _to_str(path_mock)
        content = files.get(p) or written.get(p) or b""
        stream = MagicMock()
        pos = [0]

        def read(buf):
            remaining = len(content) - pos[0]
            if remaining <= 0:
                return -1
            n = min(len(buf), remaining)
            for i in range(n):
                buf[i] = content[pos[0] + i]
            pos[0] += n
            return n

        stream.read.side_effect = read

        lines = content.decode("utf-8").splitlines() if content else []
        line_iter = iter(lines)

        def readline():
            try:
                return next(line_iter)
            except StopIteration:
                return None

        br = MagicMock()
        br.readLine.side_effect = readline
        sc._jvm.java.io.BufferedReader.return_value = br
        return stream

    def fs_create(path_mock):
        p = _to_str(path_mock)
        buf = []
        out = MagicMock()

        def write(b):
            buf.extend(b if isinstance(b, (bytes, bytearray)) else bytes(b))

        def close():
            written[p] = bytes(buf)

        out.write.side_effect = write
        out.close.side_effect = close
        return out

    fs.exists.side_effect = exists
    fs.open.side_effect = fs_open
    fs.create.side_effect = fs_create
    sc._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs

    def new_instance(type_, size):
        return bytearray(size)

    sc._jvm.java.lang.reflect.Array.newInstance.side_effect = new_instance
    sc._jvm.java.lang.Byte.TYPE = "byte"

    return sc, fs, files, written


# ══════════════════════════════════════════════════════════════════
# Logger Mock Fixtures
# ══════════════════════════════════════════════════════════════════

def make_mock_log():
    """สร้าง mock logger สำหรับ test step_log"""
    log = MagicMock()
    log.info = MagicMock()
    log.error = MagicMock()
    log.warning = MagicMock()
    return log


# ══════════════════════════════════════════════════════════════════
# pytest Fixtures (ใช้ใน test ด้วย @pytest.fixture)
# ══════════════════════════════════════════════════════════════════

@pytest.fixture
def mock_sc():
    """pytest fixture: sc + fs + existing สำหรับ test ทั่วไป"""
    sc, fs, existing = make_mock_sc()
    return sc, fs, existing


@pytest.fixture
def mock_sc_with_data():
    """pytest fixture: sc ที่อ่าน/เขียน file content ได้"""
    sc, fs, files, written = make_hdfs_sc()
    return sc, fs, files, written


@pytest.fixture
def mock_log():
    """pytest fixture: mock logger"""
    return make_mock_log()