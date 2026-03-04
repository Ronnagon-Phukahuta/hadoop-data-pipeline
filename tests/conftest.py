# tests/conftest.py
import sys
import os

# ─── Spark on Windows: ต้องการ winutils.exe ───
# set ก่อน import pyspark ใดๆ
if sys.platform == "win32":
    hadoop_home = os.environ.get("HADOOP_HOME", r"D:\hadoop_new")
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "jobs"))