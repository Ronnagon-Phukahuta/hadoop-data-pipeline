# scripts/diff_test.py
# ใช้ทดสอบ diff_versions manual
# Usage: spark-submit /jobs/scripts/diff_test.py

import sys
import json
sys.path.insert(0, '/jobs')

from pyspark.sql import SparkSession
from utils.versioning import diff_versions, list_versions

spark = SparkSession.builder.appName("diff_test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

YEAR = 2024

# ดู versions ที่มีทั้งหมด
print(f"\n=== Versions for year={YEAR} ===")
versions = list_versions(sc, year=YEAR)
for v in versions:
    print(f"  {v['version']}  rows={v['row_count']}  schema_hash={v.get('schema_hash', 'N/A')[:12]}")

# diff 2 versions ล่าสุด
if len(versions) >= 2:
    v_new = versions[0]["version"]
    v_old = versions[1]["version"]
    print(f"\n=== Diff: {v_old} → {v_new} ===")
    diff = diff_versions(sc, v_old, v_new, year=YEAR)
    print(json.dumps(diff, indent=2, ensure_ascii=False))
else:
    print("ต้องมีอย่างน้อย 2 versions ถึงจะ diff ได้")

spark.stop()