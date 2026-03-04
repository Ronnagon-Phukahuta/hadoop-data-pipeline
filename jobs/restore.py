from utils.versioning import list_versions
from utils.versioning import restore_version
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("restore")
    .enableHiveSupport()
    .getOrCreate()
)

sc = spark.sparkContext

versions = list_versions(sc, year=2023)
for v in versions:
    print(f"{v['version']} | {v['timestamp']} | rows={v['row_count']}")


restore_version(
    spark,
    version_id="v_20260302_014122",   # version ที่อยากกลับไป
    year=2023,
    target_table="finance_itsc_wide",
    target_path="hdfs://namenode:8020/datalake/staging/finance_itsc_wide",
)