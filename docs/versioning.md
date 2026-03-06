# Data Versioning

ทุกครั้งที่ ETL pipeline ประมวลผล CSV สำเร็จ จะสร้าง snapshot ไว้ใน HDFS อัตโนมัติ
เก็บไว้ **5 version ล่าสุด** ต่อปี สามารถ rollback ได้ทุกเมื่อ

## HDFS Structure

```
/datalake/
├── versions/finance_itsc/
│   └── year=2024/
│       ├── v_20260306_095749/
│       │   ├── part-00000.parquet   ← snapshot ข้อมูล
│       │   └── _version.json        ← metadata
│       └── v_20260301_120000/
│           ├── part-00000.parquet
│           └── _version.json
└── trash/
    └── 20260306/                    ← versions ที่ถูก soft delete
        └── datalake__versions__finance_itsc__year=2024__v_old/
```

## _version.json ตัวอย่าง

```json
{
  "version": "v_20260306_095749",
  "source_file": "finance_2024.csv",
  "year": 2024,
  "timestamp": "2026-03-06T09:57:49",
  "row_count": 1500,
  "checksum": "abc123def456",
  "schema_hash": "a3f9c2d1e4b8",
  "columns": ["date", "details", "year", "total_amount", "..."],
  "keep_versions": 5
}
```

`schema_hash` — 12 char hex ที่ generate จาก column names ใช้ detect ว่า schema เปลี่ยนระหว่าง version ไหม

## การใช้งานผ่าน manage.py CLI (แนะนำ)

ดูรายละเอียดทุก command ได้ที่ [docs/manage.md](manage.md)

```bash
# ดู versions ทั้งหมด
spark-submit /jobs/manage.py versions 2024

# เปรียบเทียบ 2 versions
spark-submit /jobs/manage.py diff 2024 v_20260306_095749 v_20260301_120000

# restore version เก่า
spark-submit /jobs/manage.py restore 2024 v_20260301_120000 --yes

# ดู trash
spark-submit /jobs/manage.py trash 2024

# cleanup เก็บ 5 ล่าสุด
spark-submit /jobs/manage.py cleanup 2024 --keep 5 --yes
```

## การใช้งานผ่าน Python API

### ดู versions ทั้งหมดของปี

```python
from utils.versioning import list_versions

versions = list_versions(sc, year=2024)
for v in versions:
    print(f"{v['version']} | {v['timestamp']} | rows={v['row_count']} | schema={v.get('schema_hash', 'N/A')}")

# Output:
# v_20260306_095749 | 2026-03-06T09:57:49 | rows=1500 | schema=a3f9c2d1e4b8
# v_20260301_120000 | 2026-03-01T12:00:00 | rows=1480 | schema=a3f9c2d1e4b8
```

### Rollback ไป version เก่า

```python
from utils.versioning import restore_version

restore_version(
    spark,
    version_id="v_20260215_090000",   # version ที่อยากกลับไป
    year=2024,
    target_table="finance_itsc_wide",
    target_path="hdfs://namenode:8020/datalake/staging/finance_itsc_wide",
)
```

> ✅ ใช้ atomic write — ข้อมูลปีอื่นไม่โดนแตะ ถ้า restore fail ข้อมูลเดิมยังอยู่ครบ

### เปรียบเทียบ 2 versions (Schema + Rows)

```python
from utils.versioning import diff_versions

result = diff_versions(sc, year=2024,
    version_a="v_20260306_095749",
    version_b="v_20260301_120000")

print(result["schema"]["added"])    # columns ที่เพิ่มมา
print(result["schema"]["removed"])  # columns ที่หายไป
print(result["rows"]["a"])          # row count version A
print(result["rows"]["b"])          # row count version B
```

### ตรวจ Schema Hash

```python
from utils.versioning import compute_schema_hash

hash_a = compute_schema_hash(df_a.schema)
hash_b = compute_schema_hash(df_b.schema)

if hash_a != hash_b:
    print("Schema เปลี่ยนแล้ว!")
```

### เปลี่ยนจำนวน version ที่เก็บ

ใน `.env`:
```env
KEEP_VERSIONS=10
```

หรือระบุตอนเรียก:
```python
from utils.versioning import cleanup_old_versions
cleanup_old_versions(sc, year=2024, keep=10)
```

> versions ที่ถูก cleanup จะถูก **soft delete** ไปที่ `/datalake/trash/` ไม่ได้ลบถาวรทันที

## Soft Delete & Trash

versions ที่ถูกลบผ่าน `cleanup` หรือ atomic write จะไปอยู่ใน trash ก่อนเสมอ

```python
from utils.soft_delete import list_trash, purge_old_trash

# ดูของใน trash
items = list_trash(sc)
for item in items:
    print(item)

# purge trash เก่ากว่า 30 วัน
purge_old_trash(sc, keep_days=30)
```

## Config

| Variable | Default | Description |
|----------|---------|-------------|
| `KEEP_VERSIONS` | 5 | จำนวน version ที่เก็บต่อปี |
| `VERSIONS_BASE_PATH` | `/datalake/versions/finance_itsc` | HDFS path สำหรับเก็บ versions |
| `TRASH_BASE_PATH` | `/datalake/trash` | HDFS path สำหรับ soft delete |