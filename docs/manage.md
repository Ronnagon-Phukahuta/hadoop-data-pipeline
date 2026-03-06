# manage.py — Dataset CLI

CLI สำหรับจัดการ dataset versions บน HDFS ผ่าน `spark-submit` โดยไม่ต้องแก้ไขโค้ดโดยตรง

## Usage

```bash
spark-submit /jobs/manage.py <command> <year> [options]
```

## Commands

### `versions` — ดู versions ทั้งหมด

```bash
spark-submit /jobs/manage.py versions 2024
```

ตัวอย่าง output:
```
Version              Timestamp              Rows    Schema Hash    Source
v_20260306_095749    2026-03-06T09:57:49    1500    a3f9c2d1e4b8   finance_2024.csv
v_20260301_120000    2026-03-01T12:00:00    1480    a3f9c2d1e4b8   finance_2024_v2.csv
v_20260215_090000    2026-02-15T09:00:00    1450    b1d2e3f4a5c6   finance_2024_old.csv
```

---

### `diff` — เปรียบเทียบ 2 versions

```bash
spark-submit /jobs/manage.py diff 2024 v_20260306_095749 v_20260301_120000
```

ตัวอย่าง output:
```
=== Schema ===
No schema changes

=== Rows ===
v_20260306_095749: 1500 rows
v_20260301_120000: 1480 rows
Diff: +20 rows
```

ถ้า schema เปลี่ยน:
```
=== Schema ===
Added columns  : ['new_budget_item']
Removed columns: []
```

**Export เป็น JSON:**
```bash
spark-submit /jobs/manage.py diff 2024 v1 v2 --json
```

---

### `restore` — restore version เก่ากลับ staging

```bash
spark-submit /jobs/manage.py restore 2024 v_20260215_090000 --yes
```

> ⚠️ ต้องใส่ `--yes` เสมอ — ป้องกัน restore โดยไม่ตั้งใจ

ถ้าไม่ใส่ `--yes` จะแสดง hint:
```
Hint: เพิ่ม --yes เพื่อยืนยัน restore
```

restore ใช้ **atomic write** — ข้อมูลปีอื่นไม่โดนแตะ ถ้า fail ข้อมูลเดิมยังอยู่ครบ

---

### `trash` — ดู versions ใน trash

```bash
spark-submit /jobs/manage.py trash 2024
```

ตัวอย่าง output:
```
Trash items for year=2024:
  20260306 — datalake__versions__finance_itsc__year=2024__v_20260215_090000
  20260301 — datalake__versions__finance_itsc__year=2024__v_20260101_080000
```

---

### `cleanup` — ลบ versions เก่า

```bash
spark-submit /jobs/manage.py cleanup 2024 --keep 5 --yes
```

เก็บ 5 versions ล่าสุด ลบที่เหลือ (ผ่าน soft delete → trash)

ตัวอย่าง output:
```
Keeping 5 versions, deleting 2 old versions
Deleted: v_20260101_080000
Deleted: v_20260115_100000
```

ถ้ามีไม่ถึง `--keep`:
```
Nothing to delete (3 versions ≤ keep=5)
```

---

## Notes

- ทุก command ต้องรันผ่าน `spark-submit` เพราะต้องการ SparkContext สำหรับเชื่อม HDFS
- `restore` และ `cleanup` ต้องใส่ `--yes` เสมอ — ป้องกัน side effect โดยไม่ตั้งใจ
- versions ที่ถูกลบผ่าน `cleanup` จะไปอยู่ใน `/datalake/trash/` ยังสามารถ recover ได้ภายใน 30 วัน
- Schema hash ใช้ detect ว่า schema เปลี่ยนระหว่าง version ไหม (12 char hex)