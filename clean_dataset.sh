#!/bin/bash
# clean_dataset.sh — ลบ dataset ออกจากระบบทั้งหมด
# Usage:
#   ./clean_dataset.sh finance_itsc_test_01
#   ./clean_dataset.sh finance_itsc_test_01 --keep-metadata   (ไม่ลบ Hive metadata partitions)
#   ./clean_dataset.sh finance_itsc_test_01 --dry-run         (แสดงสิ่งที่จะลบ แต่ไม่ลบจริง)

set -e

DATASET="$1"
KEEP_METADATA=false
DRY_RUN=false

if [ -z "$DATASET" ]; then
    echo "Usage: $0 <dataset_name> [--keep-metadata] [--dry-run]"
    exit 1
fi

for arg in "$@"; do
    case $arg in
        --keep-metadata) KEEP_METADATA=true ;;
        --dry-run)       DRY_RUN=true ;;
    esac
done

echo "=========================================="
echo "  Dataset Cleanup: $DATASET"
[ "$DRY_RUN" = true ]  && echo "  MODE: DRY RUN (ไม่ลบจริง)"
[ "$KEEP_METADATA" = true ] && echo "  Hive metadata: SKIP"
echo "=========================================="

run() {
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] $*"
    else
        eval "$@"
    fi
}

# ── 1. Drop Hive tables ─────────────────────────────────────
echo ""
echo "▶ [1/5] Drop Hive tables..."
run docker exec hive-server beeline -u jdbc:hive2://localhost:10000 \
    -e \"DROP TABLE IF EXISTS ${DATASET}_wide\; DROP TABLE IF EXISTS ${DATASET}_long\;\" \
    2>/dev/null
echo "   ✅ Dropped: ${DATASET}_wide, ${DATASET}_long"

# ── 2. ลบ HDFS paths ────────────────────────────────────────
echo ""
echo "▶ [2/5] ลบ HDFS paths..."

HDFS_PATHS=(
    "/datalake/raw/${DATASET}"
    "/datalake/staging/${DATASET}_wide"
    "/datalake/curated/${DATASET}_long"
    "/datalake/versions/${DATASET}"
    "/datalake/original/${DATASET}"
)

for path in "${HDFS_PATHS[@]}"; do
    HDFS_URI="hdfs://namenode:8020${path}"
    EXISTS=$(docker exec hive-server hdfs dfs -test -e "$HDFS_URI" 2>/dev/null && echo "yes" || echo "no")
    if [ "$EXISTS" = "yes" ]; then
        run docker exec hive-server hdfs dfs -rm -r "$HDFS_URI"
        echo "   ✅ Deleted: $path"
    else
        echo "   ⏭️  Not found: $path"
    fi
done

# ── 3. ลบ yaml ──────────────────────────────────────────────
echo ""
echo "▶ [3/5] ลบ dataset yaml..."
YAML_PATH="/jobs/datasets/${DATASET}.yaml"
EXISTS=$(docker exec streamlit-dashboard test -f "$YAML_PATH" 2>/dev/null && echo "yes" || echo "no")
if [ "$EXISTS" = "yes" ]; then
    run docker exec streamlit-dashboard rm -f "$YAML_PATH"
    echo "   ✅ Deleted: $YAML_PATH"
else
    echo "   ⏭️  Not found: $YAML_PATH"
fi

# ── 4. ลบ Hive metadata partitions ──────────────────────────
echo ""
if [ "$KEEP_METADATA" = true ]; then
    echo "▶ [4/5] Hive metadata — SKIPPED (--keep-metadata)"
else
    echo "▶ [4/5] ลบ Hive metadata partitions..."
    # เขียน python script ไปที่ container ก่อน (หลีก multiline -c issue ใน Git Bash)
    docker exec streamlit-dashboard bash -c "printf '%s\n' \
        'from pyhive import hive' \
        'c = hive.connect(\"hive-server\", 10000).cursor()' \
        'tables = [' \
        '  (\"ALTER TABLE column_metadata DROP IF EXISTS PARTITION (ds=\\x27${DATASET}\\x27)\", \"column_metadata\"),' \
        '  (\"ALTER TABLE category_mapping_meta DROP IF EXISTS PARTITION (dataset_name=\\x27${DATASET}\\x27)\", \"category_mapping_meta\"),' \
        '  (\"ALTER TABLE nlp_config DROP IF EXISTS PARTITION (dataset_name=\\x27${DATASET}\\x27)\", \"nlp_config\"),' \
        ']' \
        'for sql, label in tables:' \
        '    try:' \
        '        c.execute(sql)' \
        '        print(\"   OK: \" + label)' \
        '    except Exception as e:' \
        '        print(\"   WARN: \" + label + \" - \" + str(e))' \
        > /tmp/_clean_meta.py"
    if [ "$DRY_RUN" = true ]; then
        echo "[DRY RUN] drop metadata partitions for ${DATASET}"
    else
        docker exec streamlit-dashboard python3 /tmp/_clean_meta.py
    fi
fi

# ── 5. ลบ .done files ใน HDFS (ถ้าเหลืออยู่) ────────────────
echo ""
echo "▶ [5/5] ตรวจสอบ .done files..."
DONE_COUNT=$(docker exec hive-server hdfs dfs -ls -R "hdfs://namenode:8020/datalake/raw/${DATASET}" 2>/dev/null | grep "\.done" | wc -l || echo "0")
if [ "$DONE_COUNT" -gt 0 ]; then
    echo "   ⚠️  พบ .done files $DONE_COUNT ไฟล์ (จะถูกลบพร้อม raw path แล้ว)"
else
    echo "   ✅ ไม่มี .done files เหลือ"
fi

echo ""
echo "=========================================="
if [ "$DRY_RUN" = true ]; then
    echo "  DRY RUN เสร็จสิ้น — ไม่มีอะไรถูกลบ"
else
    echo "  ✅ Cleanup เสร็จสิ้น: $DATASET"
fi
echo "=========================================="