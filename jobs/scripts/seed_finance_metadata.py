#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
seed_finance_metadata.py
Usage:
    docker exec streamlit-dashboard python3 /jobs/scripts/seed_finance_metadata.py
"""

import json
import os
from datetime import datetime
from pyhive import hive

HIVE_HOST = os.environ.get("HIVE_HOST", "hive-server")
HIVE_PORT = int(os.environ.get("HIVE_PORT", 10000))
DATASET = "finance_itsc"

# แทน ' ด้วย __SQ__ แล้ว decode ตอนอ่าน
SQ = "__SQ__"

SCHEMA = [
    {"name": "date",     "type": "STRING",  "thai_name": "เดือน",      "is_id": True,  "is_date": True,  "is_amount": False, "reserved_keyword": True,  "allowed_values": [], "notes": "เดือนของรายการ เชน 2024-01"},
    {"name": "details",  "type": "STRING",  "thai_name": "ชนิดรายการ", "is_id": True,  "is_date": False, "is_amount": False, "reserved_keyword": False, "allowed_values": ["budget","spent","remaining"], "notes": "budget/spent SUM ได remaining หาม SUM"},
    {"name": "category", "type": "STRING",  "thai_name": "หมวดหมู่",   "is_id": False, "is_date": False, "is_amount": False, "reserved_keyword": False, "allowed_values": [], "notes": "ชื่อ column จาก category_mapping"},
    {"name": "amount",   "type": "DECIMAL", "thai_name": "จำนวนเงิน",  "is_id": False, "is_date": False, "is_amount": True,  "reserved_keyword": False, "allowed_values": [], "notes": "จำนวนเงิน บาท"},
]

CATEGORY_MAPPING = {
    "general_fund_admin_wifi_grant": ["กองทุนทั่วไป","งานบริหารทั่วไป","wifi"],
    "compensation_budget": ["คาตอบแทน","งบประจำคาตอบแทน"],
    "expense_budget": ["คาใชสอย","งบประจำคาใชสอย"],
    "material_budget": ["คาวัสดุ","งบประจำคาวัสดุ"],
    "utilities": ["คาสาธารณูปโภค"],
    "grant_welfare_health": ["เงินอุดหนุนสวัสดิการและสุขภาพบุคลากร"],
    "grant_ms_365": ["เงินอุดหนุน MS 365","Microsoft 365"],
    "education_fund_academic_computer_service_salary_staff": ["เงินเดือนพนักงานเงินรายได","พนงเงินรายได S"],
    "government_staff": ["พนักงานเงินแผ่นดิน"],
    "asset_fund_academic_computer_service_equipment_budget": ["คาครุภัณฑ์วงเงินไมเกิน 1 ลาน"],
    "equipment_budget_over_1m": ["คาครุภัณฑ์วงเงินเกิน 1 ลาน"],
    "permanent_asset_fund_land_construction": ["กองทุนสินทรัพย์ถาวรที่ดินและสิ่งกอสราง"],
    "equipment_firewall": ["ครุภัณฑ์ Firewall"],
    "grant_siem": ["เงินอุดหนุน SIEM"],
    "grant_data_center": ["เงินอุดหนุน data center"],
    "grant_wifi_satit": ["เงินอุดหนุน wifi satit"],
    "research_fund_research_admin_personnel_research_grant": ["วิจัยบุคลากร","เงินอุดหนุนทั่วไปวิจัย"],
    "reserve_fund_general_admin_other_expenses_reserve": ["สำรองจาย","งบสำรอง","รายจายอื่น"],
    "contribute_development_fund": ["สมทบกองทุนพัฒนา"],
    "contribute_personnel_development_fund_cmu": ["สมทบกองทุนพัฒนาบุคลากร มช"],
    "personnel_development_fund_education_management_support_special_grant": ["อุดหนุนเฉพาะกิจ","งานสนับสนุนการจัดการศึกษา"],
    "art_preservation_fund_general_grant": ["อุดหนุนทั่วไป","งานทำนุ","ทำนุบำรุงศิลปะ"],
    "wifi_jumboplus": ["Wifi Jumboplus","Jumboplus"],
    "firewall": ["Firewall"],
    "cmu_cloud": ["CMU Cloud"],
    "siem": ["SIEM","SiEM"],
    "digital_health": ["Digital Health"],
    "benefit_access_request_system": ["ระบบการขอเขาทำประโยชน"],
    "ups": ["UPS"],
    "ups_rent_wifi_care": ["เชา UPS ดูแล wifi"],
    "uplift": ["Uplift"],
    "open_data": ["Open data"],
}

NLP_RULES = [
    "date เปน reserved keyword ตองใส backtick ทุกครั้ง",
    "budget และ spent SUM ไดปกติ",
    "remaining คือ running balance หาม SUM ตองดึงเฉพาะเดือนลาสุดดวย JOIN MAX date",
    "หาม ใช SUM CASE WHEN details=remaining",
    "เวลาใช ORDER BY ตองมี column นั้นใน SELECT ดวย",
    "หามใช subquery ใน WHERE ที่มีเงื่อนไขอื่นรวม",
    "ขาดทุน/งบเกิน = remaining นอย ไมใช spent เยอะ",
    "ใชชื่อ category จาก mapping เทานั้น หามแปลชื่อเอง",
]

# ใช้ __SQ__ แทน ' ใน SQL — hive_metadata.py จะ decode กลับตอนอ่าน
EXAMPLE_QUERIES = [
    {
        "q": "คาใชสอยมียอดคงเหลือเทาไร",
        "sql": "SELECT `date`, category, amount AS remaining_budget FROM finance_itsc_long WHERE details = __SQ__remaining__SQ__ AND category = __SQ__expense_budget__SQ__ ORDER BY `date` DESC LIMIT 1"
    },
    {
        "q": "แตละหมวดมียอดคงเหลือเทาไร",
        "sql": "SELECT t.category, t.amount FROM finance_itsc_long t JOIN (SELECT category, MAX(`date`) AS max_date FROM finance_itsc_long WHERE details = __SQ__remaining__SQ__ GROUP BY category) latest ON t.category = latest.category AND t.`date` = latest.max_date WHERE t.details = __SQ__remaining__SQ__ ORDER BY t.amount ASC"
    },
    {
        "q": "ยอดใชจายรวมป 2024",
        "sql": "SELECT SUM(amount) AS total_spent FROM finance_itsc_long WHERE details = __SQ__spent__SQ__ AND year = 2024"
    },
]


def esc(s):
    """แทน single quote ด้วย __SQ__ เพื่อให้ Hive INSERT ผ่าน"""
    return str(s).replace("'", SQ)


def main():
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("Connecting to Hive...")
    conn = hive.connect(host=HIVE_HOST, port=HIVE_PORT, database="default")
    cursor = conn.cursor()
    cursor.execute("SET hive.exec.dynamic.partition.mode=nonstrict")

    print("Recreating metadata tables...")
    cursor.execute("DROP TABLE IF EXISTS column_metadata")
    cursor.execute("DROP TABLE IF EXISTS category_mapping_meta")
    cursor.execute("DROP TABLE IF EXISTS nlp_config")

    cursor.execute("""
        CREATE TABLE column_metadata (
          col_name           STRING,
          col_type           STRING,
          thai_name          STRING,
          description        STRING,
          is_amount          BOOLEAN,
          is_id              BOOLEAN,
          is_date            BOOLEAN,
          nullable           BOOLEAN,
          reserved_keyword   BOOLEAN,
          allowed_values     STRING,
          notes              STRING,
          updated_at         STRING
        )
        PARTITIONED BY (ds STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
        STORED AS TEXTFILE
    """)
    cursor.execute("""
        CREATE TABLE category_mapping_meta (
          col_name    STRING,
          thai_alias  STRING
        )
        PARTITIONED BY (dataset_name STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
        STORED AS TEXTFILE
    """)
    cursor.execute("""
        CREATE TABLE nlp_config (
          config_type  STRING,
          content      STRING,
          sort_order   INT
        )
        PARTITIONED BY (dataset_name STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
        STORED AS TEXTFILE
    """)
    print("Tables created.")

    # ── column_metadata ──────────────────────────────────────
    print("Inserting column_metadata (%d rows)..." % len(SCHEMA))
    for col in SCHEMA:
        allowed = esc(json.dumps(col["allowed_values"], ensure_ascii=False))
        cursor.execute(
            "INSERT INTO column_metadata PARTITION (ds='%s') VALUES "
            "('%s','%s','%s','',%s,%s,%s,TRUE,%s,'%s','%s','%s')"
            % (
                DATASET,
                col["name"], col["type"], esc(col["thai_name"]),
                str(col["is_amount"]).upper(),
                str(col["is_id"]).upper(),
                str(col["is_date"]).upper(),
                str(col["reserved_keyword"]).upper(),
                allowed, esc(col["notes"]), now,
            )
        )
    print("  done.")

    # ── category_mapping_meta ────────────────────────────────
    total = sum(len(v) for v in CATEGORY_MAPPING.values())
    print("Inserting category_mapping_meta (%d rows)..." % total)
    count = 0
    for col_name, aliases in CATEGORY_MAPPING.items():
        for alias in aliases:
            cursor.execute(
                "INSERT INTO category_mapping_meta PARTITION (dataset_name='%s') VALUES ('%s','%s')"
                % (DATASET, col_name, esc(alias))
            )
            count += 1
            if count % 10 == 0:
                print("  %d/%d" % (count, total))
    print("  done.")

    # ── nlp_config ───────────────────────────────────────────
    total_nlp = len(NLP_RULES) + len(EXAMPLE_QUERIES)
    print("Inserting nlp_config (%d rows)..." % total_nlp)
    for i, rule in enumerate(NLP_RULES):
        cursor.execute(
            "INSERT INTO nlp_config PARTITION (dataset_name='%s') VALUES ('rule','%s',%d)"
            % (DATASET, esc(rule), i)
        )
    for i, ex in enumerate(EXAMPLE_QUERIES):
        content = esc(json.dumps(ex, ensure_ascii=False))
        cursor.execute(
            "INSERT INTO nlp_config PARTITION (dataset_name='%s') VALUES ('example','%s',%d)"
            % (DATASET, content, i)
        )
    print("  done.")

    conn.close()
    print("\nDone! finance_itsc metadata seeded successfully.")


if __name__ == "__main__":
    main()