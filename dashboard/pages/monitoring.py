# dashboard/pages/monitoring.py
"""
Monitoring Dashboard — Pipeline Status, DQ Pass/Fail Rate, Version History
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from auth import require_auth
from services.monitoring import get_pipeline_runs, get_dq_stats, get_version_history

st.set_page_config(
    page_title="Monitoring — Finance ITSC",
    page_icon="📊",
    layout="wide",
)

require_auth()

st.title("📊 Monitoring Dashboard")

if st.button("🔄 Refresh", type="primary"):
    st.rerun()

# ═══════════════════════════════════════════════════
# SECTION 1 — Pipeline Status
# ═══════════════════════════════════════════════════
st.header("🚀 Pipeline Runs")

runs = get_pipeline_runs()

if not runs:
    st.info("ยังไม่มีข้อมูล pipeline run")
else:
    # Summary metrics
    total = len(runs)
    success = sum(1 for r in runs if r["status"] == "success")
    failed = sum(1 for r in runs if r["status"] == "failed")
    partial = sum(1 for r in runs if r["status"] == "partial")
    avg_duration = sum(r["duration_sec"] for r in runs) // total if total else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Run ทั้งหมด", total)
    col2.metric("✅ Success", success)
    col3.metric("⚠️ Partial", partial)
    col4.metric("❌ Failed", failed)
    col5.metric("เวลาเฉลี่ย", f"{avg_duration}s")

    # Run table
    rows = []
    for r in runs[:20]:
        status_icon = {"success": "✅", "failed": "❌", "partial": "⚠️", "running": "🔄"}.get(r["status"], "❓")
        rows.append({
            "เวลาเริ่ม": r["start_time"][:16].replace("T", " "),
            "Status": f"{status_icon} {r['status']}",
            "ปีที่สำเร็จ": ", ".join(str(y) for y in r["years_processed"]) or "—",
            "ปีที่ fail": ", ".join(str(y) for y in r["years_failed"]) or "—",
            "DQ Pass": r["dq_checks"]["pass"],
            "DQ Fail": r["dq_checks"]["fail"],
            "ไฟล์ใหม่": r["files"]["pending"],
            "ระยะเวลา (s)": r["duration_sec"],
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # แสดง errors ของ run ล่าสุดถ้ามี
    latest = runs[0]
    if latest["errors"]:
        with st.expander(f"❌ Errors จาก run ล่าสุด ({len(latest['errors'])} รายการ)"):
            for e in latest["errors"]:
                st.error(f"**{e['timestamp'][:16]}** — {e['message']}")
                if e["error"]:
                    st.code(e["error"][:300], language="text")

# ═══════════════════════════════════════════════════
# SECTION 2 — DQ Pass/Fail Rate
# ═══════════════════════════════════════════════════
st.divider()
st.header("🔍 Data Quality")

dq_stats = get_dq_stats()
total_pass = sum(v["pass"] for v in dq_stats.values())
total_fail = sum(v["fail"] for v in dq_stats.values())

if total_pass + total_fail == 0:
    st.info("ยังไม่มีข้อมูล DQ checks")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("✅ Pass ทั้งหมด", total_pass)
    col2.metric("❌ Fail ทั้งหมด", total_fail)
    rate = round(total_pass / (total_pass + total_fail) * 100, 1) if (total_pass + total_fail) > 0 else 0
    col3.metric("Pass Rate", f"{rate}%")

    col_left, col_right = st.columns(2)

    with col_left:
        # Bar chart pass/fail per check
        check_names = list(dq_stats.keys())
        pass_vals = [dq_stats[c]["pass"] for c in check_names]
        fail_vals = [dq_stats[c]["fail"] for c in check_names]

        fig = go.Figure()
        fig.add_trace(go.Bar(name="Pass", x=check_names, y=pass_vals, marker_color="#22c55e"))
        fig.add_trace(go.Bar(name="Fail", x=check_names, y=fail_vals, marker_color="#ef4444"))
        fig.update_layout(
            barmode="group",
            title="Pass/Fail แต่ละ Check",
            xaxis_tickangle=-20,
            legend=dict(orientation="h"),
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        # Pie chart overall
        fig2 = px.pie(
            values=[total_pass, total_fail],
            names=["Pass", "Fail"],
            color_discrete_sequence=["#22c55e", "#ef4444"],
            title="Pass Rate รวม",
            height=350,
        )
        st.plotly_chart(fig2, use_container_width=True)

# ═══════════════════════════════════════════════════
# SECTION 3 — Version History
# ═══════════════════════════════════════════════════
st.divider()
st.header("📦 Version History")

versions = get_version_history()

if not versions:
    st.info("ยังไม่มี version snapshot")
else:
    st.metric("Snapshots ทั้งหมด", len(versions))

    rows = []
    for v in versions:
        rows.append({
            "Version": v["version"],
            "ปี": v["year"],
            "Rows": v["rows"],
            "Checksum": v["checksum"],
            "เวลา": v["timestamp"][:16].replace("T", " "),
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # rows per version chart
    if len(versions) > 1:
        df_v = pd.DataFrame(rows)
        fig3 = px.bar(
            df_v,
            x="Version",
            y="Rows",
            color="ปี",
            title="Rows per Snapshot",
            height=300,
        )
        fig3.update_xaxes(tickangle=-30)
        st.plotly_chart(fig3, use_container_width=True)