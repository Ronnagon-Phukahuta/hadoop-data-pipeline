# dashboard/pages/monitoring.py
"""
Monitoring Dashboard — System Health, Pipeline Status, DQ Pass/Fail Rate, Version History
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from auth import require_auth
from services.monitoring import (
    get_pipeline_runs, get_dq_stats, get_version_history, get_health_status,
    PIPELINE_STEPS, STEP_LABELS, VERSIONS_BASE,
)
import requests
import os

WEBHDFS_URL = os.environ.get("WEBHDFS_URL", "http://namenode:50070")


def _list_datasets() -> list:
    """list dataset names จาก /datalake/versions/"""
    try:
        url = f"{WEBHDFS_URL}/webhdfs/v1{VERSIONS_BASE}?op=LISTSTATUS"
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            items = r.json().get("FileStatuses", {}).get("FileStatus", [])
            return sorted([i["pathSuffix"] for i in items if i["type"] == "DIRECTORY"])
    except Exception:
        pass
    return []


st.set_page_config(
    page_title="Monitoring — Finance ITSC",
    page_icon="📊",
    layout="wide",
)

require_auth()

st.title("📊 Monitoring Dashboard")

col_refresh, col_cache_note = st.columns([1, 5])
with col_refresh:
    if st.button("🔄 Refresh", type="primary"):
        get_pipeline_runs.clear()
        get_dq_stats.clear()
        get_version_history.clear()
        get_health_status.clear()
        st.rerun()
with col_cache_note:
    st.caption("ข้อมูล cache 60 วินาที กด Refresh เพื่ออัพเดททันที")


# ═══════════════════════════════════════════════════
# SECTION 0 — System Health
# ═══════════════════════════════════════════════════
st.header("🩺 System Health")

health = get_health_status()
st.caption(f"ตรวจสอบล่าสุด: {health['checked_at']}")

col_hdfs, col_hive = st.columns(2)

with col_hdfs:
    hdfs = health["hdfs"]
    if hdfs["ok"]:
        st.success(f"✅ HDFS — {hdfs['msg']}")
    else:
        st.error(f"❌ HDFS — {hdfs['msg']}")

with col_hive:
    hive_h = health["hive"]
    if hive_h["ok"]:
        st.success(f"✅ Hive — {hive_h['msg']}")
    else:
        st.error(f"❌ Hive — {hive_h['msg']}")

# Dataset row counts
if health["datasets"]:
    st.subheader("📦 Dataset Status")
    ds_cols = st.columns(min(len(health["datasets"]), 4))
    for i, ds in enumerate(health["datasets"]):
        with ds_cols[i % 4]:
            icon = "✅" if ds["ok"] else "❌"
            wide = f"{ds['wide_rows']:,}" if ds["wide_rows"] is not None else "N/A"
            long_ = f"{ds['long_rows']:,}" if ds["long_rows"] is not None else "N/A"
            st.metric(
                label=f"{icon} {ds['name']}",
                value=f"wide: {wide}",
                delta=f"long: {long_}",
                delta_color="off",
            )
            if not ds["ok"] and ds["msg"] not in ("", "OK"):
                st.caption(f"⚠️ {ds['msg']}")
else:
    st.info("ไม่พบ dataset ใดๆ")


# ═══════════════════════════════════════════════════
# SECTION 1 — Pipeline Status
# ═══════════════════════════════════════════════════
st.divider()
st.header("🚀 Pipeline Runs")

runs = get_pipeline_runs()

if not runs:
    st.info("ยังไม่มีข้อมูล pipeline run")
else:
    total = len(runs)
    success = sum(1 for r in runs if r["status"] == "success")
    failed = sum(1 for r in runs if r["status"] == "failed")
    partial = sum(1 for r in runs if r["status"] == "partial")

    completed_durations = [r["duration_sec"] for r in runs if r["duration_sec"] > 0 and r["status"] != "running"]
    avg_duration = int(sum(completed_durations) / len(completed_durations)) if completed_durations else 0

    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("Run ทั้งหมด", total)
    col2.metric("✅ Success", success)
    col3.metric("⚠️ Partial", partial)
    col4.metric("❌ Failed", failed)
    col5.metric("เวลาเฉลี่ย", f"{avg_duration}s")

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

    st.subheader("⏱️ Step Duration")

    run_options = {
        f"{r['start_time'][:16].replace('T', ' ')} — {r['status']} (pid {r['pid']})": r
        for r in runs
    }
    selected_label = st.selectbox("เลือก Pipeline Run", options=list(run_options.keys()))
    selected_run = run_options[selected_label]
    step_dur = selected_run.get("step_durations", {})

    if not step_dur:
        st.caption("ยังไม่มีข้อมูล step duration สำหรับ run นี้")
    else:
        ordered = [(STEP_LABELS.get(s, s), step_dur[s]) for s in PIPELINE_STEPS if s in step_dur]
        df_steps = pd.DataFrame(ordered, columns=["Step", "Duration (ms)"])

        fig_steps = px.bar(
            df_steps,
            x="Duration (ms)",
            y="Step",
            orientation="h",
            color="Duration (ms)",
            color_continuous_scale=["#22c55e", "#f59e0b", "#ef4444"],
            title=f"Step Duration — {selected_run['start_time'][:16].replace('T', ' ')}",
            height=max(250, len(ordered) * 45),
            text="Duration (ms)",
        )
        fig_steps.update_traces(texttemplate="%{text:,.0f} ms", textposition="outside")
        fig_steps.update_layout(
            coloraxis_showscale=False,
            yaxis=dict(autorange="reversed"),
            xaxis_title="milliseconds",
            margin=dict(l=10, r=80),
        )
        st.plotly_chart(fig_steps, use_container_width=True)

    if selected_run["errors"]:
        with st.expander(f"❌ Errors ({len(selected_run['errors'])} รายการ)"):
            for e in selected_run["errors"]:
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
    rate = round(total_pass / (total_pass + total_fail) * 100, 1)
    col3.metric("Pass Rate", f"{rate}%")

    col_left, col_right = st.columns(2)

    with col_left:
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
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_right:
        fig2 = px.pie(
            values=[total_pass, total_fail],
            names=["Pass", "Fail"],
            color_discrete_sequence=["#22c55e", "#ef4444"],
            title="Pass Rate รวม",
            height=350,
        )
        fig2.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5)
        )
        st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════════
# SECTION 3 — Version History
# ═══════════════════════════════════════════════════
st.divider()
st.header("📦 Version History")

available_datasets = _list_datasets()
if available_datasets:
    col_ds, _ = st.columns([2, 4])
    with col_ds:
        selected_dataset = st.selectbox(
            "Dataset",
            ["(ทั้งหมด)"] + available_datasets,
            key="version_dataset_select",
        )
    ds_filter = None if selected_dataset == "(ทั้งหมด)" else selected_dataset
else:
    ds_filter = None

versions = get_version_history(dataset_name=ds_filter)

if not versions:
    st.info("ยังไม่มี version snapshot")
else:
    st.metric("Snapshots ทั้งหมด", len(versions))

    rows = []
    for v in versions:
        rows.append({
            "Dataset": v.get("dataset", "—"),
            "Version": v["version"],
            "ปี": v["year"],
            "Rows": v["rows"],
            "Checksum": v["checksum"],
            "เวลา": v["timestamp"][:16].replace("T", " "),
        })

    st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    if len(versions) > 1:
        df_v = pd.DataFrame(rows)
        df_v["ปี"] = df_v["ปี"].astype(str)
        df_v["เวลา (สั้น)"] = df_v["เวลา"].str[5:16]
        fig3 = px.bar(
            df_v,
            x="เวลา (สั้น)",
            y="Rows",
            color="Dataset" if ds_filter is None else "ปี",
            hover_data={"Version": True, "เวลา (สั้น)": False},
            title="Rows per Snapshot",
            height=300,
        )
        fig3.update_layout(
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig3, use_container_width=True)