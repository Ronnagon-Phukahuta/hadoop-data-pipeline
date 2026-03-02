# utils/history.py
import json
import os
import streamlit as st
from datetime import datetime
from config import HISTORY_FILE, MAX_HISTORY


@st.cache_data(ttl=1)
def load_chat_history() -> list:
    """โหลด chat history จากไฟล์"""
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return []


def save_chat_history(history: list):
    """บันทึก chat history ลงไฟล์"""
    with open(HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    load_chat_history.clear()


def add_to_history(
    question: str,
    sql: str,
    summary: str,
    df_json: str,
    chart_type: str,
    execution_time_ms: int = 0,
) -> int:
    """
    เพิ่มรายการใหม่ลง history

    Returns:
        index ของ record ที่เพิ่งเพิ่ม (ใช้สำหรับ save_feedback)
    """
    history = load_chat_history()
    history.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "question": question,
        "sql": sql,
        "summary": summary,
        "df_json": df_json,
        "chart_type": chart_type,
        "execution_time_ms": execution_time_ms,
        "feedback": None,
    })
    history = history[-MAX_HISTORY:]
    save_chat_history(history)
    return len(history) - 1


def save_feedback(index: int, feedback: str):
    print(f"DEBUG save_feedback index={index} feedback={feedback}")
    """
    บันทึก feedback สำหรับ query นั้น

    Args:
        index: index ของ record ใน history list
        feedback: "up" หรือ "down"
    """
    if feedback not in ("up", "down"):
        return
    history = load_chat_history()
    if 0 <= index < len(history):
        history[index]["feedback"] = feedback
        save_chat_history(history)


def get_stats() -> dict:
    """สถิติ query history"""
    history = load_chat_history()
    if not history:
        return {"total": 0, "thumbs_up": 0, "thumbs_down": 0, "avg_execution_ms": 0}

    thumbs_up = sum(1 for r in history if r.get("feedback") == "up")
    thumbs_down = sum(1 for r in history if r.get("feedback") == "down")
    avg_ms = sum(r.get("execution_time_ms", 0) for r in history) // len(history)

    return {
        "total": len(history),
        "thumbs_up": thumbs_up,
        "thumbs_down": thumbs_down,
        "avg_execution_ms": avg_ms,
    }


def clear_history():
    """ล้าง history ทั้งหมด"""
    save_chat_history([])