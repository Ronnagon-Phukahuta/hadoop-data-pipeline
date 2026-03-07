from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
from airflow.models import Variable
from datetime import datetime, timedelta


# ============================================================
# Email Alert Functions
# ============================================================

def _get_alert_email():
    return Variable.get("alert_email", default_var=None)


def alert_on_failure(context):
    """ส่ง email เมื่อ task fail"""
    alert_email = _get_alert_email()
    if not alert_email:
        return

    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    execution_date = context["logical_date"]
    log_url = context["task_instance"].log_url
    exception = context.get("exception", "Unknown error")

    subject = f"❌ [Airflow] DAG Failed: {dag_id} > {task_id}"
    body = f"""
    <h3>❌ Task Failed</h3>
    <table>
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Task</b></td><td>{task_id}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{execution_date}</td></tr>
        <tr><td><b>Error</b></td><td>{exception}</td></tr>
    </table>
    <p><a href="{log_url}">ดู Log</a></p>
    """
    send_email(to=alert_email, subject=subject, html_content=body)


def alert_on_retry(context):
    """ส่ง email เมื่อ task retry"""
    alert_email = _get_alert_email()
    if not alert_email:
        return

    task_id = context["task_instance"].task_id
    dag_id = context["task_instance"].dag_id
    try_number = context["task_instance"].try_number
    log_url = context["task_instance"].log_url

    subject = f"⚠️ [Airflow] Task Retrying: {dag_id} > {task_id}"
    body = f"""
    <h3>⚠️ Task Retrying</h3>
    <table>
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Task</b></td><td>{task_id}</td></tr>
        <tr><td><b>Attempt</b></td><td>{try_number}</td></tr>
    </table>
    <p><a href="{log_url}">ดู Log</a></p>
    """
    send_email(to=alert_email, subject=subject, html_content=body)


def send_success_summary(**context):
    """ส่ง summary email เมื่อ pipeline success"""
    alert_email = _get_alert_email()
    if not alert_email:
        return

    dag_id = context["dag"].dag_id
    execution_date = context["logical_date"]
    ti = context["task_instance"]
    start = ti.start_date
    end = ti.end_date
    duration_str = f"{(end - start).seconds}s" if start and end else "N/A"

    subject = f"✅ [Airflow] Pipeline Success: {dag_id}"
    body = f"""
    <h3>✅ ETL Pipeline Completed</h3>
    <table>
        <tr><td><b>DAG</b></td><td>{dag_id}</td></tr>
        <tr><td><b>Dataset</b></td><td>{DATASET_NAME}</td></tr>
        <tr><td><b>Execution Date</b></td><td>{execution_date}</td></tr>
        <tr><td><b>Duration</b></td><td>{duration_str}</td></tr>
    </table>
    <p>ข้อมูลใน Hive พร้อมใช้งานแล้ว</p>
    """
    send_email(to=alert_email, subject=subject, html_content=body)


DATASET_NAME = "finance_itsc"  # 1 DAG = 1 dataset


# ============================================================
# Default Args
# ============================================================

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert_on_failure,
    "on_retry_callback": alert_on_retry,
    "email_on_failure": False,
    "email_on_retry": False,
}

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id="finance_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 0 1 1 *",  # ปีละครั้ง — 1 ม.ค. เที่ยงคืน
    catchup=False,
    tags=["finance", "etl"],
) as dag:

    wait_for_file = BashOperator(
        task_id="wait_for_raw_file",
        bash_command=f"docker exec hive-server hdfs dfs -test -e /datalake/raw/{DATASET_NAME}",
    )

    run_pipeline = BashOperator(
        task_id="run_spark_pipeline",
        bash_command=f"docker exec spark-master spark-submit /jobs/engine/run_pipeline.py {DATASET_NAME}",
    )

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=send_success_summary,
    )

    wait_for_file >> run_pipeline >> notify_success