# Copilot Instructions

## Project Overview

This is a **Finance ITSC Data Lake + ETL + Analytics Platform** for Chiang Mai University (ITSC department). It ingests financial Excel/CSV files, validates and transforms them through a 3-zone HDFS data lake (raw → staging → curated), and serves an interactive Thai-language Streamlit dashboard powered by GPT for NLP query translation.

**Core stack:** Hadoop HDFS, Hive 2.3.2 (PostgreSQL metastore), Apache Spark 2.4.5 (PySpark), Airflow 2.9.1, Streamlit, Nginx (HTTPS proxy), Docker Compose (13 services).

---

## Commands

### Lint
```bash
ruff check . --output-format=github --exclude backup_file
```

### Tests (local — mocked, Python 3.12)
```bash
pip install -r requirements.txt

# Full suite (excludes Spark tests)
pytest tests/ -v --ignore=tests/test_pipeline_spark.py --cov=. --cov-report=term-missing -p no:warnings

# Single test file
pytest tests/test_etl.py -v

# Single test function
pytest tests/test_etl.py::TestExtractYear::test_standard_path -v
```

### Tests (Spark — Docker-based, Python 3.7)
```bash
# Requires Docker; outputs reports/junit.xml + reports/report.html
./run_tests.sh                                 # All tests
./run_tests.sh tests/test_pipeline_spark.py    # Single file
./run_tests.sh tests/test_etl.py -v            # With pytest flags
```

### Docker
```bash
docker build -f Dockerfile.spark -t spark-test:ci .
docker build -f Dockerfile.streamlit -t dashboard:ci .
docker compose config --quiet   # Validate compose file
docker compose up -d
```

---

## Architecture

### Data Flow
```
Excel/CSV → /datalake/raw/finance_itsc/year=YYYY/
          → /datalake/staging/finance_itsc_wide/   (wide table, deduplicated)
          → /datalake/curated/finance_itsc_long/   (long table, partitioned by year)
          → /datalake/versions/finance_itsc/vN/    (snapshots)
          → /datalake/trash/                        (soft-deleted data)
```

### Docker Services
| Service | Role |
|---------|------|
| `namenode` / `datanode` | HDFS (WebUI: :9870) |
| `hive-metastore-postgresql` / `hive-metastore` / `hive-server` | Hive 2.3.2 (Thrift: :10000) |
| `spark-master` / `spark-worker` | Spark 2.4.5 (UI: :8080, RPC: :7077) |
| `spark-thrift` | Spark Thrift Server (:10001) |
| `airflow-webserver` / `airflow-scheduler` / `airflow-postgres` | Airflow 2.9.1 (UI: :8088) |
| `streamlit-dashboard` | Streamlit app (:8501) |
| `nginx` | HTTPS reverse proxy (:443), routes `/spark/` and `/webhdfs/` |

### Directory Layout
- **`jobs/`** — Generic ETL engine (dataset-driven via YAML + Hive metadata)
  - `jobs/datasets/` — YAML configs per dataset (e.g., `finance.yaml`)
  - `jobs/engine/pipeline.py` — 11-step ETL pipeline (run via `engine/run_pipeline.py`)
  - `jobs/quality_rules/` — Abstract base + per-dataset quality rule subclasses
  - `jobs/utils/` — `hdfs.py`, `retry.py`, `soft_delete.py`, `versioning.py`, `alerts.py`
  - `jobs/manage.py` — CLI for version list/diff/restore
- **`airflow/dags/hadoop_dag.py`** — Single DAG; BashOperator (spark-submit) + PythonOperator (email alerts)
- **`dashboard/`** — Streamlit app with `pages/` (upload, hdfs_browser, monitoring) and `services/` (hive_gpt, hive_metadata, monitoring, schema_service)
- **`tests/`** — 14 test modules; mocked fixtures in `conftest.py`
- **`certs/`** — TLS certs for Nginx (generate via `generate_cert.sh`)

---

## Key Conventions

### Dataset-Driven Pipeline
The ETL engine (`jobs/engine/pipeline.py`) is **generic** — it reads dataset identity from YAML configs in `jobs/datasets/`. To add a new dataset, create a new YAML (using `finance.yaml` as the template) and subclass `QualityRulesBase` in `jobs/quality_rules/`.

The `DatasetConfig` dataclass (loaded from YAML) drives all 11 pipeline steps: paths, Hive table names, critical columns, partition key, id columns, and schema with `ColumnDef` metadata (Thai name, type, `is_amount`, `is_date`, `reserved_keyword`, etc.).

### Atomic Write Pattern
Data writes use a **temp-then-rename swap** to prevent partial reads:
```
write to temp location → rename (atomic swap) → register Hive partition → delete old
```
This is implemented in `jobs/utils/retry.py::atomic_write_table`.

### Idempotency
Each pipeline run computes an MD5 checksum of the input file and writes a `.done` marker to HDFS. Re-running with the same file is a no-op.

### Soft Delete & Versioning
`soft_delete()` moves old partitions to `/datalake/trash/<dataset>/<timestamp>/` (non-destructive). `create_version()` snapshots the curated path with a schema hash. Use `jobs/manage.py` to list/diff/restore versions.

### Test Structure
- **Mocked tests** (no Spark needed): All tests except `test_pipeline_spark.py`. Run locally with Python 3.12.
- **Spark tests**: `test_pipeline_spark.py` requires the `spark-master` Docker container. Use `run_tests.sh`.
- **`conftest.py` fixtures**: `make_hdfs_fs()` provides a stateful mock HDFS (tracks `exists`, `rename`, `delete`, `mkdirs`); `make_mock_sc()` provides a mock SparkContext with `_jvm`.
- Use `SimpleNamespace` for `argparse.Namespace` in test args.

### Airflow DAGs
- Single DAG per file; schedule via `schedule_interval` cron.
- Use `BashOperator` for spark-submit tasks, `PythonOperator` for alert callbacks.
- Alert email address stored in Airflow Variable `alert_email`; fetch with `_get_alert_email()`.
- Always set `on_failure_callback` and `on_retry_callback`.

### Dashboard / Streamlit
- Business logic lives in `dashboard/services/`, not in page files.
- Chat state: `st.session_state.messages`, `current_chat_id`, `pending_question`.
- Entry via `dashboard/app.py`; `require_auth()` must be called first.
- Thai → HiveQL translation via `HiveGPT` service (wraps OpenAI + PyHive).

### Configuration
- Runtime secrets go in a `.env` file (see `dashboard/config.py.example`): `OPENAI_API_KEY`, `HIVE_HOST`, `SMTP_*`, `COOKIE_SECRET`, `WEBHDFS_URL`.
- Hadoop/Hive config is split between `hadoop-hive.env` (Docker env vars) and `hive-site.xml`.
- SSL certs: run `bash generate_cert.sh` or use your own PEM files in `certs/`.

### CI Pipeline (`.github/workflows/ci.yml`)
Jobs run in order: **lint** → **test (mocked)** → **test-spark (Docker)** → **docker-build (spark)** → **docker-build (streamlit)** → **compose-validate** → **airflow-dag-validate**.
