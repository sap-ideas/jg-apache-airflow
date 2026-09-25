"""
DAG: API Landlord Revenue Sharing — Jilid 01065

Sends yesterday's aggregated revenue (by payment type) for outlet 01065
to the Lippo Mall Puri Revenue API, then logs the result to the Data Lake
and emails the data team.

Schedule : 10:00 Asia/Jakarta, daily
Data scope: yesterday (Asia/Jakarta)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
import sys
import pendulum


# ---------------------------------------------------------------------------
# Path setup — add the pipelines/ sub-folder to sys.path
# ---------------------------------------------------------------------------
_DAG_DIR      = os.path.dirname(os.path.abspath(__file__))
_PIPELINES_DIR = os.path.join(_DAG_DIR, 'pipelines')

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(_PIPELINES_DIR)

from etl_orchestrator import run_all


# ---------------------------------------------------------------------------
# Config loader (mirrors the pattern used across all project DAGs)
# ---------------------------------------------------------------------------

def _load_config(config_path):
    spec   = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = _load_config("/opt/airflow/config/config_db_datawarehouse.py")


# ---------------------------------------------------------------------------
# Callable for PythonOperator
# ---------------------------------------------------------------------------

local_tz = pendulum.timezone("Asia/Jakarta")


def _target_date_yesterday_jakarta(**context):
    """
    Kalender kemarin di Asia/Jakarta relatif ke akhir interval Airflow.
    data_interval_end dipakai hanya sebagai anchor tanggal run, bukan window jam.
    """
    anchor = context.get("data_interval_end") or context.get("logical_date")
    if anchor is None:
        raise ValueError("context missing data_interval_end and logical_date")
    jakarta = pendulum.instance(anchor).in_timezone(local_tz)
    return jakarta.subtract(days=1).to_date_string()


def run_pipeline(**context):
    target_date = _target_date_yesterday_jakarta(**context)
    run_all(CONFIG_DW, target_date_str=target_date)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    'owner':            'data-team',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
}

with DAG(
    dag_id="api_landlord_revenue_sharing_01065",
    default_args=default_args,
    description=(
        "Daily ETL: send yesterday's revenue (jilid 01065) to the Lippo Mall Puri "
        "Revenue API and log results to the Data Lake."
    ),
    schedule_interval="0 10 * * *",  # 10:00 Asia/Jakarta
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["api", "landlord", "revenue-sharing", "lippomall", "production"],
    max_active_runs=1,
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
