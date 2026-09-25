"""
DAG: ETL Daily KSJ Consumer App User Session

Processes yesterday's JiwaPlus and KSJ consumer app user session data
into `daily_ksj_consumer_app_user_session` on the Data Warehouse.
Runs once daily at 06:00 Asia/Jakarta.
Sends a single email report at the end of each run.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import sys
import os
import importlib.util

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipelines'))

from etl_orchestrator import run_all


def load_config(config_path):
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW       = load_config("/opt/airflow/config/config_db_datawarehouse.py")
CONFIG_KSJ      = load_config("/opt/airflow/config/config_db_ksj_link.py")
CONFIG_JIWAPLUS = load_config("/opt/airflow/config/config_db_jiwaplus.py")


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
    run_all(CONFIG_JIWAPLUS, CONFIG_KSJ, CONFIG_DW, target_date_str=target_date)

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="f_daily_ksj_app_user_session",
    default_args=default_args,
    description="ETL Daily KSJ Consumer App User Session: build daily_ksj_consumer_app_user_session from JiwaPlus and KSJ Link",
    schedule_interval="0 6 * * *",  # 06:00 Asia/Jakarta
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "ksj", "consumer-app", "user-session", "jiwaplus", "dwh", "production"],
    max_active_runs=1,
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
