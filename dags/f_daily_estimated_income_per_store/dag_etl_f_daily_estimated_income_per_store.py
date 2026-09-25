"""
DAG: Daily Estimated Income Per Store (for Landlord)

Processes yesterday's transactions_iseller_pusat data into
`daily_est_income_per_store_for_landlord` on the Data Warehouse.
Runs once daily at 10:00 Asia/Jakarta (after etl_iseller_pusat_dwh @ 09:00
has refreshed the source table).
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
import sys
import pendulum

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipelines'))

from etl_orchestrator import run_all


def load_config(config_path):
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = load_config("/opt/airflow/config/config_db_datawarehouse.py")

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


def run_task(**context):
    target_date = _target_date_yesterday_jakarta(**context)
    run_all(CONFIG_DW, target_date_str=target_date)


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="f_daily_estimated_income_per_store",
    default_args=default_args,
    description="ETL Daily Estimated Income Per Store: build daily_est_income_per_store_for_landlord from transactions_iseller_pusat",
    schedule_interval="0 10 * * *",  # 10:00 Asia/Jakarta
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "landlord", "income", "iseller", "dwh", "production"],
    max_active_runs=1,
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_task,
    )
