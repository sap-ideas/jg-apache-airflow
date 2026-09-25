"""
DAG: ETL Rider Mangkal Summary

Aggregates KSJ Link rider data (stationed logs, transfer/checkout, transactions)
into `daily_rider_mangkal_session_summary` on the Data Warehouse.
Runs 3x/day (Asia/Jakarta): 08:00, 11:00, 15:00.
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


CONFIG_DW = load_config("/opt/airflow/config/config_db_datawarehouse.py")
CONFIG_KSJ = load_config("/opt/airflow/config/config_db_ksj_link.py")


def run_pipeline(**context):
    run_all(CONFIG_KSJ, CONFIG_DW)


local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="f_rider_mangkal_summary",
    default_args=default_args,
    description="ETL Rider Mangkal Summary: build daily_rider_mangkal_session_summary from KSJ Link sources",
    schedule_interval="0 8,11,15 * * *",  # 3x/day @ 08:00, 11:00, 15:00 Asia/Jakarta
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "riders", "mangkal", "ksj-link", "dwh", "production"],
    max_active_runs=1,
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
