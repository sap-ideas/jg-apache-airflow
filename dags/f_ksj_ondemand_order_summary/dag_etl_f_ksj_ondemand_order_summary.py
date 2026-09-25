"""DAG: ETL KSJ On-Demand Order Distance Summary

Builds `ksj_on_demand_order_summary` on the Data Warehouse from the KSJ
on-demand dispatcher, the KSJ transaction DB and JiwaPlus: straight-line vs
actual road-route distance (OSRM) per order, blast metrics, and the paid ->
accepted -> OTW -> delivered timing breakdown.

Runs 4x/day (Asia/Jakarta): 09:00, 12:00, 15:00, 17:00, each time reprocessing
TODAY and overwriting the day's rows, so every run refreshes the picture as more
of the day's orders complete.
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
CONFIG_JIWAPLUS = load_config("/opt/airflow/config/config_db_jiwaplus.py")


def run_pipeline(**context):
    run_all(CONFIG_KSJ, CONFIG_JIWAPLUS, CONFIG_DW)


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
    dag_id="f_ksj_ondemand_order_summary",
    default_args=default_args,
    description="ETL KSJ On-Demand Order Summary: straight-line vs route distance & delivery timing per order",
    schedule_interval="0 9,12,15,17 * * *",  # 4x/day @ 09:00, 12:00, 15:00, 17:00 Asia/Jakarta
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "ksj-link", "on-demand", "order-distance", "dwh", "production"],
    max_active_runs=1,
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        # This pipeline calls a third-party routing API once per unique coordinate
        # pair with a 0.3s pause between calls, so its runtime scales with order
        # volume. The cap keeps a stuck run from blocking the next batch
        # (max_active_runs=1) instead of hanging until manually cleared.
        execution_timeout=timedelta(hours=2),
    )
