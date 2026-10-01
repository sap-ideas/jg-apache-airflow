"""
DAG: ETL iSeller Pusat - Backfill Fulfillment & DWH Summary
Detects unfulfilled orders, re-pulls from iSeller API, updates Data Lake,
then re-dumps all 8 DWH summary tables in strict order.
Sends email notifications at each stage.
"""
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import sys
import os
import importlib.util

_BASE = os.path.dirname(os.path.abspath(__file__))

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(_BASE)
load_pipeline_dir(os.path.join(_BASE, 'pipelines'))

from etl_orchestrator import run_all


def load_config(config_path):
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = load_config("/opt/airflow/config/config_db_datawarehouse.py")

local_tz = pendulum.timezone("Asia/Jakarta")


def _target_end_date_yesterday_jakarta(**context):
    """
    Kalender kemarin di Asia/Jakarta relatif ke akhir interval Airflow.
    Untuk scheduled run pagi, data_interval_end adalah hari run, jadi minus 1 = H-1.
    """
    anchor = context.get("data_interval_end") or context.get("logical_date")
    if anchor is None:
        raise ValueError("context missing data_interval_end and logical_date")
    jakarta = pendulum.instance(anchor).in_timezone(local_tz)
    return jakarta.subtract(days=1).to_date_string()


RUN_MODES = ["full", "datalake_only", "dwh_only"]


def run_pipeline(**context):
    end_date = _target_end_date_yesterday_jakarta(**context)
    start_date = pendulum.parse(end_date).subtract(days=8).to_date_string()
    run_mode = (context.get("params") or {}).get("run_mode", "full")
    if run_mode not in RUN_MODES:
        raise ValueError(f"run_mode must be one of {RUN_MODES}, got {run_mode!r}")
    print(f"  RUN MODE: {run_mode}")
    run_all(
        start_date,
        end_date,
        CONFIG_DW,
        skip_dl_check=(run_mode == "dwh_only"),
        skip_dwh=(run_mode == "datalake_only"),
        airflow_context=context,
    )

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="etl_iseller_pusat_dwh",
    default_args=default_args,
    description="ETL iSeller Pusat: Backfill fulfillment, then re-dump 8 DWH summary tables",
    schedule_interval="0 9 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "iseller", "pusat", "dwh", "production"],
    max_active_runs=1,
    params={
        "run_mode": Param(
            "full",
            type="string",
            enum=RUN_MODES,
            title="Run mode",
            description=(
                "full = backfill Data Lake lalu redump DWH (default, dipakai run terjadwal). "
                "datalake_only = backfill Data Lake saja, DWH tidak disentuh. "
                "dwh_only = skip pengecekan Data Lake, langsung redump DWH."
            ),
        ),
    },
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
