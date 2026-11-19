"""
DAG: Landlord Dashboard 01165 (Summarecon Emerald Karawang)

Generate ulang dashboard HTML (template Jiwa Light) dari `daily_est_income_per_store_for_landlord`,
lalu UPDATE kolom `html_content` + `updated_at` di data_report.custom_menus (id 80). HTML tidak disimpan
sebagai file.

Schedule : Asset-triggered — jalan tepat setelah `f_daily_estimated_income_per_store` sukses. Tanpa jam tetap.
Data scope: 60 hari terakhir (paling awal 2026-11-01), dihitung dari tanggal run.
"""

from airflow import DAG
from airflow.sdk import Asset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
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
CONFIG_JIWAPLUS = load_config("/opt/airflow/config/config_db_jiwaplus.py")

local_tz = pendulum.timezone("Asia/Jakarta")

# Harus sama dengan outlet di DAG f_daily_estimated_income_per_store
DATASET_DAILY_EST_INCOME = Asset("f_daily_estimated_income_per_store://daily_est_income_per_store_for_landlord")


def run_task(**context):
    run_all(CONFIG_DW, CONFIG_JIWAPLUS)


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # aman: UPDATE idempotent
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="landlord_dashboard_01165",
    default_args=default_args,
    description="Regenerate dashboard HTML 01165 dan update data_report.custom_menus.html_content (id 80) setelah f_daily_estimated_income_per_store sukses",
    schedule=[DATASET_DAILY_EST_INCOME],
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["landlord", "dashboard", "html", "custom-menus", "production"],
    max_active_runs=1,
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_task,
    )
