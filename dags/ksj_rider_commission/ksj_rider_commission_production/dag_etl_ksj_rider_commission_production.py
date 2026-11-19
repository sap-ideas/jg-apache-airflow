"""
DAG: KSJ Rider Commission (PRODUCTION)

Hitung komisi rider (harian per produk, harian, mingguan) untuk minggu berjalan dan tulis
ke database KSJ Link PRODUCTION (dibaca rider app): rider_weekly_commission_summaries,
rider_daily_commission_summaries, rider_daily_commission_summary_products.
Jalan 02:00 setiap hari + 18:00 setiap Senin (Asia/Jakarta), memproses H-1. Run manual boleh override lewat
conf {"target_date": "YYYY-MM-DD"}.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import MultipleCronTriggerTimetable
from datetime import datetime, timedelta
import pendulum
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


CONFIG_KSJ = load_config("/opt/airflow/config/config_db_ksj_link.py")

local_tz = pendulum.timezone("Asia/Jakarta")


def _target_date_yesterday_jakarta(**context):
    """Kemarin di Asia/Jakarta relatif ke data_interval_end (fallback logical_date)."""
    dag_run = context.get("dag_run")
    anchor = (context.get("data_interval_end") or context.get("logical_date")
              or (getattr(dag_run, "run_after", None) if dag_run is not None else None))
    if anchor is None:
        raise ValueError("context missing data_interval_end, logical_date and run_after")
    return pendulum.instance(anchor).in_timezone(local_tz).subtract(days=1).to_date_string()


def run_pipeline(**context):
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run is not None and dag_run.run_type == "manual" else {}
    target_date = conf.get("target_date") or _target_date_yesterday_jakarta(**context)
    run_all(CONFIG_KSJ, target_date_str=target_date)


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="ksj_rider_commission_production",
    default_args=default_args,
    description="KSJ rider commission (daily products, daily, weekly) -> KSJ Link PRODUCTION",
    # 02:00 setiap hari + 18:00 setiap Senin (menangkap transaksi Minggu yang telat sync)
    schedule=MultipleCronTriggerTimetable("0 2 * * *", "0 18 * * 1", timezone="Asia/Jakarta"),
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "riders", "commission", "ksj-link", "production"],
    max_active_runs=1,
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
