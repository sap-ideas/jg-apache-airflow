"""
DAG: ETL iSeller Mitra - Backfill Fulfillment (Data Lake only)
Detects unfulfilled orders, re-pulls from iSeller Mitra API, updates Data Lake,
then sends email + Teams report. DWH summary tables are NOT re-dumped by this DAG.

Tanpa jadwal: dipicu etl_iseller_mitra_backfilling (rantai harian, plugins/common/mitra_chain.py), window H-8 s/d H-1.
Selesai (sukses ATAU gagal) -> memicu etl_iseller_mitra_cleaning (datalake_only): duplikat di Data Lake lebih
berbahaya daripada sedikit order unfulfilled. Window cleaning H-8 s/d H-1 kalau backfilling / fulfillment mengubah
order, selain itu H-1. Fulfillment gagal tidak mengubah Data Lake (tulis dalam satu transaksi, di-rollback).
"""
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import os
import importlib.util

_BASE = os.path.dirname(os.path.abspath(__file__))

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(_BASE)
load_pipeline_dir(os.path.join(_BASE, 'pipelines'))

from etl_orchestrator import run_all
from common.mitra_chain import (
    CLEANING, FULFILLMENT, LOOKBACK_DAYS, add_chain_tasks, chain_h1, days_before, trigger_next_param,
    window_from_params,
)


def load_config(config_path):
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = load_config("/opt/airflow/config/config_db_datawarehouse.py")

local_tz = pendulum.timezone("Asia/Jakarta")


# Isi awal form trigger = window default saat file DAG di-parse. Run dari rantai memakai window dari conf.
DEFAULT_END_DATE = pendulum.now(local_tz).subtract(days=1).to_date_string()
DEFAULT_START_DATE = days_before(DEFAULT_END_DATE, LOOKBACK_DAYS)


def run_pipeline(**context):
    # Tanpa jadwal: run manual atau dipicu backfilling (window H-8 s/d H-1 ada di conf)
    start_date, end_date = window_from_params(context.get("params") or {})
    print(f"  DATE WINDOW: {start_date} s/d {end_date} ({context['dag_run'].run_type})")

    result = run_all(
        start_date,
        end_date,
        CONFIG_DW,
        airflow_context=context,
    )
    # dibaca next_conf: ada order yang jadi fulfilled -> cleaning (dan DWH) ikut H-8 s/d H-1
    return {"changed": bool((result.get("backfill") or {}).get("total_fulfilled"))}


def _cleaning_conf(result, context):
    """Fulfillment selesai (sukses atau gagal) -> cleaning datalake_only.
    Window H-8 s/d H-1 kalau backfilling / fulfillment mengubah order, selain itu H-1 saja."""
    h1 = chain_h1(context)
    changed = bool((context["dag_run"].conf or {}).get("dl_changed")) or bool(result and result["changed"])
    return {"start_date": days_before(h1, LOOKBACK_DAYS) if changed else h1, "end_date": h1,
            "run_mode": "datalake_only"}


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=FULFILLMENT,
    default_args=default_args,
    description="ETL iSeller Mitra: Backfill fulfillment ke Data Lake (tanpa redump DWH), dipicu backfilling, lalu memicu cleaning",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "iseller", "mitra", "fulfillment", "datalake", "production"],
    max_active_runs=1,
    params={
        "start_date": Param(
            DEFAULT_START_DATE,
            type="string",
            format="date",
            title="Start date",
            description="Awal window. Run dari rantai harian selalu H-8. Satu hari saja: isi sama dengan end date.",
        ),
        "end_date": Param(
            DEFAULT_END_DATE,
            type="string",
            format="date",
            title="End date",
            description="Akhir window, ikut diproses. Run dari rantai harian selalu H-1.",
        ),
        "trigger_next": trigger_next_param(),
    },
) as dag:

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        execution_timeout=timedelta(hours=3),
    )

    add_chain_tasks(run, CLEANING, _cleaning_conf)
