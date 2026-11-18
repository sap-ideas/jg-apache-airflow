"""
DAG: ETL iSeller Mitra - Cleaning Duplicates & DWH Redump
Checks for duplicated data in Data Lake, cleans if needed,
then (run_mode full) re-dumps the DWH summary tables from cleaned Data Lake.
Sends email notifications at each stage.

Tanpa jadwal: dipicu etl_iseller_mitra_fulfillment (rantai harian, plugins/common/mitra_chain.py) dengan
run_mode datalake_only, jadi DAG ini hanya membersihkan Data Lake. Setelah selesai (sukses ATAU gagal) memicu
6 DAG DWH (etl_iseller_mitra_dwh_summary_*) H-8 s/d H-1: DWH yang mungkin dobel lebih mudah dicek daripada
DWH yang kosong. Karena dipicu setelah fulfillment selesai, DAG ini tidak pernah jalan bersamaan dengan fulfillment.
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

from redump_etl_functions import run_all
from common.mitra_chain import (
    CLEANING, DWH_ITEMS, LOOKBACK_DAYS, add_chain_tasks, days_before, trigger_next_param, window_from_params,
)


def load_config(config_path):
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

CONFIG_DW = load_config("/opt/airflow/config/config_db_datawarehouse.py")

local_tz = pendulum.timezone("Asia/Jakarta")

RUN_MODES = ["full", "datalake_only", "dwh_only"]

# Isi awal form trigger = H-1 saat file DAG di-parse. Run dari rantai memakai window dari conf.
DEFAULT_DATE = pendulum.now(local_tz).subtract(days=1).to_date_string()


def run_pipeline(**context):
    params = context.get("params") or {}
    run_mode = params.get("run_mode", "full")
    if run_mode not in RUN_MODES:
        raise ValueError(f"run_mode must be one of {RUN_MODES}, got {run_mode!r}")

    # Tanpa jadwal: run manual atau dipicu fulfillment (window + run_mode ada di conf)
    start_date, end_date = window_from_params(params)
    print(f"  DATE WINDOW: {start_date} s/d {end_date} ({context['dag_run'].run_type})")

    print(f"  RUN MODE: {run_mode}")
    run_all(
        start_date,
        end_date,
        CONFIG_DW,
        skip_dl_check=(run_mode == "dwh_only"),
        skip_dwh=(run_mode == "datalake_only"),
        airflow_context=context,
    )
    return {"run_mode": run_mode}  # XCom kosong = gagal (dibaca next_conf / status_akhir)


def _dwh_conf(result, context):
    """Cleaning selesai (sukses atau gagal) -> DWH. Rantai harian: selalu H-8 s/d H-1.
    Trigger manual dengan trigger_next: window cleaning yang sama."""
    conf = context["dag_run"].conf or {}
    if conf.get("chain_h1"):
        h1 = conf["chain_h1"]
        return {"start_date": days_before(h1, LOOKBACK_DAYS), "end_date": h1}
    start_date, end_date = window_from_params(context["params"])
    return {"start_date": start_date, "end_date": end_date}


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=CLEANING,
    default_args=default_args,
    description="ETL iSeller Mitra: Check duplicates, clean DL (dipicu fulfillment), lalu memicu 6 DAG DWH summary",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "iseller", "mitra", "dwh", "production"],
    max_active_runs=1,
    params={
        "run_mode": Param(
            "full",
            type="string",
            enum=RUN_MODES,
            title="Run mode",
            description=(
                "full = cek duplikat + bersihkan Data Lake + redump DWH di dalam DAG ini (default trigger manual). "
                "datalake_only = cek + bersihkan Data Lake saja, DWH tidak disentuh (dipakai rantai harian; "
                "DWH diisi 6 DAG etl_iseller_mitra_dwh_summary_*). "
                "dwh_only = skip cek/cleaning Data Lake, langsung redump tabel DWH dari DL apa adanya."
            ),
        ),
        "start_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="Start date",
            description="Awal window. Rantai harian: H-1, atau H-8 kalau backfilling / fulfillment mengubah order. "
                        "Satu hari saja: isi sama dengan end date.",
        ),
        "end_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="End date",
            description="Akhir window, ikut diproses. Rantai harian selalu H-1.",
        ),
        "trigger_next": trigger_next_param(),
    },
) as dag:

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )

    add_chain_tasks(run, DWH_ITEMS, _dwh_conf)
