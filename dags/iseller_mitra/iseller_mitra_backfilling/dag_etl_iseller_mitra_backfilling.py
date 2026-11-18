"""
DAG: ETL iSeller Mitra - Backfilling (Data Lake only)
Menarik ulang order H-8 s/d H-7 dari iSeller API dan memasukkan order yang TIDAK ADA SAMA SEKALI di Data Lake
(ingest harian tidak menariknya). Berbeda dengan etl_iseller_mitra_fulfillment yang hanya memperbaiki order
fulfillment_status='none'. Tabel summary DWH tidak disentuh (redump manual lewat etl_iseller_mitra_cleaning dwh_only).

Tanpa jadwal: dipicu etl_iseller_mitra_ingestion (rantai harian, plugins/common/mitra_chain.py), lalu memicu
etl_iseller_mitra_fulfillment apa pun hasilnya. Idempoten: order yang sudah ada tidak dimasukkan lagi, jadi retry / trigger ulang aman.
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
    BACKFILLING, FULFILLMENT, LOOKBACK_DAYS, add_chain_tasks, chain_h1, days_before, trigger_next_param,
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
_TODAY = pendulum.now(local_tz)
DEFAULT_START_DATE = _TODAY.subtract(days=8).to_date_string()
DEFAULT_END_DATE = _TODAY.subtract(days=7).to_date_string()


def run_pipeline(**context):
    params = context.get("params") or {}
    dry_run = bool(params.get("dry_run", False))

    # Tanpa jadwal: run manual atau dipicu ingestion (window H-8 s/d H-7 ada di conf)
    start_date, end_date = window_from_params(params)
    print(f"  DATE WINDOW: {start_date} s/d {end_date} ({context['dag_run'].run_type})")

    result = run_all(
        start_date,
        end_date,
        CONFIG_DW,
        dry_run=dry_run,
        airflow_context=context,
    )
    stats = result["stats"]
    # dibaca next_conf: ada order / details baru -> cleaning ikut cek H-8 s/d H-1
    return {"changed": bool(stats.get("orders_inserted") or stats.get("details_only_orders"))}


def _fulfillment_conf(result, context):
    """Backfilling selesai (sukses atau gagal) -> fulfillment H-8 s/d H-1.
    Backfilling gagal bisa sudah memasukkan sebagian order, jadi dianggap mengubah Data Lake."""
    h1 = chain_h1(context)
    return {"start_date": days_before(h1, LOOKBACK_DAYS), "end_date": h1,
            "dl_changed": result is None or result["changed"]}


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=BACKFILLING,
    default_args=default_args,
    description="ETL iSeller Mitra: backfilling order yang tidak ada di Data Lake (H-8 s/d H-7), Data Lake only, dipicu ingestion",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "iseller", "mitra", "backfilling", "datalake", "production"],
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
            description="Akhir window, ikut ditarik. Run dari rantai harian selalu H-7.",
        ),
        "dry_run": Param(
            False,
            type="boolean",
            title="Dry run",
            description="Hanya trigger manual. True = tarik + proses lengkap tapi TIDAK menulis Data Lake dan tidak kirim email/Teams.",
        ),
        "trigger_next": trigger_next_param(),
    },
) as dag:

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        execution_timeout=timedelta(hours=3),
    )

    add_chain_tasks(run, FULFILLMENT, _fulfillment_conf)
