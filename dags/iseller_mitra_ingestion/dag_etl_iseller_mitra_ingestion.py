"""
DAG: ETL iSeller Mitra - Ingestion harian (Data Lake only)
Menarik order H-1 dari iSeller API (header + details) ke Data Lake (transactions_iseller_mitra dan
transactions_items_iseller_mitra). Order yang sudah ada di Data Lake dilewati, jadi retry / trigger ulang aman
dan tidak menggandakan data. Tabel summary DWH tidak disentuh.

Jalan 06:00 WIB, sebelum etl_iseller_mitra_backfilling (06:10), etl_iseller_mitra_fulfillment (07:50) dan
etl_iseller_mitra_cleaning (07:51). Ingestion menulis H-1, backfilling menulis H-8 s/d H-7, jadi tidak beririsan.
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
    Untuk scheduled run pagi, data_interval_end adalah hari run, jadi minus 1 = H-1.
    """
    anchor = context.get("data_interval_end") or context.get("logical_date")
    if anchor is None:
        raise ValueError("context missing data_interval_end and logical_date")
    jakarta = pendulum.instance(anchor).in_timezone(local_tz)
    return jakarta.subtract(days=1).to_date_string()


# Isi awal form trigger = H-1 saat file DAG di-parse. Run terjadwal tidak memakainya (lihat run_pipeline).
DEFAULT_DATE = pendulum.now(local_tz).subtract(days=1).to_date_string()


def run_pipeline(**context):
    params = context.get("params") or {}
    dry_run = bool(params.get("dry_run", False))

    if context["dag_run"].run_type == "manual":
        start_date, end_date = params["start_date"], params["end_date"]
        if start_date > end_date:
            raise ValueError(f"start_date ({start_date}) tidak boleh setelah end_date ({end_date})")
        print(f"  DATE WINDOW: {start_date} s/d {end_date} (manual)")
    else:
        start_date = end_date = _target_date_yesterday_jakarta(**context)
        print(f"  DATE WINDOW: {start_date} (H-1 dari jadwal)")
        dry_run = False  # run terjadwal selalu menulis

    run_all(
        start_date,
        end_date,
        CONFIG_DW,
        dry_run=dry_run,
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
    dag_id="etl_iseller_mitra_ingestion",
    default_args=default_args,
    description="ETL iSeller Mitra: ingestion harian order H-1 dari iSeller API ke Data Lake (tanpa redump DWH)",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "iseller", "mitra", "ingestion", "datalake", "production"],
    max_active_runs=1,
    params={
        "start_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="Start date",
            description="Hanya dipakai saat trigger manual. Run terjadwal selalu H-1. Satu hari saja: isi sama dengan end date.",
        ),
        "end_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="End date",
            description="Hanya dipakai saat trigger manual. Tanggal ini ikut ditarik.",
        ),
        "dry_run": Param(
            False,
            type="boolean",
            title="Dry run",
            description="Hanya trigger manual. True = tarik + proses lengkap tapi TIDAK menulis Data Lake dan tidak kirim email/Teams.",
        ),
    },
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        execution_timeout=timedelta(hours=2),
    )
