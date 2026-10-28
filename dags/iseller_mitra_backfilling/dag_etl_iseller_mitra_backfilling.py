"""
DAG: ETL iSeller Mitra - Backfilling (Data Lake only)
Menarik ulang order H-8 s/d H-7 dari iSeller API dan memasukkan order yang TIDAK ADA SAMA SEKALI di Data Lake
(ingest harian tidak menariknya). Berbeda dengan etl_iseller_mitra_fulfillment yang hanya memperbaiki order
fulfillment_status='none'. Tabel summary DWH tidak disentuh (redump manual lewat etl_iseller_mitra_cleaning dwh_only).

Jalan 06:10 WIB (setelah ingest harian ~06:08, sebelum fulfillment 07:50 dan cleaning 07:51). Idempoten: order yang sudah ada tidak dimasukkan lagi, jadi retry / trigger ulang aman.
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


def _run_date_jakarta(**context):
    """Tanggal run di Asia/Jakarta (dari data_interval_end, fallback logical_date)."""
    anchor = context.get("data_interval_end") or context.get("logical_date")
    if anchor is None:
        raise ValueError("context missing data_interval_end and logical_date")
    return pendulum.instance(anchor).in_timezone(local_tz)


# Isi awal form trigger = window default saat file DAG di-parse. Run terjadwal tidak memakainya (lihat run_pipeline).
_TODAY = pendulum.now(local_tz)
DEFAULT_START_DATE = _TODAY.subtract(days=8).to_date_string()
DEFAULT_END_DATE = _TODAY.subtract(days=7).to_date_string()


def run_pipeline(**context):
    params = context.get("params") or {}
    dry_run = bool(params.get("dry_run", False))

    if context["dag_run"].run_type == "manual":
        start_date, end_date = params["start_date"], params["end_date"]
        if start_date > end_date:
            raise ValueError(f"start_date ({start_date}) tidak boleh setelah end_date ({end_date})")
        print(f"  DATE WINDOW: {start_date} s/d {end_date} (manual)")
    else:
        today = _run_date_jakarta(**context)
        start_date = today.subtract(days=8).to_date_string()
        end_date = today.subtract(days=7).to_date_string()
        print(f"  DATE WINDOW: {start_date} s/d {end_date} (H-8 s/d H-7 dari jadwal)")
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
    dag_id="etl_iseller_mitra_backfilling",
    default_args=default_args,
    description="ETL iSeller Mitra: backfilling order yang tidak ada di Data Lake (H-8 s/d H-7), Data Lake only",
    schedule_interval="10 6 * * *",
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
            description="Hanya dipakai saat trigger manual. Run terjadwal selalu H-8. Satu hari saja: isi sama dengan end date.",
        ),
        "end_date": Param(
            DEFAULT_END_DATE,
            type="string",
            format="date",
            title="End date",
            description="Hanya dipakai saat trigger manual. Tanggal ini ikut ditarik. Run terjadwal selalu H-7.",
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
        execution_timeout=timedelta(hours=3),
    )
