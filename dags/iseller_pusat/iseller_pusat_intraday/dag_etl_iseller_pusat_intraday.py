"""
DAG: ETL iSeller Pusat - Intraday (data hari H, Data Lake)
Menarik order HARI INI (00:00 WIB s/d sekarang) dari iSeller API tiap jam 06:00-21:00 WIB dan memperbarui tabel intraday
`transactions_iseller_pusat_intraday` + `transactions_items_iseller_pusat_intraday` (hanya berisi data hari ini; data kemarin
dihapus otomatis di run pertama hari berikutnya). Tabel Data Lake utama dan DWH tidak disentuh.

Upsert per order: order baru di-insert, order berubah (status / fulfillment / transaksi baru) diganti versi terbaru,
order yang versi barunya menyusut ditahan, order yang tidak ketarik tidak disentuh. Idempoten: retry / trigger ulang aman.
Email ringkasan tiap run (sukses dan gagal).
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


def run_pipeline(**context):
    params = context.get("params") or {}
    # Run terjadwal selalu menulis; dry_run hanya berlaku untuk trigger manual.
    dry_run = bool(params.get("dry_run", False)) if context["dag_run"].run_type == "manual" else False
    run_all(CONFIG_DW, dry_run=dry_run)


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="etl_iseller_pusat_intraday",
    default_args=default_args,
    description="ETL iSeller Pusat: data hari H tiap jam 06:00-21:00 ke tabel intraday Data Lake (upsert per order, H-1 dihapus)",
    schedule="0 6-21 * * *",
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "iseller", "pusat", "intraday", "datalake", "production"],
    max_active_runs=1,
    params={
        "dry_run": Param(
            False,
            type="boolean",
            title="Dry run",
            description="Hanya trigger manual. True = tarik + proses lengkap tapi TIDAK menulis tabel intraday dan tidak kirim email. Data yang ditarik selalu hari ini.",
        ),
    },
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        execution_timeout=timedelta(minutes=30),
    )
