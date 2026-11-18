"""
DAG: ETL iSeller Mitra - DWH summary 6/6: daily_outlet_transaction_summary_companywide_new
(hanya baris datasource='ISELLER' AND ownership_model='MITRA')

Tanpa jadwal, DAG terakhir rantai harian: etl_iseller_mitra_dwh_summary_sales_type -> DAG ini (H-8 s/d H-1).
Sumbernya tabel DWH daily_transaction_summary_iseller_pusat, jadi DAG ini TIDAK dipicu kalau
etl_iseller_mitra_dwh_summary_transactions gagal di rantai yang sama. Alur lengkap: plugins/common/mitra_chain.py.
Trigger manual: isi start_date / end_date (pastikan DAG DWH transactions untuk window itu sudah sukses).
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipelines'))

from etl_orchestrator import run_all
from common.mitra_chain import DWH_COMPANYWIDE, LOCAL_TZ, dwh_params, window_from_params


def load_config(config_path):
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = load_config("/opt/airflow/config/config_db_datawarehouse.py")


def run_pipeline(**context):
    start_date, end_date = window_from_params(context["params"])
    ti = context["ti"]
    return run_all(start_date, end_date, CONFIG_DW, dry_run=context["params"]["dry_run"],
                   notify_failure=ti.try_number > ti.max_tries)  # email gagal hanya di percobaan terakhir


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG terakhir di rantai: tidak ada DAG berikutnya, jadi tanpa trigger_next
params = dwh_params()
params.pop("trigger_next")

with DAG(
    dag_id=DWH_COMPANYWIDE,
    default_args=default_args,
    description="ETL iSeller Mitra DWH 6/6: companywide (ISELLER MITRA) dari DWH transactions (dipicu rantai harian, tanpa jadwal)",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    tags=["etl", "iseller", "mitra", "dwh", "production"],
    max_active_runs=1,
    params=params,
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        execution_timeout=timedelta(hours=2),
    )
