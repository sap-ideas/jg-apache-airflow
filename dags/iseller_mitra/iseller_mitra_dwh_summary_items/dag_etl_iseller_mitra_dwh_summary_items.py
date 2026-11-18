"""
DAG: ETL iSeller Mitra - DWH summary 1/6: daily_items_summary_iseller_pusat (sources='iseller_mitra')

Tanpa jadwal. Rantai harian: etl_iseller_mitra_cleaning -> DAG ini (H-8 s/d H-1)
-> etl_iseller_mitra_dwh_summary_bundlings. Alur lengkap: plugins/common/mitra_chain.py.
Trigger manual: isi start_date / end_date; centang trigger_next untuk lanjut ke tabel berikutnya.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipelines'))

from etl_orchestrator import run_all
from common.mitra_chain import (
    DWH_BUNDLINGS, DWH_ITEMS, LOCAL_TZ, add_chain_tasks, dwh_params, same_window_conf, window_from_params,
)


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

with DAG(
    dag_id=DWH_ITEMS,
    default_args=default_args,
    description="ETL iSeller Mitra DWH 1/6: daily_items_summary_iseller_pusat (dipicu rantai harian, tanpa jadwal)",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=LOCAL_TZ),
    catchup=False,
    tags=["etl", "iseller", "mitra", "dwh", "production"],
    max_active_runs=1,
    params=dwh_params(),
) as dag:

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
        execution_timeout=timedelta(hours=2),
    )

    add_chain_tasks(run, DWH_BUNDLINGS, same_window_conf)
