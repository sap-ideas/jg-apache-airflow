"""
DAG: Order Estimated Income (for Landlord)

Builds `order_est_income_for_landlord` (1 row per order_id + transaction_id) from
transactions_iseller_pusat on the Data Warehouse, validated against
`daily_est_income_per_store_for_landlord` before loading.

Schedule : Asset-triggered - runs right after `f_daily_estimated_income_per_store` succeeds
           (the daily table must be loaded first for the validation, and the source table is
           refreshed by etl_iseller_pusat_fulfillment @ 09:00). No fixed clock time.
Data scope: one day - the date processed by the triggering f_daily_estimated_income_per_store
            run (H-1 Asia/Jakarta). Manual trigger: pick a range via `start_date` / `end_date`
            (max 31 days).
"""
from airflow import DAG
from airflow.sdk import Asset
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
import pendulum

from common.airflow_trigger_user import get_manual_trigger_user
from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipelines'))

from etl_orchestrator import run_all
from load import DEFAULT_LOAD_DATA_BY


def load_config(config_path):
    spec = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = load_config("/opt/airflow/config/config_db_datawarehouse.py")

local_tz = pendulum.timezone("Asia/Jakarta")

# Harus sama dengan outlet di DAG f_daily_estimated_income_per_store
DATASET_DAILY_EST_INCOME = Asset("f_daily_estimated_income_per_store://daily_est_income_per_store_for_landlord")
# Emitted on success; downstream per-order landlord DAGs can subscribe to it
DATASET_ORDER_EST_INCOME = Asset("f_order_estimated_income_per_store://order_est_income_for_landlord")


def _target_date_yesterday_jakarta(anchor):
    """Rumus yang sama persis dengan DAG f_daily_estimated_income_per_store: H-1 Asia/Jakarta dari anchor run."""
    jakarta = pendulum.instance(anchor).in_timezone(local_tz)
    return jakarta.subtract(days=1).to_date_string()


def _target_date_from_upstream(**context):
    """
    Tanggal yang diproses DAG daily pada run yang men-trigger DAG ini (dari source_dag_run event asset,
    rumus H-1 yang sama), jadi selalu sama dengan tanggal yang baru di-load ke
    daily_est_income_per_store_for_landlord. Satu hari saja, bukan window.
    Airflow 3.4: key context-nya `triggering_asset_events` (bukan `triggering_dataset_events`).
    """
    events = context.get("triggering_asset_events") or {}
    upstream = sorted(
        (ev.timestamp, ev.source_dag_run)
        for evs in events.values() for ev in evs
        if ev.source_dag_run is not None
    )
    if not upstream:
        raise ValueError(
            "Tidak ada upstream DagRun dari asset event. Untuk proses tanggal tertentu, "
            "trigger manual DAG ini dan isi param start_date / end_date."
        )

    dates = sorted({
        _target_date_yesterday_jakarta(run.data_interval_end or run.logical_date)
        for _, run in upstream
    })
    target = _target_date_yesterday_jakarta(
        upstream[-1][1].data_interval_end or upstream[-1][1].logical_date
    )  # run upstream terbaru
    if len(dates) > 1:
        print(f"  WARNING: beberapa run upstream menumpuk, tanggal {dates}; hanya {target} yang diproses")
    return target


# Isi awal form trigger = H-1 saat file DAG di-parse. Run otomatis tidak memakainya (lihat run_task).
DEFAULT_DATE = pendulum.now(local_tz).subtract(days=1).to_date_string()


def run_task(**context):
    if context["dag_run"].run_type == "manual":
        start_date = context["params"]["start_date"]
        end_date = context["params"]["end_date"]
        # run manual dari UI = email user Airflow yang trigger; run otomatis = NEXUS_AIRFLOW
        load_data_by = get_manual_trigger_user(context["dag_run"])
        print(f"  DATE RANGE: {start_date} s/d {end_date} (manual, load_data_by={load_data_by})")
    else:
        start_date = end_date = _target_date_from_upstream(**context)
        load_data_by = DEFAULT_LOAD_DATA_BY
        print(f"  TARGET DATE: {start_date} (tanggal yang diproses f_daily_estimated_income_per_store)")

    run_all(CONFIG_DW, start_date_str=start_date, end_date_str=end_date, load_data_by=load_data_by)


default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,  # aman: DELETE + INSERT dalam satu transaksi
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="f_order_estimated_income_per_store",
    default_args=default_args,
    description="ETL Order Estimated Income: build order_est_income_for_landlord from transactions_iseller_pusat after f_daily_estimated_income_per_store succeeds",
    schedule=[DATASET_DAILY_EST_INCOME],  # no clock time: triggered by the upstream DAG
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["etl", "landlord", "income", "iseller", "dwh", "production"],
    max_active_runs=1,
    params={
        "start_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="Start date",
            description=(
                "Hanya dipakai saat trigger manual (proses ulang / backfill). Awal rentang tanggal, inklusif. "
                "Satu hari saja: isi start_date dan end_date dengan tanggal yang sama. Run otomatis selalu satu hari."
            ),
        ),
        "end_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="End date",
            description="Hanya dipakai saat trigger manual. Akhir rentang tanggal, inklusif. Maksimal 31 hari dari start_date.",
        ),
    },
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_task,
        outlets=[DATASET_ORDER_EST_INCOME],
    )
