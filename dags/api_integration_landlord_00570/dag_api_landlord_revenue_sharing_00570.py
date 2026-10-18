"""
DAG: API Landlord Revenue Sharing — Jilid 00570 (District 2 Meikarta)

Sends yesterday's estimated income (by payment type) for outlet 00570 to the
Lippo Revenue API, then logs the result to the Data Lake and emails the data team.

Schedule : Dataset-triggered — runs right after `f_daily_estimated_income_per_store`
           succeeds (its source table `daily_est_income_per_store_for_landlord`
           must be loaded first). No fixed clock time.
Data scope: one day — the date processed by the triggering f_daily_estimated_income_per_store
            run (normally yesterday / H-1 Asia/Jakarta). Manual trigger: pick any date via
            the `start_date` / `end_date` params (up to 31 days).
"""

from airflow import DAG, Dataset
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
import sys
import pendulum


# ---------------------------------------------------------------------------
# Path setup — add the pipelines/ sub-folder to sys.path
# ---------------------------------------------------------------------------
_DAG_DIR       = os.path.dirname(os.path.abspath(__file__))
_PIPELINES_DIR = os.path.join(_DAG_DIR, 'pipelines')

from common.airflow_trigger_user import get_manual_trigger_user
from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(_PIPELINES_DIR)

from etl_orchestrator import run_all, PostAttemptedError
from load import DEFAULT_LOAD_DATA_BY


# ---------------------------------------------------------------------------
# Config loader (mirrors the pattern used across all project DAGs)
# ---------------------------------------------------------------------------

def _load_config(config_path):
    spec   = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = _load_config("/opt/airflow/config/config_db_datawarehouse.py")

# Must match the outlet declared in f_daily_estimated_income_per_store DAG
DATASET_DAILY_EST_INCOME = Dataset("f_daily_estimated_income_per_store://daily_est_income_per_store_for_landlord")


# ---------------------------------------------------------------------------
# Callable for PythonOperator
# ---------------------------------------------------------------------------

local_tz = pendulum.timezone("Asia/Jakarta")


def _target_date_yesterday_jakarta(anchor):
    """Rumus yang sama persis dengan DAG f_daily_estimated_income_per_store: H-1 Asia/Jakarta dari anchor run."""
    jakarta = pendulum.instance(anchor).in_timezone(local_tz)
    return jakarta.subtract(days=1).to_date_string()


def _target_date_from_upstream(**context):
    """
    Tanggal yang diproses DAG income per store pada run yang men-trigger DAG ini.
    Diambil dari DagRun upstream (source_dag_run milik dataset event) dengan rumus H-1 yang sama
    dengan DAG tsb, jadi selalu sama dengan tanggal yang baru di-load ke
    daily_est_income_per_store_for_landlord. Satu hari saja, bukan window.
    """
    events = context.get("triggering_dataset_events") or {}
    upstream = sorted(
        (ev.timestamp, ev.source_dag_run)
        for evs in events.values() for ev in evs
        if ev.source_dag_run is not None
    )
    if not upstream:
        raise ValueError(
            "Tidak ada upstream DagRun dari dataset event. Untuk kirim ulang tanggal tertentu, "
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


# Isi awal form trigger = H-1 saat file DAG di-parse. Run otomatis tidak memakainya (lihat run_pipeline).
DEFAULT_DATE = pendulum.now(local_tz).subtract(days=1).to_date_string()


def run_pipeline(**context):
    if context["dag_run"].run_type == "manual":
        start_date   = context["params"]["start_date"]
        end_date     = context["params"]["end_date"]
        force_resend = context["params"]["force_resend"]
        # run manual dari UI = email user Airflow yang trigger; run otomatis = NEXUS_AIRFLOW (default load.py)
        load_data_by = get_manual_trigger_user(context["dag_run"])
        print(f"  DATE RANGE: {start_date} s/d {end_date} (manual, force_resend={force_resend}, load_data_by={load_data_by})")
    else:
        start_date = end_date = _target_date_from_upstream(**context)
        force_resend = False
        load_data_by = DEFAULT_LOAD_DATA_BY
        print(f"  TARGET DATE: {start_date} (tanggal yang diproses f_daily_estimated_income_per_store)")

    # AirflowFailException = gagal tanpa retry (retry akan POST ulang dengan TransactionNumber baru)
    try:
        result = run_all(CONFIG_DW, start_date_str=start_date, end_date_str=end_date,
                         force_resend=force_resend, load_data_by=load_data_by)
    except PostAttemptedError as e:
        raise AirflowFailException(str(e)) from e

    if result["status"] == "API_ERROR":
        raise AirflowFailException(
            f"Gagal untuk tanggal {', '.join(result['failed_dates'])} ({result['api_error']} record API error). "
            "Log tersimpan di DB. Trigger manual lagi dengan rentang yang sama: hanya yang belum ada di mall yang dikirim."
        )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    'owner':            'data-team',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
}

with DAG(
    dag_id="api_landlord_revenue_sharing_00570",
    default_args=default_args,
    description=(
        "ETL: send yesterday's estimated income (jilid 00570) to the Lippo Revenue API "
        "after f_daily_estimated_income_per_store succeeds, and log results to the Data Lake."
    ),
    schedule=[DATASET_DAILY_EST_INCOME],  # no clock time: triggered by the upstream DAG
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["api", "landlord", "revenue-sharing", "lippomall", "meikarta", "production"],
    max_active_runs=1,
    params={
        "start_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="Start date",
            description=(
                "Hanya dipakai saat trigger manual (kirim ulang ke API). Awal rentang tanggal, inklusif. "
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
        "force_resend": Param(
            False,
            type="boolean",
            title="Force resend",
            description=(
                "Default mati: payment type yang sudah ada di mall (pernah sukses / ditolak 'duplicate') "
                "tidak dikirim ulang, hanya sisanya. Centang untuk kirim semua (bisa dobel di sisi mall)."
            ),
        ),
    },
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
