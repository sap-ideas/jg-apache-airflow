"""
DAG: API Landlord Revenue Sharing — Jilid 01065

Sends yesterday's aggregated revenue (by payment type) for outlet 01065
to the Lippo Mall Puri Revenue API, then logs the result to the Data Lake
and emails the data team.

Schedule : 10:00 Asia/Jakarta, daily
Data scope: yesterday (Asia/Jakarta)
"""

from airflow import DAG
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
_DAG_DIR      = os.path.dirname(os.path.abspath(__file__))
_PIPELINES_DIR = os.path.join(_DAG_DIR, 'pipelines')

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(_PIPELINES_DIR)

from etl_orchestrator import run_all, PostAttemptedError


# ---------------------------------------------------------------------------
# Config loader (mirrors the pattern used across all project DAGs)
# ---------------------------------------------------------------------------

def _load_config(config_path):
    spec   = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = _load_config("/opt/airflow/config/config_db_datawarehouse.py")


# ---------------------------------------------------------------------------
# Callable for PythonOperator
# ---------------------------------------------------------------------------

local_tz = pendulum.timezone("Asia/Jakarta")


def _target_date_yesterday_jakarta(**context):
    """
    Kalender kemarin di Asia/Jakarta relatif ke akhir interval Airflow.
    data_interval_end dipakai hanya sebagai anchor tanggal run, bukan window jam.
    """
    anchor = context.get("data_interval_end") or context.get("logical_date")
    if anchor is None:
        raise ValueError("context missing data_interval_end and logical_date")
    jakarta = pendulum.instance(anchor).in_timezone(local_tz)
    return jakarta.subtract(days=1).to_date_string()


# Isi awal form trigger = H-1 saat file DAG di-parse. Run terjadwal tidak memakainya (lihat run_pipeline).
DEFAULT_DATE = pendulum.now(local_tz).subtract(days=1).to_date_string()


def run_pipeline(**context):
    if context["dag_run"].run_type == "manual":
        target_date = context["params"]["target_date"]
        force_resend = context["params"]["force_resend"]
        print(f"  TARGET DATE: {target_date} (manual, force_resend={force_resend})")
    else:
        target_date = _target_date_yesterday_jakarta(**context)
        force_resend = False
        print(f"  TARGET DATE: {target_date} (H-1 dari jadwal)")

    # AirflowFailException = gagal tanpa retry (retry akan POST ulang dengan TransactionNumber baru)
    try:
        result = run_all(CONFIG_DW, target_date_str=target_date, force_resend=force_resend)
    except PostAttemptedError as e:
        raise AirflowFailException(str(e)) from e

    if result["status"] == "API_ERROR":
        raise AirflowFailException(
            f"API mengembalikan error untuk {target_date}: {result['api_error']} record gagal. "
            "Log tersimpan di DB. Trigger manual lagi: hanya yang belum ada di mall yang dikirim."
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
    dag_id="api_landlord_revenue_sharing_01065",
    default_args=default_args,
    description=(
        "Daily ETL: send yesterday's revenue (jilid 01065) to the Lippo Mall Puri "
        "Revenue API and log results to the Data Lake."
    ),
    schedule_interval="0 10 * * *",  # 10:00 Asia/Jakarta
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["api", "landlord", "revenue-sharing", "lippomall", "production"],
    max_active_runs=1,
    params={
        "target_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="Target date",
            description="Hanya dipakai saat trigger manual (kirim ulang ke API untuk satu tanggal). Run terjadwal selalu H-1.",
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
