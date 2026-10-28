"""
DAG: ETL iSeller Mitra - Cleaning Duplicates & DWH Redump
Checks for duplicated data in Data Lake, cleans if needed,
then re-dumps all 5 DWH summary tables from cleaned Data Lake.
Sends email notifications at each stage.

Scheduled runs wait until etl_iseller_mitra_fulfillment (07:50) has finished (success or failed):
fulfillment appends to transactions_items_iseller_mitra, and cleaning must not DELETE + re-dump
that table while fulfillment is still writing to it.
"""
from airflow import DAG
from airflow.models import DagModel, DagRun
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.state import State
from datetime import datetime, timedelta
import pendulum
import sys
import os
import importlib.util

_BASE = os.path.dirname(os.path.abspath(__file__))

from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(_BASE)
load_pipeline_dir(os.path.join(_BASE, 'pipelines'))

from redump_etl_functions import run_all


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

MITRA_FULFILLMENT_DAG_ID = "etl_iseller_mitra_fulfillment"
# Fulfillment jalan 07:50, DAG ini 07:51 -> logical date fulfillment 1 menit SEBELUM logical date DAG ini.
# DAG ini sengaja dijadwalkan tepat setelah fulfillment mulai: sensor yang menentukan kapan benar-benar jalan
# (begitu fulfillment selesai). Kalau salah satu jadwal diubah, ubah selisih ini juga
# (kalau tidak, sensor menunggu run yang tidak pernah ada)
FULFILLMENT_RUNS_BEFORE_THIS = timedelta(minutes=1)


def _mitra_fulfillment_finished(dag_run, logical_date, **_):
    """True kalau run fulfillment Mitra hari yang sama sudah selesai (sukses atau gagal)."""
    if dag_run.run_type == "manual":
        return True  # trigger manual tidak ikut menunggu
    fulfillment = DagModel.get_dagmodel(MITRA_FULFILLMENT_DAG_ID)
    if fulfillment is None or fulfillment.is_paused:
        print(f"  {MITRA_FULFILLMENT_DAG_ID} tidak ada / paused -> tidak ditunggu")
        return True
    runs = DagRun.find(dag_id=MITRA_FULFILLMENT_DAG_ID, execution_date=logical_date - FULFILLMENT_RUNS_BEFORE_THIS)
    state = runs[0].state if runs else None
    print(f"  {MITRA_FULFILLMENT_DAG_ID} state: {state}")
    return state in (State.SUCCESS, State.FAILED)


RUN_MODES = ["full", "datalake_only", "dwh_only"]

# Isi awal form trigger = H-1 saat file DAG di-parse. Run terjadwal tidak memakainya (lihat run_pipeline).
DEFAULT_DATE = pendulum.now(local_tz).subtract(days=1).to_date_string()


def run_pipeline(**context):
    params = context.get("params") or {}
    run_mode = params.get("run_mode", "full")
    if run_mode not in RUN_MODES:
        raise ValueError(f"run_mode must be one of {RUN_MODES}, got {run_mode!r}")

    if context["dag_run"].run_type == "manual":
        start_date, end_date = params["start_date"], params["end_date"]
        if start_date > end_date:
            raise ValueError(f"start_date ({start_date}) tidak boleh setelah end_date ({end_date})")
        print(f"  DATE WINDOW: {start_date} s/d {end_date} (manual)")
    else:
        start_date = end_date = _target_date_yesterday_jakarta(**context)
        print(f"  DATE WINDOW: {start_date} (H-1 dari jadwal)")

    print(f"  RUN MODE: {run_mode}")
    run_all(
        start_date,
        end_date,
        CONFIG_DW,
        skip_dl_check=(run_mode == "dwh_only"),
        skip_dwh=(run_mode == "datalake_only"),
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
    dag_id="etl_iseller_mitra_cleaning",
    default_args=default_args,
    description="ETL iSeller Mitra: Check duplicates, clean DL, re-dump 5 DWH summary tables",
    schedule_interval="51 7 * * *",
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
                "full = cek duplikat + bersihkan Data Lake + redump DWH (default, dipakai run terjadwal). "
                "datalake_only = cek + bersihkan Data Lake saja, DWH tidak disentuh. "
                "dwh_only = skip cek/cleaning Data Lake, langsung redump 5 tabel DWH dari DL apa adanya."
            ),
        ),
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
            description="Hanya dipakai saat trigger manual. Tanggal ini ikut diproses.",
        ),
    },
) as dag:

    wait_fulfillment = PythonSensor(
        task_id="wait_mitra_fulfillment",
        python_callable=_mitra_fulfillment_finished,
        mode="reschedule",                   # lepas worker slot selama menunggu
        poke_interval=60,
        timeout=timedelta(hours=4).total_seconds(),
        retries=0,
    )

    run = PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )

    wait_fulfillment >> run
