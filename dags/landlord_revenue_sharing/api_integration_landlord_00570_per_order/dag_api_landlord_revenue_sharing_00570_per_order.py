"""
DAG: API Landlord Revenue Sharing PER ORDER — Jilid 00570 (District 2 Meikarta)

Sends estimated income per order (`order_est_income_for_landlord`) for outlet 00570 to the
Lippo Revenue API (one POST per date), logs every row to api_landlord_logs /
api_landlord_log_detail (row 'pending' before POST, final status after), and emails the data team.

Schedule : Asset-triggered — runs right after `f_order_estimated_income_per_store` succeeds.
           Date range = `extra` of the asset event (the range that run loaded). Dates before
           MIN_SEND_DATE (cutover 2026-11-23) are dropped. No fixed clock time.
Manual   : `start_date` / `end_date` (max 31 days, >= 2026-11-23), `dry_run` (default True),
           `resend_uncertain` (default False).
Guard    : rows already accepted by the mall are never sent again; timeout_error / pending rows
           are skipped until checked manually. See docs/pipelines/api_integration_landlord_00570_per_order.md.
"""

from airflow import DAG
from airflow.sdk import Asset
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import importlib.util
import os
import pendulum

from common.airflow_trigger_user import get_manual_trigger_user
from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipelines'))

from etl_orchestrator import run_all, PostAttemptedError, MIN_SEND_DATE
from load import DEFAULT_LOAD_DATA_BY


def _load_config(config_path):
    spec   = importlib.util.spec_from_file_location("config", config_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


CONFIG_DW = _load_config("/opt/airflow/config/config_db_datawarehouse.py")

local_tz = pendulum.timezone("Asia/Jakarta")

# Harus sama dengan outlet di DAG f_order_estimated_income_per_store
DATASET_ORDER_EST_INCOME = Asset("f_order_estimated_income_per_store://order_est_income_for_landlord")


def _range_from_upstream(**context):
    """Rentang tanggal yang di-load run f_order_estimated_income_per_store (dari `extra` asset event).

    Kalau beberapa event menumpuk, dipakai gabungan min(start_date)..max(end_date).
    """
    events = context.get("triggering_asset_events") or {}
    ranges = [
        (ev.extra["start_date"], ev.extra["end_date"])
        for evs in events.values() for ev in evs
        if ev.extra and ev.extra.get("start_date") and ev.extra.get("end_date")
    ]
    if not ranges:
        raise ValueError(
            "Asset event tidak membawa start_date / end_date di extra. Untuk kirim tanggal tertentu, "
            "trigger manual DAG ini dan isi param start_date / end_date."
        )
    start = min(r[0] for r in ranges)
    end   = max(r[1] for r in ranges)
    if len(ranges) > 1:
        print(f"  INFO: {len(ranges)} event upstream menumpuk {sorted(ranges)} -> rentang gabungan {start} s/d {end}")
    return start, end


# Isi awal form trigger = H-1 saat file DAG di-parse. Run otomatis tidak memakainya (lihat run_pipeline).
DEFAULT_DATE = max(pendulum.now(local_tz).subtract(days=1).to_date_string(), MIN_SEND_DATE)


def run_pipeline(**context):
    if context["dag_run"].run_type == "manual":
        start_date       = context["params"]["start_date"]
        end_date         = context["params"]["end_date"]
        dry_run          = context["params"]["dry_run"]
        resend_uncertain = context["params"]["resend_uncertain"]
        # run manual dari UI = email user Airflow yang trigger; run otomatis = NEXUS_AIRFLOW
        load_data_by = get_manual_trigger_user(context["dag_run"])
        print(f"  DATE RANGE: {start_date} s/d {end_date} (manual)")
    else:
        start_date, end_date = _range_from_upstream(**context)
        dry_run, resend_uncertain = False, False
        load_data_by = DEFAULT_LOAD_DATA_BY
        if start_date < MIN_SEND_DATE:
            print(f"  Tanggal sebelum cutover {MIN_SEND_DATE} dibuang (sudah dikirim per payment type oleh DAG lama).")
            start_date = MIN_SEND_DATE
        if start_date > end_date:
            print(f"  Tidak ada tanggal >= {MIN_SEND_DATE} di rentang upstream. Tidak ada yang dikirim.")
            return
        print(f"  DATE RANGE: {start_date} s/d {end_date} (dari f_order_estimated_income_per_store)")

    # AirflowFailException = gagal tanpa retry (retry akan POST ulang dengan TransactionNumber baru)
    try:
        result = run_all(CONFIG_DW, start_date_str=start_date, end_date_str=end_date, dry_run=dry_run,
                         resend_uncertain=resend_uncertain, load_data_by=load_data_by)
    except PostAttemptedError as e:
        raise AirflowFailException(str(e)) from e

    if result["status"] == "API_ERROR":
        raise AirflowFailException(
            f"API menolak / timeout untuk tanggal {', '.join(result['failed_dates'])}. Log tersimpan di DB. "
            "Trigger manual lagi dengan rentang yang sama: yang sudah ada di mall tidak dikirim ulang."
        )


default_args = {
    'owner':            'data-team',
    'depends_on_past':  False,
    'email_on_failure': False,
    'email_on_retry':   False,
    'retries':          2,  # hanya berlaku untuk error sebelum POST; setelah POST -> AirflowFailException
    'retry_delay':      timedelta(minutes=5),
}

with DAG(
    dag_id="api_landlord_revenue_sharing_00570_per_order",
    default_args=default_args,
    description=(
        "Send estimated income per order (jilid 00570) to the Lippo Revenue API after "
        "f_order_estimated_income_per_store succeeds, with anti-duplicate guard and per-date logs."
    ),
    schedule=[DATASET_ORDER_EST_INCOME],  # no clock time: triggered by the upstream DAG
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    tags=["api", "landlord", "revenue-sharing", "lippomall", "meikarta", "per-order", "production"],
    max_active_runs=1,
    params={
        "start_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="Start date",
            description=(
                f"Hanya dipakai saat trigger manual. Awal rentang tanggal, inklusif, tidak boleh sebelum {MIN_SEND_DATE}. "
                "Satu hari saja: isi start_date dan end_date dengan tanggal yang sama."
            ),
        ),
        "end_date": Param(
            DEFAULT_DATE,
            type="string",
            format="date",
            title="End date",
            description="Hanya dipakai saat trigger manual. Akhir rentang tanggal, inklusif. Maksimal 31 hari dari start_date.",
        ),
        "dry_run": Param(
            True,
            type="boolean",
            title="Dry run",
            description=(
                "Default nyala: hanya hitung & tampilkan payload + email DRY RUN, TIDAK kirim ke API dan TIDAK nulis log. "
                "Matikan untuk kirim beneran. Run otomatis selalu kirim beneran."
            ),
        ),
        "resend_uncertain": Param(
            False,
            type="boolean",
            title="Resend timeout_error / pending",
            description=(
                "Default mati: baris yang di log masih timeout_error / pending (belum pasti sampai ke mall) di-skip. "
                "Centang HANYA setelah dicek ke landlord bahwa transaksinya memang belum masuk."
            ),
        ),
    },
) as dag:

    PythonOperator(
        task_id="run_pipeline",
        python_callable=run_pipeline,
    )
