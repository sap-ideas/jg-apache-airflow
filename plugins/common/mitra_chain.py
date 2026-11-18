"""
Rantai harian iSeller Mitra. Hanya ingestion yang punya jadwal (06:00 WIB); DAG lain jalan
begitu DAG sebelumnya selesai:

  ingestion (H-1) -> backfilling (H-8..H-7) -> fulfillment (H-8..H-1)
    -> cleaning (H-1, atau H-8..H-1 kalau backfilling/fulfillment mengubah order; mode datalake_only)
    -> DWH items -> bundlings -> transactions -> hourly -> sales_type -> companywide (H-8..H-1)

Aturan:
- Ingestion gagal -> rantai berhenti (tidak ada data baru untuk diproses).
- DAG lain gagal -> rantai tetap lanjut. Pengecualian: companywide dilewati kalau DWH transactions
  gagal di rantai yang sama (companywide menyalin tabel transactions).
- Run manual hanya memicu DAG berikutnya kalau param trigger_next dicentang. dry_run tidak pernah memicu.

Task tiap DAG di rantai: run_pipeline -> next_conf -> trigger_<dag berikutnya>, plus status_akhir.
next_conf selalu jalan (trigger_rule all_done), jadi run_pipeline yang gagal tidak memutus rantai.
status_akhir membuat run merah kalau run_pipeline gagal; tanpa task ini run terlihat hijau karena
task terakhirnya sukses.

Conf ke DAG berikutnya: start_date + end_date (window DAG berikutnya), trigger_next, chain_h1 (H-1
rantai harian), failed (dag_id yang gagal di rantai ini), plus kunci khusus (dl_changed, run_mode).
run_pipeline wajib return dict (bukan None): XCom kosong = run_pipeline gagal.
"""

import datetime

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

LOCAL_TZ = pendulum.timezone("Asia/Jakarta")
# Run manual dari UI = "manual"; run yang dipicu DAG sebelumnya = "operator_triggered"
MANUAL_RUN_TYPES = ("manual", "operator_triggered")
# Paling jauh H-8 = H-1 minus 7 hari
LOOKBACK_DAYS = 7

INGESTION = "etl_iseller_mitra_ingestion"
BACKFILLING = "etl_iseller_mitra_backfilling"
FULFILLMENT = "etl_iseller_mitra_fulfillment"
CLEANING = "etl_iseller_mitra_cleaning"
DWH_ITEMS = "etl_iseller_mitra_dwh_summary_items"
DWH_BUNDLINGS = "etl_iseller_mitra_dwh_summary_bundlings"
DWH_TRANSACTIONS = "etl_iseller_mitra_dwh_summary_transactions"
DWH_HOURLY = "etl_iseller_mitra_dwh_summary_hourly"
DWH_SALES_TYPE = "etl_iseller_mitra_dwh_summary_sales_type"
DWH_COMPANYWIDE = "etl_iseller_mitra_dwh_summary_companywide"


def yesterday_jakarta():
    return pendulum.now(LOCAL_TZ).subtract(days=1).to_date_string()


def days_before(date_str, days):
    return pendulum.parse(date_str).subtract(days=days).to_date_string()


def chain_h1(context):
    """H-1 rantai harian (dibawa dari ingestion). Run manual di tengah rantai: H-1 hari ini."""
    return (context["dag_run"].conf or {}).get("chain_h1") or yesterday_jakarta()


def window_from_params(params):
    """start_date / end_date dari form atau conf. Keduanya wajib, format YYYY-MM-DD, start <= end."""
    start_date, end_date = params.get("start_date"), params.get("end_date")
    if not start_date or not end_date:
        raise ValueError(f"start_date dan end_date wajib diisi (start_date={start_date!r}, end_date={end_date!r})")
    datetime.date.fromisoformat(start_date)
    datetime.date.fromisoformat(end_date)
    if start_date > end_date:
        raise ValueError(f"start_date ({start_date}) tidak boleh setelah end_date ({end_date})")
    return start_date, end_date


def trigger_next_param():
    return Param(
        False,
        type="boolean",
        title="Trigger next",
        description="Hanya trigger manual. Centang = setelah selesai, picu DAG berikutnya di rantai iSeller Mitra. "
                    "Default mati: hanya DAG ini yang jalan.",
    )


def add_chain_tasks(run_task, next_dag_id, next_conf):
    """Pasang next_conf -> trigger_<next_dag_id> dan status_akhir setelah run_task.

    next_conf(result, context) -> dict conf untuk DAG berikutnya, atau None = jangan trigger.
    result = return value run_task (None kalau run_task gagal).
    """
    def _next_conf(**context):
        dag_run, params = context["dag_run"], context["params"]
        if dag_run.run_type in MANUAL_RUN_TYPES and not params.get("trigger_next"):
            print(f"  trigger_next mati: {next_dag_id} tidak dipicu")
            return None
        if params.get("dry_run"):
            print(f"  dry_run: {next_dag_id} tidak dipicu")
            return None

        result = context["ti"].xcom_pull(task_ids=run_task.task_id)
        failed = list((dag_run.conf or {}).get("failed") or [])
        if result is None:
            failed.append(dag_run.dag_id)
        if next_dag_id == DWH_COMPANYWIDE and DWH_TRANSACTIONS in failed:
            print(f"  {DWH_TRANSACTIONS} gagal di rantai ini: {DWH_COMPANYWIDE} TIDAK dipicu "
                  "(companywide menyalin tabel transactions)")
            return None

        conf = next_conf(result, context)
        if conf is None:
            return None
        conf = {"chain_h1": chain_h1(context), **conf, "trigger_next": True, "failed": failed}
        print(f"  Trigger {next_dag_id} dengan conf: {conf}")
        return conf

    def _status_akhir(**context):
        if context["ti"].xcom_pull(task_ids=run_task.task_id) is None:
            raise AirflowFailException(f"{run_task.task_id} gagal, lihat log task itu. "
                                       "Rantai tetap dilanjutkan sesuai aturan di next_conf.")

    check = ShortCircuitOperator(
        task_id="next_conf",
        python_callable=_next_conf,
        trigger_rule="all_done",
        retries=0,
    )
    trigger = TriggerDagRunOperator(
        task_id=f"trigger_{next_dag_id}",
        trigger_dag_id=next_dag_id,
        conf="{{ ti.xcom_pull(task_ids='next_conf') | tojson }}",  # operator mem-parse string JSON jadi dict
    )
    status = PythonOperator(
        task_id="status_akhir",
        python_callable=_status_akhir,
        trigger_rule="all_done",
        retries=0,
    )
    run_task >> check >> trigger
    run_task >> status


# ---------------------------------------------------------------------------
# 6 DAG DWH summary: params dan aturan window yang sama
# ---------------------------------------------------------------------------

def dwh_params():
    """Form trigger manual DAG DWH. Default = H-8 s/d H-1 saat file DAG di-parse."""
    end_date = yesterday_jakarta()
    return {
        "start_date": Param(days_before(end_date, LOOKBACK_DAYS), type="string", format="date", title="Start date",
                            description="Awal window, inklusif. Run dari rantai harian selalu H-8."),
        "end_date": Param(end_date, type="string", format="date", title="End date",
                          description="Akhir window, inklusif. Satu hari saja: isi sama dengan start date."),
        "trigger_next": trigger_next_param(),
        "dry_run": Param(False, type="boolean", title="Dry run",
                         description="True = hitung lalu bandingkan dengan isi DWH sekarang per tanggal. "
                                     "Tidak menulis DWH, tidak kirim email, tidak memicu DAG berikutnya."),
    }


def same_window_conf(result, context):
    """DWH -> tabel DWH berikutnya: window yang sama."""
    start_date, end_date = window_from_params(context["params"])
    return {"start_date": start_date, "end_date": end_date}
