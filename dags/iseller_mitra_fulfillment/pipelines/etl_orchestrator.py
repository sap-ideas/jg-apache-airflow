"""
Master Orchestrator - iSeller Mitra Fulfillment Pipeline

Mirror dari iseller_pusat_dwh/pipelines/etl_orchestrator.py, TANPA step re-dump DWH.

Flow:
  1. Backfill fulfillment: detect unfulfilled orders, re-pull from API, update Data Lake
  2. Early exit: no unfulfilled | token missing/expired | zero fulfilled after pull (Hub Manager sync)
  3. Send email + Teams report (Data Lake updated)
"""

import os
import sys
from html import escape as html_escape
import smtplib
import ssl
from email.mime.text import MIMEText

import pendulum

# When this file is parsed directly (without the DAG first adding sys.path),
# `pipeline_*` modules live in this same `pipelines/` directory.
_PIPELINES_DIR = os.path.dirname(os.path.abspath(__file__))
if _PIPELINES_DIR not in sys.path:
    sys.path.insert(0, _PIPELINES_DIR)

_MITRA_DAG_ROOT = os.path.dirname(_PIPELINES_DIR)
if _MITRA_DAG_ROOT not in sys.path:
    sys.path.insert(0, _MITRA_DAG_ROOT)

from teams_report_iseller_mitra_fulfillment import (
    post_teams_safe,
    send_teams_mitra_all_fulfilled,
    send_teams_mitra_backfill_failed,
    send_teams_mitra_completed,
    send_teams_mitra_hub_sync_required,
    send_teams_mitra_missing_access_token,
    send_teams_mitra_token_expired,
)

from pipeline_backfill_fulfillment import (
    MissingAccessTokenError,
    run_backfill_fulfillment,
    TokenExpiredError,
)

DEFAULT_DAG_ID = "etl_iseller_mitra_fulfillment"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]

_FOOTER = '<p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>'
_TH = 'style="border:1px solid #ddd;padding:8px 12px;background-color:{bg};color:#fff;text-align:left;font-size:13px;"'
_TD = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
_TD_R = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
_TABLE = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'


def _send_email_safe(label, fn, *args):
    """Email gagal (SMTP error) tidak boleh menggagalkan task: retry akan menjalankan ulang seluruh pipeline."""
    try:
        fn(*args)
    except Exception as e:
        print(f"  WARNING: email ({label}) gagal dikirim: {type(e).__name__}: {e}")


def _fail_without_retry(message):
    """AirflowFailException (tanpa retry) kalau jalan di Airflow, selain itu RuntimeError."""
    try:
        from airflow.exceptions import AirflowFailException  # type: ignore
        return AirflowFailException(message)
    except ImportError:
        return RuntimeError(message)


def _mitra_teams_meta(airflow_context):
    now_wib = pendulum.now("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ss") + " WIB"
    if not airflow_context:
        return DEFAULT_DAG_ID, "n/a", now_wib
    return (
        airflow_context["dag"].dag_id,
        airflow_context["dag_run"].run_id,
        now_wib,
    )


def _send_html_email(label, subject, html):
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECEIVER_EMAIL)

    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(SENDER_EMAIL, password)
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())

    print(f"\n  {label} email sent to: {', '.join(RECEIVER_EMAIL)}")


def _page(title, title_color, start_date, end_date, body_html):
    return f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{title_color};">{title}</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>
        {body_html}
        {_FOOTER}
    </body>
    </html>"""


def _box(bg, border, color, inner):
    return (f'<div style="background-color:{bg};border:1px solid {border};color:{color};'
            f'padding:15px;border-radius:5px;margin-bottom:20px;">{inner}</div>')


def _send_all_fulfilled_email(start_date, end_date):
    """Send email when no unfulfilled orders found — Data Lake is clean."""
    body = _box("#d4edda", "#c3e6cb", "#155724",
                "<strong>Tidak ditemukan order yang unfulfilled.</strong><br><br>"
                "Semua order di Data Lake sudah berstatus <code>fulfilled</code> — pipeline selesai.")
    _send_html_email(
        "All-fulfilled",
        f"[OK] ETL iSeller Mitra - All Orders Fulfilled ({start_date} s/d {end_date})",
        _page("ETL iSeller Mitra - Data Lake Safe", "#28a745", start_date, end_date, body),
    )


def _send_hub_sync_required_email(start_date, end_date, still_unfulfilled_count):
    """API returned data but no orders became fulfilled — ask Hub Manager to sync, then re-run manual."""
    body = _box("#fff3cd", "#ffc107", "#856404",
                "<strong>Telah dilakukan tarikan data dari API iSeller</strong>, namun <strong>tidak ada satupun order</strong> "
                "yang dapat di-update menjadi fulfilled (<strong>Fulfilled: 0</strong>).<br><br>"
                f"Saat ini masih ada <strong>{still_unfulfilled_count:,}</strong> order dengan status unfulfilled.<br><br>"
                "<strong>Pipeline dihentikan</strong> — Data Lake tidak diubah.") + \
        ('<div style="background-color:#f8f9fa;border-left:4px solid #4472C4;padding:12px 16px;margin-bottom:20px;">'
         "<strong>Tindakan yang diperlukan:</strong><br><br>"
         "Order-order tersebut masih belum fulfilled di sistem. Mohon <strong>segera hubungi Hub Manager</strong> "
         "untuk melakukan sync data agar order dapat fulfilled di iSeller, lalu <strong>jalankan ulang pipeline secara manual</strong> "
         "setelah data sudah sinkron.</div>")
    _send_html_email(
        "Hub sync required",
        f"[ACTION REQUIRED] iSeller Mitra — Orders Unfulfilled (Hub Sync) ({start_date} s/d {end_date})",
        _page("ETL iSeller Mitra - STOPPED (Orders Masih Unfulfilled)", "#fd7e14", start_date, end_date, body),
    )


def _send_missing_access_token_email(start_date, end_date):
    """Pipeline stopped: access_token_mitra missing (Variable or env)."""
    body = _box("#f8d7da", "#f5c6cb", "#721c24",
                "<strong>Pipeline dihentikan.</strong><br><br>"
                "Token iSeller Mitra (<code>access_token_mitra</code>) <strong>tidak ada atau kosong</strong>.<br><br>"
                "Prioritas konfigurasi: <strong>Airflow Variable</strong> <code>access_token_mitra</code> (disarankan — bisa di-update via UI), "
                "atau fallback environment variable <code>access_token_mitra</code> di container.<br><br>"
                "Backfill fulfillment tidak bisa dipanggil.<br><br>"
                "Set Variable di <strong>Admin → Variables</strong> atau set env di <code>.env</code> / Docker Compose, lalu jalankan ulang DAG.")
    _send_html_email(
        "Missing access token",
        f"[ALERT] ETL iSeller Mitra - STOPPED - access_token_mitra Not Set ({start_date} s/d {end_date})",
        _page("ETL iSeller Mitra - STOPPED (Access Token Not Set)", "#dc3545", start_date, end_date, body),
    )


def _send_backfill_failed_email(start_date, end_date, error_message):
    """Generic backfill failure."""
    safe_msg = html_escape(error_message or "")
    body = _box("#f8d7da", "#f5c6cb", "#721c24",
                "<strong>Pipeline dihentikan.</strong> Step backfill fulfillment gagal.<br><br>"
                "Penulisan ke Data Lake di-rollback. Periksa log task untuk detail.<br><br>"
                f'<strong>Error:</strong><br><pre style="white-space:pre-wrap;font-size:12px;">{safe_msg}</pre>')
    _send_html_email(
        "Backfill failed",
        f"[ALERT] ETL iSeller Mitra - STOPPED - Backfill Error ({start_date} s/d {end_date})",
        _page("ETL iSeller Mitra - STOPPED (Backfill Failed)", "#dc3545", start_date, end_date, body),
    )


def _send_token_expired_email(start_date, end_date, df_unfulfilled):
    """Send alert email when pipeline is stopped due to expired API token."""
    th = _TH.format(bg="#dc3545")
    unfulfilled_rows = ""
    total_amount = 0
    count = 0
    if df_unfulfilled is not None and len(df_unfulfilled) > 0:
        count = len(df_unfulfilled)
        total_amount = df_unfulfilled['total_amount'].astype(float).sum()
        for i, (_, row) in enumerate(df_unfulfilled.iterrows()):
            bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
            unfulfilled_rows += (
                f'<tr{bg}>'
                f'<td {_TD}>{row.get("date", "")}</td>'
                f'<td {_TD}>{row.get("outlet_code", "")}</td>'
                f'<td {_TD}>{row.get("order_id", "")}</td>'
                f'<td {_TD}>{row.get("order_reference", "")}</td>'
                f'<td {_TD}>{row.get("fulfillment_status", "")}</td>'
                f'<td {_TD_R}>{float(row.get("total_amount", 0)):,.0f}</td>'
                f'</tr>'
            )

    body = _box("#f8d7da", "#f5c6cb", "#721c24",
                "<strong>Pipeline dihentikan.</strong><br>"
                "API Token iSeller Mitra (<code>access_token_mitra</code>) sudah <strong>expired</strong>.<br>"
                "Data unfulfilled orders <strong>tidak bisa diproses</strong>.<br><br>"
                "Segera update token di environment variable dan jalankan ulang pipeline.") + f"""
        <h3 style="color:#333;">Unfulfilled Orders ({count} orders, total: Rp {total_amount:,.0f})</h3>
        <table {_TABLE}>
            <tr>
                <th {th}>Date</th><th {th}>Outlet</th><th {th}>Order ID</th>
                <th {th}>Reference</th><th {th}>Status</th><th {th}>Amount</th>
            </tr>
            {unfulfilled_rows}
        </table>"""
    _send_html_email(
        "Token expired alert",
        f"[ALERT] ETL iSeller Mitra - STOPPED - API Token Expired ({start_date} s/d {end_date})",
        _page("ETL iSeller Mitra - STOPPED (API Token Expired)", "#dc3545", start_date, end_date, body),
    )


def _send_completed_email(start_date, end_date, backfill_stats):
    """Backfill selesai, Data Lake ter-update."""
    th = _TH.format(bg="#4472C4")
    bs = backfill_stats or {}

    body = _box("#d4edda", "#c3e6cb", "#155724",
                "Backfill fulfillment ke Data Lake selesai.") + f"""
        <h3 style="color:#333;">Backfill Fulfillment</h3>
        <table {_TABLE}>
            <tr><th {th}>Metric</th><th {th}>Value</th></tr>
            <tr><td {_TD}>Orders Fulfilled</td><td {_TD_R}>{bs.get('total_fulfilled', 0):,}</td></tr>
            <tr style="background-color:#f2f2f2;"><td {_TD}>Fulfilled Amount</td><td {_TD_R}>Rp {float(bs.get('fulfilled_amount', 0) or 0):,.0f}</td></tr>
            <tr><td {_TD}>Orders Still Unfulfilled</td><td {_TD_R}>{bs.get('total_unfulfilled', 0):,}</td></tr>
            <tr style="background-color:#f2f2f2;"><td {_TD}>Unfulfilled Amount</td><td {_TD_R}>Rp {float(bs.get('unfulfilled_amount', 0) or 0):,.0f}</td></tr>
            <tr><td {_TD}>Detail Rows Dumped</td><td {_TD_R}>{bs.get('total_details_dumped', 0):,}</td></tr>
        </table>"""
    _send_html_email(
        "Completed",
        f"[OK] ETL iSeller Mitra - Fulfillment Completed ({start_date} s/d {end_date})",
        _page("ETL iSeller Mitra - Fulfillment Completed", "#28a745", start_date, end_date, body),
    )


def _run_dry(start_date, end_date, config_dw):
    """Preview read-only: tidak ada tulis ke DL, tidak ada email/Teams."""
    print("=" * 65)
    print("  DRY RUN — tidak ada perubahan data, tidak ada email/Teams")
    print("=" * 65)
    backfill_stats = run_backfill_fulfillment(config_dw, start_date, end_date, dry_run=True)
    return {"_stopped": True, "_reason": "dry_run", "backfill": backfill_stats}


def run_all(start_date, end_date, config_dw, dry_run=False, airflow_context=None):
    """
    Flow:
      1. Backfill fulfillment (Data Lake)
      2. Send email + Teams report

    Pipeline Mitra berhenti di Data Lake — tabel DWH tidak disentuh.

    Args:
        start_date: format YYYY-MM-DD
        end_date: format YYYY-MM-DD
        config_dw: DB config module
        dry_run: jika True, read-only: tidak ada tulis ke DL, tidak ada email/Teams
        airflow_context: optional Airflow task context (for Teams metadata)
    """
    if dry_run:
        return _run_dry(start_date, end_date, config_dw)

    dag_id, run_id, exec_at = _mitra_teams_meta(airflow_context)
    teams_meta = dict(period_start=start_date, period_end=end_date,
                      dag_id=dag_id, run_id=run_id, executed_at_display=exec_at)

    # ---- STEP 1: Backfill Fulfillment ----
    try:
        backfill_stats = run_backfill_fulfillment(config_dw, start_date, end_date)
    except MissingAccessTokenError as e:
        print(f"\n  access_token_mitra NOT SET — pipeline dihentikan.")
        print(f"  {e}")
        _send_email_safe("missing access token", _send_missing_access_token_email, start_date, end_date)
        post_teams_safe("missing access token", send_teams_mitra_missing_access_token, **teams_meta)
        raise _fail_without_retry(f"Pipeline dihentikan: access_token_mitra tidak ada. {e}")
    except TokenExpiredError as e:
        print(f"\n  API TOKEN EXPIRED — pipeline dihentikan.")
        print(f"  {e}")
        _send_email_safe("token expired", _send_token_expired_email, start_date, end_date, e.df_unfulfilled)
        post_teams_safe("token expired", send_teams_mitra_token_expired,
                        df_unfulfilled=e.df_unfulfilled, **teams_meta)
        raise _fail_without_retry(f"Pipeline dihentikan: access_token_mitra expired. {e}")
    except Exception as e:
        print(f"\n  BACKFILL FAILED: {e}")
        print("\n  Backfill error — pipeline dihentikan.")
        _send_email_safe("backfill failed", _send_backfill_failed_email, start_date, end_date, str(e))
        post_teams_safe("backfill failed", send_teams_mitra_backfill_failed,
                        error_message=str(e), error_type=type(e).__name__, **teams_meta)
        # Backfill idempoten (guard + transaksi atomik), jadi retry Airflow aman.
        raise

    if backfill_stats.get('no_unfulfilled_found'):
        print("\n  No unfulfilled orders found — Data Lake is clean. Pipeline selesai.")
        _send_email_safe("all fulfilled", _send_all_fulfilled_email, start_date, end_date)
        post_teams_safe("all fulfilled", send_teams_mitra_all_fulfilled, **teams_meta)
        return {"_stopped": True, "_reason": "all_fulfilled"}

    if backfill_stats.get('total_fulfilled', 0) == 0 and backfill_stats.get('total_unfulfilled', 0) > 0:
        print("\n  Tidak ada order yang menjadi fulfilled setelah tarikan API.")
        print("  Pipeline dihentikan — Hub Manager sync diperlukan.")
        _send_email_safe("hub sync required", _send_hub_sync_required_email,
                         start_date, end_date, backfill_stats['total_unfulfilled'])
        post_teams_safe(
            "hub sync required",
            send_teams_mitra_hub_sync_required,
            still_unfulfilled_count=backfill_stats["total_unfulfilled"],
            total_amount=backfill_stats.get("unfulfilled_amount", 0.0),
            by_outlet=backfill_stats.get("unfulfilled_by_outlet", []),
            **teams_meta,
        )
        return {"_stopped": True, "_reason": "no_progress_all_still_unfulfilled"}

    # ---- STEP 2: Report (Data Lake updated; DWH sengaja tidak disentuh) ----
    print()
    print("=" * 65)
    print("  SUMMARY")
    print("=" * 65)
    print(f"  BACKFILL | Fulfilled={backfill_stats['total_fulfilled']}, "
          f"Unfulfilled={backfill_stats['total_unfulfilled']}, "
          f"Details={backfill_stats['total_details_dumped']}")
    print("  DWH tidak di-redump (bukan bagian pipeline ini).")
    print("=" * 65)

    _send_email_safe("completed", _send_completed_email, start_date, end_date, backfill_stats)
    post_teams_safe("completed", send_teams_mitra_completed, backfill_stats=backfill_stats, **teams_meta)

    return {"_stopped": False, "_reason": "completed", "backfill": backfill_stats}
