"""
Master Orchestrator - iSeller Pusat ETL Pipeline

Flow:
  1. Backfill fulfillment: detect unfulfilled orders, re-pull from API, update Data Lake
  2. Early exit (no DWH): no unfulfilled | token expired | zero fulfilled after pull
  3. Run 8 DWH summary pipelines in strict order
  4. Send email report with results
"""

import os
import sys
import time
import datetime
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

_PUSAT_DAG_ROOT = os.path.dirname(_PIPELINES_DIR)
if _PUSAT_DAG_ROOT not in sys.path:
    sys.path.insert(0, _PUSAT_DAG_ROOT)

from teams_report_iseller_pusat_unfulfilled import (
    post_teams_safe,
    send_teams_pusat_all_fulfilled,
    send_teams_pusat_completed,
    send_teams_pusat_backfill_failed,
    send_teams_pusat_hub_sync_required,
    send_teams_pusat_missing_access_token,
    send_teams_pusat_token_expired,
)

from pipeline_backfill_fulfillment import (
    MissingAccessTokenError,
    run_backfill_fulfillment,
    TokenExpiredError,
)
from pipeline_daily_transactions import run_daily_transactions
from pipeline_daily_sales_type import run_daily_sales_type
from pipeline_hourly_transactions import run_hourly_transactions
from pipeline_daily_items import run_daily_items
from pipeline_daily_bundlings import run_daily_bundlings
from pipeline_daily_items_sales_type import run_daily_items_sales_type
from pipeline_daily_companywide import run_daily_companywide
from pipeline_hourly_companywide import run_hourly_companywide


def _pusat_teams_meta(airflow_context):
    now_wib = pendulum.now("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ss") + " WIB"
    if not airflow_context:
        return "etl_iseller_pusat_dwh", "n/a", now_wib
    return (
        airflow_context["dag"].dag_id,
        airflow_context["dag_run"].run_id,
        now_wib,
    )


def _send_all_fulfilled_email(start_date, end_date):
    """Send email when no unfulfilled orders found — Data Lake is clean."""
    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#28a745;">ETL iSeller Pusat - Data Lake Safe</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>

        <div style="background-color:#d4edda;border:1px solid #c3e6cb;color:#155724;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Tidak ditemukan order yang unfulfilled.</strong><br><br>
            Semua order di Data Lake sudah berstatus <code>fulfilled</code>.<br>
            DWH summary <strong>tidak perlu di-update</strong> — pipeline selesai.
        </div>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[OK] ETL iSeller Pusat - All Orders Fulfilled ({start_date} s/d {end_date})"

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print(f"\n  All-fulfilled email sent to: {', '.join(receiver_email)}")


def _send_hub_sync_required_email(start_date, end_date, still_unfulfilled_count):
    """API returned data but no orders became fulfilled — ask Hub Manager to sync, then re-run manual."""
    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#fd7e14;">ETL iSeller Pusat - STOPPED (Orders Masih Unfulfilled)</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>

        <div style="background-color:#fff3cd;border:1px solid #ffc107;color:#856404;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Telah dilakukan tarikan data dari API iSeller</strong>, namun <strong>tidak ada satupun order</strong>
            yang dapat di-update menjadi fulfilled (<strong>Fulfilled: 0</strong>).<br><br>
            Saat ini masih ada <strong>{still_unfulfilled_count:,}</strong> order dengan status unfulfilled.<br><br>
            <strong>Pipeline dihentikan</strong> — DWH summary <strong>tidak di-update</strong>.
        </div>

        <div style="background-color:#f8f9fa;border-left:4px solid #4472C4;padding:12px 16px;margin-bottom:20px;">
            <strong>Tindakan yang diperlukan:</strong><br><br>
            Order-order tersebut masih belum fulfilled di sistem. Mohon <strong>segera hubungi Hub Manager</strong>
            untuk melakukan sync data agar order dapat fulfilled di iSeller, lalu <strong>jalankan ulang pipeline secara manual</strong>
            setelah data sudah sinkron.
        </div>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = (
        f"[ACTION REQUIRED] iSeller Pusat — Orders Unfulfilled (Hub Sync) ({start_date} s/d {end_date})"
    )

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print(f"\n  Hub sync required email sent to: {', '.join(receiver_email)}")


def _send_missing_access_token_email(start_date, end_date):
    """Pipeline stopped: access_token_pusat missing (Variable or env)."""
    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#dc3545;">ETL iSeller Pusat - STOPPED (Access Token Not Set)</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>

        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline dihentikan.</strong><br><br>
            Token iSeller Pusat (<code>access_token_pusat</code>) <strong>tidak ada atau kosong</strong>.<br><br>
            Prioritas konfigurasi: <strong>Airflow Variable</strong> <code>access_token_pusat</code> (disarankan — bisa di-update via UI),
            atau fallback environment variable <code>access_token_pusat</code> di container.<br><br>
            Backfill fulfillment tidak bisa dipanggil dan DWH summary <strong>tidak di-update</strong>.<br><br>
            Set Variable di <strong>Admin → Variables</strong> atau set env di <code>.env</code> / Docker Compose, lalu jalankan ulang DAG.
        </div>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[ALERT] ETL iSeller Pusat - STOPPED - access_token_pusat Not Set ({start_date} s/d {end_date})"

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print(f"\n  Missing access token email sent to: {', '.join(receiver_email)}")


def _send_backfill_failed_email(start_date, end_date, error_message):
    """Generic backfill failure — DWH not updated."""
    safe_msg = html_escape(error_message or "")

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#dc3545;">ETL iSeller Pusat - STOPPED (Backfill Failed)</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>

        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline dihentikan.</strong> Step backfill fulfillment gagal.<br><br>
            DWH summary <strong>tidak di-update</strong>. Periksa log task untuk detail.<br><br>
            <strong>Error:</strong><br><pre style="white-space:pre-wrap;font-size:12px;">{safe_msg}</pre>
        </div>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[ALERT] ETL iSeller Pusat - STOPPED - Backfill Error ({start_date} s/d {end_date})"

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print(f"\n  Backfill failed email sent to: {', '.join(receiver_email)}")


def _send_token_expired_email(start_date, end_date, df_unfulfilled):
    """Send alert email when pipeline is stopped due to expired API token."""
    th = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#dc3545;color:#fff;text-align:left;font-size:13px;"'
    td = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    unfulfilled_rows = ""
    if df_unfulfilled is not None and len(df_unfulfilled) > 0:
        for i, (_, row) in enumerate(df_unfulfilled.iterrows()):
            bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
            unfulfilled_rows += (
                f'<tr{bg}>'
                f'<td {td}>{row.get("date", "")}</td>'
                f'<td {td}>{row.get("outlet_code", "")}</td>'
                f'<td {td}>{row.get("order_id", "")}</td>'
                f'<td {td}>{row.get("order_reference", "")}</td>'
                f'<td {td}>{row.get("fulfillment_status", "")}</td>'
                f'<td {td_r}>{float(row.get("total_amount", 0)):,.0f}</td>'
                f'</tr>'
            )
        total_amount = df_unfulfilled['total_amount'].astype(float).sum()
    else:
        total_amount = 0

    count = len(df_unfulfilled) if df_unfulfilled is not None else 0

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#dc3545;">ETL iSeller Pusat - STOPPED (API Token Expired)</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>

        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline dihentikan.</strong><br>
            API Token iSeller Pusat (<code>access_token_pusat</code>) sudah <strong>expired</strong>.<br>
            Data unfulfilled orders <strong>tidak bisa diproses</strong> dan DWH summary <strong>tidak di-update</strong>.<br><br>
            Segera update token di environment variable dan jalankan ulang pipeline.
        </div>

        <h3 style="color:#333;">Unfulfilled Orders ({count} orders, total: Rp {total_amount:,.0f})</h3>
        <table {table}>
            <tr>
                <th {th}>Date</th><th {th}>Outlet</th><th {th}>Order ID</th>
                <th {th}>Reference</th><th {th}>Status</th><th {th}>Amount</th>
            </tr>
            {unfulfilled_rows}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[ALERT] ETL iSeller Pusat - STOPPED - API Token Expired ({start_date} s/d {end_date})"

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print(f"\n  Token expired alert email sent to: {', '.join(receiver_email)}")


def _send_report_email(start_date, end_date, backfill_stats, results):
    th = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    # Backfill summary
    backfill_html = ""
    if backfill_stats:
        backfill_html = f"""
        <h3 style="color:#333;">Backfill Fulfillment</h3>
        <table {table}>
            <tr><th {th}>Metric</th><th {th}>Value</th></tr>
            <tr><td {td}>Orders Fulfilled</td><td {td_r}>{backfill_stats.get('total_fulfilled', 0):,}</td></tr>
            <tr style="background-color:#f2f2f2;"><td {td}>Orders Still Unfulfilled</td><td {td_r}>{backfill_stats.get('total_unfulfilled', 0):,}</td></tr>
            <tr><td {td}>Detail Rows Dumped</td><td {td_r}>{backfill_stats.get('total_details_dumped', 0):,}</td></tr>
        </table>
        """

    # Pipeline summary
    summary_rows = ""
    for i, (table_name, r) in enumerate(results.items()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        if r["status"] == "SUCCESS":
            badge = '<span style="color:#fff;background-color:#28a745;padding:2px 8px;border-radius:4px;font-size:12px;">SUCCESS</span>'
            summary_rows += f'<tr{bg}><td {td}>{table_name}</td><td {td}>{badge}</td><td {td_r}>{r["deleted"]:,}</td><td {td_r}>{r["dumped"]:,}</td><td {td_r}>{r["time"]}</td></tr>'
        else:
            badge = '<span style="color:#fff;background-color:#dc3545;padding:2px 8px;border-radius:4px;font-size:12px;">FAILED</span>'
            summary_rows += f'<tr{bg}><td {td}>{table_name}</td><td {td}>{badge}</td><td {td} colspan="3">{r["error"]}</td></tr>'

    # Before vs After
    ba_rows = ""
    row_idx = 0
    for table_name, r in results.items():
        if r["status"] != "SUCCESS":
            continue
        short_name = table_name.replace("_iseller_pusat", "").replace("_companywide_new", "_cw")
        for col in r["before"]:
            b, a = r["before"][col], r["after"][col]
            diff = a - b
            sign = "+" if diff >= 0 else ""
            diff_color = "#28a745" if diff == 0 else ("#dc3545" if diff < 0 else "#ff8c00")
            bg = ' style="background-color:#f2f2f2;"' if row_idx % 2 == 0 else ""
            ba_rows += (
                f'<tr{bg}>'
                f'<td {td}>{short_name}</td><td {td}>{col}</td>'
                f'<td {td_r}>{b:,.0f}</td><td {td_r}>{a:,.0f}</td>'
                f'<td style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;color:{diff_color};font-weight:bold;">{sign}{diff:,.0f}</td>'
                f'</tr>'
            )
            row_idx += 1

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#4472C4;">ETL iSeller Pusat - Completed</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>

        {backfill_html}

        <h3 style="color:#333;">DWH Pipeline Summary</h3>
        <table {table}>
            <tr><th {th}>Table</th><th {th}>Status</th><th {th}>Deleted</th><th {th}>Dumped</th><th {th}>Time</th></tr>
            {summary_rows}
        </table>

        <h3 style="color:#333;">Before vs After</h3>
        <table {table}>
            <tr><th {th}>Table</th><th {th}>Column</th><th {th}>Before</th><th {th}>After</th><th {th}>Diff</th></tr>
            {ba_rows}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[REPORT] ETL iSeller Pusat - Completed ({start_date} s/d {end_date})"

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print(f"\n  Report email sent to: {', '.join(receiver_email)}")


def run_all(start_date, end_date, config_dw, skip_dl_check=False, airflow_context=None):
    """
    Full ETL flow:
      1. Backfill fulfillment (Data Lake) — skip jika skip_dl_check=True
      2. Run 8 DWH summary pipelines in strict order
      3. Send email report

    Args:
        start_date: format YYYY-MM-DD
        end_date: format YYYY-MM-DD
        config_dw: DB config module
        skip_dl_check: jika True, skip backfill fulfillment (DL sudah di-clean manual)
        airflow_context: optional Airflow task context (for Teams metadata)
    """
    dag_id, run_id, exec_at = _pusat_teams_meta(airflow_context)

    # ---- STEP 1: Backfill Fulfillment ----
    backfill_stats = None

    if skip_dl_check:
        print("=" * 65)
        print("  SKIP Step 1 - Backfill Fulfillment (DL sudah bersih / cleaned manual)")
        print("=" * 65)
    else:
        try:
            backfill_stats = run_backfill_fulfillment(config_dw, start_date, end_date)
        except MissingAccessTokenError as e:
            print(f"\n  access_token_pusat NOT SET — pipeline dihentikan.")
            print(f"  {e}")
            _send_missing_access_token_email(start_date, end_date)
            post_teams_safe(
                "missing access token",
                send_teams_pusat_missing_access_token,
                period_start=start_date,
                period_end=end_date,
                dag_id=dag_id,
                run_id=run_id,
                executed_at_display=exec_at,
            )
            return {"_stopped": True, "_reason": "missing_access_token", "_error": str(e)}
        except TokenExpiredError as e:
            print(f"\n  API TOKEN EXPIRED — pipeline dihentikan.")
            print(f"  {e}")
            _send_token_expired_email(start_date, end_date, e.df_unfulfilled)
            post_teams_safe(
                "token expired",
                send_teams_pusat_token_expired,
                period_start=start_date,
                period_end=end_date,
                df_unfulfilled=e.df_unfulfilled,
                dag_id=dag_id,
                run_id=run_id,
                executed_at_display=exec_at,
            )
            return {"_stopped": True, "_reason": "token_expired", "_error": str(e)}
        except Exception as e:
            print(f"\n  BACKFILL FAILED: {e}")
            backfill_stats = {
                'error': str(e),
                'error_type': type(e).__name__,
                'total_fulfilled': 0, 'total_unfulfilled': 0, 'total_details_dumped': 0,
            }

        if backfill_stats and backfill_stats.get('error'):
            err = backfill_stats['error']
            print("\n  Backfill error — pipeline dihentikan. DWH tidak di-update.")
            _send_backfill_failed_email(start_date, end_date, err)
            post_teams_safe(
                "backfill failed",
                send_teams_pusat_backfill_failed,
                period_start=start_date,
                period_end=end_date,
                error_message=str(err),
                error_type=backfill_stats.get('error_type', 'Exception'),
                dag_id=dag_id,
                run_id=run_id,
                executed_at_display=exec_at,
            )
            return {"_stopped": True, "_reason": "backfill_failed", "_error": str(err)}

        if backfill_stats and 'error' not in backfill_stats and backfill_stats.get('no_unfulfilled_found'):
            print("\n  No unfulfilled orders found — Data Lake is clean.")
            print("  Skipping DWH re-dump. Pipeline selesai.")
            _send_all_fulfilled_email(start_date, end_date)
            post_teams_safe(
                "all fulfilled",
                send_teams_pusat_all_fulfilled,
                period_start=start_date,
                period_end=end_date,
                dag_id=dag_id,
                run_id=run_id,
                executed_at_display=exec_at,
            )
            return {"_stopped": True, "_reason": "all_fulfilled"}

        if (
            backfill_stats and 'error' not in backfill_stats
            and backfill_stats.get('total_fulfilled', 0) == 0
            and backfill_stats.get('total_unfulfilled', 0) > 0
        ):
            print("\n  Tidak ada order yang menjadi fulfilled setelah tarikan API.")
            print("  Pipeline dihentikan — Hub Manager sync diperlukan. DWH tidak di-update.")
            _send_hub_sync_required_email(
                start_date, end_date, backfill_stats['total_unfulfilled']
            )
            post_teams_safe(
                "hub sync required",
                send_teams_pusat_hub_sync_required,
                period_start=start_date,
                period_end=end_date,
                still_unfulfilled_count=backfill_stats["total_unfulfilled"],
                total_amount=backfill_stats.get("unfulfilled_amount", 0.0),
                by_outlet=backfill_stats.get("unfulfilled_by_outlet", []),
                dag_id=dag_id,
                run_id=run_id,
                executed_at_display=exec_at,
            )
            return {"_stopped": True, "_reason": "no_progress_all_still_unfulfilled"}

    # ---- STEP 2: DWH Summary Pipelines (strict order) ----
    pipelines = [
        ("daily_transaction_summary_iseller_pusat", run_daily_transactions),
        ("daily_sales_type_summary_iseller_pusat", run_daily_sales_type),
        ("hourly_transaction_summary_iseller_pusat", run_hourly_transactions),
        ("daily_items_summary_iseller_pusat", run_daily_items),
        ("daily_bundling_summary_iseller_pusat", run_daily_bundlings),
        ("daily_items_salestype_summary_pusat", run_daily_items_sales_type),
        ("daily_outlet_transaction_summary_companywide_new", run_daily_companywide),
        ("hourly_outlet_transaction_summary_companywide_new", run_hourly_companywide),
    ]

    print()
    print("=" * 65)
    print("  STEP 2: DWH SUMMARY PIPELINES - iSeller Pusat")
    print(f"  Period: {start_date} s/d {end_date}")
    print("=" * 65)

    results = {}
    total_start = time.time()

    for idx, (table_name, func) in enumerate(pipelines):
        print()
        try:
            t0 = time.time()
            deleted, dumped, before, after = func(start_date, end_date, config_dw)
            elapsed = time.time() - t0
            results[table_name] = {
                "status": "SUCCESS", "deleted": deleted,
                "dumped": dumped, "time": f"{elapsed:.1f}s",
                "before": before, "after": after
            }
        except Exception as e:
            results[table_name] = {"status": "FAILED", "error": str(e)}
            print(f"        ERROR: {e}")


    total_elapsed = time.time() - total_start

    # ---- Summary ----
    print()
    print("=" * 65)
    print("  SUMMARY")
    print("=" * 65)

    if backfill_stats:
        if 'error' in backfill_stats:
            print(f"  BACKFILL | FAILED: {backfill_stats['error']}")
        else:
            print(f"  BACKFILL | Fulfilled={backfill_stats['total_fulfilled']}, "
                  f"Unfulfilled={backfill_stats['total_unfulfilled']}, "
                  f"Details={backfill_stats['total_details_dumped']}")

    for table_name, result in results.items():
        if result["status"] == "SUCCESS":
            print(f"  {result['status']} | {table_name}")
            print(f"           Deleted={result['deleted']}, Dumped={result['dumped']}, Time={result['time']}")
        else:
            print(f"  {result['status']} | {table_name}")
            print(f"           Error: {result['error']}")

    print(f"\n  Total time: {total_elapsed:.1f}s")
    print("=" * 65)

    # ---- STEP 3: Send email report ----
    _send_report_email(start_date, end_date, backfill_stats, results)
    post_teams_safe(
        "completed",
        send_teams_pusat_completed,
        period_start=start_date,
        period_end=end_date,
        backfill_stats=backfill_stats or {},
        results=results,
        dag_id=dag_id,
        run_id=run_id,
        executed_at_display=exec_at,
    )

    return results
