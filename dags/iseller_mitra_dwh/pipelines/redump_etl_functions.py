"""
Master Orchestrator - Re-Dump All DWH Summary Tables (iSeller Mitra)

Flow:
  1. Check duplicated order_id di Data Lake → kirim email alert jika ada
  2. Clean duplicated data di DL (headers & details)
  3. Jalankan 5 pipeline re-dump DWH summary secara berurutan
  4. Kirim email report before/after summary dari semua pipeline
"""

import os
import sys
import time
import smtplib
import ssl
from email.mime.text import MIMEText

import pendulum

# This file is loaded directly by the DAG parser; `check_duplicates` and peers live
# in this same `pipelines/` directory (not the DAG root).
_PIPELINES_DIR = os.path.dirname(os.path.abspath(__file__))
if _PIPELINES_DIR not in sys.path:
    sys.path.insert(0, _PIPELINES_DIR)

_MITRA_ROOT = os.path.dirname(_PIPELINES_DIR)
if _MITRA_ROOT not in sys.path:
    sys.path.insert(0, _MITRA_ROOT)

from teams_report_iseller_mitra import (
    send_mitra_dwh_teams_report_clean,
    send_mitra_dwh_teams_report_duplicate_flow,
)

from check_duplicates import check_duplicates, generate_report_outlet_01154
from pipeline_clean_duplicates import run_clean_duplicates
from pipeline_daily_items import run_daily_items
from pipeline_daily_bundlings import run_daily_bundlings
from pipeline_daily_transactions import run_daily_transactions
from pipeline_hourly_transactions import run_hourly_transactions
from pipeline_daily_sales_type import run_daily_sales_type


def _send_redump_report_email(start_date, end_date, results, clean_stats=None):
    """Send HTML email report with DL cleaning stats and before/after summary tables."""
    th = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    # --- Table 0: DL Cleaning Summary ---
    cleaning_html = ""
    if clean_stats:
        cleaning_rows = ""
        for i, (label, key) in enumerate([
            ("transactions_iseller_mitra (headers)", "headers"),
            ("transactions_items_iseller_mitra (details)", "details"),
        ]):
            before_key = f"{key}_before"
            after_key = f"{key}_after"
            if before_key not in clean_stats:
                continue
            b = clean_stats[before_key]
            a = clean_stats[after_key]
            bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
            cleaning_rows += (
                f'<tr{bg}>'
                f'<td {td}>{label}</td>'
                f'<td {td_r}>{b:,}</td>'
                f'<td {td_r}>{a:,}</td>'
                f'<td {td_r}>{b - a:,}</td>'
                f'</tr>'
            )
        cleaning_html = f"""
        <h3 style="color:#333;">Data Lake Cleaning</h3>
        <table {table}>
            <tr>
                <th {th}>Table</th>
                <th {th}>Before</th>
                <th {th}>After</th>
                <th {th}>Removed</th>
            </tr>
            {cleaning_rows}
        </table>
        """

    # --- Table 1: Pipeline Summary ---
    summary_rows = ""
    for i, (table_name, r) in enumerate(results.items()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        if r["status"] == "SUCCESS":
            status_badge = '<span style="color:#fff;background-color:#28a745;padding:2px 8px;border-radius:4px;font-size:12px;">SUCCESS</span>'
            summary_rows += f'<tr{bg}><td {td}>{table_name}</td><td {td}>{status_badge}</td><td {td_r}>{r["deleted"]:,}</td><td {td_r}>{r["dumped"]:,}</td><td {td_r}>{r["time"]}</td></tr>'
        else:
            status_badge = '<span style="color:#fff;background-color:#dc3545;padding:2px 8px;border-radius:4px;font-size:12px;">FAILED</span>'
            summary_rows += f'<tr{bg}><td {td}>{table_name}</td><td {td}>{status_badge}</td><td {td} colspan="3">{r["error"]}</td></tr>'

    # --- Table 2: Before vs After ---
    ba_rows = ""
    row_idx = 0
    for table_name, r in results.items():
        if r["status"] != "SUCCESS":
            continue
        short_name = table_name.replace("_iseller_pusat", "")
        for col in r["before"]:
            b = r["before"][col]
            a = r["after"][col]
            diff = a - b
            sign = "+" if diff >= 0 else ""
            diff_color = "#28a745" if diff == 0 else ("#dc3545" if diff < 0 else "#ff8c00")
            bg = ' style="background-color:#f2f2f2;"' if row_idx % 2 == 0 else ""
            ba_rows += (
                f'<tr{bg}>'
                f'<td {td}>{short_name}</td>'
                f'<td {td}>{col}</td>'
                f'<td {td_r}>{b:,.0f}</td>'
                f'<td {td_r}>{a:,.0f}</td>'
                f'<td style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;color:{diff_color};font-weight:bold;">{sign}{diff:,.0f}</td>'
                f'</tr>'
            )
            row_idx += 1

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#4472C4;">Re-Dump DWH Summary - Completed</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>

        {cleaning_html}

        <h3 style="color:#333;">Pipeline Summary</h3>
        <table {table}>
            <tr>
                <th {th}>Table</th>
                <th {th}>Status</th>
                <th {th}>Deleted</th>
                <th {th}>Dumped</th>
                <th {th}>Time</th>
            </tr>
            {summary_rows}
        </table>

        <h3 style="color:#333;">Before vs After</h3>
        <table {table}>
            <tr>
                <th {th}>Table</th>
                <th {th}>Column</th>
                <th {th}>Before</th>
                <th {th}>After</th>
                <th {th}>Diff</th>
            </tr>
            {ba_rows}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[REPORT] Re-Dump DWH Summary Completed - transactions_iseller_mitra ({start_date} s/d {end_date})"

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


def _mitra_teams_meta(airflow_context):
    """dag_id, run_id, executed_at label (WIB)."""
    now_wib = pendulum.now("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ss") + " WIB"
    if not airflow_context:
        return "etl_iseller_mitra_dwh_summary", "n/a", now_wib
    return (
        airflow_context["dag"].dag_id,
        airflow_context["dag_run"].run_id,
        now_wib,
    )


def _teams_notify_safe(label: str, fn, **kwargs) -> None:
    try:
        fn(**kwargs)
        print(f"  Teams: {label} sent.")
    except Exception as e:
        print(f"  WARNING: Teams notification ({label}) failed: {e}")


def run_all(start_date, end_date, config_dw, skip_dl_check=False, airflow_context=None):
    """
    Full flow:
      1. Check duplicates → email alert if found
      2. Clean duplicated data di DL — hanya tabel yang memiliki duplikat
      3. Run DWH summary pipelines (selektif):
           - daily_items + daily_bundlings selalu jalan (keduanya JOIN headers & details)
           - daily_transactions + hourly_transactions + daily_sales_type hanya jalan
             jika headers duplikat
      4. Send email report with before/after summary

    Args:
        skip_dl_check: jika True, skip step 1 & 2 (untuk case DL sudah di-clean manual),
                       dan jalankan semua 5 pipeline.
    """
    clean_stats = None
    dup_result = None
    dag_id, run_id, exec_at = _mitra_teams_meta(airflow_context)

    # Default: assume both tables dirty (safe fallback for skip_dl_check=True)
    has_dup_headers = True
    has_dup_details = True

    if skip_dl_check:
        print("=" * 65)
        print("  SKIP Step 1 & 2 (DL sudah bersih / cleaned manual)")
        print("=" * 65)
    else:
        # STEP 1: Check duplicates + send alert email
        dup_result = check_duplicates(start_date, end_date, config_dw)

        if not dup_result["has_duplicates"]:
            _teams_notify_safe(
                "clean run",
                send_mitra_dwh_teams_report_clean,
                report_df=dup_result["report_df"],
                period_start=start_date,
                period_end=end_date,
                dag_id=dag_id,
                run_id=run_id,
                executed_at_display=exec_at,
            )
            return None

        has_dup_headers = dup_result["total_duplicated_headers"] > 0
        has_dup_details = dup_result["total_duplicated_details"] > 0

        # STEP 2: Clean only the DL tables that have duplicates
        clean_stats = run_clean_duplicates(
            start_date, end_date, config_dw,
            clean_headers=has_dup_headers,
            clean_details=has_dup_details,
        )

    # STEP 3: Run only the DWH pipelines whose source tables were modified.
    #
    # daily_items and daily_bundlings JOIN both headers + details, so they run
    # whenever either table is dirty (which is always True here since has_duplicates=True).
    # The three headers-only pipelines only run when headers are dirty.
    pipelines = [
        ("daily_items_summary_iseller_pusat", run_daily_items),
        ("daily_bundling_summary_iseller_pusat", run_daily_bundlings),
    ]
    if has_dup_headers:
        pipelines += [
            ("daily_transaction_summary_iseller_pusat", run_daily_transactions),
            ("hourly_transaction_summary_iseller_pusat", run_hourly_transactions),
            ("daily_sales_type_summary_iseller_pusat", run_daily_sales_type),
        ]

    total_pipelines = len(pipelines)
    print()
    print("=" * 65)
    print(f"  STEP 3: RE-DUMP DWH SUMMARY - iSeller Mitra  ({total_pipelines} of 5 pipelines)")
    print(f"  Period: {start_date} s/d {end_date}")
    if not has_dup_headers:
        print("  (headers bersih → skip daily_transactions, hourly_transactions, daily_sales_type)")
    print("=" * 65)

    results = {}
    total_start = time.time()

    for table_name, func in pipelines:
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

    print()
    print("=" * 65)
    print("  SUMMARY")
    print("=" * 65)
    for table_name, result in results.items():
        if result["status"] == "SUCCESS":
            print(f"  {result['status']} | {table_name}")
            print(f"           Deleted={result['deleted']}, Dumped={result['dumped']}, Time={result['time']}")
        else:
            print(f"  {result['status']} | {table_name}")
            print(f"           Error: {result['error']}")
    print(f"\n  Total time: {total_elapsed:.1f}s")
    print("=" * 65)

    # STEP 4: Send email report
    _send_redump_report_email(start_date, end_date, results, clean_stats)

    # STEP 5: Teams — same Outlet 01154 figures as email; before (at duplicate detection) vs after (post re-dump)
    if not skip_dl_check and dup_result is not None:
        report_after = generate_report_outlet_01154(start_date, end_date, config_dw)
        _teams_notify_safe(
            "duplicate flow (before + after)",
            send_mitra_dwh_teams_report_duplicate_flow,
            total_duplicated_orders=dup_result["total_duplicated_orders"],
            total_duplicated_details=dup_result["total_duplicated_details"],
            report_before=dup_result["report_df"],
            report_after=report_after,
            period_start=start_date,
            period_end=end_date,
            dag_id=dag_id,
            run_id=run_id,
            executed_at_display=exec_at,
            clean_stats=clean_stats,
        )

    return results
