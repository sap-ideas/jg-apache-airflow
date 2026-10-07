"""
Master Orchestrator - Re-Dump All DWH Summary Tables (iSeller Mitra)

Flow:
  1. Check duplicated data di Data Lake → kirim email alert jika ada
  2. Load master bundling (jumlah slot per bundling) dari iSeller API
     Gagal (token tidak ada / expired / API error) → STOP: email + Teams + task gagal, DL tidak disentuh
  3. Clean duplicated data di DL (headers & details, combo-aware) — hanya tabel yang dobel
  4. Jalankan pipeline re-dump DWH summary secara berurutan
  5. Kirim email report before/after summary dari semua pipeline + Teams
     Ada pipeline DWH yang gagal → task di-fail setelah report terkirim
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
    send_mitra_dwh_teams_report_master_error,
)

from check_duplicates import _send_email_clean, check_duplicates, generate_report_outlet_01154
from master_bundlings import RETRYABLE_REASONS, failure_action, failure_title, load_bundling_slot_count
from pipeline_clean_duplicates import run_clean_duplicates
from pipeline_daily_items import run_daily_items
from pipeline_daily_bundlings import run_daily_bundlings
from pipeline_daily_transactions import run_daily_transactions
from pipeline_hourly_transactions import run_hourly_transactions
from pipeline_daily_sales_type import run_daily_sales_type


def _send_master_error_email(start_date, end_date, master_status, dup_result):
    """Pipeline STOP: master bundling gagal di-load dari API. DL & DWH tidak disentuh."""
    reason = master_status.get('reason')
    title = failure_title(reason)
    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#dc3545;">STOPPED: {title}</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>
        <div style="background-color:#dc3545;color:#fff;padding:12px 16px;border-radius:4px;margin-bottom:16px;">
            <strong>{title}</strong><br>{master_status.get('message', '-')}
        </div>
        <p>Duplikat terdeteksi di Data Lake
           (<strong>{dup_result['total_duplicated_headers']:,}</strong> header key,
           <strong>{dup_result['total_duplicated_details']:,}</strong> detail key non-combo),
           tapi cleaning <strong>TIDAK dijalankan</strong> (penyebab di atas).
           Data Lake &amp; DWH <strong>tidak disentuh</strong>.</p>
        <p><strong>Tindak lanjut:</strong> {failure_action(reason)}</p>
        <p style="color:#888;font-size:12px;">Reason: <code>{reason}</code> | Token source: {master_status.get('source') or '-'}</p>
        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[ALERT] ETL iSeller Mitra - STOPPED - {title} ({start_date} s/d {end_date})"

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

    print(f"\n  Master error email sent to: {', '.join(receiver_email)}")


def _stop_master_failed(start_date, end_date, master_status, dup_result, dry_run, dag_id, run_id, exec_at):
    """Kirim notifikasi (email + Teams) lalu raise: cleaning tidak boleh jalan tanpa master bundling."""
    reason = master_status.get('reason')
    title = failure_title(reason)
    message = master_status.get('message', '-')

    print()
    print("=" * 65)
    print(f"  STOP: {title}")
    print(f"  {message}")
    print("  Data Lake & DWH TIDAK disentuh.")
    print("=" * 65)

    if not dry_run:
        # notifikasi gagal tidak boleh menutupi error utamanya
        try:
            _send_master_error_email(start_date, end_date, master_status, dup_result)
        except Exception as e:
            print(f"  WARNING: master error email failed: {e}")
        _teams_notify_safe(
            "master error",
            send_mitra_dwh_teams_report_master_error,
            title=title,
            message=message,
            action=failure_action(reason),
            reason=reason or '-',
            total_duplicated_headers=dup_result["total_duplicated_headers"],
            total_duplicated_details=dup_result["total_duplicated_details"],
            period_start=start_date,
            period_end=end_date,
            dag_id=dag_id,
            run_id=run_id,
            executed_at_display=exec_at,
        )

    error = f"STOPPED - {title} [{reason}]: {message}"
    if reason in RETRYABLE_REASONS:
        # gangguan sementara (5xx / koneksi) -> biarkan Airflow retry
        raise RuntimeError(error)
    # token tidak ada / expired / response aneh -> retry tidak membantu
    raise _fail_without_retry(error)


def _dedup_summary_html(summary, th, td, table):
    """Tabel ringkasan dedup details (combo-aware) untuk email report."""
    if not summary:
        return ""
    src = summary['dup_factor_source_counts']
    match_pct = summary['combo_slot_match_pct']
    rewritten = [name for name in ('headers', 'details') if summary.get(f'{name}_rewritten')]
    rows = [
        ("Master bundling (API)", summary['master_status']),
        ("Tabel DL yang ditulis ulang", ", ".join(rewritten) or "- (tidak ada)"),
        ("Details non-combo RAW → CLEAN",
         f"{summary['details_noncombo_raw']:,} → {summary['details_noncombo_clean']:,}"),
        ("Details combo RAW → CLEAN",
         f"{summary['details_combo_raw']:,} → {summary['details_combo_clean']:,}"),
        ("Sumber dup_factor combo", " | ".join(f"{k}: {v:,}" for k, v in src.items())),
        ("Combo sesuai jumlah slot",
         f"{match_pct:.2f}% dari {summary['combo_in_master']:,}" if match_pct is not None else "-"),
        ("Bundling tidak ada di master",
         f"{summary['bundling_not_in_master']:,} bundling ({summary['combo_not_in_master']:,} order_detail, tidak dipotong)"),
        ("Slot mismatch", f"{summary['slot_mismatch_count']:,} order_detail"),
        ("needs_manual_check", f"{summary['needs_manual_check_orders']:,} order"),
        ("Folder report CSV", summary.get('report_dir') or "- (semua report kosong)"),
    ]
    zebra = ' style="background-color:#f2f2f2;"'
    body = "".join(
        f'<tr{zebra if i % 2 == 0 else ""}><td {td}>{k}</td><td {td}>{v}</td></tr>'
        for i, (k, v) in enumerate(rows)
    )
    return f"""
        <h3 style="color:#333;">Dedup Details (combo-aware)</h3>
        <table {table}>
            <tr><th {th}>Item</th><th {th}>Value</th></tr>
            {body}
        </table>
        """


def _fail_without_retry(message):
    """AirflowFailException (tanpa retry) kalau jalan di Airflow, selain itu RuntimeError."""
    try:
        from airflow.exceptions import AirflowFailException  # type: ignore
        return AirflowFailException(message)
    except ImportError:
        return RuntimeError(message)


def _send_redump_report_email(start_date, end_date, results, clean_stats=None, datalake_only=False):
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
        cleaning_html += _dedup_summary_html(clean_stats.get("dedup_summary"), th, td, table)

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

    title = "Data Lake Cleaning - Completed (run mode: datalake_only)" if datalake_only else "Re-Dump DWH Summary - Completed"
    if datalake_only:
        dwh_html = ('<p><strong>Run mode datalake_only:</strong> tabel DWH summary sengaja TIDAK disentuh. '
                    'Jalankan run_mode dwh_only untuk me-re-dump DWH dari Data Lake yang sudah bersih.</p>')
    else:
        dwh_html = f"""
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
        """

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#4472C4;">{title}</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>

        {cleaning_html}

        {dwh_html}

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[REPORT] Re-Dump DWH Summary Completed - transactions_iseller_mitra ({start_date} s/d {end_date})"
    if datalake_only:
        subject = f"[REPORT] Data Lake Cleaning Completed (datalake_only) - transactions_iseller_mitra ({start_date} s/d {end_date})"
    if any(r["status"] != "SUCCESS" for r in results.values()):
        subject = f"[FAILED PIPELINE] {subject}"

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


def run_all(start_date, end_date, config_dw, skip_dl_check=False, airflow_context=None, dry_run=False, skip_dwh=False):
    """
    Full flow:
      1. Check duplicates → email alert if found
      2. Load master bundling (jumlah slot per bundling) dari iSeller API.
         Gagal → STOP sebelum DL disentuh: email + Teams (kecuali dry_run) lalu raise.
      3. Clean duplicated data di DL (combo-aware) — hanya tabel yang memiliki duplikat
      4. Run DWH summary pipelines (selektif):
           - daily_items + daily_bundlings selalu jalan (keduanya JOIN headers & details)
           - daily_transactions + hourly_transactions + daily_sales_type hanya jalan
             jika headers duplikat
      5. Send email report with before/after summary + Teams.
         Ada pipeline DWH yang FAILED → raise setelah report terkirim (task Airflow gagal).

    Args:
        skip_dl_check: jika True, skip step 1-3 (untuk case DL sudah di-clean manual),
                       dan jalankan semua 5 pipeline.
        dry_run: jika True, jalankan step 1-3 tanpa email, tanpa DELETE/re-dump DL,
                 lalu berhenti (step 4 & 5 di-skip). Return clean_stats untuk dicek.
        skip_dwh: jika True (run_mode datalake_only), jalankan step 1-3 (DL benar-benar dibersihkan) lalu
                  berhenti: DWH tidak disentuh. Report email + Teams tetap dikirim (versi Data Lake saja).
    """
    if skip_dl_check and skip_dwh:
        raise ValueError("skip_dl_check dan skip_dwh tidak boleh True bersamaan: tidak ada yang dikerjakan.")
    clean_stats = None
    dup_result = None
    dag_id, run_id, exec_at = _mitra_teams_meta(airflow_context)

    # Default: assume both tables dirty (safe fallback for skip_dl_check=True)
    has_dup_headers = True
    has_dup_details = True

    if skip_dl_check:
        print("=" * 65)
        print("  SKIP Step 1-3 (DL sudah bersih / cleaned manual)")
        print("=" * 65)
    else:
        if dry_run:
            print("=" * 65)
            print("  DRY RUN: tanpa email, tanpa DELETE/re-dump DL, berhenti setelah STEP 3")
            print("=" * 65)

        # STEP 1: Check duplicates + send alert email
        dup_result = check_duplicates(start_date, end_date, config_dw, send_email=not dry_run)

        if not dup_result["has_duplicates"]:
            if not dry_run:
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

        # Type details tidak dikenal bisa salah dianggap non-combo (komponen combo sah ikut terhapus) -> STOP
        if dup_result["unknown_detail_types"]:
            unknown = dup_result["unknown_detail_types"]
            _stop_master_failed(
                start_date, end_date,
                {'ok': False, 'reason': 'unknown_detail_type',
                 'message': f"type details tidak dikenal di Data Lake: {unknown}", 'source': None},
                dup_result, dry_run, dag_id, run_id, exec_at,
            )

        has_dup_headers = dup_result["total_duplicated_headers"] > 0
        # key combo dobel juga membuka tabel details: boleh ditulis ulang kalau master memutuskan ada row dobel
        has_dup_details = dup_result["total_duplicated_details"] > 0 or dup_result["total_duplicated_details_combo"] > 0

        # STEP 2: Load jumlah slot per bundling dari API. Gagal (token tidak ada / expired / API error)
        # -> STOP sebelum DL disentuh: notifikasi + raise. Tidak ada fallback.
        bundling_slot_count, master_status = load_bundling_slot_count()
        if not master_status['ok']:
            _stop_master_failed(start_date, end_date, master_status, dup_result, dry_run, dag_id, run_id, exec_at)

        # STEP 3: Clean only the DL tables that have duplicates
        clean_stats = run_clean_duplicates(
            start_date, end_date, config_dw,
            clean_headers=has_dup_headers,
            clean_details=has_dup_details,
            bundling_slot_count=bundling_slot_count,
            master_status=master_status,
            dry_run=dry_run,
        )

        if dry_run:
            print("\n  DRY RUN selesai: STEP 4 (re-dump DWH) & STEP 5 (email/Teams) di-skip.")
            return clean_stats

        # Hanya key combo yang dobel dan master memutuskan semuanya sah -> tidak ada yang dibersihkan: sama dengan "aman"
        if dup_result["combo_only"] and not clean_stats["headers_rewritten"] and not clean_stats["details_rewritten"]:
            print("\n  Tidak ada row yang dibersihkan. Data AMAN. SKIP re-dump DWH.")
            _send_email_clean(start_date, end_date, dup_result["report_df"])
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

        # datalake_only: DL sudah dibersihkan di STEP 3, DWH sengaja tidak disentuh
        if skip_dwh:
            print("\n  RUN MODE datalake_only: STEP 4 (re-dump DWH) di-skip. DWH tidak disentuh.")
            _send_redump_report_email(start_date, end_date, {}, clean_stats, datalake_only=True)
            _teams_notify_safe(
                "duplicate flow datalake_only",
                send_mitra_dwh_teams_report_duplicate_flow,
                total_duplicated_orders=dup_result["total_duplicated_orders"],
                total_duplicated_details=dup_result["total_duplicated_details"],
                report_before=dup_result["report_df"],
                report_after=generate_report_outlet_01154(start_date, end_date, config_dw),
                period_start=start_date,
                period_end=end_date,
                dag_id=dag_id,
                run_id=run_id,
                executed_at_display=exec_at,
                clean_stats=clean_stats,
                datalake_only=True,
            )
            return clean_stats

    # STEP 4: Run only the DWH pipelines whose source tables were modified.
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
    print(f"  STEP 4: RE-DUMP DWH SUMMARY - iSeller Mitra  ({total_pipelines} of 5 pipelines)")
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

    # STEP 5: Send email report + Teams
    print()
    print("=" * 65)
    print("  STEP 5: REPORT")
    print("=" * 65)
    _send_redump_report_email(start_date, end_date, results, clean_stats)

    # Teams — same Outlet 01154 figures as email; before (at duplicate detection) vs after (post re-dump)
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

    # Pipeline DWH yang gagal tidak boleh membuat task terlihat sukses (report sudah terkirim di atas)
    failed = [name for name, r in results.items() if r["status"] != "SUCCESS"]
    if failed:
        message = f"{len(failed)} DWH pipeline FAILED: {', '.join(failed)}"
        dl_rewritten = bool(clean_stats) and bool(
            clean_stats.get("headers_rewritten") or clean_stats.get("details_rewritten")
        )
        if dl_rewritten:
            # DL sudah bersih -> retry akan lihat "tidak ada duplikat" dan skip re-dump DWH,
            # jadi jangan retry otomatis; re-dump ulang manual dengan skip_dl_check=True.
            raise _fail_without_retry(
                f"{message}. DL sudah dibersihkan, retry otomatis tidak akan re-dump DWH. "
                f"Jalankan ulang manual: run_all('{start_date}', '{end_date}', CONFIG_DW, skip_dl_check=True)"
            )
        raise RuntimeError(message)

    return results
