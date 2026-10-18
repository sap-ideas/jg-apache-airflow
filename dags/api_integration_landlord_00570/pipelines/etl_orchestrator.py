"""
Master Orchestrator - API Landlord Revenue Sharing ETL (Jilid 00570).

Flow:
  1. Extract  → get_access_token + extract_sales      (extract.py)
  2. Transform → transform_sales                       (transform.py)
  3. Load API  → post_to_api, satu POST per tanggal    (load.py)
  4. Load DB   → dump_logs per tanggal (append, tanpa DELETE) (load.py)
  5. Email     → satu HTML summary report per run (satu baris per tanggal)

Rentang tanggal: start_date..end_date (maks 31 hari); run terjadwal = satu hari.
"""

import logging
import os
import smtplib
import ssl
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText

from pytz import timezone

from extract import get_access_token, extract_sales
from transform import transform_sales
from load import (
    post_to_api, dump_logs, get_sent_payment_types,
    LOG_SUMMARY_TABLE, LOG_DETAIL_TABLE, DEFAULT_LOAD_DATA_BY,
)

JILID = "00570"

SMTP_SERVER    = "smtp.gmail.com"
SMTP_PORT      = 587
SENDER_EMAIL   = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
SMTP_PASSWORD  = os.getenv("GMAIL_SMTP_PASSWORD")


class PostAttemptedError(Exception):
    """Error setelah POST ke API dijalankan. Jangan di-retry otomatis: retry = POST ulang."""


# ---------------------------------------------------------------------------
# Email report
# ---------------------------------------------------------------------------

def _send_report_email(target_date: str, stats: dict, error: str | None = None):
    """Send an HTML pipeline summary email to the data team."""
    th    = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td    = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r  = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    if error:
        header_color = "#dc3545"
        title        = f"API Landlord Revenue Sharing ({JILID}) - FAILED"
        error_html   = f"""
        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline FAILED.</strong><br>
            <code>{error}</code>
        </div>
        """
    else:
        header_color = "#4472C4"
        title        = f"API Landlord Revenue Sharing ({JILID}) - Completed"
        error_html   = ""

    rows_html = ""
    for i, (label, value) in enumerate(stats.items()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        rows_html += f'<tr{bg}><td {td}>{label}</td><td {td_r}>{value}</td></tr>'

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{header_color};">{title}</h2>
        <p>Date: <strong>{target_date}</strong> &nbsp;|&nbsp; Jilid: <strong>{JILID}</strong></p>

        {error_html}

        <h3 style="color:#333;">Pipeline Stats</h3>
        <table {table}>
            <tr><th {th}>Metric</th><th {th}>Value</th></tr>
            {rows_html}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    status_tag = "FAILED" if error else "REPORT"
    subject    = f"[{status_tag}] API Landlord Revenue Sharing {JILID} ({target_date})"

    msg            = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From']    = SENDER_EMAIL
    msg['To']      = ", ".join(RECEIVER_EMAIL)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
        server.login(SENDER_EMAIL, SMTP_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())

    print(f"\n  Report email sent to: {', '.join(RECEIVER_EMAIL)}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

MAX_RANGE_DAYS = 31


def _date_range(start_date_str: str, end_date_str: str) -> list[str]:
    """Daftar tanggal 'YYYY-MM-DD' dari start sampai end (inklusif). ValueError kalau terbalik / terlalu panjang."""
    start = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end   = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    if start > end:
        raise ValueError(f"start_date {start} lebih besar dari end_date {end}")
    n_days = (end - start).days + 1
    if n_days > MAX_RANGE_DAYS:
        raise ValueError(f"Rentang {n_days} hari melebihi batas {MAX_RANGE_DAYS} hari")
    return [(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(n_days)]


def run_all(config_dw, start_date_str: str | None = None, end_date_str: str | None = None,
            force_resend: bool = False, load_data_by: str = DEFAULT_LOAD_DATA_BY):
    """Full ETL pipeline for API Landlord Revenue Sharing — Jilid 00570.

    Satu POST + satu baris log summary per tanggal. Kalau satu tanggal gagal, tanggal berikutnya
    tetap diproses; kegagalan dilaporkan di akhir (status API_ERROR, `failed_dates`).

    Args:
        config_dw:        Data Warehouse config module (config_db_datawarehouse.py).
        start_date_str:   'YYYY-MM-DD' awal rentang. Default kemarin (Asia/Jakarta).
        end_date_str:     'YYYY-MM-DD' akhir rentang (inklusif). Default = start_date_str.
                          Maksimal MAX_RANGE_DAYS hari.
        force_resend:     False = payment type yang sudah ada di mall (lihat get_sent_payment_types)
                          tidak dikirim ulang, dicek per tanggal. True = kirim semua.
        load_data_by:     tag untuk kolom load_data_by di kedua tabel log. Default NEXUS_AIRFLOW;
                          DAG mengisinya dengan operator saat trigger manual.

    Status per tanggal: SUCCESS, NO_DATA, SKIPPED_ALREADY_SENT (semua sudah ada di mall),
    API_ERROR (API menolak/timeout, log tersimpan), FAILED (exception).
    Status return: API_ERROR kalau ada tanggal gagal, selain itu SUCCESS / SKIPPED_ALREADY_SENT /
    SUCCESS_NO_DATA. Error sebelum ada POST (extract/transform) di-raise apa adanya; error setelah
    POST di luar loop tanggal -> PostAttemptedError.
    """
    if start_date_str is None:
        yesterday      = (datetime.now(timezone('Asia/Jakarta')) - timedelta(days=1)).date()
        start_date_str = yesterday.strftime('%Y-%m-%d')
    if end_date_str is None:
        end_date_str = start_date_str

    dates       = _date_range(start_date_str, end_date_str)
    range_label = start_date_str if len(dates) == 1 else f"{start_date_str} s/d {end_date_str}"

    print("=" * 65)
    print(f"  ETL API LANDLORD REVENUE SHARING  |  jilid={JILID}  |  date={range_label}  ({len(dates)} hari)")
    print("=" * 65)

    total_start = time.time()
    run_now     = datetime.now(timezone('Asia/Jakarta')).replace(tzinfo=None)

    post_attempted = False
    results        = {}
    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/4] EXTRACT")

        t0       = time.time()
        df_sales = extract_sales(config_dw, start_date_str, end_date_str)
        print(f"        extract_sales     : {len(df_sales):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 2: TRANSFORM ----
        print("\n  [2/4] TRANSFORM")

        t0         = time.time()
        final_data = transform_sales(df_sales, run_now)
        print(f"        transform_sales   : {len(final_data):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 3+4: LOAD (POST TO API + DUMP LOGS), per tanggal ----
        print("\n  [3-4/4] LOAD — POST TO API + DUMP LOGS (per tanggal)")

        token = None
        for d in dates:
            print(f"\n        ---- {d} ----")
            df_d = final_data[final_data['TransactionDate'].astype(str) == d].reset_index(drop=True)
            if df_d.empty:
                print("        No data — skipping API call and log dump.")
                results[d] = {"status": "NO_DATA"}
                continue

            posted = False
            sent   = set()
            try:
                if not force_resend:
                    sent = get_sent_payment_types(config_dw, d)
                    if sent:
                        df_d = df_d[~df_d['PaymentType'].isin(sent)].reset_index(drop=True)
                        print(f"        sudah ada di mall, tidak dikirim ulang: {sorted(sent)}")
                    if df_d.empty:
                        print("        Semua payment type sudah ada di mall. Tidak ada POST.")
                        results[d] = {"status": "SKIPPED_ALREADY_SENT", "skipped": sorted(sent)}
                        continue

                if token is None:
                    token = get_access_token(JILID)

                posted                        = True
                post_attempted                = True
                df_log_summary, df_log_detail = post_to_api(
                    df_d, token, run_now,
                    refresh_token_fn=lambda: get_access_token(JILID),
                    load_data_by=load_data_by,
                )
                # tetap terlihat di log task walaupun insert log ke DB gagal
                print(df_log_detail[['payment_type', 'transaction_number', 'status', 'mall_transaction_id', 'error_message']].to_string(index=False))

                counts = dump_logs(config_dw, df_log_summary, df_log_detail)
                print(f"        {LOG_SUMMARY_TABLE} (append): +{counts['summary_inserted']} rows")
                print(f"        {LOG_DETAIL_TABLE} (append): +{counts['detail_inserted']} rows")

                # dihitung per record (bukan TotalError dari API); 'duplicate' = sudah ada di mall, bukan gagal
                is_duplicate  = df_log_detail['error_message'].astype(str).str.strip().eq('duplicate')
                api_success   = int((df_log_detail['status'] == 'success').sum())
                api_duplicate = int(is_duplicate.sum())
                api_error     = len(df_log_detail) - api_success - api_duplicate
                results[d] = {
                    "status":        "API_ERROR" if api_error > 0 else "SUCCESS",
                    "records_sent":  len(df_d),
                    "api_success":   api_success,
                    "api_duplicate": api_duplicate,
                    "api_error":     api_error,
                    "skipped":       sorted(sent),
                }
            except Exception as date_err:
                msg = str(date_err)
                if posted:
                    msg += f" | POST ke API sudah dijalankan: cek {LOG_DETAIL_TABLE} sebelum kirim ulang."
                print(f"        FAILED {d}: {msg}")
                results[d] = {"status": "FAILED", "error": msg}

        total_elapsed = time.time() - total_start
        print(f"\n  TOTAL TIME: {total_elapsed:.1f}s")
        print("=" * 65)

        failed_dates = [d for d, r in results.items() if r["status"] in ("API_ERROR", "FAILED")]
        sent_dates   = [r for r in results.values() if r["status"] in ("SUCCESS", "API_ERROR")]
        api_error    = sum(r.get("api_error", 0) for r in results.values())

        stats = {
            "range":                       range_label,
            "jilid":                       JILID,
            "rows_extracted (raw)":        f"{len(df_sales):,}",
            "api_records_sent":            f"{sum(r['records_sent'] for r in sent_dates):,}",
            "api_success":                 f"{sum(r.get('api_success', 0) for r in sent_dates):,}",
            "api_duplicate (sudah ada di mall)": f"{sum(r.get('api_duplicate', 0) for r in sent_dates):,}",
            "api_error":                   f"{api_error:,}",
            "total_time":                  f"{total_elapsed:.1f}s",
        }
        for d, r in results.items():
            if r["status"] in ("SUCCESS", "API_ERROR"):
                detail = (f"{r['status']} | sent {r['records_sent']} | success {r['api_success']} | "
                          f"duplicate {r['api_duplicate']} | error {r['api_error']}")
                if r["skipped"]:
                    detail += f" | skipped (sudah ada di mall): {', '.join(r['skipped'])}"
            elif r["status"] == "SKIPPED_ALREADY_SENT":
                detail = f"SKIPPED_ALREADY_SENT | {', '.join(r['skipped'])}"
            elif r["status"] == "FAILED":
                detail = f"FAILED | {r['error']}"
            else:
                detail = "NO_DATA"
            stats[d] = detail

        error_msg = (
            f"Gagal untuk tanggal: {', '.join(failed_dates)} ({api_error:,} record API error). "
            f"Detail di {LOG_DETAIL_TABLE} (status bukan success). Kirim ulang lewat trigger manual DAG: "
            "hanya yang belum ada di mall yang dikirim."
        ) if failed_dates else None

        try:
            _send_report_email(range_label, stats, error=error_msg)
        except Exception as mail_err:
            logging.warning(f"Failed to send report email: {mail_err}")

        if failed_dates:
            status = "API_ERROR"
        elif sent_dates:
            status = "SUCCESS"
        elif any(r["status"] == "SKIPPED_ALREADY_SENT" for r in results.values()):
            status = "SKIPPED_ALREADY_SENT"
        else:
            status = "SUCCESS_NO_DATA"

        return {
            "status":       status,
            "start_date":   start_date_str,
            "end_date":     end_date_str,
            "dates":        results,
            "failed_dates": failed_dates,
            "api_error":    api_error,
            "stats":        stats,
        }

    except Exception as e:
        error = str(e)
        if post_attempted:
            error += f" | POST ke API sudah dijalankan: cek log task / {LOG_DETAIL_TABLE} sebelum kirim ulang."
        print(f"\n  PIPELINE FAILED: {error}")
        try:
            _send_report_email(range_label, {"error": error}, error=error)
        except Exception as mail_err:
            logging.warning(f"Failed to send failure email: {mail_err}")
        if post_attempted:
            raise PostAttemptedError(error) from e
        raise
