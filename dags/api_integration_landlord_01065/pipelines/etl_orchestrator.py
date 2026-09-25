"""
Master Orchestrator - API Landlord Revenue Sharing ETL (Jilid 01065).

Flow:
  1. Extract  → get_access_token + extract_sales      (extract.py)
  2. Transform → transform_sales                       (transform.py)
  3. Load API  → post_to_api                           (load.py)
  4. Load DB   → dump_logs_idempotent                  (load.py)
  5. Email     → HTML summary report to the data team
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
from load import post_to_api, dump_logs_idempotent, LOG_SUMMARY_TABLE, LOG_DETAIL_TABLE

JILID = "01065"

SMTP_SERVER    = "smtp.gmail.com"
SMTP_PORT      = 587
SENDER_EMAIL   = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
SMTP_PASSWORD  = os.getenv("GMAIL_SMTP_PASSWORD")


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
        <p>Target date: <strong>{target_date}</strong> &nbsp;|&nbsp; Jilid: <strong>{JILID}</strong></p>

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

def run_all(config_dw, target_date_str: str | None = None):
    """Full ETL pipeline for API Landlord Revenue Sharing — Jilid 01065.

    Args:
        config_dw:        Data Warehouse config module (config_db_datawarehouse.py).
        target_date_str:  'YYYY-MM-DD' to process. Defaults to yesterday (Asia/Jakarta).
    """
    if target_date_str is None:
        yesterday       = (datetime.now(timezone('Asia/Jakarta')) - timedelta(days=1)).date()
        target_date_str = yesterday.strftime('%Y-%m-%d')

    print("=" * 65)
    print(f"  ETL API LANDLORD REVENUE SHARING  |  jilid={JILID}  |  date={target_date_str}")
    print("=" * 65)

    total_start = time.time()
    run_now     = datetime.now(timezone('Asia/Jakarta')).replace(tzinfo=None)

    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/4] EXTRACT")

        t0       = time.time()
        df_sales = extract_sales(config_dw, target_date_str, target_date_str)
        print(f"        extract_sales     : {len(df_sales):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 2: TRANSFORM ----
        print("\n  [2/4] TRANSFORM")

        t0         = time.time()
        final_data = transform_sales(df_sales, run_now)
        print(f"        transform_sales   : {len(final_data):>6} rows  ({time.time() - t0:.1f}s)")

        if final_data.empty:
            print("        No data to send — skipping API call and log dump.")
            total_elapsed = time.time() - total_start
            stats = {
                "target_date":      target_date_str,
                "jilid":            JILID,
                "rows_extracted":   "0",
                "rows_transformed": "0",
                "api_records_sent": "0 (no data)",
                "total_time":       f"{total_elapsed:.1f}s",
            }
            try:
                _send_report_email(target_date_str, stats)
            except Exception as mail_err:
                logging.warning(f"Failed to send report email: {mail_err}")
            return {"status": "SUCCESS_NO_DATA", "target_date": target_date_str}

        # ---- STEP 3: LOAD — POST TO API ----
        print("\n  [3/4] LOAD — POST TO API")

        t0    = time.time()
        token = get_access_token(JILID)
        print(f"        get_access_token  ({time.time() - t0:.1f}s)")

        t0                            = time.time()
        df_log_summary, df_log_detail = post_to_api(
            final_data, token, run_now,
            refresh_token_fn=lambda: get_access_token(JILID),
        )
        print(f"        log_summary       : {len(df_log_summary):>6} rows")
        print(f"        log_detail        : {len(df_log_detail):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 4: LOAD — DUMP LOGS TO DB ----
        print("\n  [4/4] LOAD — DUMP LOGS TO DB")

        t0     = time.time()
        counts = dump_logs_idempotent(config_dw, df_log_summary, df_log_detail, target_date_str)
        print(
            f"        {LOG_SUMMARY_TABLE} (idempotent): "
            f"-{counts['summary_deleted']} / +{counts['summary_inserted']} rows"
        )
        print(
            f"        {LOG_DETAIL_TABLE} (idempotent): "
            f"-{counts['detail_deleted']} / +{counts['detail_inserted']} rows  "
            f"({time.time() - t0:.1f}s)"
        )

        total_elapsed = time.time() - total_start
        print(f"\n  TOTAL TIME: {total_elapsed:.1f}s")
        print("=" * 65)

        api_success = int(df_log_summary['total_success'].sum()) if not df_log_summary.empty else 0
        api_error   = int(df_log_summary['total_error'].sum())   if not df_log_summary.empty else 0

        stats = {
            "target_date":                                   target_date_str,
            "jilid":                                         JILID,
            "rows_extracted (raw)":                          f"{len(df_sales):,}",
            "rows_transformed (by payment_type)":            f"{len(final_data):,}",
            "api_records_sent":                              f"{len(final_data):,}",
            "api_success":                                   f"{api_success:,}",
            "api_error":                                     f"{api_error:,}",
            f"{LOG_SUMMARY_TABLE} (idempotent -del/+ins)":  f"-{counts['summary_deleted']:,} / +{counts['summary_inserted']:,}",
            f"{LOG_DETAIL_TABLE} (idempotent -del/+ins)":   f"-{counts['detail_deleted']:,} / +{counts['detail_inserted']:,}",
            "total_time":                                    f"{total_elapsed:.1f}s",
        }

        try:
            _send_report_email(target_date_str, stats)
        except Exception as mail_err:
            logging.warning(f"Failed to send report email: {mail_err}")

        return {
            "status":           "SUCCESS",
            "target_date":      target_date_str,
            "api_success":      api_success,
            "api_error":        api_error,
            "summary_inserted": counts['summary_inserted'],
            "detail_inserted":  counts['detail_inserted'],
            "stats":            stats,
        }

    except Exception as e:
        print(f"\n  PIPELINE FAILED: {e}")
        try:
            _send_report_email(target_date_str, {"error": str(e)}, error=str(e))
        except Exception as mail_err:
            logging.warning(f"Failed to send failure email: {mail_err}")
        raise
