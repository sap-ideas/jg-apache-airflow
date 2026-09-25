"""Master Orchestrator - Daily Estimated Income Per Store (for Landlord) ETL.

Flow:
  1. Extract  raw transactions per outlet/payment_type for one target date
     (extract.py)
  2. Transform into channel/commission columns, one row per outlet
     (transform.py)
  3. Load     atomic DELETE + INSERT for the target date (load.py)
  4. Send a single email report with row counts & timing
"""

import logging
import os
import smtplib
import ssl
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText

from pytz import timezone

from extract import extract_transactions
from transform import build_daily_income_summary
from load import load_daily_income, TARGET_TABLE

SMTP_SERVER    = "smtp.gmail.com"
SMTP_PORT      = 587
SENDER_EMAIL   = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
SMTP_PASSWORD  = os.getenv("GMAIL_SMTP_PASSWORD")


def _send_report_email(target_date, stats, error=None):
    """Send a concise HTML report covering extract / transform / load counts."""
    th    = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td    = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r  = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    if error:
        header_color = "#dc3545"
        title        = "ETL Daily Estimated Income Per Store - FAILED"
        error_html   = f"""
        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline FAILED.</strong><br>
            <code>{error}</code>
        </div>
        """
    else:
        header_color = "#4472C4"
        title        = "ETL Daily Estimated Income Per Store - Completed"
        error_html   = ""

    rows_html = ""
    for i, (label, value) in enumerate(stats.items()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        rows_html += f'<tr{bg}><td {td}>{label}</td><td {td_r}>{value}</td></tr>'

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{header_color};">{title}</h2>
        <p>Target date: <strong>{target_date}</strong></p>

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
    subject    = f"[{status_tag}] ETL Daily Estimated Income Per Store ({target_date})"

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


def run_all(config_dw, target_date_str=None):
    """Full ETL flow for daily_est_income_per_store_for_landlord.

    Args:
        config_dw:        Data Warehouse config module.
        target_date_str:  'YYYY-MM-DD' to process. Defaults to yesterday (Asia/Jakarta).
    """
    if target_date_str is None:
        yesterday = (datetime.now(timezone('Asia/Jakarta')) - timedelta(days=1)).date()
        target_date_str = yesterday.strftime('%Y-%m-%d')

    print("=" * 65)
    print(f"  ETL DAILY ESTIMATED INCOME PER STORE  |  date = {target_date_str}")
    print("=" * 65)

    total_start = time.time()

    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/3] EXTRACT")

        t0 = time.time()
        df_raw = extract_transactions(config_dw, target_date_str)
        print(f"        transactions      : {len(df_raw):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 2: TRANSFORM ----
        print("\n  [2/3] TRANSFORM")

        t0 = time.time()
        df_summary = build_daily_income_summary(df_raw)
        print(f"        daily_summary     : {len(df_summary):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 3: LOAD ----
        print("\n  [3/3] LOAD")

        t0 = time.time()
        deleted, inserted = load_daily_income(config_dw, df_summary, target_date_str)
        if df_summary.empty:
            print(f"        No rows to load — skipping insert ({TARGET_TABLE} left untouched).")
        else:
            print(
                f"        Overwrite (atomic):  -{deleted} / +{inserted} rows on {TARGET_TABLE} "
                f"for date = {target_date_str}  ({time.time() - t0:.1f}s)"
            )

        total_elapsed = time.time() - total_start
        print(f"\n  TOTAL TIME: {total_elapsed:.1f}s")
        print("=" * 65)

        stats = {
            "target_date":                      target_date_str,
            "transactions (extracted)":         f"{len(df_raw):,}",
            "daily_summary (transformed)":      f"{len(df_summary):,}",
            f"{TARGET_TABLE} (overwrite -del/+ins)": f"-{deleted:,} / +{inserted:,}",
            "total_time":                       f"{total_elapsed:.1f}s",
        }

        try:
            _send_report_email(target_date_str, stats)
        except Exception as mail_err:
            logging.warning(f"Failed to send report email: {mail_err}")

        return {
            "status":      "SUCCESS",
            "target_date": target_date_str,
            "deleted":     deleted,
            "inserted":    inserted,
            "stats":       stats,
        }

    except Exception as e:
        print(f"\n  PIPELINE FAILED: {e}")
        try:
            _send_report_email(target_date_str, {"error": str(e)}, error=str(e))
        except Exception as mail_err:
            logging.warning(f"Failed to send failure email: {mail_err}")
        raise
