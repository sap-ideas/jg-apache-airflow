"""Master Orchestrator - Daily KSJ Consumer App User Session ETL.

Flow:
  1. Extract: JiwaPlus app sessions + KSJ app sessions (rider-found, click tracker)
  2. Transform: outer-join on date, cast numeric columns
  3. Load (atomic — single SQLAlchemy transaction):
       OVERWRITE-per-date: DELETE WHERE date = target_date, then INSERT.
       Airflow retries are safe — a re-run simply re-deletes then re-inserts.
  4. Send a single email report with row counts & timing
"""

import logging
import os
import smtplib
import ssl
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText

import sqlalchemy.types as types
from pytz import timezone
from sqlalchemy import text

from common.db_helpers import get_engine
from extract import (
    get_jiwaplus_consumer_app_user_session,
    get_ksj_consumer_app_user_session,
)
from transform import build_daily_session_combined


TARGET_TABLE = "daily_ksj_consumer_app_user_session"
DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"

DTYPE_MAP = {
    'date':                       types.DATE(),
    'total_session':              types.BigInteger(),
    'total_users_open_jiwaplus':  types.BigInteger(),
    'total_users_open_ksj':       types.BigInteger(),
    'total_rider_found':          types.BigInteger(),
    'total_rider_not_found':      types.BigInteger(),
    'rider_found_perc':           types.DECIMAL(precision=20, scale=10),
    'total_click':                types.BigInteger(),
    'total_click_direction':      types.BigInteger(),
    'total_wa_direction':         types.BigInteger(),
    'click_ratio_to_found_rider': types.DECIMAL(precision=20, scale=10),
    'load_data_by':               types.VARCHAR(length=100),
}


def _overwrite_for_date(engine, df, target_date_str):
    """Atomic DELETE + INSERT for the given date.

    Both operations run inside a single SQLAlchemy transaction — if INSERT
    fails, the DELETE is rolled back so the table is never left empty for
    that date.

    Returns:
        (deleted_rowcount, inserted_rowcount)
    """
    if df.empty:
        return 0, 0

    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {TARGET_TABLE} WHERE date = :d"),
            {"d": target_date_str},
        )
        deleted = result.rowcount

        df.to_sql(
            name=TARGET_TABLE,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            dtype=DTYPE_MAP,
        )

    return deleted, len(df)


def _send_report_email(target_date, stats, error=None):
    """Send a concise HTML report covering extract / transform / load counts."""
    th    = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td    = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r  = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    if error:
        header_color = "#dc3545"
        title        = "ETL Daily KSJ Consumer App User Session - FAILED"
        error_html   = f"""
        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline FAILED.</strong><br>
            <code>{error}</code>
        </div>
        """
    else:
        header_color = "#4472C4"
        title        = "ETL Daily KSJ Consumer App User Session - Completed"
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
    subject    = f"[{status_tag}] ETL Daily KSJ Consumer App User Session ({target_date})"

    port        = 587
    smtp_server = "smtp.gmail.com"
    sender_email    = "saputra.christabel20@gmail.com"
    receiver_email  = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password        = os.getenv("GMAIL_SMTP_PASSWORD")

    msg            = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From']    = sender_email
    msg['To']      = ", ".join(receiver_email)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print(f"\n  Report email sent to: {', '.join(receiver_email)}")


def run_all(config_jiwaplus, config_ksj, config_dw, target_date_str=None, load_data_by=DEFAULT_LOAD_DATA_BY):
    """Full ETL flow for the daily KSJ consumer app user session table.

    Args:
        config_jiwaplus:  JiwaPlus config module
        config_ksj:       KSJ Link config module
        config_dw:        DWH config module
        target_date_str:  'YYYY-MM-DD' to process. Defaults to yesterday (Asia/Jakarta).
        load_data_by:     tag written to the load_data_by column.
    """
    if target_date_str is None:
        yesterday = (datetime.now(timezone('Asia/Jakarta')) - timedelta(days=1)).date()
        target_date_str = yesterday.strftime('%Y-%m-%d')

    print("=" * 65)
    print(f"  ETL DAILY KSJ CONSUMER APP USER SESSION  |  date = {target_date_str}")
    print("=" * 65)

    total_start = time.time()

    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/3] EXTRACT")

        t0 = time.time()
        df_jiwaplus_session = get_jiwaplus_consumer_app_user_session(
            config_jiwaplus, target_date_str, target_date_str
        )
        print(f"        jiwaplus_session : {len(df_jiwaplus_session):>6} rows  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_ksj_session = get_ksj_consumer_app_user_session(
            config_ksj, target_date_str, target_date_str
        )
        print(f"        ksj_session      : {len(df_ksj_session):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 2: TRANSFORM ----
        print("\n  [2/3] TRANSFORM")

        t0 = time.time()
        df_final = build_daily_session_combined(
            df_jiwaplus_session=df_jiwaplus_session,
            df_ksj_session=df_ksj_session,
            load_data_by=load_data_by,
        )
        print(f"        final_df         : {len(df_final):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 3: LOAD ----
        print("\n  [3/3] LOAD")

        deleted  = 0
        inserted = 0

        if df_final.empty:
            print(f"        No rows to load — skipping insert ({TARGET_TABLE} left untouched).")
        else:
            engine = get_engine(config_dw)
            t0 = time.time()
            deleted, inserted = _overwrite_for_date(engine, df_final, target_date_str)
            print(
                f"        Overwrite (atomic):  -{deleted} / +{inserted} rows on {TARGET_TABLE} "
                f"for date = {target_date_str}  ({time.time() - t0:.1f}s)"
            )

        total_elapsed = time.time() - total_start
        print(f"\n  TOTAL TIME: {total_elapsed:.1f}s")
        print("=" * 65)

        stats = {
            "target_date":                      target_date_str,
            "jiwaplus_session (extracted)":     f"{len(df_jiwaplus_session):,}",
            "ksj_session (extracted)":          f"{len(df_ksj_session):,}",
            "final_df (transformed)":           f"{len(df_final):,}",
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
