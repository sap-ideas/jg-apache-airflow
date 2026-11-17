"""Master Orchestrator - Hourly Rider Performance (TI-TO) ETL.

Flow:
  1. Extract: hourly transactions, TI-TO time-slots, rider-hub mapping
  2. Transform: handle 3 data-quality cases, merge into the final DataFrame
  3. Load (atomic — single SQLAlchemy transaction):
       OVERWRITE-per-date: DELETE WHERE date = target_date, then INSERT.
       Airflow retries are safe — a re-run simply re-deletes then re-inserts.
  4. Send a single email report with row counts & timing
"""

import logging
import math
import os
import smtplib
import ssl
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText

from pytz import timezone
from sqlalchemy import text
import sqlalchemy.types as types

from common.db_helpers import get_engine
from extract import (
    get_rider_hourly_transactions,
    get_rider_timeslot_tito,
    get_rider_mapping,
)
from transform import build_hourly_performance


TARGET_TABLE = "hourly_sejutajiwa_rider_performance"
DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"
BATCH_SIZE = 50_000

DTYPE_MAP = {
    'rider_id':          types.VARCHAR(length=50),
    'jilid':             types.VARCHAR(length=20),
    'hub_name':          types.VARCHAR(length=255),
    'region':            types.VARCHAR(length=100),
    'provinsi':          types.VARCHAR(length=100),
    'kota':              types.VARCHAR(length=100),
    'sejuta_jiwa_type':  types.VARCHAR(length=50),
    'date':              types.DATE(),
    'hour_slot':         types.Integer(),
    'week':              types.Integer(),
    'hour':              types.Integer(),
    'payment_type':      types.VARCHAR(length=50),
    'total_orders':      types.BigInteger(),
    'total_qty':         types.BigInteger(),
    'total_sales':       types.DECIMAL(precision=18, scale=2),
    'load_data_by':      types.VARCHAR(length=100),
}


def _overwrite_for_date(engine, df, target_date_str):
    """Atomic DELETE + batched INSERT for the given date.

    Both operations run inside a single SQLAlchemy transaction — if any INSERT
    batch fails, the DELETE is rolled back so the table is never left empty for
    that date.

    Returns:
        (deleted_rowcount, inserted_rowcount)
    """
    if df.empty:
        return 0, 0

    total_batches = math.ceil(len(df) / BATCH_SIZE)

    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {TARGET_TABLE} WHERE date = :d"),
            {"d": target_date_str},
        )
        deleted = result.rowcount

        for i in range(total_batches):
            batch = df.iloc[i * BATCH_SIZE: (i + 1) * BATCH_SIZE]
            batch.to_sql(
                name=TARGET_TABLE,
                con=conn,
                if_exists="append",
                index=False,
                method="multi",
                dtype=DTYPE_MAP,
            )
            print(
                f"        batch {i + 1}/{total_batches} inserted "
                f"({min((i + 1) * BATCH_SIZE, len(df))}/{len(df)} rows)"
            )

    return deleted, len(df)


def _send_report_email(target_date, stats, error=None):
    """Send a concise HTML report covering extract / transform / load counts."""
    th = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    if error:
        header_color = "#dc3545"
        title = "ETL Hourly Rider Performance (TI-TO) - FAILED"
        error_html = f"""
        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline FAILED.</strong><br>
            <code>{error}</code>
        </div>
        """
    else:
        header_color = "#4472C4"
        title = "ETL Hourly Rider Performance (TI-TO) - Completed"
        error_html = ""

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
    subject = f"[{status_tag}] ETL Hourly Rider Performance TI-TO ({target_date})"

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print(f"\n  Report email sent to: {', '.join(receiver_email)}")


def run_all(config_ksj, config_dw, target_date_str=None, load_data_by=DEFAULT_LOAD_DATA_BY):
    """Full ETL flow for the hourly rider performance table.

    Args:
        config_ksj:       KSJ Link config module
        config_dw:        DWH config module
        target_date_str:  'YYYY-MM-DD' to process. Defaults to yesterday (Asia/Jakarta).
        load_data_by:     tag written to the load_data_by column.
    """
    if target_date_str is None:
        yesterday = (datetime.now(timezone('Asia/Jakarta')) - timedelta(days=1)).date()
        target_date_str = yesterday.strftime('%Y-%m-%d')

    print("=" * 65)
    print(f"  ETL HOURLY RIDER PERFORMANCE (TI-TO)  |  date = {target_date_str}")
    print("=" * 65)

    total_start = time.time()

    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/3] EXTRACT")

        t0 = time.time()
        df_hourly_transactions = get_rider_hourly_transactions(
            config_ksj, target_date_str, target_date_str
        )
        print(f"        hourly_transactions : {len(df_hourly_transactions):>6} rows  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_rider_timeslot = get_rider_timeslot_tito(
            config_ksj, target_date_str, target_date_str
        )
        print(f"        rider_timeslot_tito : {len(df_rider_timeslot):>6} rows  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_rider_mapping = get_rider_mapping(config_dw)
        print(f"        rider_mapping       : {len(df_rider_mapping):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 2: TRANSFORM ----
        print("\n  [2/3] TRANSFORM")

        t0 = time.time()
        df_final = build_hourly_performance(
            df_hourly_transactions=df_hourly_transactions,
            df_rider_timeslot=df_rider_timeslot,
            df_rider_mapping=df_rider_mapping,
            load_data_by=load_data_by,
        )
        print(f"        final_df            : {len(df_final):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 3: LOAD ----
        print("\n  [3/3] LOAD")

        deleted = 0
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
            "target_date":                        target_date_str,
            "hourly_transactions (extracted)":    f"{len(df_hourly_transactions):,}",
            "rider_timeslot_tito (extracted)":    f"{len(df_rider_timeslot):,}",
            "rider_mapping (extracted)":          f"{len(df_rider_mapping):,}",
            "final_df (transformed)":             f"{len(df_final):,}",
            f"{TARGET_TABLE} (overwrite -del/+ins)": f"-{deleted:,} / +{inserted:,}",
            "total_time":                         f"{total_elapsed:.1f}s",
        }

        try:
            _send_report_email(target_date_str, stats)
        except Exception as mail_err:
            logging.warning(f"Failed to send report email: {mail_err}")

        return {
            "status": "SUCCESS",
            "target_date": target_date_str,
            "deleted": deleted,
            "inserted": inserted,
            "stats": stats,
        }

    except Exception as e:
        print(f"\n  PIPELINE FAILED: {e}")
        try:
            _send_report_email(target_date_str, {"error": str(e)}, error=str(e))
        except Exception as mail_err:
            logging.warning(f"Failed to send failure email: {mail_err}")
        raise
