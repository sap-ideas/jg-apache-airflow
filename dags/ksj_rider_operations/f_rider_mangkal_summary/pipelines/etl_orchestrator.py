"""
Master Orchestrator - Rider Mangkal Summary ETL.

Flow:
  1. Extract: rider stationed logs, working time, raw transactions, hub mapping,
     rider profile clicks (ksj_user_click_tracker on jiwa-ksj)
  2. Transform: build mangkal sessions, then merge into the daily summary table
  3. Load (both atomic — single SQLAlchemy transaction each):
       a) IDEMPOTENT INSERT into daily_rider_mangkal_session_summary_logs
          DELETE WHERE operating_date IN (...) AND batch_label = X, then INSERT.
          Keeps batch_label so analysts can see the per-batch history per day.
          PRIMARY KEY (operating_date, rider_code, batch_label) — pre-delete
          makes Airflow retries safe (no duplicate-key crash).
       b) OVERWRITE-per-operating_date into daily_rider_mangkal_session_summary
          DELETE WHERE operating_date IN (...), then INSERT (without batch_label).
          Holds only the freshest batch per day. Use the DB column `load_data_at`
          (DEFAULT CURRENT_TIMESTAMP) to know which batch was last loaded.
  4. Send a single email report with row counts & timing
"""

import time
import os
import smtplib
import ssl
import logging
from datetime import datetime
from email.mime.text import MIMEText

from pytz import timezone
from sqlalchemy import text
import sqlalchemy.types as types

from common.db_helpers import get_engine
from extract import (
    get_rider_total_working_time,
    get_rider_stationed_logs,
    get_raw_transactions,
    get_hub_mapping,
    get_rider_profile_clicks,
)
from transform import create_mangkal_session, build_final_summary


# Latest snapshot per day (overwrite-per-operating_date behavior — only keeps the freshest batch)
LATEST_TABLE = "daily_rider_mangkal_session_summary"
# Full audit history (append-only — every 3x/day batch is preserved with its batch_label)
LOGS_TABLE = "daily_rider_mangkal_session_summary_logs"

DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"

# Dtype maps — only used by SQLAlchemy when auto-creating the table; for
# `to_sql(if_exists='append')` against an existing table they're ignored.
# Kept aligned with the actual DDL so behavior is correct if the table is ever
# recreated automatically.

# LOGS table (full audit history) — keeps batch_label
LOGS_DTYPE_MAP = {
    'operating_date': types.DATE(),
    'rider_code': types.VARCHAR(length=50),
    'batch_label': types.VARCHAR(length=8),
    'jilid': types.VARCHAR(length=20),
    'hub_name': types.VARCHAR(length=255),
    'business_unit': types.VARCHAR(length=50),
    'region': types.VARCHAR(length=100),
    'kota': types.VARCHAR(length=100),
    'provinsi': types.VARCHAR(length=100),
    'load_data_by': types.VARCHAR(length=100),
    'total_click_by_user': types.BigInteger(),
}

# LATEST table (overwrite-per-day snapshot) — drops batch_label
# Use `load_data_at` (DB DEFAULT CURRENT_TIMESTAMP) to know which batch was last loaded.
LATEST_DTYPE_MAP = {k: v for k, v in LOGS_DTYPE_MAP.items() if k != 'batch_label'}


def _overwrite_for_dates(engine, df, table_name, operating_dates, dtype_map):
    """Atomic DELETE + INSERT for the given operating_date(s).

    Both operations run in a single SQLAlchemy transaction — if INSERT fails,
    the DELETE is rolled back, so the table is never left in an empty state
    for the affected dates.

    Args:
        engine: SQLAlchemy engine
        df: DataFrame to insert
        table_name: target MySQL table name
        operating_dates: iterable of date strings 'YYYY-MM-DD'
        dtype_map: SQLAlchemy dtype mapping for to_sql

    Returns:
        (deleted_rowcount, inserted_rowcount)
    """
    if not operating_dates or df.empty:
        return 0, 0

    in_clause = ", ".join([f"'{d}'" for d in operating_dates])
    with engine.begin() as conn:
        result = conn.execute(text(
            f"DELETE FROM {table_name} WHERE operating_date IN ({in_clause})"
        ))
        deleted = result.rowcount
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            dtype=dtype_map,
        )
    return deleted, len(df)


def _idempotent_insert_logs(engine, df, table_name, operating_dates, batch_label, dtype_map):
    """Atomic pre-DELETE + INSERT for the LOGS table — idempotent on retry.

    The logs table has PRIMARY KEY (operating_date, rider_code, batch_label).
    A vanilla `to_sql(append)` would crash with duplicate-key error if the same
    batch is replayed (e.g. Airflow auto-retry). To keep the pipeline replayable,
    we DELETE rows for the same (operating_date IN [...], batch_label = X)
    combination before inserting, all inside one transaction.

    Other batches' rows for the same date(s) are NOT touched — only this batch's
    previous attempt (if any) is replaced.

    Returns:
        (deleted_rowcount, inserted_rowcount)
    """
    if not operating_dates or df.empty:
        return 0, 0

    in_clause = ", ".join([f"'{d}'" for d in operating_dates])
    with engine.begin() as conn:
        result = conn.execute(text(
            f"DELETE FROM {table_name} "
            f"WHERE operating_date IN ({in_clause}) AND batch_label = :bl"
        ), {"bl": batch_label})
        deleted = result.rowcount
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            dtype=dtype_map,
        )
    return deleted, len(df)


def _resolve_batch_label():
    """Return one of '08AM' / '11AM' / '03PM' based on the current Jakarta hour.

    Mirrors the notebook logic so each 3x/day run tags its rows with the batch.
    """
    current_hour_wib = datetime.now(timezone('Asia/Jakarta')).hour
    if current_hour_wib < 11:
        return '08AM'
    if current_hour_wib < 15:
        return '11AM'
    return '03PM'


def _send_report_email(operating_date, stats, error=None):
    """Send a concise HTML report covering extract / transform / load counts."""
    th = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    if error:
        header_color = "#dc3545"
        title = "ETL Rider Mangkal Summary - FAILED"
        error_html = f"""
        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline FAILED.</strong><br>
            <code>{error}</code>
        </div>
        """
    else:
        header_color = "#4472C4"
        title = "ETL Rider Mangkal Summary - Completed"
        error_html = ""

    rows_html = ""
    for i, (label, value) in enumerate(stats.items()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        rows_html += f'<tr{bg}><td {td}>{label}</td><td {td_r}>{value}</td></tr>'

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{header_color};">{title}</h2>
        <p>Operating date: <strong>{operating_date}</strong></p>

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
    subject = f"[{status_tag}] ETL Rider Mangkal Summary ({operating_date})"

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


def run_all(config_ksj, config_dw, load_data_by=DEFAULT_LOAD_DATA_BY, batch_label=None):
    """Full ETL flow for the rider mangkal daily summary table.

    All extract queries are scoped to "today" (Asia/Jakarta) inside the
    SQL itself, so we don't pass a date range from Airflow.

    Args:
        batch_label: optional override ('08AM'/'11AM'/'03PM').
                     When None, derived from the current Jakarta wall-clock hour
                     (matches the 3x/day schedule).
    """
    if batch_label is None:
        batch_label = _resolve_batch_label()

    print("=" * 65)
    print(f"  ETL RIDER MANGKAL SUMMARY  |  batch_label = {batch_label}")
    print("=" * 65)

    total_start = time.time()
    operating_date = time.strftime("%Y-%m-%d")  # Just for the report header

    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/3] EXTRACT")

        t0 = time.time()
        df_stationed_logs = get_rider_stationed_logs(config_ksj)
        print(f"        rider_stationed_logs    : {len(df_stationed_logs):>6} rows  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_working_time = get_rider_total_working_time(config_ksj)
        print(f"        rider_total_working_time: {len(df_working_time):>6} rows  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_raw_transactions = get_raw_transactions(config_ksj)
        print(f"        raw_transactions        : {len(df_raw_transactions):>6} rows  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_hub_mapping = get_hub_mapping(config_ksj)
        print(f"        hub_mapping             : {len(df_hub_mapping):>6} rows  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_profile_clicks = get_rider_profile_clicks(config_ksj)
        print(f"        rider_profile_clicks    : {len(df_profile_clicks):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 2: TRANSFORM ----
        print("\n  [2/3] TRANSFORM")

        t0 = time.time()
        _, df_session_summary = create_mangkal_session(df_stationed_logs)
        print(f"        mangkal_sessions        : {len(df_session_summary):>6} sessions  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_final_summary = build_final_summary(
            df_session_summary=df_session_summary,
            df_rider_total_working_time=df_working_time,
            df_raw_transactions=df_raw_transactions,
            df_hub_mapping=df_hub_mapping,
            df_profile_clicks=df_profile_clicks,
            load_data_by=load_data_by,
            batch_label=batch_label,
        )
        print(f"        final_summary           : {len(df_final_summary):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 3: LOAD ----
        print("\n  [3/3] LOAD")

        logs_deleted = 0
        logs_dumped = 0
        latest_deleted = 0
        latest_dumped = 0

        if df_final_summary.empty:
            print(f"        No rows to load — skipping inserts (both tables left untouched).")
        else:
            engine = get_engine(config_dw)
            unique_dates = sorted(
                d for d in df_final_summary['operating_date'].dropna().unique() if d
            )

            # --- 3a) IDEMPOTENT INSERT into *_logs (audit history) ---
            # Pre-DELETE same (operating_date, batch_label) then INSERT — atomic.
            # Allows Airflow retry of the same batch without duplicate-PK crash.
            t0 = time.time()
            logs_deleted, logs_dumped = _idempotent_insert_logs(
                engine, df_final_summary, LOGS_TABLE, unique_dates, batch_label, LOGS_DTYPE_MAP,
            )
            print(
                f"        Logs (idempotent):   -{logs_deleted} / +{logs_dumped} rows on {LOGS_TABLE} "
                f"for (date IN {unique_dates}, batch={batch_label})  ({time.time() - t0:.1f}s)"
            )

            # --- 3b) OVERWRITE-per-operating_date on main table (atomic) ---
            # DELETE all rows for today's operating_date (regardless of batch) + INSERT.
            # We strip batch_label from the DataFrame because the LATEST table doesn't
            # store it — analysts use load_data_at to know when the row was refreshed.
            df_for_latest = df_final_summary.drop(columns=['batch_label'], errors='ignore')

            t0 = time.time()
            latest_deleted, latest_dumped = _overwrite_for_dates(
                engine, df_for_latest, LATEST_TABLE, unique_dates, LATEST_DTYPE_MAP,
            )
            print(
                f"        Overwrite (atomic):  -{latest_deleted} / +{latest_dumped} rows on {LATEST_TABLE} "
                f"for operating_date IN {unique_dates}  ({time.time() - t0:.1f}s)"
            )

        total_elapsed = time.time() - total_start
        print(f"\n  TOTAL TIME: {total_elapsed:.1f}s")
        print("=" * 65)

        stats = {
            "batch_label": batch_label,
            "rider_stationed_logs (extracted)": f"{len(df_stationed_logs):,}",
            "rider_working_time (extracted)": f"{len(df_working_time):,}",
            "raw_transactions (extracted)": f"{len(df_raw_transactions):,}",
            "hub_mapping (extracted)": f"{len(df_hub_mapping):,}",
            "profile_click_rows (extracted)": f"{len(df_profile_clicks):,}",
            "mangkal_sessions (built)": f"{len(df_session_summary):,}",
            f"{LOGS_TABLE} (idempotent: -del/+ins)": f"-{logs_deleted:,} / +{logs_dumped:,}",
            f"{LATEST_TABLE} (overwrite: -del/+ins)": f"-{latest_deleted:,} / +{latest_dumped:,}",
            "total_time": f"{total_elapsed:.1f}s",
        }

        try:
            _send_report_email(operating_date, stats)
        except Exception as mail_err:
            logging.warning(f"Failed to send report email: {mail_err}")

        return {
            "status": "SUCCESS",
            "logs_deleted": logs_deleted,
            "logs_dumped": logs_dumped,
            "latest_deleted": latest_deleted,
            "latest_dumped": latest_dumped,
            "stats": stats,
        }

    except Exception as e:
        print(f"\n  PIPELINE FAILED: {e}")
        try:
            _send_report_email(operating_date, {"error": str(e)}, error=str(e))
        except Exception as mail_err:
            logging.warning(f"Failed to send failure email: {mail_err}")
        raise
