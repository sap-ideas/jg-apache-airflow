"""Master Orchestrator - KSJ On-Demand Order Distance Summary ETL.

Flow:
  1. Extract: pending orders, blast metrics/rounds, accept context, nearest blasted
     rider, delivery detail (KSJ), plus jiwaplus transaction detail, blasted rider
     codes, per-rider blast positions and the markassajiwa voucher holders
  2. Transform: OSRM route distances per blasted rider, then merge everything into
     the per-order summary
  3. Load: OVERWRITE-per-order_date into ksj_on_demand_order_summary
     (DELETE WHERE order_date IN (...) then INSERT, in ONE transaction)
  4. Send a single email report with row counts & timing

Date window
-----------
Unlike the sibling daily DAGs this one processes TODAY (plus a 1-day lookback),
not H-1, because it runs 4x/day (09:00, 12:00, 15:00, 17:00 Asia/Jakarta) and
each run refreshes the window as more orders complete. `today` is computed from
the wall clock at run time, so no Airflow data_interval/logical_date math is
involved.

LOOKBACK_DAYS widens the window backwards from today. It is 1 (today + yesterday)
so that orders CREATED after the last run of the day (17:00) -- which a
today-only window would never pick up, since tomorrow's runs only look at
tomorrow -- get caught by tomorrow's 09:00 run instead. Same for orders whose
delivery finishes after 17:00: they'd be stuck with NULL delivery timings
forever under a today-only window; with LOOKBACK_DAYS=1 the next morning's run
finalises them. Every run still re-processes the full window (today AND
yesterday) each time, via the atomic overwrite-per-date in
_overwrite_for_dates() below, so this is idempotent and safe to run 4x/day.
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
from sqlalchemy import inspect, text

from common.db_helpers import get_engine
from extract import (
    get_accept_context,
    get_blast_rider_metrics,
    get_blast_rider_positions,
    get_blast_round_metrics,
    get_blasted_rider_codes,
    get_delivery_detail,
    get_first_dispatch_reason,
    get_jiwaplus_transaction_detail,
    get_markassajiwa_user_ids,
    get_nearest_blasted_rider,
    get_pending_orders,
)
from transform import build_blast_route_agg, build_order_summary

TABLE_ORDER_SUMMARY = 'ksj_on_demand_order_summary'

# 1 = today + yesterday, re-processed on every run. See the module docstring.
LOOKBACK_DAYS = 1

DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"

LOCAL_TZ = timezone('Asia/Jakarta')

# Dtype map -- only used by SQLAlchemy when auto-creating the table. For
# `to_sql(if_exists='append')` against an existing table it is ignored. There is no
# DDL for this table anywhere in the repo (the notebook let to_sql create it), so
# this is what pins sensible types if it ever gets created from scratch.
DTYPE_MAP = {
    'transaction_id':                   types.BigInteger(),
    'transaction_datetime_gmt':         types.DateTime(),
    'order_date':                       types.DATE(),
    'order_hour':                       types.Integer(),
    'pending_order_id':                 types.BigInteger(),
    'latest_transaction_status':        types.VARCHAR(length=50),
    'order_status':                     types.VARCHAR(length=20),
    'order_status_detail':              types.VARCHAR(length=255),
    'hub_code':                         types.VARCHAR(length=50),
    'customer_id':                      types.BigInteger(),
    'customer_name':                    types.VARCHAR(length=255),
    'customer_phone':                   types.VARCHAR(length=50),
    'have_markassajiwa':                types.Boolean(),
    'order_qty':                        types.Integer(),
    'total_price':                      types.Numeric(precision=18, scale=2),
    'payment_type':                     types.VARCHAR(length=50),
    'minutes_paid_to_completed':        types.Float(),
    'seconds_to_accept':                types.Float(),
    'minutes_accept_to_otw':            types.Float(),
    'minutes_otw_to_delivered':         types.Float(),
    'minutes_accept_to_delivered':      types.Float(),
    'origin_type':                      types.VARCHAR(length=30),
    'origin_rider_code':                types.VARCHAR(length=50),
    'origin_lat':                       types.Float(),
    'origin_lng':                       types.Float(),
    'delivery_lat':                     types.Float(),
    'delivery_lng':                     types.Float(),
    'straight_line_distance_m':         types.Float(),
    'actual_route_distance_m':          types.Float(),
    'route_duration_seconds':           types.Float(),
    'delivered_distance_m':             types.Float(),
    'nearest_blasted_rider_distance_m': types.Float(),
    'avg_blasted_rider_distance_m':     types.Float(),
    'median_blasted_rider_distance_m':  types.Float(),
    'max_blasted_rider_distance_m':     types.Float(),
    'route_source':                     types.VARCHAR(length=20),
    'total_rider_blasted':              types.Integer(),
    'total_rider_blasted_until_accept': types.Integer(),
    'total_blast_rounds':               types.Integer(),
    'accepted_blast_round':             types.Integer(),
    'blasted_rider_codes':              types.Text(),
    'load_data_at':                     types.DateTime(),
    'load_data_by':                     types.VARCHAR(length=100),
}


def _resolve_date_window(lookback_days=LOOKBACK_DAYS):
    """Return (start_date_str, end_date_str) ending at TODAY in Asia/Jakarta.

    Recomputed on every call, so each of the 4 daily runs picks up the current day
    on its own without any Airflow date context.
    """
    today = datetime.now(LOCAL_TZ).date()
    start = today - timedelta(days=lookback_days)
    return start.strftime('%Y-%m-%d'), today.strftime('%Y-%m-%d')


def _resolve_batch_label():
    """Return '09AM' / '12PM' / '03PM' / '05PM' from the current Jakarta hour.

    Reporting only -- the table holds a single overwritten snapshot per day, so the
    label just tells you which of the 4 daily runs produced the email you are reading.
    """
    hour = datetime.now(LOCAL_TZ).hour
    if hour < 12:
        return '09AM'
    if hour < 15:
        return '12PM'
    if hour < 17:
        return '03PM'
    return '05PM'


def _overwrite_for_dates(engine, df, table_name, order_dates, dtype_map):
    """Atomic DELETE + INSERT for the given order_date(s).

    Both statements run in a single SQLAlchemy transaction -- if the INSERT fails the
    DELETE rolls back, so the table is never left empty for those dates.

    The DELETE is skipped when the table does not exist yet; `to_sql` then creates it
    using dtype_map.

    Returns:
        (deleted_rowcount, inserted_rowcount)
    """
    if df.empty or not order_dates:
        return 0, 0

    table_exists = table_name in inspect(engine).get_table_names()
    in_clause = ", ".join(["'{0}'".format(d) for d in order_dates])

    with engine.begin() as conn:
        deleted = 0
        if table_exists:
            result = conn.execute(text(
                "DELETE FROM {0} WHERE order_date IN ({1})".format(table_name, in_clause)
            ))
            deleted = result.rowcount

        df.to_sql(
            name=table_name,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=500,
            dtype=dtype_map,
        )

    return deleted, len(df)


def _send_report_email(date_window, stats, error=None):
    """Send a concise HTML report covering extract / transform / load counts."""
    th = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    if error:
        header_color = "#dc3545"
        title = "ETL KSJ On-Demand Order Summary - FAILED"
        error_html = """
        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline FAILED.</strong><br>
            <code>{0}</code>
        </div>
        """.format(error)
    else:
        header_color = "#4472C4"
        title = "ETL KSJ On-Demand Order Summary - Completed"
        error_html = ""

    rows_html = ""
    for i, (label, value) in enumerate(stats.items()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        rows_html += '<tr{0}><td {1}>{2}</td><td {3}>{4}</td></tr>'.format(bg, td, label, td_r, value)

    html = """\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{0};">{1}</h2>
        <p>Order date window: <strong>{2}</strong></p>

        {3}

        <h3 style="color:#333;">Pipeline Stats</h3>
        <table {4}>
            <tr><th {5}>Metric</th><th {5}>Value</th></tr>
            {6}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>""".format(header_color, title, date_window, error_html, table, th, rows_html)

    status_tag = "FAILED" if error else "REPORT"
    subject = "[{0}] ETL KSJ On-Demand Order Summary ({1})".format(status_tag, date_window)

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

    print("\n  Report email sent to: {0}".format(", ".join(receiver_email)))


def run_all(config_ksj, config_jiwaplus, config_dw, lookback_days=LOOKBACK_DAYS,
            load_data_by=DEFAULT_LOAD_DATA_BY):
    """Full ETL flow for the KSJ on-demand order distance summary table."""
    batch_label = _resolve_batch_label()
    start_date_str, end_date_str = _resolve_date_window(lookback_days)
    date_window = start_date_str if start_date_str == end_date_str \
        else "{0} .. {1}".format(start_date_str, end_date_str)

    print("=" * 70)
    print("  ETL KSJ ON-DEMAND ORDER SUMMARY  |  batch = {0}  |  window = {1}".format(
        batch_label, date_window))
    print("=" * 70)

    total_start = time.time()

    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/3] EXTRACT")

        t0 = time.time()
        df_orders = get_pending_orders(config_ksj, start_date_str, end_date_str)
        print("        pending_orders          : {0:>6} rows  ({1:.1f}s)".format(
            len(df_orders), time.time() - t0))

        t0 = time.time()
        df_blast_riders = get_blast_rider_metrics(config_ksj, start_date_str, end_date_str)
        print("        blast_rider_metrics     : {0:>6} rows  ({1:.1f}s)".format(
            len(df_blast_riders), time.time() - t0))

        t0 = time.time()
        df_blast_rounds = get_blast_round_metrics(config_ksj, start_date_str, end_date_str)
        print("        blast_round_metrics     : {0:>6} rows  ({1:.1f}s)".format(
            len(df_blast_rounds), time.time() - t0))

        t0 = time.time()
        df_accept_ctx = get_accept_context(config_ksj, start_date_str, end_date_str)
        print("        accept_context          : {0:>6} rows  ({1:.1f}s)".format(
            len(df_accept_ctx), time.time() - t0))

        t0 = time.time()
        df_nearest = get_nearest_blasted_rider(config_ksj, start_date_str, end_date_str)
        print("        nearest_blasted_rider   : {0:>6} rows  ({1:.1f}s)".format(
            len(df_nearest), time.time() - t0))

        t0 = time.time()
        df_delivery = get_delivery_detail(config_ksj, start_date_str, end_date_str)
        print("        delivery_detail         : {0:>6} rows  ({1:.1f}s)".format(
            len(df_delivery), time.time() - t0))

        # These three need the id lists from df_orders, so they run after it.
        pending_order_ids = df_orders['pending_order_id'].tolist() if not df_orders.empty else []
        transaction_ids = (df_orders['transaction_id'].dropna().tolist()
                           if not df_orders.empty else [])

        t0 = time.time()
        df_jiwaplus_detail = get_jiwaplus_transaction_detail(config_jiwaplus, transaction_ids)
        print("        jiwaplus_trx_detail     : {0:>6} rows  ({1:.1f}s)".format(
            len(df_jiwaplus_detail), time.time() - t0))

        t0 = time.time()
        df_blasted_codes = get_blasted_rider_codes(config_ksj, pending_order_ids)
        print("        blasted_rider_codes     : {0:>6} rows  ({1:.1f}s)".format(
            len(df_blasted_codes), time.time() - t0))

        t0 = time.time()
        df_blast_positions = get_blast_rider_positions(config_ksj, pending_order_ids)
        print("        blast_rider_positions   : {0:>6} rows  ({1:.1f}s)".format(
            len(df_blast_positions), time.time() - t0))

        t0 = time.time()
        df_status_detail = get_first_dispatch_reason(config_ksj, pending_order_ids)
        print("        first_dispatch_reason   : {0:>6} rows  ({1:.1f}s)".format(
            len(df_status_detail), time.time() - t0))

        t0 = time.time()
        markassajiwa_user_ids = get_markassajiwa_user_ids(config_jiwaplus)
        print("        markassajiwa_user_ids   : {0:>6} ids   ({1:.1f}s)".format(
            len(markassajiwa_user_ids), time.time() - t0))

        # ---- STEP 2: TRANSFORM ----
        print("\n  [2/3] TRANSFORM")

        t0 = time.time()
        blast_route_agg = build_blast_route_agg(df_blast_positions, df_orders)
        print("        blast_route_agg         : {0:>6} orders routed  ({1:.1f}s)".format(
            len(blast_route_agg), time.time() - t0))

        t0 = time.time()
        df_summary = build_order_summary(
            df_orders=df_orders,
            df_blast_riders=df_blast_riders,
            df_blast_rounds=df_blast_rounds,
            df_accept_ctx=df_accept_ctx,
            df_nearest=df_nearest,
            df_delivery=df_delivery,
            df_jiwaplus_detail=df_jiwaplus_detail,
            df_blasted_codes=df_blasted_codes,
            blast_route_agg=blast_route_agg,
            df_status_detail=df_status_detail,
            markassajiwa_user_ids=markassajiwa_user_ids,
        )
        print("        order_summary           : {0:>6} rows  ({1:.1f}s)".format(
            len(df_summary), time.time() - t0))

        if not df_summary.empty:
            df_summary['load_data_at'] = datetime.now(LOCAL_TZ).replace(tzinfo=None)
            df_summary['load_data_by'] = load_data_by

        status_counts = (df_summary['order_status'].value_counts().to_dict()
                         if not df_summary.empty else {})
        route_counts = (df_summary['route_source'].value_counts().to_dict()
                        if not df_summary.empty else {})

        # ---- STEP 3: LOAD ----
        print("\n  [3/3] LOAD")

        deleted = 0
        dumped = 0

        if df_summary.empty:
            # Deliberately do NOT delete here. An empty result is normal on a quiet
            # morning, but it can also mean a source hiccup -- and deleting on an empty
            # result would wipe the good rows an earlier batch of the same day loaded.
            print("        No rows to load -- skipping DELETE+INSERT ({0} left untouched).".format(
                TABLE_ORDER_SUMMARY))
        else:
            engine = get_engine(config_dw)
            order_dates = sorted(
                str(d) for d in df_summary['order_date'].dropna().unique() if d
            )

            t0 = time.time()
            deleted, dumped = _overwrite_for_dates(
                engine, df_summary, TABLE_ORDER_SUMMARY, order_dates, DTYPE_MAP,
            )
            print("        Overwrite (atomic):  -{0} / +{1} rows on {2} "
                  "for order_date IN {3}  ({4:.1f}s)".format(
                      deleted, dumped, TABLE_ORDER_SUMMARY, order_dates, time.time() - t0))

        total_elapsed = time.time() - total_start
        print("\n  TOTAL TIME: {0:.1f}s".format(total_elapsed))
        print("=" * 70)

        stats = {
            "batch": batch_label,
            "pending_orders (extracted)": "{0:,}".format(len(df_orders)),
            "delivery_detail (extracted)": "{0:,}".format(len(df_delivery)),
            "jiwaplus_trx_detail (extracted)": "{0:,}".format(len(df_jiwaplus_detail)),
            "blast_rider_positions (extracted)": "{0:,}".format(len(df_blast_positions)),
            "first_dispatch_reason (extracted)": "{0:,}".format(len(df_status_detail)),
            "order_summary (built)": "{0:,}".format(len(df_summary)),
            "order_status breakdown": status_counts or "-",
            "route_source breakdown": route_counts or "-",
            "{0} (overwrite: -del/+ins)".format(TABLE_ORDER_SUMMARY):
                "-{0:,} / +{1:,}".format(deleted, dumped),
            "total_time": "{0:.1f}s".format(total_elapsed),
        }

        try:
            _send_report_email(date_window, stats)
        except Exception as mail_err:
            logging.warning("Failed to send report email: %s", mail_err)

        return {
            "status": "SUCCESS",
            "deleted": deleted,
            "dumped": dumped,
            "stats": stats,
        }

    except Exception as e:
        print("\n  PIPELINE FAILED: {0}".format(e))
        try:
            _send_report_email(date_window, {"error": str(e)}, error=str(e))
        except Exception as mail_err:
            logging.warning("Failed to send failure email: %s", mail_err)
        raise
