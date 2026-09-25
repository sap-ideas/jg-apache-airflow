"""Extract functions - KSJ on-demand order distance summary.

Ported from etl_ksj_on_demand_order_distance.ipynb (cells 14-23). The SQL is
kept byte-for-byte identical to the notebook: every query there was verified
against the live databases when the notebook was written, and the comments
below record the non-obvious findings from that verification.

Three source databases are involved. Two of them (jiwa-order-dispatcher,
d_transaction) sit on the same host but are still SEPARATE databases, so none
of them can be JOINed together in SQL — the merge happens in pandas inside
transform.py:

    jiwa-order-dispatcher (config_ksj.d_jiwa_ondemand) : pending_orders, dispatch_blasts,
                                                         dispatch_blast_riders
    d_transaction         (config_ksj.d_transaction)   : transactions,
                                                         on_demand_order_deliveries
    jiwaplus              (config_jiwaplus)            : transactions, user_invoice_voucher

Timestamps on the servers are UTC and get shifted to WIB in the SQL itself.
EXCEPTIONS (do NOT shift these again):
  - d_transaction.transactions.transaction_date is already WIB
  - jiwaplus.transactions.transaction_datetime_gmt is already WIB despite its name
"""

import pandas as pd

from common.db_helpers import jiwaplus_db_conn, ksj_link_db_conn


# Voucher "Markas Sejuta Jiwa" -- drives the have_markassajiwa flag.
MARKASSAJIWA_VOUCHER_ID = 96965


def _run_query(config_ksj, db_name, sql, params=None):
    """Run a query against a KSJ Link PostgreSQL database, return a DataFrame."""
    connection = ksj_link_db_conn(config_ksj, db_name)
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        records = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()
    finally:
        connection.close()

    return pd.DataFrame(records, columns=columns)


def _run_query_jiwaplus(config_jiwaplus, sql, params=None):
    """Run a query against the JiwaPlus PostgreSQL database, return a DataFrame."""
    connection = jiwaplus_db_conn(config_jiwaplus)
    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        records = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        cursor.close()
    finally:
        connection.close()

    return pd.DataFrame(records, columns=columns)


def get_pending_orders(config_ksj, start_date_str, end_date_str):
    """One row per on-demand order, from jiwa-order-dispatcher.pending_orders.

    This is the main table (the grain of our summary table). Flow: customer
    payment succeeds -> row lands in pending_orders -> system blasts to riders.

    Column notes:
      rider_lat / rider_long = position of the rider who ACCEPTED the order (not
                               the outlet). Verified identical to the accepting
                               rider's responded_rider_lat/long.
      status                 = ACCEPTED (a rider took it) / EXPIRED (nobody took it
                               within the TTL, order is auto reject-refunded) / CANCELLED.
    """
    query = """
        SELECT
            id                                                          AS pending_order_id,
            jiwaplus_ref_id,
            transaction_id,
            hub_code,
            customer_id,
            status                                                      AS dispatcher_status,
            total_item_qty,
            total_price,
            total_price_after_discount,
            payment_type,
            delivery_lat,
            delivery_lng,
            rider_lat                                                   AS accepted_rider_lat,
            rider_long                                                  AS accepted_rider_lng,
            blast_count,
            accepted_rider_code,
            (created_at   AT TIME ZONE 'Asia/Jakarta')                  AS created_at_wib,
            (accepted_at  AT TIME ZONE 'Asia/Jakarta')                  AS accepted_at_wib,
            (expired_at   AT TIME ZONE 'Asia/Jakarta')                  AS expired_at_wib,
            (cancelled_at AT TIME ZONE 'Asia/Jakarta')                  AS cancelled_at_wib,
            ROUND(EXTRACT(EPOCH FROM (expires_at - created_at)) / 60, 2) AS order_ttl_minutes,
            ROUND(EXTRACT(EPOCH FROM (accepted_at - created_at)), 1)     AS seconds_to_accept
        FROM pending_orders
        WHERE (created_at AT TIME ZONE 'Asia/Jakarta')::date BETWEEN '{0}' AND '{1}'
        ORDER BY id;
    """.format(start_date_str, end_date_str)

    return _run_query(config_ksj, config_ksj.d_jiwa_ondemand, query)


def get_blast_rider_metrics(config_ksj, start_date_str, end_date_str):
    """Per-order aggregate from dispatch_blast_riders -- how many riders were
    pinged and how they responded.

    IMPORTANT: do NOT join dispatch_blasts and dispatch_blast_riders in one query
    and then COUNT, because that produces a cartesian product (an order with 2
    blast rounds would report double the pings). That is why the two aggregates
    are computed separately and merged later.

    NOTE: nearest/avg/median/max_blasted_rider_distance_m are NOT computed here
    any more. They used to be MIN/AVG/MAX(distance_in_meter), which is the
    STRAIGHT-LINE distance the dispatcher computes (blast position -> delivery).
    They are now road-route distances (OSRM), computed per rider in transform.py
    from get_blast_rider_positions() below.
    """
    query = """
        SELECT
            r.pending_order_id,
            COUNT(*)                                                AS total_riders_blasted,
            COUNT(*) FILTER (WHERE r.response = 'ACCEPTED')          AS riders_accepted,
            COUNT(*) FILTER (WHERE r.response = 'REJECTED')          AS riders_rejected,
            COUNT(*) FILTER (WHERE r.response = 'TIMEOUT')           AS riders_timeout,
            COUNT(*) FILTER (WHERE r.response IS NULL)               AS riders_no_response,
            COUNT(*) FILTER (WHERE r.wa_status   = 'SENT')           AS riders_wa_sent,
            COUNT(*) FILTER (WHERE r.push_status = 'SENT')           AS riders_push_sent
        FROM dispatch_blast_riders r
        JOIN pending_orders p ON p.id = r.pending_order_id
        WHERE (p.created_at AT TIME ZONE 'Asia/Jakarta')::date BETWEEN '{0}' AND '{1}'
        GROUP BY 1;
    """.format(start_date_str, end_date_str)

    return _run_query(config_ksj, config_ksj.d_jiwa_ondemand, query)


def get_blast_round_metrics(config_ksj, start_date_str, end_date_str):
    """Per-order aggregate from dispatch_blasts -- how many blast rounds and radii.

    The system blasts every 2 minutes, max 3 rounds, widening the radius each
    round (3000 m -> 7500 m). If nobody accepts by the last round the order EXPIRES.
    """
    query = """
        SELECT
            b.pending_order_id,
            COUNT(*)                    AS total_blast_rounds,
            MAX(b.blast_round)          AS last_blast_round,
            MIN(b.radius_m)             AS first_blast_radius_m,
            MAX(b.radius_m)             AS max_blast_radius_m,
            SUM(b.riders_notified)      AS total_riders_notified,
            (MIN(b.sent_at) AT TIME ZONE 'Asia/Jakarta')  AS first_blast_at_wib,
            (MAX(b.sent_at) AT TIME ZONE 'Asia/Jakarta')  AS last_blast_at_wib
        FROM dispatch_blasts b
        JOIN pending_orders p ON p.id = b.pending_order_id
        WHERE (p.created_at AT TIME ZONE 'Asia/Jakarta')::date BETWEEN '{0}' AND '{1}'
        GROUP BY 1;
    """.format(start_date_str, end_date_str)

    return _run_query(config_ksj, config_ksj.d_jiwa_ondemand, query)


def get_accept_context(config_ksj, start_date_str, end_date_str):
    """For accepted orders: which blast round the rider finally accepted on, and
    how many riders the order had been dispatched to by that point.

    Answers: "for every accepted order, the rider accepted it after the order had
    been dispatched to how many riders?"
    """
    query = """
        WITH acc AS (
            SELECT r.pending_order_id, MIN(b.blast_round) AS accepted_blast_round
            FROM dispatch_blast_riders r
            JOIN dispatch_blasts b ON b.id = r.blast_id
            WHERE r.response = 'ACCEPTED'
            GROUP BY 1
        )
        SELECT
            r.pending_order_id,
            MIN(a.accepted_blast_round)                                              AS accepted_blast_round,
            COUNT(*) FILTER (WHERE b.blast_round <= a.accepted_blast_round)          AS riders_blasted_until_accept
        FROM dispatch_blast_riders r
        JOIN dispatch_blasts  b ON b.id = r.blast_id
        JOIN acc              a ON a.pending_order_id = r.pending_order_id
        JOIN pending_orders   p ON p.id = r.pending_order_id
        WHERE (p.created_at AT TIME ZONE 'Asia/Jakarta')::date BETWEEN '{0}' AND '{1}'
        GROUP BY 1;
    """.format(start_date_str, end_date_str)

    return _run_query(config_ksj, config_ksj.d_jiwa_ondemand, query)


def get_nearest_blasted_rider(config_ksj, start_date_str, end_date_str):
    """Coordinates of the NEAREST blasted rider per order.

    Used as the origin point for FAILED orders (EXPIRED / CANCELLED): those have
    no accepting rider, so there is no definitive origin. Reading: "even the
    nearest available rider was this far away".
    """
    query = """
        SELECT DISTINCT ON (r.pending_order_id)
            r.pending_order_id,
            r.rider_code             AS nearest_blasted_rider_code,
            r.blast_rider_lat        AS nearest_blasted_rider_lat,
            r.blast_rider_long       AS nearest_blasted_rider_lng
        FROM dispatch_blast_riders r
        JOIN pending_orders p ON p.id = r.pending_order_id
        WHERE r.distance_in_meter IS NOT NULL
          AND (p.created_at AT TIME ZONE 'Asia/Jakarta')::date BETWEEN '{0}' AND '{1}'
        ORDER BY r.pending_order_id, r.distance_in_meter ASC;
    """.format(start_date_str, end_date_str)

    return _run_query(config_ksj, config_ksj.d_jiwa_ondemand, query)


def get_blast_rider_positions(config_ksj, pending_order_ids):
    """Position of EVERY blasted rider (not one aggregate row per order).

    Used to compute per-rider road-route distance (OSRM), which transform.py then
    aggregates (min/mean/median/max). This query deliberately does NOT filter on
    order status -- ACCEPTED and EXPIRED/CANCELLED orders alike are included,
    because the distance of every blasted rider stays relevant regardless of how
    the order ended up.
    """
    pending_order_ids = list(pending_order_ids)
    if not pending_order_ids:
        # `= ANY('{}')` makes Postgres fail on an untyped empty array, and there is
        # nothing to look up anyway.
        return pd.DataFrame(
            columns=['pending_order_id', 'rider_code', 'blast_rider_lat', 'blast_rider_long']
        )

    query = """
        SELECT
            pending_order_id,
            rider_code,
            blast_rider_lat,
            blast_rider_long
        FROM dispatch_blast_riders
        WHERE pending_order_id = ANY(%s)
          AND blast_rider_lat  IS NOT NULL
          AND blast_rider_long IS NOT NULL;
    """

    return _run_query(config_ksj, config_ksj.d_jiwa_ondemand, query, params=(pending_order_ids,))


def get_delivery_detail(config_ksj, start_date_str, end_date_str):
    """Transaction & delivery detail from d_transaction
    (transactions x on_demand_order_deliveries).

    Both tables live in the same database so they can be JOINed directly in SQL.
    transaction_type = 'ON_DEMAND' separates them from regular KSJ_LINK transactions.

    paid_at      = when the customer actually paid (verified identical to the
                   millisecond with pending_orders.order_snapshot->>'paid_at').
    completed_at = when the system closed the transaction. Verified to differ from
                   on_demand_order_deliveries.delivered_at by only a few
                   milliseconds (23 DELIVERED orders checked, all < 10 ms) --
                   effectively the same event.
    rider_completion_lat/lng = rider position when they pressed "done" and
                   uploaded the delivery proof.

    The date window is padded by +/- 1 day because transaction_date can fall on
    the neighbouring day relative to the order's created_at.
    """
    query = """
        SELECT
            t.pending_order_id,
            t.id                                            AS trx_id,
            t.transaction_no,
            t.rider_code                                    AS delivery_rider_code,
            t.transaction_status,
            t.transaction_date,
            t.total_item_qty                                AS trx_total_item_qty,
            (t.paid_at      AT TIME ZONE 'Asia/Jakarta')     AS paid_at_wib,
            (t.completed_at AT TIME ZONE 'Asia/Jakarta')     AS trx_completed_at_wib,
            d.delivery_status,
            d.failed_reason,
            d.rider_completion_lat,
            d.rider_completion_lng,
            (d.out_for_delivery_at + INTERVAL '7 hours')     AS out_for_delivery_at_wib,
            (d.delivered_at        + INTERVAL '7 hours')     AS delivered_at_wib
        FROM transactions t
        LEFT JOIN on_demand_order_deliveries d ON d.transaction_id = t.id
        WHERE t.transaction_type = 'ON_DEMAND'
          AND t.transaction_date BETWEEN '{0}'::date - INTERVAL '1 day' AND '{1}'::date + INTERVAL '1 day'
          AND t.pending_order_id IS NOT NULL;
    """.format(start_date_str, end_date_str)

    return _run_query(config_ksj, config_ksj.d_transaction, query)


def get_jiwaplus_transaction_detail(config_jiwaplus, transaction_ids):
    """JiwaPlus-side transaction detail (customer_name, customer_phone, checkout status).

    JOIN key: jiwaplus.transactions.id_transaction == pending_orders.transaction_id.
    CAREFUL: this is NOT d_transaction.transactions.id -- that one uses a separate
    numbering scheme and is already exposed as trx_id by get_delivery_detail().

    transaction_datetime_gmt: despite the name this column holds WIB, not GMT/UTC --
    verified consistently +7 hours from pending_orders.created_at (real UTC) across
    4 samples. Do NOT shift it by another 7 hours.
    """
    transaction_ids = list(transaction_ids)
    if not transaction_ids:
        return pd.DataFrame(columns=[
            'id_transaction', 'transaction_datetime_gmt', 'latest_transaction_status',
            'customer_name', 'customer_phone',
        ])

    query = """
        SELECT
            id_transaction,
            transaction_datetime_gmt,
            latest_transaction_status,
            customer_name,
            customer_phone
        FROM transactions
        WHERE id_transaction = ANY(%s);
    """

    return _run_query_jiwaplus(config_jiwaplus, query, params=(transaction_ids,))


def get_markassajiwa_user_ids(config_jiwaplus):
    """user_ids holding an active 'Markas Sejuta Jiwa' membership voucher RIGHT NOW.

    Checked against CURRENT_DATE (the day this ETL runs), so the flag reflects the
    customer's membership TODAY, not their membership on the historical order date.

    Assumption (not cross-verified): user_invoice_voucher.user_id == customer_id in
    pending_orders / jiwaplus.transactions.customer_id -- both are JiwaPlus account ids.
    """
    query = """
        SELECT DISTINCT(uiv.user_id)
        FROM user_invoice_voucher uiv
        WHERE uiv.voucher_id = %s
          AND uiv.active_at > CURRENT_DATE;
    """

    df = _run_query_jiwaplus(config_jiwaplus, query, params=(MARKASSAJIWA_VOUCHER_ID,))
    if df.empty:
        return set()

    return set(df['user_id'].tolist())


def get_blasted_rider_codes(config_ksj, pending_order_ids):
    """Comma-separated rider_codes blasted/pinged per order, ordered NEAREST first
    (distance_in_meter ASC, falling back to ping time when distance is NULL).
    """
    pending_order_ids = list(pending_order_ids)
    if not pending_order_ids:
        return pd.DataFrame(columns=['pending_order_id', 'blasted_rider_codes'])

    query = """
        SELECT
            pending_order_id,
            rider_code,
            distance_in_meter,
            created_at
        FROM dispatch_blast_riders
        WHERE pending_order_id = ANY(%s)
        ORDER BY pending_order_id, distance_in_meter ASC NULLS LAST, created_at ASC;
    """

    df = _run_query(config_ksj, config_ksj.d_jiwa_ondemand, query, params=(pending_order_ids,))
    if df.empty:
        return pd.DataFrame(columns=['pending_order_id', 'blasted_rider_codes'])

    return (df.groupby('pending_order_id')['rider_code']
              .apply(lambda codes: ', '.join(codes))
              .reset_index()
              .rename(columns={'rider_code': 'blasted_rider_codes'}))


def get_first_dispatch_reason(config_ksj, pending_order_ids):
    """Earliest dispatch reason per order, from dispatch_records.

    dispatch_records has no formal FK to pending_orders -- joined manually via
    order_id (text, e.g. 'SJ-xxxx') = pending_orders.jiwaplus_ref_id.

    Why EARLIEST (not just any row): a failed order always gets a second,
    generic "expired - no rider accepted" row later -- the first row is the one
    carrying the real reason (e.g. "no rider with sufficient stock", "no nearby
    rider"). Used as-is as order_status_detail, with no further hardcoding or
    renaming.
    """
    pending_order_ids = list(pending_order_ids)
    if not pending_order_ids:
        return pd.DataFrame(columns=['pending_order_id', 'order_status_detail'])

    query = """
        SELECT DISTINCT ON (po.id)
            po.id AS pending_order_id,
            dr.reason AS order_status_detail
        FROM pending_orders po
        JOIN dispatch_records dr ON dr.order_id = po.jiwaplus_ref_id
        WHERE po.id = ANY(%s)
        ORDER BY po.id, dr.created_at ASC;
    """

    return _run_query(config_ksj, config_ksj.d_jiwa_ondemand, query, params=(pending_order_ids,))
