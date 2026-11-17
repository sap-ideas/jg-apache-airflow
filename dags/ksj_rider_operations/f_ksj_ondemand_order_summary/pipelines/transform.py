"""Transform functions - KSJ on-demand order distance summary.

Ported from etl_ksj_on_demand_order_distance.ipynb (cells 25-26 and 35-47):
distance helpers (haversine + routing API) plus the merge/derive logic that turns
the six source frames into the final per-order summary table.
"""

import time

import numpy as np
import pandas as pd
import requests


# ==========================================================================================
# ROUTING API
# ------------------------------------------------------------------------------------------
# No KSJ database stores a route distance anywhere (every column named %distance% was
# checked). The only thing available is dispatch_blast_riders.distance_in_meter, and that
# is a STRAIGHT-LINE (haversine) rider -> delivery distance, not a road-route distance.
# So the actual route distance has to be computed here against a routing API.
#
# ROUTE_PROVIDER:
#   'OSRM'   -> router.project-osrm.org, free, no API key. Default.
#               Note: the OSRM demo server only offers the 'driving' profile
#               (cycling/foot return identical numbers, they are aliases).
#               For motorbikes it tends to over-estimate -> conservative.
#   'GOOGLE' -> needs GOOGLE_MAPS_API_KEY. More accurate & stable, but paid.
#   'NONE'   -> routing disabled, actual_route_distance_m stays NULL.
#
# On API failure/timeout the column is left NULL and route_source='FAILED'
# -- this ETL must NEVER die because of a third-party API.
# ==========================================================================================
ROUTE_PROVIDER      = 'OSRM'
OSRM_BASE           = 'https://router.project-osrm.org/route/v1/driving'
GOOGLE_MAPS_API_KEY = None
ROUTE_TIMEOUT       = 15      # seconds per request
ROUTE_MAX_RETRY     = 2       # retries on failure
ROUTE_SLEEP_SECONDS = 0.3     # pause between requests, to stay polite to the public server


RENAME_MAP = {
    'total_item_qty':              'order_qty',
    'total_riders_blasted':        'total_rider_blasted',
    'riders_blasted_until_accept': 'total_rider_blasted_until_accept',
}

# Final column list -- exactly the agreed spec. Columns that are no longer used
# (transaction_no, delivery_status, failed_reason, the per-response rider breakdown,
# the raw created_at_wib timestamps) are DELIBERATELY dropped from the output.
FINAL_COLUMNS = [
    'transaction_id', 'transaction_datetime_gmt', 'order_date', 'order_hour', 'pending_order_id',
    'latest_transaction_status', 'order_status', 'order_status_detail', 'hub_code', 'customer_id',
    'customer_name', 'customer_phone', 'have_markassajiwa',
    'order_qty', 'total_price', 'payment_type',
    'minutes_paid_to_completed', 'seconds_to_accept', 'minutes_accept_to_otw',
    'minutes_otw_to_delivered', 'minutes_accept_to_delivered',
    'origin_type', 'origin_rider_code', 'origin_lat', 'origin_lng', 'delivery_lat', 'delivery_lng',
    'straight_line_distance_m', 'actual_route_distance_m', 'route_duration_seconds',
    'delivered_distance_m', 'nearest_blasted_rider_distance_m', 'avg_blasted_rider_distance_m',
    'median_blasted_rider_distance_m', 'max_blasted_rider_distance_m', 'route_source',
    'total_rider_blasted', 'total_rider_blasted_until_accept', 'total_blast_rounds',
    'accepted_blast_round', 'blasted_rider_codes',
]

COUNT_COLUMNS = [
    'blast_count', 'total_riders_blasted', 'riders_accepted', 'riders_rejected',
    'riders_timeout', 'riders_no_response', 'riders_wa_sent', 'riders_push_sent',
    'total_blast_rounds', 'last_blast_round', 'total_riders_notified',
    'accepted_blast_round', 'riders_blasted_until_accept',
]

NUMERIC_COLUMNS = [
    'total_price', 'straight_line_distance_m', 'actual_route_distance_m',
    'route_duration_seconds', 'delivered_distance_m',
    'nearest_blasted_rider_distance_m', 'avg_blasted_rider_distance_m',
    'median_blasted_rider_distance_m', 'max_blasted_rider_distance_m', 'seconds_to_accept',
    'minutes_paid_to_completed', 'minutes_accept_to_otw', 'minutes_otw_to_delivered',
    'minutes_accept_to_delivered', 'origin_lat', 'origin_lng', 'delivery_lat', 'delivery_lng',
]

BLAST_ROUTE_AGG_COLUMNS = [
    'nearest_blasted_rider_distance_m', 'avg_blasted_rider_distance_m',
    'median_blasted_rider_distance_m', 'max_blasted_rider_distance_m',
]

JIWAPLUS_COLUMNS = [
    'id_transaction', 'transaction_datetime_gmt', 'latest_transaction_status',
    'customer_name', 'customer_phone',
]


_route_cache = {}


def haversine_m(lat1, lng1, lat2, lng2):
    """Straight-line (great-circle) distance in meters. Earth radius = 6,371,000 m.

    lat/lng values arrive from Postgres as Decimal -> cast to float first.
    """
    vals = [lat1, lng1, lat2, lng2]
    if any(v is None or pd.isna(v) for v in vals):
        return np.nan

    lat1, lng1, lat2, lng2 = [float(v) for v in vals]
    R = 6371000.0

    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi       = np.radians(lat2 - lat1)
    dlambda    = np.radians(lng2 - lng1)

    a = np.sin(dphi / 2.0) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda / 2.0) ** 2
    return round(R * 2.0 * np.arcsin(np.sqrt(a)), 2)


def _osrm_route(lat1, lng1, lat2, lng2):
    """Call OSRM. Its URL format is {lng},{lat} -- not {lat},{lng}."""
    url = "{0}/{1},{2};{3},{4}?overview=false".format(OSRM_BASE, lng1, lat1, lng2, lat2)
    for attempt in range(ROUTE_MAX_RETRY + 1):
        try:
            resp = requests.get(url, timeout=ROUTE_TIMEOUT)
            data = resp.json()
            if data.get('code') == 'Ok' and data.get('routes'):
                route = data['routes'][0]
                return (round(float(route['distance']), 2),
                        round(float(route['duration']), 1),
                        'OSRM')
            return (None, None, 'NO_ROUTE')
        except Exception as e:
            if attempt == ROUTE_MAX_RETRY:
                print('   OSRM failed (%s,%s)->(%s,%s): %s' % (lat1, lng1, lat2, lng2, type(e).__name__))
                return (None, None, 'FAILED')
            time.sleep(1.0)
    return (None, None, 'FAILED')


def _google_route(lat1, lng1, lat2, lng2):
    """Call the Google Directions API. Needs GOOGLE_MAPS_API_KEY."""
    if not GOOGLE_MAPS_API_KEY:
        return (None, None, 'NO_API_KEY')

    url = 'https://maps.googleapis.com/maps/api/directions/json'
    params = {'origin': '%s,%s' % (lat1, lng1),
              'destination': '%s,%s' % (lat2, lng2),
              'mode': 'driving',
              'key': GOOGLE_MAPS_API_KEY}
    for attempt in range(ROUTE_MAX_RETRY + 1):
        try:
            data = requests.get(url, params=params, timeout=ROUTE_TIMEOUT).json()
            if data.get('status') == 'OK' and data.get('routes'):
                leg = data['routes'][0]['legs'][0]
                return (round(float(leg['distance']['value']), 2),
                        round(float(leg['duration']['value']), 1),
                        'GOOGLE')
            return (None, None, 'NO_ROUTE')
        except Exception as e:
            if attempt == ROUTE_MAX_RETRY:
                print('   Google failed: %s' % type(e).__name__)
                return (None, None, 'FAILED')
            time.sleep(1.0)
    return (None, None, 'FAILED')


def get_route_distance(lat1, lng1, lat2, lng2):
    """Actual road-route distance + estimated duration.

    Results are cached per coordinate pair so orders sharing a point (e.g. the
    Kedoya head office, a frequent delivery address) are not requested twice.

    Returns:
        (distance_m, duration_seconds, source)
    """
    vals = [lat1, lng1, lat2, lng2]
    if any(v is None or pd.isna(v) for v in vals):
        return (None, None, 'NO_COORDINATE')

    key = tuple(round(float(v), 6) for v in vals)
    if key in _route_cache:
        return _route_cache[key]

    if ROUTE_PROVIDER == 'OSRM':
        result = _osrm_route(*key)
    elif ROUTE_PROVIDER == 'GOOGLE':
        result = _google_route(*key)
    else:
        result = (None, None, 'DISABLED')

    _route_cache[key] = result
    time.sleep(ROUTE_SLEEP_SECONDS)
    return result


def _left_merge(df, part, on='pending_order_id'):
    """Left-merge a per-order support frame, tolerating an empty one.

    psycopg2 returns every column as dtype `object` when a query yields no rows,
    which makes a plain merge fail against an int64 join key
    ("You are trying to merge on int64 and object columns"). When the support
    frame is empty we add its columns as NA instead of merging.
    """
    extra_cols = [c for c in part.columns if c != on]
    if part.empty:
        for col in extra_cols:
            df[col] = pd.NA
        return df

    return df.merge(part, on=on, how='left')


def build_blast_route_agg(df_blast_positions, df_orders):
    """Road-route (OSRM) distance for EVERY blasted rider, aggregated per order.

    This replaces the old nearest/avg/max_blasted_rider_distance_m, which came from
    distance_in_meter -- the STRAIGHT-LINE distance the dispatcher computes from the
    blast position. Computed for ALL orders, both ACCEPTED and EXPIRED/CANCELLED.

    Shares _route_cache with build_order_summary(), so coordinate pairs already
    requested (e.g. a delivery point used repeatedly) are not requested again.
    """
    if df_blast_positions.empty:
        return pd.DataFrame(columns=['pending_order_id'] + BLAST_ROUTE_AGG_COLUMNS)

    df_positions = df_blast_positions.merge(
        df_orders[['pending_order_id', 'delivery_lat', 'delivery_lng']],
        on='pending_order_id',
        how='left',
    )

    blast_routes = df_positions.apply(
        lambda r: get_route_distance(r['blast_rider_lat'], r['blast_rider_long'],
                                     r['delivery_lat'], r['delivery_lng']), axis=1)
    df_positions['route_distance_m'] = [x[0] for x in blast_routes]

    return (df_positions
            .groupby('pending_order_id')['route_distance_m']
            .agg(nearest_blasted_rider_distance_m='min',
                 avg_blasted_rider_distance_m='mean',
                 median_blasted_rider_distance_m='median',
                 max_blasted_rider_distance_m='max')
            .round(2)
            .reset_index())


def build_order_summary(df_orders, df_blast_riders, df_blast_rounds, df_accept_ctx,
                        df_nearest, df_delivery, df_jiwaplus_detail, df_blasted_codes,
                        blast_route_agg, df_status_detail, markassajiwa_user_ids):
    """Merge every source and derive the final per-order summary.

    Returns a DataFrame with exactly FINAL_COLUMNS. When there are no orders in the
    window it returns an empty frame with those columns rather than failing -- an
    on-demand day with zero orders is normal for this young product, and the caller
    is expected to skip the load in that case.
    """
    if df_orders.empty:
        return pd.DataFrame(columns=FINAL_COLUMNS)

    # ---- Merge every source. The three databases are separate, so this happens
    #      here in pandas rather than in SQL.
    df = df_orders.copy()

    for part in [df_blast_riders, df_blast_rounds, df_accept_ctx, df_nearest]:
        df = _left_merge(df, part)

    df = _left_merge(df, df_delivery.drop_duplicates(subset=['pending_order_id'], keep='last'))

    for col in COUNT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    if len(df) != len(df_orders):
        raise ValueError(
            "row count changed after merge (%s -> %s) -- a support table has duplicates"
            % (len(df_orders), len(df))
        )

    # ---- Origin point per order.
    # ACCEPTED order -> origin = position of the rider who accepted it.
    # Failed order    -> no accepting rider, so no definitive origin. The NEAREST
    #                    blasted rider is used as a proxy so the distance columns stay
    #                    populated for failed cases, reading as "even the nearest
    #                    available rider was this far away".
    #
    # DATA NOTE: blast_rider_lat/long and distance_in_meter in dispatch_blast_riders
    # only started being populated on 11 August 2026. Older orders (and some of the
    # 11 Aug ones) have no rider coordinates at all -> their origin genuinely cannot
    # be computed, flagged origin_type = 'NO_RIDER_COORDINATE' (a data limitation,
    # not an error).
    is_accepted = df['dispatcher_status'].eq('ACCEPTED') & df['accepted_rider_lat'].notna()
    has_nearest = df['nearest_blasted_rider_lat'].notna()
    no_blast    = df['total_riders_blasted'].eq(0)

    df['origin_lat'] = np.where(is_accepted, df['accepted_rider_lat'],
                                np.where(has_nearest, df['nearest_blasted_rider_lat'], np.nan))
    df['origin_lng'] = np.where(is_accepted, df['accepted_rider_lng'],
                                np.where(has_nearest, df['nearest_blasted_rider_lng'], np.nan))
    df['origin_rider_code'] = np.where(is_accepted, df['accepted_rider_code'],
                                       np.where(has_nearest, df['nearest_blasted_rider_code'], None))
    df['origin_type'] = np.select(
        [is_accepted, has_nearest, no_blast],
        ['ACCEPTED_RIDER', 'NEAREST_BLASTED_RIDER', 'NO_RIDER_BLASTED'],
        default='NO_RIDER_COORDINATE')

    df['origin_lat'] = pd.to_numeric(df['origin_lat'], errors='coerce')
    df['origin_lng'] = pd.to_numeric(df['origin_lng'], errors='coerce')

    # ---- Straight-line distance.
    # This column stays haversine ON PURPOSE -- it is named "straight_line" and acts as
    # the baseline for route_detour_ratio, so it must not become an OSRM route like the
    # other distance columns (otherwise the detour ratio becomes meaningless).
    df['straight_line_distance_m'] = df.apply(
        lambda r: haversine_m(r['origin_lat'], r['origin_lng'],
                              r['delivery_lat'], r['delivery_lng']), axis=1)

    # ---- Actual route distance (routing API).
    # Only unique coordinate pairs hit the API (see _route_cache). On API failure the
    # column is NULL and route_source='FAILED', and the run continues.
    routes = df.apply(lambda r: get_route_distance(r['origin_lat'], r['origin_lng'],
                                                   r['delivery_lat'], r['delivery_lng']), axis=1)
    df['actual_route_distance_m'] = [x[0] for x in routes]
    df['route_duration_seconds']  = [x[1] for x in routes]
    df['route_source']            = [x[2] for x in routes]

    # QA-only column: not exported, but reported in the run summary.
    df['route_detour_ratio'] = (pd.to_numeric(df['actual_route_distance_m'], errors='coerce') /
                                pd.to_numeric(df['straight_line_distance_m'], errors='coerce')
                                  .where(lambda s: s > 0)).round(3)

    # ---- Extra sources: jiwaplus, blasted rider codes, per-rider route distances.
    if df_jiwaplus_detail.empty:
        for col in JIWAPLUS_COLUMNS:
            df[col] = pd.NA
    else:
        df = df.merge(df_jiwaplus_detail, left_on='transaction_id',
                      right_on='id_transaction', how='left')

    df = _left_merge(df, df_blasted_codes)
    df = _left_merge(df, blast_route_agg)
    df = _left_merge(df, df_status_detail)

    # delivered_distance_m used to be haversine (origin -> rider_completion_lat/lng);
    # it is now an OSRM route -- the real road distance from the origin to the point
    # where the rider pressed "delivered".
    df['delivered_distance_m'] = df.apply(
        lambda r: get_route_distance(r['origin_lat'], r['origin_lng'],
                                     r['rider_completion_lat'], r['rider_completion_lng'])[0], axis=1)

    df['have_markassajiwa'] = df['customer_id'].isin(markassajiwa_user_ids)

    # ---- Derived columns & finalisation.
    # order_status = pending_orders.status AS-IS (ACCEPTED/EXPIRED/CANCELLED) --
    # no longer mapped to SUCCESS/FAILED/CANCELLED, since only these 3 raw values
    # have been confirmed to exist in the DB.
    df['order_status'] = df['dispatcher_status']

    # order_status_detail already came from the get_first_dispatch_reason() merge
    # above -- no hardcoding/derivation here, it is exactly the earliest
    # dispatch_records.reason. This replaces the old derived 'type' column
    # ('Rider Accepted'/'Rider Not Accepted' from order_status=='SUCCESS'), which
    # mislabeled orders that were CANCELLED after a rider had already accepted
    # (e.g. order ids 5/11/12) as 'Rider Not Accepted'. order_status_detail
    # reflects reality instead of an assumed label.

    for col in ['created_at_wib', 'accepted_at_wib', 'delivered_at_wib',
                'out_for_delivery_at_wib', 'paid_at_wib', 'trx_completed_at_wib']:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    df['order_date'] = df['created_at_wib'].dt.date
    df['order_hour'] = df['created_at_wib'].dt.hour

    # Full flow of one order: paid_at -> ... -> accepted_at -> out_for_delivery_at
    # ("OTW", rider heads to the customer) -> delivered_at/completed_at (rider arrives
    # and uploads the proof). Each segment answers a different question. Outliers are
    # deliberately left as-is (one order had minutes_paid_to_completed ~1549 min /
    # 25.8 h) -- filter them at analysis time if needed.
    df['minutes_paid_to_completed']   = ((df['trx_completed_at_wib'] - df['paid_at_wib'])
                                         .dt.total_seconds() / 60).round(2)
    df['minutes_accept_to_otw']       = ((df['out_for_delivery_at_wib'] - df['accepted_at_wib'])
                                         .dt.total_seconds() / 60).round(2)
    df['minutes_otw_to_delivered']    = ((df['delivered_at_wib'] - df['out_for_delivery_at_wib'])
                                         .dt.total_seconds() / 60).round(2)
    df['minutes_accept_to_delivered'] = ((df['delivered_at_wib'] - df['accepted_at_wib'])
                                         .dt.total_seconds() / 60).round(2)

    df = df.rename(columns=RENAME_MAP)

    df_summary = df[FINAL_COLUMNS].copy()

    for col in NUMERIC_COLUMNS:
        df_summary[col] = pd.to_numeric(df_summary[col], errors='coerce')

    return df_summary
