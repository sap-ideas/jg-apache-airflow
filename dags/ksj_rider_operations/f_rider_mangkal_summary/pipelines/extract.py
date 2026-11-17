"""Extract functions - pulls raw data from KSJ Link PostgreSQL sources."""

import pandas as pd

from common.db_helpers import ksj_link_db_conn, ksj_link_location_db_conn


def get_rider_total_working_time(config_ksj):
    """Total working minutes per rider for today (Jakarta time).

    Sources: rider_transfer_log + rider_reverse_request (db: d_jiwa_ksj).
    Working time = checkin -> reverse_request_at (only APPROVED reverse requests).
    """
    connection = ksj_link_db_conn(config_ksj, config_ksj.d_jiwa_ksj)
    cursor = connection.cursor()

    query = """
        SELECT
            TO_CHAR(date, 'YYYY-MM-DD') AS operating_date,
            rider_transfer_log.rider_code,
            checkin + INTERVAL '7 hours' AS checkin,
            checkout + INTERVAL '7 hours' AS checkout,
            reverse_request_at + INTERVAL '7 hours' AS reversed_at,
            EXTRACT(EPOCH FROM (reverse_request_at - checkin)) / 60.0 AS total_working_minutes,
            EXTRACT(EPOCH FROM (reverse_request_at - checkin)) / 60.0 / 60.0 AS total_working_hour,
            EXTRACT(EPOCH FROM (checkout - reverse_request_at)) / 60.0 AS total_reverse_process_admin
        FROM
            rider_transfer_log
            JOIN rider_reverse_request ON rider_transfer_log.id = rider_reverse_request.rider_transfer_log_id
        WHERE
            (rider_transfer_log.checkin + INTERVAL '7 hours')::date = (NOW() + INTERVAL '7 hours')::date
            AND checkin IS NOT NULL
            AND rider_reverse_request.status = 'APPROVED'
    """

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df


def get_rider_stationed_logs(config_ksj):
    """Raw rider stationed log events for today (Jakarta time).

    Source: rider_stationed_logs in the KSJ Location-service DB (db: d_location).
    """
    connection = ksj_link_location_db_conn(config_ksj, config_ksj.d_location)
    cursor = connection.cursor()

    query = """
        SELECT
            TO_CHAR(created_at, 'YYYY-MM-DD') AS date,
            id,
            rider_code,
            latitude,
            longitude,
            is_stationed,
            created_at + INTERVAL '7 hours' AS timestamp
        FROM
            rider_stationed_logs
        WHERE
            (created_at + INTERVAL '7 hours')::date = (NOW() + INTERVAL '7 hours')::date
        ORDER BY created_at ASC
    """

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df


def get_raw_transactions(config_ksj):
    """Completed rider transactions for today (Jakarta time).

    Source: transactions table (db: d_transaction).
    """
    connection = ksj_link_db_conn(config_ksj, config_ksj.d_transaction)
    cursor = connection.cursor()

    query = """
        SELECT
            id,
            transaction_datetime_utc,
            transaction_datetime_utc + INTERVAL '7 hour' AS transaction_datetime_wib,
            (transaction_datetime_utc + INTERVAL '7 hour')::date AS transaction_date,
            rider_code,
            rider_name,
            hub_code,
            total_item_qty,
            total_price_after_discount,
            payment_type
        FROM transactions
        WHERE
            transaction_status = 'COMPLETED'
            AND (transaction_datetime_utc + INTERVAL '7 hours')::date = (NOW() + INTERVAL '7 hours')::date
    """

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df


def get_rider_profile_clicks(config_ksj):
    """Daily profile-click counts per rider (KSJ Link app) for today (Jakarta time).

    Counts how many times customers opened a rider's profile in the app
    (`ksj_user_click_tracker`), grouped by calendar day and rider.

    The `timestamp` column is already stored in WIB — do not add INTERVAL '7 hours'
    to it. "Today" is evaluated as the current calendar date in Asia/Jakarta.

    Source DB: jiwa-ksj (same host as d_transaction; `config_ksj.d_jiwa_ksj`).
    Join key to the rest of the pipeline: `rider_id` is treated as `rider_code`.
    """
    connection = ksj_link_db_conn(config_ksj, config_ksj.d_jiwa_ksj)
    cursor = connection.cursor()

    query = """
        SELECT
            TO_CHAR(timestamp, 'YYYY-MM-DD') AS date,
            rider_id::text AS rider_id,
            COUNT(*)::bigint AS total_click_by_user
        FROM ksj_user_click_tracker
        WHERE rider_id IS NOT NULL
            AND timestamp::date = (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta')::date
        GROUP BY 1, 2
    """

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df


def get_hub_mapping(config_ksj):
    """Hub <-> outlet (jilid) mapping master.

    Source: d_sejutajiwa_riders_hubs_mapping JOIN d_outlet_mapping_custom (db: d_transaction).
    """
    connection = ksj_link_db_conn(config_ksj, config_ksj.d_transaction)
    cursor = connection.cursor()

    query = """
        SELECT DISTINCT
            d1.jilid,
            d1.hub_id AS hub_code,
            d1.hub_name,
            d2.business_unit,
            d2.region,
            d2.kota,
            d2.provinsi
        FROM d_sejutajiwa_riders_hubs_mapping d1
        JOIN d_outlet_mapping_custom d2 ON d1.jilid = d2.jilid
    """

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df
