"""Extract functions - pulls raw data from KSJ Link (PostgreSQL) and DWH (MySQL)."""

import pandas as pd

from common.db_helpers import get_connection, ksj_link_db_conn


def get_rider_hourly_transactions(config_ksj, start_date_str, end_date_str):
    """Completed rider transactions grouped by rider, date, hour, and payment type.

    Source: transactions + transaction_products (db: d_transaction).
    Only COMPLETED transactions, excludes rider JW000065.
    """
    connection = ksj_link_db_conn(config_ksj, config_ksj.d_transaction)
    cursor = connection.cursor()

    query = """
        SELECT
            t.rider_code,
            t.transaction_date AS date,

            EXTRACT(
                WEEK FROM
                COALESCE(
                    t.transaction_datetime_utc,
                    t.transaction_date::timestamp
                )
            )::INT AS week,

            EXTRACT(
                HOUR FROM transaction_datetime_utc + INTERVAL '7 hours'
            )::INT AS hour,

            t.payment_type,

            COUNT(DISTINCT t.id)              AS total_orders,
            SUM(tp.qty)                       AS total_qty,
            SUM(tp.total_price_after_discount) AS total_sales

        FROM transactions t

        JOIN transaction_products tp
            ON t.id = tp.transaction_id

        WHERE
            t.transaction_status = 'COMPLETED'
            AND t.rider_code != 'JW000065'
            AND t.transaction_date BETWEEN '{start}' AND '{end}'

        GROUP BY 1, 2, 3, 4, 5
    """.format(start=start_date_str, end=end_date_str)

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df


def get_rider_timeslot_tito(config_ksj, start_date_str, end_date_str):
    """Hourly time-slot slots per rider derived from TI-TO (check-in / check-out) data.

    Each row represents one working hour-slot for a rider on a given date.
    Source: rider_transfer_log (db: d_jiwa_ksj).
    """
    connection = ksj_link_db_conn(config_ksj, config_ksj.d_jiwa_ksj)
    cursor = connection.cursor()

    query = """
        WITH rider_shift AS (
            SELECT
                rider_code,
                date,
                checkin  + INTERVAL '7 hours' AS checkin,
                checkout + INTERVAL '7 hours' AS checkout
            FROM rider_transfer_log
            WHERE checkin  IS NOT NULL
              AND checkout IS NOT NULL
              AND date BETWEEN '{start}' AND '{end}'
        )

        SELECT
            rider_code,
            date,
            checkin,
            checkout,
            TO_CHAR(timeslot, 'HH24') AS hour_slot
        FROM rider_shift
        CROSS JOIN LATERAL
            generate_series(
                date_trunc('hour', checkin),
                date_trunc('hour', checkout),
                interval '1 hour'
            ) AS timeslot
        ORDER BY date, rider_code, hour_slot;
    """.format(start=start_date_str, end=end_date_str)

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df


def get_rider_mapping(config_dw):
    """Rider-to-hub mapping with region/kota/provinsi/sejuta_jiwa_type.

    Source: sejutajiwa_riders_hubs_mapping JOIN outlet_mapping_custom (MySQL DWH db_resource).
    Only HUB-type riders are relevant for this pipeline, but the full mapping
    is returned so the caller can filter as needed.
    """
    connection = get_connection(config_dw, config_dw.db_resource)
    cursor = connection.cursor()

    query = """
        SELECT DISTINCT
            drm.rider_id,
            drm.jilid,
            drm.hub_name,
            domc.region,
            domc.provinsi,
            domc.kota,
            domc.sejuta_jiwa_type
        FROM sejutajiwa_riders_hubs_mapping  drm
        JOIN outlet_mapping_custom           domc ON drm.jilid = domc.jilid
    """

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df
