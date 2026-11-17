"""Extract functions - pulls raw data from JiwaPlus (PostgreSQL) and KSJ Link (PostgreSQL)."""

import pandas as pd

from common.db_helpers import jiwaplus_db_conn, ksj_link_db_conn


def get_jiwaplus_consumer_app_user_session(config_jiwaplus, start_date_str, end_date_str):
    """Daily user sessions from the JiwaPlus consumer app.

    Counts distinct sessions and users who opened JiwaPlus on each date.
    Source: outlet_user_distances (JiwaPlus PostgreSQL).
    """
    connection = jiwaplus_db_conn(config_jiwaplus)
    cursor = connection.cursor()

    query = """
        SELECT
            TO_CHAR(created_at + INTERVAL '7 hours', 'YYYY-MM-DD') AS date,
            COUNT(DISTINCT id)      AS total_session,
            COUNT(DISTINCT id_user) AS total_users_open_jiwaplus
        FROM outlet_user_distances
        WHERE
            TO_CHAR(created_at + INTERVAL '7 hours', 'YYYY-MM-DD') BETWEEN '{start}' AND '{end}'
        GROUP BY 1
    """.format(start=start_date_str, end=end_date_str)

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df


def get_ksj_consumer_app_user_session(config_ksj, start_date_str, end_date_str):
    """KSJ consumer app daily usage: rider found/not-found + user click interactions.

    Combines three sources into one row per date:
      - ksj_rider_found_logs: how many users found/didn't find a rider
      - ksj_user_click_tracker: how many users clicked direction / WhatsApp

    Source: d_jiwa_ksj (KSJ Link PostgreSQL).
    """
    connection = ksj_link_db_conn(config_ksj, config_ksj.d_jiwa_ksj)
    cursor = connection.cursor()

    query = """
        SELECT
            open_app.*,
            click_rider.total_click,
            click_rider.total_click_direction,
            click_rider.total_wa_direction,
            click_rider.total_click / open_app.total_rider_found AS click_ratio_to_found_rider
        FROM
        (
            SELECT
                date,
                SUM(total_users)                                                           AS total_users_open_ksj,
                SUM(CASE WHEN type = 'Rider Found'     THEN total_users ELSE 0 END)        AS total_rider_found,
                SUM(CASE WHEN type = 'Rider Not Found' THEN total_users ELSE 0 END)        AS total_rider_not_found,
                SUM(CASE WHEN type = 'Rider Found'     THEN total_users ELSE 0 END)
                    / SUM(total_users)                                                     AS rider_found_perc
            FROM
            (
                SELECT
                    TO_CHAR(created_at + INTERVAL '7 hours', 'YYYY-MM-DD') AS date,
                    'Rider Found'                                           AS type,
                    COUNT(*)                                                AS total_session,
                    COUNT(DISTINCT user_id)                                 AS total_users
                FROM ksj_rider_found_logs
                WHERE
                    TO_CHAR(created_at + INTERVAL '7 hours', 'YYYY-MM-DD') BETWEEN '{start}' AND '{end}'
                    AND riders_found_count >= 1
                GROUP BY 1

                UNION

                SELECT
                    TO_CHAR(created_at + INTERVAL '7 hours', 'YYYY-MM-DD') AS date,
                    'Rider Not Found'                                       AS type,
                    COUNT(*)                                                AS total_session,
                    COUNT(DISTINCT user_id)                                 AS total_users
                FROM ksj_rider_found_logs
                WHERE
                    TO_CHAR(created_at + INTERVAL '7 hours', 'YYYY-MM-DD') BETWEEN '{start}' AND '{end}'
                    AND riders_found_count = 0
                GROUP BY 1
            ) AS open_screen
            GROUP BY 1
        ) AS open_app

        JOIN
        (
            SELECT
                date,
                SUM(total_user)                                                               AS total_click,
                SUM(CASE WHEN type = 'Click Direction' THEN total_user ELSE 0 END)            AS total_click_direction,
                SUM(CASE WHEN type = 'Click Whatsapp'  THEN total_user ELSE 0 END)            AS total_wa_direction,
                SUM(CASE WHEN type = 'Click Whatsapp'  THEN total_user ELSE 0 END)
                    / SUM(total_user)                                                         AS total_wa_perc
            FROM
            (
                SELECT
                    TO_CHAR(timestamp, 'YYYY-MM-DD') AS date,
                    'Click Direction'               AS type,
                    COUNT(DISTINCT event_id)         AS total_event,
                    COUNT(DISTINCT user_id)          AS total_user
                FROM ksj_user_click_tracker
                WHERE
                    event_name  = 'contactRider'
                    AND event_label = 'direction'
                    AND TO_CHAR(timestamp, 'YYYY-MM-DD') BETWEEN '{start}' AND '{end}'
                GROUP BY 1

                UNION

                SELECT
                    TO_CHAR(timestamp, 'YYYY-MM-DD') AS date,
                    'Click Whatsapp'                AS type,
                    COUNT(DISTINCT event_id)         AS total_event,
                    COUNT(DISTINCT user_id)          AS total_user
                FROM ksj_user_click_tracker
                WHERE
                    event_name  = 'contactRider'
                    AND event_label = 'whatsapp'
                    AND TO_CHAR(timestamp, 'YYYY-MM-DD') BETWEEN '{start}' AND '{end}'
                GROUP BY 1
            ) AS click_data
            GROUP BY 1
        ) AS click_rider ON open_app.date = click_rider.date

        ORDER BY 1 ASC
    """.format(start=start_date_str, end=end_date_str)

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df
