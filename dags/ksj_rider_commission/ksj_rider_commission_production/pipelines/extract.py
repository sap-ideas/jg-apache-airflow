"""Extract functions - KSJ Link PRODUCTION (PostgreSQL, db d_transaction)."""

import pandas as pd

from common.db_helpers import ksj_link_db_conn


def _read_sql(config_ksj, query, params=None):
    conn = ksj_link_db_conn(config_ksj, config_ksj.d_transaction)
    try:
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()


def get_rider_daily_transaction_summaries(config_ksj, start_date_str, end_date_str):
    query = """
        WITH rider_status AS (
            SELECT
                rider_daily_transaction_summaries.rider_id AS rider_id,
                MAX(rider_daily_transaction_summaries.ownership_status) AS ownership_status
            FROM rider_daily_transaction_summaries
            GROUP BY rider_daily_transaction_summaries.rider_id
        )
        SELECT rider_daily_transaction_summaries.*
        FROM rider_daily_transaction_summaries
        JOIN rider_status ON rider_daily_transaction_summaries.rider_id = rider_status.rider_id
        WHERE rider_daily_transaction_summaries.transaction_date BETWEEN %s AND %s
            AND rider_status.ownership_status = 'KBN'
    """
    return _read_sql(config_ksj, query, params=(start_date_str, end_date_str))


def get_rider_daily_transaction_summary_products(config_ksj, start_date_str, end_date_str):
    query = """
        WITH rider_status AS (
            SELECT
                rider_daily_transaction_summaries.rider_id AS rider_id,
                MAX(rider_daily_transaction_summaries.ownership_status) AS ownership_status
            FROM rider_daily_transaction_summaries
            GROUP BY rider_daily_transaction_summaries.rider_id
        )
        SELECT rider_daily_transaction_summary_products.*
        FROM rider_daily_transaction_summary_products
        JOIN rider_status
            ON rider_daily_transaction_summary_products.rider_id = rider_status.rider_id
        WHERE rider_daily_transaction_summary_products.transaction_date BETWEEN %s AND %s
            AND rider_status.ownership_status = 'KBN'
    """
    return _read_sql(config_ksj, query, params=(start_date_str, end_date_str))


def get_commission_schemes(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_schemes")


def get_commission_scheme_by_weekly_sales(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_scheme_by_weekly_sales")


def get_commission_scheme_by_prod_solds(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_scheme_by_prod_solds")


def get_commission_extra_schemes(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_extra_schemes")


def get_commission_extra_scheme_by_prod_solds(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_extra_scheme_by_prod_solds")
