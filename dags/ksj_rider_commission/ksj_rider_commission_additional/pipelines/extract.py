"""Extract functions - KSJ Link PRODUCTION (PostgreSQL, db d_transaction) - rider BLITZ/HMS, vendor JW."""

import pandas as pd

from common.db_helpers import ksj_link_db_conn


def _read_sql(config_ksj, query, params=None):
    conn = ksj_link_db_conn(config_ksj, config_ksj.d_transaction)
    try:
        return pd.read_sql(query, conn, params=params)
    finally:
        conn.close()


def get_rider_daily_transaction_summaries(config_ksj, start_date_str, end_date_str):
    """Rider BLITZ% / HMS% (tanpa filter ownership_status)."""
    query = """
        SELECT *
        FROM rider_daily_transaction_summaries
        WHERE transaction_date BETWEEN %s AND %s
            AND (rider_code like %s or rider_code like %s)
    """
    return _read_sql(config_ksj, query, params=(start_date_str, end_date_str, 'BLITZ%', 'HMS%'))


def get_rider_daily_transaction_summary_products(config_ksj, start_date_str, end_date_str):
    query = """
        SELECT *
        FROM rider_daily_transaction_summary_products
        JOIN (
            SELECT DISTINCT rider_code, rider_id
            FROM rider_daily_transaction_summaries
            WHERE transaction_date BETWEEN %s AND %s
            AND (rider_code like %s or rider_code like %s)
        ) AS filtered_summaries
        ON rider_daily_transaction_summary_products.rider_id = filtered_summaries.rider_id
        WHERE rider_daily_transaction_summary_products.transaction_date BETWEEN %s AND %s
    """
    df = _read_sql(
        config_ksj, query,
        params=(start_date_str, end_date_str, 'BLITZ%', 'HMS%', start_date_str, end_date_str),
    )
    return df.loc[:, ~df.columns.duplicated()]  # drop duplicate-named columns (rider_id), keep first


def get_commission_schemes(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_schemes WHERE vendor_code = 'JW'")


def get_commission_scheme_by_weekly_sales(config_ksj):
    return _read_sql(config_ksj, """
        SELECT * FROM commission_scheme_by_weekly_sales
        WHERE commission_scheme_id IN (
            SELECT id FROM commission_schemes WHERE vendor_code = 'JW'
        )
    """)


def get_commission_scheme_by_prod_solds(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_scheme_by_prod_solds")


def get_commission_extra_schemes(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_extra_schemes")


def get_commission_extra_scheme_by_prod_solds(config_ksj):
    return _read_sql(config_ksj, "SELECT * FROM commission_extra_scheme_by_prod_solds")
