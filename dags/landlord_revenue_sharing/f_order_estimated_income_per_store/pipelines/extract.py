"""Extract functions - pulls order-level transactions from the Data Lake and the daily summary from the DWH (MySQL)."""

import pandas as pd

from common.db_helpers import get_connection

# outlet yang mau dihitung (dipakai di extract, validasi & DELETE load). Outlet baru cukup ditambah di sini.
OUTLET_CODES = ["00570"]


def _fetch_df(connection, sql):
    cursor = connection.cursor()
    cursor.execute(sql)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cursor.close()
    return pd.DataFrame(records, columns=columns)


def extract_order_transactions(config_dw, start_date_str, end_date_str):
    """Raw transactions (1 baris per order_id + transaction_id) for OUTLET_CODES in the date range.

    Source: transactions_iseller_pusat (MySQL Data Lake). Row level, tanpa GROUP BY.
    """
    outlet_list = "', '".join(OUTLET_CODES)

    query = f"""
        SELECT
            DATE_FORMAT(transaction_date, '%Y-%m-%d') AS date,
            transaction_date,
            outlet_code,
            outlet_name,
            order_id,
            transaction_id,
            order_reference,
            channel,
            transactions_payment_type_name AS payment_type_raw,
            total_order_amount,
            total_discount_amount,
            total_promotion_amount,
            subtotal,
            total_tax_amount AS tax_amount,
            total_amount AS total_sales
        FROM transactions_iseller_pusat
        WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') >= '{start_date_str}'
            AND DATE_FORMAT(transaction_date, '%Y-%m-%d') <= '{end_date_str}'
            AND outlet_code IN ('{outlet_list}')
            AND fulfillment_status = 'fulfilled'
            AND status = 'completed'
            AND payment_status = 'paid'
            AND transactions_status = 'success'
        ORDER BY transaction_date
    """

    connection = get_connection(config_dw, config_dw.db_resource)
    try:
        return _fetch_df(connection, query)
    finally:
        connection.close()


def extract_daily_summary(config_dw, start_date_str, end_date_str):
    """Rows of daily_est_income_per_store_for_landlord (DWH) in the same range & outlets - dipakai untuk validasi."""
    outlet_list = "', '".join(OUTLET_CODES)

    query = f"""
        SELECT date, outlet_code, payment_type, subtotal, tax_amount, total_sales, sales_type_commission, total_order
        FROM daily_est_income_per_store_for_landlord
        WHERE date BETWEEN '{start_date_str}' AND '{end_date_str}'
            AND outlet_code IN ('{outlet_list}')
    """

    connection = get_connection(config_dw, config_dw.db_target)
    try:
        return _fetch_df(connection, query)
    finally:
        connection.close()
