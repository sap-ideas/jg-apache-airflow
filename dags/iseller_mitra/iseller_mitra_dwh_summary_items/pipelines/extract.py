"""Extract daily_items_summary_iseller_pusat (iSeller Mitra) dari Data Lake."""

import pandas as pd
from common.db_helpers import get_connection
from common.mitra_dwh_summary import OUTLET_MAPPING_SQL


def extract_details(start_date, end_date, config_dw):
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT
            ti.order_id,
            ti.sku,
            ti.product_name AS item_name,
            CASE
                WHEN ti.product_variant_name IS NOT NULL THEN ti.product_variant_name
                ELSE '-'
            END AS item_variant_name,
            SUM(ti.quantity) AS total_quantity,
            SUM(ti.total_order_amount) AS total_gross_sales,
            SUM(ti.discount_amount) AS total_discount_amount,
            SUM(ti.promotion_amount) AS total_promotion_amount,
            SUM(ti.subtotal) AS total_net_sales,
            SUM(ti.tax_amount) AS total_taxes,
            SUM(ti.total_amount) AS total_collected
        FROM transactions_items_iseller_mitra ti
        WHERE DATE_FORMAT(ti.transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
            AND ti.payment_status = 'paid'
            AND ti.transactions_status = 'success'
            AND ti.sales_type NOT LIKE '%jiwa+%'
        GROUP BY 1, 2, 3, 4
    """)
    df = pd.DataFrame(cursor.fetchall(), columns=[
        "order_id", "sku", "item_name", "item_variant_name", "total_quantity",
        "total_gross_sales", "total_discount_amount", "total_promotion_amount",
        "total_net_sales", "total_taxes", "total_collected"
    ])
    conn.close()
    print(f"        Details: {len(df):,} rows")
    return df


def extract_headers(start_date, end_date, config_dw):
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT DISTINCT
            transactions.order_id,
            DATE_FORMAT(transactions.transaction_date, '%Y-%m-%d') AS transaction_date,
            CAST(WEEK(transactions.transaction_date, 7) AS CHAR) AS week,
            CAST(MONTH(transactions.transaction_date) AS CHAR) AS month,
            CAST(YEAR(transactions.transaction_date) AS CHAR) AS year,
            transactions.outlet_code AS jilid,
            CASE
                WHEN date_format(transactions.transaction_date, "%Y-%m-%d") < outlet_mapping.start_date THEN outlet_mapping.old_ownership_model
                WHEN outlet_mapping.ownership_model IS NOT NULL THEN outlet_mapping.ownership_model
                ELSE '-'
            END as ownership_model,
            CASE
                WHEN outlet_mapping.outlet_name IS NOT NULL THEN outlet_mapping.outlet_name
                ELSE '-'
            END as outlet_name,
            CASE
                WHEN date_format(transactions.transaction_date, "%Y-%m-%d") < outlet_mapping.start_date THEN outlet_mapping.old_outlet_brand
                WHEN outlet_mapping.outlet_brand IS NOT NULL THEN outlet_mapping.outlet_brand
                ELSE '-'
            END as outlet_brand,
            CASE WHEN outlet_mapping.region IS NOT NULL THEN outlet_mapping.region ELSE '-' END as region,
            CASE WHEN outlet_mapping.provinsi IS NOT NULL THEN outlet_mapping.provinsi ELSE '-' END as provinsi,
            CASE WHEN outlet_mapping.kota IS NOT NULL THEN outlet_mapping.kota ELSE '-' END as kota
        FROM
            transactions_iseller_mitra transactions
        LEFT JOIN {OUTLET_MAPPING_SQL} outlet_mapping ON transactions.outlet_code = outlet_mapping.jilid
        WHERE
            date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND payment_status = 'paid'
            AND fulfillment_status = 'fulfilled'
            AND transactions_status = 'success'
            AND sales_type NOT LIKE '%jiwa+%'
    """)
    df = pd.DataFrame(cursor.fetchall(), columns=[
        'order_id', 'transaction_date', 'week', 'month', 'year', 'jilid',
        'ownership_model', 'outlet_name', 'outlet_brand', 'region', 'provinsi', 'kota'
    ])
    conn.close()
    print(f"        Headers: {len(df):,} rows")
    return df
