"""Extract daily_bundling_summary_iseller_pusat (iSeller Mitra) dari Data Lake."""

import pandas as pd
from common.db_helpers import get_connection
from common.mitra_dwh_summary import OUTLET_MAPPING_SQL


def _fetch(config_dw, sql, columns):
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(sql)
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    conn.close()
    return df


def extract_detail_qty(start_date, end_date, config_dw):
    df = _fetch(config_dw, f"""
        SELECT
            order_id,
            bundling_name,
            SUM(total_qty) as quantity
        FROM
        (
            SELECT
                order_id,
                order_detail_id,
                bundling_id,
                bundling_name,
                SUM(quantity) AS total_qty
            FROM
            (
                SELECT DISTINCT
                    order_id,
                    order_detail_id,
                    bundling_id,
                    product_generic_name AS bundling_name,
                    quantity
                FROM transactions_items_iseller_mitra
                WHERE
                    bundling_id != ''
                    AND type = 'comboset'
                    AND date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            ) AS unique_order_details
            GROUP BY order_id, order_detail_id, bundling_id, bundling_name
        ) AS details
        GROUP BY order_id, bundling_name
    """, ['order_id', 'bundling_name', 'quantity'])
    print(f"        Detail qty: {len(df):,} rows")
    return df


def extract_detail_amount(start_date, end_date, config_dw):
    df = _fetch(config_dw, f"""
        SELECT
            order_id,
            bundling_name,
            SUM(total_amount) as total_amount
        FROM
        (
            SELECT
                order_id,
                order_detail_id,
                bundling_id,
                product_generic_name AS bundling_name,
                SUM(total_amount) as total_amount
            FROM transactions_items_iseller_mitra
            WHERE
                bundling_id != ''
                AND type = 'comboset'
                AND date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY 1, 2, 3, 4
        ) AS details
        GROUP BY order_id, bundling_name
    """, ['order_id', 'bundling_name', 'total_amount'])
    print(f"        Detail amount: {len(df):,} rows")
    return df


def extract_headers(start_date, end_date, config_dw):
    df = _fetch(config_dw, f"""
        SELECT
            transactions.order_id,
            DATE_FORMAT(transactions.transaction_date, '%Y-%m-%d') AS date,
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
            CASE WHEN outlet_mapping.kota IS NOT NULL THEN outlet_mapping.kota ELSE '-' END as kota,
            CASE
                WHEN transactions_payment_type_name = 'GoFood' THEN 'GO FOOD'
                WHEN transactions_payment_type_name = 'ShopeeFood' THEN 'SHOPEE FOOD'
                WHEN transactions_payment_type_name = 'GrabFood' THEN 'GRAB FOOD'
            ELSE 'WALK IN'
            END AS sales_type,
            transactions.transactions_payment_type_name as payment_method
        FROM
            transactions_iseller_mitra transactions
        LEFT JOIN {OUTLET_MAPPING_SQL} outlet_mapping ON transactions.outlet_code = outlet_mapping.jilid
        WHERE
            date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND payment_status = 'paid'
            AND fulfillment_status = 'fulfilled'
            AND transactions_status = 'success'
    """, ['order_id', 'transaction_date', 'week', 'month', 'year', 'jilid',
          'ownership_model', 'outlet_name', 'outlet_brand', 'region', 'provinsi', 'kota',
          'sales_type', 'payment_method'])
    print(f"        Headers: {len(df):,} rows")
    return df
