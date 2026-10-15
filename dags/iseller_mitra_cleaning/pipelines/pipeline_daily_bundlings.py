"""Pipeline 2: daily_bundling_summary_iseller_pusat"""

import datetime
import pandas as pd
import sqlalchemy.types as types
from common.db_helpers import get_connection, get_engine, get_summary_before, print_before_after

TABLE_NAME = 'daily_bundling_summary_iseller_pusat'
SUMMARY_COLS = ['total_amount', 'quantity']


def run_daily_bundlings(start_date, end_date, config_dw):
    print("  [2/5] daily_bundling_summary_iseller_pusat")

    # BEFORE summary
    before = get_summary_before(config_dw, TABLE_NAME, SUMMARY_COLS, start_date, end_date, sources='iseller_mitra')

    # SELECT detail qty
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
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
    """)
    df_detail_qty = pd.DataFrame(cursor.fetchall(), columns=['order_id', 'bundling_name', 'quantity'])
    conn.close()

    # SELECT detail amount
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
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
    """)
    df_detail_amount = pd.DataFrame(cursor.fetchall(), columns=['order_id', 'bundling_name', 'total_amount'])
    conn.close()

    # SELECT headers
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
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
        LEFT JOIN 
        (
            SELECT
                outlet_mapping.jilid,
                outlet_mapping.customer_name as outlet_name,
                outlet_mapping.region,
                outlet_mapping.provinsi,
                outlet_mapping.kota,
                outlet_mapping.ownership_model,
                outlet_mapping.outlet_brand,
                kbn_transition_outlets.start_date,
                kbn_transition_outlets.old_ownership_model,
                kbn_transition_outlets.old_outlet_brand
            FROM 
            (
                SELECT DISTINCT
                    jilid, customer_name, region, provinsi, kota, ownership_model,
                    CASE WHEN outlet_brand_mapping.brand_name IS NOT NULL THEN outlet_brand_mapping.brand_name 
                         ELSE outlet_mapping.brand
                    END outlet_brand
                FROM outlet_mapping
                LEFT JOIN outlet_brand_mapping ON outlet_mapping.brand = outlet_brand_mapping.brand
            ) outlet_mapping 
            LEFT JOIN 
            (
                SELECT 
                    jilid, 
                    date_format(start_date, "%Y-%m-%d") as start_date, 
                    ownership_model as old_ownership_model, 
                    outlet_brand_mapping.brand_name as old_outlet_brand
                FROM kbn_transition_outlets
                LEFT JOIN outlet_brand_mapping ON kbn_transition_outlets.brand = outlet_brand_mapping.brand
            ) kbn_transition_outlets ON outlet_mapping.jilid = kbn_transition_outlets.jilid
        ) outlet_mapping ON transactions.outlet_code = outlet_mapping.jilid 
        WHERE 
            date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND payment_status = 'paid'
            AND fulfillment_status = 'fulfilled'
            AND transactions_status = 'success'
    """)
    df_headers = pd.DataFrame(cursor.fetchall(), columns=[
        'order_id', 'transaction_date', 'week', 'month', 'year', 'jilid',
        'ownership_model', 'outlet_name', 'outlet_brand', 'region', 'provinsi', 'kota',
        'sales_type', 'payment_method'
    ])
    df_headers['jilid'] = df_headers['jilid'].astype(str).str.zfill(5)
    conn.close()

    # TRANSFORM
    df_merge_details = pd.merge(df_detail_qty, df_detail_amount, on=['order_id', 'bundling_name'], how='inner')
    df_merge_headers = df_merge_details.merge(df_headers, on='order_id', how='inner')

    grouping_col = ['transaction_date', 'week', 'month', 'year', 'jilid', 'ownership_model',
                    'outlet_name', 'outlet_brand', 'region', 'provinsi', 'kota',
                    'sales_type', 'payment_method', 'bundling_name']
    sum_col = ['quantity', 'total_amount']
    df_final = df_merge_headers.groupby(grouping_col)[sum_col].sum().reset_index()
    df_final['load_data_at'] = datetime.datetime.now()
    df_final['transaction_date'] = pd.to_datetime(df_final['transaction_date'])
    df_final['week'] = df_final['week'].astype(int)
    df_final['month'] = df_final['month'].astype(int)
    df_final['year'] = df_final['year'].astype(int)
    df_final['sources'] = 'iseller_mitra'

    # DELETE — dilakukan setelah df_final siap di memory
    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()
    cursor.execute(f"""
        DELETE FROM daily_bundling_summary_iseller_pusat
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND sources = 'iseller_mitra'
    """)
    conn.commit()
    deleted = cursor.rowcount
    print(f"        Deleted: {deleted} records")
    conn.close()

    # DUMP
    engine = get_engine(config_dw)
    df_final.to_sql('daily_bundling_summary_iseller_pusat', con=engine, if_exists='append',
                    dtype={
                        'sku': types.VARCHAR(length=255),
                        'item_name': types.VARCHAR(length=255),
                        'item_variant_name': types.VARCHAR(length=255),
                        'jilid': types.VARCHAR(length=255),
                        'ownership_model': types.VARCHAR(length=255),
                        'bundling_name': types.VARCHAR(length=255),
                        'sources': types.VARCHAR(length=255)
                    })

    print(f"        Dumped:  {len(df_final)} records")

    # AFTER summary
    after = {
        'total_amount': float(df_final['total_amount'].sum()),
        'quantity': float(df_final['quantity'].sum())
    }
    print_before_after(before, after)

    return deleted, len(df_final), before, after
