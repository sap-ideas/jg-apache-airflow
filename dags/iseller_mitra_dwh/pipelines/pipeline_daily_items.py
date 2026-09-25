"""Pipeline 1: daily_items_summary_iseller_pusat"""

import datetime
import pandas as pd
import sqlalchemy.types as types
from common.db_helpers import get_connection, get_engine, get_summary_before, print_before_after

TABLE_NAME = 'daily_items_summary_iseller_pusat'
SUMMARY_COLS = ['total_collected', 'total_quantity']


def run_daily_items(start_date, end_date, config_dw):
    print("  [1/5] daily_items_summary_iseller_pusat")

    # BEFORE summary
    before = get_summary_before(config_dw, TABLE_NAME, SUMMARY_COLS, start_date, end_date, sources='iseller_mitra')

    # SELECT details
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
    df_details = pd.DataFrame(cursor.fetchall(), columns=[
        "order_id", "sku", "item_name", "item_variant_name", "total_quantity",
        "total_gross_sales", "total_discount_amount", "total_promotion_amount",
        "total_net_sales", "total_taxes", "total_collected"
    ])
    conn.close()

    # SELECT headers
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
            AND sales_type NOT LIKE '%jiwa+%'
    """)
    df_headers = pd.DataFrame(cursor.fetchall(), columns=[
        'order_id', 'transaction_date', 'week', 'month', 'year', 'jilid',
        'ownership_model', 'outlet_name', 'outlet_brand', 'region', 'provinsi', 'kota'
    ])
    df_headers['jilid'] = df_headers['jilid'].astype(str).str.zfill(5)
    conn.close()

    # TRANSFORM
    df_merge = df_headers.merge(df_details, on='order_id', how='inner')
    grouping_col = ['transaction_date', 'week', 'month', 'year', 'jilid', 'ownership_model',
                    'outlet_name', 'outlet_brand', 'region', 'provinsi', 'kota', 'sku',
                    'item_name', 'item_variant_name']
    sum_col = ['total_quantity', 'total_gross_sales', 'total_discount_amount',
               'total_promotion_amount', 'total_net_sales', 'total_taxes', 'total_collected']
    df_final = df_merge.groupby(grouping_col)[sum_col].sum().reset_index()
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
        DELETE FROM daily_items_summary_iseller_pusat
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND sources = 'iseller_mitra'
    """)
    conn.commit()
    deleted = cursor.rowcount
    print(f"        Deleted: {deleted} records")
    conn.close()

    # DUMP
    engine = get_engine(config_dw)
    df_final.to_sql('daily_items_summary_iseller_pusat', con=engine, if_exists='append',
                    dtype={
                        'sku': types.VARCHAR(length=255),
                        'item_name': types.VARCHAR(length=255),
                        'item_variant_name': types.VARCHAR(length=255),
                        'outlet_code': types.VARCHAR(length=255),
                        'ownership_model': types.VARCHAR(length=255),
                        'sources': types.VARCHAR(length=255)
                    })

    print(f"        Dumped:  {len(df_final)} records")

    # AFTER summary
    after = {
        'total_collected': float(df_final['total_collected'].sum()),
        'total_quantity': float(df_final['total_quantity'].sum())
    }
    print_before_after(before, after)

    return deleted, len(df_final), before, after
