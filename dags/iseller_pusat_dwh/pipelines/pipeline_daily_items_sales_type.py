"""Pipeline 6/8: daily_items_salestype_summary_pusat"""

import datetime
import pandas as pd
import sqlalchemy.types as types
from common.db_helpers import get_connection, get_engine, get_summary_before, print_before_after

TABLE_NAME = 'daily_items_salestype_summary_pusat'
SUMMARY_COLS = ['total_collected', 'total_quantity']


def run_daily_items_sales_type(start_date, end_date, config_dw):
    print("  [6/8] daily_items_salestype_summary_pusat")

    before = get_summary_before(config_dw, TABLE_NAME, SUMMARY_COLS, start_date, end_date)

    # --- iSeller details (with 1.1x gross multiplier) ---
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT 
            transaction_items.order_id,
            transaction_items.sku,
            transaction_items.product_name AS item_name,
            CASE 
                WHEN transaction_items.product_variant_name IS NOT NULL THEN transaction_items.product_variant_name
                ELSE '-'
            END item_variant_name,
            SUM(transaction_items.quantity) AS total_quantity,
            SUM(transaction_items.total_order_amount) * 1.1 AS total_gross_sales,
            SUM(transaction_items.discount_amount) AS total_discount_amount,
            SUM(transaction_items.promotion_amount) AS total_promotion_amount,
            SUM(transaction_items.subtotal) AS total_net_sales,
            SUM(transaction_items.tax_amount) AS total_taxes,
            SUM(transaction_items.total_amount) AS total_collected
        FROM transactions_items_iseller_pusat transaction_items
        WHERE DATE_FORMAT(load_data_at, "%Y-%m-%d") >= DATE_SUB('{start_date}', INTERVAL 1 DAY)
        GROUP BY 1, 2, 3, 4
    """)
    df_details = pd.DataFrame(cursor.fetchall(), columns=[
        "order_id", "sku", "item_name", "item_variant_name", "total_quantity",
        "total_gross_sales", "total_discount_amount", "total_promotion_amount",
        "total_net_sales", "total_taxes", "total_collected"
    ])
    conn.close()

    # --- iSeller headers ---
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT 
            order_id,
            DATE_FORMAT(transactions.transaction_date, '%Y-%m-%d') AS date,
            CAST(WEEK(transactions.transaction_date, 7) AS CHAR) AS week,
            CAST(MONTH(transactions.transaction_date) AS CHAR) AS month,
            CAST(YEAR(transactions.transaction_date) AS CHAR) AS year,
            CASE 
                WHEN outlet_mapping.jilid IS NOT NULL THEN outlet_mapping.jilid 
                ELSE '-'
            END as jilid,
            sales_type AS sales_type_name,
            transactions.transactions_payment_type_name AS payment_type,
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
            transactions_iseller_pusat transactions
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
        'sales_type_name', 'payment_type', 'ownership_model', 'outlet_name',
        'outlet_brand', 'region', 'provinsi', 'kota'
    ])
    df_headers['jilid'] = df_headers['jilid'].astype(str).str.zfill(5)
    conn.close()

    df_merge = df_headers.merge(df_details, on='order_id', how='inner')
    grouping_col = ['transaction_date', 'jilid', 'ownership_model', 'outlet_name', 'sales_type_name', 'sku', 'item_name']
    sum_col = ['total_quantity', 'total_gross_sales', 'total_collected']
    df_iseller = df_merge.groupby(grouping_col)[sum_col].sum().reset_index()
    df_iseller['transaction_date'] = pd.to_datetime(df_iseller['transaction_date'])

    # --- JIWA+ data from apps summary ---
    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT
            date_format(transaction_date, "%Y-%m-%d") as transaction_date,
            outlet_code as jilid,
            'KBN' as ownership_model,
            outlet_name,
            'JIWA+' as sales_type,
            sku,
            product_name as product_name,
            SUM(total_quantity) as total_quantity,
            SUM(total_gross_sales) as total_gross_sales,
            SUM(total_net_sales) as total_collected
        FROM daily_items_summary_apps_new
        WHERE 
            transaction_date IS NOT NULL
            AND date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND ownership_model = 'KBN'
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    """)
    df_jiwaplus = pd.DataFrame(cursor.fetchall(), columns=[
        'transaction_date', 'jilid', 'ownership_model', 'outlet_name',
        'sales_type_name', 'sku', 'item_name', 'total_quantity', 'total_gross_sales', 'total_collected'
    ])
    conn.close()

    df_final = pd.concat([df_iseller, df_jiwaplus])
    df_final = df_final[['transaction_date', 'jilid', 'ownership_model', 'outlet_name',
                         'sales_type_name', 'sku', 'item_name', 'total_quantity',
                         'total_gross_sales', 'total_collected']]
    df_final['load_data_at'] = datetime.datetime.now()
    df_final['transaction_date'] = pd.to_datetime(df_final['transaction_date'])

    # DELETE — dilakukan setelah df_final siap di memory
    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()
    cursor.execute(f"""
        DELETE FROM daily_items_salestype_summary_pusat
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
    """)
    conn.commit()
    deleted = cursor.rowcount
    print(f"        Deleted: {deleted} records")
    conn.close()

    engine = get_engine(config_dw)
    df_final.to_sql('daily_items_salestype_summary_pusat', con=engine, if_exists='append',
                    dtype={
                        'sku': types.VARCHAR(length=255),
                        'sales_type_name': types.VARCHAR(length=255),
                        'item_name': types.VARCHAR(length=255),
                        'jilid': types.VARCHAR(length=255),
                        'ownership_model': types.VARCHAR(length=255)
                    })

    print(f"        Dumped:  {len(df_final)} records")

    after = {
        'total_collected': float(df_final['total_collected'].sum()),
        'total_quantity': float(df_final['total_quantity'].sum())
    }
    print_before_after(before, after)

    return deleted, len(df_final), before, after
