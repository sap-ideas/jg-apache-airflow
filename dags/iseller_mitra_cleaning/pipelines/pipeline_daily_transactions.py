"""Pipeline 3: daily_transaction_summary_iseller_pusat"""

import datetime
import pandas as pd
import sqlalchemy.types as types
from common.db_helpers import get_connection, get_engine, get_summary_before, print_before_after

TABLE_NAME = 'daily_transaction_summary_iseller_pusat'
SUMMARY_COLS = ['total_collected', 'total_order']


def run_daily_transactions(start_date, end_date, config_dw):
    print("  [3/5] daily_transaction_summary_iseller_pusat")

    # BEFORE summary
    before = get_summary_before(config_dw, TABLE_NAME, SUMMARY_COLS, start_date, end_date, sources='iseller_mitra')

    # SELECT
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT 
            DATE_FORMAT(transaction_date, '%Y-%m-%d') AS date,
            CAST(WEEK(transaction_date, 7) AS CHAR) AS week,
            CAST(MONTH(transaction_date) AS CHAR) AS month,
            CAST(YEAR(transaction_date) AS CHAR) AS year,
            transactions.outlet_code AS jilid,
            CASE 
                WHEN date_format(transaction_date, "%Y-%m-%d") < outlet_mapping.start_date THEN outlet_mapping.old_ownership_model
                WHEN outlet_mapping.ownership_model IS NOT NULL THEN outlet_mapping.ownership_model
                ELSE '-'
            END as ownership_model,
            CASE 
                WHEN outlet_mapping.outlet_name IS NOT NULL THEN outlet_mapping.outlet_name 
                ELSE '-'
            END as outlet_name,
            CASE 
                WHEN date_format(transaction_date, "%Y-%m-%d") < outlet_mapping.start_date THEN outlet_mapping.old_outlet_brand
                WHEN outlet_mapping.outlet_brand IS NOT NULL THEN outlet_mapping.outlet_brand 
                ELSE '-'
            END as outlet_brand,
            CASE WHEN outlet_mapping.region IS NOT NULL THEN outlet_mapping.region ELSE '-' END as region,
            CASE WHEN outlet_mapping.provinsi IS NOT NULL THEN outlet_mapping.provinsi ELSE '-' END as provinsi,
            CASE WHEN outlet_mapping.kota IS NOT NULL THEN outlet_mapping.kota ELSE '-' END as kota,
            SUM(transactions.total_order_amount) AS total_gross_sales,
            SUM(transactions.total_discount_amount) AS total_discount_amount,
            SUM(transactions.total_promotion_amount) AS total_promotion_amount,
            SUM(transactions.subtotal) AS total_net_sales,
            SUM(transactions.total_tax_amount) AS total_taxes,
            SUM(transactions.total_amount) AS total_collected,
            COUNT(transactions.order_id) AS total_order 
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
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
    """)
    data_x = pd.DataFrame(cursor.fetchall(), columns=[
        "transaction_date", "week", "month", "year", "outlet_code",
        "ownership_model", "outlet_name", "outlet_brand", "region", "provinsi", "kota",
        "total_gross_sales", "total_discount_amount", "total_promotion_amount",
        "total_net_sales", "total_taxes", "total_collected", "total_order"
    ])
    conn.close()

    # TRANSFORM
    data_x['load_data_at'] = datetime.datetime.now()
    data_x['transaction_date'] = pd.to_datetime(data_x['transaction_date'])
    data_x['week'] = data_x['week'].astype(int)
    data_x['month'] = data_x['month'].astype(int)
    data_x['year'] = data_x['year'].astype(int)
    data_x['sources'] = 'iseller_mitra'

    # DELETE — dilakukan setelah data baru siap di memory
    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()
    cursor.execute(f"""
        DELETE FROM daily_transaction_summary_iseller_pusat
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND sources = 'iseller_mitra'
    """)
    conn.commit()
    deleted = cursor.rowcount
    print(f"        Deleted: {deleted} records")
    conn.close()

    # DUMP
    engine = get_engine(config_dw)
    data_x.to_sql('daily_transaction_summary_iseller_pusat', con=engine, if_exists='append',
                  dtype={
                      'outlet_code': types.VARCHAR(length=255),
                      'ownership_model': types.VARCHAR(length=255),
                      'outlet_name': types.VARCHAR(length=255),
                      'outlet_brand': types.VARCHAR(length=255),
                      'sources': types.VARCHAR(length=255)
                  })

    print(f"        Dumped:  {len(data_x)} records")

    # AFTER summary
    after = {
        'total_collected': float(data_x['total_collected'].sum()),
        'total_order': float(data_x['total_order'].sum())
    }
    print_before_after(before, after)

    return deleted, len(data_x), before, after
