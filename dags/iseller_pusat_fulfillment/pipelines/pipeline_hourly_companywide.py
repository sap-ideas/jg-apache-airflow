"""Pipeline 8/8: hourly_outlet_transaction_summary_companywide_new"""

import datetime
import pandas as pd
import sqlalchemy.types as types
from common.db_helpers import get_connection, get_engine, get_summary_before, print_before_after

TABLE_NAME = 'hourly_outlet_transaction_summary_companywide_new'
SUMMARY_COLS = ['total_collected', 'total_order']


def run_hourly_companywide(start_date, end_date, config_dw):
    print("  [8/8] hourly_outlet_transaction_summary_companywide_new")

    before = get_summary_before(config_dw, TABLE_NAME, SUMMARY_COLS, start_date, end_date)

    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT 
            transaction_date,
            WEEK(transaction_date, 7) as week,
            MONTH(transaction_date) as month, 
            YEAR(transaction_date) as year,
            transaction_hour,
            outlet_code as jilid, 
            outlet_name,
            ownership_model,
            outlet_brand,
            region, 
            provinsi,
            kota,
            SUM(total_gross_sales) as total_gross_sales,
            SUM(total_discount) as total_discount,
            SUM(total_net_sales) as total_net_sales,
            SUM(total_taxes) as total_taxes,
            SUM(total_collected) as total_collected,
            SUM(total_order) as total_order,
            'MOKA' as datasource
        FROM 
        (
            SELECT 
                date_format(hourly_outlet_transaction_summary_new.transaction_date, "%Y-%m-%d") as transaction_date,
                transaction_hour,
                hourly_outlet_transaction_summary_new.outlet_id,
                hourly_outlet_transaction_summary_new.outlet_code,
                outlet_name,
                ownership_model,
                outlet_brand,
                region, 
                provinsi,
                kota,
                SUM(total_order) - SUM(total_refund_order * 2) as total_order,
                SUM(total_gross_sales) as total_gross_sales,
                SUM(total_discount) as total_discount,
                SUM(total_net_sales) as total_net_sales,
                SUM(total_taxes) as total_taxes,
                SUM(total_collected) as total_collected
            FROM hourly_outlet_transaction_summary_new
            WHERE 
                date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
                AND outlet_id != 217043
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        ) hourly_outlet_transaction_summary_new 
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

        UNION 

        SELECT 
            transaction_date, week, month, year, transaction_hour,
            LPAD(outlet_code, 5, "0") as jilid,
            outlet_name, ownership_model, outlet_brand,
            region, provinsi, kota,
            total_gross_sales, total_discount, total_net_sales,
            total_taxes, total_collected, total_order,
            'JIWA+' as datasource
        FROM hourly_outlet_transaction_summary_apps_new
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
        
        UNION 

        SELECT 
            transaction_date, week, month, year, transaction_hour,
            LPAD(outlet_code, 5, "0") as jilid,
            outlet_name, ownership_model, brand_name,
            region, provinsi, kota,
            total_gross_sales, total_discount, total_net_sales,
            total_taxes, total_collected, total_order,
            'ARTHAPOS' as datasource
        FROM hourly_outlet_transaction_summary_arthapos_new
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
        
        UNION 
        
        SELECT 
            transaction_date, week, month, year, transaction_hour,
            LPAD(outlet_code, 5, "0") as jilid,
            outlet_name, ownership_model,
            outlet_brand as brand_name,
            region, provinsi, kota,
            total_gross_sales,
            total_discount_amount + total_promotion_amount as total_discount,
            total_net_sales, total_taxes, total_collected, total_order,
            'ISELLER' as datasource
        FROM hourly_transaction_summary_iseller_pusat
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
    """)
    data_x = pd.DataFrame(cursor.fetchall(), columns=[
        "transaction_date", "week", "month", "year", "transaction_hour", "jilid",
        "outlet_name", "ownership_model", "outlet_brand", "region",
        "provinsi", "kota", "total_gross_sales", "total_discount",
        "total_net_sales", "total_taxes", "total_collected", "total_order", "datasource"
    ])
    conn.close()

    numeric_cols = ['total_gross_sales', 'total_discount', 'total_net_sales', 'total_taxes', 'total_collected', 'total_order']
    for col in numeric_cols:
        data_x[col] = pd.to_numeric(data_x[col], errors='coerce').fillna(0)

    data_x['load_data_at'] = datetime.datetime.now()
    data_x['transaction_date'] = pd.to_datetime(data_x['transaction_date'], format='mixed')

    # DELETE — dilakukan setelah data_x siap di memory
    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()
    cursor.execute(f"""
        DELETE FROM hourly_outlet_transaction_summary_companywide_new
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
    """)
    conn.commit()
    deleted = cursor.rowcount
    print(f"        Deleted: {deleted} records")
    conn.close()

    engine = get_engine(config_dw)
    data_x.to_sql('hourly_outlet_transaction_summary_companywide_new', con=engine, if_exists='append',
                  dtype={
                      'jilid': types.VARCHAR(length=255),
                      'outlet_name': types.VARCHAR(length=255),
                      'ownership_model': types.VARCHAR(length=255),
                      'datasource': types.VARCHAR(length=255)
                  })

    print(f"        Dumped:  {len(data_x)} records")

    after = {
        'total_collected': float(data_x['total_collected'].sum()),
        'total_order': float(data_x['total_order'].sum())
    }
    print_before_after(before, after)

    return deleted, len(data_x), before, after
