"""Pipeline 6: daily_outlet_transaction_summary_companywide_new (bagian ISELLER + MITRA)

Sumbernya daily_transaction_summary_iseller_pusat, jadi harus jalan SETELAH pipeline_daily_transactions.
Hanya menyentuh baris datasource='ISELLER' AND ownership_model='MITRA'. Sisanya dipegang pipeline pusat
(iseller_pusat_fulfillment/pipelines/pipeline_daily_companywide.py memakai kondisi kebalikannya),
jadi tidak ada baris yang ditulis oleh dua pipeline.
"""

import datetime
import pandas as pd
import sqlalchemy.types as types
from common.db_helpers import get_connection, get_engine, get_summary_before, print_before_after

TABLE_NAME = 'daily_outlet_transaction_summary_companywide_new'
SUMMARY_COLS = ['total_collected', 'total_order']
OWNED_ROWS = "datasource = 'ISELLER' AND ownership_model = 'MITRA'"
NUMERIC_COLS = ['total_gross_sales', 'total_discount', 'total_net_sales', 'total_taxes', 'total_collected', 'total_order']


def run_daily_companywide(start_date, end_date, config_dw):
    print("  [6/6] daily_outlet_transaction_summary_companywide_new (ISELLER MITRA)")

    before = get_summary_before(config_dw, TABLE_NAME, SUMMARY_COLS, start_date, end_date, where_extra=OWNED_ROWS)

    # SELECT
    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT
            transaction_date,
            week, month, year,
            LPAD(outlet_code, 5, "0") AS jilid,
            outlet_name, ownership_model, outlet_brand,
            region, provinsi, kota,
            total_gross_sales,
            total_discount_amount + total_promotion_amount AS total_discount,
            total_net_sales, total_taxes, total_collected, total_order,
            'ISELLER' AS datasource
        FROM daily_transaction_summary_iseller_pusat
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND ownership_model = 'MITRA'
    """)
    data_x = pd.DataFrame(cursor.fetchall(), columns=[
        "transaction_date", "week", "month", "year", "jilid",
        "outlet_name", "ownership_model", "outlet_brand", "region",
        "provinsi", "kota", "total_gross_sales", "total_discount",
        "total_net_sales", "total_taxes", "total_collected", "total_order", "datasource"
    ])
    conn.close()

    # TRANSFORM
    for col in NUMERIC_COLS:
        data_x[col] = pd.to_numeric(data_x[col], errors='coerce').fillna(0)
    data_x['load_data_at'] = datetime.datetime.now()
    data_x['transaction_date'] = pd.to_datetime(data_x['transaction_date'])

    # DELETE — dilakukan setelah data baru siap di memory
    conn = get_connection(config_dw, config_dw.db_target)
    cursor = conn.cursor()
    cursor.execute(f"""
        DELETE FROM {TABLE_NAME}
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND {OWNED_ROWS}
    """)
    conn.commit()
    deleted = cursor.rowcount
    print(f"        Deleted: {deleted} records")
    conn.close()

    # DUMP
    engine = get_engine(config_dw)
    data_x.to_sql(TABLE_NAME, con=engine, if_exists='append',
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
