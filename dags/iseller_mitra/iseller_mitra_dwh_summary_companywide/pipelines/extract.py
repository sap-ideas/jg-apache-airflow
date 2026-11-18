"""Extract daily_outlet_transaction_summary_companywide_new (ISELLER MITRA) dari tabel DWH
daily_transaction_summary_iseller_pusat, jadi harus jalan SETELAH DAG DWH transactions."""

import pandas as pd
from common.db_helpers import get_connection


def extract_companywide(start_date, end_date, config_dw):
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
    df = pd.DataFrame(cursor.fetchall(), columns=[
        "transaction_date", "week", "month", "year", "jilid",
        "outlet_name", "ownership_model", "outlet_brand", "region",
        "provinsi", "kota", "total_gross_sales", "total_discount",
        "total_net_sales", "total_taxes", "total_collected", "total_order", "datasource"
    ])
    conn.close()
    print(f"        Extracted: {len(df):,} rows")
    return df
