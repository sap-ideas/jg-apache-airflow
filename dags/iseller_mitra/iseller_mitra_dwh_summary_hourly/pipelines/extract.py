"""Extract hourly_transaction_summary_iseller_pusat (iSeller Mitra) dari Data Lake."""

import pandas as pd
from common.db_helpers import get_connection
from common.mitra_dwh_summary import OUTLET_MAPPING_SQL


def extract_hourly(start_date, end_date, config_dw):
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT
            HOUR(STR_TO_DATE(transactions.transaction_date, "%Y-%m-%d %T")) as hour,
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
            COUNT(DISTINCT transactions.order_id) AS total_order
        FROM
            transactions_iseller_mitra transactions
            JOIN {OUTLET_MAPPING_SQL} outlet_mapping ON transactions.outlet_code = outlet_mapping.jilid
            WHERE
                date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
                AND payment_status = 'paid'
                AND fulfillment_status = 'fulfilled'
                AND transactions_status = 'success'
                AND sales_type NOT LIKE '%jiwa+%'
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    """)
    df = pd.DataFrame(cursor.fetchall(), columns=[
        "transaction_hour", "transaction_date", "week", "month", "year", "outlet_code",
        "ownership_model", "outlet_name", "outlet_brand", "region", "provinsi", "kota",
        "total_gross_sales", "total_discount_amount", "total_promotion_amount",
        "total_net_sales", "total_taxes", "total_collected", "total_order"
    ])
    conn.close()
    print(f"        Extracted: {len(df):,} rows")
    return df
