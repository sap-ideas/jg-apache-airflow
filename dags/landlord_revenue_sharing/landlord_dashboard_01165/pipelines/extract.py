"""Extract - ambil data income outlet 01165 dari Data Warehouse (MySQL)."""

import pandas as pd

from common.db_helpers import get_connection

OUTLET_CODE = "01165"


def extract_income(config_dw, start_date):
    """Grain: date x outlet_name x payment bucket. Payment type yang belum ter-mapping / NULL masuk 'other'.

    Source: daily_est_income_per_store_for_landlord (data_warehouse_jiwa).
    """
    connection = get_connection(config_dw, config_dw.db_target)
    query = f"""
        SELECT
            DATE_FORMAT(`date`, '%Y-%m-%d') AS `date`,
            outlet_name,
            CASE
                WHEN payment_type = 'Cash'          THEN 'cash'
                WHEN payment_type = 'BRI'           THEN 'bri'
                WHEN payment_type = 'Go Food'       THEN 'go_food'
                WHEN payment_type = 'Grab Food'     THEN 'grab_food'
                WHEN payment_type = 'Grab Dine Out' THEN 'grab_dine_out'
                WHEN payment_type = 'Shopee Food'   THEN 'shopee_food'
                WHEN payment_type = 'Tokopedia Go'  THEN 'tokopedia_go'
                WHEN payment_type = 'Jiwa+'         THEN 'jiwaplus'
                WHEN payment_type IN ('iSeller QRIS', 'iseller_qris', 'isellerpay_qris', 'iSeller Pay QRIS') THEN 'iseller_qris'
                WHEN payment_type = 'QPON'          THEN 'qpon'
                ELSE 'other'
            END AS bucket,
            SUM(total_order) AS total_order,
            SUM(subtotal - COALESCE(sales_type_commission, 0)) AS income
        FROM daily_est_income_per_store_for_landlord
        WHERE outlet_code = '{OUTLET_CODE}'
            AND `date` >= '{start_date}'
        GROUP BY 1, 2, 3
        ORDER BY 1
    """
    try:
        df = pd.read_sql(query, connection)
    finally:
        connection.close()

    print(f"  Extracted {len(df)} rows (outlet {OUTLET_CODE}, date >= {start_date})")
    return df
