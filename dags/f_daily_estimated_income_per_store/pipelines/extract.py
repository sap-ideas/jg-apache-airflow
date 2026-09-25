"""Extract functions - pulls raw transaction data from the Data Lake (MySQL)."""

import pandas as pd

from common.db_helpers import get_connection

# outlet yang mau dihitung uang masuknya
OUTLET_CODES = ["00570", "00989"]


def extract_transactions(config_dw, target_date_str):
    """Raw transactions for OUTLET_CODES on target_date_str, grouped by payment_type.

    Source: transactions_iseller_pusat (MySQL Data Lake).
    """
    connection = get_connection(config_dw, config_dw.db_resource)
    cursor = connection.cursor()

    outlet_list = "', '".join(OUTLET_CODES)

    query = f"""
        SELECT
            DATE_FORMAT(transaction_date, '%Y-%m-%d') AS date,
            outlet_code,
            outlet_name,
            transactions_payment_type_name AS payment_type,
            SUM(total_amount) AS total_amount,
            SUM(total_tax_amount) AS total_tax_amount,
            SUM(subtotal) AS subtotal
        FROM transactions_iseller_pusat
        WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') = '{target_date_str}'
            AND outlet_code IN ('{outlet_list}')
            AND fulfillment_status = 'fulfilled'
            AND status = 'completed'
            AND payment_status = 'paid'
            AND transactions_status = 'success'
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2
    """

    cursor.execute(query)
    records = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(records, columns=columns)

    cursor.close()
    connection.close()
    return df
