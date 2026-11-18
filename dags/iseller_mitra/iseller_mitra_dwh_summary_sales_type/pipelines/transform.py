"""Transform daily_sales_type_summary_iseller_pusat: tipe kolom, sales_type_payment_method, huruf besar, sources."""

import datetime
import pandas as pd


def transform_sales_type(data_x):
    data_x = data_x.copy()
    data_x['load_data_at'] = datetime.datetime.now()
    data_x['transaction_date'] = pd.to_datetime(data_x['transaction_date'])
    data_x['sales_type_payment_method'] = data_x['sales_type'] + data_x['payment_method']
    data_x['week'] = data_x['week'].astype(int)
    data_x['month'] = data_x['month'].astype(int)
    data_x['year'] = data_x['year'].astype(int)
    data_x['sales_type'] = data_x['sales_type'].str.upper()
    data_x['payment_method'] = data_x['payment_method'].str.upper()
    data_x['sources'] = 'iseller_mitra'
    return data_x
