"""Transform daily_outlet_transaction_summary_companywide_new: kolom angka numerik + load_data_at."""

import datetime
import pandas as pd

NUMERIC_COLS = ['total_gross_sales', 'total_discount', 'total_net_sales', 'total_taxes', 'total_collected', 'total_order']


def transform_companywide(data_x):
    data_x = data_x.copy()
    for col in NUMERIC_COLS:
        data_x[col] = pd.to_numeric(data_x[col], errors='coerce').fillna(0)
    data_x['load_data_at'] = datetime.datetime.now()
    data_x['transaction_date'] = pd.to_datetime(data_x['transaction_date'])
    return data_x
