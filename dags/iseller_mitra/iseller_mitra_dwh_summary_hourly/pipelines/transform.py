"""Transform hourly_transaction_summary_iseller_pusat: tipe kolom + load_data_at + sources."""

import datetime
import pandas as pd


def transform_hourly(data_x):
    data_x = data_x.copy()
    data_x['load_data_at'] = datetime.datetime.now()
    data_x['transaction_date'] = pd.to_datetime(data_x['transaction_date'])
    data_x['transaction_hour'] = data_x['transaction_hour'].astype(int)
    data_x['week'] = data_x['week'].astype(int)
    data_x['month'] = data_x['month'].astype(int)
    data_x['year'] = data_x['year'].astype(int)
    data_x['sources'] = 'iseller_mitra'
    return data_x
