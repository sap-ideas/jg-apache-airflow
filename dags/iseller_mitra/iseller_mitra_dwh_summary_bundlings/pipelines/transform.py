"""Transform daily_bundling_summary_iseller_pusat: gabung qty + amount + header, agregasi per tanggal/outlet/bundling."""

import datetime
import pandas as pd

GROUPING_COLS = ['transaction_date', 'week', 'month', 'year', 'jilid', 'ownership_model',
                 'outlet_name', 'outlet_brand', 'region', 'provinsi', 'kota',
                 'sales_type', 'payment_method', 'bundling_name']
SUM_COLS = ['quantity', 'total_amount']


def transform_bundlings(df_detail_qty, df_detail_amount, df_headers):
    df_headers = df_headers.copy()
    df_headers['jilid'] = df_headers['jilid'].astype(str).str.zfill(5)
    df_merge_details = pd.merge(df_detail_qty, df_detail_amount, on=['order_id', 'bundling_name'], how='inner')
    # Order split payment punya >1 baris header (1 per transaksi); nominalnya sudah dibagi per porsi, tapi details hanya 1 set.
    # 1 header per order supaya details tidak tergandakan saat merge.
    df_merge_headers = df_merge_details.merge(df_headers.drop_duplicates(subset='order_id'), on='order_id', how='inner')
    df_final = df_merge_headers.groupby(GROUPING_COLS)[SUM_COLS].sum().reset_index()
    df_final['load_data_at'] = datetime.datetime.now()
    df_final['transaction_date'] = pd.to_datetime(df_final['transaction_date'])
    df_final['week'] = df_final['week'].astype(int)
    df_final['month'] = df_final['month'].astype(int)
    df_final['year'] = df_final['year'].astype(int)
    df_final['sources'] = 'iseller_mitra'
    print(f"        Transformed: {len(df_final):,} rows")
    return df_final
