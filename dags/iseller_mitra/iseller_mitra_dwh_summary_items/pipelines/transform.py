"""Transform daily_items_summary_iseller_pusat: gabung header + details, agregasi per tanggal/outlet/item."""

import datetime
import pandas as pd

GROUPING_COLS = ['transaction_date', 'week', 'month', 'year', 'jilid', 'ownership_model',
                 'outlet_name', 'outlet_brand', 'region', 'provinsi', 'kota', 'sku',
                 'item_name', 'item_variant_name']
SUM_COLS = ['total_quantity', 'total_gross_sales', 'total_discount_amount',
            'total_promotion_amount', 'total_net_sales', 'total_taxes', 'total_collected']


def transform_items(df_details, df_headers):
    df_headers = df_headers.copy()
    df_headers['jilid'] = df_headers['jilid'].astype(str).str.zfill(5)
    df_merge = df_headers.merge(df_details, on='order_id', how='inner')
    df_final = df_merge.groupby(GROUPING_COLS)[SUM_COLS].sum().reset_index()
    df_final['load_data_at'] = datetime.datetime.now()
    df_final['transaction_date'] = pd.to_datetime(df_final['transaction_date'])
    df_final['week'] = df_final['week'].astype(int)
    df_final['month'] = df_final['month'].astype(int)
    df_final['year'] = df_final['year'].astype(int)
    df_final['sources'] = 'iseller_mitra'
    print(f"        Transformed: {len(df_final):,} rows")
    return df_final
