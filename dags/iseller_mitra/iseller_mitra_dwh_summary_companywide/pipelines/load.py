"""Load daily_outlet_transaction_summary_companywide_new: ganti HANYA baris ISELLER + MITRA di window (satu transaksi).

Baris lain dipegang pipeline pusat (iseller_pusat_fulfillment/pipelines/pipeline_daily_companywide.py memakai
kondisi kebalikannya), jadi tidak ada baris yang ditulis oleh dua pipeline.
"""

import sqlalchemy.types as types
from common.mitra_dwh_summary import replace_rows

TABLE_NAME = 'daily_outlet_transaction_summary_companywide_new'
OWNED_WHERE = "datasource = 'ISELLER' AND ownership_model = 'MITRA'"
SUMMARY_COLS = ['total_collected', 'total_order']
DTYPE = {
    'jilid': types.VARCHAR(length=255),
    'outlet_name': types.VARCHAR(length=255),
    'ownership_model': types.VARCHAR(length=255),
    'datasource': types.VARCHAR(length=255),
}


def load_companywide(config_dw, df_final, start_date, end_date):
    return replace_rows(config_dw, TABLE_NAME, df_final, start_date, end_date, OWNED_WHERE, DTYPE)
