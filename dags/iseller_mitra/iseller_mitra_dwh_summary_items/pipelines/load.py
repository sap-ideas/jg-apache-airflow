"""Load daily_items_summary_iseller_pusat: ganti baris sources='iseller_mitra' di window (satu transaksi)."""

import sqlalchemy.types as types
from common.mitra_dwh_summary import replace_rows

TABLE_NAME = 'daily_items_summary_iseller_pusat'
OWNED_WHERE = "sources = 'iseller_mitra'"
SUMMARY_COLS = ['total_collected', 'total_quantity']
DTYPE = {
    'sku': types.VARCHAR(length=255),
    'item_name': types.VARCHAR(length=255),
    'item_variant_name': types.VARCHAR(length=255),
    'outlet_code': types.VARCHAR(length=255),
    'ownership_model': types.VARCHAR(length=255),
    'sources': types.VARCHAR(length=255),
}


def load_items(config_dw, df_final, start_date, end_date):
    return replace_rows(config_dw, TABLE_NAME, df_final, start_date, end_date, OWNED_WHERE, DTYPE)
