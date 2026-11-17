"""
Load backfilling iSeller Pusat: append header + details order baru ke Data Lake dalam 1 TRANSAKSI.

Satu transaksi supaya tidak ada order "sudah ada" di header tanpa details (run berikutnya akan melewatinya
dan details-nya tidak pernah terisi). Sebelum menulis, order_id yang sudah ada dicek ulang di dalam transaksi,
jadi retry Airflow / run ulang tidak menggandakan data.
"""

import pandas as pd
from sqlalchemy import bindparam, text

from common.db_helpers import get_engine_datalake

HEADERS_TABLE = 'transactions_iseller_pusat'
DETAILS_TABLE = 'transactions_items_iseller_pusat'
EXISTING_CHECK_CHUNK = 20000


def load_missing_orders(config_dw, df_headers, df_details, dry_run=False):
    """
    Return dict: headers_inserted, details_inserted, orders_inserted, orders_skipped_existing.
    dry_run: hitung yang AKAN ditulis, tidak menulis apa pun.
    """
    engine = get_engine_datalake(config_dw)
    existing_stmt = text(f"SELECT DISTINCT order_id FROM {HEADERS_TABLE} WHERE order_id IN :ids").bindparams(
        bindparam('ids', expanding=True))
    order_ids = list(df_headers['order_id'].unique())
    with (engine.connect() if dry_run else engine.begin()) as conn:
        # Cek per order_id tanpa filter tanggal (order bisa tertarik di window tanggal lain dari tanggal transaksinya).
        existing = set()
        for i in range(0, len(order_ids), EXISTING_CHECK_CHUNK):
            existing.update(row[0] for row in conn.execute(existing_stmt, {'ids': order_ids[i:i + EXISTING_CHECK_CHUNK]}))
        headers_new = df_headers[~df_headers['order_id'].isin(existing)]
        details_new = df_details[~df_details['order_id'].isin(existing)]
        skipped = df_headers['order_id'].nunique() - headers_new['order_id'].nunique()
        if not dry_run:
            if len(headers_new):
                headers_new.to_sql(HEADERS_TABLE, con=conn, if_exists='append', index=False)
            if len(details_new):
                details_new.to_sql(DETAILS_TABLE, con=conn, if_exists='append', index=False)
    return {
        'headers_inserted': len(headers_new),
        'details_inserted': len(details_new),
        'orders_inserted': headers_new['order_id'].nunique(),
        'orders_skipped_existing': skipped,
        'headers_amount': float(pd.to_numeric(headers_new['total_amount']).sum()) if len(headers_new) else 0.0,
        'details_amount': float(pd.to_numeric(details_new['total_amount']).sum()) if len(details_new) else 0.0,
    }


def load_missing_details(config_dw, df_details, dry_run=False):
    """
    Append details saja untuk order yang headernya sudah ada tapi details-nya kosong. Header TIDAK disentuh.
    Dicek ulang di dalam transaksi: order yang details-nya sudah terisi dilewati.
    Return dict: details_only_orders, details_only_rows, details_only_amount.
    """
    engine = get_engine_datalake(config_dw)
    exist_stmt = text(f"SELECT DISTINCT order_id FROM {DETAILS_TABLE} WHERE order_id IN :ids").bindparams(
        bindparam('ids', expanding=True))
    order_ids = list(df_details['order_id'].unique())
    with (engine.connect() if dry_run else engine.begin()) as conn:
        with_details = set()
        for i in range(0, len(order_ids), EXISTING_CHECK_CHUNK):
            with_details.update(row[0] for row in conn.execute(exist_stmt, {'ids': order_ids[i:i + EXISTING_CHECK_CHUNK]}))
        details_new = df_details[~df_details['order_id'].isin(with_details)]
        if not dry_run and len(details_new):
            details_new.to_sql(DETAILS_TABLE, con=conn, if_exists='append', index=False)
    return {
        'details_only_orders': details_new['order_id'].nunique(),
        'details_only_rows': len(details_new),
        'details_only_amount': float(pd.to_numeric(details_new['total_amount']).sum()) if len(details_new) else 0.0,
    }
