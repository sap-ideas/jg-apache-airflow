"""
Load backfilling iSeller Mitra: append header + details order baru ke Data Lake dalam 1 TRANSAKSI.

Satu transaksi supaya tidak ada order "sudah ada" di header tanpa details (run berikutnya akan melewatinya
dan details-nya tidak pernah terisi). Sebelum menulis, order_id yang sudah ada dicek ulang di dalam transaksi,
jadi retry Airflow / run ulang tidak menggandakan data.
"""

import pandas as pd
from sqlalchemy import text

from common.db_helpers import get_engine_datalake

HEADERS_TABLE = 'transactions_iseller_mitra'
DETAILS_TABLE = 'transactions_items_iseller_mitra'


def load_missing_orders(config_dw, df_headers, df_details, start_date, end_date, dry_run=False):
    """
    start_date/end_date: rentang transaction_date untuk cek order yang sudah ada (inklusif).
    Return dict: headers_inserted, details_inserted, orders_inserted, orders_skipped_existing.
    dry_run: hitung yang AKAN ditulis, tidak menulis apa pun.
    """
    engine = get_engine_datalake(config_dw)
    existing_stmt = text(
        f"SELECT DISTINCT order_id FROM {HEADERS_TABLE} "
        "WHERE transaction_date >= :s AND transaction_date < DATE_ADD(:e, INTERVAL 1 DAY)"
    )
    with (engine.connect() if dry_run else engine.begin()) as conn:
        existing = {row[0] for row in conn.execute(existing_stmt, {'s': start_date, 'e': end_date})}
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
