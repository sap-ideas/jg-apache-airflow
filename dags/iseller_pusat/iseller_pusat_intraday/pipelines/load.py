"""
Load intraday iSeller Pusat: upsert PER ORDER header + details ke tabel intraday dalam 1 TRANSAKSI.

Aturan (disepakati 2026-11-23):
- Order yang tidak ada di hasil tarik run ini TIDAK PERNAH dihapus. Satu-satunya penghapusan massal = rollover H-1.
- Order baru: insert header + details.
- Order sudah ada: versi lama dihapus (header + details) lalu versi terbaru di-insert, supaya status/fulfillment/kolom lain
  mengikuti data terbaru dan details yang tadinya kosong (fulfillment 'none') ikut terisi.
- Order yang versi barunya MENYUSUT (baris header atau details lebih sedikit dari yang di tabel) DITAHAN: versi lama
  dipertahankan, karena detail yang sudah pernah masuk tidak boleh hilang hanya karena 1 tarikan tidak membawanya.
- Rollover: baris dengan order_date (UTC) sebelum awal hari ini (00:00 WIB) dihapus di transaksi yang sama.
- `load_data_at` diisi jam WIB (container Airflow berjalan di UTC), 1 nilai untuk header + details satu run. Sengaja beda dari tabel
  Data Lake utama (UTC); `transform.py` tidak diubah karena salinan identik dari pipeline tabel utama.
"""

import pandas as pd
import pendulum
from sqlalchemy import bindparam, text

from common.db_helpers import get_engine_datalake

HEADERS_TABLE = 'transactions_iseller_pusat_intraday'
DETAILS_TABLE = 'transactions_items_iseller_pusat_intraday'
CHUNK = 20000
HELD_SAMPLE = 10


def _count_per_order(conn, table, order_ids):
    """{order_id: jumlah baris} di `table` untuk order_ids (dipecah per CHUNK)."""
    stmt = text(f"SELECT order_id, COUNT(*) FROM {table} WHERE order_id IN :ids GROUP BY order_id").bindparams(
        bindparam('ids', expanding=True))
    counts = {}
    for i in range(0, len(order_ids), CHUNK):
        counts.update({row[0]: row[1] for row in conn.execute(stmt, {'ids': order_ids[i:i + CHUNK]})})
    return counts


def _delete_orders(conn, table, order_ids):
    stmt = text(f"DELETE FROM {table} WHERE order_id IN :ids").bindparams(bindparam('ids', expanding=True))
    for i in range(0, len(order_ids), CHUNK):
        conn.execute(stmt, {'ids': order_ids[i:i + CHUNK]})


def sync_intraday(config_dw, df_headers, df_details, cutoff_utc, dry_run=False):
    """
    df_headers / df_details boleh kosong (run tanpa order baru/berubah tetap menjalankan rollover).
    cutoff_utc: 'YYYY-MM-DD HH:MM:SS' = awal hari ini dalam UTC; baris dengan order_date lebih lama dihapus.
    dry_run: hitung yang AKAN terjadi, tidak menulis apa pun.
    Return dict: orders_new, orders_updated, orders_held, held_orders (order_reference), headers_written, details_written,
    rollover_headers, rollover_details.
    """
    engine = get_engine_datalake(config_dw)
    stats = {'orders_new': 0, 'orders_updated': 0, 'orders_held': 0, 'held_orders': [],
             'headers_written': 0, 'details_written': 0, 'rollover_headers': 0, 'rollover_details': 0}

    with (engine.connect() if dry_run else engine.begin()) as conn:
        # 1) Rollover H-1
        for key, table in (('rollover_headers', HEADERS_TABLE), ('rollover_details', DETAILS_TABLE)):
            if dry_run:
                stats[key] = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE order_date < :c"), {'c': cutoff_utc}).scalar()
            else:
                stats[key] = conn.execute(text(f"DELETE FROM {table} WHERE order_date < :c"), {'c': cutoff_utc}).rowcount

        if df_headers.empty:
            return stats

        # 2) Klasifikasi per order: baru / ter-update / ditahan (menyusut)
        order_ids = list(df_headers['order_id'].unique())
        old_h = _count_per_order(conn, HEADERS_TABLE, order_ids)
        old_d = _count_per_order(conn, DETAILS_TABLE, order_ids)
        new_h = df_headers.groupby('order_id').size()
        new_d = df_details.groupby('order_id').size() if len(df_details) else pd.Series(dtype=int)

        write_ids, update_ids, held_ids = [], [], []
        for oid in order_ids:
            if oid not in old_h:
                write_ids.append(oid)
            elif new_h.get(oid, 0) < old_h[oid] or new_d.get(oid, 0) < old_d.get(oid, 0):
                held_ids.append(oid)
            else:
                write_ids.append(oid)
                update_ids.append(oid)

        headers_new = df_headers[df_headers['order_id'].isin(write_ids)]
        details_new = df_details[df_details['order_id'].isin(write_ids)] if len(df_details) else df_details

        # load_data_at = jam WIB saat run ini (transform memberi jam UTC container)
        loaded_at = pendulum.now('Asia/Jakarta').naive()
        headers_new = headers_new.assign(load_data_at=loaded_at)
        if len(details_new):
            details_new = details_new.assign(load_data_at=loaded_at)

        # 3) Replace per order: hapus versi lama, insert versi terbaru
        if not dry_run and write_ids:
            if update_ids:
                _delete_orders(conn, HEADERS_TABLE, update_ids)
                _delete_orders(conn, DETAILS_TABLE, update_ids)
            headers_new.to_sql(HEADERS_TABLE, con=conn, if_exists='append', index=False)
            if len(details_new):
                details_new.to_sql(DETAILS_TABLE, con=conn, if_exists='append', index=False)

    held_refs = (df_headers[df_headers['order_id'].isin(held_ids)].drop_duplicates('order_id')['order_reference'].tolist())
    stats.update({
        'orders_new': len(write_ids) - len(update_ids),
        'orders_updated': len(update_ids),
        'orders_held': len(held_ids),
        'held_orders': held_refs[:HELD_SAMPLE],
        'headers_written': len(headers_new),
        'details_written': len(details_new),
    })
    return stats
