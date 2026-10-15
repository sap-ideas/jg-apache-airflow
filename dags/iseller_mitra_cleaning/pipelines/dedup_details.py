"""
Dedup details iSeller Mitra (transactions_items_iseller_mitra) - aman untuk comboset.

Ide: data yang dobel ke-load ke DL sebanyak `dup_factor` kali. Yang dibuang adalah kelipatannya,
bukan kembarannya, karena combo bisa sah punya row identik (produk sama dipilih di beberapa slot).

  - Non-combo : simpan 1 row per DETAIL_KEY
  - Combo     : simpan rows_per_key / dup_factor row per DETAIL_KEY

dup_factor combo HANYA dari master bundling (API GetProducts):
    dup_factor = row per order_detail combo / bundling_slot_count   (harus bilangan bulat >= 1)

Tidak ada cadangan. Kalau dup_factor tidak bisa ditentukan dari master, combo TIDAK dipotong
(dup_factor = 1) dan masuk needs_manual_check:
  - not_in_master : bundling_id tidak ada di master (mis. paket sudah dinonaktifkan)
  - slot_mismatch : row per order_detail bukan kelipatan bulat dari jumlah slot di master
Kalau rows_per_key tidak habis dibagi dup_factor, combo juga tidak dipotong + needs_manual_check.
"""

import numpy as np
import pandas as pd

DETAIL_KEY = [
    'order_id', 'order_detail_id', 'type', 'bundling_id', 'product_id',
    'sku', 'status', 'payment_status', 'transactions_type',
]
# 1 combo group = 1 order_detail combo (tanpa kolom produk)
COMBO_GROUP_KEY = ['order_id', 'order_detail_id', 'bundling_id', 'status', 'payment_status', 'transactions_type']
HEADER_KEY = ['order_id', 'transaction_id']
# type details yang dikenal. Type lain (mis. paket dengan nama baru) bisa salah dianggap non-combo -> STOP
KNOWN_DETAIL_TYPES = ('standard', 'variant', 'comboset')


def add_dedup_key_stats(df):
    """rows_per_key + row_seq_in_key per DETAIL_KEY (NaN-safe)."""
    df = df.copy()
    key_values = df[DETAIL_KEY].astype(object).where(df[DETAIL_KEY].notna(), '<NA>').astype(str)
    df['dedup_key'] = pd.util.hash_pandas_object(key_values, index=False).values
    by_key = df.groupby('dedup_key', sort=False)
    df['rows_per_key'] = by_key['dedup_key'].transform('size')
    df['row_seq_in_key'] = by_key.cumcount()
    df['is_combo'] = df['type'].eq('comboset')
    return df


def _as_whole_number(series):
    """Nilai bulat >= 1, selain itu NaN (tidak bisa dipakai)."""
    return series.where((series >= 1) & (series % 1 == 0))


def compute_dup_factor(df, bundling_slot_count):
    """
    dup_factor per baris combo dari master bundling.
    dup_factor_source: 'bundling' | 'not_in_master' | 'slot_mismatch' (dua terakhir -> dup_factor 1).
    """
    df = df.copy()

    df['combo_group_rows'] = df[df['is_combo']].groupby(COMBO_GROUP_KEY, dropna=False)['dedup_key'].transform('size')
    df['bundling_slot_count'] = df['bundling_id'].map(bundling_slot_count)
    df['dup_factor_from_bundling'] = _as_whole_number(df['combo_group_rows'] / df['bundling_slot_count'])

    df['dup_factor'] = df['dup_factor_from_bundling']
    df['dup_factor_source'] = np.select(
        [df['dup_factor'].notna(), df['bundling_slot_count'].isna()],
        ['bundling', 'not_in_master'],
        default='slot_mismatch',
    )
    df['dup_factor'] = df['dup_factor'].fillna(1).astype(int)
    return df


def dedup_details_v2(df_details, bundling_slot_count):
    """
    Return (df_clean, df_work).
      df_clean : kolom asli saja, siap di-dump ke DL
      df_work  : df_details + kolom bantu (rows_per_key, dup_factor, keep, ...)
    """
    unknown = sorted(set(df_details['type'].fillna('<NULL>')) - set(KNOWN_DETAIL_TYPES))
    if unknown:
        raise ValueError(f"type details tidak dikenal: {unknown}. Dedup dibatalkan, DL tidak disentuh.")
    original_cols = list(df_details.columns)
    df = add_dedup_key_stats(df_details)
    df = compute_dup_factor(df, bundling_slot_count)

    is_divisible = (df['rows_per_key'] % df['dup_factor']) == 0
    df['rows_to_keep'] = np.where(
        df['is_combo'],
        np.where(is_divisible, df['rows_per_key'] // df['dup_factor'], df['rows_per_key']),
        1,
    )
    # tidak habis dibagi, atau dup_factor tidak bisa ditentukan dari master -> tidak dipotong
    df['needs_manual_check'] = df['is_combo'] & (~is_divisible | df['dup_factor_source'].ne('bundling'))
    df['keep'] = df['row_seq_in_key'] < df['rows_to_keep']

    df_clean = df.loc[df['keep'], original_cols]
    return df_clean, df


def _sum_by_date(df, label):
    return (df.assign(tgl=pd.to_datetime(df['transaction_date']).dt.strftime('%Y-%m-%d'))
              .groupby('tgl')['total_amount'].sum().rename(label))


def build_dedup_reports(df_headers, df_headers_clean, df_details, df_details_clean, df_work):
    """
    Return (summary: dict, reports: dict[str, DataFrame]).
    reports: needs_manual_check, slot_mismatch, bundling_not_in_master, accuracy_per_date
    """
    is_combo_raw = df_details['type'].eq('comboset')
    is_combo_clean = df_details_clean['type'].eq('comboset')

    combo_groups = df_work[df_work['is_combo']].drop_duplicates(COMBO_GROUP_KEY)

    # cek slot setelah cleaning
    after = df_work[df_work['keep'] & df_work['is_combo']]
    slot_check = (after.groupby(COMBO_GROUP_KEY, dropna=False)
                       .agg(order_reference=('order_reference', 'first'),
                            bundling_name=('product_generic_name', 'first'),
                            rows_after=('dedup_key', 'size'),
                            bundling_slot_count=('bundling_slot_count', 'first'),
                            dup_factor=('dup_factor', 'first'),
                            dup_factor_source=('dup_factor_source', 'first'))
                       .reset_index())
    in_master = slot_check['bundling_slot_count'].notna()
    matches = slot_check['rows_after'] == slot_check['bundling_slot_count']
    slot_mismatch = slot_check[in_master & ~matches]

    bundling_not_in_master = (slot_check[~in_master]
                              .groupby(['bundling_id', 'bundling_name'], dropna=False)
                              .agg(n_order_detail=('order_detail_id', 'size'),
                                   dup_factor_source=('dup_factor_source', lambda x: ', '.join(sorted(set(x)))))
                              .reset_index()
                              .sort_values('n_order_detail', ascending=False))

    needs_manual_check = (df_work.loc[df_work['needs_manual_check'],
                                      ['order_id', 'order_reference', 'order_detail_id', 'bundling_id',
                                       'product_generic_name', 'product_name', 'sku', 'transactions_type',
                                       'rows_per_key', 'bundling_slot_count', 'dup_factor', 'dup_factor_source']]
                          .drop_duplicates()
                          .rename(columns={'product_generic_name': 'bundling_name'}))

    accuracy = pd.concat([
        _sum_by_date(df_headers, 'headers_raw'),
        _sum_by_date(df_details, 'details_raw'),
        _sum_by_date(df_headers_clean, 'headers_clean'),
        _sum_by_date(df_details_clean, 'details_clean'),
    ], axis=1).fillna(0)
    accuracy.loc['TOTAL'] = accuracy.sum()
    accuracy['acc_details_%'] = (accuracy['details_clean'] / accuracy['headers_clean'].replace(0, np.nan) * 100).round(3)
    accuracy = accuracy.reset_index().rename(columns={'index': 'tgl'})

    n_groups = len(slot_check)
    summary = {
        'headers_raw': len(df_headers), 'headers_clean': len(df_headers_clean),
        'details_raw': len(df_details), 'details_clean': len(df_details_clean),
        'details_noncombo_raw': int((~is_combo_raw).sum()), 'details_noncombo_clean': int((~is_combo_clean).sum()),
        'details_combo_raw': int(is_combo_raw.sum()), 'details_combo_clean': int(is_combo_clean.sum()),
        'needs_manual_check_orders': int(needs_manual_check['order_id'].nunique()),
        'dup_factor_source_counts': {s: int(n) for s, n in combo_groups['dup_factor_source'].value_counts().items()},
        'combo_order_details': n_groups,
        'combo_in_master': int(in_master.sum()),
        'combo_slot_match_pct': round(matches[in_master].mean() * 100, 2) if in_master.any() else None,
        'combo_not_in_master': int((~in_master).sum()),
        'slot_mismatch_count': len(slot_mismatch),
        'bundling_in_period': int(df_details.loc[is_combo_raw, 'bundling_id'].nunique()),
        'bundling_not_in_master': len(bundling_not_in_master),
    }
    reports = {
        'needs_manual_check': needs_manual_check,
        'slot_mismatch': slot_mismatch,
        'bundling_not_in_master': bundling_not_in_master,
        'accuracy_per_date': accuracy,
    }
    return summary, reports
