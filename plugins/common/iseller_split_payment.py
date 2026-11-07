"""
Split payment & transaksi "hantu" iSeller (Pusat dan Mitra), satu logika untuk semua transform.

Grain header = 1 baris per TRANSAKSI, tapi kolom nominal API (total_order_amount ... total_amount) adalah nominal
ORDER. Tanpa penanganan, order dengan >1 transaksi `sale` terhitung dobel di header, dan details ikut tergandakan
karena di-merge ke header per order_id.

Aturan (terverifikasi di data Maret & Oktober 2026 dan dengan order asli di backoffice iSeller):
1. Transaksi `sale` bernilai 0 padahal ada transaksi `sale` lain yang dibayar = hantu -> dibuang.
   Order yang SEMUA transaksi sale-nya 0 hanya disisakan 1 baris.
2. Order dengan >1 transaksi `sale` (split payment asli): kolom nominal header dikalikan porsi pembayaran
   (amount transaksi / jumlah amount semua sale order). Jumlah per order tetap = nominal order.
3. Details hanya ditempel ke 1 transaksi `sale` per order split (amount terbesar) -> 1 set item per order.

Baris refund dan order dengan 1 transaksi `sale` tidak berubah sama sekali.
"""
import pandas as pd

MONEY_COLS_HEADER = ['total_order_amount', 'total_discount_amount', 'total_promotion_amount',
                     'subtotal', 'total_tax_amount', 'total_amount']


def allocate_split_payment(df_merged, amount_col='tx_amount'):
    """
    df_merged: header per transaksi, punya kolom order_id, transactions_type, transactions_status,
               MONEY_COLS_HEADER, dan `amount_col` (= transactions[].amount dari API).
    Return (df_merged baru tanpa baris hantu + nominal split terbagi, index baris header yang boleh menerima details).
    Pakai index itu untuk merge details: df_final_headers.loc[details_header_index, right_cols].
    """
    df = df_merged.copy()
    order = df['order_id']
    df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)

    is_sale = (df['transactions_type'] == 'sale') & (df['transactions_status'] == 'success')
    max_amt = df[amount_col].where(is_sale, 0).groupby(order).transform('max')
    sale_rank = is_sale.astype(int).groupby(order).cumsum()

    ghost = is_sale & (df[amount_col] <= 0) & ((max_amt > 0) | (sale_rank > 1))
    n_ghost = int(ghost.sum())
    df = df[~ghost].copy()
    order = df['order_id']

    is_sale = (df['transactions_type'] == 'sale') & (df['transactions_status'] == 'success')
    n_sale = is_sale.groupby(order).transform('sum')
    is_split = is_sale & (n_sale > 1)
    sale_sum = df[amount_col].where(is_sale, 0).groupby(order).transform('sum')
    share = (df[amount_col] / sale_sum).where(is_split, 1.0)
    for col in MONEY_COLS_HEADER:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float) * share

    primary = (df[is_split].sort_values(amount_col, ascending=False, kind='stable')
               .groupby('order_id').head(1).index)
    details_header_index = df.index[~(is_split & ~df.index.isin(primary))]

    if n_ghost or is_split.any():
        print(f"        split payment: {df.loc[is_split, 'order_id'].nunique()} order ({int(is_split.sum())} baris header) | "
              f"transaksi hantu dibuang: {n_ghost} baris")
    return df, details_header_index
