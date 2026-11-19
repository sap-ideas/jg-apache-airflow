"""Transform - hitung komisi rider harian (produk + summary) dan mingguan.

Port 1:1 dari notebook `etl_rider_apps_weekly_incentive_stg.ipynb` (tanpa sel debug /
duplikat). Logika komisi sengaja TIDAK dirapikan supaya hasilnya identik dengan notebook.
"""

import numpy as np
import pandas as pd

INT_LIKE_COLS = ['current_commission_tier', 'rider_id', 'total_operating_days']

COLUMNS_PRODUCTS = [
    'rider_id', 'transaction_date', 'product_id', 'sku_code', 'short_code', 'product_name', 'qty', 'product_price',
    'total_price', 'total_bundling_discount', 'total_price_after_discount',
    'total_product_sold_commission', 'total_product_sold_commission_extra',
    'commission_per_product_in_idr', 'commission_extra_per_product_in_idr',
]

COLUMNS_DAILY = [
    'rider_id', 'transaction_date', 'rider_code', 'hub_code', 'total_price_cash', 'total_price_after_discount_cash',
    'total_order_cash', 'total_price_qris', 'total_price_after_discount_qris', 'total_order_qris', 'total_item_qty',
    'commission_scheme_id', 'commission_extra_scheme_id', 'commission_type', 'commission_extra_type',
    'total_price_jiwaplus', 'total_price_after_discount_jiwaplus', 'total_order_jiwaplus',
    'attendance_commission', 'attendance_commission_extra',
    'daily_commission', 'daily_commission_extra',
    'daily_sales_commission_percentage', 'daily_sales_commission_extra_percentage',
]

COLUMNS_WEEKLY = [
    'rider_id', 'start_date', 'end_date', 'rider_code', 'hub_code', 'total_revenue', 'total_operating_days',
    'is_weekly_commission', 'commission_scheme_id', 'current_weekly_commission_percentage', 'current_commission_tier',
    'next_revenue_target', 'total_commission', 'total_attendance_commission', 'total_commission_extra',
    'total_attendance_commission_extra',
]


def to_string_cols(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '', 1).isdigit()
                else 'NaN' if pd.isna(x)
                else str(x).strip()
            )


def prepare_schemes(df_schemes, df_by_weekly, df_by_prod, df_extra, df_extra_by_prod, df_trx_summaries):
    """Samakan tipe kolom id jadi string (sama seperti notebook). Mutasi in-place."""
    to_string_cols(df_schemes, ['id'])
    to_string_cols(df_by_weekly, ['id', 'commission_scheme_id'])
    to_string_cols(df_by_prod, ['id', 'commission_scheme_id'])
    df_by_weekly['next_start_range'] = df_by_weekly.groupby('commission_scheme_id')['start_range'].shift(-1)

    to_string_cols(df_extra, ['id'])
    to_string_cols(df_extra_by_prod, ['id', 'commission_extra_scheme_id'])

    to_string_cols(df_trx_summaries, ['commission_scheme_id', 'commission_extra_scheme_id'])


def build_daily_products(df_trx_products, df_trx_summaries, df_by_prod, df_extra_by_prod):
    """rider_daily_commission_summary_products (seluruh minggu)."""
    df = (
        df_trx_products
        .fillna(0)
        .groupby(
            ['rider_id', 'transaction_date', 'product_id', 'sku_code', 'short_code', 'product_name', 'product_price'],
            as_index=False
        )
        .agg(
            qty=('qty', 'sum'),
            total_price=('total_price', 'sum'),
            total_bundling_discount=('total_bundling_discount', 'sum'),
            total_price_after_discount=('total_price_after_discount', 'sum')
        )
    )

    for c in INT_LIKE_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')

    df = df.merge(
        df_trx_summaries[['rider_id', 'transaction_date', 'commission_scheme_id', 'commission_extra_scheme_id',
                          'commission_type', 'commission_extra_type']],
        on=['rider_id', 'transaction_date'],
        how='left',
    )

    for col in ["commission_scheme_id", "commission_extra_scheme_id"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        if col in df_by_prod.columns:
            df_by_prod[col] = pd.to_numeric(df_by_prod[col], errors="coerce").astype("Int64")
        if col in df_extra_by_prod.columns:
            df_extra_by_prod[col] = pd.to_numeric(df_extra_by_prod[col], errors="coerce").astype("Int64")

    df = df.merge(
        df_by_prod[['commission_scheme_id', 'sku_code', 'commission_in_idr']],
        on=['commission_scheme_id', 'sku_code'],
        how='left'
    )
    df = df.merge(
        df_extra_by_prod[['sku_code', 'commission_extra_scheme_id', 'commission_in_idr']],
        on=['commission_extra_scheme_id', 'sku_code'],
        how='left',
        suffixes=('', '_extra')
    )

    df = df.rename(columns={
        'commission_in_idr': 'commission_per_product_in_idr',
        'commission_in_idr_extra': 'commission_extra_per_product_in_idr'
    })

    df['commission_extra_per_product_in_idr'] = np.where(
        (df['commission_extra_type'] == 'product_sold'),
        df['commission_extra_per_product_in_idr'],
        0
    )

    df['total_product_sold_commission'] = df['qty'] * df['commission_per_product_in_idr']
    df['total_product_sold_commission_extra'] = df['qty'] * df['commission_extra_per_product_in_idr']

    df_out = pd.concat([pd.DataFrame(columns=COLUMNS_PRODUCTS), df[COLUMNS_PRODUCTS]], ignore_index=True)
    df_out['transaction_date'] = pd.to_datetime(df_out['transaction_date'], format="%Y-%m-%d", errors="coerce")
    return df_out


def build_daily_summaries(df_trx_summaries, df_schemes, df_extra_schemes, df_daily_products):
    """rider_daily_commission_summaries (seluruh minggu)."""
    grouped_weekly_products_scheme = (
        df_daily_products
        .groupby(['rider_id', 'transaction_date'], as_index=False)
        .agg(
            daily_commission=('total_product_sold_commission', 'sum'),
            daily_commission_extra=('total_product_sold_commission_extra', 'sum')
        )
    ).reset_index(drop=True)

    df = df_trx_summaries.copy()

    for c in INT_LIKE_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')

    df = df.merge(
        df_schemes[['id', 'type', 'attendance_commission_in_idr', 'daily_commission_in_percentage', 'is_active']],
        left_on='commission_scheme_id', right_on='id', how='left', suffixes=('', '_scheme')
    )
    df = df.merge(
        df_extra_schemes[['id', 'type', 'attendance_commission_in_idr', 'daily_commission_in_percentage', 'is_active']],
        left_on='commission_extra_scheme_id', right_on='id', how='left', suffixes=('', '_extra')
    )

    df = df.rename(columns={
        'attendance_commission_in_idr': 'attendance_commission',
        'attendance_commission_in_idr_extra': 'attendance_commission_extra',
        'daily_commission_in_percentage': 'daily_sales_commission_percentage',
        'daily_commission_in_percentage_extra': 'daily_sales_commission_extra_percentage',
    })

    df['total_revenue'] = (
        df['total_price_after_discount_cash'].fillna(0) +
        df['total_price_after_discount_qris'].fillna(0) +
        df['total_price_after_discount_jiwaplus'].fillna(0)
    )
    df['daily_commission'] = df['total_revenue'] * df['daily_sales_commission_percentage'] * 0.01
    df['daily_commission_extra'] = df['total_revenue'] * df['daily_sales_commission_extra_percentage'] * 0.01
    df = df.drop(columns=['total_revenue'])

    df = df.astype(object).where(pd.notnull(df), np.nan)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'], format="%Y-%m-%d", errors="coerce")

    df = df.merge(
        grouped_weekly_products_scheme[['rider_id', 'transaction_date', 'daily_commission', 'daily_commission_extra']],
        on=['rider_id', 'transaction_date'],
        how='left',
        suffixes=('', '_daily')
    )

    # Timpa nilai hanya untuk skema product_sold
    mask_prod_scheme = df['type'] == 'product_sold'
    mask_extra_prod_scheme = df['type_extra'] == 'product_sold'
    df.loc[mask_prod_scheme, 'daily_commission'] = df.loc[mask_prod_scheme, 'daily_commission_daily']
    df.loc[mask_extra_prod_scheme, 'daily_commission_extra'] = df.loc[mask_extra_prod_scheme, 'daily_commission_extra_daily']
    df = df.drop(columns=['daily_commission_daily', 'daily_commission_extra_daily'])

    df_out = pd.concat([pd.DataFrame(columns=COLUMNS_DAILY), df[COLUMNS_DAILY]], ignore_index=True)

    for col in ["commission_scheme_id", "commission_extra_scheme_id"]:
        if col in df_out.columns:
            df_out[col] = pd.to_numeric(df_out[col], errors='coerce').astype("Int64")
    return df_out


def build_weekly_summaries(df_trx_summaries, df_schemes, df_by_weekly, df_daily_summaries, start_date, end_date):
    """rider_weekly_commission_summaries (1 baris per rider)."""
    df_weekly_calc = (
        df_trx_summaries
        .sort_values('transaction_date')
        .groupby(['rider_id', 'rider_code', 'hub_code'], as_index=False)
        .agg({
            'total_price_after_discount_cash': 'sum',
            'total_price_after_discount_qris': 'sum',
            'total_price_after_discount_jiwaplus': 'sum',
            'transaction_date': 'nunique',
            'commission_scheme_id': 'last',
            'commission_type': 'last'
        })
        .rename(columns={'transaction_date': 'total_operating_days'})
    )

    for c in INT_LIKE_COLS:
        if c in df_weekly_calc.columns:
            df_weekly_calc[c] = pd.to_numeric(df_weekly_calc[c], errors='coerce').astype('Int64')

    df_weekly_calc['total_revenue'] = (
        df_weekly_calc['total_price_after_discount_cash'].fillna(0) +
        df_weekly_calc['total_price_after_discount_qris'].fillna(0) +
        df_weekly_calc['total_price_after_discount_jiwaplus'].fillna(0)
    )
    df_weekly_calc = df_weekly_calc.drop(columns=['total_price_after_discount_cash',
                                                  'total_price_after_discount_qris',
                                                  'total_price_after_discount_jiwaplus'])

    df_weekly_calc['start_date'] = start_date
    df_weekly_calc['end_date'] = end_date

    df_scheme = df_by_weekly.copy()
    df_scheme = df_scheme.sort_values(['commission_scheme_id', 'start_range'])
    df_scheme['next_start_range'] = df_scheme.groupby('commission_scheme_id')['start_range'].shift(-1)

    dfj = df_weekly_calc.merge(df_scheme, on='commission_scheme_id', how='left')
    mask = (dfj['total_revenue'] >= dfj['start_range']) & (dfj['total_revenue'] <= dfj['end_range'])
    df_tier = (
        dfj[mask]
        .loc[:, ['rider_id', 'rider_code', 'hub_code',
                 'commission_in_percentage', 'attendance_commission_in_idr',
                 'tier', 'next_start_range']]
    )

    df_weekly_calc = df_weekly_calc.merge(df_tier, on=['rider_id', 'rider_code', 'hub_code'], how='left')

    df_weekly_calc['is_weekly_commission'] = (
        df_weekly_calc['commission_scheme_id'].map(df_schemes.set_index('id')['type']) == 'weekly_sales'
    )
    df_weekly_calc['is_weekly_commission'] = df_weekly_calc['is_weekly_commission'].fillna(False).astype(bool)

    df_weekly_calc['current_commission_tier'] = df_weekly_calc['tier']
    df_weekly_calc['current_weekly_commission_percentage'] = df_weekly_calc['commission_in_percentage']
    df_weekly_calc['next_revenue_target'] = df_weekly_calc['next_start_range']

    df_weekly_calc['total_commission'] = (
        df_weekly_calc['total_revenue'] * df_weekly_calc['current_weekly_commission_percentage'] * 0.01
    ).fillna(0)
    df_weekly_calc['total_attendance_commission'] = (
        df_weekly_calc['total_operating_days'].fillna(0) *
        df_weekly_calc['attendance_commission_in_idr'].fillna(0)
    )

    df_weekly = df_weekly_calc.reindex(columns=COLUMNS_WEEKLY)

    # Rekap mingguan dari tabel daily
    grouped_weekly_scheme_summaries = (
        df_daily_summaries
        .groupby(['rider_id'], as_index=False)
        .agg(
            total_commission=('daily_commission', 'sum'),
            total_attendance_commission=('attendance_commission', 'sum'),
            total_commission_extra=('daily_commission_extra', 'sum'),
            total_attendance_commission_extra=('attendance_commission_extra', 'sum')
        )
    ).reset_index(drop=True)

    df_weekly = df_weekly.merge(
        grouped_weekly_scheme_summaries[['rider_id', 'total_commission', 'total_attendance_commission',
                                         'total_commission_extra', 'total_attendance_commission_extra']],
        on=['rider_id'],
        how='left',
        suffixes=('', '_grp')
    )

    # Skema non-weekly (daily_sales / product_sold): pakai jumlah tabel daily
    scheme_type = df_weekly['commission_scheme_id'].map(df_schemes.set_index('id')['type'])
    mask_prod_scheme = scheme_type.isin(['daily_sales', 'product_sold'])

    df_weekly.loc[mask_prod_scheme, 'total_commission'] = df_weekly.loc[mask_prod_scheme, 'total_commission_grp']
    df_weekly.loc[mask_prod_scheme, 'total_attendance_commission'] = df_weekly.loc[mask_prod_scheme, 'total_attendance_commission_grp']
    df_weekly['total_commission_extra'] = df_weekly['total_commission_extra_grp']
    df_weekly['total_attendance_commission_extra'] = df_weekly['total_attendance_commission_extra_grp']

    df_weekly = df_weekly.drop(
        columns=['total_commission_grp', 'total_attendance_commission_grp',
                 'total_commission_extra_grp', 'total_attendance_commission_extra_grp']
    )
    df_weekly = df_weekly.rename(columns={"commission_scheme_id": "weekly_commission_scheme_id"})
    df_weekly['weekly_commission_scheme_id'] = df_weekly['weekly_commission_scheme_id'].replace('NaN', None)
    return df_weekly
