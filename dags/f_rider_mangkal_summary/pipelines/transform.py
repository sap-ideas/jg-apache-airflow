"""Transform functions - build mangkal sessions and final daily summary."""

import numpy as np
import pandas as pd


def create_mangkal_session(df_logs):
    """Group consecutive `is_stationed=True` events into mangkal sessions per rider.

    Returns:
        detail_df: per-event dataframe with session_id assigned
        session_summary: one row per session with start/end/duration
    """
    df = df_logs.copy()

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.sort_values(['rider_code', 'timestamp', 'id']).reset_index(drop=True)

    # Force boolean — some sources return string 'true'/'false'
    if df['is_stationed'].dtype != bool:
        df['is_stationed'] = df['is_stationed'].astype(str).str.lower().map({
            'true': True,
            'false': False,
        })

    grp = df.groupby('rider_code', sort=False)

    # A session starts on the first True after a non-True row
    prev_is_stationed = grp['is_stationed'].shift(fill_value=False)
    df['is_session_start'] = df['is_stationed'] & (~prev_is_stationed)

    df['session_seq'] = df['is_session_start'].groupby(df['rider_code']).cumsum()

    df['session_id'] = np.where(
        df['session_seq'] > 0,
        df['rider_code'] + '_S' + df['session_seq'].astype(int).astype(str).str.zfill(4),
        pd.NA,
    )
    df.loc[df['session_seq'] == 0, 'session_id'] = pd.NA

    session_df = df[df['session_id'].notna()].copy()

    if session_df.empty:
        empty_summary = pd.DataFrame(columns=[
            'rider_code', 'session_seq', 'session_id', 'session_start',
            'session_last_false', 'session_last_event', 'total_logs',
            'total_true_logs', 'total_false_logs', 'session_end',
            'session_duration_minutes',
        ])
        return df, empty_summary

    def get_first_true_ts(x):
        mask = session_df.loc[x.index, 'is_stationed'] == True  # noqa: E712
        return x[mask].min() if mask.any() else pd.NaT

    def get_last_false_ts(x):
        mask = session_df.loc[x.index, 'is_stationed'] == False  # noqa: E712
        return x[mask].max() if mask.any() else pd.NaT

    session_summary = (
        session_df
        .groupby(['rider_code', 'session_seq', 'session_id'], as_index=False)
        .agg(
            session_start=('timestamp', get_first_true_ts),
            session_last_false=('timestamp', get_last_false_ts),
            session_last_event=('timestamp', 'max'),
            total_logs=('id', 'count'),
            total_true_logs=('is_stationed', lambda x: int((x == True).sum())),  # noqa: E712
            total_false_logs=('is_stationed', lambda x: int((x == False).sum())),  # noqa: E712
        )
    )

    # session_end = last False (mangkal ended) else fallback to last event
    session_summary['session_end'] = session_summary['session_last_false'].fillna(
        session_summary['session_last_event']
    )

    session_summary['session_duration_minutes'] = (
        (session_summary['session_end'] - session_summary['session_start']).dt.total_seconds() / 60
    ).round(2)

    return df, session_summary


def build_final_summary(
    df_session_summary,
    df_rider_total_working_time,
    df_raw_transactions,
    df_hub_mapping,
    df_profile_clicks,
    load_data_by,
    batch_label,
):
    """Combine all DataFrames into the final daily rider mangkal summary.

    Args:
        df_profile_clicks: per (operating_date, rider_id) counts from
            `ksj_user_click_tracker` (joined as `total_click_by_user` on rider_code).
        batch_label: which 3x/day run produced this row ('08AM' / '11AM' / '03PM').

    Returns the final DataFrame to be loaded into
    `daily_rider_mangkal_session_summary` / `_logs`.
    """
    summary_df = df_session_summary.copy()

    # Aggregate session summary -> per rider per operating date
    if not summary_df.empty:
        summary_df['session_start'] = pd.to_datetime(summary_df['session_start'], errors='coerce')
        summary_df['operating_date'] = summary_df['session_start'].dt.strftime('%Y-%m-%d')

        df_rider_total_mangkal_time = summary_df.groupby(
            ['operating_date', 'rider_code']
        ).agg(
            total_mangkal_time=('session_duration_minutes', 'sum'),
            total_mangkal_session=('session_id', 'nunique'),
        ).reset_index()
    else:
        df_rider_total_mangkal_time = pd.DataFrame(
            columns=['operating_date', 'rider_code', 'total_mangkal_time', 'total_mangkal_session']
        )

    # Daily transactions per rider/hub
    if not df_raw_transactions.empty:
        df_daily_transactions = df_raw_transactions.groupby(
            ['transaction_date', 'rider_code', 'rider_name', 'hub_code']
        ).agg(
            total_sales=('total_price_after_discount', 'sum'),
            total_order=('id', 'nunique'),
            total_qty=('total_item_qty', 'sum'),
        ).reset_index()
    else:
        df_daily_transactions = pd.DataFrame(
            columns=['transaction_date', 'rider_code', 'rider_name', 'hub_code',
                     'total_sales', 'total_order', 'total_qty']
        )

    # Normalize join keys
    df_daily_transactions_m = df_daily_transactions.rename(
        columns={'transaction_date': 'operating_date'}
    ).copy()
    df_rider_total_working_time_m = df_rider_total_working_time[
        ['operating_date', 'rider_code', 'total_working_minutes']
    ].copy() if not df_rider_total_working_time.empty else pd.DataFrame(
        columns=['operating_date', 'rider_code', 'total_working_minutes']
    )
    df_rider_total_mangkal_time_m = df_rider_total_mangkal_time.copy()

    if not df_profile_clicks.empty:
        df_profile_clicks_m = df_profile_clicks.rename(
            columns={'date': 'operating_date', 'rider_id': 'rider_code'}
        ).copy()
        df_profile_clicks_m['operating_date'] = pd.to_datetime(
            df_profile_clicks_m['operating_date'], errors='coerce'
        ).dt.strftime('%Y-%m-%d')
        df_profile_clicks_m['rider_code'] = df_profile_clicks_m['rider_code'].astype(str).str.strip()
    else:
        df_profile_clicks_m = pd.DataFrame(
            columns=['operating_date', 'rider_code', 'total_click_by_user']
        )

    for _df in (df_daily_transactions_m, df_rider_total_working_time_m, df_rider_total_mangkal_time_m):
        if _df.empty:
            continue
        _df['operating_date'] = pd.to_datetime(_df['operating_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        _df['rider_code'] = _df['rider_code'].astype(str).str.strip()

    # Merge: transactions <- working time <- mangkal time <- hub mapping <- profile clicks
    df_final_summary = (
        df_daily_transactions_m
        .merge(df_rider_total_working_time_m, on=['operating_date', 'rider_code'], how='left')
        .merge(df_rider_total_mangkal_time_m, on=['operating_date', 'rider_code'], how='left')
        .merge(df_hub_mapping, on='hub_code', how='left')
        .merge(df_profile_clicks_m, on=['operating_date', 'rider_code'], how='left')
    )

    numeric_cols = [
        'total_working_minutes', 'total_mangkal_time', 'total_mangkal_session',
        'total_sales', 'total_order', 'total_qty', 'total_click_by_user',
    ]
    for col in numeric_cols:
        if col in df_final_summary.columns:
            df_final_summary[col] = pd.to_numeric(df_final_summary[col], errors='coerce').fillna(0)

    string_cols = ['jilid', 'hub_name', 'business_unit', 'region', 'kota', 'provinsi']
    for col in string_cols:
        if col in df_final_summary.columns:
            df_final_summary[col] = df_final_summary[col].fillna(0)

    df_final_summary['mangkal_mins_pct'] = (
        df_final_summary['total_mangkal_time']
        / df_final_summary['total_working_minutes'].replace(0, np.nan)
        * 100
    ).round(2).fillna(0)

    final_columns = [
        'operating_date',
        'rider_code',
        'jilid',
        'hub_name',
        'business_unit',
        'region',
        'kota',
        'provinsi',
        'total_working_minutes',
        'total_mangkal_time',
        'total_mangkal_session',
        'mangkal_mins_pct',
        'total_sales',
        'total_order',
        'total_qty',
        'total_click_by_user',
    ]
    for col in final_columns:
        if col not in df_final_summary.columns:
            df_final_summary[col] = 0

    df_final_summary = df_final_summary[final_columns]
    df_final_summary['batch_label'] = batch_label
    df_final_summary['load_data_by'] = load_data_by

    return df_final_summary
