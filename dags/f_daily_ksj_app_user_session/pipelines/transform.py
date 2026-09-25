"""Transform functions - merge JiwaPlus and KSJ consumer app session DataFrames."""

import pandas as pd


def build_daily_session_combined(df_jiwaplus_session, df_ksj_session, load_data_by):
    """Outer-join JiwaPlus and KSJ consumer app session data on date.

    PostgreSQL NUMERIC columns arrive as Python Decimal/string objects;
    they are cast to float so MySQL can accept them without type errors.

    Args:
        df_jiwaplus_session: output of extract.get_jiwaplus_consumer_app_user_session
        df_ksj_session:      output of extract.get_ksj_consumer_app_user_session
        load_data_by:        string tag recorded in the load_data_by column

    Returns:
        DataFrame ready to be loaded into daily_ksj_consumer_app_user_session.
    """
    df_combined = (
        df_jiwaplus_session
        .merge(df_ksj_session, on='date', how='outer')
        .sort_values('date')
        .reset_index(drop=True)
    )

    numeric_cols = [
        'total_session',
        'total_users_open_jiwaplus',
        'total_users_open_ksj',
        'total_rider_found',
        'total_rider_not_found',
        'rider_found_perc',
        'total_click',
        'total_click_direction',
        'total_wa_direction',
        'click_ratio_to_found_rider',
    ]
    for col in numeric_cols:
        if col in df_combined.columns:
            df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce')

    df_combined['load_data_by'] = load_data_by

    return df_combined
