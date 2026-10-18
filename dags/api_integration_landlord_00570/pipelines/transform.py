"""Transform functions — unpivot wide est-income columns and build the revenue payload columns."""

from datetime import datetime

import pandas as pd

# column prefix in daily_est_income_per_store_for_landlord -> PaymentType label sent to the landlord
CHANNEL_LABELS = {
    'cash':          'Cash',
    'bri':           'BRI',
    'go_food':       'Go Food',
    'grab_food':     'Grab Food',
    'grab_dine_out': 'Grab Dine Out',
    'shopee_food':   'Shopee Food',
    'tokopedia_go':  'Tokopedia Go',
    'jiwaplus':      'Jiwa+',
    'iseller_qris':  'iSeller QRIS',
    'qpon':          'QPON',
}


def transform_sales(df_est_income: pd.DataFrame, run_timestamp: datetime) -> pd.DataFrame:
    """Unpivot channel columns into one row per (date, payment type) and build the API payload columns.

    Per channel:
        TotalSales = <channel>
        TotalTax   = <channel> - <channel>_subtotal
        Amount     = <channel>_subtotal - <channel>_commission   (est. income shared to landlord)
    Channels with zero sales that day are dropped (they never appear in the payload).

    Args:
        df_est_income: output of extract.extract_sales
        run_timestamp: datetime used to build the time suffix in TransactionNumber / Remarks

    Returns:
        DataFrame with columns:
            jilid, TransactionDate, PaymentType, TotalSales, TotalTax,
            Amount, TransactionNumber, Remarks
        Returns an empty DataFrame (same columns) when there is nothing to send.
    """
    _COLUMNS = [
        'jilid', 'TransactionDate', 'PaymentType',
        'TotalSales', 'TotalTax', 'Amount',
        'TransactionNumber', 'Remarks',
    ]

    if df_est_income.empty:
        return pd.DataFrame(columns=_COLUMNS)

    rows = []
    for col, label in CHANNEL_LABELS.items():
        tmp = df_est_income[['outlet_code', 'date']].copy()
        tmp['date']        = tmp['date'].astype(str)
        tmp['payment_type'] = label
        tmp['total_sales'] = df_est_income[col]
        tmp['total_tax']   = df_est_income[col] - df_est_income[f'{col}_subtotal']
        tmp['est_income']  = df_est_income[f'{col}_subtotal'] - df_est_income[f'{col}_commission']
        rows.append(tmp)

    sales = pd.concat(rows, ignore_index=True).rename(
        columns={'outlet_code': 'jilid', 'date': 'transaction_date'}
    )
    sales['jilid'] = sales['jilid'].astype(str)
    sales = sales[sales['total_sales'] != 0]

    if sales.empty:
        return pd.DataFrame(columns=_COLUMNS)

    final_data = (
        sales
        .groupby(['jilid', 'transaction_date', 'payment_type'])
        .agg(
            TotalSales=('total_sales', 'sum'),
            TotalTax=('total_tax', 'sum'),
            Amount=('est_income', 'sum'),
        )
        .reset_index()
        .rename(columns={
            'transaction_date': 'TransactionDate',
            'payment_type':     'PaymentType',
        })
    )

    ts_str       = run_timestamp.strftime("%H%M%S")
    date_compact = final_data["TransactionDate"].astype(str).str.replace('-', '', regex=False)

    final_data['TransactionNumber'] = (
        "Janji_Jiwa_"
        + final_data['jilid']
        + "_"
        + final_data['PaymentType'].astype(str)
        + "_"
        + date_compact
        + "_"
        + ts_str
    )
    final_data['Remarks'] = (
        "Janji_Jiwa_"
        + final_data['jilid']
        + "_"
        + final_data['PaymentType'].astype(str)
        + "_"
        + final_data['TransactionDate'].astype(str)
        + "_"
        + ts_str
    )
    final_data['Amount'] = final_data['Amount'].round(4)

    return final_data[_COLUMNS]
