"""Transform functions — build the revenue payload columns from est-income rows (one per payment type)."""

from datetime import datetime

import pandas as pd

# payment type resmi yang boleh dikirim ke landlord (label = PaymentType di payload & api_landlord_log_detail).
# Payment type lain di table (belum resmi) tidak dikirim; lihat find_unsent_payment_types.
SENDABLE_PAYMENT_TYPES = [
    'Cash', 'BRI', 'Go Food', 'Grab Food', 'Grab Dine Out', 'Shopee Food',
    'Tokopedia Go', 'Jiwa+', 'iSeller QRIS', 'Mandiri QRIS', 'QPON',
]


def find_unsent_payment_types(df_est_income: pd.DataFrame) -> pd.DataFrame:
    """Payment type di est-income yang ada transaksinya tapi bukan SENDABLE_PAYMENT_TYPES.

    Returns:
        DataFrame [payment_type, total_sales] (kosong kalau semua payment type dikirim).
    """
    if df_est_income.empty:
        return pd.DataFrame(columns=['payment_type', 'total_sales'])
    unsent = df_est_income[
        ~df_est_income['payment_type'].isin(SENDABLE_PAYMENT_TYPES) & (df_est_income['total_sales'] != 0)
    ]
    return (
        unsent.groupby('payment_type', as_index=False)['total_sales'].sum()
        .sort_values('payment_type').reset_index(drop=True)
    )


def transform_sales(df_est_income: pd.DataFrame, run_timestamp: datetime) -> pd.DataFrame:
    """Build the landlord API payload columns, one row per (date, payment type).

    Per payment type:
        TotalSales = total_sales
        TotalTax   = tax_amount
        Amount     = subtotal - sales_type_commission   (est. income shared to landlord)
    Payment types outside SENDABLE_PAYMENT_TYPES and rows with zero sales are dropped.

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

    sales = df_est_income[
        df_est_income['payment_type'].isin(SENDABLE_PAYMENT_TYPES) & (df_est_income['total_sales'] != 0)
    ].copy()

    if sales.empty:
        return pd.DataFrame(columns=_COLUMNS)

    sales['jilid']       = sales['outlet_code'].astype(str)
    sales['date']        = sales['date'].astype(str)
    sales['est_income']  = sales['subtotal'] - sales['sales_type_commission']

    final_data = (
        sales
        .groupby(['jilid', 'date', 'payment_type'])
        .agg(
            TotalSales=('total_sales', 'sum'),
            TotalTax=('tax_amount', 'sum'),
            Amount=('est_income', 'sum'),
        )
        .reset_index()
        .rename(columns={
            'date':         'TransactionDate',
            'payment_type': 'PaymentType',
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
