"""Transform functions — aggregate raw sales rows and build the revenue payload columns."""

import re
from datetime import datetime

import pandas as pd


def _sanitize_payment_type(s: str) -> str:
    """Replace non-word characters with underscores for safe use in identifiers."""
    s = str(s).strip()
    s = re.sub(r"[^\w\-]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "UNKNOWN"


def transform_sales(df_sales: pd.DataFrame, run_timestamp: datetime) -> pd.DataFrame:
    """Aggregate raw sales rows by payment_type and build the landlord API payload columns.

    Args:
        df_sales:      output of extract.extract_sales
        run_timestamp: datetime used to build the time suffix in TransactionNumber / Remarks

    Returns:
        DataFrame with columns:
            jilid, TransactionDate, PaymentType, TotalSales, TotalTax,
            Amount, TransactionNumber, Remarks
        Returns an empty DataFrame (same columns) when df_sales is empty.
    """
    _COLUMNS = [
        'jilid', 'TransactionDate', 'PaymentType',
        'TotalSales', 'TotalTax', 'Amount',
        'TransactionNumber', 'Remarks',
    ]

    if df_sales.empty:
        return pd.DataFrame(columns=_COLUMNS)

    final_data = (
        df_sales
        .groupby(['jilid', 'transaction_date', 'payment_type'])
        .agg(
            TotalSales=('total_sales', 'sum'),
            TotalTax=('total_tax', 'sum'),
            Amount=('net_sales', 'sum'),
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
