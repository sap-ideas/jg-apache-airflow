"""Transform functions - map raw payment_type to the official payment type, compute commission per payment type."""

import pandas as pd

# mapping raw transactions_payment_type_name (dari DB) -> payment type resmi.
# Label resmi ini juga yang dikirim ke landlord & dicatat di api_landlord_log_detail.payment_type.
# Tambah payment type baru = tambah entry di PAYMENT_TYPE_MAP & COMMISSION_PCT (tidak perlu ubah table).
PAYMENT_TYPE_MAP = {
    "Cash": "Cash",
    "BRI": "BRI",
    "Go Food": "Go Food",
    "Grab Food": "Grab Food",
    "Grab Dine Out": "Grab Dine Out",
    "Shopee Food": "Shopee Food",
    "Tokopedia Go": "Tokopedia Go",
    "Jiwa+": "Jiwa+",
    "iSellerpay (Sejutajiwariders)": "iSeller QRIS",
    "iSeller Pay QRIS": "iSeller QRIS",
    "isellerpay_qris": "iSeller QRIS",
    "Mandiri QRIS": "Mandiri QRIS",
    "QPON": "QPON",
}
UNKNOWN_PAYMENT_TYPE = "Unknown"  # raw payment_type NULL

# besaran commission cost per payment type resmi (pct dari subtotal / net sales)
COMMISSION_PCT = {
    "Cash": 0.0,
    "BRI": 0.0,          # all banks
    "Go Food": 0.16,
    "Grab Food": 0.15,
    "Grab Dine Out": 0.15,
    "Shopee Food": 0.15,
    "Tokopedia Go": 0.07,
    "Jiwa+": 0.0,
    "iSeller QRIS": 0.007,
    "Mandiri QRIS": 0.0,
    "QPON": 0.05,
}

SUMMARY_COLUMNS = [
    "date", "outlet_code", "outlet_name", "payment_type",
    "subtotal", "tax_amount", "total_sales", "sales_type_commission",
]

# lookup tidak sensitif huruf besar/kecil & spasi di pinggir (kolom di MySQL juga case-insensitive)
_NORMALIZED_MAP = {k.strip().casefold(): v for k, v in PAYMENT_TYPE_MAP.items()}


def map_payment_type(raw_payment_type):
    """Raw payment type -> payment type resmi. Yang belum dikenal dikembalikan apa adanya (tanpa spasi pinggir)."""
    if pd.isna(raw_payment_type):
        return UNKNOWN_PAYMENT_TYPE
    raw = str(raw_payment_type).strip()
    return _NORMALIZED_MAP.get(raw.casefold(), raw)


def build_daily_income_summary(df_raw):
    """One row per date + outlet + payment type, with subtotal / tax / total sales / commission.

    Nominal yang dibagi ke landlord (subtotal - sales_type_commission) sengaja tidak disimpan.
    Payment type yang belum ada di PAYMENT_TYPE_MAP tetap disimpan (nama aslinya, komisi 0)
    supaya total per outlet-hari selalu cocok dengan sumber; lihat find_unmapped_payment_types.

    Args:
        df_raw: output of extract.extract_transactions

    Returns:
        DataFrame with SUMMARY_COLUMNS, ready to be loaded into daily_est_income_per_store_for_landlord.
    """
    if df_raw.empty:
        return pd.DataFrame(columns=SUMMARY_COLUMNS)

    df = df_raw.copy()
    # MySQL SUM() balik sebagai Decimal -> float supaya bisa dikali pct
    for col in ("total_amount", "total_tax_amount", "subtotal"):
        df[col] = pd.to_numeric(df[col]).astype(float)

    df["payment_type"] = df["payment_type"].apply(map_payment_type)
    # kunci agregasi tidak sensitif huruf besar/kecil, selaras dengan UNIQUE KEY di table
    df["_pt_key"] = df["payment_type"].str.casefold()

    # agregasi per date, outlet, payment type (beberapa raw payment_type bisa map ke payment type yang sama)
    result = (
        df.groupby(["date", "outlet_code", "outlet_name", "_pt_key"], as_index=False, dropna=False)
        .agg(payment_type=("payment_type", "first"),
             subtotal=("subtotal", "sum"),
             tax_amount=("total_tax_amount", "sum"),
             total_sales=("total_amount", "sum"))
    )

    pct = result["payment_type"].map(COMMISSION_PCT).fillna(0.0)
    # tidak dibulatkan di kode (kolom DECIMAL(20,4) yang membatasi 4 desimal); Amount dibulatkan 4 desimal saat kirim ke landlord
    result["sales_type_commission"] = result["subtotal"] * pct

    return result[SUMMARY_COLUMNS].sort_values(["date", "outlet_code", "payment_type"]).reset_index(drop=True)


def find_unmapped_payment_types(df_summary):
    """Payment type di summary yang belum punya komisi resmi (COMMISSION_PCT), per payment type.

    Returns:
        DataFrame [payment_type, total_sales] (kosong kalau semua sudah dikenal).
    """
    if df_summary.empty:
        return pd.DataFrame(columns=["payment_type", "total_sales"])
    unmapped = df_summary[~df_summary["payment_type"].isin(COMMISSION_PCT)]
    return (
        unmapped.groupby("payment_type", as_index=False)["total_sales"].sum()
        .sort_values("payment_type").reset_index(drop=True)
    )
