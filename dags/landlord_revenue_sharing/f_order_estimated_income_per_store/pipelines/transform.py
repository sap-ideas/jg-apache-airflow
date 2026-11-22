"""Transform functions - map payment type, compute commission per order row, validate against the daily table."""

import pandas as pd

# PAYMENT_TYPE_MAP / UNKNOWN_PAYMENT_TYPE / COMMISSION_PCT / map_payment_type HARUS identik dengan
# dags/landlord_revenue_sharing/f_daily_estimated_income_per_store/pipelines/transform.py.
# Sengaja di-copy (bukan import lintas DAG, supaya tidak kena module collision DagBag).
# Ubah rate di salah satu saja -> validate_vs_daily gagal (pengaman).
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

# kolom final sesuai urutan di table (di luar load_data_at & load_data_by)
ORDER_COLUMNS = [
    "date", "transaction_date", "outlet_code", "outlet_name", "order_id", "transaction_id",
    "order_reference", "channel", "payment_type_raw", "payment_type",
    "total_order_amount", "total_discount_amount", "total_promotion_amount",
    "subtotal", "tax_amount", "total_sales", "commission_pct", "sales_type_commission",
]

_MONEY_IN = ["total_order_amount", "total_discount_amount", "total_promotion_amount",
             "subtotal", "tax_amount", "total_sales"]
_MONEY_CHECK = ["subtotal", "tax_amount", "total_sales", "sales_type_commission"]
_TOLERANCE = 0.01

# lookup tidak sensitif huruf besar/kecil & spasi di pinggir (kolom di MySQL juga case-insensitive)
_NORMALIZED_MAP = {k.strip().casefold(): v for k, v in PAYMENT_TYPE_MAP.items()}


def map_payment_type(raw_payment_type):
    """Raw payment type -> payment type resmi. Yang belum dikenal dikembalikan apa adanya (tanpa spasi pinggir)."""
    if pd.isna(raw_payment_type):
        return UNKNOWN_PAYMENT_TYPE
    raw = str(raw_payment_type).strip()
    return _NORMALIZED_MAP.get(raw.casefold(), raw)


def build_order_income(df_raw):
    """One row per order_id + transaction_id with payment type, commission pct and commission rupiah.

    Uang masuk landlord (subtotal - sales_type_commission) sengaja tidak disimpan.
    Payment type yang belum ada di COMMISSION_PCT tetap disimpan (komisi 0), lihat find_unmapped_payment_types.
    """
    if df_raw.empty:
        return pd.DataFrame(columns=ORDER_COLUMNS)

    df = df_raw.copy()
    # MySQL balikin Decimal -> float supaya bisa dikali pct
    for col in _MONEY_IN:
        df[col] = pd.to_numeric(df[col]).astype(float)

    df["payment_type"] = df["payment_type_raw"].apply(map_payment_type)
    df["commission_pct"] = df["payment_type"].map(COMMISSION_PCT).fillna(0.0)
    # tidak dibulatkan di kode (kolom DECIMAL(20,4) yang membatasi 4 desimal)
    df["sales_type_commission"] = df["subtotal"] * df["commission_pct"]

    return df[ORDER_COLUMNS].reset_index(drop=True)


def find_unmapped_payment_types(df):
    """Payment type yang belum punya komisi resmi (COMMISSION_PCT). Returns DataFrame [payment_type, total_sales]."""
    if df.empty:
        return pd.DataFrame(columns=["payment_type", "total_sales"])
    unmapped = df[~df["payment_type"].isin(COMMISSION_PCT)]
    return (
        unmapped.groupby("payment_type", as_index=False)["total_sales"].sum()
        .sort_values("payment_type").reset_index(drop=True)
    )


def find_split_payments(df):
    """Order dengan >1 baris (split payment)."""
    if df.empty:
        return df
    return df[df.duplicated("order_id", keep=False)]


def check_duplicates(df):
    """Fail loud kalau (order_id, transaction_id) dobel di sumber (PK table juga bakal gagal)."""
    dup = df[df.duplicated(["order_id", "transaction_id"], keep=False)]
    if not dup.empty:
        detail = dup.sort_values(["order_id", "transaction_id"])[
            ["date", "order_reference", "order_id", "transaction_id"]
        ].to_string(index=False)
        raise ValueError(
            f"Ada {len(dup)} baris duplikat (order_id, transaction_id), cek sumber dulu:\n{detail}"
        )


def validate_vs_daily(df, daily):
    """Agregat level order harus sama dengan daily_est_income_per_store_for_landlord (date + outlet + payment type).

    Selisih uang < 0.01 dan total_order (COUNT DISTINCT order_id) sama. Raise ValueError kalau tidak lolos,
    sehingga load tidak jalan.
    """
    daily = daily.copy()
    for col in _MONEY_CHECK + ["total_order"]:
        daily[col] = pd.to_numeric(daily[col]).astype(float)
    daily["date"] = daily["date"].astype(str)
    daily["_pt_key"] = daily["payment_type"].str.casefold()

    chk = df.copy()
    chk["_pt_key"] = chk["payment_type"].str.casefold()
    agg = (
        chk.groupby(["date", "outlet_code", "_pt_key"], as_index=False)
        .agg(payment_type=("payment_type", "first"),
             subtotal=("subtotal", "sum"),
             tax_amount=("tax_amount", "sum"),
             total_sales=("total_sales", "sum"),
             sales_type_commission=("sales_type_commission", "sum"),
             total_order=("order_id", "nunique"))
    )

    cmp = agg.merge(daily, on=["date", "outlet_code", "_pt_key"], how="outer",
                    suffixes=("_order", "_daily"), indicator=True)
    for col in _MONEY_CHECK + ["total_order"]:
        cmp[f"diff_{col}"] = cmp[f"{col}_order"].fillna(0) - cmp[f"{col}_daily"].fillna(0)

    one_side = cmp["_merge"] != "both"
    bad_money = (cmp[[f"diff_{c}" for c in _MONEY_CHECK]].abs() >= _TOLERANCE).any(axis=1)
    bad_order = cmp["diff_total_order"] != 0
    bad = cmp[one_side | bad_money | bad_order]
    if not bad.empty:
        cols = ["date", "outlet_code", "_pt_key", "_merge"] + [f"diff_{c}" for c in _MONEY_CHECK + ["total_order"]]
        raise ValueError(
            "Validasi vs daily GAGAL, load dibatalkan. Baris yang beda / hanya di salah satu sisi:\n"
            + bad[cols].sort_values(["date", "_pt_key"]).to_string(index=False)
        )
    return len(cmp)
