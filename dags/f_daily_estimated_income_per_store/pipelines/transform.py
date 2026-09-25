"""Transform functions - map payment_type to channel, compute commission, aggregate per outlet."""

import pandas as pd

# mapping raw payment_type_name (dari DB) -> nama channel yang dipakai di perhitungan
PAYMENT_TYPE_TO_CHANNEL = {
    "Cash": "cash",
    "BRI": "bri",
    "Go Food": "go_food",
    "Grab Food": "grab_food",
    "Grab Dine Out": "grab_dine_out",
    "Shopee Food": "shopee_food",
    "Tokopedia Go": "tokopedia_go",
    "Jiwa+": "jiwaplus",
    "iSellerpay (Sejutajiwariders)": "iseller_qris",
    "iSeller Pay QRIS": "iseller_qris",
    "QPON": "qpon",
}
NULL_PAYMENT_CHANNEL = "null_payment"
OTHER_PAYMENT_CHANNEL = "other"

# besaran commission cost per channel (pct dari subtotal / net sales)
COMMISSION_PCT = {
    "cash": 0.0,
    "bri": 0.0,
    "go_food": 0.16,
    "grab_food": 0.15,
    "grab_dine_out": 0.15,
    "shopee_food": 0.15,
    "tokopedia_go": 0.07,
    "jiwaplus": 0.0,
    "iseller_qris": 0.007,
    "qpon": 0.05,
}

CHANNEL_ORDER = [
    "cash", "bri", "go_food", "grab_food", "grab_dine_out", "shopee_food",
    "tokopedia_go", "jiwaplus", "iseller_qris", "qpon",
]


def map_channel(payment_type):
    if pd.isna(payment_type):
        return NULL_PAYMENT_CHANNEL
    return PAYMENT_TYPE_TO_CHANNEL.get(payment_type, OTHER_PAYMENT_CHANNEL)


def build_daily_income_summary(df_raw):
    """One row per date+outlet, with total_amount/subtotal/commission per channel.

    Args:
        df_raw: output of extract.extract_transactions

    Returns:
        DataFrame ready to be loaded into daily_est_income_per_store_for_landlord.
    """
    df = df_raw.copy()
    df["channel"] = df["payment_type"].apply(map_channel)

    # agregasi per date, outlet, channel (jaga-jaga kalau ada raw payment_type yg beda tapi map ke channel sama)
    grouped = (
        df.groupby(["date", "outlet_code", "outlet_name", "channel"], as_index=False)
        .agg(total_amount=("total_amount", "sum"),
             total_tax_amount=("total_tax_amount", "sum"),
             subtotal=("subtotal", "sum"))
    )

    # pivot jadi wide: satu baris per date+outlet, kolom per channel (total_amount & subtotal)
    pivot_amount = grouped.pivot_table(index=["date", "outlet_code", "outlet_name"],
                                        columns="channel", values="total_amount",
                                        aggfunc="sum", fill_value=0)
    pivot_subtotal = grouped.pivot_table(index=["date", "outlet_code", "outlet_name"],
                                          columns="channel", values="subtotal",
                                          aggfunc="sum", fill_value=0)

    for ch in CHANNEL_ORDER:
        if ch not in pivot_amount.columns:
            pivot_amount[ch] = 0
        if ch not in pivot_subtotal.columns:
            pivot_subtotal[ch] = 0

    summary = grouped.groupby(["date", "outlet_code", "outlet_name"], as_index=False).agg(
        net_sales=("subtotal", "sum"),
        tax_amount=("total_tax_amount", "sum"),
        total_sales=("total_amount", "sum"),
    ).set_index(["date", "outlet_code", "outlet_name"])

    result = summary.copy()
    total_commission_cost = pd.Series(0.0, index=result.index)

    # per channel cuma nyimpen: total_amount (raw), subtotal (net sales), commission
    for ch in CHANNEL_ORDER:
        pct = COMMISSION_PCT[ch]
        amount = pivot_amount[ch]
        sub = pivot_subtotal[ch]
        commission = (sub * pct).round(2)

        result[ch] = amount
        result[f"{ch}_subtotal"] = sub
        result[f"{ch}_commission"] = commission

        total_commission_cost = total_commission_cost + commission

    result["total_commission_cost"] = total_commission_cost.round(2)
    result = result.reset_index().sort_values(["date", "outlet_code"])

    return result
