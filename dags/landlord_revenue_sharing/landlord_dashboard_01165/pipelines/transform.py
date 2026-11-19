"""Transform - pivot data income + render HTML dashboard dari template (string, tidak ditulis ke file)."""

import json
import os
from datetime import datetime

import pandas as pd
from pytz import timezone

OUTLET_CODE = "01165"
OUTLET_TITLE = "01165 SUMMARECON EMERALD KARAWANG"
LOOKBACK_DAYS = 60            # lock L60D (dashboard juga mengunci di sisi browser)
DATA_START = "2026-11-01"     # outlet buka 1 Nov 2026; transaksi 29-31 Okt = data testing

TEMPLATE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "templates", "dashboard_template_jiwa_light.html",
)

PAYMENT_COLUMNS = [
    ("cash",          "Cash"),
    ("bri",           "BRI"),
    ("go_food",       "Go Food"),
    ("grab_food",     "Grab Food"),
    ("grab_dine_out", "Grab Dine Out"),
    ("shopee_food",   "Shopee Food"),
    ("tokopedia_go",  "Tokopedia Go"),
    ("jiwaplus",      "Jiwa+"),
    ("iseller_qris",  "iSeller QRIS"),
    ("qpon",          "QPON"),
    ("other",         "Other"),
]
BUCKETS = [k for k, _ in PAYMENT_COLUMNS]

DATA_PLACEHOLDER = "/*__DASHBOARD_DATA__*/null"


def compute_start_date():
    today_jkt = datetime.now(timezone("Asia/Jakarta")).date()
    return max((today_jkt - pd.Timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d"), DATA_START)


def pivot_detail(raw):
    """1 baris per (date, outlet_name); daily_income = jumlah semua kolom payment."""
    if raw.empty:
        raise ValueError("Data kosong dari daily_est_income_per_store_for_landlord; dashboard tidak ditimpa")

    df = raw.copy()
    df["outlet_name"] = df["outlet_name"].fillna("")
    df["total_order"] = pd.to_numeric(df["total_order"], errors="coerce").fillna(0).astype(float)
    df["income"] = pd.to_numeric(df["income"], errors="coerce").fillna(0).astype(float)

    pivot = (df.pivot_table(index=["date", "outlet_name"], columns="bucket", values="income",
                            aggfunc="sum", fill_value=0)
               .reindex(columns=BUCKETS, fill_value=0))
    orders = df.groupby(["date", "outlet_name"])["total_order"].sum()

    detail = pivot.join(orders).reset_index().sort_values("date").reset_index(drop=True)
    detail["daily_income"] = detail[BUCKETS].sum(axis=1)
    return detail[["date", "outlet_name", "total_order"] + BUCKETS + ["daily_income"]]


def build_html(detail):
    """Embed JSON ke template Jiwa Light -> string HTML penuh."""
    columns = ([{"key": "date", "label": "Date", "numeric": False},
                {"key": "outlet_name", "label": "Outlet Name", "numeric": False}]
               + [{"key": k, "label": label, "numeric": True} for k, label in PAYMENT_COLUMNS]
               + [{"key": "daily_income", "label": "Daily Income", "numeric": True}])

    num_cols = ["total_order"] + BUCKETS + ["daily_income"]
    rows = detail.copy()
    rows[num_cols] = rows[num_cols].round(4)

    payload = {
        "outlet_code":   OUTLET_CODE,
        "outlet_title":  OUTLET_TITLE,
        "lookback_days": LOOKBACK_DAYS,
        "data_start":    DATA_START,
        "generated_at":  datetime.now(timezone("Asia/Jakarta")).strftime("%d %b %Y %H:%M"),
        "columns":       columns,
        "rows":          rows.to_dict("records"),
    }
    data_json = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")

    # newline="" supaya CRLF di template tidak diubah (sama dengan file hasil notebook)
    with open(TEMPLATE_PATH, encoding="utf-8", newline="") as f:
        template = f.read()
    assert DATA_PLACEHOLDER in template, f"Placeholder data tidak ditemukan di {TEMPLATE_PATH}"

    return (template
            .replace("__PAGE_TITLE__", f"{OUTLET_TITLE} - Sales Dashboard")
            .replace(DATA_PLACEHOLDER, data_json))
