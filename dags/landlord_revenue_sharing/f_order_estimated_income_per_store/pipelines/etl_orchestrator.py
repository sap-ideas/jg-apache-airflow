"""Master Orchestrator - Order Estimated Income (for Landlord) ETL.

Flow:
  1. Extract  order-level transactions (Data Lake) + daily table (DWH, buat validasi)  (extract.py)
  2. Transform commission per order row, cek duplikat, validasi vs daily             (transform.py)
  3. Load     atomic DELETE (range + outlet) + INSERT                                (load.py)
  4. Send a single email report with row counts & timing
"""

import logging
import os
import smtplib
import ssl
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText

from pytz import timezone

from extract import OUTLET_CODES, extract_order_transactions, extract_daily_summary
from transform import (
    build_order_income, find_unmapped_payment_types, find_split_payments,
    check_duplicates, validate_vs_daily,
)
from load import load_order_income, TARGET_TABLE, DEFAULT_LOAD_DATA_BY

SMTP_SERVER    = "smtp.gmail.com"
SMTP_PORT      = 587
SENDER_EMAIL   = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
SMTP_PASSWORD  = os.getenv("GMAIL_SMTP_PASSWORD")

MAX_RANGE_DAYS = 31


def _send_report_email(date_label, stats, error=None):
    """Send a concise HTML report covering extract / transform / load counts."""
    th    = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td    = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r  = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    if error:
        header_color = "#dc3545"
        title        = "ETL Order Estimated Income (Landlord) - FAILED"
        error_html   = f"""
        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline FAILED.</strong><br>
            <pre style="white-space:pre-wrap;margin:0;">{error}</pre>
        </div>
        """
    else:
        header_color = "#4472C4"
        title        = "ETL Order Estimated Income (Landlord) - Completed"
        error_html   = ""

    rows_html = ""
    for i, (label, value) in enumerate(stats.items()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        rows_html += f'<tr{bg}><td {td}>{label}</td><td {td_r}>{value}</td></tr>'

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{header_color};">{title}</h2>
        <p>Date: <strong>{date_label}</strong></p>

        {error_html}

        <h3 style="color:#333;">Pipeline Stats</h3>
        <table {table}>
            <tr><th {th}>Metric</th><th {th}>Value</th></tr>
            {rows_html}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    status_tag = "FAILED" if error else "REPORT"
    subject    = f"[{status_tag}] ETL Order Estimated Income Landlord ({date_label})"

    msg            = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From']    = SENDER_EMAIL
    msg['To']      = ", ".join(RECEIVER_EMAIL)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
        server.login(SENDER_EMAIL, SMTP_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())

    print(f"\n  Report email sent to: {', '.join(RECEIVER_EMAIL)}")


def run_all(config_dw, start_date_str=None, end_date_str=None, load_data_by=DEFAULT_LOAD_DATA_BY):
    """Full ETL flow for order_est_income_for_landlord.

    Args:
        config_dw:       Data Warehouse config module.
        start_date_str:  'YYYY-MM-DD' awal rentang (inklusif). Default kemarin (Asia/Jakarta).
        end_date_str:    'YYYY-MM-DD' akhir rentang (inklusif). Default = start_date_str.
        load_data_by:    Audit tag (email operator untuk run manual, NEXUS_AIRFLOW untuk otomatis).
    """
    if start_date_str is None:
        yesterday = (datetime.now(timezone('Asia/Jakarta')) - timedelta(days=1)).date()
        start_date_str = yesterday.strftime('%Y-%m-%d')
    if end_date_str is None:
        end_date_str = start_date_str

    start = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end   = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    if end < start:
        raise ValueError(f"end_date {end_date_str} lebih awal dari start_date {start_date_str}")
    if (end - start).days + 1 > MAX_RANGE_DAYS:
        raise ValueError(f"Rentang maksimal {MAX_RANGE_DAYS} hari, diminta {(end - start).days + 1} hari")

    date_label = start_date_str if start == end else f"{start_date_str} s/d {end_date_str}"

    print("=" * 65)
    print(f"  ETL ORDER ESTIMATED INCOME (LANDLORD)  |  {date_label}  |  outlets = {OUTLET_CODES}")
    print("=" * 65)

    total_start = time.time()

    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/3] EXTRACT")

        t0 = time.time()
        df_raw = extract_order_transactions(config_dw, start_date_str, end_date_str)
        print(f"        transactions      : {len(df_raw):>6} rows  ({time.time() - t0:.1f}s)")

        t0 = time.time()
        df_daily = extract_daily_summary(config_dw, start_date_str, end_date_str)
        print(f"        daily table       : {len(df_daily):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 2: TRANSFORM + VALIDATE ----
        print("\n  [2/3] TRANSFORM")

        t0 = time.time()
        df_order = build_order_income(df_raw)
        print(f"        order_income      : {len(df_order):>6} rows  ({time.time() - t0:.1f}s)")

        check_duplicates(df_order)

        df_unmapped = find_unmapped_payment_types(df_order)
        if not df_unmapped.empty:
            print("        WARNING payment type belum ada di PAYMENT_TYPE_MAP / COMMISSION_PCT (komisi dianggap 0):")
            for _, r in df_unmapped.iterrows():
                print(f"          - {r['payment_type']}: total_sales {r['total_sales']:,.0f}")

        n_split = find_split_payments(df_order)["order_id"].nunique() if not df_order.empty else 0
        print(f"        split payment     : {n_split:>6} order")

        # validasi vs daily; raise kalau beda -> load tidak jalan. Tabel kosong di dua sisi = OK (tidak ada transaksi).
        compared = validate_vs_daily(df_order, df_daily)
        print(f"        validasi vs daily : OK ({compared} baris tanggal x outlet x payment type)")

        # ---- STEP 3: LOAD ----
        print("\n  [3/3] LOAD")

        t0 = time.time()
        deleted, inserted = load_order_income(config_dw, df_order, start_date_str, end_date_str, load_data_by)
        if df_order.empty:
            print(f"        No rows to load - skipping ({TARGET_TABLE} left untouched).")
        else:
            print(
                f"        Overwrite (atomic):  -{deleted} / +{inserted} rows on {TARGET_TABLE} "
                f"for {date_label}  ({time.time() - t0:.1f}s)"
            )

        total_elapsed = time.time() - total_start
        print(f"\n  TOTAL TIME: {total_elapsed:.1f}s")
        print("=" * 65)

        n_order = int(df_order["order_id"].nunique()) if not df_order.empty else 0
        uang_masuk = (
            (df_order["subtotal"] - df_order["sales_type_commission"]).sum() if not df_order.empty else 0.0
        )
        stats = {
            "date":                           date_label,
            "outlets":                        ", ".join(OUTLET_CODES),
            "load_data_by":                   load_data_by,
            "transactions (extracted)":       f"{len(df_raw):,}",
            "order (distinct order_id)":      f"{n_order:,}",
            "split payment (order)":          f"{n_split:,}",
            "uang masuk (subtotal - komisi)": f"{uang_masuk:,.4f}",
            "validasi vs daily":              "OK",
            f"{TARGET_TABLE} (overwrite -del/+ins)": f"-{deleted:,} / +{inserted:,}",
            "total_time":                     f"{total_elapsed:.1f}s",
        }
        if not df_unmapped.empty:
            stats["WARNING payment type belum dikenal (komisi 0)"] = "; ".join(
                f"{r['payment_type']} ({r['total_sales']:,.0f})" for _, r in df_unmapped.iterrows()
            )

        try:
            _send_report_email(date_label, stats)
        except Exception as mail_err:
            logging.warning(f"Failed to send report email: {mail_err}")

        return {
            "status":     "SUCCESS",
            "start_date": start_date_str,
            "end_date":   end_date_str,
            "deleted":    deleted,
            "inserted":   inserted,
            "stats":      stats,
        }

    except Exception as e:
        print(f"\n  PIPELINE FAILED: {e}")
        try:
            _send_report_email(date_label, {"error": "lihat detail di atas"}, error=str(e))
        except Exception as mail_err:
            logging.warning(f"Failed to send failure email: {mail_err}")
        raise
