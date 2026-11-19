"""Master Orchestrator - KSJ Rider Commission (PRODUCTION).

Flow:
  1. Extract: transaction summaries/products + commission schemes (KSJ Link PRODUCTION)
  2. Transform: daily products -> daily summaries -> weekly summaries
  3. Load (atomic, 1 transaksi): replace minggu berjalan di 3 tabel
  4. Email report (best-effort)
Minggu = Senin..Minggu dari tanggal target (H-1); data diambil Senin s/d target_date saja.
"""

import logging
import os
import smtplib
import ssl
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText

from pytz import timezone

from extract import (
    get_rider_daily_transaction_summaries,
    get_rider_daily_transaction_summary_products,
    get_commission_schemes,
    get_commission_scheme_by_weekly_sales,
    get_commission_scheme_by_prod_solds,
    get_commission_extra_schemes,
    get_commission_extra_scheme_by_prod_solds,
)
from transform import (
    prepare_schemes,
    build_daily_products,
    build_daily_summaries,
    build_weekly_summaries,
)
from load import get_engine_ksj_link, load_all

TITLE = "KSJ Rider Commission"
SENDER_EMAIL = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com"]


def _week_range(target_date_str):
    today = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    start_date = today - timedelta(days=today.weekday())
    return start_date, start_date + timedelta(days=6)


def _now_pg():
    return datetime.now(timezone('Asia/Jakarta')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + " +0700"


def _send_report_email(target_date, stats, error=None):
    color = "#dc3545" if error else "#4472C4"
    status = "FAILED" if error else "Completed"
    rows = "".join(
        f'<tr><td style="border:1px solid #ddd;padding:6px 12px;">{k}</td>'
        f'<td style="border:1px solid #ddd;padding:6px 12px;text-align:right;">{v}</td></tr>'
        for k, v in stats.items()
    )
    html = f"""\
    <html><body style="font-family:Arial,sans-serif;color:#333;">
        <h2 style="color:{color};">{TITLE} - {status}</h2>
        <p>Target date: <strong>{target_date}</strong></p>
        <table style="border-collapse:collapse;font-size:13px;">{rows}</table>
        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body></html>"""

    msg = MIMEText(html, 'html')
    msg['Subject'] = f"[{'FAILED' if error else 'REPORT'}] {TITLE} ({target_date})"
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECEIVER_EMAIL)

    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.ehlo()
        server.starttls(context=ssl.create_default_context())
        server.ehlo()
        server.login(SENDER_EMAIL, os.getenv("GMAIL_SMTP_PASSWORD"))
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
    print(f"\n  Report email sent to: {', '.join(RECEIVER_EMAIL)}")


def run_all(config_ksj, target_date_str, dry_run=False):
    """Full ETL. dry_run=True: extract + transform + print totals, tanpa load & email."""
    start_date, end_date = _week_range(target_date_str)

    # Aturan H-1: data hari ini (real-time, belum final) tidak boleh ikut dihitung.
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    today_jkt = datetime.now(timezone('Asia/Jakarta')).date()
    if target_date >= today_jkt:
        raise ValueError(f"target_date {target_date} belum boleh dihitung (hari ini {today_jkt}, aturan H-1).")
    # Minggu yang sudah lewat harus dihitung ulang dengan tanggal Minggu-nya; tanggal tengah minggu
    # akan menimpa baris weekly dengan akumulasi parsial.
    if end_date < today_jkt and target_date != end_date:
        raise ValueError(f"Minggu {start_date}..{end_date} sudah lewat; pakai target_date = {end_date} (Minggu).")

    print("=" * 65)
    print(f"  {TITLE.upper()}  |  date = {target_date_str}  |  week = {start_date} .. {end_date}")
    print("=" * 65)

    total_start = time.time()
    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/3] EXTRACT")
        sd, ed = start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')
        df_schemes = get_commission_schemes(config_ksj)
        df_by_weekly = get_commission_scheme_by_weekly_sales(config_ksj)
        df_by_prod = get_commission_scheme_by_prod_solds(config_ksj)
        df_extra = get_commission_extra_schemes(config_ksj)
        df_extra_by_prod = get_commission_extra_scheme_by_prod_solds(config_ksj)
        df_trx = get_rider_daily_transaction_summaries(config_ksj, sd, target_date_str)
        df_trx_products = get_rider_daily_transaction_summary_products(config_ksj, sd, target_date_str)
        print(f"        schemes={len(df_schemes)} weekly_sales={len(df_by_weekly)} prod_solds={len(df_by_prod)} "
              f"extra={len(df_extra)} extra_prod={len(df_extra_by_prod)}")
        print(f"        trx_summaries={len(df_trx)} trx_products={len(df_trx_products)}")

        if df_trx.empty:
            raise ValueError(f"Tidak ada transaction summaries untuk {sd}..{target_date_str}; load dibatalkan.")

        # ---- STEP 2: TRANSFORM ----
        print("\n  [2/3] TRANSFORM")
        prepare_schemes(df_schemes, df_by_weekly, df_by_prod, df_extra, df_extra_by_prod, df_trx)
        df_products = build_daily_products(df_trx_products, df_trx, df_by_prod, df_extra_by_prod)
        df_daily = build_daily_summaries(df_trx, df_schemes, df_extra, df_products)
        df_weekly = build_weekly_summaries(df_trx, df_schemes, df_by_weekly, df_daily, start_date, end_date)

        # Sama seperti notebook: created_at/updated_at hanya di-set di tabel weekly
        # (tabel daily memakai default DB).
        now_pg = _now_pg()
        df_weekly["created_at"] = now_pg
        df_weekly["updated_at"] = now_pg

        stats = {
            "week": f"{sd} .. {ed}",
            "weekly rows": f"{len(df_weekly):,}",
            "daily rows": f"{len(df_daily):,}",
            "daily product rows": f"{len(df_products):,}",
            "total revenue (weekly)": f"{df_weekly['total_revenue'].sum():,.0f}",
            "total commission (weekly)": f"{df_weekly['total_commission'].sum():,.0f}",
            "total attendance commission": f"{df_weekly['total_attendance_commission'].sum():,.0f}",
            "total payout": f"{(df_weekly['total_commission'].sum() + df_weekly['total_attendance_commission'].sum()):,.0f}",
            "daily_commission (sum daily)": f"{df_daily['daily_commission'].sum():,.0f}",
            "product_sold_commission (sum)": f"{df_products['total_product_sold_commission'].sum():,.0f}",
        }
        for k, v in stats.items():
            print(f"        {k:<32}: {v}")

        if dry_run:
            print("\n  DRY RUN - tidak ada load & email.")
            return {"status": "DRY_RUN", "target_date": target_date_str, "stats": stats}

        # ---- STEP 3: LOAD ----
        print("\n  [3/3] LOAD")
        t0 = time.time()
        result = load_all(get_engine_ksj_link(config_ksj), df_weekly, df_daily, df_products, start_date, end_date, target_date)
        for table, (deleted, inserted) in result.items():
            print(f"        {table}: -{deleted} / +{inserted}")
            stats[f"{table} (del/ins)"] = f"-{deleted:,} / +{inserted:,}"
        print(f"        load time {time.time() - t0:.1f}s")

        stats["total_time"] = f"{time.time() - total_start:.1f}s"
        print("=" * 65)

        try:
            _send_report_email(target_date_str, stats)
        except Exception as mail_err:
            logging.warning(f"Failed to send report email: {mail_err}")

        return {"status": "SUCCESS", "target_date": target_date_str, "stats": stats}

    except Exception as e:
        print(f"\n  PIPELINE FAILED: {e}")
        if not dry_run:
            try:
                _send_report_email(target_date_str, {"error": str(e)}, error=str(e))
            except Exception as mail_err:
                logging.warning(f"Failed to send failure email: {mail_err}")
        raise
