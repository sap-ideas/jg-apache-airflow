"""
Master Orchestrator - iSeller Pusat Backfilling

Flow:
  1. Token + order_id yang sudah ada di Data Lake (rentang tanggal target)
  2. Extract: outlet (getstoreinfo), master produk, GetOrders per (outlet x jendela 4 jam)
  3. Filter: hanya order yang BELUM ada di Data Lake
  4. Transform (logika sama dengan iseller_pusat_fulfillment / notebook ingest v3)
  5. Load header + details dalam 1 transaksi (order yang sudah ada dicek ulang di dalam transaksi)
  6. Email + Teams. Kalau ada panggilan API yang tetap gagal: data yang berhasil TETAP di-load, lalu task digagalkan
     supaya Airflow retry (aman: run ulang hanya memasukkan order yang belum ada).

Tidak ada step DWH: tabel summary DWH diperbarui oleh etl_iseller_pusat_fulfillment (09:00, window 9 hari).
"""

import os
import sys
import smtplib
import ssl
from html import escape as html_escape
from email.mime.text import MIMEText

import pendulum

_PIPELINES_DIR = os.path.dirname(os.path.abspath(__file__))
_DAG_ROOT = os.path.dirname(_PIPELINES_DIR)
for _p in (_PIPELINES_DIR, _DAG_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from teams_report_iseller_pusat_backfilling import (
    post_teams_safe,
    send_teams_backfilling_completed,
    send_teams_backfilling_failed,
    send_teams_backfilling_partial,
)
from extract import (
    MissingAccessTokenError,
    TokenExpiredError,
    build_windows,
    fetch_all_orders,
    get_access_token,
    get_existing_order_ids,
    get_master_products,
    get_outlet_ids,
)
from transform import transform_orders
from load import load_missing_orders

DEFAULT_DAG_ID = "etl_iseller_pusat_backfilling"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]

_TD = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
_TD_R = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
_TH = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'


def _fail_without_retry(message):
    """AirflowFailException (tanpa retry) kalau jalan di Airflow, selain itu RuntimeError."""
    try:
        from airflow.exceptions import AirflowFailException  # type: ignore
        return AirflowFailException(message)
    except ImportError:
        return RuntimeError(message)


def _teams_meta(airflow_context):
    now_wib = pendulum.now("Asia/Jakarta").format("YYYY-MM-DD HH:mm:ss") + " WIB"
    if not airflow_context:
        return DEFAULT_DAG_ID, "n/a", now_wib
    return airflow_context["dag"].dag_id, airflow_context["dag_run"].run_id, now_wib


def _send_html_email(subject, title, title_color, start_date, end_date, body_html):
    msg = MIMEText(f"""\
    <html><body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{title_color};">{title}</h2>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>
        {body_html}
        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body></html>""", 'html')
    msg['Subject'] = subject
    msg['From'] = SENDER_EMAIL
    msg['To'] = ", ".join(RECEIVER_EMAIL)
    context = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(SENDER_EMAIL, os.getenv("GMAIL_SMTP_PASSWORD"))
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
    print(f"  Email sent: {subject}")


def _send_email_safe(*args):
    """Email gagal tidak boleh menggagalkan task (retry akan menjalankan ulang seluruh pipeline)."""
    try:
        _send_html_email(*args)
    except Exception as e:
        print(f"  WARNING: email gagal dikirim: {type(e).__name__}: {e}")


def _stats_table(stats):
    rows = [
        ("Order dari API", stats.get("orders_from_api", 0)),
        ("Sudah ada di Data Lake", stats.get("orders_already_in_dl", 0)),
        ("Order baru di-insert", stats.get("orders_inserted", 0)),
        ("Header rows", stats.get("headers_inserted", 0)),
        ("Details rows", stats.get("details_inserted", 0)),
    ]
    body = "".join(f'<tr><td {_TD}>{k}</td><td {_TD_R}>{int(v):,}</td></tr>' for k, v in rows)
    body += (f'<tr><td {_TD}>Amount header</td><td {_TD_R}>Rp {float(stats.get("headers_amount", 0)):,.0f}</td></tr>'
             f'<tr><td {_TD}>Amount details</td><td {_TD_R}>Rp {float(stats.get("details_amount", 0)):,.0f}</td></tr>')
    return f'<table style="border-collapse:collapse;width:100%;"><tr><th {_TH}>Metric</th><th {_TH}>Value</th></tr>{body}</table>'


def run_all(start_date, end_date, config_dw, dry_run=False, airflow_context=None):
    """
    start_date / end_date: YYYY-MM-DD (inklusif), hari yang ditarik dari API.
    dry_run: tarik + proses lengkap, tapi TIDAK menulis Data Lake dan tidak kirim email/Teams.
    """
    dag_id, run_id, exec_at = _teams_meta(airflow_context)
    meta = dict(period_start=start_date, period_end=end_date, dag_id=dag_id, run_id=run_id, executed_at_display=exec_at)
    # Cek order yang sudah ada: hari target + 1 hari (transaksi/refund yang tercatat di hari berikutnya)
    check_end = pendulum.parse(end_date).add(days=1).to_date_string()

    print("=" * 65)
    print("  iSeller Pusat BACKFILLING" + ("  [DRY RUN - Data Lake tidak ditulis]" if dry_run else ""))
    print(f"  Tarik API: {start_date} s/d {end_date} | cek order existing: {start_date} s/d {check_end}")
    print("=" * 65)

    try:
        token = get_access_token()

        print("\n  [1/5] Order yang sudah ada di Data Lake...")
        existing_ids = get_existing_order_ids(config_dw, start_date, check_end)
        print(f"        {len(existing_ids):,} order")

        print("  [2/5] Outlet + master produk + GetOrders...")
        outlet_ids = get_outlet_ids(token)
        print(f"        {len(outlet_ids)} outlet")
        df_products = get_master_products(token)
        get_data, failed_calls = fetch_all_orders(token, outlet_ids, build_windows(start_date, end_date))

        print("  [3/5] Filter order yang belum ada di Data Lake...")
        all_orders = [o for batch in get_data if batch for o in batch]
        missing = [o for o in all_orders if o['order_id'] not in existing_ids]
        # 1 order bisa muncul lebih dari sekali bila tertarik di 2 jendela: ambil sekali per order_id
        missing = list({o['order_id']: o for o in missing}.values())
        stats = {
            'orders_from_api': len({o['order_id'] for o in all_orders}),
            'orders_already_in_dl': len({o['order_id'] for o in all_orders}) - len(missing),
            'orders_inserted': 0, 'headers_inserted': 0, 'details_inserted': 0,
            'headers_amount': 0.0, 'details_amount': 0.0,
        }
        print(f"        Order dari API: {stats['orders_from_api']:,} | sudah ada: {stats['orders_already_in_dl']:,} | "
              f"perlu di-backfill: {len(missing):,}")

        if missing:
            print("  [4/5] Transform...")
            df_headers, df_details = transform_orders([missing], df_products)
            if df_headers.empty:
                print("  [5/5] Load: dilewati, tidak ada order dengan transaksi.")
            else:
                h_total, d_total = df_headers['total_amount'].astype(float).sum(), df_details['total_amount'].sum()
                print(f"        Header {h_total:,.2f} | Details {d_total:,.2f} | akurasi {d_total / h_total * 100 if h_total else 0:.3f}%")

                print("  [5/5] Load (1 transaksi)...")
                stats.update(load_missing_orders(config_dw, df_headers, df_details, start_date, check_end, dry_run=dry_run))
                print(f"        {'AKAN di-insert' if dry_run else 'Di-insert'}: {stats['orders_inserted']:,} order, "
                      f"{stats['headers_inserted']:,} header, {stats['details_inserted']:,} details "
                      f"({stats['orders_skipped_existing']} order di-skip, sudah ada)")
        else:
            print("  [4/5] Transform + [5/5] Load: dilewati, tidak ada order baru.")

    except MissingAccessTokenError as e:
        print(f"\n  access_token_pusat NOT SET: {e}")
        _fail_report("MissingAccessTokenError", str(e), meta, start_date, end_date, dry_run)
        raise _fail_without_retry(f"Pipeline dihentikan: access_token_pusat tidak ada. {e}")
    except TokenExpiredError as e:
        print(f"\n  API TOKEN EXPIRED: {e}")
        _fail_report("TokenExpiredError", f"access_token_pusat expired. {e}", meta, start_date, end_date, dry_run)
        raise _fail_without_retry(f"Pipeline dihentikan: access_token_pusat expired. {e}")
    except Exception as e:
        print(f"\n  BACKFILLING FAILED: {type(e).__name__}: {e}")
        _fail_report(type(e).__name__, str(e), meta, start_date, end_date, dry_run)
        raise  # idempoten (order existing dicek ulang di dalam transaksi), retry Airflow aman

    if dry_run:
        print("\n  DRY RUN selesai: tidak ada email/Teams, Data Lake tidak ditulis.")
        return {"_reason": "dry_run", "stats": stats, "failed_calls": failed_calls}

    if failed_calls:
        detail = "".join(f"<li>{html_escape(m[:200])}</li>" for _o, _s, _e, m in failed_calls[:30])
        _send_email_safe(
            f"[ALERT] ETL iSeller Pusat Backfilling - PARTIAL ({len(failed_calls)} API calls failed) ({start_date} s/d {end_date})",
            "iSeller Pusat Backfilling - PARTIAL", "#fd7e14", start_date, end_date,
            f'<p><strong>{len(failed_calls)} panggilan API tetap gagal</strong> setelah semua retry. Data yang berhasil sudah '
            f'masuk ke Data Lake; task digagalkan supaya Airflow retry (run ulang hanya memasukkan order yang belum ada).</p>'
            f'{_stats_table(stats)}<h3>Panggilan gagal (maks 30)</h3><ul>{detail}</ul>')
        post_teams_safe("partial", send_teams_backfilling_partial, stats=stats, failed_calls=failed_calls, **meta)
        raise RuntimeError(f"{len(failed_calls)} panggilan API tetap gagal setelah retry; data yang berhasil sudah di-load. "
                           f"Contoh: {failed_calls[0][3][:200]}")

    _send_email_safe(
        f"[OK] ETL iSeller Pusat Backfilling - Completed ({start_date} s/d {end_date})",
        "iSeller Pusat Backfilling - Completed", "#28a745", start_date, end_date,
        f'<p>{"Semua order dari API sudah ada di Data Lake." if not stats["orders_inserted"] else "Order yang sebelumnya tidak ada sudah dimasukkan ke Data Lake."}</p>{_stats_table(stats)}')
    post_teams_safe("completed", send_teams_backfilling_completed, stats=stats, **meta)
    return {"_reason": "completed", "stats": stats, "failed_calls": []}


def _fail_report(error_type, error_message, meta, start_date, end_date, dry_run):
    if dry_run:
        return
    _send_email_safe(
        f"[ALERT] ETL iSeller Pusat Backfilling - STOPPED - {error_type} ({start_date} s/d {end_date})",
        f"iSeller Pusat Backfilling - STOPPED ({error_type})", "#dc3545", start_date, end_date,
        f'<div style="background:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;">'
        f'<strong>Pipeline dihentikan.</strong><br><pre style="white-space:pre-wrap;font-size:12px;">{html_escape(error_message)}</pre></div>')
    post_teams_safe("failed", send_teams_backfilling_failed, error_type=error_type, error_message=error_message, **meta)
