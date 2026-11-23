"""
Master Orchestrator - iSeller Pusat Intraday

Tabel intraday (`transactions_iseller_pusat_intraday` + `transactions_items_iseller_pusat_intraday`) berisi data HARI INI saja,
diperbarui tiap jam (06:00-21:00 WIB). Tabel Data Lake utama tidak disentuh.

Flow:
  1. Token + outlet aktif + kondisi tabel intraday saat ini
  2. Extract: GetOrders 1 call per outlet untuk 00:00 WIB s/d sekarang (call yang terpotong 200 order dibelah dua)
  3. Saring: hanya order BARU / BERUBAH dibanding tabel yang di-transform + ditulis
  4. Transform (salinan transform iseller_pusat_backfilling)
  5. Load per order dalam 1 transaksi: order baru insert, order berubah diganti versi terbaru, order yang menyusut ditahan,
     order yang tidak ketarik tidak disentuh, baris H-1 dihapus
  6. Email ringkasan (sukses dan gagal, isinya sama). Kalau ada outlet yang tetap gagal: data yang berhasil TETAP di-load
     (aman, upsert per order tidak pernah menghapus order yang tidak ketarik), lalu task digagalkan supaya Airflow retry.
"""

import os
import sys
import smtplib
import ssl
import time
from html import escape as html_escape
from email.mime.text import MIMEText

import pandas as pd
import pendulum

_PIPELINES_DIR = os.path.dirname(os.path.abspath(__file__))
_DAG_ROOT = os.path.dirname(_PIPELINES_DIR)
for _p in (_PIPELINES_DIR, _DAG_ROOT):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from extract import (
    MissingAccessTokenError,
    TokenExpiredError,
    build_today_window,
    fetch_all_orders,
    get_access_token,
    get_intraday_metrics,
    get_intraday_state,
    get_master_products,
    get_outlet_ids,
    pick_orders_to_write,
)
from transform import transform_orders
from load import sync_intraday

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


def _send_html_email(subject, title, title_color, body_html):
    msg = MIMEText(f"""\
    <html><body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{title_color};">{title}</h2>
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
    n = lambda k: f"{int(stats.get(k, 0)):,}"
    rows = [
        ("Tanggal data", stats['data_date']),
        ("Jam tarik", stats['run_time']),
        ("Durasi run", f"{stats.get('duration_min', 0):.1f} menit"),
        ("Outlet aktif (API)", n('active_outlets')),
        ("Outlet yang punya order (di tabel)", n('table_outlets')),
        ("Total order (DISTINCT order_id di tabel)", n('table_orders')),
        ("Order belum fulfilled (di tabel)", n('orders_unfulfilled')),
        ("Total amount header", f"Rp {float(stats.get('amount_headers', 0)):,.0f}"),
        ("Total amount details", f"Rp {float(stats.get('amount_details', 0)):,.0f}"),
        ("Order dari API run ini", n('orders_from_api')),
        ("Order baru (insert)", n('orders_new')),
        ("Order ter-update (diganti versi terbaru)", n('orders_updated')),
        ("Order tidak berubah (dilewati)", n('orders_unchanged')),
        ("Order ditahan (versi baru menyusut, versi lama dipertahankan)", n('orders_held')),
        ("Baris H-1 dihapus (header / details)", f"{n('rollover_headers')} / {n('rollover_details')}"),
        ("Outlet gagal ditarik", n('failed_outlets')),
    ]
    body = "".join(f'<tr><td {_TD}>{k}</td><td {_TD_R}>{v}</td></tr>' for k, v in rows)
    held = stats.get('held_orders') or []
    extra = (f'<p>Order ditahan (contoh, order_reference): {html_escape(", ".join(map(str, held)))}</p>' if held else '')
    return f'<table style="border-collapse:collapse;width:100%;"><tr><th {_TH}>Metric</th><th {_TH}>Value</th></tr>{body}</table>{extra}'


def run_all(config_dw, dry_run=False):
    """
    Data yang ditarik = HARI INI (00:00 WIB s/d sekarang), dihitung dari jam saat run jalan.
    dry_run: tarik + proses lengkap, hitung yang akan ditulis, tapi TIDAK menulis Data Lake dan tidak kirim email.
    """
    t_start = time.time()
    now_wib = pendulum.now("Asia/Jakarta")
    start_dt, end_dt, cutoff_utc = build_today_window(now_wib)
    stats = {'data_date': now_wib.to_date_string(), 'run_time': now_wib.format("HH:mm") + " WIB"}

    print("=" * 65)
    print("  iSeller Pusat INTRADAY" + ("  [DRY RUN - Data Lake tidak ditulis]" if dry_run else ""))
    print(f"  Data hari ini: {stats['data_date']} 00:00 s/d {stats['run_time']} (API {start_dt} ~ {end_dt} UTC)")
    print("=" * 65)

    try:
        token = get_access_token()

        print("\n  [1/5] Outlet + kondisi tabel intraday...")
        outlet_ids = get_outlet_ids(token)
        state = get_intraday_state(config_dw)
        stats['active_outlets'] = len(outlet_ids)
        print(f"        {len(outlet_ids)} outlet aktif | tabel intraday sekarang: {len(state):,} order")

        print("  [2/5] GetOrders (1 call per outlet)...")
        orders, failed_calls = fetch_all_orders(token, outlet_ids, start_dt, end_dt)
        stats['orders_from_api'] = len(orders)
        stats['failed_outlets'] = len(failed_calls)
        print(f"        {len(orders):,} order dari API | outlet gagal: {len(failed_calls)}")

        print("  [3/5] Saring order baru / berubah...")
        to_write, stats['orders_unchanged'], n_no_tx = pick_orders_to_write(orders, state)
        print(f"        perlu ditulis: {len(to_write):,} | tidak berubah: {stats['orders_unchanged']:,} | belum dibayar (dilewati): {n_no_tx:,}")

        if to_write:
            print("  [4/5] Transform...")
            df_headers, df_details = transform_orders([to_write], get_master_products(token))
            if len(df_headers):
                h_total, d_total = df_headers['total_amount'].astype(float).sum(), df_details['total_amount'].sum()
                print(f"        Header {h_total:,.2f} | Details {d_total:,.2f}")
        else:
            df_headers, df_details = pd.DataFrame(), pd.DataFrame()
            print("  [4/5] Transform: dilewati, tidak ada order baru/berubah.")

        print("  [5/5] Load (1 transaksi)...")
        stats.update(sync_intraday(config_dw, df_headers, df_details, cutoff_utc, dry_run=dry_run))
        print(f"        {'AKAN' if dry_run else 'Sudah'}: baru {stats['orders_new']:,} | ter-update {stats['orders_updated']:,} | "
              f"ditahan {stats['orders_held']:,} | H-1 dihapus {stats['rollover_headers']:,} header / {stats['rollover_details']:,} details")

    except MissingAccessTokenError as e:
        print(f"\n  access_token_pusat NOT SET: {e}")
        _fail_report("MissingAccessTokenError", str(e), stats, t_start, dry_run)
        raise _fail_without_retry(f"Pipeline dihentikan: access_token_pusat tidak ada. {e}")
    except TokenExpiredError as e:
        print(f"\n  API TOKEN EXPIRED: {e}")
        _fail_report("TokenExpiredError", f"access_token_pusat expired. {e}", stats, t_start, dry_run)
        raise _fail_without_retry(f"Pipeline dihentikan: access_token_pusat expired. {e}")
    except Exception as e:
        print(f"\n  INTRADAY FAILED: {type(e).__name__}: {e}")
        _fail_report(type(e).__name__, str(e), stats, t_start, dry_run)
        raise  # idempoten (upsert per order), retry Airflow aman

    stats['duration_min'] = (time.time() - t_start) / 60
    if dry_run:
        print(f"\n  DRY RUN selesai ({stats['duration_min']:.1f} menit): tidak ada email, Data Lake tidak ditulis.")
        return {"_reason": "dry_run", "stats": stats, "failed_calls": failed_calls}

    stats.update(get_intraday_metrics(config_dw))
    title_date = f"{stats['data_date']} {stats['run_time']}"

    if failed_calls:
        detail = "".join(f"<li>{html_escape(m[:200])}</li>" for _o, m in failed_calls[:30])
        _send_email_safe(
            f"[ALERT] ETL iSeller Pusat Intraday - PARTIAL ({len(failed_calls)} outlet gagal) - {title_date}",
            "iSeller Pusat Intraday - PARTIAL", "#fd7e14",
            f'<p><strong>{len(failed_calls)} outlet tetap gagal ditarik</strong> setelah semua retry. Data yang berhasil sudah masuk; '
            f'order outlet itu tetap memakai versi jam sebelumnya (tidak dihapus). Task digagalkan supaya Airflow retry.</p>'
            f'{_stats_table(stats)}<h3>Outlet gagal (maks 30)</h3><ul>{detail}</ul>')
        raise RuntimeError(f"{len(failed_calls)} outlet tetap gagal ditarik setelah retry; data yang berhasil sudah di-load. "
                           f"Contoh: {failed_calls[0][1][:200]}")

    _send_email_safe(
        f"[OK] ETL iSeller Pusat Intraday - Completed - {title_date}",
        "iSeller Pusat Intraday - Completed", "#28a745", _stats_table(stats))
    return {"_reason": "completed", "stats": stats, "failed_calls": []}


def _fail_report(error_type, error_message, stats, t_start, dry_run):
    if dry_run:
        return
    stats['duration_min'] = (time.time() - t_start) / 60
    title_date = f"{stats['data_date']} {stats['run_time']}"
    _send_email_safe(
        f"[ALERT] ETL iSeller Pusat Intraday - STOPPED - {error_type} - {title_date}",
        f"iSeller Pusat Intraday - STOPPED ({error_type})", "#dc3545",
        f'<div style="background:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;">'
        f'<strong>Pipeline dihentikan.</strong><br><pre style="white-space:pre-wrap;font-size:12px;">{html_escape(error_message)}</pre></div>'
        f'<p>Kondisi sampai titik gagal (kolom yang belum terisi = 0):</p>{_stats_table(stats)}')
