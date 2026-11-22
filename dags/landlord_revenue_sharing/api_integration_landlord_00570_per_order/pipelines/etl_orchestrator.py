"""
Master Orchestrator - API Landlord Revenue Sharing PER ORDER (Jilid 00570).

Flow:
  1. Extract   → extract_order_income (DWH) + extract_send_log (log API)      (extract.py)
  2. Transform → build_payload + apply_guard (semua cek sebelum POST pertama) (transform.py)
  3. Load      → per tanggal: insert_pending_logs → post_one_date → finalize_logs (load.py)
  4. Email     → satu HTML summary report per run

STOP (raise, tanggal berikutnya tidak dikirim):
  - simpan log setelah POST gagal (baris tetap 'pending', run berikutnya tidak kirim ulang)
  - TotalSuccess dari mall beda dengan jumlah success yang cocok di log
API menolak / timeout di satu tanggal: log tersimpan, lanjut tanggal berikutnya, status akhir API_ERROR.
"""

import logging
import os
import smtplib
import ssl
import time
from datetime import datetime, timedelta
from email.mime.text import MIMEText

from pytz import timezone

from extract import JILID, get_access_token, extract_order_income, extract_send_log
from transform import build_payload, apply_guard
from load import (
    insert_pending_logs, post_one_date, finalize_logs,
    LOG_SUMMARY_TABLE, LOG_DETAIL_TABLE, DEFAULT_LOAD_DATA_BY,
)

SMTP_SERVER    = "smtp.gmail.com"
SMTP_PORT      = 587
SENDER_EMAIL   = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
SMTP_PASSWORD  = os.getenv("GMAIL_SMTP_PASSWORD")

MAX_RANGE_DAYS = 31
# cutover per order (owner, 2026-11-22): sales s/d 2026-11-22 dikirim per payment type oleh DAG lama
MIN_SEND_DATE  = "2026-11-23"


class PostAttemptedError(Exception):
    """Error setelah POST ke API dijalankan. Jangan di-retry otomatis: retry = POST ulang."""


# ---------------------------------------------------------------------------
# Email report
# ---------------------------------------------------------------------------

def _send_report_email(date_label: str, stats: dict, error: str | None = None, dry_run: bool = False):
    """Send an HTML pipeline summary email to the data team."""
    th    = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    td    = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r  = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    mode = " (DRY RUN)" if dry_run else ""
    if error:
        header_color = "#dc3545"
        title        = f"API Landlord Revenue Sharing Per Order ({JILID}){mode} - FAILED"
        error_html   = f"""
        <div style="background-color:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;margin-bottom:20px;">
            <strong>Pipeline FAILED.</strong><br>
            <pre style="white-space:pre-wrap;margin:0;">{error}</pre>
        </div>
        """
    else:
        header_color = "#4472C4"
        title        = f"API Landlord Revenue Sharing Per Order ({JILID}){mode} - Completed"
        error_html   = ""

    rows_html = ""
    for i, (label, value) in enumerate(stats.items()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        rows_html += f'<tr{bg}><td {td}>{label}</td><td {td_r}>{value}</td></tr>'

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:{header_color};">{title}</h2>
        <p>Date: <strong>{date_label}</strong> &nbsp;|&nbsp; Jilid: <strong>{JILID}</strong></p>

        {error_html}

        <h3 style="color:#333;">Pipeline Stats</h3>
        <table {table}>
            <tr><th {th}>Metric</th><th {th}>Value</th></tr>
            {rows_html}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    status_tag = "FAILED" if error else ("DRY RUN" if dry_run else "REPORT")
    subject    = f"[{status_tag}] API Landlord Revenue Sharing Per Order {JILID} ({date_label})"

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


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _date_range(start_date_str: str, end_date_str: str) -> list[str]:
    """Daftar tanggal 'YYYY-MM-DD' dari start sampai end (inklusif). ValueError kalau terbalik / terlalu panjang."""
    start = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    end   = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    if start > end:
        raise ValueError(f"start_date {start} lebih besar dari end_date {end}")
    n_days = (end - start).days + 1
    if n_days > MAX_RANGE_DAYS:
        raise ValueError(f"Rentang {n_days} hari melebihi batas {MAX_RANGE_DAYS} hari")
    return [(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(n_days)]


def run_all(config_dw, start_date_str: str, end_date_str: str, dry_run: bool = False,
            resend_uncertain: bool = False, load_data_by: str = DEFAULT_LOAD_DATA_BY):
    """Full pipeline: kirim uang masuk per order jilid 00570 ke API landlord.

    Args:
        config_dw:         Data Warehouse config module.
        start_date_str:    'YYYY-MM-DD' awal rentang (inklusif), tidak boleh < MIN_SEND_DATE.
        end_date_str:      'YYYY-MM-DD' akhir rentang (inklusif). Maksimal MAX_RANGE_DAYS hari.
        dry_run:           True = hanya hitung & tampilkan payload, tidak POST, tidak nulis log.
        resend_uncertain:  True = baris yang di log masih timeout_error / pending ikut dikirim.
                           HANYA setelah dicek manual bahwa transaksinya belum masuk ke mall.
        load_data_by:      tag kolom load_data_by di kedua tabel log.

    Status return: SUCCESS, SUCCESS_NO_DATA, SKIPPED_ALREADY_SENT, DRY_RUN, API_ERROR (ada tanggal gagal).
    Error sebelum POST di-raise apa adanya; error setelah POST -> PostAttemptedError.
    """
    if start_date_str < MIN_SEND_DATE:
        raise ValueError(f"start_date {start_date_str} sebelum cutover per order {MIN_SEND_DATE}. "
                         "Tanggal sebelum itu dikirim per payment type oleh DAG api_landlord_revenue_sharing_00570.")
    dates       = _date_range(start_date_str, end_date_str)
    range_label = start_date_str if len(dates) == 1 else f"{start_date_str} s/d {end_date_str}"

    print("=" * 65)
    print(f"  API LANDLORD REVENUE SHARING PER ORDER  |  jilid={JILID}  |  date={range_label}  ({len(dates)} hari)")
    print(f"  dry_run={dry_run}  resend_uncertain={resend_uncertain}  load_data_by={load_data_by}")
    print("=" * 65)

    total_start = time.time()
    run_now     = datetime.now(timezone('Asia/Jakarta')).replace(tzinfo=None)   # HHMMSS di TransactionNumber = jam WIB

    post_attempted = False
    results        = {}
    try:
        # ---- STEP 1: EXTRACT ----
        print("\n  [1/4] EXTRACT")
        t0       = time.time()
        df_order = extract_order_income(config_dw, start_date_str, end_date_str)
        print(f"        order_est_income  : {len(df_order):>6} rows, {df_order['order_id'].nunique() if not df_order.empty else 0} order  ({time.time() - t0:.1f}s)")
        t0      = time.time()
        log_all = extract_send_log(config_dw)
        print(f"        log jilid {JILID}  : {len(log_all):>6} rows  ({time.time() - t0:.1f}s)")

        # ---- STEP 2: TRANSFORM + GUARD (semua cek sebelum POST pertama) ----
        print("\n  [2/4] TRANSFORM + GUARD")
        final_data, n_zero = build_payload(df_order, run_now)
        print(f"        payload           : {len(final_data):>6} rows  (total_sales = 0 di-skip: {n_zero})")

        to_send, info = apply_guard(final_data, log_all, JILID, resend_uncertain=resend_uncertain, dry_run=dry_run)
        n_uncertain = len(info['uncertain'])
        print(f"        sudah di mall (skip): {info['already']}")
        if n_uncertain:
            action = "TETAP DIKIRIM (resend_uncertain=True)" if resend_uncertain else "SKIP, cek manual dulu"
            print(f"        PERINGATAN {n_uncertain} baris timeout_error / pending di log -> {action}:")
            print(info['uncertain'][['TransactionDate', 'PaymentType', 'Amount', 'TransactionNumber']].to_string(index=False))
        if not info['dup_only'].empty:
            print(f"        INFO {info['dup_only']['key'].nunique()} nomor ditolak 'duplicate' tanpa pernah success di log:")
            print(info['dup_only'][['transaction_date', 'transaction_number', 'amount', 'created_at']].to_string(index=False))
        print(f"        akan dikirim      : {len(to_send):>6} rows")

        # ---- STEP 3: LOAD ----
        if dry_run:
            print("\n  [3/4] LOAD — DRY RUN: tidak ada POST & tidak ada log. Contoh payload:")
            sample = to_send[['TransactionNumber', 'TransactionDate', 'Amount', 'Remarks']].head(5).to_dict('records')
            for rec in sample:
                print(f"        {rec}")
            for d in dates:
                df_d = to_send[to_send['TransactionDate'] == d]
                results[d] = {"status": "DRY_RUN", "records": len(df_d), "amount": float(df_d['Amount'].sum())}
        else:
            print("\n  [3/4] LOAD — PENDING LOG → POST → FINALIZE LOG (per tanggal)")
            token = None
            for i, d in enumerate(dates, start=1):
                group = to_send[to_send['TransactionDate'] == d].reset_index(drop=True)
                if group.empty:
                    has_data = (final_data['TransactionDate'] == d).any()
                    results[d] = {"status": "SKIPPED_ALREADY_SENT" if has_data else "NO_DATA"}
                    print(f"        [{i}/{len(dates)}] {d} | {results[d]['status']} — tidak ada POST")
                    continue

                if token is None:
                    token = get_access_token(JILID)

                # jejak sebelum POST: gagal di sini = belum ada POST, aman di-retry
                insert_pending_logs(config_dw, group, run_now, load_data_by)

                post_attempted = True
                df_log_summary, df_result = post_one_date(
                    group, token, run_now,
                    refresh_token_fn=lambda: get_access_token(JILID),
                    load_data_by=load_data_by,
                )
                # tetap terlihat di log task walaupun simpan log ke DB gagal
                print(df_result[['transaction_number', 'amount', 'status', 'mall_transaction_id', 'error_message']].to_string(index=False))

                try:
                    finalize_logs(config_dw, df_log_summary, df_result)
                except Exception as e:
                    raise RuntimeError(
                        f"POST tanggal {d} SUDAH jalan tapi simpan log GAGAL: {e}. STOP, tanggal berikutnya tidak dikirim. "
                        f"Baris tanggal ini tetap 'pending' di {LOG_DETAIL_TABLE}; hasil POST ada di log task di atas."
                    ) from e

                is_duplicate  = df_result['error_message'].fillna('').astype(str).str.strip().eq('duplicate')
                api_success   = int((df_result['status'] == 'success').sum())
                api_duplicate = int(is_duplicate.sum())
                api_error     = len(df_result) - api_success - api_duplicate
                results[d] = {
                    "status":        "API_ERROR" if api_error > 0 else "SUCCESS",
                    "records_sent":  len(group),
                    "amount":        float(group['Amount'].sum()),
                    "api_success":   api_success,
                    "api_duplicate": api_duplicate,
                    "api_error":     api_error,
                }
                print(f"        [{i}/{len(dates)}] {d} | sent {len(group)} | amount {group['Amount'].sum():,.4f} | "
                      f"success {api_success} | duplicate {api_duplicate} | error {api_error} | log tersimpan")

                # jumlah success yang cocok di log HARUS sama dgn TotalSuccess dari mall (mis. nomor terpotong)
                total_success = df_log_summary['total_success'].iloc[0]
                if df_log_summary['http_status_code'].iloc[0] == 200 and int(total_success or 0) != api_success:
                    raise RuntimeError(
                        f"Tanggal {d}: mall bilang TotalSuccess = {total_success}, tapi yang cocok di log kita {api_success}. "
                        f"STOP. Cek response_payload di {LOG_SUMMARY_TABLE} dan betulkan status di {LOG_DETAIL_TABLE} "
                        "sebelum kirim ulang."
                    )

        total_elapsed = time.time() - total_start
        print(f"\n  TOTAL TIME: {total_elapsed:.1f}s")
        print("=" * 65)

        failed_dates = [d for d, r in results.items() if r["status"] == "API_ERROR"]
        stats = {
            "range":                          range_label,
            "jilid":                          JILID,
            "mode":                           "DRY RUN (tidak ada POST)" if dry_run else "LIVE",
            "load_data_by":                   load_data_by,
            "baris order (extract)":          f"{len(df_order):,}",
            "total_sales = 0 (skip)":         f"{n_zero:,}",
            "sudah di mall (skip)":           f"{info['already']:,}",
            "timeout/pending (skip, cek manual)" if not resend_uncertain else "timeout/pending (dikirim ulang)": f"{n_uncertain:,}",
            "dikirim":                        f"{len(to_send):,}",
            "total_time":                     f"{total_elapsed:.1f}s",
        }
        for d, r in results.items():
            if r["status"] in ("SUCCESS", "API_ERROR"):
                stats[d] = (f"{r['status']} | sent {r['records_sent']} | amount {r['amount']:,.4f} | success {r['api_success']} | "
                            f"duplicate {r['api_duplicate']} | error {r['api_error']}")
            elif r["status"] == "DRY_RUN":
                stats[d] = f"DRY_RUN | akan dikirim {r['records']} | amount {r['amount']:,.4f}"
            else:
                stats[d] = r["status"]

        warnings = []
        if n_uncertain and not resend_uncertain:
            warnings.append(f"{n_uncertain} baris timeout_error / pending di-skip (belum pasti sampai mall), cek manual")
        if not info['legacy'].empty:
            warnings.append("range berisi tanggal yang sudah diterima dalam format lama (per payment type)")
        if warnings:
            stats["PERINGATAN"] = "; ".join(warnings)

        error_msg = (
            f"API menolak / timeout untuk tanggal: {', '.join(failed_dates)}. Detail di {LOG_DETAIL_TABLE}. "
            "http_error / error dikirim ulang otomatis di run berikutnya; timeout_error perlu cek manual."
        ) if failed_dates else None

        try:
            _send_report_email(range_label, stats, error=error_msg, dry_run=dry_run)
        except Exception as mail_err:
            logging.warning(f"Failed to send report email: {mail_err}")

        if dry_run:
            status = "DRY_RUN"
        elif failed_dates:
            status = "API_ERROR"
        elif any(r["status"] == "SUCCESS" for r in results.values()):
            status = "SUCCESS"
        elif any(r["status"] == "SKIPPED_ALREADY_SENT" for r in results.values()):
            status = "SKIPPED_ALREADY_SENT"
        else:
            status = "SUCCESS_NO_DATA"

        return {"status": status, "dates": results, "failed_dates": failed_dates, "stats": stats}

    except Exception as e:
        error = str(e)
        if post_attempted:
            error += f" | POST ke API sudah dijalankan: cek log task / {LOG_DETAIL_TABLE} sebelum kirim ulang."
        print(f"\n  PIPELINE FAILED: {error}")
        try:
            _send_report_email(range_label, {"error": "lihat detail di atas"}, error=error, dry_run=dry_run)
        except Exception as mail_err:
            logging.warning(f"Failed to send failure email: {mail_err}")
        if post_attempted:
            raise PostAttemptedError(error) from e
        raise
