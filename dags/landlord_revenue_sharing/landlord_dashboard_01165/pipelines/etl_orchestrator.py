"""Orchestrator - extract -> transform -> load -> email report."""

import os
import smtplib
import ssl

from email.mime.text import MIMEText

from extract import extract_income
from transform import compute_start_date, pivot_detail, build_html, OUTLET_CODE
from load import update_custom_menu, CUSTOM_MENU_ID

SMTP_SERVER    = "smtp.gmail.com"
SMTP_PORT      = 587
SENDER_EMAIL   = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
SMTP_PASSWORD  = os.getenv("GMAIL_SMTP_PASSWORD")


def _send_report_email(stats: dict, error: str | None = None):
    td   = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    th   = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'
    rows = "".join(f"<tr><td {td}>{k}</td><td {td}>{v}</td></tr>" for k, v in stats.items())
    err  = (f'<div style="background:#f8d7da;border:1px solid #f5c6cb;color:#721c24;padding:15px;border-radius:5px;">'
            f'<strong>Pipeline FAILED.</strong><br><code>{error}</code></div>') if error else ""
    title = f"Landlord Dashboard {OUTLET_CODE} - {'FAILED' if error else 'Completed'}"

    html = f"""\
    <html><body style="font-family:Arial,sans-serif;color:#333;">
        <h2 style="color:{'#dc3545' if error else '#4472C4'};">{title}</h2>
        {err}
        <table style="border-collapse:collapse;width:100%;margin:15px 0;">
            <tr><th {th}>Metric</th><th {th}>Value</th></tr>{rows}
        </table>
        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body></html>"""

    msg = MIMEText(html, "html")
    msg["Subject"] = f"[{'FAILED' if error else 'REPORT'}] Landlord Dashboard {OUTLET_CODE} -> custom_menus id {CUSTOM_MENU_ID}"
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(RECEIVER_EMAIL)

    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.ehlo()
        server.starttls(context=ctx)
        server.ehlo()
        server.login(SENDER_EMAIL, SMTP_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
    print(f"  Report email sent to: {', '.join(RECEIVER_EMAIL)}")


def run_all(config_dw, config_jiwaplus, dry_run_path=None):
    """dry_run_path: kalau diisi, HTML ditulis ke path itu dan DB TIDAK disentuh (untuk test manual)."""
    print("=" * 65)
    print(f"Landlord Dashboard {OUTLET_CODE} -> data_report.custom_menus id={CUSTOM_MENU_ID}")
    print("=" * 65)

    stats = {}
    try:
        start_date = compute_start_date()
        print(f"[1/3] extract (date >= {start_date})")
        raw = extract_income(config_dw, start_date)

        print("[2/3] transform")
        detail = pivot_detail(raw)
        html = build_html(detail)
        stats = {
            "Date range":   f"{detail['date'].min()} s/d {detail['date'].max()}",
            "Days":         len(detail),
            "Total order":  f"{detail['total_order'].sum():,.0f}",
            "Total income": f"{detail['daily_income'].sum():,.0f}",
            "HTML size":    f"{len(html) / 1024:,.1f} KB",
        }
        print("  " + " | ".join(f"{k}: {v}" for k, v in stats.items()))

        if dry_run_path:
            with open(dry_run_path, "w", encoding="utf-8", newline="") as f:
                f.write(html)
            print(f"[3/3] DRY RUN: HTML ditulis ke {dry_run_path}, DB tidak disentuh")
            return stats

        print("[3/3] load")
        stats["Rows updated"] = update_custom_menu(config_jiwaplus, html)
    except Exception as e:
        try:
            _send_report_email(stats, error=str(e))
        except Exception as mail_err:
            print(f"  WARNING: email gagal terkirim: {mail_err}")
        raise

    try:
        _send_report_email(stats)
    except Exception as mail_err:
        print(f"  WARNING: email gagal terkirim: {mail_err}")
    return stats
