"""
Check for duplicated transaction headers and line-item details in iSeller Mitra Data Lake.

If duplicates found:
  - Send email alert with report (outlet_code 01154: sum total_amount, count order_id per date)
  - Return has_duplicates=True -> proceed with Data Lake cleanup and DWH re-dump

If no duplicates:
  - Return has_duplicates=False -> skip re-dump
"""

import os
import smtplib
import ssl
import pandas as pd
from email.mime.text import MIMEText
from common.db_helpers import get_connection


def generate_report_outlet_01154(start_date, end_date, config_dw):
    """Same rows as the email report for outlet 01154 — reuse for Teams / callers."""
    return _generate_report_01154(start_date, end_date, config_dw)


def check_duplicates(start_date, end_date, config_dw):
    """
    Check whether headers or details have duplicated business keys in the given date range.

    Header duplicates:
      - transactions_iseller_mitra grouped by (order_id, transaction_id)

    Detail duplicates:
      - transactions_items_iseller_mitra grouped by composite business key:
        (order_id, order_detail_id, type, bundling_id, product_id, sku,
         status, payment_status, transactions_type)

    Returns a dict:
      - has_duplicates: bool
      - total_duplicated_orders: int (backward-compatible header duplicate count)
      - total_duplicated_headers: int (0 when none)
      - total_duplicated_details: int (0 when none)
      - report_df: pandas DataFrame for outlet 01154 (same data as email tables)
    """
    print("=" * 65)
    print("  STEP 1: CHECK DUPLICATED DATA di Data Lake")
    print(f"  Period: {start_date} s/d {end_date}")
    print("=" * 65)

    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()

    print("\n  Checking headers: transactions_iseller_mitra by (order_id, transaction_id) ...")
    cursor.execute(f"""
        SELECT order_id, transaction_id, COUNT(*) as cnt
        FROM transactions_iseller_mitra
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY order_id, transaction_id
        HAVING cnt > 1
    """)
    header_duplicates = cursor.fetchall()
    total_duplicated_headers = len(header_duplicates)

    print("  Checking details: transactions_items_iseller_mitra by composite business key ...")
    cursor.execute(f"""
        SELECT order_id, order_detail_id, type, bundling_id, product_id, sku,
               status, payment_status, transactions_type, COUNT(*) as cnt
        FROM transactions_items_iseller_mitra
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY order_id, order_detail_id, type, bundling_id, product_id, sku,
                 status, payment_status, transactions_type
        HAVING cnt > 1
    """)
    detail_duplicates = cursor.fetchall()
    total_duplicated_details = len(detail_duplicates)
    conn.close()

    print("\n  Duplicate check result:")
    print(f"        Headers duplicated keys: {total_duplicated_headers:,}")
    print(f"        Details duplicated keys: {total_duplicated_details:,}")

    has_duplicates = total_duplicated_headers > 0 or total_duplicated_details > 0

    if not has_duplicates:
        print("\n  RESULT: Tidak ada duplicated headers/details.")
        print("  Data mitra AMAN. SKIP re-dump pipelines.")

        report_df = _generate_report_01154(start_date, end_date, config_dw)
        _send_email_clean(start_date, end_date, report_df)

        print("\n  Email notifikasi data aman sudah dikirim.")
        print("=" * 65)
        return {
            "has_duplicates": False,
            "total_duplicated_orders": 0,
            "total_duplicated_headers": 0,
            "total_duplicated_details": 0,
            "report_df": report_df,
        }

    print(
        "\n  FOUND: duplicated data detected! "
        f"headers={total_duplicated_headers:,}, details={total_duplicated_details:,}"
    )

    # Generate report khusus outlet_code 01154
    report_df = _generate_report_01154(start_date, end_date, config_dw)

    # Send email alert
    _send_email_alert(
        start_date,
        end_date,
        total_duplicated_headers,
        total_duplicated_details,
        report_df,
    )

    print("\n  Email alert sudah dikirim.")
    print("  PROCEED ke re-dump pipelines...")
    print("=" * 65)
    return {
        "has_duplicates": True,
        "total_duplicated_orders": total_duplicated_headers,
        "total_duplicated_headers": total_duplicated_headers,
        "total_duplicated_details": total_duplicated_details,
        "report_df": report_df,
    }


def _generate_report_01154(start_date, end_date, config_dw):
    """Generate report: sum(total_amount) dan count(order_id) per date untuk outlet_code 01154."""
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT
            DATE_FORMAT(transaction_date, '%Y-%m-%d') AS transaction_date,
            COUNT(order_id) AS total_order_count,
            SUM(total_amount) AS total_amount
        FROM transactions_iseller_mitra
        WHERE date_format(transaction_date, "%Y-%m-%d") BETWEEN '{start_date}' AND '{end_date}'
            AND outlet_code = '01154'
        GROUP BY 1
        ORDER BY 1
    """)
    records = cursor.fetchall()
    conn.close()

    df = pd.DataFrame(records, columns=['transaction_date', 'total_order_count', 'total_amount'])

    print("\n  --- Report Outlet 01154 ---")
    print(df.to_string(index=False))
    print(f"\n  Grand Total: orders={df['total_order_count'].sum()}, amount={df['total_amount'].sum():,.0f}")

    return df


def _send_email_alert(start_date, end_date, total_duplicated_headers, total_duplicated_details, report_df):
    """Send HTML email alert with duplicate report."""
    th = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#dc3545;color:#fff;text-align:left;font-size:13px;"'
    td = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    td_total = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;font-weight:bold;background-color:#f8f8f8;"'
    td_total_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;font-weight:bold;background-color:#f8f8f8;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    grand_total_orders = report_df['total_order_count'].sum()
    grand_total_amount = report_df['total_amount'].sum()

    report_rows = ""
    for i, (_, row) in enumerate(report_df.iterrows()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        report_rows += (
            f'<tr{bg}>'
            f'<td {td}>{row["transaction_date"]}</td>'
            f'<td {td_r}>{int(row["total_order_count"]):,}</td>'
            f'<td {td_r}>{row["total_amount"]:,.0f}</td>'
            f'</tr>'
        )
    report_rows += (
        f'<tr>'
        f'<td {td_total}>Grand Total</td>'
        f'<td {td_total_r}>{int(grand_total_orders):,}</td>'
        f'<td {td_total_r}>{grand_total_amount:,.0f}</td>'
        f'</tr>'
    )

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#dc3545;">ALERT: Duplicated Data Detected</h2>
        <p>Ditemukan duplicated data di Data Lake iSeller Mitra:</p>
        <ul>
            <li><strong style="color:#dc3545;">{total_duplicated_headers:,}</strong> duplicated header key(s)
                <code>(order_id, transaction_id)</code> di table <code>transactions_iseller_mitra</code></li>
            <li><strong style="color:#dc3545;">{total_duplicated_details:,}</strong> duplicated detail key(s)
                <code>order_detail_id</code> di table <code>transactions_items_iseller_mitra</code></li>
        </ul>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>
        <p>Data Lake cleaning dan re-dump DWH summary akan dijalankan secara otomatis (selective: hanya pipeline yang sumber datanya terdampak).</p>

        <h3 style="color:#333;">Report Outlet 01154</h3>
        <table {table}>
            <tr>
                <th {th}>Transaction Date</th>
                <th {th}>Total Order Count</th>
                <th {th}>Total Amount</th>
            </tr>
            {report_rows}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[ALERT] Duplicated Data Found - iseller_mitra DL ({start_date} s/d {end_date})"

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print("  Email sent to:", ", ".join(receiver_email))


def _send_email_clean(start_date, end_date, report_df):
    """Send HTML email notification when no duplicates found (data aman)."""
    th = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#28a745;color:#fff;text-align:left;font-size:13px;"'
    td = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
    td_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
    td_total = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;font-weight:bold;background-color:#f8f8f8;"'
    td_total_r = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;font-weight:bold;background-color:#f8f8f8;text-align:right;"'
    table = 'style="border-collapse:collapse;width:100%;font-family:Arial,sans-serif;margin-bottom:20px;"'

    grand_total_orders = report_df['total_order_count'].sum()
    grand_total_amount = report_df['total_amount'].sum()

    report_rows = ""
    for i, (_, row) in enumerate(report_df.iterrows()):
        bg = ' style="background-color:#f2f2f2;"' if i % 2 == 0 else ""
        report_rows += (
            f'<tr{bg}>'
            f'<td {td}>{row["transaction_date"]}</td>'
            f'<td {td_r}>{int(row["total_order_count"]):,}</td>'
            f'<td {td_r}>{row["total_amount"]:,.0f}</td>'
            f'</tr>'
        )
    report_rows += (
        f'<tr>'
        f'<td {td_total}>Grand Total</td>'
        f'<td {td_total_r}>{int(grand_total_orders):,}</td>'
        f'<td {td_total_r}>{grand_total_amount:,.0f}</td>'
        f'</tr>'
    )

    html = f"""\
    <html>
    <body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
        <h2 style="color:#28a745;">DATA MITRA AMAN - No Duplicates Detected</h2>
        <p>Tidak ditemukan duplikat header <code>(order_id, transaction_id)</code>
           di <code>transactions_iseller_mitra</code>, dan tidak ditemukan duplikat detail
           <code>order_detail_id</code> di <code>transactions_items_iseller_mitra</code>.</p>
        <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong></p>
        <p>Re-dump DWH summary <strong>tidak diperlukan</strong>. Data sudah bersih.</p>

        <h3 style="color:#333;">Report Outlet 01154</h3>
        <table {table}>
            <tr>
                <th {th}>Transaction Date</th>
                <th {th}>Total Order Count</th>
                <th {th}>Total Amount</th>
            </tr>
            {report_rows}
        </table>

        <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
    </body>
    </html>"""

    subject = f"[OK] Data Mitra AMAN - transactions_iseller_mitra ({start_date} s/d {end_date})"

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    msg = MIMEText(html, 'html')
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = ", ".join(receiver_email)

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())

    print("  Email sent to:", ", ".join(receiver_email))
