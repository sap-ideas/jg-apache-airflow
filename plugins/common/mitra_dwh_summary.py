"""
Bagian bersama 6 DAG agregasi DWH iSeller Mitra (etl_iseller_mitra_dwh_summary_*).

- OUTLET_MAPPING_SQL: subquery mapping outlet, sama persis dengan pipeline re-dump di
  iseller_mitra_cleaning/pipelines/pipeline_daily_*.py.
- replace_rows: DELETE window + INSERT dalam SATU transaksi (gagal di tengah -> baris lama utuh).
- compare_rows: dry_run, bandingkan hasil hitung dengan isi DWH sekarang per tanggal, tanpa menulis.
- send_report_email: email sukses / gagal per tabel.
"""

import logging
import os
import smtplib
import ssl
from email.mime.text import MIMEText

import pandas as pd
from sqlalchemy import text

from common.db_helpers import get_engine

logger = logging.getLogger(__name__)

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SENDER_EMAIL = "saputra.christabel20@gmail.com"
RECEIVER_EMAIL = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]

_TD = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;"'
_TD_R = 'style="border:1px solid #ddd;padding:8px 12px;font-size:13px;text-align:right;"'
_TH = 'style="border:1px solid #ddd;padding:8px 12px;background-color:#4472C4;color:#fff;text-align:left;font-size:13px;"'

# Dipakai dengan alias tabel transaksi `transactions`: ... JOIN {OUTLET_MAPPING_SQL} outlet_mapping ON transactions.outlet_code = outlet_mapping.jilid
OUTLET_MAPPING_SQL = """(
            SELECT
                outlet_mapping.jilid,
                outlet_mapping.customer_name as outlet_name,
                outlet_mapping.region,
                outlet_mapping.provinsi,
                outlet_mapping.kota,
                outlet_mapping.ownership_model,
                outlet_mapping.outlet_brand,
                kbn_transition_outlets.start_date,
                kbn_transition_outlets.old_ownership_model,
                kbn_transition_outlets.old_outlet_brand
            FROM
            (
                SELECT DISTINCT
                    jilid, customer_name, region, provinsi, kota, ownership_model,
                    CASE WHEN outlet_brand_mapping.brand_name IS NOT NULL THEN outlet_brand_mapping.brand_name
                         ELSE outlet_mapping.brand
                    END outlet_brand
                FROM outlet_mapping
                LEFT JOIN outlet_brand_mapping ON outlet_mapping.brand = outlet_brand_mapping.brand
            ) outlet_mapping
            LEFT JOIN
            (
                SELECT
                    jilid,
                    date_format(start_date, "%Y-%m-%d") as start_date,
                    ownership_model as old_ownership_model,
                    outlet_brand_mapping.brand_name as old_outlet_brand
                FROM kbn_transition_outlets
                LEFT JOIN outlet_brand_mapping ON kbn_transition_outlets.brand = outlet_brand_mapping.brand
            ) kbn_transition_outlets ON outlet_mapping.jilid = kbn_transition_outlets.jilid
        )"""


def _where(start_date, end_date, owned_where):
    """Predikat baris milik pipeline ini di window. Selalu BETWEEN start AND end, tidak pernah >= start saja."""
    if not start_date or not end_date:
        raise ValueError(f"start_date dan end_date wajib diisi (start_date={start_date!r}, end_date={end_date!r})")
    return (f"date_format(transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}' "
            f"AND {owned_where}")


def replace_rows(config_dw, table, df, start_date, end_date, owned_where, dtype):
    """DELETE baris window lalu INSERT df, dalam satu transaksi. Return (deleted, inserted).

    df kosong padahal DWH punya baris di window -> raise tanpa menghapus apa pun
    (lebih aman daripada mengosongkan DWH karena sumber kosong).
    """
    where = _where(start_date, end_date, owned_where)
    with get_engine(config_dw).begin() as conn:
        if df.empty:
            existing = conn.execute(text(f"SELECT COUNT(*) FROM {table} WHERE {where}")).scalar()
            if existing:
                raise RuntimeError(f"Hasil hitung 0 baris padahal {table} punya {existing:,} baris di "
                                   f"{start_date} s/d {end_date}. Tidak ada yang dihapus; cek Data Lake dulu.")
            return 0, 0
        deleted = conn.execute(text(f"DELETE FROM {table} WHERE {where}")).rowcount
        # index tidak dimatikan: tabel DWH punya kolom `index` (sama dengan pipeline lama)
        df.to_sql(table, con=conn, if_exists='append', dtype=dtype)
    print(f"        Deleted: {deleted:,} records")
    print(f"        Dumped:  {len(df):,} records")
    return int(deleted), int(len(df))


def compare_rows(config_dw, table, df, start_date, end_date, owned_where, sum_cols):
    """dry_run: per tanggal, jumlah baris + jumlah kolom utama hasil hitung vs isi DWH sekarang. Tidak menulis."""
    where = _where(start_date, end_date, owned_where)
    sums = ", ".join(f"COALESCE(SUM({c}), 0)" for c in sum_cols)
    with get_engine(config_dw).connect() as conn:
        rows = conn.execute(text(
            f"SELECT date_format(transaction_date, '%Y-%m-%d'), COUNT(*), {sums} FROM {table} WHERE {where} GROUP BY 1"
        )).fetchall()
        table_cols = {r[0] for r in conn.execute(text(f"SHOW COLUMNS FROM {table}"))}

    dwh = {r[0]: [float(v) for v in r[1:]] for r in rows}
    new = {}
    if not df.empty:
        dates = pd.to_datetime(df['transaction_date']).dt.strftime('%Y-%m-%d')
        for d, g in df.groupby(dates):
            new[d] = [float(len(g))] + [float(pd.to_numeric(g[c]).sum()) for c in sum_cols]

    labels = ["rows"] + list(sum_cols)
    mismatch = []
    print(f"        DRY RUN {table}: hasil hitung vs DWH sekarang")
    for d in sorted(set(dwh) | set(new)):
        a, b = new.get(d), dwh.get(d)
        ok = a is not None and b is not None and all(abs(x - y) <= 0.01 for x, y in zip(a, b))
        if not ok:
            mismatch.append(d)
        detail = " | ".join(f"{k}: {('-' if a is None else f'{a[i]:,.2f}')} vs {('-' if b is None else f'{b[i]:,.2f}')}"
                            for i, k in enumerate(labels))
        print(f"        {d}  {'OK      ' if ok else 'MISMATCH'}  {detail}")

    missing_cols = [c for c in ['index', *df.columns] if c not in table_cols]
    if missing_cols:
        print(f"        PARITY MISMATCH: kolom tidak ada di tabel {table}: {missing_cols}")
    print(f"        PARITY {'OK' if not mismatch and not missing_cols else 'MISMATCH'}: "
          f"{len(mismatch)} tanggal beda {mismatch}")
    return {"dry_run": True, "mismatch_dates": mismatch, "missing_columns": missing_cols}


def send_report_email(table, start_date, end_date, result=None, error=None):
    """Email sukses (result) atau gagal (error). Gagal kirim email tidak boleh menggagalkan task."""
    try:
        if error is not None:
            subject = f"[FAILED] DWH Mitra - {table} ({start_date} s/d {end_date})"
            title, color = f"DWH Mitra {table} - FAILED", "#dc3545"
            body = (f'<p>DWH <strong>tidak berubah</strong> (DELETE + INSERT satu transaksi, di-rollback).</p>'
                    f'<p><strong>Error:</strong> <code>{type(error).__name__}: {str(error)[:1000]}</code></p>'
                    f'<p>Rerun: trigger manual DAG tabel ini dengan window yang sama.</p>')
        else:
            subject = f"[OK] DWH Mitra - {table} ({start_date} s/d {end_date})"
            title, color = f"DWH Mitra {table} - Completed", "#28a745"
            rows = [("Deleted", f"{result['deleted']:,}"), ("Inserted", f"{result['inserted']:,}"),
                    ("Durasi", f"{result['seconds']:,.1f} detik")]
            rows += [(f"{c} before → after", f"{result['before'][c]:,.0f} → {result['after'][c]:,.0f}")
                     for c in result['before']]
            body = ('<table style="border-collapse:collapse;width:100%;">'
                    f'<tr><th {_TH}>Metric</th><th {_TH}>Value</th></tr>'
                    + "".join(f'<tr><td {_TD}>{k}</td><td {_TD_R}>{v}</td></tr>' for k, v in rows)
                    + '</table>')
        msg = MIMEText(f"""\
        <html><body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;">
            <h2 style="color:{color};">{title}</h2>
            <p>Periode: <strong>{start_date}</strong> s/d <strong>{end_date}</strong> (sources iSeller Mitra)</p>
            {body}
            <p style="color:#888;font-size:12px;">Best Regards,<br><strong>DatO</strong> ( Data autOmation )</p>
        </body></html>""", 'html')
        msg['Subject'] = subject
        msg['From'] = SENDER_EMAIL
        msg['To'] = ", ".join(RECEIVER_EMAIL)
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=ssl.create_default_context())
            server.ehlo()
            server.login(SENDER_EMAIL, os.getenv("GMAIL_SMTP_PASSWORD"))
            server.sendmail(SENDER_EMAIL, RECEIVER_EMAIL, msg.as_string())
        print(f"  Email sent: {subject}")
    except Exception as e:
        logger.warning("Email DWH Mitra %s gagal dikirim: %s: %s", table, type(e).__name__, e)
