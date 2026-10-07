"""
Pipeline: Clean Duplicated Data in Data Lake

Flow:
  1. Load headers (transactions_iseller_mitra) & details (transactions_items_iseller_mitra)
  2. Remove duplicates:
       - Headers: based on (order_id, transaction_id)
       - Details: dedup_details_v2 (lihat dedup_details.py)
           * non-combo -> 1 row per composite key
           * combo     -> rows_per_key / dup_factor, dup_factor HANYA dari master bundling (API);
                          tidak bisa ditentukan dari master -> tidak dipotong + needs_manual_check
  3. Log hasil + simpan report CSV (needs_manual_check, slot mismatch, dll) di folder log Airflow
  4. Delete + re-dump hanya tabel yang terdeteksi dobel di STEP 1 (clean_headers / clean_details)
     dan benar-benar berubah. DELETE & re-dump dalam 1 transaksi: gagal -> rollback, DL tetap utuh.
"""

import os

import pandas as pd
import pendulum
from sqlalchemy import text
from common.db_helpers import get_connection, get_engine_datalake
from dedup_details import HEADER_KEY, build_dedup_reports, dedup_details_v2

PREVIEW_ROWS = 10
REPORT_FOLDER_NAME = 'iseller_mitra_dedup_reports'
# report yang disimpan sebagai CSV kalau tidak kosong
CSV_REPORTS = ['needs_manual_check', 'slot_mismatch', 'bundling_not_in_master']

_MITRA_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _print_df(df, indent=8, max_rows=PREVIEW_ROWS):
    pad = ' ' * indent
    text_df = df.head(max_rows).to_string(index=False)
    print('\n'.join(pad + line for line in text_df.splitlines()))
    if len(df) > max_rows:
        print(f"{pad}... ({len(df):,} rows, preview {max_rows})")


def _report_base_dir():
    """Folder log Airflow kalau ada, selain itu <project>/reports (run lokal)."""
    try:
        from airflow.configuration import conf  # type: ignore
        base_log_folder = conf.get('logging', 'base_log_folder')
        if base_log_folder and os.path.isdir(base_log_folder):
            return os.path.join(base_log_folder, REPORT_FOLDER_NAME)
    except Exception:
        pass
    return os.path.join(_MITRA_ROOT, 'reports')


def _save_reports(reports, start_date, end_date):
    """Simpan report yang tidak kosong. Return (report_dir|None, {name: path})."""
    to_save = {name: reports[name] for name in CSV_REPORTS if len(reports[name])}
    if not to_save:
        return None, {}
    run_ts = pendulum.now('Asia/Jakarta').format('YYYYMMDDTHHmmss')
    report_dir = os.path.join(_report_base_dir(), f"{start_date}_{end_date}_{run_ts}")
    os.makedirs(report_dir, exist_ok=True)
    paths = {}
    for name, df in to_save.items():
        path = os.path.join(report_dir, f"{name}.csv")
        df.to_csv(path, index=False)
        paths[name] = path
    return report_dir, paths


def _print_report_section(df, name, report_files, empty_text):
    if df.empty:
        if empty_text:
            print(f"        {empty_text}")
        return
    _print_df(df)
    if name in report_files:
        print(f"        Report CSV: {report_files[name]}")


def run_clean_duplicates(start_date, end_date, config_dw, clean_headers=True, clean_details=True,
                         bundling_slot_count=None, master_status=None, dry_run=False):
    """
    Clean duplicated rows in Data Lake source tables for the given date range.

    Args:
        clean_headers: True kalau headers terdeteksi dobel di STEP 1 -> boleh di-DELETE & re-dump.
        clean_details: True kalau details non-combo terdeteksi dobel di STEP 1 -> boleh di-DELETE & re-dump.
                       Tabel dengan flag False TIDAK disentuh (hasil dedup-nya hanya info di log/report).

    Returns dict:
      headers_before, headers_after  (hanya kalau clean_headers=True)
      details_before, details_after  (hanya kalau clean_details=True)
      headers_rewritten, details_rewritten, dedup_summary, report_files, dry_run
    """
    # Tanpa master bundling dari API, cleaning tidak boleh jalan (tidak ada fallback)
    if not (master_status or {}).get('ok') or bundling_slot_count is None or bundling_slot_count.empty:
        raise ValueError(
            "run_clean_duplicates butuh master bundling yang berhasil di-load dari API "
            f"(master_status: {(master_status or {}).get('message', '-')}). Cleaning dibatalkan, DL tidak disentuh."
        )

    print()
    print("=" * 65)
    print("  STEP 3: CLEAN DUPLICATED DATA IN DATA LAKE")
    print(f"  Period: {start_date} s/d {end_date}")
    scope = [name for name, flag in (('headers', clean_headers), ('details', clean_details)) if flag]
    print(f"  Scope : {', '.join(scope) or '-'}")
    if dry_run:
        print("  MODE  : DRY RUN (tanpa DELETE & re-dump)")
    print("=" * 65)

    # ---- 1. LOAD ----
    # headers selalu di-load: untuk dedup headers + akurasi amount per tanggal
    conn = get_connection(config_dw, config_dw.db_resource)

    print("\n  Loading headers (transactions_iseller_mitra) ...")
    df_headers = pd.read_sql(f"""
        SELECT * FROM transactions_iseller_mitra
        WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
    """, conn)
    print(f"        Loaded: {len(df_headers):,} rows")

    print("  Loading details (transactions_items_iseller_mitra) ...")
    df_details = pd.read_sql(f"""
        SELECT * FROM transactions_items_iseller_mitra
        WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
    """, conn)
    print(f"        Loaded: {len(df_details):,} rows")

    conn.close()

    # ---- 2. DEDUPLICATE ----
    df_headers_clean = df_headers.drop_duplicates(subset=HEADER_KEY)
    df_details_clean, df_work = dedup_details_v2(df_details, bundling_slot_count)
    summary, reports = build_dedup_reports(df_headers, df_headers_clean, df_details, df_details_clean, df_work)
    summary['master_status'] = (master_status or {}).get('message', '-')
    summary['master_ok'] = bool((master_status or {}).get('ok'))

    report_dir, report_files = _save_reports(reports, start_date, end_date)
    summary['report_dir'] = report_dir

    s = summary
    print("\n  [3.1] Hasil dedup")
    print(f"        Headers RAW -> CLEAN : {s['headers_raw']:,} -> {s['headers_clean']:,}  "
          f"(removed {s['headers_raw'] - s['headers_clean']:,})")
    print(f"        Details RAW -> CLEAN : {s['details_raw']:,} -> {s['details_clean']:,}  "
          f"(removed {s['details_raw'] - s['details_clean']:,})")
    print(f"          - non-combo        : {s['details_noncombo_raw']:,} -> {s['details_noncombo_clean']:,}  "
          f"(removed {s['details_noncombo_raw'] - s['details_noncombo_clean']:,})")
    print(f"          - combo            : {s['details_combo_raw']:,} -> {s['details_combo_clean']:,}  "
          f"(removed {s['details_combo_raw'] - s['details_combo_clean']:,})")
    print(f"        needs_manual_check   : {s['needs_manual_check_orders']:,} order")

    print("\n  [3.2] Sumber dup_factor combo (per order_detail; selain 'bundling' = tidak dipotong)")
    print("        " + " | ".join(f"{k} : {v:,}" for k, v in s['dup_factor_source_counts'].items()))

    print("\n  [3.3] Cek hasil: jumlah row combo setelah cleaning = jumlah slot?")
    match_pct = f"{s['combo_slot_match_pct']:.2f}%" if s['combo_slot_match_pct'] is not None else '-'
    print(f"        Combo order_detail sesuai slot : {match_pct} dari {s['combo_in_master']:,} (yang ada di master)")
    print(f"        Bundling di DL periode ini     : {s['bundling_in_period']:,} | "
          f"tidak ada di master: {s['bundling_not_in_master']:,} ({s['combo_not_in_master']:,} order_detail, tidak dipotong)")
    print(f"        Slot mismatch                  : {s['slot_mismatch_count']:,} order_detail")
    _print_report_section(reports['slot_mismatch'], 'slot_mismatch', report_files, '')
    if 'bundling_not_in_master' in report_files:
        print(f"        Report CSV bundling tidak ada di master: {report_files['bundling_not_in_master']}")

    print("\n  [3.4] Akurasi Total Amount per Tanggal (info; amount combo mitra tidak presisi)")
    acc = reports['accuracy_per_date'].copy()
    for col in ['headers_raw', 'details_raw', 'headers_clean', 'details_clean']:
        acc[col] = acc[col].map('{:,.0f}'.format)
    _print_df(acc, max_rows=len(acc))

    print("\n  [3.5] List needs_manual_check")
    _print_report_section(reports['needs_manual_check'], 'needs_manual_check', report_files, 'Tidak ada.')

    # Hanya tabel yang terdeteksi dobel di STEP 1 DAN benar-benar berubah yang di-DELETE & re-dump.
    # Tabel yang tidak dobel tidak disentuh, walaupun dedup membuang row (mis. fan-out promo combo).
    tables = [
        ('Headers', 'transactions_iseller_mitra', df_headers_clean,
         s['headers_raw'] - s['headers_clean'], clean_headers),
        ('Details', 'transactions_items_iseller_mitra', df_details_clean,
         s['details_raw'] - s['details_clean'], clean_details),
    ]
    to_rewrite = [t for t in tables if t[4] and t[3] > 0]

    summary['headers_rewritten'] = False
    summary['details_rewritten'] = False
    result = {
        'headers_rewritten': False, 'details_rewritten': False,
        'dedup_summary': summary, 'report_files': report_files, 'dry_run': dry_run,
    }
    if clean_headers:
        result['headers_before'], result['headers_after'] = s['headers_raw'], s['headers_clean']
    if clean_details:
        result['details_before'], result['details_after'] = s['details_raw'], s['details_clean']

    print("\n  Tabel yang akan ditulis ulang:")
    for label, table, _, removed, allowed in tables:
        if not allowed:
            action = "tidak dobel di STEP 1 -> TIDAK disentuh (hasil dedup di atas hanya info)"
        elif removed > 0:
            action = f"DELETE & re-dump (removed {removed:,})"
        else:
            action = "tidak ada row yang dibuang -> TIDAK disentuh"
        print(f"        {label:8s}: {action}")

    if not to_rewrite:
        print("\n  No duplicates to clean. Skipping DELETE & re-dump.")
        print("=" * 65)
        return result

    if dry_run:
        print("\n  DRY RUN: DELETE & re-dump di-skip.")
        print("=" * 65)
        return result

    # ---- 3. DELETE + RE-DUMP dalam 1 transaksi ----
    # Kalau re-dump gagal di tengah, DELETE ikut di-rollback -> DL tidak kehilangan data.
    engine = get_engine_datalake(config_dw)
    deleted = {}
    with engine.begin() as conn:
        for label, table, df_clean, _, _ in to_rewrite:
            res = conn.execute(
                text(f"""
                    DELETE FROM {table}
                    WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') BETWEEN :start_date AND :end_date
                """),
                {"start_date": start_date, "end_date": end_date},
            )
            deleted[label] = res.rowcount
            df_clean.to_sql(table, con=conn, if_exists='append', index=False)

    print()
    for label, table, df_clean, _, _ in to_rewrite:
        result[f"{label.lower()}_rewritten"] = True
        summary[f"{label.lower()}_rewritten"] = True
        print(f"  {label:8s}: deleted {deleted[label]:,} -> re-dumped {len(df_clean):,}")

    print("\n  DL cleaning completed (committed).")
    print("=" * 65)

    return result
