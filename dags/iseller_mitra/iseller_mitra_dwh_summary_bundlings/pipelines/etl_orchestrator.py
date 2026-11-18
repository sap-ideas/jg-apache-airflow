"""
ETL DWH iSeller Mitra 2/6: daily_bundling_summary_iseller_pusat (baris sources='iseller_mitra').

  1. Extract detail qty + detail amount + headers dari Data Lake
  2. Transform: merge + agregasi
  3. Load: DELETE window + INSERT dalam satu transaksi (dry_run: hanya bandingkan dengan DWH sekarang)
  4. Email sukses / gagal
"""

import time

from common.db_helpers import get_summary_before, print_before_after
from common.mitra_dwh_summary import compare_rows, send_report_email

from extract import extract_detail_amount, extract_detail_qty, extract_headers
from transform import transform_bundlings
from load import OWNED_WHERE, SUMMARY_COLS, TABLE_NAME, load_bundlings


def run_all(start_date, end_date, config_dw, dry_run=False, notify_failure=True):
    print("=" * 65)
    print(f"  [2/6] {TABLE_NAME} (iseller_mitra): {start_date} s/d {end_date}" + ("  [DRY RUN]" if dry_run else ""))
    print("=" * 65)
    t0 = time.time()
    try:
        df_final = transform_bundlings(extract_detail_qty(start_date, end_date, config_dw),
                                       extract_detail_amount(start_date, end_date, config_dw),
                                       extract_headers(start_date, end_date, config_dw))
        if dry_run:
            return compare_rows(config_dw, TABLE_NAME, df_final, start_date, end_date, OWNED_WHERE, SUMMARY_COLS)
        before = get_summary_before(config_dw, TABLE_NAME, SUMMARY_COLS, start_date, end_date, where_extra=OWNED_WHERE)
        deleted, inserted = load_bundlings(config_dw, df_final, start_date, end_date)
    except Exception as e:
        if not dry_run and notify_failure:
            send_report_email(TABLE_NAME, start_date, end_date, error=e)
        raise

    after = {c: float(df_final[c].sum()) for c in SUMMARY_COLS}
    print_before_after(before, after)
    result = {"deleted": deleted, "inserted": inserted, "before": before, "after": after,
              "seconds": round(time.time() - t0, 1)}
    send_report_email(TABLE_NAME, start_date, end_date, result=result)
    return result
