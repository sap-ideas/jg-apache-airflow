"""
Pipeline: Clean Duplicated Data in Data Lake

Flow:
  1. Load only the tables that have duplicates (headers and/or details)
  2. Remove duplicates:
       - Headers: based on (order_id, transaction_id)
       - Details: based on (order_id, order_detail_id, type, bundling_id,
                            product_id, sku, status, payment_status, transactions_type)
  3. Delete rows for the date range from the affected DL tables only
  4. Re-dump cleaned DataFrames back to DL
"""

import pandas as pd
from common.db_helpers import get_connection, get_engine_datalake

_DETAIL_DEDUP_KEY = [
    'order_id', 'order_detail_id', 'type', 'bundling_id', 'product_id',
    'sku', 'status', 'payment_status', 'transactions_type',
]


def run_clean_duplicates(start_date, end_date, config_dw, clean_headers=True, clean_details=True):
    """
    Clean duplicated rows in Data Lake source tables for the given date range.

    Args:
        clean_headers: If True, clean transactions_iseller_mitra (headers).
        clean_details: If True, clean transactions_items_iseller_mitra (details).

    Returns dict with before/after row counts for each cleaned table.
    Keys present only for tables that were actually cleaned:
      headers_before, headers_after  (if clean_headers=True)
      details_before, details_after  (if clean_details=True)
    """
    print()
    print("=" * 65)
    print("  STEP 2: CLEAN DUPLICATED DATA IN DATA LAKE")
    print(f"  Period: {start_date} s/d {end_date}")
    scope = []
    if clean_headers:
        scope.append("headers")
    if clean_details:
        scope.append("details")
    print(f"  Scope : {', '.join(scope)}")
    print("=" * 65)

    result = {}
    df_headers_clean = None
    df_details_clean = None

    # ---- 1. LOAD & DEDUPLICATE ----
    conn = get_connection(config_dw, config_dw.db_resource)

    if clean_headers:
        print("\n  Loading headers (transactions_iseller_mitra) ...")
        df_headers = pd.read_sql(f"""
            SELECT * FROM transactions_iseller_mitra
            WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
        """, conn)
        before_headers = len(df_headers)
        df_headers_clean = df_headers.drop_duplicates(subset=['order_id', 'transaction_id'])
        after_headers = len(df_headers_clean)
        removed_headers = before_headers - after_headers
        print(f"        Loaded: {before_headers:,} rows  →  after dedup: {after_headers:,}  (removed {removed_headers:,})")
        result['headers_before'] = before_headers
        result['headers_after'] = after_headers

    if clean_details:
        print("  Loading details (transactions_items_iseller_mitra) ...")
        df_details = pd.read_sql(f"""
            SELECT * FROM transactions_items_iseller_mitra
            WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
        """, conn)
        before_details = len(df_details)
        df_details_clean = df_details.drop_duplicates(subset=_DETAIL_DEDUP_KEY)
        after_details = len(df_details_clean)
        removed_details = before_details - after_details
        print(f"        Loaded: {before_details:,} rows  →  after dedup: {after_details:,}  (removed {removed_details:,})")
        result['details_before'] = before_details
        result['details_after'] = after_details

    conn.close()

    # ---- 2. DELETE from DL ----
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()

    if clean_headers:
        cursor.execute(f"""
            DELETE FROM transactions_iseller_mitra
            WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
        """)
        deleted_headers = cursor.rowcount
        print(f"\n  Deleted from DL — Headers: {deleted_headers:,} rows")

    if clean_details:
        cursor.execute(f"""
            DELETE FROM transactions_items_iseller_mitra
            WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
        """)
        deleted_details = cursor.rowcount
        print(f"  Deleted from DL — Details: {deleted_details:,} rows")

    conn.commit()
    conn.close()

    # ---- 3. RE-DUMP cleaned data ----
    engine = get_engine_datalake(config_dw)

    print("\n  Re-dumping cleaned data to DL ...")
    if clean_headers:
        df_headers_clean.to_sql(
            'transactions_iseller_mitra', con=engine,
            if_exists='append', index=False
        )
        print(f"        Headers: {after_headers:,} rows dumped")

    if clean_details:
        df_details_clean.to_sql(
            'transactions_items_iseller_mitra', con=engine,
            if_exists='append', index=False
        )
        print(f"        Details: {after_details:,} rows dumped")

    print("\n  DL cleaning completed.")
    print("=" * 65)

    return result
