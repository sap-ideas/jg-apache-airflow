"""Load functions — tulis baris 'pending', POST ke API landlord, lalu catat hasilnya di log (Data Lake).

Urutan per tanggal (dipanggil dari etl_orchestrator):
  1. insert_pending_logs : baris detail status 'pending' (commit sendiri) -> jejak SEBELUM POST
  2. post_one_date       : POST semua baris 1 tanggal (1 batch)
  3. finalize_logs       : insert summary + UPDATE baris 'pending' -> status akhir, satu transaksi DB
Kalau langkah 3 gagal (atau proses mati di tengah POST), baris tetap 'pending' -> guard run berikutnya
skip + peringatan, jadi tidak ada kirim ulang otomatis (yang bisa dobel di mall).
"""

import json
import time
from collections.abc import Callable
from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import text

from common.db_helpers import get_engine_datalake

JILID             = "00570"
REVENUE_API_URL   = "https://revenueapi.lippomalls.com:8443/api/revenue/v1/save"
LOG_SUMMARY_TABLE = "api_landlord_logs"
LOG_DETAIL_TABLE  = "api_landlord_log_detail"

DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"
PENDING = "pending"

MAX_ATTEMPTS    = 2
TIMEOUT_SECONDS = 90
BACKOFF_SECONDS = 5

_DETAIL_COLS  = ['jilid', 'TransactionNumber', 'TransactionDate', 'PaymentType', 'TotalSales', 'TotalTax', 'Amount', 'Remarks']
_DETAIL_NAMES = ['jilid', 'transaction_number', 'transaction_date', 'payment_type', 'total_sales', 'total_tax', 'amount', 'remarks']


def _none_if_na(value):
    return None if pd.isna(value) else value


def insert_pending_logs(config_dw, group: pd.DataFrame, now: datetime, load_data_by: str) -> int:
    """Insert baris detail 1 tanggal dengan status 'pending' (commit sendiri, sebelum POST). Return jumlah baris."""
    detail = group[_DETAIL_COLS].copy()
    detail.columns = _DETAIL_NAMES
    detail['mall_transaction_id'] = None
    detail['revenue_batch_id']    = None
    detail['status']              = PENDING
    detail['error_message']       = None
    detail['created_at']          = now
    detail['load_data_by']        = load_data_by

    engine = get_engine_datalake(config_dw)
    with engine.begin() as conn:
        detail.to_sql(name=LOG_DETAIL_TABLE, con=conn, if_exists="append", index=False)
    return len(detail)


def post_one_date(
    group: pd.DataFrame,
    access_token: str,
    now: datetime,
    refresh_token_fn: Callable[[], str] | None = None,
    load_data_by: str = DEFAULT_LOAD_DATA_BY,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """POST semua baris 1 tanggal (1 batch).

    Returns:
        (df_log_summary, df_result) — df_result: transaction_number, amount, status,
        mall_transaction_id, revenue_batch_id, error_message (status akhir per baris).
    """
    revenue_data    = group[['TransactionNumber', 'TransactionDate', 'Amount', 'Remarks']].to_dict('records')
    request_payload = {"revenueDatas": revenue_data}
    headers         = {"Content-Type": "application/json", "Authorization": f"Bearer {access_token}"}
    target_date     = group['TransactionDate'].iloc[0]

    response      = None
    request_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            # retry di dalam run memakai TransactionNumber yang SAMA -> kalau attempt 1 ternyata sampai,
            # attempt 2 ditolak mall sebagai 'duplicate', bukan dobel
            response = requests.post(REVENUE_API_URL, json=request_payload, headers=headers, timeout=TIMEOUT_SECONDS)
            break
        except requests.exceptions.RequestException as e:
            request_error = e
            if attempt < MAX_ATTEMPTS:
                print(f"        RETRY | {target_date} | attempt {attempt} failed, waiting {BACKOFF_SECONDS}s: {str(e)}")
                time.sleep(BACKOFF_SECONDS)
            else:
                print(f"        RETRY | {target_date} | attempt {attempt} failed: {str(e)}")

    # 401 = token ditolak, mall tidak menyimpan apa pun -> aman ambil token baru & POST sekali lagi
    if response is not None and response.status_code == 401 and refresh_token_fn is not None:
        print(f"        RETRY | {target_date} | HTTP 401, refreshing access_token and retrying once")
        try:
            headers["Authorization"] = f"Bearer {refresh_token_fn()}"
            response = requests.post(REVENUE_API_URL, json=request_payload, headers=headers, timeout=TIMEOUT_SECONDS)
        except Exception as e:
            print(f"        RETRY | {target_date} | token refresh + retry failed, keeping original 401 response: {str(e)}")

    result = pd.DataFrame({
        'transaction_number': group['TransactionNumber'].values,
        'amount':             group['Amount'].values,
    })
    summary = {
        'revenue_batch_id':   None,
        'jilid':              JILID,
        'transaction_date':   target_date,
        'total_records_sent': len(revenue_data),
        'total_success':      0,
        'total_error':        len(revenue_data),
        'http_status_code':   None,
        'api_code':           None,
        'api_message':        None,
        'api_url':            REVENUE_API_URL,
        'request_payload':    json.dumps(request_payload),
        'response_payload':   None,
        'created_at':         now,
        'load_data_by':       load_data_by,
    }

    if response is None:
        error_msg = f"Timeout/ConnectionError after {MAX_ATTEMPTS} attempts: {str(request_error)}"
        summary.update({'api_message': error_msg, 'response_payload': error_msg})
        result['mall_transaction_id'] = None
        result['revenue_batch_id']    = None
        result['status']              = 'timeout_error'
        result['error_message']       = error_msg
        print(f"        TIMEOUT | {target_date} | after {MAX_ATTEMPTS} attempts: {str(request_error)}")

    elif response.status_code == 200:
        try:
            resp_json = response.json()
        except ValueError:
            resp_json = {'Message': f'Non-JSON 200 body: {response.text[:500]}',
                         'transactionReturnDatas': [], 'ErrorTransactionNumber': []}
        summary.update({
            'revenue_batch_id': resp_json.get('RevenueBatchId'),
            'total_success':    resp_json.get('TotalSuccess'),
            'total_error':      resp_json.get('TotalError'),
            'http_status_code': response.status_code,
            'api_code':         resp_json.get('Code'),
            'api_message':      resp_json.get('Message'),
            'response_payload': json.dumps(resp_json),
        })
        response_details = pd.DataFrame(resp_json.get('transactionReturnDatas', []))
        if not response_details.empty:
            response_details = response_details.rename(columns={
                'TenantTransactionNumber': 'transaction_number',
                'MallTransactionId':       'mall_transaction_id',
            })
            result = result.merge(response_details[['transaction_number', 'mall_transaction_id']],
                                  on='transaction_number', how='left')
        else:
            result['mall_transaction_id'] = None
        error_map = {}
        for err in resp_json.get('ErrorTransactionNumber', []):
            txn_num, _, error_msg = str(err).partition('|')
            error_map[txn_num] = error_msg if error_msg else 'unknown error'
        result['revenue_batch_id'] = resp_json.get('RevenueBatchId')
        result['status']           = result['mall_transaction_id'].apply(lambda x: 'success' if pd.notna(x) else 'error')
        result['error_message']    = result['transaction_number'].map(error_map)
        print(f"        HTTP 200 | {target_date} | {resp_json.get('Message')}")

    else:
        summary.update({'http_status_code': response.status_code, 'api_message': response.text[:500],
                        'response_payload': response.text[:500]})
        result['mall_transaction_id'] = None
        result['revenue_batch_id']    = None
        result['status']              = 'http_error'
        result['error_message']       = f"HTTP {response.status_code}: {response.text[:200]}"
        print(f"        FAILED | {target_date} | HTTP {response.status_code} | {response.text[:200]}")

    return pd.DataFrame([summary]), result


def finalize_logs(config_dw, df_log_summary: pd.DataFrame, df_result: pd.DataFrame) -> dict:
    """Satu transaksi: insert summary + UPDATE baris 'pending' tanggal ini ke status akhir.

    Baris di-match lewat TransactionNumber lengkap (dengan HHMMSS, unik per run) + status 'pending'.
    Kalau jumlah baris ter-update tidak sama dengan jumlah yang dikirim -> raise (rollback, baris tetap 'pending').
    """
    engine = get_engine_datalake(config_dw)
    updated = 0
    with engine.begin() as conn:
        df_log_summary.to_sql(name=LOG_SUMMARY_TABLE, con=conn, if_exists="append", index=False)
        stmt = text(
            f"UPDATE {LOG_DETAIL_TABLE} "
            "SET status = :status, mall_transaction_id = :mall_id, revenue_batch_id = :batch_id, error_message = :err "
            "WHERE jilid = :jilid AND transaction_number = :tn AND status = :pending"
        )
        for r in df_result.itertuples(index=False):
            updated += conn.execute(stmt, {
                'status':   r.status,
                'mall_id':  _none_if_na(r.mall_transaction_id),
                'batch_id': _none_if_na(r.revenue_batch_id),
                'err':      _none_if_na(r.error_message),
                'jilid':    JILID,
                'tn':       r.transaction_number,
                'pending':  PENDING,
            }).rowcount
        if updated != len(df_result):
            raise RuntimeError(
                f"UPDATE log detail cuma kena {updated} dari {len(df_result)} baris 'pending' (rollback)."
            )
    return {"summary_inserted": len(df_log_summary), "detail_updated": updated}
