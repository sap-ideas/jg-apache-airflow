"""Load functions — POST revenue records to the landlord API and dump logs to the Data Lake."""

import json
import time
from collections.abc import Callable
from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import text

from common.db_helpers import get_engine_datalake

JILID            = "01065"
REVENUE_API_URL  = "https://revenueapi.lippomalls.com:8443/api/revenue/v1/save"
LOG_SUMMARY_TABLE = "api_landlord_logs"
LOG_DETAIL_TABLE  = "api_landlord_log_detail"

DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"

MAX_ATTEMPTS     = 2
TIMEOUT_SECONDS  = 90
BACKOFF_SECONDS  = 5


def post_to_api(
    final_data: pd.DataFrame,
    access_token: str,
    now: datetime,
    refresh_token_fn: Callable[[], str] | None = None,
    load_data_by: str = DEFAULT_LOAD_DATA_BY,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """POST aggregated revenue records to the landlord Revenue API.

    Args:
        final_data:       output of transform.transform_sales
        access_token:     Bearer token from extract.get_access_token
        now:              timestamp used for the created_at column in logs
        refresh_token_fn: optional callable that fetches a brand new access_token.
                           If the API returns HTTP 401 on the first attempt, it is
                           called once to obtain a fresh token and the POST is retried.
        load_data_by:     tag written to the load_data_by column of both logs.

    Returns:
        (df_log_summary, df_log_detail)
    """
    revenue_data    = final_data[['TransactionNumber', 'TransactionDate', 'Amount', 'Remarks']].to_dict('records')
    request_payload = {"revenueDatas": revenue_data}
    headers         = {
        "Content-Type":  "application/json",
        "Authorization": f"Bearer {access_token}",
    }

    response      = None
    request_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.post(
                REVENUE_API_URL, json=request_payload, headers=headers, timeout=TIMEOUT_SECONDS,
            )
            break
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.RequestException) as e:
            request_error = e
            if attempt < MAX_ATTEMPTS:
                print(f"        RETRY | jilid: {JILID} | attempt {attempt} failed, waiting {BACKOFF_SECONDS}s: {str(e)}")
                time.sleep(BACKOFF_SECONDS)
            else:
                print(f"        RETRY | jilid: {JILID} | attempt {attempt} failed: {str(e)}")

    if response is not None and response.status_code == 401 and refresh_token_fn is not None:
        print(f"        RETRY | jilid: {JILID} | HTTP 401, refreshing access_token and retrying once")
        try:
            headers["Authorization"] = f"Bearer {refresh_token_fn()}"
            response = requests.post(
                REVENUE_API_URL, json=request_payload, headers=headers, timeout=TIMEOUT_SECONDS,
            )
        except Exception as e:
            print(f"        RETRY | jilid: {JILID} | token refresh + retry failed, keeping original 401 response: {str(e)}")

    request_df = final_data[
        ['jilid', 'TransactionNumber', 'TransactionDate', 'PaymentType',
         'TotalSales', 'TotalTax', 'Amount', 'Remarks']
    ].copy()
    request_df.columns = [
        'jilid', 'transaction_number', 'transaction_date', 'payment_type',
        'total_sales', 'total_tax', 'amount', 'remarks',
    ]

    if response is None:
        error_msg = f"Timeout/ConnectionError after {MAX_ATTEMPTS} attempts: {str(request_error)}"

        df_log_summary = pd.DataFrame([{
            'revenue_batch_id':   None,
            'jilid':              JILID,
            'transaction_date':   final_data['TransactionDate'].iloc[0] if not final_data.empty else None,
            'total_records_sent': len(revenue_data),
            'total_success':      0,
            'total_error':        len(revenue_data),
            'http_status_code':   None,
            'api_code':           None,
            'api_message':        error_msg,
            'api_url':            REVENUE_API_URL,
            'request_payload':    json.dumps(request_payload),
            'response_payload':   error_msg,
            'created_at':         now,
        }])

        df_log_detail = request_df.copy()
        df_log_detail['mall_transaction_id'] = None
        df_log_detail['revenue_batch_id']    = None
        df_log_detail['status']              = 'timeout_error'
        df_log_detail['error_message']       = error_msg
        df_log_detail['created_at']          = now

        print(f"        TIMEOUT | jilid: {JILID} | skipped after {MAX_ATTEMPTS} attempts: {str(request_error)}")

    elif response.status_code == 200:
        try:
            resp_json = response.json()
        except ValueError:
            resp_json = {
                'Message':                'Non-JSON 200 body: ' + response.text[:500],
                'transactionReturnDatas': [],
                'ErrorTransactionNumber': [],
            }

        df_log_summary = pd.DataFrame([{
            'revenue_batch_id':   resp_json.get('RevenueBatchId'),
            'jilid':              JILID,
            'transaction_date':   final_data['TransactionDate'].iloc[0],
            'total_records_sent': len(revenue_data),
            'total_success':      resp_json.get('TotalSuccess'),
            'total_error':        resp_json.get('TotalError'),
            'http_status_code':   response.status_code,
            'api_code':           resp_json.get('Code'),
            'api_message':        resp_json.get('Message'),
            'api_url':            REVENUE_API_URL,
            'request_payload':    json.dumps(request_payload),
            'response_payload':   json.dumps(resp_json),
            'created_at':         now,
        }])

        response_details = pd.DataFrame(resp_json.get('transactionReturnDatas', []))
        if not response_details.empty:
            response_details = response_details.rename(columns={
                'TenantTransactionNumber': 'transaction_number',
                'MallTransactionId':       'mall_transaction_id',
            })
            df_log_detail = request_df.merge(response_details, on='transaction_number', how='left')
        else:
            df_log_detail = request_df.copy()
            df_log_detail['mall_transaction_id'] = None

        error_map = {}
        for err in resp_json.get('ErrorTransactionNumber', []):
            if '|' in str(err):
                txn_num, error_msg = str(err).split('|', 1)
                error_map[txn_num] = error_msg
            else:
                error_map[str(err)] = 'unknown error'

        df_log_detail['revenue_batch_id'] = resp_json.get('RevenueBatchId')
        df_log_detail['status']           = df_log_detail['mall_transaction_id'].apply(
            lambda x: 'success' if pd.notna(x) else 'error'
        )
        df_log_detail['error_message'] = df_log_detail['transaction_number'].map(error_map)
        df_log_detail['created_at']    = now

        print(f"        SUCCESS | jilid: {JILID} | HTTP {response.status_code} | {resp_json.get('Message')}")

    else:
        df_log_summary = pd.DataFrame([{
            'revenue_batch_id':   None,
            'jilid':              JILID,
            'transaction_date':   final_data['TransactionDate'].iloc[0] if not final_data.empty else None,
            'total_records_sent': len(revenue_data),
            'total_success':      0,
            'total_error':        len(revenue_data),
            'http_status_code':   response.status_code,
            'api_code':           None,
            'api_message':        response.text[:500],
            'api_url':            REVENUE_API_URL,
            'request_payload':    json.dumps(request_payload),
            'response_payload':   response.text[:500],
            'created_at':         now,
        }])

        df_log_detail = request_df.copy()
        df_log_detail['mall_transaction_id'] = None
        df_log_detail['revenue_batch_id']    = None
        df_log_detail['status']              = 'http_error'
        df_log_detail['error_message']       = f"HTTP {response.status_code}: {response.text[:200]}"
        df_log_detail['created_at']          = now

        print(f"        FAILED | jilid: {JILID} | HTTP {response.status_code} | {response.text[:200]}")

    df_log_summary['load_data_by'] = load_data_by
    df_log_detail['load_data_by']  = load_data_by

    return df_log_summary, df_log_detail


def dump_logs_idempotent(
    config_dw,
    df_log_summary: pd.DataFrame,
    df_log_detail: pd.DataFrame,
    target_date_str: str,
) -> dict:
    """Atomically replace existing log rows for (jilid, transaction_date) then re-insert.

    Keeps Airflow retries safe — a re-run simply re-deletes then re-inserts,
    never producing duplicates.

    Args:
        config_dw:        Data Warehouse config module.
        df_log_summary:   output of post_to_api (summary df).
        df_log_detail:    output of post_to_api (detail df).
        target_date_str:  'YYYY-MM-DD' of the target processing date.

    Returns:
        dict with keys: summary_deleted, summary_inserted, detail_deleted, detail_inserted
    """
    engine = get_engine_datalake(config_dw)

    summary_deleted = summary_inserted = 0
    detail_deleted  = detail_inserted  = 0

    if not df_log_summary.empty:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"DELETE FROM {LOG_SUMMARY_TABLE} "
                    f"WHERE jilid = :jilid AND transaction_date = :td"
                ),
                {"jilid": JILID, "td": target_date_str},
            )
            summary_deleted = result.rowcount
            df_log_summary.to_sql(
                name=LOG_SUMMARY_TABLE, con=conn, if_exists="append", index=False,
            )
            summary_inserted = len(df_log_summary)

    if not df_log_detail.empty:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"DELETE FROM {LOG_DETAIL_TABLE} "
                    f"WHERE jilid = :jilid AND transaction_date = :td"
                ),
                {"jilid": JILID, "td": target_date_str},
            )
            detail_deleted = result.rowcount
            df_log_detail.to_sql(
                name=LOG_DETAIL_TABLE, con=conn, if_exists="append", index=False,
            )
            detail_inserted = len(df_log_detail)

    return {
        "summary_deleted": summary_deleted, "summary_inserted": summary_inserted,
        "detail_deleted":  detail_deleted,  "detail_inserted":  detail_inserted,
    }
