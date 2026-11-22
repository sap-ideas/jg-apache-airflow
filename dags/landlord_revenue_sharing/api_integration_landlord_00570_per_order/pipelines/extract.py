"""Extract functions — token, estimated income per order (DWH) and the send log (Data Lake)."""

import os
import time

import pandas as pd
import requests
from dotenv import load_dotenv

from common.db_helpers import get_connection

load_dotenv()

JILID = "00570"
SOURCE_TABLE = "order_est_income_for_landlord"
LOG_DETAIL_TABLE = "api_landlord_log_detail"

MAX_ATTEMPTS    = 2
TIMEOUT_SECONDS = 30
BACKOFF_SECONDS = 5


def get_access_token(jilid: str = JILID) -> str:
    """Read credentials for `jilid` from environment variables and obtain a Bearer token.

    Credentials live in `.env` (see `.env.example`), keyed per outlet so adding a new
    jilid never needs a code change:
        LANDLORD_TOKEN_API_URL              — shared token endpoint (all outlets)
        LANDLORD_OUTLET_<jilid>_EMAIL
        LANDLORD_OUTLET_<jilid>_PASSWORD

    Args:
        jilid: outlet code to fetch credentials for. Defaults to this module's JILID.

    Returns:
        access_token string

    Raises:
        RuntimeError: if credentials for `jilid` are not set, the token request fails,
                      or every retry attempt times out / fails to connect.
    """
    url      = os.getenv("LANDLORD_TOKEN_API_URL")
    email    = os.getenv(f"LANDLORD_OUTLET_{jilid}_EMAIL")
    password = os.getenv(f"LANDLORD_OUTLET_{jilid}_PASSWORD")

    if not url or not email or not password:
        raise RuntimeError(
            f"Missing landlord API credentials for jilid {jilid} — set "
            f"LANDLORD_TOKEN_API_URL, LANDLORD_OUTLET_{jilid}_EMAIL and "
            f"LANDLORD_OUTLET_{jilid}_PASSWORD in .env (see .env.example)"
        )

    response      = None
    request_error = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = requests.post(
                url,
                data={"grant_type": "password", "username": email, "password": password},
                timeout=TIMEOUT_SECONDS,
            )
            break
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError,
                requests.exceptions.RequestException) as e:
            request_error = e
            if attempt < MAX_ATTEMPTS:
                print(f"        RETRY | jilid: {jilid} | token request attempt {attempt} failed, waiting {BACKOFF_SECONDS}s: {str(e)}")
                time.sleep(BACKOFF_SECONDS)
            else:
                print(f"        RETRY | jilid: {jilid} | token request attempt {attempt} failed: {str(e)}")

    if response is None:
        raise RuntimeError(
            f"Token request failed after {MAX_ATTEMPTS} attempts (timeout/connection error): {str(request_error)}"
        )

    if response.status_code == 200:
        token = response.json().get("access_token")
        if not token:
            raise RuntimeError(f"access_token missing in response: {response.text[:300]}")
        print(f"        access_token obtained for jilid {jilid}  (truncated): {token[:20]}...")
        return token

    raise RuntimeError(
        f"Token request failed — HTTP {response.status_code}: {response.text[:300]}"
    )


def extract_order_income(config_dw, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    """Estimated income per order (1 baris per order_id + transaction_id) untuk JILID di rentang tanggal.

    Sumber: order_est_income_for_landlord (DWH), dibangun DAG f_order_estimated_income_per_store.
    """
    conn = get_connection(config_dw, config_dw.db_target)
    query = f"""
        SELECT date, outlet_code, order_id, transaction_id, order_reference, payment_type,
               subtotal, tax_amount, total_sales, sales_type_commission
        FROM {SOURCE_TABLE}
        WHERE outlet_code = %s
            AND date BETWEEN %s AND %s
        ORDER BY transaction_date
    """
    df = pd.read_sql(query, conn, params=(JILID, start_date_str, end_date_str))
    conn.close()
    return df


def extract_send_log(config_dw) -> pd.DataFrame:
    """SEMUA baris api_landlord_log_detail untuk JILID (tabel kecil) — dipakai guard anti-dobel.

    Per order dicek per nomor (tanpa HHMMSS), format lama (per payment type) dicek per tanggal.
    """
    conn = get_connection(config_dw, config_dw.db_resource)
    query = f"""
        SELECT transaction_number, transaction_date, payment_type, amount, remarks, status, error_message, created_at
        FROM {LOG_DETAIL_TABLE}
        WHERE jilid = %s
    """
    df = pd.read_sql(query, conn, params=(JILID,))
    conn.close()
    return df
