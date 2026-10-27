"""
Extract backfilling iSeller Pusat: token, order_id yang sudah ada di Data Lake, outlet, master produk,
dan tarik GetOrders per (outlet x jendela 4 jam).

Beda dengan iseller_pusat_fulfillment (hanya order fulfillment_status='none'): backfilling ini menutup order
yang TIDAK ADA SAMA SEKALI di Data Lake karena ingest harian tidak menariknya (outlet baru, panggilan gagal, dst.).
"""

import datetime
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests

from common.db_helpers import get_connection

BASE_URL = 'https://janjijiwapusat.isellershop.com/api/v2'
API_TIMEOUT_SEC = 60
API_MAX_RETRIES = 5            # jeda antar percobaan 2, 4, 8, 16 detik (error sementara iSeller, mis. deadlock)
API_RETRY_BACKOFF_SEC = 2
FETCH_MAX_WORKERS = 10
REFETCH_FAILED_WORKERS = 3     # panggilan yang gagal diulang dengan beban lebih ringan
REFETCH_EMPTY_WORKERS = 5
WINDOW_HOURS = 4               # 6 jendela per hari, sama dengan notebook ingest
GET_PRODUCTS_PAGE_SIZE = 500
GET_PRODUCTS_MAX_PAGES = 200   # batas pengaman loop, paging berhenti di page kosong
PROGRESS_EVERY_N_OUTLETS = 100


class MissingAccessTokenError(Exception):
    """access_token_pusat tidak ada di Airflow Variable maupun environment."""


class TokenExpiredError(Exception):
    """iSeller API membalas 401: token expired."""


def get_access_token():
    """Airflow Variable `access_token_pusat` (utama) -> env var `access_token_pusat` (fallback)."""
    token = ""
    try:
        from airflow.models import Variable  # type: ignore

        token = (Variable.get("access_token_pusat", default_var="") or "").strip()
    except Exception:
        token = ""
    if not token:
        token = (os.environ.get("access_token_pusat") or "").strip()
    if not token:
        raise MissingAccessTokenError(
            "access_token_pusat is missing. Set Airflow Variable `access_token_pusat` "
            "or environment variable `access_token_pusat`."
        )
    return token


def _adjust_timezone(dt_str, offset_hours):
    dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    return (dt + datetime.timedelta(hours=offset_hours)).strftime("%Y-%m-%d %H:%M:%S")


def build_windows(start_date, end_date):
    """
    Jendela tarik 4 jam per hari untuk start_date..end_date (YYYY-MM-DD, inklusif), dalam UTC (offset -7) seperti API.
    Akhir jendela xx:59:59.999 (bukan xx:59:59): transaksi yang closed di xx:59:59,xxx jatuh di celah 1 detik
    antar jendela dan tidak ketarik (terbukti Mitra 11-13 Okt 2026).
    """
    first = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    last = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    windows = []
    for day_offset in range((last - first).days + 1):
        date_str = (first + datetime.timedelta(days=day_offset)).strftime("%Y-%m-%d")
        for start_hour in range(0, 24, WINDOW_HOURS):
            end_hour = start_hour + WINDOW_HOURS - 1
            start_dt = _adjust_timezone(f"{date_str} {start_hour:02}:00:00", -7)
            end_dt = _adjust_timezone(f"{date_str} {end_hour:02}:59:59", -7) + ".999"
            windows.append((start_dt, end_dt))
    return windows


def get_existing_order_ids(config_dw, start_date, end_date):
    """order_id yang sudah ada di header Data Lake untuk transaction_date start_date .. end_date (inklusif)."""
    conn = get_connection(config_dw, config_dw.db_resource)
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT DISTINCT order_id FROM transactions_iseller_pusat "
            "WHERE transaction_date >= %s AND transaction_date < DATE_ADD(%s, INTERVAL 1 DAY)",
            (start_date, end_date),
        )
        ids = {row[0] for row in cursor.fetchall()}
        cursor.close()
    finally:
        conn.close()
    return ids


def _auth_headers(access_token):
    return {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/x-www-form-urlencoded'}


def _with_retry(fn):
    """Jalankan fn() dengan retry (error sementara iSeller, mis. deadlock, juga muncul di GetProducts/getstoreinfo). 401 tidak di-retry."""
    for attempt in range(API_MAX_RETRIES):
        try:
            return fn()
        except TokenExpiredError:
            raise
        except Exception:
            if attempt == API_MAX_RETRIES - 1:
                raise
            time.sleep(API_RETRY_BACKOFF_SEC ** (attempt + 1))


def get_outlet_ids(access_token):
    """Semua outlet aktif dari getstoreinfo (bukan dari Data Lake: outlet baru yang belum pernah tertarik harus ikut)."""
    def _call():
        resp = requests.get(f'{BASE_URL}/getstoreinfo', headers=_auth_headers(access_token), timeout=API_TIMEOUT_SEC)
        if resp.status_code == 401:
            raise TokenExpiredError("iSeller API returned 401 Unauthorized on getstoreinfo.")
        resp.raise_for_status()
        return json.loads(resp.text)['store_info']['active_outlets']

    outlets = _with_retry(_call)
    df_outlet = pd.DataFrame(outlets)
    df_outlet['outlet_code'] = df_outlet['outlet_code'].str.zfill(5)
    df_outlet = df_outlet[df_outlet['outlet_code'] != 'Main Outlet']
    return list(df_outlet['outlet_id'].unique())


def get_master_products(access_token):
    """Master produk (non-combo) dari GetProducts: dipakai hanya sebagai bobot harga untuk membagi nilai combo."""
    frames = []
    for page in range(1, GET_PRODUCTS_MAX_PAGES + 1):
        data = {'modified_after': '2023-01-01', 'track_inventory': 'FALSE',
                'page_size': GET_PRODUCTS_PAGE_SIZE, 'page': page}

        def _call(data=data, page=page):
            resp = requests.post(f'{BASE_URL}/GetProducts', headers=_auth_headers(access_token), data=data,
                                 timeout=API_TIMEOUT_SEC)
            if resp.status_code == 401:
                raise TokenExpiredError("iSeller API returned 401 Unauthorized on GetProducts.")
            resp.raise_for_status()
            result = json.loads(resp.text)
            products = result.get('products') if isinstance(result, dict) else None
            if not isinstance(products, list):
                raise ValueError(f"GetProducts page {page}: response tidak berisi list 'products': {resp.text[:200]}")
            return products

        products = _with_retry(_call)
        if not products:
            break
        frames.append(pd.DataFrame(products))
    else:
        raise ValueError(f"GetProducts belum habis setelah {GET_PRODUCTS_MAX_PAGES} page, master produk bisa terpotong")
    if not frames:
        raise ValueError("GetProducts tidak mengembalikan produk sama sekali")
    df = pd.concat(frames, ignore_index=True)
    df = df[df['type'] != 'comboset'][['product_id', 'product_type', 'sku', 'name', 'type', 'taxable', 'price', 'sold_count', 'modified_date']]
    df = df[df['sku'] != '']
    df = df[df['price'] != 0]
    print(f"        Master products: {len(df):,}")
    return df


def _fetch_orders(access_token, outlet_id, start_dt, end_dt):
    """1 panggilan GetOrders. Retry 5x; 401 langsung TokenExpiredError (tidak di-retry)."""
    data = {
        'created_after': start_dt,
        'created_before': end_dt,
        'timezone': '7',
        'includes': 'orderdetails, promotiondetails, transactions, discountdetails',
        'outlet_id': outlet_id
    }
    for attempt in range(API_MAX_RETRIES):
        try:
            resp = requests.post(f'{BASE_URL}/GetOrders', headers=_auth_headers(access_token), data=data,
                                 timeout=API_TIMEOUT_SEC)
            if resp.status_code == 401:
                raise TokenExpiredError("iSeller API returned 401 Unauthorized on GetOrders.")
            resp.raise_for_status()
            result = resp.json()
            # API bisa balas 200 tapi isinya error (rate limit, deadlock, dll.) -> anggap gagal, coba lagi
            if result.get('status') is False or result.get('error_message'):
                raise ValueError(result.get('error_message'))
            return list(result.values())[1]
        except TokenExpiredError:
            raise
        except Exception as e:
            if attempt == API_MAX_RETRIES - 1:
                raise Exception(f"GAGAL outlet={outlet_id} {start_dt}~{end_dt}: {e}")
            time.sleep(API_RETRY_BACKOFF_SEC ** (attempt + 1))


def fetch_all_orders(access_token, outlet_ids, windows):
    """
    Tarik semua (outlet x jendela). Return (get_data, failed_calls):
      get_data[i]  = list order, [] kalau 0 order, None kalau tetap gagal
      failed_calls = [(outlet_id, start_dt, end_dt, pesan_error)] untuk panggilan yang TETAP gagal setelah semua retry.
    Panggilan gagal TIDAK menghentikan run (hasil yang berhasil tetap dipakai); caller yang memutuskan.
    """
    tasks = [(outlet_id, start_dt, end_dt) for start_dt, end_dt in windows for outlet_id in outlet_ids]
    print(f"        Total API calls: {len(tasks):,} ({len(outlet_ids)} outlet x {len(windows)} jendela)")
    get_data = [None] * len(tasks)
    calls_per_outlet = len(windows)
    outlet_done = {o: 0 for o in outlet_ids}
    n_outlet_done = 0

    def _run(indices, workers, track_outlets=False):
        nonlocal n_outlet_done
        failed = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_fetch_orders, access_token, *tasks[i]): i for i in indices}
            for future in as_completed(futures):
                i = futures[future]
                try:
                    get_data[i] = future.result()
                except TokenExpiredError:
                    for f in futures:
                        f.cancel()
                    raise
                except Exception as e:
                    failed[i] = str(e)
                if track_outlets:
                    outlet_done[tasks[i][0]] += 1
                    if outlet_done[tasks[i][0]] == calls_per_outlet:
                        n_outlet_done += 1
                        if n_outlet_done % PROGRESS_EVERY_N_OUTLETS == 0 or n_outlet_done == len(outlet_ids):
                            print(f"        [fetch] {n_outlet_done}/{len(outlet_ids)} outlet selesai")
        return failed

    failed = _run(range(len(tasks)), FETCH_MAX_WORKERS, track_outlets=True)

    if failed:
        print(f"        Panggilan gagal: {len(failed)} -> diulang dengan {REFETCH_FAILED_WORKERS} worker")
        failed = _run(sorted(failed), REFETCH_FAILED_WORKERS)

    # Panggilan 0 order diulang sekali: kalau ternyata ada order, percobaan pertama gagal diam-diam.
    # Kalau re-fetch ini gagal, hasil awal ([] = 0 order) tetap dipakai.
    empty_idx = [i for i, orders in enumerate(get_data) if orders is not None and not orders]
    _run(empty_idx, REFETCH_EMPTY_WORKERS)
    recovered = sum(1 for i in empty_idx if get_data[i])
    print(f"        Panggilan 0 order: {len(empty_idx):,} -> re-fetch memulihkan {recovered}")

    failed_calls = [(tasks[i][0], tasks[i][1], tasks[i][2], msg) for i, msg in sorted(failed.items())]
    return get_data, failed_calls
