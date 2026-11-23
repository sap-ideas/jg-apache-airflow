"""
Extract intraday iSeller Pusat: token, outlet, master produk, GetOrders hari berjalan (00:00 WIB s/d sekarang),
dan kondisi tabel intraday saat ini.

Beda dengan iseller_pusat_backfilling (6 jendela 4 jam per outlet): intraday menarik 1 call per outlet untuk seluruh
hari berjalan (+-940 call, +-3 menit). GetOrders membalas MAKSIMAL 200 order per call (terukur 2026-11-23: outlet
tersibuk 1 call = 200 order, 6 jendela 4 jam = 349 order), jadi call yang membalas 200 order dibelah dua
(rekursif) sampai di bawah batas. Call kosong tidak di-re-fetch: 694 dari 937 outlet memang tidak aktif, dan
run jam berikutnya menarik ulang seluruh hari.
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
ORDERS_PER_CALL_CAP = 200      # batas order per call GetOrders; respons sebanyak ini dianggap terpotong
MIN_SPLIT_SECONDS = 60         # jendela sependek ini masih 200 order = tidak wajar, gagalkan (jangan diam-diam terpotong)
GET_PRODUCTS_PAGE_SIZE = 500
GET_PRODUCTS_MAX_PAGES = 200   # batas pengaman loop, paging berhenti di page kosong
PROGRESS_EVERY_N_OUTLETS = 100

HEADERS_TABLE = 'transactions_iseller_pusat_intraday'
DETAILS_TABLE = 'transactions_items_iseller_pusat_intraday'
_DT_FMT = "%Y-%m-%d %H:%M:%S"


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


def build_today_window(now_wib):
    """
    Jendela tarik hari berjalan: 00:00 WIB s/d sekarang, dalam UTC seperti API. `now_wib` = pendulum datetime Asia/Jakarta.
    Akhir jendela xx:xx:xx.999 (bukan xx:xx:xx): transaksi yang closed di detik terakhir jatuh di celah dan tidak ketarik
    (terbukti Mitra 11-13 Okt 2026). Order sesudah `sekarang` ketarik di run berikutnya (selalu 00:00 s/d sekarang).
    Return (start_dt, end_dt, cutoff_utc): cutoff_utc = awal hari ini dalam UTC = batas order_date untuk rollover H-1.
    """
    start_utc = now_wib.start_of('day').in_timezone('UTC')
    end_utc = now_wib.in_timezone('UTC')
    return start_utc.strftime(_DT_FMT), end_utc.strftime(_DT_FMT) + ".999", start_utc.strftime(_DT_FMT)


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


def _fetch_window(access_token, outlet_id, start_dt, end_dt):
    """
    GetOrders 1 outlet untuk start_dt..end_dt. Kalau balasan sebanyak ORDERS_PER_CALL_CAP (terpotong), jendela dibelah
    dua dan masing-masing ditarik ulang (rekursif). Dua separuh tidak tumpang tindih: separuh pertama berakhir
    di detik sebelum titik tengah (.999), separuh kedua mulai tepat di titik tengah.
    """
    orders = _fetch_orders(access_token, outlet_id, start_dt, end_dt)
    if len(orders) < ORDERS_PER_CALL_CAP:
        return orders
    start = datetime.datetime.strptime(start_dt, _DT_FMT)
    end = datetime.datetime.strptime(end_dt[:19], _DT_FMT)
    span = int((end - start).total_seconds())
    if span < MIN_SPLIT_SECONDS:
        raise ValueError(f"outlet={outlet_id} {start_dt}~{end_dt}: jendela {span} detik masih {len(orders)} order (batas API {ORDERS_PER_CALL_CAP})")
    mid = start + datetime.timedelta(seconds=span // 2)
    first_end = (mid - datetime.timedelta(seconds=1)).strftime(_DT_FMT) + ".999"
    return (_fetch_window(access_token, outlet_id, start_dt, first_end)
            + _fetch_window(access_token, outlet_id, mid.strftime(_DT_FMT), end_dt))


def fetch_all_orders(access_token, outlet_ids, start_dt, end_dt):
    """
    Tarik semua outlet (1 call per outlet, dibelah kalau terpotong). Return (orders, failed_calls):
      orders       = list order unik per order_id (gabungan semua outlet)
      failed_calls = [(outlet_id, pesan_error)] untuk outlet yang TETAP gagal setelah semua retry.
    Outlet gagal TIDAK menghentikan run (hasil yang berhasil tetap dipakai); caller yang memutuskan.
    """
    print(f"        Total API calls: {len(outlet_ids):,} (1 per outlet, jendela {start_dt} ~ {end_dt} UTC)")
    results, failed = {}, {}
    n_done = 0

    def _run(outlets, workers, track=False):
        nonlocal n_done
        still_failed = {}
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_fetch_window, access_token, o, start_dt, end_dt): o for o in outlets}
            for future in as_completed(futures):
                o = futures[future]
                try:
                    results[o] = future.result()
                except TokenExpiredError:
                    for f in futures:
                        f.cancel()
                    raise
                except Exception as e:
                    still_failed[o] = str(e)
                if track:
                    n_done += 1
                    if n_done % PROGRESS_EVERY_N_OUTLETS == 0 or n_done == len(outlets):
                        print(f"        [fetch] {n_done}/{len(outlets)} outlet selesai")
        return still_failed

    failed = _run(outlet_ids, FETCH_MAX_WORKERS, track=True)
    if failed:
        print(f"        Outlet gagal: {len(failed)} -> diulang dengan {REFETCH_FAILED_WORKERS} worker")
        failed = _run(sorted(failed), REFETCH_FAILED_WORKERS)

    orders = list({o['order_id']: o for batch in results.values() for o in batch}.values())
    return orders, sorted(failed.items())


def get_intraday_state(config_dw):
    """
    Kondisi tabel intraday saat ini (isinya cuma data 1 hari, jadi dibaca utuh):
      {order_id: {'flags': (status, payment_status, fulfillment_status), 'tx': {transaction_id: transactions_status}, 'details': n_baris_details}}
    """
    conn = get_connection(config_dw, config_dw.db_resource)
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT order_id, transaction_id, status, payment_status, fulfillment_status, transactions_status FROM {HEADERS_TABLE}")
        state = {}
        for order_id, transaction_id, status, payment_status, fulfillment_status, tx_status in cursor.fetchall():
            entry = state.setdefault(order_id, {'flags': (status, payment_status, fulfillment_status), 'tx': {}, 'details': 0})
            entry['tx'][transaction_id] = tx_status
        cursor.execute(f"SELECT order_id, COUNT(*) FROM {DETAILS_TABLE} GROUP BY order_id")
        for order_id, n in cursor.fetchall():
            if order_id in state:
                state[order_id]['details'] = n
        cursor.close()
    finally:
        conn.close()
    return state


def pick_orders_to_write(orders, state):
    """
    Saring order hasil API: hanya yang BARU atau BERUBAH dibanding tabel intraday yang perlu di-transform + ditulis.
    Berubah = status / payment_status / fulfillment_status order beda, ada transaksi baru (mis. refund) atau status transaksi beda,
    atau details sudah ada di API tapi belum di tabel (order fulfilled belakangan).
    Transaksi `sale` bernilai 0 yang tidak ada di tabel = transaksi hantu yang sengaja dibuang transform (split payment),
    bukan perubahan. Order tanpa transaksi (belum dibayar) dilewati: belum punya baris header.
    Return (to_write, n_unchanged, n_no_transaction).
    """
    to_write, n_unchanged, n_no_tx = [], 0, 0
    for order in orders:
        transactions = order.get('transactions') or []
        if not transactions:
            n_no_tx += 1
            continue
        old = state.get(order['order_id'])
        if old is None:
            to_write.append(order)
            continue
        flags = (order.get('status'), order.get('payment_status'), order.get('fulfillment_status'))
        changed = flags != old['flags']
        for t in transactions:
            tid = t['transaction_id']
            if tid in old['tx']:
                changed = changed or t.get('status') != old['tx'][tid]
            elif not (t.get('type') == 'sale' and float(t.get('amount') or 0) <= 0):
                changed = True
        changed = changed or (bool(order.get('order_details')) and old['details'] == 0)
        if changed:
            to_write.append(order)
        else:
            n_unchanged += 1
    return to_write, n_unchanged, n_no_tx


def get_intraday_metrics(config_dw):
    """Ringkasan isi tabel intraday setelah load (untuk email)."""
    conn = get_connection(config_dw, config_dw.db_resource)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT COUNT(DISTINCT outlet_code), COUNT(DISTINCT order_id), COALESCE(SUM(total_amount), 0), "
            f"COUNT(DISTINCT CASE WHEN fulfillment_status = 'none' THEN order_id END), MAX(load_data_at) FROM {HEADERS_TABLE}")
        outlets, orders, amount_headers, unfulfilled, last_load = cursor.fetchone()
        cursor.execute(f"SELECT COALESCE(SUM(total_amount), 0) FROM {DETAILS_TABLE}")
        amount_details = cursor.fetchone()[0]
        cursor.close()
    finally:
        conn.close()
    return {'table_outlets': int(outlets), 'table_orders': int(orders), 'amount_headers': float(amount_headers),
            'amount_details': float(amount_details), 'orders_unfulfilled': int(unfulfilled)}
