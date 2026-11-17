"""
Master Bundling iSeller Mitra -> jumlah slot per bundling.

Satu bundling (comboset) di master punya N slot (kolom `category` di `bundles`), setiap slot quantity 1.
Jumlah slot = jumlah row normal per order_detail combo di Data Lake, dipakai dedup_details.py
untuk menghitung dup_factor combo.

Token API:
  1. Airflow Variable `access_token_mitra`
  2. Environment variable `access_token_mitra`
  3. File `.env` (project root atau parent folder-nya)

Kalau token kosong / expired / API error -> TIDAK raise di sini. Return Series kosong + status
(ok=False, reason, message). Caller (run_all) WAJIB menghentikan pipeline + kirim notifikasi:
cleaning tidak boleh jalan tanpa master bundling dari API.

status['reason']:
  token_not_found  : access_token_mitra tidak ada di Variable / env / .env
  token_expired    : API balas 401 (atau body error yang menyebut token / unauthorized)
  token_invalid    : API balas 403
  server_error     : API balas 5xx (sementara, boleh retry)
  connection_error : timeout / koneksi gagal (sementara, boleh retry)
  http_error       : HTTP error lain
  invalid_response : response bukan format GetProducts yang dikenal
  truncated        : page tidak habis-habis, master bisa terpotong
  empty            : API tidak mengembalikan bundling sama sekali
  unexpected_error : error lain
"""

import json
import os

import pandas as pd
import requests

TOKEN_KEY = 'access_token_mitra'
GET_PRODUCTS_URL = 'https://janjijiwamitra.isellershop.com/api/v2/GetProducts'
# Batas pengaman loop (200 x 500 = 100.000 produk), bukan batas data. Paging berhenti di page kosong.
MAX_PAGES = 200
PAGE_SIZE = 500
# reason yang kemungkinan sementara -> task boleh di-retry Airflow; selain ini retry tidak membantu
RETRYABLE_REASONS = {'server_error', 'connection_error'}
TOKEN_REASONS = {'token_not_found', 'token_expired', 'token_invalid'}
_TOKEN_ERROR_HINTS = ('token', 'unauthorized', 'unauthorised', 'expired')

# judul notifikasi (email / Teams) per reason
FAILURE_TITLES = {
    'token_not_found': f"{TOKEN_KEY} TIDAK DITEMUKAN",
    'token_expired': f"{TOKEN_KEY} EXPIRED / TIDAK VALID",
    'token_invalid': f"{TOKEN_KEY} TIDAK PUNYA AKSES",
    'server_error': "API iSeller Mitra sedang error (5xx)",
    'connection_error': "Tidak bisa terhubung ke API iSeller Mitra",
    'http_error': "API iSeller Mitra error",
    'invalid_response': "Response API iSeller Mitra tidak dikenal",
    'truncated': "Master bundling dari API terpotong",
    'empty': "Master bundling dari API kosong",
    'unexpected_error': "Gagal load master bundling",
    'unknown_detail_type': "Ada type details yang tidak dikenal di Data Lake",
}


def failure_title(reason):
    return FAILURE_TITLES.get(reason, FAILURE_TITLES['unexpected_error'])


def failure_action(reason):
    """Instruksi tindak lanjut untuk notifikasi."""
    if reason in TOKEN_REASONS:
        return (f"Update {TOKEN_KEY} di Airflow Variable (Admin > Variables), lalu jalankan ulang DAG "
                "(clear task). Data Lake belum disentuh, jadi aman di-rerun.")
    if reason in RETRYABLE_REASONS:
        return ("Kemungkinan gangguan sementara: Airflow akan retry otomatis. Kalau tetap gagal, cek API iSeller "
                "Mitra lalu jalankan ulang DAG (clear task). Data Lake belum disentuh, jadi aman di-rerun.")
    if reason == 'unknown_detail_type':
        return ("iSeller memakai type details baru (bukan standard / variant / comboset). Cek apakah itu jenis paket, "
                "lalu tambahkan ke KNOWN_DETAIL_TYPES / aturan combo di dedup_details.py. "
                "Data Lake belum disentuh, jadi aman di-rerun.")
    return ("Cek response API GetProducts iSeller Mitra, lalu jalankan ulang DAG (clear task). "
            "Data Lake belum disentuh, jadi aman di-rerun.")


class MasterBundlingError(Exception):
    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason

_PIPELINES_DIR = os.path.dirname(os.path.abspath(__file__))
_MITRA_ROOT = os.path.dirname(_PIPELINES_DIR)
_ENV_FILE_CANDIDATES = [
    os.path.join(_MITRA_ROOT, '.env'),
    os.path.join(os.path.dirname(_MITRA_ROOT), '.env'),
    '/opt/airflow/.env',
]


def _read_env_file(key):
    """Cari `key=value` di file .env (parser sederhana, tanpa python-dotenv)."""
    for path in _ENV_FILE_CANDIDATES:
        if not os.path.isfile(path):
            continue
        with open(path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                if k.strip() == key:
                    return v.strip().strip('"').strip("'"), path
    return '', None


def get_access_token_mitra():
    """Return (token, source). token '' kalau tidak ketemu."""
    try:
        from airflow.models import Variable  # type: ignore
        token = (Variable.get(TOKEN_KEY, default_var='') or '').strip()
        if token:
            return token, 'Airflow Variable'
    except Exception:
        pass

    token = (os.environ.get(TOKEN_KEY) or '').strip()
    if token:
        return token, 'environment variable'

    token, path = _read_env_file(TOKEN_KEY)
    if token:
        return token, f'.env ({path})'
    return '', None


def _snippet(resp_text, limit=200):
    return ' '.join((resp_text or '').split())[:limit] or '(body kosong)'


def _raise_for_http_status(resp):
    code = resp.status_code
    if code < 400:
        return
    body = _snippet(resp.text)
    if code == 401:
        raise MasterBundlingError(
            'token_expired',
            f"API iSeller Mitra menolak token (HTTP 401 Unauthorized): {TOKEN_KEY} expired / tidak valid. Response: {body}")
    if code == 403:
        raise MasterBundlingError(
            'token_invalid',
            f"API iSeller Mitra menolak akses (HTTP 403 Forbidden): {TOKEN_KEY} tidak punya akses GetProducts. Response: {body}")
    if code >= 500:
        raise MasterBundlingError('server_error', f"API iSeller Mitra error (HTTP {code}). Response: {body}")
    raise MasterBundlingError('http_error', f"API iSeller Mitra balas HTTP {code}. Response: {body}")


def _page_products(resp):
    """
    List produk dari 1 page GetProducts. Body error (walau HTTP 200) -> MasterBundlingError.
    Format normal: {"products": [...], "has_next_item": bool, "status": true, "error_message": null, ...};
    page sesudah data habis -> "products": [].
    """
    try:
        result = json.loads(resp.text)
    except ValueError:
        raise MasterBundlingError('invalid_response', f"Response GetProducts bukan JSON: {_snippet(resp.text)}")
    if not isinstance(result, dict):
        raise MasterBundlingError('invalid_response', f"Response GetProducts bukan object JSON: {_snippet(resp.text)}")
    products = result.get('products')
    if result.get('status') is not False and not result.get('error_message') and isinstance(products, list):
        return products
    body = _snippet(resp.text)
    if any(hint in body.lower() for hint in _TOKEN_ERROR_HINTS):
        raise MasterBundlingError(
            'token_expired', f"API iSeller Mitra menolak token: {TOKEN_KEY} expired / tidak valid. Response: {body}")
    raise MasterBundlingError('invalid_response', f"Response GetProducts tidak berisi list produk: {body}")


def _fetch_bundlings(access_token):
    """GetProducts (semua page) -> DataFrame comboset dengan kolom bundling_id & bundles."""
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/x-www-form-urlencoded'}
    frames = []
    for page in range(1, MAX_PAGES + 1):
        data = {'modified_after': '2023-01-01', 'track_inventory': 'FALSE', 'page_size': PAGE_SIZE, 'page': page}
        try:
            resp = requests.post(GET_PRODUCTS_URL, headers=headers, data=data, timeout=60)
        except requests.exceptions.RequestException as e:
            raise MasterBundlingError(
                'connection_error', f"Tidak bisa terhubung ke API iSeller Mitra ({type(e).__name__}: {e})")
        _raise_for_http_status(resp)
        page_data = _page_products(resp)
        if not page_data:
            break
        frames.append(pd.DataFrame(page_data))
    else:
        # belum ketemu page kosong -> master bisa terpotong, jangan dipakai
        raise MasterBundlingError(
            'truncated', f"GetProducts belum habis setelah {MAX_PAGES} page, master bisa terpotong")
    if not frames:
        return pd.DataFrame(columns=['bundling_id', 'bundles'])
    df_products = pd.concat(frames, ignore_index=True)
    df_bundlings = df_products[df_products['type'] == 'comboset']
    return df_bundlings.rename(columns={'product_id': 'bundling_id'})[['bundling_id', 'bundles']]


def _slot_count_from_bundlings(df_bundlings):
    """bundling_id -> jumlah category unik di list `bundles`."""
    rows = []
    for bundling_id, bundles in zip(df_bundlings['bundling_id'], df_bundlings['bundles']):
        if not isinstance(bundles, list):
            continue
        categories = {b.get('category') for b in bundles if isinstance(b, dict)}
        if categories:
            rows.append((bundling_id, len(categories)))
    return (pd.DataFrame(rows, columns=['bundling_id', 'bundling_slot_count'])
              .drop_duplicates('bundling_id')
              .set_index('bundling_id')['bundling_slot_count'])


def _failed(empty, source, reason, message):
    status = {'ok': False, 'reason': reason, 'source': source, 'n_bundling': 0, 'message': f"GAGAL ({message})"}
    _print_status(status)
    return empty, status


def load_bundling_slot_count():
    """
    Return (bundling_slot_count: pd.Series, status: dict).
    status = {'ok': bool, 'reason': str|None, 'message': str, 'source': str|None, 'n_bundling': int}
    ok=False -> caller wajib STOP pipeline (lihat docstring modul untuk daftar reason).
    """
    empty = pd.Series(dtype='int64', name='bundling_slot_count')

    print()
    print("=" * 65)
    print("  STEP 2: LOAD MASTER BUNDLING (jumlah slot per bundling)")
    print("=" * 65)

    token, source = get_access_token_mitra()
    if not token:
        return _failed(empty, None, 'token_not_found',
                       f"{TOKEN_KEY} tidak ditemukan di Airflow Variable / environment variable / .env")

    try:
        slot_count = _slot_count_from_bundlings(_fetch_bundlings(token))
    except MasterBundlingError as e:
        return _failed(empty, source, e.reason, str(e))
    except Exception as e:
        return _failed(empty, source, 'unexpected_error', f"{type(e).__name__}: {e}")

    if slot_count.empty:
        return _failed(empty, source, 'empty', "API iSeller Mitra tidak mengembalikan bundling sama sekali")

    status = {'ok': True, 'reason': None, 'source': source, 'n_bundling': len(slot_count),
              'message': f"OK | {len(slot_count):,} bundling"}
    _print_status(status)
    return slot_count, status


def _print_status(status):
    print(f"  Source  : iSeller API GetProducts (token dari {status['source'] or '-'})")
    print(f"  Status  : {status['message']}")
    if not status['ok']:
        print(f"  Reason  : {status['reason']}")
        print("            -> pipeline DIHENTIKAN (cleaning tidak jalan tanpa master bundling)")
