"""
Pipeline: Backfill Fulfillment None - iSeller Mitra

Detects unfulfilled orders in transactions_iseller_mitra, re-pulls data from
iSeller Mitra API, inserts details to DL, and updates fulfillment_status to 'fulfilled'.

Mirror dari iseller_pusat_fulfillment/pipelines/pipeline_backfill_fulfillment.py. Beda dengan pusat:
  - tabel / domain API / token (access_token_mitra) milik Mitra
  - logika transform mengikuti notebook ingest transaction_janji_jiwa_mitra_v2 (2026-10-16), termasuk
    sumber diskon dari array discounts[] (Pusat tetap memakai discount_order_amount)
  - API dipanggil hanya untuk pasangan (outlet, hari) yang punya order unfulfilled, jendela 4 jam
    berakhir di xx:59:59.999 (sama dengan notebook ingest)
"""

import os
import datetime
import json
import time
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

import requests
import pandas as pd
import numpy as np
from pytz import timezone
from sqlalchemy import bindparam, text
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.db_helpers import get_connection, get_engine_datalake

class MissingAccessTokenError(Exception):
    """Raised when access_token_mitra is missing from Variable or env."""
    pass


class TokenExpiredError(Exception):
    """Raised when iSeller API returns 401 — token expired."""
    def __init__(self, message, df_unfulfilled=None):
        super().__init__(message)
        self.df_unfulfilled = df_unfulfilled

API_TIMEOUT_SEC = 60
API_MAX_RETRIES = 3
API_RETRY_BACKOFF_SEC = 2
REFETCH_EMPTY_MAX_WORKERS = 5
API_WINDOW_HOURS = 4
GET_PRODUCTS_PAGE_SIZE = 500
# Batas pengaman loop (200 x 500 produk), bukan batas data: paging berhenti di page kosong.
GET_PRODUCTS_MAX_PAGES = 200
DETAILS_TABLE = 'transactions_items_iseller_mitra'
DB_BATCH_SIZE = 1000
# Dedup key details iSeller (sama dengan pipelines/dedup_details.py di project cleaning mitra). Row sale/refund beda transactions_type.
DETAIL_KEY = ['order_id', 'order_detail_id', 'type', 'bundling_id', 'product_id', 'sku',
              'status', 'payment_status', 'transactions_type']
RECONCILE_TOLERANCE = 0.01

FINAL_DETAIL_COLUMNS = [
    'order_id', 'order_detail_id', 'type', 'bundling_id', 'product_id', 'sku',
    'product_name', 'product_generic_name', 'base_price', 'quantity', 'total_order_amount', 'promotion_name',
    'promotion_amount', 'discount_amount', 'subtotal', 'product_variant_id', 'product_variant_name',
    'product_modifier_id', 'product_modifier_name', 'notes', 'taxable', 'tax_percentage', 'tax_amount',
    'additional_charge', 'total_amount', 'product_type', 'discount_name', 'discount_percentage',
    'promotion_id', 'promotion_type', 'transaction_date', 'order_reference', 'outlet_id', 'outlet_code',
    'outlet_name', 'channel', 'channel_detail', 'order_date', 'closed_date', 'status', 'payment_status',
    'transactions_type', 'transactions_status', 'sales_type', 'transactions_payment_type_name', 'customer_id',
    'customer_first_name', 'customer_last_name', 'customer_phone_number', 'customer_email', 'cashier_id',
    'fulfillment_status', 'load_data_at',
]

# Label sales_type 'Sejuta Jiwa': order berisi minimal 1 produk di list ini.
# Sumber: notebook ingest transaction_janji_jiwa_mitra_v2 (list sama dengan Pusat).
# Di DL Mitra 1-14 Okt 2026 tidak ada produk dari list ini sama sekali, jadi list ini praktis tidak terpakai.
# Tarif pajak TIDAK lagi dari list produk, tapi dari tax_percentage API per detail (lihat _process_api_data).
PRODUCT_ID_SJW_LABEL = {
    '1d2664fc-f2c4-4d63-93b0-0150c5bad9ca',
    '514f7f40-fcf1-4b24-a881-0e32a6e2da88',
    'b7a5bd88-cda9-4623-814a-734767026910',
    '134f7a5b-09a2-455c-8f70-93a01468131e',
    'b26ce832-29a4-46f7-be9f-e9f4d72c1dca',
    '64b0cc8b-88e4-498a-b5b5-ff093b5915e5',
    '2306edf4-9f70-4c79-88d9-2a836a292816',  # added at 2024-03-13
    '9eef163d-fd2f-4e52-98e6-4593679a16b5',  # added at 2024-04-01 semua SJO
    '9ea8be5c-8bc1-4f14-b564-5eb75a19d907',
    'a96b7a4e-b86d-4171-956b-77a09d1c54f9',
    '6b07599f-14f8-44f9-ab5b-4389bf7e82d1',
    '328e5672-48a1-4683-b65d-c4a45f344ca1',  # added at 2024-04-02
    'baa1d6c2-7b48-40af-a6f3-b9e481f93e61',
    '3ab4aa05-f664-4eaf-ad7e-bdf643d421d4',
    '2fcb7dad-bd59-44c9-a846-1279f9840d37',
    'c038d0fa-a12a-4644-9bbb-3878b3064912',
    '4521dc51-b3d6-4493-814a-617fde292c04',  # added at 2024-05-14
    '62701d95-4d24-449e-b0db-ea8994d00b62',  # added at 2024-07-08
    '8f5fe78b-7d7c-46ed-9f5b-c9f8d195b60d',  # added at 2024-08-04
    '44d2e817-8745-4cbf-aa36-d4abfc75aebf',
    'a26111f7-3aa5-48b0-a77b-2f952aab5eae',
    '74ee1eae-21b8-467a-828c-b59cf780d065',  # added at 2024-08-16
    'a89893bf-9026-4a5c-9f0c-949c0f4539d7',
    '150c96b0-fe81-4835-b3cc-593bfb6c45db',  # added at 2024-09-14
    '999251a7-5e8b-4267-b67c-b33cedead904',  # added at 2024-10-03
    '0c8a73fe-ae1b-4287-a6af-7d48fce86de5',
    'c9d038f6-fea1-494f-813c-ad0aed0dde1e',  # added at 2024-10-23
    'a94730e0-4f5b-4af9-b70b-68dbea299b78',
    '5842673c-8a8b-423b-9804-1c0d484c60ac',  # added at 2024-11-18
    'bae1b783-e862-4c5f-bb46-b20bb7e02b63',
    '00295e2b-66bb-489f-9615-6e25676c8331',
    'd8a7f659-c1dd-448a-9110-688fd1105bf8',
    '47855957-b056-49bc-8be3-c8ea68e0160b',
    '5864c6dc-216e-4764-9c8b-71bc9d77c41a',  # added at 2025-02-26
    '3ffe31ac-f4c6-4206-959f-26f2ec6224fc',
    '64b5deb0-fcef-412f-bd2e-714a9b65a6b9',  # added at 2025-06-23
    '7ddc0da7-72bf-4518-ac37-c1c62e48d418',  # added at 2026-08-20 (Basreng Pedas Daun Jeruk, ITM010872)
    '7fed141a-8b22-4cee-80c0-df4242e136ec',  # added at 2026-10-09 (Es Kopi Susu Honey, FCH040025)
    '5faac2db-7827-4414-a5ac-1345a79299dd',  # added at 2026-10-09 (Matcha - KSJ, FCH040036)
    '06aa6648-b40a-4d02-b9b7-3105fa83c762',  # added at 2026-10-09 (Matcha - SJO, ITM022191)
}


def _expand_nested_lists(df, column_names):
    expanded_data = []
    for _, row in df.iterrows():
        for column_name in column_names:
            data_list = row[column_name]
            if isinstance(data_list, list):
                for item in data_list:
                    if item is not None:
                        new_row = row.copy()
                        for key, value in item.items():
                            new_row[f"{column_name}_{key}"] = value
                        expanded_data.append(new_row)
            else:
                expanded_data.append(row)
    return pd.DataFrame(expanded_data)


def _frame(rows, columns):
    """DataFrame dari list dict, kolom `columns` dijamin ada walau rows kosong / key tidak ada."""
    df = pd.DataFrame(rows)
    for col in columns:
        if col not in df.columns:
            df[col] = np.nan
    return df


def _assert_no_duplicate_details(df, combo_count):
    """
    Pengaman sebelum details ditulis ke DL. Duplikat TIDAK dibuang diam-diam (itu menyembunyikan bug),
    tapi pipeline dihentikan.
      - non-combo : unik per DETAIL_KEY (sama dengan dedup key iseller_mitra_dwh)
      - combo     : row per (order, order_detail, transaksi) = jumlah entri product_combos di raw JSON
    """
    if df.empty:
        return
    problems = []

    non_combo = df[df['type'] != 'comboset']
    dup = non_combo[non_combo.duplicated(DETAIL_KEY, keep=False)]
    if len(dup):
        problems.append(f"{dup['order_detail_id'].nunique()} order_detail non-combo dobel, "
                        f"contoh: {dup['order_detail_id'].drop_duplicates().head(3).tolist()}")

    combo = df[df['type'] == 'comboset']
    if len(combo):
        rows = combo.groupby(['order_id', 'order_detail_id', 'transactions_type'], dropna=False).size()
        expected = [max(combo_count.get(odid, 0), 1) for odid in rows.index.get_level_values('order_detail_id')]
        bad = rows[rows.values != np.array(expected)]
        if len(bad):
            problems.append(f"{len(bad)} combo jumlah row-nya != jumlah product_combos, "
                            f"contoh: {bad.index.get_level_values('order_detail_id')[:3].tolist()}")

    if problems:
        raise ValueError("Details backfill dobel, Data Lake tidak ditulis: " + "; ".join(problems))


def _log_reconciliation(df_headers, df_details):
    """Info saja (tidak menghentikan): order yang total details-nya meleset > RECONCILE_TOLERANCE dari header API."""
    if df_details.empty:
        return
    sale_h = df_headers[df_headers['transactions_type'] == 'sale'].drop_duplicates(subset=['order_id', 'transaction_id'])
    sale_d = df_details[df_details['transactions_type'] == 'sale']
    cmp = (sale_h.groupby('order_id').agg(order_reference=('order_reference', 'first'),
                                          header_total=('total_amount', lambda s: pd.to_numeric(s, errors='coerce').sum()))
           .join(sale_d.groupby('order_id')['total_amount'].sum().rename('details_total'), how='inner'))
    cmp['ratio'] = cmp['details_total'] / cmp['header_total'].replace(0, np.nan)
    off = cmp[(cmp['ratio'] - 1).abs() > RECONCILE_TOLERANCE]
    total_ratio = cmp['details_total'].sum() / cmp['header_total'].sum() if cmp['header_total'].sum() else float('nan')
    print(f"        Rekonsiliasi details vs header API: {len(cmp)} order, total {total_ratio:.4f}x, "
          f"{len(off)} order meleset > {RECONCILE_TOLERANCE:.0%}")
    for oid, r in off.head(5).iterrows():
        print(f"          {r['order_reference']}: header {r['header_total']:,.0f} | details {r['details_total']:,.0f} ({r['ratio']:.3f}x)")


def _group_per_detail(df, amount_col, join_cols):
    """Gabung banyak promo/discount per order_detail_id: amount di-SUM, kolom teks di-join (nilai unik, urutan asli)."""
    df = df.copy()
    df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)

    def _join(sep):
        return lambda s: sep.join(dict.fromkeys(str(v) for v in s if pd.notna(v) and str(v) != '')) or np.nan

    agg = {amount_col: 'sum', **{col: _join(sep) for col, sep in join_cols.items()}}
    return df.groupby('order_detail_id', sort=False).agg(agg).reset_index()


def _adjust_timezone(dt_str, offset_hours):
    dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    adjusted = dt + datetime.timedelta(hours=offset_hours)
    return adjusted.strftime("%Y-%m-%d %H:%M:%S")


def _get_fulfillment_none(config_dw, start_date, end_date):
    """
    Unfulfilled headers in calendar window start_date … end_date (YYYY-MM-DD).
    """
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()

    cursor.execute(f"""
        SELECT
            date_format(t.transaction_date, '%Y-%m-%d') AS date,
            t.outlet_id, t.outlet_code,
            CASE WHEN om.customer_name IS NOT NULL THEN om.customer_name ELSE '-' END AS outlet_name,
            t.fulfillment_status, t.status,
            t.payment_status, t.transactions_status, t.order_id, t.order_reference,
            SUM(t.total_amount) AS total_amount
        FROM transactions_iseller_mitra t
        LEFT JOIN (SELECT DISTINCT jilid, customer_name FROM outlet_mapping) om
            ON t.outlet_code = om.jilid
        WHERE
            date_format(t.transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
            AND t.fulfillment_status = 'none'
            AND t.payment_status = 'paid'
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    """)
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    cursor.close()
    conn.close()
    return df


def _get_master_products(access_token):
    url = 'https://janjijiwamitra.isellershop.com/api/v2/GetProducts'
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/x-www-form-urlencoded'}

    frames = []
    for page in range(1, GET_PRODUCTS_MAX_PAGES + 1):
        data = {'modified_after': '2023-01-01', 'track_inventory': 'FALSE',
                'page_size': GET_PRODUCTS_PAGE_SIZE, 'page': page}
        resp = requests.post(url, headers=headers, data=data, timeout=API_TIMEOUT_SEC)
        resp.raise_for_status()
        result = json.loads(resp.text)
        products = result.get('products') if isinstance(result, dict) else None
        if not isinstance(products, list):
            raise ValueError(f"GetProducts page {page}: response tidak berisi list 'products': {resp.text[:200]}")
        if not products:
            break
        frames.append(pd.DataFrame(products))
    else:
        raise ValueError(f"GetProducts belum habis setelah {GET_PRODUCTS_MAX_PAGES} page, master produk bisa terpotong")
    if not frames:
        raise ValueError("GetProducts tidak mengembalikan produk sama sekali")
    df_product_data = pd.concat(frames, ignore_index=True)
    print(f"        GetProducts: {page - 1} page, {len(df_product_data):,} produk")

    df_bundlings = df_product_data[df_product_data['type'] == 'comboset']
    df_products = df_product_data[df_product_data['type'] != 'comboset']
    df_products = df_products[['product_id', 'product_type', 'sku', 'name', 'type', 'taxable', 'price', 'sold_count', 'modified_date']]
    df_products = df_products[df_products['sku'] != '']
    df_products = df_products[df_products['price'] != 0]

    df_bundlings = df_bundlings[['product_id', 'name', 'sku', 'taxable', 'sold_count', 'bundles', 'modified_date']]
    df_bundlings.columns = ['bundling_id', 'bundling_name', 'bundling_code', 'taxable', 'sold_count', 'bundles', 'modified_date']
    df_bundling_exp = _expand_nested_lists(df_bundlings, ['bundles'])
    x_columns = [col for col in df_bundling_exp.columns if col.startswith('bundles_')]
    for col in x_columns:
        df_bundling_exp.rename(columns={col: col.replace('bundles_', '')}, inplace=True)
    df_bundling_exp.drop('bundles', axis=1, inplace=True)
    df_bundling_final = pd.merge(df_bundling_exp, df_products[['product_id', 'price']], on='product_id', how='left')
    df_bundling_final['bundling_code'] = df_bundling_final.apply(
        lambda row: row['bundling_code'][:14] if pd.notnull(row['bundling_code']) else None, axis=1
    )

    print(f"        Master products: {len(df_products)}, bundlings: {df_bundling_final['bundling_id'].nunique()}")
    return df_products, df_bundling_final


def _fetch_single_order(access_token, outlet_id, start_dt, end_dt):
    """
    1 call GetOrders untuk (outlet_id, jendela waktu). Return list order.
    HTTP 200 dengan body error (status false / error_message, mis. rate limit) di-retry, bukan dianggap 0 order.
    HTTP 401/403 tidak di-retry (dilempar apa adanya -> TokenExpiredError di caller).
    """
    url = 'https://janjijiwamitra.isellershop.com/api/v2/GetOrders'
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'created_after': start_dt,
        'created_before': end_dt,
        'timezone': '7',
        'includes': 'orderdetails, promotiondetails, transactions, discountdetails',
        'outlet_id': outlet_id
    }
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            resp = requests.post(url, headers=headers, data=data, timeout=API_TIMEOUT_SEC)
            resp.raise_for_status()
            result = json.loads(resp.text)
            if not isinstance(result, dict):
                raise ValueError(f"respons GetOrders bukan object JSON: {resp.text[:200]}")
            if result.get('status') is False or result.get('error_message'):
                raise ValueError(f"GetOrders error (status={result.get('status')}): {result.get('error_message')}")
            orders = list(result.values())[1]   # sama dengan df['value'][1] versi lama
            if not isinstance(orders, list):
                raise ValueError(f"field order GetOrders bukan list ({type(orders).__name__}): {resp.text[:200]}")
            return orders
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            if status in (401, 403) or attempt == API_MAX_RETRIES:
                raise
            print(f"        Retry {attempt}/{API_MAX_RETRIES} outlet={outlet_id} {start_dt}~{end_dt}: HTTP {status}")
        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt == API_MAX_RETRIES:
                raise
            print(f"        Retry {attempt}/{API_MAX_RETRIES} outlet={outlet_id} {start_dt}~{end_dt}: {e}")
        time.sleep(API_RETRY_BACKOFF_SEC * attempt)


def _build_api_tasks(df_transaction):
    """
    1 task per (outlet_id, jendela waktu), hanya untuk pasangan (outlet, hari) yang punya order unfulfilled.
    Jendela 4 jam sama dengan notebook ingest Mitra. Akhir jendela xx:59:59.999 (bukan xx:59:59): transaksi
    yang closed di xx:59:59,xxx jatuh di celah 1 detik antar jendela dan tidak ketarik (terbukti Mitra 11-13 Okt 2026).
    """
    tasks = []
    for _, row in df_transaction[['outlet_id', 'date']].drop_duplicates().iterrows():
        for start_hour in range(0, 24, API_WINDOW_HOURS):
            end_hour = start_hour + API_WINDOW_HOURS - 1
            start_dt = _adjust_timezone(f"{row['date']} {start_hour:02}:00:00", -7)
            end_dt = _adjust_timezone(f"{row['date']} {end_hour:02}:59:59", -7) + ".999"
            tasks.append((row['outlet_id'], start_dt, end_dt))
    return tasks


def _get_orders_from_api(access_token, tasks, max_workers=10):
    """Fetch orders for all (outlet_id, time_window) tasks in parallel using ThreadPoolExecutor."""
    print(f"        Total API calls: {len(tasks)} (parallelized, max_workers={max_workers})")

    get_data = [None] * len(tasks)

    def _run(indices, workers):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_idx = {executor.submit(_fetch_single_order, access_token, *tasks[i]): i for i in indices}
            for future in as_completed(future_to_idx):
                i = future_to_idx[future]
                get_data[i] = future.result()

    _run(range(len(tasks)), max_workers)

    # Panggilan 0 order diulang sekali dengan worker lebih sedikit: kalau ternyata ada order,
    # percobaan pertama gagal diam-diam (mis. rate limit) dan datanya dipakai.
    empty_idx = [i for i, orders in enumerate(get_data) if not orders]
    if empty_idx:
        _run(empty_idx, REFETCH_EMPTY_MAX_WORKERS)
        recovered = [i for i in empty_idx if get_data[i]]
        print(f"        Panggilan 0 order: {len(empty_idx)} -> re-fetch memulihkan {len(recovered)} panggilan "
              f"({sum(len(get_data[i]) for i in recovered)} order)")

    return get_data


def _process_api_data(data, order_id_list, df_products):
    """Process raw API data into cleaned headers and details DataFrames."""
    headers_list_cols = [
        'outlet_id', 'order_reference', 'outlet_code', 'outlet_name', 'order_date', 'closed_date',
        'status', 'channel', 'channel_detail', 'payment_status', 'fulfillment_status', 'notes',
        'customer_id', 'customer_first_name', 'customer_last_name', 'customer_phone_number',
        'customer_email', 'cashier_id', 'total_order_amount', 'total_discount_amount',
        'total_promotion_amount', 'subtotal', 'total_tax_amount', 'total_amount'
    ]

    # Setiap item `data` = list order dari 1 call GetOrders. Combo/promo/discount diberi order_detail_id
    # induknya secara eksplisit. 1 entri product_combos = 1 row, termasuk entri identik (komponen sah).
    header_frames, detail_rows, combo_rows, promo_rows, discount_rows = [], [], [], [], []
    combo_count = {}
    for orders in data:
        if not orders:
            continue
        header_frames.append(pd.json_normalize(orders, record_path=['transactions'], meta=headers_list_cols, meta_prefix='transactions_'))
        for order in orders:
            for detail in order.get('order_details') or []:
                detail_rows.append(detail)
                odid = detail.get('order_detail_id')
                combos = detail.get('product_combos') or []
                combo_count[odid] = len(combos)
                combo_rows += [{**c, 'order_detail_id': odid} for c in combos]
                promo_rows += [{**p, 'order_detail_id': odid} for p in (detail.get('promotions') or [])]
                discount_rows += [{**d, 'order_detail_id': odid} for d in (detail.get('discounts') or [])]

    df_headers = pd.concat(header_frames, ignore_index=True) if header_frames else pd.DataFrame()
    if len(df_headers) == 0:
        return (pd.DataFrame(columns=['order_id', 'fulfillment_status']),
                pd.DataFrame(columns=['order_id', 'sales_type']),
                pd.DataFrame(columns=FINAL_DETAIL_COLUMNS))

    df_details = pd.json_normalize(detail_rows) if detail_rows else pd.DataFrame(columns=['order_id', 'product_id'])
    df_product_combos = _frame(combo_rows, ['order_detail_id', 'product_id', 'product_sku', 'product_name', 'variant_name', 'quantity'])
    # 1 baris per order_detail_id (amount di-SUM) supaya merge ke details tidak fan-out.
    df_promo = _group_per_detail(
        _frame(promo_rows, ['order_detail_id', 'promotion_id', 'promotion_name', 'promotion_type', 'promotion_amount']),
        amount_col='promotion_amount', join_cols={'promotion_id': ', ', 'promotion_name': '-', 'promotion_type': ', '},
    )
    df_discount = _group_per_detail(
        _frame(discount_rows, ['order_detail_id', 'discount_name', 'discount_amount']),
        amount_col='discount_amount', join_cols={'discount_name': ', '},
    )

    # --- Process headers ---
    df_headers = df_headers[['order_id', 'transaction_id', 'transaction_date', 'transactions_order_reference',
        'transactions_outlet_id', 'transactions_outlet_code', 'transactions_outlet_name',
        'transactions_channel', 'transactions_channel_detail', 'transactions_order_date',
        'transactions_closed_date', 'transactions_status', 'transactions_payment_status',
        'transactions_fulfillment_status', 'type', 'status', 'gateway', 'payment_type_name',
        'transactions_notes', 'transactions_customer_id', 'transactions_customer_first_name',
        'transactions_customer_last_name', 'transactions_customer_phone_number',
        'transactions_customer_email', 'transactions_cashier_id', 'transactions_total_order_amount',
        'transactions_total_discount_amount', 'transactions_total_promotion_amount', 'transactions_subtotal',
        'transactions_total_tax_amount', 'transactions_total_amount', 'mdr']]

    df_headers.columns = [
        'order_id', 'transaction_id', 'transaction_date', 'order_reference', 'outlet_id', 'outlet_code',
        'outlet_name', 'channel', 'channel_detail', 'order_date', 'closed_date', 'status', 'payment_status',
        'fulfillment_status', 'transactions_type', 'transactions_status', 'transactions_gateway',
        'transactions_payment_type_name', 'notes', 'customer_id', 'customer_first_name', 'customer_last_name',
        'customer_phone_number', 'customer_email', 'cashier_id', 'total_order_amount', 'total_discount_amount',
        'total_promotion_amount', 'subtotal', 'total_tax_amount', 'total_amount', 'mdr'
    ]

    df_headers['order_date'] = pd.to_datetime(df_headers['order_date'])
    df_headers['closed_date'] = pd.to_datetime(df_headers['closed_date'])
    df_headers['outlet_code'] = df_headers['outlet_code'].str.zfill(5)
    gmt_plus_7 = timezone('Etc/GMT-7')
    df_headers['transaction_date'] = pd.to_datetime(df_headers['transaction_date'])
    df_headers['transaction_date'] = df_headers['transaction_date'].dt.tz_localize('UTC').dt.tz_convert(gmt_plus_7)

    for col in ['customer_first_name', 'customer_last_name', 'notes']:
        df_headers[col] = df_headers[col].astype(str).apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))

    # Sales type determination. is_sjw harus 1 nilai per order: order campuran (produk SJW + non-SJW)
    # kalau tidak di-any() jadi 2 row (True & False) dan menggandakan header + semua details-nya.
    df_headers_sjw = (df_details['product_id'].isin(PRODUCT_ID_SJW_LABEL)
                      .groupby(df_details['order_id']).any()
                      .rename('is_sjw').reset_index())
    df_merged = pd.merge(df_headers, df_headers_sjw, on='order_id', how='left')

    conditions = [
        (df_merged['transactions_payment_type_name'] == 'Go Food'),
        (df_merged['transactions_payment_type_name'] == 'Grab Food'),
        (df_merged['transactions_payment_type_name'] == 'Shopee Food'),
        (df_merged['transactions_payment_type_name'] == 'Jiwa+'),
        (df_merged['transactions_payment_type_name'] == 'Grab Dine Out'),
        (df_merged['transactions_payment_type_name'] == 'Tokopedia Go'),
        (df_merged['transactions_payment_type_name'] == 'QPON'),
    ]
    values = ['Go Food', 'Grab Food', 'Shopee Food', 'Jiwa+', 'Grab Dine Out', 'Tokopedia Go', 'Qpon']
    df_merged['sales_type'] = np.select(conditions, values, default='Walk In')
    df_merged.loc[df_merged['is_sjw'] == True, 'sales_type'] = 'Sejuta Jiwa'

    df_final_headers = df_merged[[
        'order_id', 'transaction_id', 'transaction_date', 'order_reference', 'outlet_id', 'outlet_code',
        'outlet_name', 'channel', 'channel_detail', 'order_date', 'closed_date', 'status', 'payment_status',
        'fulfillment_status', 'transactions_type', 'transactions_status', 'transactions_gateway', 'sales_type',
        'transactions_payment_type_name', 'notes', 'customer_id', 'customer_first_name', 'customer_last_name',
        'customer_phone_number', 'customer_email', 'cashier_id', 'total_order_amount', 'total_discount_amount',
        'total_promotion_amount', 'subtotal', 'total_tax_amount', 'total_amount', 'mdr'
    ]]
    df_final_headers['load_data_at'] = datetime.datetime.now()

    order_sales_type = (df_final_headers.loc[df_final_headers['order_id'].isin(order_id_list), ['order_id', 'sales_type']]
                        .drop_duplicates(subset=['order_id']))

    if df_details.empty:
        return df_final_headers, order_sales_type, pd.DataFrame(columns=FINAL_DETAIL_COLUMNS)

    # --- Process details ---
    df_merge_details = df_details.merge(df_promo, on='order_detail_id', how='left')
    df_merge_details = df_merge_details.merge(df_discount, on='order_detail_id', how='left')

    df_merge_details = df_merge_details[['order_id', 'order_detail_id', 'product_id', 'product_name',
        'product_generic_name', 'product_type', 'product_variant_id', 'product_variant_name',
        'product_modifier_id', 'product_modifier_name', 'notes', 'sku', 'fulfillment_status',
        'quantity', 'base_price', 'total_order_amount', 'subtotal', 'additional_charge',
        'discount_percentage', 'discount_order_amount', 'taxable', 'tax_percentage', 'tax_amount',
        'type', 'promotion_id', 'promotion_name', 'promotion_type', 'promotion_amount',
        'discount_name', 'discount_amount_y', 'modifier_product_sku']]

    right_cols = ['order_id', 'transaction_date', 'order_reference', 'outlet_id', 'outlet_code', 'outlet_name',
        'channel', 'channel_detail', 'order_date', 'closed_date', 'status', 'payment_status',
        'fulfillment_status', 'transactions_type', 'transactions_status', 'sales_type',
        'transactions_payment_type_name', 'notes', 'customer_id', 'customer_first_name',
        'customer_last_name', 'customer_phone_number', 'customer_email', 'cashier_id']

    df_details_2 = df_merge_details.merge(df_final_headers[right_cols], on='order_id', how='left')
    df_details_combo = df_details_2.merge(df_product_combos, on='order_detail_id', how='left')
    df_details_combo['bundling_id'] = ''

    for index, row in df_details_combo.iterrows():
        if row['type'] == 'comboset':
            df_details_combo.at[index, 'bundling_id'] = df_details_combo.at[index, 'product_id_x']
            df_details_combo.at[index, 'product_id_x'] = df_details_combo.at[index, 'product_id_y']
            df_details_combo.at[index, 'product_name_x'] = df_details_combo.at[index, 'product_name_y']
            df_details_combo.at[index, 'quantity_x'] = df_details_combo.at[index, 'quantity_y']
            df_details_combo.at[index, 'product_variant_name'] = df_details_combo.at[index, 'variant_name']
            df_details_combo.at[index, 'sku'] = df_details_combo.at[index, 'product_sku']

    df_filtered = df_details_combo[~((df_details_combo['product_modifier_id'] != '') & (df_details_combo['base_price'] == 0))]

    df_filtered = df_filtered[['order_id', 'transaction_date', 'order_reference', 'outlet_id', 'outlet_code',
        'outlet_name', 'channel', 'channel_detail', 'order_date', 'closed_date', 'status', 'payment_status',
        'transactions_type', 'transactions_status', 'sales_type', 'transactions_payment_type_name',
        'customer_id', 'customer_first_name', 'customer_last_name', 'customer_phone_number', 'customer_email',
        'cashier_id', 'order_detail_id', 'product_id_x', 'bundling_id', 'product_name_x', 'product_generic_name',
        'type', 'product_type', 'product_variant_id', 'product_variant_name', 'product_modifier_id',
        'product_modifier_name', 'notes_x', 'sku', 'fulfillment_status_x', 'quantity_x', 'base_price',
        'total_order_amount', 'discount_name', 'discount_percentage', 'discount_order_amount', 'discount_amount_y',
        'promotion_id', 'promotion_name', 'promotion_type', 'promotion_amount', 'subtotal', 'taxable',
        'tax_percentage', 'tax_amount', 'additional_charge', 'modifier_product_sku']]

    df_filtered.columns = ['order_id', 'transaction_date', 'order_reference', 'outlet_id', 'outlet_code',
        'outlet_name', 'channel', 'channel_detail', 'order_date', 'closed_date', 'status', 'payment_status',
        'transactions_type', 'transactions_status', 'sales_type', 'transactions_payment_type_name',
        'customer_id', 'customer_first_name', 'customer_last_name', 'customer_phone_number', 'customer_email',
        'cashier_id', 'order_detail_id', 'product_id', 'bundling_id', 'product_name', 'product_generic_name',
        'type', 'product_type', 'product_variant_id', 'product_variant_name', 'product_modifier_id',
        'product_modifier_name', 'notes', 'sku', 'fulfillment_status', 'quantity', 'base_price',
        'total_order_amount', 'discount_name', 'discount_percentage', 'discount_order_amount', 'discount_amount',
        'promotion_id', 'promotion_name', 'promotion_type', 'promotion_amount', 'subtotal', 'taxable',
        'tax_percentage', 'tax_amount', 'additional_charge', 'modifier_product_sku']

    df_filtered = pd.merge(df_filtered, df_products[['product_id', 'price']], on='product_id', how='left')

    # Tax/amount calculations
    numeric_columns = df_filtered.select_dtypes(include='number').columns
    string_columns = df_filtered.select_dtypes(include='object').columns
    df_filtered[numeric_columns] = df_filtered[numeric_columns].fillna(0)
    df_filtered[string_columns] = df_filtered[string_columns].fillna('')

    # Tarif pajak dari API: `tax_percentage` per order_detail (untuk combo = tarif level paket).
    df_filtered['tax_rate'] = pd.to_numeric(df_filtered['tax_percentage'], errors='coerce').fillna(0) / 100
    df_filtered['divisor'] = 1 + df_filtered['tax_rate']
    print(f"        Tarif pajak dari API (tax_percentage -> jumlah row): {df_filtered['tax_percentage'].value_counts().to_dict()}")

    # Combo: nilai paket dari API (total_order_amount level detail, sebelum pajak) dibagi ke komponen
    # proporsional harga master x qty. Harga master hanya bobot -> total komponen = harga paket = header.
    # Kalau ada komponen tanpa harga master, paket dibagi rata.
    is_combo = df_filtered['type'] == 'comboset'
    df_filtered['combo_share'] = 1.0
    if is_combo.any():
        combo = df_filtered[is_combo]
        key = [combo['order_detail_id'], combo['transactions_type']]
        weight = (combo['price'] * combo['quantity']).clip(lower=0)
        all_priced = (weight > 0).groupby(key).transform('all')
        share = np.where(all_priced,
                         weight / weight.groupby(key).transform('sum'),
                         1 / weight.groupby(key).transform('size'))
        df_filtered.loc[is_combo, 'combo_share'] = share
        df_filtered.loc[is_combo, 'total_order_amount'] = combo['total_order_amount'] * share

    def calculate_new_amount(group, amount_column):
        # 1 group = 1 order_detail dalam 1 transaksi -> tarif pajaknya sama (dari API)
        divisor = group['divisor'].iloc[0]
        if (group['type'] == 'comboset').all():
            # promo/diskon paket dibagi dengan bobot yang sama, supaya komponen murah tidak jadi minus
            return group[amount_column] * group['combo_share'] / divisor
        return group[amount_column] / len(group) / divisor

    # Group per (order_detail_id, transactions_type): row sale & refund dihitung terpisah.
    # Kalau per order_detail_id saja, order ber-refund cuma kena 1/2 promo/diskon di tiap row.
    detail_key = ['order_detail_id', 'transactions_type']

    # Diskon diambil dari total array `discounts[]` API (kolom discount_amount hasil _group_per_detail), bukan
    # discount_order_amount: diskon per item "Custom ..." (diskon manual kasir) tidak masuk ke discount_order_amount,
    # termasuk saat 1 detail punya "Order Custom" + "Custom" sekaligus. Khusus Mitra; Pusat tetap discount_order_amount
    # karena di Pusat Jiwapoint tercatat 2x di discounts[].
    df_filtered['discount_base'] = pd.to_numeric(df_filtered['discount_amount'], errors='coerce').fillna(0)

    df_filtered['discount_amount_new'] = df_filtered.groupby(detail_key)['discount_base'].transform(
        lambda x: calculate_new_amount(df_filtered.loc[x.index], 'discount_base')
    )
    df_filtered['promotion_amount_new'] = df_filtered.groupby(detail_key)['promotion_amount'].transform(
        lambda x: calculate_new_amount(df_filtered.loc[x.index], 'promotion_amount')
    )
    df_filtered['subtotal'] = df_filtered['total_order_amount'] - df_filtered['discount_amount_new'] - df_filtered['promotion_amount_new']
    df_filtered['tax_amount'] = df_filtered['subtotal'] * df_filtered['tax_rate']
    df_filtered['total_amount'] = df_filtered['subtotal'] + df_filtered['tax_amount']
    df_filtered.drop(columns=['divisor', 'tax_rate', 'discount_base'], inplace=True)

    df_final_details = df_filtered[['order_id', 'order_detail_id', 'type', 'bundling_id', 'product_id', 'sku',
        'product_name', 'product_generic_name', 'base_price', 'quantity', 'total_order_amount', 'promotion_name',
        'promotion_amount_new', 'discount_amount_new', 'subtotal', 'product_variant_id', 'product_variant_name',
        'product_modifier_id', 'product_modifier_name', 'notes', 'taxable', 'tax_percentage', 'tax_amount',
        'additional_charge', 'total_amount', 'product_type', 'discount_name', 'discount_percentage',
        'promotion_id', 'promotion_type', 'transaction_date', 'order_reference', 'outlet_id', 'outlet_code',
        'outlet_name', 'channel', 'channel_detail', 'order_date', 'closed_date', 'status', 'payment_status',
        'transactions_type', 'transactions_status', 'sales_type', 'transactions_payment_type_name', 'customer_id',
        'customer_first_name', 'customer_last_name', 'customer_phone_number', 'customer_email', 'cashier_id',
        'fulfillment_status', 'modifier_product_sku']]

    df_final_details.columns = ['order_id', 'order_detail_id', 'type', 'bundling_id', 'product_id', 'sku',
        'product_name', 'product_generic_name', 'base_price', 'quantity', 'total_order_amount', 'promotion_name',
        'promotion_amount', 'discount_amount', 'subtotal', 'product_variant_id', 'product_variant_name',
        'product_modifier_id', 'product_modifier_name', 'notes', 'taxable', 'tax_percentage', 'tax_amount',
        'additional_charge', 'total_amount', 'product_type', 'discount_name', 'discount_percentage',
        'promotion_id', 'promotion_type', 'transaction_date', 'order_reference', 'outlet_id', 'outlet_code',
        'outlet_name', 'channel', 'channel_detail', 'order_date', 'closed_date', 'status', 'payment_status',
        'transactions_type', 'transactions_status', 'sales_type', 'transactions_payment_type_name', 'customer_id',
        'customer_first_name', 'customer_last_name', 'customer_phone_number', 'customer_email', 'cashier_id',
        'fulfillment_status', 'modifier_product_sku']

    df_final_details['load_data_at'] = datetime.datetime.now()
    df_final_details['notes'] = df_final_details['notes'].astype(str).apply(lambda x: x.encode('ascii', 'ignore').decode('ascii'))
    df_final_details.loc[df_final_details['modifier_product_sku'] != '', 'sku'] = df_final_details['modifier_product_sku']
    df_final_details.drop(columns=['modifier_product_sku'], inplace=True)

    # Sengaja TANPA drop_duplicates: row kembar di sini adalah komponen combo identik (sah) atau
    # baris sale/refund. Lihat KNOWLEDGE_comboset_komponen_identik_vs_duplicate.md di project cleaning mitra.
    details_cleaned = df_final_details[df_final_details['order_id'].isin(order_id_list)]
    _assert_no_duplicate_details(details_cleaned, combo_count)

    return df_final_headers, order_sales_type, details_cleaned[FINAL_DETAIL_COLUMNS]


def _send_alert_email(df_unfulfilled):
    if len(df_unfulfilled) == 0:
        return

    port = 587
    smtp_server = "smtp.gmail.com"
    sender_email = "saputra.christabel20@gmail.com"
    receiver_email = ["athens.jiwagroup@gmail.com", "lahia.ardhanlahia@gmail.com"]
    password = os.getenv("GMAIL_SMTP_PASSWORD")

    dates = ", ".join(sorted(set(df_unfulfilled['transaction_date'].astype(str))))
    total = df_unfulfilled['total_amount'].sum()

    subject = "[iSeller MITRA] - ALERT - Found Order Still Unfulfilled"
    body = f"""Hi Team,

Masih terdapat {len(df_unfulfilled)} order yang belum fulfilled.

   Date         : {dates}
   Total Amount : {total:,.0f}

Mohon segera lakukan pengecekan di sistem iSeller.

Best Regards,
DatO (Data autOmation)
"""
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_email)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    context = ssl.create_default_context()
    with smtplib.SMTP(smtp_server, port) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, msg.as_string())
    print("        Alert email sent.")


def _batches(items, size=DB_BATCH_SIZE):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _orders_with_existing_details(conn, order_ids):
    """order_id yang sudah punya baris di tabel details DL (jangan di-append lagi)."""
    stmt = text(f"SELECT DISTINCT order_id FROM {DETAILS_TABLE} WHERE order_id IN :ids").bindparams(
        bindparam('ids', expanding=True))
    existing = set()
    for batch in _batches(order_ids):
        existing.update(row[0] for row in conn.execute(stmt, {'ids': batch}))
    return existing


def _update_fulfilled_orders(conn, order_ids, sales_type_by_order):
    """Set fulfilled + sales_type per order (hasil _process_api_data, bukan hardcode). Dipanggil di dalam transaksi."""
    with_type = text("""
        UPDATE transactions_iseller_mitra
        SET fulfillment_status = 'fulfilled', sales_type = :sales_type
        WHERE order_id IN :ids
    """).bindparams(bindparam('ids', expanding=True))
    status_only = text("""
        UPDATE transactions_iseller_mitra
        SET fulfillment_status = 'fulfilled'
        WHERE order_id IN :ids
    """).bindparams(bindparam('ids', expanding=True))

    by_type = {}
    for oid in order_ids:
        by_type.setdefault(sales_type_by_order.get(oid), []).append(oid)

    total_updated = 0
    for sales_type, ids in by_type.items():
        for batch in _batches(ids):
            if sales_type is None:
                result = conn.execute(status_only, {'ids': batch})
            else:
                result = conn.execute(with_type, {'ids': batch, 'sales_type': sales_type})
            total_updated += result.rowcount
        print(f"        sales_type {sales_type or '(tidak diubah)'}: {len(ids)} order")
    return total_updated


def run_backfill_fulfillment(config_dw, start_date, end_date, dry_run=False):
    """
    Main entry point for backfilling fulfillment data.
    start_date / end_date: YYYY-MM-DD.
    dry_run: proses lengkap (baca DL + API) tapi TIDAK menulis DL dan tidak kirim email.
             Hasil yang akan ditulis dikembalikan di stats['frames'].

    Returns dict with stats.
    """
    print("=" * 65)
    print("  STEP 1: BACKFILL FULFILLMENT - iSeller Mitra" + ("  [DRY RUN - Data Lake tidak ditulis]" if dry_run else ""))
    print(f"  Unfulfilled check window: {start_date} s/d {end_date}")
    print("=" * 65)

    # Token source of truth:
    # - Airflow Variable: key `access_token_mitra` (preferred; rotate via UI)
    # - Fallback env var: `access_token_mitra`
    access_token = ""
    try:
        from airflow.models import Variable  # type: ignore

        access_token = (Variable.get("access_token_mitra", default_var="") or "").strip()
    except Exception:
        access_token = ""

    if not access_token:
        access_token = (os.environ.get("access_token_mitra") or "").strip()

    if not access_token:
        raise MissingAccessTokenError(
            "access_token_mitra is missing. Set Airflow Variable `access_token_mitra` "
            "or environment variable `access_token_mitra`."
        )

    # 1. Detect unfulfilled orders
    print("\n  Detecting unfulfilled orders...")
    df_transaction = _get_fulfillment_none(config_dw, start_date, end_date)

    if len(df_transaction) == 0:
        print("  No unfulfilled orders found. Data Lake is safe — skipping backfill.")
        print("=" * 65)
        return {
            'total_unfulfilled': 0,
            'total_fulfilled': 0,
            'total_details_dumped': 0,
            'no_unfulfilled_found': True,  # signal to orchestrator: short-circuit
            'frames': {'df_unfulfilled_dl': df_transaction},
        }

    order_id_list = df_transaction['order_id'].unique().tolist()

    print(f"        Found {len(order_id_list)} unfulfilled orders")
    print(f"        Total amount: {df_transaction['total_amount'].sum():,.0f}")

    # 2. API tasks: hanya (outlet, hari) yang punya order unfulfilled
    api_tasks = _build_api_tasks(df_transaction)

    # 3. Get master products from API
    print("\n  Fetching master products from API...")
    try:
        df_products, _ = _get_master_products(access_token)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            raise TokenExpiredError(
                "iSeller API returned 401 Unauthorized. "
                "access_token_mitra sudah expired, perlu di-update di environment variable.",
                df_unfulfilled=df_transaction
            ) from e
        raise

    # 4. Get orders from API
    print("  Fetching orders from API...")
    try:
        get_data = _get_orders_from_api(access_token, api_tasks)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            raise TokenExpiredError(
                "iSeller API returned 401 Unauthorized. "
                "access_token_mitra sudah expired, perlu di-update di environment variable.",
                df_unfulfilled=df_transaction
            ) from e
        raise

    if not any(d for d in get_data if d and len(d) > 0):
        print("  No order data returned from API. Skipping.")
        print("=" * 65)
        unfulfilled_by_outlet = (
            df_transaction
            .groupby(['outlet_code', 'outlet_name', 'date'], as_index=False)
            .agg(total_orders_unfulfilled=('order_id', 'nunique'), total_amount=('total_amount', 'sum'))
            .sort_values(['date', 'outlet_code'])
            .to_dict('records')
        )
        return {
            'total_unfulfilled': len(order_id_list),
            'total_fulfilled': 0,
            'total_details_dumped': 0,
            'unfulfilled_amount': float(df_transaction['total_amount'].astype(float).sum()),
            'fulfilled_amount': 0.0,
            'unfulfilled_by_outlet': unfulfilled_by_outlet,
            'frames': {'df_unfulfilled_dl': df_transaction},
        }

    # 5. Process data
    print("  Processing API data...")
    df_api_headers, order_sales_type, details_cleaned = _process_api_data(get_data, order_id_list, df_products)

    new_order_id_list = details_cleaned['order_id'].unique().tolist()
    order_id_not_fulfilled = list(set(order_id_list) - set(new_order_id_list))
    order_id_done_fulfilled = [oid for oid in order_id_list if oid in new_order_id_list]

    print(f"        Fulfilled: {len(order_id_done_fulfilled)}")
    print(f"        Still unfulfilled: {len(order_id_not_fulfilled)}")
    _log_reconciliation(df_api_headers, details_cleaned)

    # Amount breakdown (for Teams/email reporting) — sourced from the initial
    # unfulfilled-orders snapshot (df_transaction), sliced by final outcome.
    df_unfulfilled_detail = df_transaction[df_transaction['order_id'].isin(order_id_not_fulfilled)]
    df_fulfilled_detail = df_transaction[df_transaction['order_id'].isin(order_id_done_fulfilled)]

    unfulfilled_amount = float(df_unfulfilled_detail['total_amount'].astype(float).sum()) if len(df_unfulfilled_detail) else 0.0
    fulfilled_amount = float(df_fulfilled_detail['total_amount'].astype(float).sum()) if len(df_fulfilled_detail) else 0.0

    if len(df_unfulfilled_detail) > 0:
        unfulfilled_by_outlet = (
            df_unfulfilled_detail
            .groupby(['outlet_code', 'outlet_name', 'date'], as_index=False)
            .agg(total_orders_unfulfilled=('order_id', 'nunique'), total_amount=('total_amount', 'sum'))
            .sort_values(['date', 'outlet_code'])
            .to_dict('records')
        )
    else:
        unfulfilled_by_outlet = []

    # 6. Dump details + update status dalam 1 transaksi (gagal di tengah -> rollback semua).
    #    Order yang sudah punya details di DL tidak di-append lagi, jadi run ulang tidak menggandakan data.
    details_dumped, details_skipped = 0, 0
    details_to_dump = details_cleaned.iloc[0:0]
    existing = set()
    if order_id_done_fulfilled:
        sales_type_by_order = dict(zip(order_sales_type['order_id'], order_sales_type['sales_type']))
        engine = get_engine_datalake(config_dw)
        # dry run: koneksi baca saja (guard tetap dicek supaya hasilnya sama dengan run asli)
        with (engine.connect() if dry_run else engine.begin()) as conn:
            existing = _orders_with_existing_details(conn, order_id_done_fulfilled)
            details_to_dump = details_cleaned[~details_cleaned['order_id'].isin(existing)]
            details_skipped = len(details_cleaned) - len(details_to_dump)
            print(f"\n  Details: {len(details_to_dump):,} row {'akan ' if dry_run else ''}di-dump, "
                  f"{details_skipped:,} row di-skip ({len(existing)} order sudah punya details di DL)")
            if dry_run:
                print(f"  DRY RUN: details tidak di-dump, {len(order_id_done_fulfilled)} header tidak di-UPDATE.")
            else:
                if len(details_to_dump) > 0:
                    details_to_dump.to_sql(DETAILS_TABLE, con=conn, if_exists='append', index=False)
                details_dumped = len(details_to_dump)
                print("  Updating fulfilled orders in Data Lake...")
                updated = _update_fulfilled_orders(conn, order_id_done_fulfilled, sales_type_by_order)
        if not dry_run:
            print(f"        Committed: updated {updated}/{len(order_id_done_fulfilled)} orders to fulfilled.")

    # 7. Alert partial (sebagian masih unfulfilled), dari snapshot DL. Fulfilled:0 ditangani orchestrator.
    if order_id_not_fulfilled and order_id_done_fulfilled and not dry_run:
        df_unfulfilled = df_unfulfilled_detail.rename(columns={'date': 'transaction_date'})
        try:
            _send_alert_email(df_unfulfilled)
        except Exception as e:
            print(f"        WARNING: alert email gagal dikirim: {e}")

    print("\n  Backfill fulfillment completed.")
    print("=" * 65)

    return {
        'total_unfulfilled': len(order_id_not_fulfilled),
        'total_fulfilled': len(order_id_done_fulfilled),
        'total_details_dumped': details_dumped,
        'details_skipped_existing': details_skipped,
        'unfulfilled_amount': unfulfilled_amount,
        'fulfilled_amount': fulfilled_amount,
        'unfulfilled_by_outlet': unfulfilled_by_outlet,
        'frames': {
            # snapshot order unfulfilled dari DL (input backfill)
            'df_unfulfilled_dl': df_transaction,
            # header API order yang fulfilled. Di DL yang di-UPDATE hanya fulfillment_status='fulfilled' + sales_type
            'df_headers': df_api_headers[df_api_headers['order_id'].isin(order_id_done_fulfilled)],
            # details yang di-append ke transactions_items_iseller_mitra
            'df_details': details_to_dump,
            # details order yang sudah punya details di DL (di-skip)
            'df_details_skipped': details_cleaned[details_cleaned['order_id'].isin(existing)],
        },
    }
