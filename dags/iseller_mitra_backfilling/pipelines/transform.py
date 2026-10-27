"""
Transform backfilling iSeller Mitra: raw GetOrders -> header + details DataFrame siap dump.

Logika ini identik dengan transform di iseller_mitra_fulfillment/pipelines/pipeline_backfill_fulfillment.py
(sudah diuji terhadap notebook ingest transaction_janji_jiwa_mitra_v2): sumber diskon discounts[] (discount_amount, khusus
Mitra: diskon "Custom" kasir tidak masuk discount_order_amount), pajak dari tax_percentage API, combo_share, group per
(order_detail_id, transactions_type), sales_type 7 kondisi, list SJW = ingest v2. Kalau logika ini diubah, ubah juga di file pasangannya.
"""

import datetime

import numpy as np
import pandas as pd
from pytz import timezone

# Dedup key details iSeller (sama dengan iseller_mitra_cleaning/pipelines/dedup_details.py). Row sale/refund beda transactions_type.
# Dedup key details iSeller (sama dengan pipelines/dedup_details.py di project cleaning mitra). Row sale/refund beda transactions_type.
DETAIL_KEY = ['order_id', 'order_detail_id', 'type', 'bundling_id', 'product_id', 'sku',
              'status', 'payment_status', 'transactions_type']

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

def _group_per_detail(df, amount_col, join_cols):
    """Gabung banyak promo/discount per order_detail_id: amount di-SUM, kolom teks di-join (nilai unik, urutan asli)."""
    df = df.copy()
    df[amount_col] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)

    def _join(sep):
        return lambda s: sep.join(dict.fromkeys(str(v) for v in s if pd.notna(v) and str(v) != '')) or np.nan

    agg = {amount_col: 'sum', **{col: _join(sep) for col, sep in join_cols.items()}}
    return df.groupby('order_detail_id', sort=False).agg(agg).reset_index()

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

def transform_orders(data, df_products):
    """
    data: list hasil GetOrders (1 item = list order dari 1 panggilan; None/[] dilewati).
    Return (df_final_headers, df_final_details) untuk order di `data` yang punya transaksi.

    Order TANPA transaksi (belum dibayar / belum ada pembayaran saat ditarik) tidak punya baris header, jadi
    tidak dimasukkan sama sekali: details tanpa header menghasilkan baris dengan kolom header kosong
    (transactions_type '', sales_type '', dst.) dan meleset dari akurasi. Order itu akan tertarik di run
    berikutnya kalau transaksinya sudah ada.
    """
    order_ids = list({order['order_id'] for orders in data if orders for order in orders})
    df_headers, _order_sales_type, df_details = _process_api_data(data, order_ids, df_products)
    if 'transaction_id' not in df_headers.columns or df_headers.empty:
        print(f"        {len(order_ids):,} order tanpa transaksi (belum dibayar): tidak dimasukkan")
        return pd.DataFrame(), pd.DataFrame()
    df_headers = df_headers.drop_duplicates(subset=['order_id', 'transaction_id']).reset_index(drop=True)
    with_header = df_details['order_id'].isin(df_headers['order_id'])
    n_no_tx = df_details.loc[~with_header, 'order_id'].nunique()
    if n_no_tx:
        print(f"        {n_no_tx:,} order tanpa transaksi (belum dibayar): details-nya tidak dimasukkan")
    return df_headers, df_details[with_header].reset_index(drop=True)
