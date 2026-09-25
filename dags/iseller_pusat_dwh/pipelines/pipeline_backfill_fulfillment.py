"""
Pipeline: Backfill Fulfillment None - iSeller Pusat

Detects unfulfilled orders in transactions_iseller_pusat, re-pulls data from
iSeller API, inserts details to DL, and updates fulfillment_status to 'fulfilled'.
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
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed

from common.db_helpers import get_connection, get_engine_datalake

class MissingAccessTokenError(Exception):
    """Raised when access_token_pusat is missing from Variable or env."""
    pass


class TokenExpiredError(Exception):
    """Raised when iSeller API returns 401 — token expired."""
    def __init__(self, message, df_unfulfilled=None):
        super().__init__(message)
        self.df_unfulfilled = df_unfulfilled

PUSAT_OUTLET_CODES = ('01150', '07001', '07003', '07004', '07005', '07006', '07007', '07008', '07009', '07010')

PRODUCT_ID_SJW_SJO_HEADERS = {
    '1d2664fc-f2c4-4d63-93b0-0150c5bad9ca', '514f7f40-fcf1-4b24-a881-0e32a6e2da88',
    'b7a5bd88-cda9-4623-814a-734767026910', '134f7a5b-09a2-455c-8f70-93a01468131e',
    'b26ce832-29a4-46f7-be9f-e9f4d72c1dca', '64b0cc8b-88e4-498a-b5b5-ff093b5915e5',
    '2306edf4-9f70-4c79-88d9-2a836a292816', '9eef163d-fd2f-4e52-98e6-4593679a16b5',
    '9ea8be5c-8bc1-4f14-b564-5eb75a19d907', 'a96b7a4e-b86d-4171-956b-77a09d1c54f9',
    '6b07599f-14f8-44f9-ab5b-4389bf7e82d1', '328e5672-48a1-4683-b65d-c4a45f344ca1',
    'baa1d6c2-7b48-40af-a6f3-b9e481f93e61', '3ab4aa05-f664-4eaf-ad7e-bdf643d421d4',
    '2fcb7dad-bd59-44c9-a846-1279f9840d37', 'c038d0fa-a12a-4644-9bbb-3878b3064912',
    '4521dc51-b3d6-4493-814a-617fde292c04', '62701d95-4d24-449e-b0db-ea8994d00b62',
    '8f5fe78b-7d7c-46ed-9f5b-c9f8d195b60d', '44d2e817-8745-4cbf-aa36-d4abfc75aebf',
    'a26111f7-3aa5-48b0-a77b-2f952aab5eae', '74ee1eae-21b8-467a-828c-b59cf780d065',
    'a89893bf-9026-4a5c-9f0c-949c0f4539d7', '150c96b0-fe81-4835-b3cc-593bfb6c45db',
    '999251a7-5e8b-4267-b67c-b33cedead904', '0c8a73fe-ae1b-4287-a6af-7d48fce86de5',
    'c9d038f6-fea1-494f-813c-ad0aed0dde1e', 'a94730e0-4f5b-4af9-b70b-68dbea299b78',
    '5842673c-8a8b-423b-9804-1c0d484c60ac', 'bae1b783-e862-4c5f-bb46-b20bb7e02b63',
    '00295e2b-66bb-489f-9615-6e25676c8331', 'd8a7f659-c1dd-448a-9110-688fd1105bf8',
    '47855957-b056-49bc-8be3-c8ea68e0160b', '5864c6dc-216e-4764-9c8b-71bc9d77c41a',
    '3ffe31ac-f4c6-4206-959f-26f2ec6224fc', '64b5deb0-fcef-412f-bd2e-714a9b65a6b9',
}

PRODUCT_ID_SJW_SJO_DETAILS = {
    '1d2664fc-f2c4-4d63-93b0-0150c5bad9ca', '514f7f40-fcf1-4b24-a881-0e32a6e2da88',
    'b7a5bd88-cda9-4623-814a-734767026910', '134f7a5b-09a2-455c-8f70-93a01468131e',
    'b26ce832-29a4-46f7-be9f-e9f4d72c1dca', '64b0cc8b-88e4-498a-b5b5-ff093b5915e5',
    '2306edf4-9f70-4c79-88d9-2a836a292816', '328e5672-48a1-4683-b65d-c4a45f344ca1',
    'baa1d6c2-7b48-40af-a6f3-b9e481f93e61', '3ab4aa05-f664-4eaf-ad7e-bdf643d421d4',
    '2fcb7dad-bd59-44c9-a846-1279f9840d37', 'c038d0fa-a12a-4644-9bbb-3878b3064912',
    '4521dc51-b3d6-4493-814a-617fde292c04', '11669a10-4cfa-4cdf-89d8-ee21303011b4',
    'd12c672d-f9ec-4bce-a05f-1ba9f33b78fd', 'fa9f3400-d5b2-4f90-bed9-62b3d7729634',
    '62701d95-4d24-449e-b0db-ea8994d00b62', '8f5fe78b-7d7c-46ed-9f5b-c9f8d195b60d',
    '44d2e817-8745-4cbf-aa36-d4abfc75aebf', 'a26111f7-3aa5-48b0-a77b-2f952aab5eae',
    '74ee1eae-21b8-467a-828c-b59cf780d065', '150c96b0-fe81-4835-b3cc-593bfb6c45db',
    '999251a7-5e8b-4267-b67c-b33cedead904', 'c9d038f6-fea1-494f-813c-ad0aed0dde1e',
    'a94730e0-4f5b-4af9-b70b-68dbea299b78', '5842673c-8a8b-423b-9804-1c0d484c60ac',
    'bae1b783-e862-4c5f-bb46-b20bb7e02b63', '00295e2b-66bb-489f-9615-6e25676c8331',
    'd8a7f659-c1dd-448a-9110-688fd1105bf8', '47855957-b056-49bc-8be3-c8ea68e0160b',
    '12b0ffba-e456-4faf-a4ce-4f58dff0b3c4', 'c05dba29-ccd5-4fc3-90a1-4d616d7852d9',
    '05637d36-2b3e-429b-a7b0-3896eec94f3e', '7c56f36e-2595-4ab2-a322-655ee731baf0',
    'c457003b-c93f-4e82-a54b-177db2f3589b', '5864c6dc-216e-4764-9c8b-71bc9d77c41a',
    '75de7ef4-5119-4784-97b8-8d17e1fd5069', '835bcdf5-4555-4ff1-a18d-8ed6be7fe0ff',
    'b40cc79c-48d0-493a-a1bc-4139f184a0df', '7fc6fcf2-6845-4c7e-ae0d-3a07031e8c31',
    '88c16c67-7e2e-454a-a81b-63d548f37318', '1d0d46e1-1e3b-4eec-a888-16c856e00f6a',
    'aae367a0-edfe-4363-a43e-59911694539d', 'cdfdc515-39cc-4438-948d-8090f62eecab',
    '9caea397-4a5f-4a1e-a5b2-b62e7a8d3436', '6455a9cc-91f5-4d66-b8f5-ed2a0dbf8faa',
    'e3e79651-221b-4e25-855e-3493a0ae35aa', '64b5deb0-fcef-412f-bd2e-714a9b65a6b9',
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


def _adjust_timezone(dt_str, offset_hours):
    dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    adjusted = dt + datetime.timedelta(hours=offset_hours)
    return adjusted.strftime("%Y-%m-%d %H:%M:%S")


def _get_fulfillment_none(config_dw, start_date, end_date):
    """
    Unfulfilled headers — same calendar window as DWH re-dump (start_date … end_date, YYYY-MM-DD).
    """
    conn = get_connection(config_dw, config_dw.db_resource)
    cursor = conn.cursor()

    outlet_codes = ", ".join([f"'{c}'" for c in PUSAT_OUTLET_CODES])
    cursor.execute(f"""
        SELECT
            date_format(t.transaction_date, '%Y-%m-%d') AS date,
            t.outlet_id, t.outlet_code,
            CASE WHEN om.customer_name IS NOT NULL THEN om.customer_name ELSE '-' END AS outlet_name,
            t.fulfillment_status, t.status,
            t.payment_status, t.transactions_status, t.order_id, t.order_reference,
            SUM(t.total_amount) AS total_amount
        FROM transactions_iseller_pusat t
        LEFT JOIN (SELECT DISTINCT jilid, customer_name FROM outlet_mapping) om
            ON t.outlet_code = om.jilid
        WHERE
            date_format(t.transaction_date, '%Y-%m-%d') BETWEEN '{start_date}' AND '{end_date}'
            AND t.fulfillment_status = 'none'
            AND t.payment_status = 'paid'
        GROUP BY 1,2,3,4,5,6,7,8,9,10
    """)
    # outlet_code IN ({outlet_codes})
    #         AND 
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    cursor.close()
    conn.close()
    return df


def _get_master_products(access_token):
    url = 'https://janjijiwapusat.isellershop.com/api/v2/GetProducts'
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/x-www-form-urlencoded'}

    df_product_data = pd.DataFrame()
    for i in range(1, 5):
        data = {'modified_after': '2023-01-01', 'track_inventory': 'FALSE', 'page_size': 500, 'page': i}
        resp = requests.post(url, headers=headers, data=data)
        resp.raise_for_status()
        result = json.loads(resp.text)
        df = pd.DataFrame(list(result.items()), columns=['name', 'value'])
        try:
            product_data = pd.DataFrame(df['value'][0])
            df_product_data = pd.concat([product_data, df_product_data], ignore_index=False)
        except Exception:
            pass

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
    """Single API call for one (outlet_id, time_window) pair — used by ThreadPoolExecutor."""
    url = 'https://janjijiwapusat.isellershop.com/api/v2/GetOrders'
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'created_after': start_dt,
        'created_before': end_dt,
        'timezone': '7',
        'includes': 'orderdetails, promotiondetails, transactions, discountdetails',
        'outlet_id': outlet_id
    }
    resp = requests.post(url, headers=headers, data=data)
    resp.raise_for_status()
    result = json.loads(resp.text)
    df = pd.DataFrame(list(result.items()), columns=['name', 'value'])
    return df['value'][1]


def _get_orders_from_api(access_token, outlet_id_list, start_date_list, end_date_list, max_workers=10):
    """Fetch orders for all (outlet_id × time_window) combos in parallel using ThreadPoolExecutor."""
    tasks = [
        (outlet_id, start_date_list[idx], end_date_list[idx])
        for outlet_id in outlet_id_list
        for idx in range(len(start_date_list))
    ]

    print(f"        Total API calls: {len(tasks)} (parallelized, max_workers={max_workers})")

    get_data = [None] * len(tasks)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(_fetch_single_order, access_token, outlet_id, start_dt, end_dt): i
            for i, (outlet_id, start_dt, end_dt) in enumerate(tasks)
        }
        for future in as_completed(future_to_idx):
            i = future_to_idx[future]
            get_data[i] = future.result()

    return get_data


def _process_api_data(data, order_id_list, df_products):
    """Process raw API data into cleaned headers and details DataFrames."""
    df_headers = pd.DataFrame()
    df_details = pd.DataFrame()
    df_product_combos = pd.DataFrame(columns=['order_detail_id', 'product_id', 'product_sku', 'product_name', 'variant_name', 'quantity'])
    df_promo = pd.DataFrame(columns=['order_detail_id', 'promotion_id', 'promotion_name', 'promotion_type', 'promotion_amount', 'external_id'])
    df_discount = pd.DataFrame(columns=['order_detail_id', 'discount_name', 'discount_amount', 'external_id'])

    headers_list_cols = [
        'outlet_id', 'order_reference', 'outlet_code', 'outlet_name', 'order_date', 'closed_date',
        'status', 'channel', 'channel_detail', 'payment_status', 'fulfillment_status', 'notes',
        'customer_id', 'customer_first_name', 'customer_last_name', 'customer_phone_number',
        'customer_email', 'cashier_id', 'total_order_amount', 'total_discount_amount',
        'total_promotion_amount', 'subtotal', 'total_tax_amount', 'total_amount'
    ]

    for headers in data:
        if not headers or len(headers) == 0:
            continue
        df_headers = pd.concat([df_headers, pd.json_normalize(headers, record_path=['transactions'], meta=headers_list_cols, meta_prefix='transactions_')])
        df_details = pd.concat([df_details, pd.json_normalize(headers, record_path=['order_details'])])

        if 'order_details' in headers:
            for detail in headers['order_details']:
                if 'product_combos' in detail and detail['product_combos'] is not None:
                    df_product_combos = pd.concat([df_product_combos, pd.json_normalize(detail['product_combos'], meta='order_detail_id')])
                if 'promotions' in detail and len(detail['promotions']) > 0:
                    df_promo = pd.concat([df_promo, pd.json_normalize(detail['promotions'], meta='order_detail_id')])
                if 'discounts' in detail and len(detail['discounts']) > 0:
                    df_discount = pd.concat([df_discount, pd.json_normalize(detail['discounts'], meta='order_detail_id')])

    if len(df_headers) == 0:
        return pd.DataFrame(), pd.DataFrame()

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

    # Sales type determination
    df_headers_sjw = df_details[['order_id', 'product_id']].copy()
    df_headers_sjw['is_sjw'] = df_headers_sjw['product_id'].apply(lambda x: x in PRODUCT_ID_SJW_SJO_HEADERS)
    df_headers_sjw = df_headers_sjw[['order_id', 'is_sjw']].drop_duplicates()
    df_merged = pd.merge(df_headers, df_headers_sjw, on='order_id', how='left')

    conditions = [
        (df_merged['transactions_payment_type_name'] == 'Go Food'),
        (df_merged['transactions_payment_type_name'] == 'Grab Food'),
        (df_merged['transactions_payment_type_name'] == 'Shopee Food'),
        (df_merged['transactions_payment_type_name'] == 'Jiwa+'),
        (df_merged['transactions_payment_type_name'] == 'Grab Dine Out'),
        (df_merged['transactions_payment_type_name'] == 'Tokopedia Go'),
    ]
    values = ['Go Food', 'Grab Food', 'Shopee Food', 'Jiwa+', 'Grab Dine Out', 'Tokopedia Go']
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

    df_filtered['divisor'] = df_filtered['product_id'].apply(lambda x: 1.1 if x not in PRODUCT_ID_SJW_SJO_DETAILS else 1.11)
    df_filtered['total_order_amount'] = df_filtered.apply(
        lambda row: row['price'] * row['quantity'] / row['divisor'] if row['type'] == 'comboset' else row['total_order_amount'], axis=1
    )

    def calculate_new_amount(group, amount_column):
        divisor = 1.1 if all(pid not in PRODUCT_ID_SJW_SJO_DETAILS for pid in group['product_id']) else 1.11
        group_size = len(group)
        return group[amount_column] / group_size / divisor

    df_filtered['discount_amount_new'] = df_filtered.groupby('order_detail_id')['discount_order_amount'].transform(
        lambda x: calculate_new_amount(df_filtered.loc[x.index], 'discount_order_amount')
    )
    df_filtered['promotion_amount_new'] = 0
    df_filtered['subtotal'] = df_filtered['total_order_amount'] - df_filtered['discount_amount_new'] - df_filtered['promotion_amount_new']
    df_filtered['tax_amount'] = df_filtered.apply(
        lambda row: row['subtotal'] * 0.11 if row['product_id'] in PRODUCT_ID_SJW_SJO_DETAILS else row['subtotal'] * 0.1, axis=1
    )
    df_filtered['total_amount'] = df_filtered['subtotal'] + df_filtered['tax_amount']
    df_filtered.drop(columns=['divisor'], inplace=True)

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

    # Filter only the unfulfilled order_ids
    headers_cleaned = df_final_headers[df_final_headers['order_id'].isin(order_id_list)].drop_duplicates(subset=['order_id'])
    details_cleaned = df_final_details[df_final_details['order_id'].isin(order_id_list)].drop_duplicates(subset=['order_detail_id'])

    return df_final_headers, headers_cleaned, details_cleaned


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

    subject = "[POS HUB Reckon] - ALERT - Found Order Still Unfulfilled"
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


def _update_fulfilled_orders(order_ids, config_dw):
    if not order_ids:
        return

    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(
        host=config_dw.hostname, user=config_dw.username,
        pw=config_dw.password, db=config_dw.db_resource
    ))

    batch_size = 1000
    total_updated = 0

    with engine.begin() as conn:
        for i in range(0, len(order_ids), batch_size):
            batch = tuple(order_ids[i:i + batch_size])
            result = conn.execute(text("""
                UPDATE transactions_iseller_pusat
                SET fulfillment_status = 'fulfilled',
                    sales_type = 'Sejuta Jiwa'
                WHERE order_id IN :ids
            """), {"ids": batch})
            total_updated += result.rowcount

    print(f"        Updated {total_updated}/{len(order_ids)} orders to fulfilled.")


def run_backfill_fulfillment(config_dw, start_date, end_date):
    """
    Main entry point for backfilling fulfillment data.
    start_date / end_date: same as DWH pipelines (YYYY-MM-DD).

    Returns dict with stats or None if no unfulfilled orders found.
    """
    print("=" * 65)
    print("  STEP 1: BACKFILL FULFILLMENT - iSeller Pusat")
    print(f"  Unfulfilled check window (same as DWH): {start_date} s/d {end_date}")
    print("=" * 65)

    # Token source of truth:
    # - Airflow Variable: key `access_token_pusat` (preferred; rotate via UI)
    # - Fallback env var: `access_token_pusat`
    access_token = ""
    try:
        from airflow.models import Variable  # type: ignore

        access_token = (Variable.get("access_token_pusat", default_var="") or "").strip()
    except Exception:
        access_token = ""

    if not access_token:
        access_token = (os.environ.get("access_token_pusat") or "").strip()

    if not access_token:
        raise MissingAccessTokenError(
            "access_token_pusat is missing. Set Airflow Variable `access_token_pusat` "
            "or environment variable `access_token_pusat`."
        )

    # 1. Detect unfulfilled orders
    print("\n  Detecting unfulfilled orders...")
    df_transaction = _get_fulfillment_none(config_dw, start_date, end_date)

    if len(df_transaction) == 0:
        print("  No unfulfilled orders found. Data Lake is safe — skipping backfill & DWH redump.")
        print("=" * 65)
        return {
            'total_unfulfilled': 0,
            'total_fulfilled': 0,
            'total_details_dumped': 0,
            'no_unfulfilled_found': True,  # signal to orchestrator: short-circuit, skip DWH redump
        }

    order_id_list = df_transaction['order_id'].unique().tolist()
    outlet_id_list = df_transaction['outlet_id'].unique().tolist()

    print(f"        Found {len(order_id_list)} unfulfilled orders")
    print(f"        Total amount: {df_transaction['total_amount'].sum():,.0f}")

    # 2. Generate time windows for API calls (6-hour intervals for all days in range)
    start_date_list, end_date_list = [], []

    # Loop through all dates from start_date to end_date
    start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
    num_days = (end_date_obj - start_date_obj).days + 1

    for day_offset in range(num_days):
        current_date = start_date_obj + datetime.timedelta(days=day_offset)

        # 4 intervals per day: 0-5h, 6-11h, 12-17h, 18-23h
        for i in range(4):
            start_hour = i * 6
            end_hour = (i + 1) * 6 - 1

            date_str = current_date.strftime("%Y-%m-%d")
            start_dt = _adjust_timezone(f"{date_str} {start_hour:02}:00:00", -7)
            end_dt = _adjust_timezone(f"{date_str} {end_hour:02}:59:59", -7)
            start_date_list.append(start_dt)
            end_date_list.append(end_dt)

    # 3. Get master products from API
    print("\n  Fetching master products from API...")
    try:
        df_products, _ = _get_master_products(access_token)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            raise TokenExpiredError(
                "iSeller API returned 401 Unauthorized. "
                "access_token_pusat sudah expired, perlu di-update di environment variable.",
                df_unfulfilled=df_transaction
            ) from e
        raise

    # 4. Get orders from API
    print("  Fetching orders from API...")
    try:
        get_data = _get_orders_from_api(access_token, outlet_id_list, start_date_list, end_date_list)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            raise TokenExpiredError(
                "iSeller API returned 401 Unauthorized. "
                "access_token_pusat sudah expired, perlu di-update di environment variable.",
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
        }

    # 5. Process data
    print("  Processing API data...")
    df_final_headers, headers_cleaned, details_cleaned = _process_api_data(get_data, order_id_list, df_products)

    new_order_id_list = details_cleaned['order_id'].unique().tolist()
    order_id_not_fulfilled = list(set(order_id_list) - set(new_order_id_list))
    order_id_done_fulfilled = [oid for oid in order_id_list if oid in new_order_id_list]

    print(f"        Fulfilled: {len(order_id_done_fulfilled)}")
    print(f"        Still unfulfilled: {len(order_id_not_fulfilled)}")

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

    # 6. Dump details to Data Lake
    if len(details_cleaned) > 0:
        print(f"\n  Dumping {len(details_cleaned)} detail rows to Data Lake...")
        engine = get_engine_datalake(config_dw)
        details_cleaned.to_sql('transactions_items_iseller_pusat', con=engine, if_exists='append', index=False)
        print("        Details dumped successfully.")

    # 7. Alert jika masih ada unfulfilled tapi sudah ada yang fulfilled (partial) — Fulfilled:0 handled di orchestrator
    if order_id_not_fulfilled and order_id_done_fulfilled:
        df_unfulfilled = df_final_headers[df_final_headers['fulfillment_status'] == 'none'][
            ['transaction_date', 'outlet_code', 'order_id', 'fulfillment_status', 'total_amount']
        ]
        df_unfulfilled['transaction_date'] = pd.to_datetime(df_unfulfilled['transaction_date']).dt.tz_localize(None).dt.date
        _send_alert_email(df_unfulfilled)

    # 8. Update fulfilled orders in Data Lake
    if order_id_done_fulfilled:
        print("  Updating fulfilled orders in Data Lake...")
        _update_fulfilled_orders(order_id_done_fulfilled, config_dw)

    print("\n  Backfill fulfillment completed.")
    print("=" * 65)

    return {
        'total_unfulfilled': len(order_id_not_fulfilled),
        'total_fulfilled': len(order_id_done_fulfilled),
        'total_details_dumped': len(details_cleaned),
        'unfulfilled_amount': unfulfilled_amount,
        'fulfilled_amount': fulfilled_amount,
        'unfulfilled_by_outlet': unfulfilled_by_outlet,
    }
