# `iseller_pusat_dwh` — ETL iSeller Pusat

DAG paling kompleks di project ini: backfill status fulfillment lewat API iSeller,
lalu redump 8 tabel summary Data Warehouse.

## Metadata DAG

File: `dag_etl_iseller_pusat_dwh.py`

| Field | Nilai |
|---|---|
| `dag_id` | `etl_iseller_pusat_dwh` |
| `schedule_interval` | `"0 9 * * *"` — harian 09:00 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "iseller", "pusat", "dwh", "production"]` |
| `default_args` | `owner='data-team'`, `depends_on_past=False`, `email_on_failure=False`, `email_on_retry=False`, `retries=2`, `retry_delay=timedelta(minutes=5)` |
| Task | 1x `PythonOperator("run_pipeline")` — seluruh flow dalam 1 callable Python |

**Date window**: `end_date` = "kemarin" di Asia/Jakarta (dari `data_interval_end`/`logical_date`), `start_date = end_date - 8 hari` → **rolling window 9 hari (H-9 s.d. H-1)** setiap run, supaya data fulfillment yang telat masuk tetap tertangkap.

## Run Mode (Trigger DAG w/ config)

DAG punya satu param `run_mode` (dropdown di form **Trigger DAG w/ config** di Airflow UI):

| `run_mode` | STEP 1 (Data Lake backfill) | STEP 2 (DWH redump) | Kapan dipakai |
|---|---|---|---|
| `full` (default) | ✅ | ✅ | Run terjadwal 09:00 selalu pakai ini |
| `datalake_only` | ✅ | ⛔ tidak disentuh | Mau update status fulfilled + dump detail ke Data Lake tanpa menyentuh DWH |
| `dwh_only` | ⛔ skip | ✅ | Data Lake sudah bersih/di-clean manual, langsung redump DWH |

- Run terjadwal tidak terpengaruh — params cuma diisi saat trigger manual.
- `datalake_only` tetap tunduk ke early-exit STEP 1 (token expired, backfill gagal, semua fulfilled, hub sync required). Kalau backfill sukses, dikirim email `[OK] ... Data Lake Only, DWH Not Updated` + kartu Teams hijau "Data Lake updated — DWH not touched".
- `dwh_only` = `run_all(..., skip_dl_check=True)`. Kartu Teams "ETL completed" menampilkan jumlah tabel DWH yang SUCCESS, bukan statistik backfill.

## Alur Pipeline

```
1. STEP 1 — Backfill fulfillment (pipeline_backfill_fulfillment.py)
   ├─ Deteksi order fulfillment_status='none' & payment_status='paid' di window 9 hari
   ├─ Kalau ada → panggil API iSeller (GetProducts, GetOrders) paralel (ThreadPoolExecutor, 10 workers)
   ├─ Proses response → insert detail baru ke transactions_items_iseller_pusat
   └─ Update transactions_iseller_pusat.fulfillment_status = 'fulfilled' (batch 1000 baris)

   ⛔ 4 early-exit branch (skip STEP 2, kirim email+Teams, lalu selesai):
      - MissingAccessTokenError  → token belum di-set
      - TokenExpiredError        → API 401, kirim tabel unfulfilled orders
      - Exception saat backfill  → generic failure
      - no_unfulfilled_found     → semua sudah fulfilled, tidak ada kerjaan
      - total_fulfilled==0 tapi total_unfulfilled>0 → "Hub sync required"

2. STEP 2 — Redump 8 tabel DWH, urutan STRICT (lihat alasan di bawah):
   a. daily_transaction_summary_iseller_pusat        (pipeline_daily_transactions.py)
   b. daily_sales_type_summary_iseller_pusat          (pipeline_daily_sales_type.py)
   c. hourly_transaction_summary_iseller_pusat        (pipeline_hourly_transactions.py)
   d. daily_items_summary_iseller_pusat                (pipeline_daily_items.py)
   e. daily_bundling_summary_iseller_pusat             (pipeline_daily_bundlings.py)
   f. daily_items_salestype_summary_pusat              (pipeline_daily_items_sales_type.py)
   g. daily_outlet_transaction_summary_companywide_new (pipeline_daily_companywide.py)
   h. hourly_outlet_transaction_summary_companywide_new(pipeline_hourly_companywide.py)

3. STEP 3 — Reporting: email HTML (Gmail SMTP) + Teams Adaptive Card (best-effort)
```

Setiap pipeline STEP 2 dibungkus try/except sendiri — 1 tabel gagal **tidak** menghentikan tabel lain (beda dengan STEP 1 yang menghentikan seluruh run kalau backfill gagal fatal).

**Kenapa urutan strict?** Pipeline (g) dan (h) melakukan `UNION` lintas sumber termasuk tabel yang baru saja ditulis oleh pipeline (a) dan (c) — jadi (g)/(h) wajib jalan setelah (a)/(c) selesai di run yang sama.

## Detail Tiap Pipeline

- **`pipeline_backfill_fulfillment.py`** (gatekeeper, bukan bagian dari "8 tabel"): baca `access_token_pusat` dari Airflow Variable (fallback env var), raise `MissingAccessTokenError` kalau kosong. Panggil REST API `https://janjijiwapusat.isellershop.com/api/v2/{GetProducts,GetOrders}` per outlet per jendela 4 jam (6 jendela/hari), paralel via `ThreadPoolExecutor`. HTTP 401 → `TokenExpiredError`. Reshape pandas berat: expand bundling ("comboset"), tax berbeda untuk produk SJW/SJO (11%) vs produk lain (10%) berdasarkan set product-id hardcoded. Custom exception `TokenExpiredError(message, df_unfulfilled=None)` membawa DataFrame unfulfilled untuk ditampilkan di email/Teams.
- **`pipeline_daily_transactions.py`**: agregasi harian per outlet dari `transactions_iseller_pusat`, join `outlet_mapping`/`kbn_transition_outlets`/`outlet_brand_mapping` untuk metadata (region/provinsi/kota/brand/ownership model) — ada logic "transition-aware": pakai ownership/brand **lama** kalau transaksi terjadi sebelum tanggal transisi KBN. Filter: `payment_status='paid' AND fulfillment_status='fulfilled' AND transactions_status='success' AND sales_type NOT LIKE '%jiwa+%'`.
- **`pipeline_daily_sales_type.py`**: sama joinnya, grouping tambahan by `sales_type`/`payment_type`, bikin kolom komposit `sales_type_payment_method`.
- **`pipeline_hourly_transactions.py`**: sama seperti daily_transactions, tambah grouping `HOUR(transaction_date)`.
- **`pipeline_daily_items.py`**: breakdown level item dari `transactions_items_iseller_pusat` (ada lookback 1 hari via `load_data_at >= start_date - 1 day` untuk item yang telat ke-load), join header + outlet mapping.
- **`pipeline_daily_bundlings.py`**: agregasi combo/bundle (`type='comboset'`, `bundling_id != ''`).
- **`pipeline_daily_items_sales_type.py`**: gabungkan item iSeller (gross sales × 1.1) dengan baris `daily_items_summary_apps_new` yang `ownership_model='KBN'` (dilabel `sales_type='JIWA+'`) → `daily_items_salestype_summary_pusat`. Catatan: tabel ini tidak pakai filter `sources` di `get_summary_before`/DELETE, beda dari pipeline lain.
- **`pipeline_daily_companywide.py`**: rollup lintas 4 sumber POS via `UNION` — MOKA (`daily_outlet_transaction_summary_new`, exclude `outlet_id=217043`, refund order di-net ×2), JIWA+ (`daily_outlet_transaction_summary_apps_new`), ARTHAPOS (`daily_outlet_transaction_summary_arthapos_new`), ISELLER (output pipeline a).
- **`pipeline_hourly_companywide.py`**: `UNION` sama tapi hourly, sumber ISELLER-nya dari output pipeline c.

## Sistem Eksternal

- **iSeller REST API** — `GetProducts`, `GetOrders` (Bearer token), dipanggil hanya saat backfill.
- **MySQL Data Lake** (`config_dw.db_resource`) — source: `transactions_iseller_pusat`, `transactions_items_iseller_pusat`, `outlet_mapping`, `outlet_brand_mapping`, `kbn_transition_outlets`.
- **MySQL Data Warehouse** (`config_dw.db_target`) — 8 tabel summary + tabel companywide cross-source (`daily_outlet_transaction_summary_new` [MOKA], `_apps_new` [JIWA+], `_arthapos_new` [ARTHAPOS], plus hourly counterpart-nya).
- **Email (Gmail SMTP)** — `smtp.gmail.com`, pengirim `saputra.christabel20@gmail.com` (kredensial hardcoded di source), penerima `athens.jiwagroup@gmail.com`, `lahia.ardhanlahia@gmail.com`.
- **MS Teams via Power Automate** — `teams_report_iseller_pusat_unfulfilled.py`, mirroring skenario email. Warna kartu mengikuti severity:
  - 🔴 merah (`attention`) — pipeline berhenti karena error: missing token, token expired, backfill failed (judul berisi tipe exception + excerpt log).
  - 🟡 kuning (`warning`) — hub sync required: total unfulfilled + total amount, tabel per outlet (`Jilid | Nama Outlet | Date | Total Orders Unfulfilled | Total Amount`).
  - 🟢 hijau (`good`) — semua fulfilled, completed (+ fulfilled/unfulfilled amount), atau datalake_only.

  Best-effort via `post_teams_safe`.

## Connections / Variables

- Airflow Variable `access_token_pusat` — token API iSeller Pusat (**expired berkala, harus di-update manual** — lihat root [`README.md`](../../README.md#api-token-access_token_pusat)).
- Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` — webhook Teams.
- Tidak ada Airflow Connection native — semua kredensial DB dari `config/config_db_datawarehouse.py` (env var + fallback hardcoded).
- Kredensial SMTP Gmail hardcoded langsung di source (bukan Variable/Connection).

## Manual Run

Ada notebook pendamping `dags/iseller_pusat_dwh/manual_run_iseller_pusat_dwh.ipynb` untuk trigger manual dengan parameter `AUTO_MODE`, `MANUAL_START_DATE`, `MANUAL_END_DATE`, `SKIP_DL_CHECK` — detail lengkap ada di root [`README.md`](../../README.md#manual-run-notebook).

## Catatan Lain

- Konstanta `PUSAT_OUTLET_CODES` dideklarasikan di `pipeline_backfill_fulfillment.py` tapi filter SQL yang memakainya di-comment out — kode mati/vestigial.