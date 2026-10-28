# `iseller_mitra_fulfillment` — ETL iSeller Mitra (Backfill Fulfillment)

Backfill status fulfillment order iSeller Mitra lewat API iSeller: order yang di Data Lake masih
`fulfillment_status = 'none'` ditarik ulang, details-nya di-append, lalu header di-UPDATE jadi
`fulfilled`. **Berhenti di Data Lake** — tidak ada redump tabel summary DWH (beda dengan
[`iseller_pusat_fulfillment`](./iseller_pusat_fulfillment.md)).

## Metadata DAG

File: `dag_etl_iseller_mitra_fulfillment.py`

| Field | Nilai |
|---|---|
| `dag_id` | `etl_iseller_mitra_fulfillment` |
| `schedule_interval` | `"50 7 * * *"` — harian 07:50 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "iseller", "mitra", "fulfillment", "datalake", "production"]` |
| `default_args` | `owner='data-team'`, `depends_on_past=False`, `email_on_failure=False`, `email_on_retry=False`, `retries=2`, `retry_delay=timedelta(minutes=5)` |
| Task | 1x `PythonOperator("run_pipeline")`, `execution_timeout=timedelta(hours=3)` |
| Param | `start_date`, `end_date` — hanya dipakai saat **Trigger DAG w/ config** (manual). Run terjadwal mengabaikannya |

**Date window**: run terjadwal memproses **rolling 9 hari (H-9 s.d. H-1)** di Asia/Jakarta, sama dengan
Pusat. Trigger manual memakai `start_date`/`end_date` dari form (isi awal = window terjadwal; start
setelah end ditolak). Tidak ada `run_mode` karena DAG ini memang tidak menyentuh DWH.

**Urutan dengan DAG lain**: DAG ini jalan duluan (07:50). [`etl_iseller_mitra_cleaning`](./iseller_mitra_cleaning.md)
(07:51) menunggu run terjadwal DAG ini selesai (sukses atau gagal), karena cleaning melakukan DELETE +
re-dump `transactions_items_iseller_mitra` — tabel yang sama yang di-append DAG ini. Sensor cleaning
**tidak** mengenali run dari notebook manual, jadi jangan jalankan notebook di sekitar 07:50 sampai
cleaning selesai (atau pause DAG cleaning dulu). Trigger manual lewat Airflow UI aman: karena
`max_active_runs=1`, run terjadwal ikut antre dan cleaning menunggu sampai selesai.

## Alur Pipeline

```
1. Backfill fulfillment (pipelines/pipeline_backfill_fulfillment.py :: run_backfill_fulfillment)
   ├─ Token: Airflow Variable access_token_mitra (fallback env var) → kosong = MissingAccessTokenError
   ├─ Deteksi order fulfillment_status='none' & payment_status='paid' di transactions_iseller_mitra
   ├─ Kalau ada → GetProducts (semua page sampai kosong) + GetOrders paralel (10 workers)
   │    HANYA untuk pasangan (outlet, hari) yang punya order unfulfilled, jendela 4 jam/hari
   ├─ Proses response → details per (order_detail × komponen combo × transaksi sale/refund),
   │    sales_type per order; pengaman dobel sebelum menulis (lihat Detail)
   ├─ Order yang sudah punya details di DL di-skip → run ulang tidak menggandakan data
   ├─ Dalam 1 transaksi DB: append details ke transactions_items_iseller_mitra
   │    + UPDATE transactions_iseller_mitra SET fulfillment_status='fulfilled', sales_type=<hasil hitung>
   │    (gagal di tengah → rollback semua)
   └─ Sebagian masih unfulfilled → email alert partial

   ⛔ Early-exit (kirim email + Teams):
      - MissingAccessTokenError → token belum di-set              → task GAGAL tanpa retry
      - TokenExpiredError       → API 401, tabel unfulfilled       → task GAGAL tanpa retry
      - Exception lain          → error teknis (API, DB, dobel)    → task GAGAL, Airflow retry (aman, idempoten)
      - no_unfulfilled_found    → semua sudah fulfilled            → task SUCCESS
      - total_fulfilled==0 tapi masih ada unfulfilled → "Hub sync required" → task SUCCESS

2. Reporting: email HTML "Fulfillment Completed" + Teams Adaptive Card (best-effort)
```

## Detail Transform

Mengikuti notebook ingest `transaction_janji_jiwa_mitra_v2.ipynb` (2026-10-16). Hasil diuji terhadap
data API asli 1 Okt 2026 (4.056 order): details = header **100,0000%**, dan identik per order dengan
output notebook.

- **Diskon dari array `discounts[]`** (kolom `discount_amount` setelah di-group per `order_detail_id`),
  **bukan** `discount_order_amount`. Di Mitra, diskon manual kasir ("Custom 50%", "Custom 3000", dst.)
  tidak masuk ke `discount_order_amount`, termasuk saat 1 detail punya "Order Custom" + "Custom"
  sekaligus. Terbukti 1-14 Okt: total `discounts[]` per order = diskon header × 1,1 untuk 57.701/57.701 order.
  > **Jangan disamakan dengan Pusat.** Pusat tetap memakai `discount_order_amount`, karena di Pusat
  > Jiwapoint tercatat 2x di `discounts[]` ("Jiwapoint" + "Diskon 15% & Jiwapoint").
- **Tarif pajak dari API** (`tax_percentage` per order_detail; combo memakai tarif level paket):
  `divisor = 1 + tarif`, `pajak = subtotal × tarif`.
- **Promo & diskon** dibagi per `(order_detail_id, transactions_type)`: `amount ÷ jumlah row detail ÷ divisor`,
  jadi row `sale` dan `refund` masing-masing kena potongan penuh.
- **Combo:** nilai paket dari API dibagi ke komponen proporsional harga master × qty (`combo_share`),
  promo/diskon paket dibagi dengan bobot yang sama.
- **`sales_type`** dari `payment_type_name`: Go Food, Grab Food, Shopee Food, Jiwa+, Grab Dine Out,
  Tokopedia Go, QPON → `Qpon`, default Walk In. `Sejuta Jiwa` kalau order berisi minimal 1 produk di
  `PRODUCT_ID_SJW_LABEL` (40 ID, sama dengan notebook; di data Mitra praktis tidak pernah cocok).
- **Details tidak di-`drop_duplicates`** — komponen combo identik dan baris sale/refund itu sah. Lihat
  [`KNOWLEDGE_comboset_komponen_identik_vs_duplicate.md`](../../dags/iseller_mitra_cleaning/KNOWLEDGE_comboset_komponen_identik_vs_duplicate.md).
  Pengaman sebelum menulis: non-combo harus unik per dedup key details, combo harus = jumlah
  `product_combos` di raw JSON. Dilanggar → pipeline berhenti, DL tidak ditulis.

## Detail Extract (API)

- **Jendela 4 jam, berakhir `xx:59:59.999`.** Dengan akhir `xx:59:59`, transaksi yang closed di
  `xx:59:59,xxx` jatuh di celah 1 detik antar jendela dan tidak ketarik (terbukti Mitra 11-13 Okt 2026:
  3 order closed 15:59:59). API menerima milidetik dan batas jendelanya tidak overlap, jadi tidak ada dobel.
- **Retry:** timeout, 5xx, dan HTTP 200 dengan body error (`status: false` / `error_message`, mis. rate
  limit) di-retry 3×. 401/403 tidak di-retry: 401 → `TokenExpiredError`, 403 → backfill failed (error biasa).
- **Re-fetch 0 order:** panggilan yang hasilnya 0 order diulang sekali (5 worker) untuk menangkap
  kegagalan diam-diam; jumlah yang terpulihkan dicetak di log.

## Sistem Eksternal

- **iSeller Mitra REST API** — `https://janjijiwamitra.isellershop.com/api/v2/{GetProducts,GetOrders}` (Bearer token).
- **MySQL Data Lake** (`config_dw.db_resource`) — `transactions_iseller_mitra` (baca + UPDATE),
  `transactions_items_iseller_mitra` (append), `outlet_mapping` (nama outlet untuk laporan).
- **Email (Gmail SMTP)** — pengirim `saputra.christabel20@gmail.com` (password dari env `GMAIL_SMTP_PASSWORD`).
- **MS Teams via Power Automate** — `teams_report_iseller_mitra_fulfillment.py`, mirroring skenario email:
  - 🔴 merah (`attention`) — missing token, token expired, backfill failed.
  - 🟡 kuning (`warning`) — hub sync required (tabel unfulfilled per outlet).
  - 🟢 hijau (`good`) — semua fulfilled, atau completed.

  Best-effort via `post_teams_safe`.

## Connections / Variables

- Airflow Variable `access_token_mitra` — token API iSeller Mitra (fallback env var). Dipakai juga oleh `iseller_mitra_cleaning`.
- Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` — webhook Teams.
- Kredensial DB dari `config/config_db_datawarehouse.py`; password SMTP dari env `GMAIL_SMTP_PASSWORD`.

## Manual Run

Dua cara:

1. **Airflow UI → Trigger DAG w/ config**, isi `start_date`/`end_date`. Disarankan untuk backfill
   periode panjang: **pecah per bulan**, karena `execution_timeout` 3 jam (lewat itu task dibunuh lalu
   di-retry dari awal — aman tapi buang waktu).
2. **Notebook** `dags/iseller_mitra_fulfillment/manual_run_iseller_mitra_fulfillment.ipynb` — param
   `AUTO_MODE`, `MANUAL_START_DATE`, `MANUAL_END_DATE`, `DRY_RUN`. `DRY_RUN = True` menjalankan backfill
   penuh (baca DL + API) tanpa menulis apa pun dan tanpa email/Teams; hasilnya di
   `result['backfill']['frames']` (`df_headers`, `df_details`, `df_details_skipped`, `df_unfulfilled_dl`).
   Bawaan notebook `DRY_RUN = False` — cek dulu sebelum menjalankan.

**DWH tidak ikut ter-update.** Order yang baru fulfilled di tanggal lama tidak otomatis masuk tabel
summary DWH. Setelah backfill yang memproses order, redump manual lewat DAG cleaning: trigger
`etl_iseller_mitra_cleaning` dengan `run_mode = dwh_only` dan tanggal yang sama — **per bulan** untuk
periode panjang (tiap tabel memuat seluruh rentang ke memory dan DELETE + insert tidak dalam 1
transaksi, jadi gagal di tengah hanya berdampak ke 1 bulan).
