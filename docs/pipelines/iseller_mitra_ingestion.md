# `iseller_mitra_ingestion` — ETL iSeller Mitra (Ingestion harian order H-1)

Menarik order **H-1** dari API iSeller Mitra (header + details) ke Data Lake. Order yang sudah ada di Data Lake **dilewati**,
jadi retry atau trigger ulang tidak menggandakan data. Berhenti di Data Lake: tidak ada redump DWH.

Pipeline-nya sama dengan [`iseller_mitra_backfilling`](./iseller_mitra_backfilling.md) (extract, transform, load disalin dari sana);
bedanya hanya jendela tanggal. Ingestion menulis **H-1**, backfilling menulis **H-8 s/d H-7**.

**Urutan dengan DAG lain**: jalan 06:00, paling awal: lalu `etl_iseller_mitra_backfilling` (06:10), `etl_iseller_mitra_fulfillment`
(07:50), `etl_iseller_mitra_cleaning` (07:51). Cleaning menunggu fulfillment, **tidak** menunggu DAG ini; kalau ingestion
normalnya selesai dalam hitungan menit itu tidak masalah. Kalau suatu saat ingestion bisa molor mendekati 07:50, tambahkan sensor.

## Metadata DAG

File: `dag_etl_iseller_mitra_ingestion.py`

| Field | Nilai |
|---|---|
| `dag_id` | `etl_iseller_mitra_ingestion` |
| `schedule_interval` | `"0 6 * * *"` — harian 06:00 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "iseller", "mitra", "ingestion", "datalake", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=timedelta(minutes=5)` |
| Task | 1x `PythonOperator("run_pipeline")`, `execution_timeout=timedelta(hours=2)` |
| Param | `start_date`, `end_date`, `dry_run` — hanya dipakai saat **Trigger DAG w/ config** (manual, `run_type == "manual"`) |

**Date window**: run terjadwal (juga backfill / dataset) memproses **H-1** di Asia/Jakarta dari `data_interval_end`. Trigger manual memakai
`start_date`/`end_date` dari form (isi awal = H-1; start setelah end ditolak). `dry_run = True` (manual saja) = tarik + proses lengkap
tapi **tidak menulis Data Lake dan tidak kirim email/Teams**; run terjadwal selalu menulis.

## Alur Pipeline (`etl_orchestrator.run_all`)

```
1. Token: Airflow Variable access_token_mitra (fallback env var) → kosong = MissingAccessTokenError (tanpa retry)
2. order_id yang sudah ada di transactions_iseller_mitra (transaction_date window + 1 hari)
3. Extract: getstoreinfo + GetProducts + GetOrders per (outlet x jendela 4 jam), 10 worker, retry 5x
4. Filter: hanya order yang belum ada di Data Lake
5. Transform (pipelines/transform.py, sama dengan backfilling dan fulfillment)
6. Load: header + details dalam 1 TRANSAKSI; order_id existing dicek ulang di dalam transaksi
7. Email + Teams. Panggilan API yang TETAP gagal: data yang berhasil tetap di-load, task digagalkan (PARTIAL) supaya Airflow retry
   ⛔ MissingAccessTokenError / TokenExpiredError (401) → task GAGAL tanpa retry; exception lain → GAGAL, retry Airflow (aman, idempoten)
```

## Detail

- **Kenapa aman diulang:** cek "order sudah ada" memindai `transactions_iseller_mitra` (±1,6 juta baris, tanpa index) dan hanya makan
  beberapa detik (diukur 1–5 detik). Dilakukan dua kali per run (sebelum POST dan ulang di dalam transaksi load). Hasilnya cuma
  beberapa ribu `order_id` di memori.
- **Header + details dalam 1 transaksi:** kalau header berhasil tapi details gagal, keduanya di-rollback, jadi tidak ada order "sudah ada" tanpa details.
- **Transform** disalin dari `iseller_mitra_backfilling/pipelines/transform.py`. **Kalau transform diubah, ubah juga di file pasangannya**
  (`iseller_mitra_fulfillment`, `iseller_mitra_backfilling`).
- Order yang saat ditarik belum punya transaksi tidak dimasukkan; tertarik lagi di run berikutnya (oleh backfilling atau fulfillment).

## Sistem Eksternal

- **iSeller Mitra REST API** — `https://janjijiwamitra.isellershop.com/api/v2/{getstoreinfo,GetProducts,GetOrders}` (Bearer token).
- **MySQL Data Lake** (`config_dw.db_resource`) — `transactions_iseller_mitra` dan `transactions_items_iseller_mitra` (append).
- **Email (Gmail SMTP)** — password dari env `GMAIL_SMTP_PASSWORD`.
- **MS Teams via Power Automate** — `teams_report_iseller_mitra_ingestion.py` (completed / partial / stopped), best-effort.

## Connections / Variables

- Airflow Variable `access_token_mitra` (**expired berkala**; Variable didahulukan atas env var).
- Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL`; password SMTP dari env `GMAIL_SMTP_PASSWORD`.

## Manual Run

- **Airflow UI → Trigger DAG w/ config**: isi `start_date`/`end_date`; centang `dry_run` untuk uji tanpa menulis.
- Notebook asal `transaction_janji_jiwa_mitra_v2.ipynb` ada di folder yang sama (referensi; token dan password di notebook jangan di-commit).
