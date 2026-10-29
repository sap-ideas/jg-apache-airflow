# `f_daily_estimated_income_per_store` — Estimasi Uang Masuk Harian Per Store (untuk Landlord)

Menghitung estimasi uang masuk harian per outlet dari transaksi iSeller Pusat,
dipecah per payment type (satu baris per date + outlet + payment_type) dengan
commission masing-masing payment type, untuk keperluan pelaporan ke landlord.
Nominal yang dibagi ke landlord (`subtotal - sales_type_commission`) tidak disimpan,
dihitung saat dipakai (mis. oleh `api_landlord_revenue_sharing_00570`).

## Metadata DAG

File: `dag_etl_f_daily_estimated_income_per_store.py`

| Field | Nilai |
|---|---|
| `dag_id` | `f_daily_estimated_income_per_store` |
| `schedule_interval` | `"0 10 * * *"` — harian 10:00 Asia/Jakarta (setelah `etl_iseller_pusat_fulfillment` @ 09:00 refresh source table) |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "landlord", "income", "iseller", "dwh", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |

**Date window**: `target_date` = "kemarin" di Asia/Jakarta (`_target_date_yesterday_jakarta`, anchor dari `data_interval_end`/`logical_date`) — hanya 1 hari per run, bukan rolling range.

## Alur Pipeline (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py :: extract_transactions)
  MySQL Data Lake — transactions_iseller_pusat, filter outlet_code IN OUTLET_CODES
  ('00570', '00989'), fulfillment_status='fulfilled', status='completed',
  payment_status='paid', transactions_status='success'; aggregate
  total_amount/total_tax_amount/subtotal per date+outlet+payment_type

TRANSFORM (transform.py :: build_daily_income_summary)
  map_payment_type(): payment_type mentah -> payment type resmi (PAYMENT_TYPE_MAP,
    tidak sensitif huruf besar/kecil), NULL -> 'Unknown'; yang belum dikenal tetap
    disimpan dengan nama aslinya & komisi 0 (total per outlet-hari selalu cocok dengan sumber)
  Aggregate per date+outlet+payment_type (format long, 1 baris per payment type):
    subtotal, tax_amount (SUM total_tax_amount apa adanya dari iSeller), total_sales,
    sales_type_commission = subtotal * COMMISSION_PCT[payment_type] (tidak dibulatkan di kode;
      kolom DECIMAL(20,4) membatasi 4 desimal. Amount dibulatkan 4 desimal saat kirim ke landlord)
  find_unmapped_payment_types(): payment type tanpa komisi resmi -> warning di log & email
  Payment type baru = tambah entry di PAYMENT_TYPE_MAP & COMMISSION_PCT (tanpa kolom baru)

LOAD (load.py :: load_daily_income)
  Atomic DELETE + INSERT (engine.begin()) ke MySQL DWH:
  DELETE FROM daily_est_income_per_store_for_landlord WHERE date = :d
  → to_sql(if_exists='append')

NOTIFIKASI (_send_report_email, inline di etl_orchestrator.py)
  Email HTML via Gmail SMTP, tabel statistik row count/timing.
  Kegagalan kirim email hanya di-logging.warning — tidak menggagalkan task.
```

## Sistem Eksternal

- **MySQL Data Lake** — source `transactions_iseller_pusat`.
- **MySQL Data Warehouse** — target `daily_est_income_per_store_for_landlord`.
- **Email (Gmail SMTP)** — kredensial hardcoded, sama pola DAG lain.
- Tidak memakai Teams/Power Automate.

## Connections / Variables

- Tidak ada Airflow Connection/Variable native. Config di-load dinamis via `importlib.util` dari `config_db_datawarehouse.py`.
- Koneksi via `plugins/common/db_helpers.py`: `get_connection` (raw MySQL, extract dari `config_dw.db_resource`), `get_engine` (SQLAlchemy, load ke `config_dw.db_target`).
- Kredensial SMTP (Gmail) hardcoded inline di `etl_orchestrator.py`, password dari `GMAIL_SMTP_PASSWORD`.

## Pola Kode Khas

- Struktur 4-langkah penuh: `extract.py` → `transform.py` → `load.py` → email inline di `etl_orchestrator.py`.
- `OUTLET_CODES` (extract.py) dan rules komisi (`PAYMENT_TYPE_MAP`, `COMMISSION_PCT` di transform.py) sengaja dipisah dari SQL supaya gampang diubah tanpa nyentuh query.
- Print-based progress logging dengan banner `"[N/3] STEP_NAME"` dan timer elapsed via `time.time()`.
- Try/except membungkus seluruh `run_all`; kalau exception, print + kirim email FAILED (merah), lalu di-raise ulang supaya Airflow tetap menandai gagal dan retry sesuai `default_args`.

## Manual Run

Ada notebook `dags/f_daily_estimated_income_per_store/daily_est_income_per_store_for_landlord.ipynb` sebagai precursor/dev notebook (support custom date range lewat `USE_CUSTOM_DATE_RANGE`; DELETE range lalu INSERT dalam satu transaksi, `load_data_by` = email operator). Dipakai juga untuk backfill histori setelah cutover ke format long.

Migrasi dari format wide lama ke long: `sql/migrate_wide_to_long.sql` (dijalankan manual di DBeaver: rename tabel lama jadi `_old`, buat tabel baru, backfill via notebook, query rekonsiliasi).
