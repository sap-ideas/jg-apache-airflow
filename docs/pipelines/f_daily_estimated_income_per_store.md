# `f_daily_estimated_income_per_store` — Estimasi Uang Masuk Harian Per Store (untuk Landlord)

Menghitung estimasi uang masuk harian per outlet dari transaksi iSeller Pusat,
dipecah per payment channel dan dikurangi commission cost masing-masing channel,
untuk keperluan pelaporan ke landlord.

## Metadata DAG

File: `dag_etl_f_daily_estimated_income_per_store.py`

| Field | Nilai |
|---|---|
| `dag_id` | `f_daily_estimated_income_per_store` |
| `schedule_interval` | `"0 10 * * *"` — harian 10:00 Asia/Jakarta (setelah `etl_iseller_pusat_dwh` @ 09:00 refresh source table) |
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
  map_channel(): payment_type mentah -> channel tetap (PAYMENT_TYPE_TO_CHANNEL),
    fallback 'null_payment' (payment_type NULL) / 'other' (tidak dikenal)
  Aggregate ulang per date+outlet+channel, lalu pivot jadi wide: 1 baris per
  date+outlet, kolom per channel (<channel>, <channel>_subtotal,
  <channel>_commission = subtotal * COMMISSION_PCT[channel])
  total_commission_cost = jumlah semua *_commission

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
- `OUTLET_CODES` (extract.py) dan rules komisi (`PAYMENT_TYPE_TO_CHANNEL`, `COMMISSION_PCT`, `CHANNEL_ORDER` di transform.py) sengaja dipisah dari SQL supaya gampang diubah tanpa nyentuh query.
- Print-based progress logging dengan banner `"[N/3] STEP_NAME"` dan timer elapsed via `time.time()`.
- Try/except membungkus seluruh `run_all`; kalau exception, print + kirim email FAILED (merah), lalu di-raise ulang supaya Airflow tetap menandai gagal dan retry sesuai `default_args`.

## Manual Run

Ada notebook `dags/f_daily_estimated_income_per_store/daily_est_income_per_store_for_landlord.ipynb` sebagai precursor/dev notebook (support custom date range lewat `USE_CUSTOM_DATE_RANGE`).
