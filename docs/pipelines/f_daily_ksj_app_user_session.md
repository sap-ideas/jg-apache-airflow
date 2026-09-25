# `f_daily_ksj_app_user_session` — Sesi User App Harian (JiwaPlus + KSJ)

Menggabungkan metrik sesi user dari 2 aplikasi konsumen berbeda (JiwaPlus dan
KSJ) jadi satu tabel summary harian.

## Metadata DAG

File: `dag_etl_f_daily_ksj_app_user_session.py`

| Field | Nilai |
|---|---|
| `dag_id` | `f_daily_ksj_app_user_session` |
| `schedule_interval` | `"0 6 * * *"` — harian 06:00 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "ksj", "consumer-app", "user-session", "jiwaplus", "dwh", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |

**Date window**: `target_date` = "kemarin" di Asia/Jakarta (`_target_date_yesterday_jakarta`, anchor dari `data_interval_end`/`logical_date`).

Catatan struktur: DAG ini **tidak** punya `load.py` terpisah — logic load ditulis inline di `pipelines/etl_orchestrator.py`.

## Alur Pipeline (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py)
  ├─ get_jiwaplus_consumer_app_user_session() — JiwaPlus Postgres (jiwaplus_db_conn):
  │    query outlet_user_distances, hitung distinct session id & distinct user
  │    per tanggal (di-shift +7 jam untuk Jakarta time)
  └─ get_ksj_consumer_app_user_session() — KSJ Link Postgres d_jiwa_ksj:
       join ksj_rider_found_logs (rider found/not-found count, rider_found_perc)
       dengan ksj_user_click_tracker (klik arah/WhatsApp, click_ratio_to_found_rider),
       keyed by tanggal

TRANSFORM (transform.py :: build_daily_session_combined)
  Outer-merge kedua DataFrame on 'date', cast kolom numerik via pd.to_numeric
  (coerce errors — perlu karena Postgres NUMERIC pulang sebagai Decimal/str yang
  tidak diterima langsung oleh MySQL), tambah tag load_data_by='NEXUS_AIRFLOW'

LOAD (_overwrite_for_date, inline di etl_orchestrator.py)
  Satu transaksi SQLAlchemy (engine.begin()) ke MySQL DWH:
  DELETE FROM daily_ksj_consumer_app_user_session WHERE date = :d
  → to_sql(if_exists='append', method='multi', dtype=DTYPE_MAP)
  DTYPE_MAP eksplisit pin tipe kolom (BigInteger, DECIMAL(20,10), VARCHAR(100), DATE)

NOTIFIKASI (_send_report_email)
  Email HTML via Gmail SMTP, tabel statistik row count/timing.
  Kegagalan kirim email hanya di-logging.warning — tidak menggagalkan task.
```

## Sistem Eksternal

- **JiwaPlus PostgreSQL** — `outlet_user_distances` (source metrik sesi consumer app).
- **KSJ Link PostgreSQL** (`d_jiwa_ksj`) — `ksj_rider_found_logs`, `ksj_user_click_tracker`.
- **MySQL Data Warehouse** — target `daily_ksj_consumer_app_user_session`.
- **Email (Gmail SMTP)** — kredensial hardcoded, sama pola DAG lain.
- Tidak memakai Teams/Power Automate — meski modul `plugins/common/teams_powerautomate_report.py` tersedia, DAG ini tidak mengimpornya.

## Connections / Variables

- Tidak ada Airflow Connection/Variable native. Config di-load dinamis via `importlib.util` dari `config_db_datawarehouse.py` dan `config_db_ksj_link.py`.
- Koneksi via `plugins/common/db_helpers.py`: `get_engine` (MySQL DWH), `jiwaplus_db_conn`, `ksj_link_db_conn`.
- Kredensial SMTP (Gmail) hardcoded inline di `etl_orchestrator.py`.

## Pola Kode Khas

- Struktur 3-langkah: `extract.py` → `transform.py` → load+email inline di `etl_orchestrator.py` (berbeda dari DAG lain yang punya `load.py` terpisah).
- Docstring module-level di puncak `etl_orchestrator.py` dan DAG file menjelaskan flow secara ringkas ("Flow:" summary).
- Print-based progress logging dengan banner `"[N/3] STEP_NAME"` dan timer elapsed via `time.time()`.
- Try/except membungkus seluruh `run_all`; kalau exception, print + kirim email FAILED (merah), lalu di-raise ulang supaya Airflow tetap menandai gagal dan retry sesuai `default_args`.
