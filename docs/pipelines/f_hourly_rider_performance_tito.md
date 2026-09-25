# `f_hourly_rider_performance_tito` — Performa Rider per Jam (TI-TO)

Menghitung performa transaksi rider per jam, direkonsiliasi dengan jadwal
check-in/check-out ("TI-TO" = Time-In Time-Out) mereka.

## Metadata DAG

File: `dag_etl_f_hourly_rider_performance_tito.py`

| Field | Nilai |
|---|---|
| `dag_id` | `f_hourly_rider_performance_tito` |
| `schedule_interval` | `"0 8 * * *"` — harian 08:00 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "riders", "performance", "tito", "ksj-link", "dwh", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |

**Date window**: `target_date` = "kemarin" di Asia/Jakarta (`_target_date_yesterday_jakarta`).

## Alur Pipeline (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py)
  ├─ get_rider_hourly_transactions() — KSJ Link Postgres d_transaction:
  │    transactions + transaction_products JOIN, status COMPLETED,
  │    exclude rider JW000065, group by rider/date/week/hour/payment_type
  │    (total orders/qty/sales). UTC→WIB via + INTERVAL '7 hours'.
  ├─ get_rider_timeslot_tito() — KSJ Link Postgres d_jiwa_ksj:
  │    rider_transfer_log (check-in/out), expand tiap shift jadi slot per jam
  │    via generate_series()
  └─ get_rider_mapping() — MySQL DWH db_resource:
       sejutajiwa_riders_hubs_mapping JOIN outlet_mapping_custom
       → hub_name/region/provinsi/kota/sejuta_jiwa_type

TRANSFORM (transform.py :: build_hourly_performance)
  Filter ke rider tipe HUB saja, lalu selesaikan 3 skenario data quality:
  ├─ Case 1: jam transaksi di luar rentang shift TI-TO
  │          → snap ke slot_min/slot_max terdekat, re-aggregate
  ├─ Case 2: ada transaksi tapi TIDAK ADA slot TI-TO sama sekali
  │          → synthesize slot jam dari min/max jam transaksi
  └─ Case 3: ada slot TI-TO tapi TIDAK ADA transaksi hari itu
             → exclude rider+tanggal tsb dari output
  Lalu merge timeslot base + transaksi yang sudah difix + rider mapping,
  bersihkan whitespace/NaN, tambah load_data_by='NEXUS_AIRFLOW'.

LOAD (_overwrite_for_date)
  DELETE FROM hourly_sejutajiwa_rider_performance WHERE date = :d
  → to_sql batched (BATCH_SIZE=50_000) dengan DTYPE_MAP eksplisit
  (MySQL DWH, via common.db_helpers.get_engine)

NOTIFIKASI (_send_report_email)
  Email HTML via Gmail SMTP → athens.jiwagroup@gmail.com, lahia.ardhanlahia@gmail.com
  (kegagalan kirim email hanya di-logging.warning, tidak menggagalkan task)
```

Modul `transform.py` mendokumentasikan 3 skenario data-quality di atas langsung
di docstring-nya — "documentation-as-spec" style yang khas di DAG ini.

## Sistem Eksternal

- **KSJ Link PostgreSQL** — `d_transaction` (transactions, transaction_products), `d_jiwa_ksj` (rider_transfer_log — data check-in/out).
- **MySQL Data Warehouse** — source rider mapping (`sejutajiwa_riders_hubs_mapping`, `outlet_mapping_custom`), target `hourly_sejutajiwa_rider_performance`.
- **Email (Gmail SMTP)** — kredensial hardcoded, sama pola dengan DAG lain.
- Tidak memakai Teams/Power Automate.

## Connections / Variables

- Tidak ada Airflow Connection/Variable native yang dipakai. Config di-load dinamis dari `/opt/airflow/config/config_db_datawarehouse.py` dan `config_db_ksj_link.py` via `importlib.util`.
- Koneksi lewat `plugins/common/db_helpers.py`: `get_engine` (MySQL DWH), `ksj_link_db_conn`/`ksj_link_location_db_conn` (Postgres, 2 cluster host berbeda).

## Pola Kode Khas

- Query SQL pakai `.format()` string interpolation untuk rentang tanggal (`extract.py`) — bukan parameterized, meski input selalu berasal dari konteks Airflow (bukan input user).
- Desain idempotensi (DELETE+INSERT per tanggal) didokumentasikan eksplisit di docstring & komentar sebagai alasan kenapa `retries=2` "aman" untuk pipeline ini.
- Print-based progress logging dengan format `f"{len(df):>6} rows ({elapsed:.1f}s)"`; `logging.warning` hanya dipakai untuk fallback kegagalan email.
