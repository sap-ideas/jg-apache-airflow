# `f_rider_mangkal_summary` — Ringkasan Sesi Mangkal Rider

Mendeteksi sesi "mangkal" (loitering/parkir) rider berdasarkan log lokasi, lalu
gabungkan dengan transaksi & jam kerja. Berjalan **3x sehari**, berbeda dari
kebanyakan DAG lain yang cuma sekali.

## Metadata DAG

File: `dag_etl_f_rider_mangkal_summary.py`

| Field | Nilai |
|---|---|
| `dag_id` | `f_rider_mangkal_summary` |
| `schedule_interval` | `"0 8,11,15 * * *"` — 08:00, 11:00, 15:00 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "riders", "mangkal", "ksj-link", "dwh", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |

**Date window**: **tidak** ada parameter tanggal eksplisit dari Airflow — `run_pipeline` cuma memanggil `run_all(CONFIG_KSJ, CONFIG_DW)`. Semua scoping "hari ini" dilakukan **di dalam SQL** (`(NOW() + INTERVAL '7 hours')::date = ...` atau `AT TIME ZONE 'Asia/Jakarta'`), karena DAG ini jalan 3x/hari dan tiap run perlu melihat data hari berjalan, bukan H-1.

`batch_label` (`'08AM'` / `'11AM'` / `'03PM'`) diturunkan otomatis dari jam wall-clock Jakarta saat run — mencerminkan jadwal cron di atas.

## Alur Pipeline (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py, semua scoped ke "hari ini" Asia/Jakarta di dalam SQL)
  ├─ get_rider_total_working_time() — KSJ Link Postgres d_jiwa_ksj:
  │    rider_transfer_log JOIN rider_reverse_request (status=APPROVED saja)
  │    → total_working_minutes/hour, total_reverse_process_admin
  ├─ get_rider_stationed_logs() — KSJ Link Postgres d_location (cluster TERPISAH,
  │    via ksj_link_location_db_conn): event lat/long + flag is_stationed
  ├─ get_raw_transactions() — KSJ Link Postgres d_transaction:
  │    transaksi COMPLETED hari ini
  ├─ get_rider_profile_clicks() — KSJ Link Postgres d_jiwa_ksj, ksj_user_click_tracker:
  │    jumlah view profil rider harian
  │    ⚠️ catatan penting: kolom timestamp di sumber ini SUDAH WIB — TIDAK
  │       boleh ditambah offset +7 jam lagi (beda dari tabel lain)
  └─ get_hub_mapping() — KSJ Link Postgres d_transaction:
       d_sejutajiwa_riders_hubs_mapping JOIN d_outlet_mapping_custom

TRANSFORM (transform.py)
  ├─ create_mangkal_session() — group event is_stationed=True yang berurutan
  │    jadi sesi diskrit pakai pola shift+cumsum (vectorized pandas, bukan loop),
  │    hitung start/end/duration tiap sesi (end = event False terakhir dalam sesi,
  │    kalau tidak ada pakai event terakhir)
  └─ build_final_summary() — aggregate durasi sesi per rider/hari, join dengan
       transaksi harian, working time, hub mapping, profile clicks; hitung
       mangkal_mins_pct (% waktu mangkal terhadap waktu kerja); fillna numerik=0,
       string=0; tambah batch_label & load_data_by

LOAD — DUA tulisan atomik terpisah (MySQL DWH via common.db_helpers.get_engine):
  ├─ _idempotent_insert_logs() → daily_rider_mangkal_session_summary_logs
  │    (tabel AUDIT/HISTORY, PK (operating_date, rider_code, batch_label)):
  │    DELETE WHERE (operating_date, batch_label) match → INSERT
  │    → aman kalau Airflow retry re-insert batch yang sama
  └─ _overwrite_for_dates() → daily_rider_mangkal_session_summary
       (tabel LATEST-SNAPSHOT, tanpa kolom batch_label):
       DELETE WHERE operating_date IN (...) → INSERT
       → hanya simpan hasil batch TERBARU dari 3 batch hari itu

NOTIFIKASI (_send_report_email)
  Email HTML via Gmail SMTP, sama pola/penerima seperti DAG performa TITO
```

## Sistem Eksternal

- **KSJ Link PostgreSQL** — `d_jiwa_ksj` (rider_transfer_log, rider_reverse_request, ksj_user_click_tracker), `d_location` (**cluster terpisah**, stationed logs), `d_transaction` (transactions, hub mapping).
- **MySQL Data Warehouse** — 2 tabel target: `daily_rider_mangkal_session_summary_logs` (audit) dan `daily_rider_mangkal_session_summary` (snapshot terbaru).
- **Email (Gmail SMTP)** — kredensial hardcoded, sama pola DAG lain.
- Tidak memakai Teams/Power Automate.

## Connections / Variables

- Tidak ada Airflow Connection/Variable native. Config di-load dinamis via `importlib.util` dari `config_db_datawarehouse.py` dan `config_db_ksj_link.py`.
- Koneksi via `plugins/common/db_helpers.py`: `get_engine` (load), `ksj_link_db_conn` & `ksj_link_location_db_conn` (extract, 2 cluster host KSJ Link berbeda — utama vs location-service).

## Pola Kode Khas

- Pola dual-table load (audit-log + latest-snapshot) — **tidak** ada di DAG TITO, khas DAG ini karena berjalan 3x/hari dan perlu tetap punya riwayat tiap batch selain snapshot terbaru.
- Komentar defensif eksplisit soal "gotcha" data sumber (mis. peringatan jangan double-apply offset WIB di `get_rider_profile_clicks`).
- `create_mangkal_session` pakai teknik pandas vektorisasi (`groupby` + `shift` + `cumsum`) untuk deteksi batas sesi — lebih advanced dibanding transform di DAG TITO.
- SQL di sini tidak pakai `.format()` interpolation eksternal karena semua sudah "hari ini"-scoped langsung di SQL — beda dari pola `.format()` yang dipakai di DAG TITO.
