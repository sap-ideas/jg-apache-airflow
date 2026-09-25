# `sjw_riders_mapping` — Mapping Rider KSJ Link

Folder ini berisi **2 DAG** yang saling terhubung lewat Airflow **Dataset**
(bukan `TriggerDagRunOperator` atau cron terpisah) — DAG kedua otomatis jalan
begitu DAG pertama selesai update data.

> Folder `dags/sjw_riders_mapping/__pycache__/` menyimpan bytecode sisa modul lama
> (`dag_riders_hubs_mapping.py`, `extract_gsheets_hubs_mapping.py`, `extract_missing_riders.py`,
> `postgres_helpers.py`, `transform.py`) yang sumbernya sudah tidak ada — jejak refactor dari
> sumber data Google Sheets ke KSJ Link PostgreSQL. Bukan bagian dari DAG aktif.

## DAG 1: `dag_riders_ksj_link_mapping.py`

| Field | Nilai |
|---|---|
| `dag_id` | `etl_sejutajiwa_riders_ksj_link_mapping` |
| `schedule_interval` | `"0 8 * * *"` — harian 08:00 Asia/Jakarta |
| `start_date` | `datetime(2024, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["dimension", "ksj-link", "master-data", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task flow | `run_pipeline >> send_reports_task` (2 task, beda dari kebanyakan DAG lain yang cuma 1 task) |

`send_reports_task` punya `trigger_rule="all_success"` — hanya jalan kalau `run_pipeline` sukses.

### Alur

```
EXTRACT (extract_missing_ksj_link_riders.py, tiap panggilan dibungkus retry_with_backoff)
  ├─ get_missing_ksj_link_riders() — KSJ Link Postgres d_transaction:
  │    transaction_status='COMPLETED', jilid IS NULL, LEFT JOIN d_sejutajiwa_riders_hubs_mapping,
  │    window 10 hari terakhir → daftar rider yang belum ter-mapping ke hub
  ├─ get_rider_list()  — KSJ Link Postgres d_rider: SELECT * FROM riders (full list)
  └─ get_hub_id()      — MySQL Data Lake: SELECT DISTINCT jilid, hub_id, hub_name FROM sejutajiwa_riders_hubs_mapping

TRANSFORM (transform_ksj_link.py :: transform_ksj_link())
  ├─ Filter rider_list ke rider yang missing saja
  ├─ Left-merge dengan hub mapping on hub_code == hub_id
  ├─ Special-case hub HB-TESTING
  ├─ Derive rider_id/cart_id/rider_name/status (ACTIVE/INACTIVE dari is_active)/scheme='COMMISSION'
  ├─ Bersihkan dummy phone 628123456789 → NA
  ├─ Synthesize cashier_id = ksj_link.<rider_code_lower>@hotmail.com
  ├─ updated_by = "NEXUS_AIRFLOW"
  └─ Dedup on (hub_id, rider_id)

LOAD (load.py :: append_only_ignore_duplicates()) — TRIPLE DESTINATION:
  1. MySQL Data Lake      (data_lake_jiwa.sejutajiwa_riders_hubs_mapping)      — INSERT IGNORE
  2. MySQL Data Warehouse (data_warehouse_jiwa.sejutajiwa_riders_hubs_mapping)— INSERT IGNORE
  3. (via DAG 2, terpicu Dataset) PostgreSQL KSJ Link                         — ON CONFLICT DO NOTHING

NOTIFIKASI (send_teams_and_email_notification)
  ├─ fetch_riders_mapping_summary() — SQL di sql/riders_mapping_teams_summary.sql
  ├─ Teams Adaptive Card via resolve_power_automate_webhook_url() (skip kalau tidak di-set)
  └─ Email HTML — SELALU dikirim, terlepas dari webhook Teams ada atau tidak
```

Kolom yang di-insert: `hub_id, jilid, hub_name, rider_id, cart_id, rider_name, status, scheme, phone_number, cashier_id, updated_by`.

### Cross-DAG trigger via Dataset

`run_pipeline` mendeklarasikan `outlets=[DATASET_MYSQL_UPDATED]` dengan
`DATASET_MYSQL_UPDATED = Dataset("sejutajiwa_riders_hubs_mapping://mysql_updated")`.
Setiap kali task ini sukses, Airflow otomatis men-trigger DAG 2 di bawah — **tidak** ada
`TriggerDagRunOperator` eksplisit, murni fitur Dataset-scheduling Airflow 2.4+.

## DAG 2: `dag_sync_postgresql.py`

| Field | Nilai |
|---|---|
| `dag_id` | `sync_sejutajiwa_riders_hubs_to_postgresql` |
| `schedule` | `[DATASET_MYSQL_UPDATED]` — **bukan cron**, dipicu Dataset dari DAG 1 |
| `start_date` | `datetime(2024, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["sync", "postgresql", "master-data", "production"]` |
| `default_args` | sama seperti DAG 1 |
| Task | 1x `PythonOperator("sync_to_postgresql")`, tanpa downstream task |

### Alur (`load_postgresql.py :: sync_mysql_to_postgresql()`)

File ini menggabungkan read+load jadi satu (tidak ada extract/transform terpisah — full-table sync):

```
READ  — SELECT DISTINCT hub_id, jilid, hub_name, rider_id, cart_id, rider_name,
         status, scheme, phone_number, cashier_id FROM sejutajiwa_riders_hubs_mapping
         (MySQL Data Lake)
ENRICH— tambah load_data_at (timestamp sekarang), load_data_by='NEXUS_AIRFLOW'
LOAD  — untuk MASING-MASING dari 2 target PostgreSQL (retry independen per target):
         ├─ pg_config.d_transaction: TRUNCATE d_sejutajiwa_riders_hubs_mapping → INSERT (append)
         └─ pg_config.d_location:    TRUNCATE d_sejutajiwa_riders_hubs_mapping → INSERT (append)
```

`d_location` pakai host database terpisah (`pg_config.hostname_location`), mencerminkan topologi cluster KSJ Link yang di-split (lihat `config_db_ksj_link.py`). Idempoten by design (full TRUNCATE+INSERT, bukan delete-by-date).

## Modul Bersama di Folder Ini

| File | Peran |
|---|---|
| `extract_missing_ksj_link_riders.py` | Extract — 3 fungsi, koneksi psycopg2/pymysql dibuka-tutup per panggilan (tanpa pooling/engine reuse) |
| `transform_ksj_link.py` | Transform — 1 fungsi, pure pandas |
| `load.py` + `load_helpers.py` | Load MySQL append-only (`INSERT IGNORE`) + util `convert_dataframe_to_records` (konversi tipe numpy→native) dan `log_insert_summary` (ASCII box logging) |
| `load_postgresql.py` | Load PostgreSQL full-table sync (TRUNCATE+INSERT) |
| `retry_helpers.py` | `retry_with_backoff(func, max_retries=3, base_delay=5, description=...)` — dipakai untuk membungkus hampir semua panggilan I/O ke DB di kedua DAG |
| `teams_report_riders_mapping.py` | Notifikasi khusus DAG 1: Teams Adaptive Card + email HTML (SMTP Gmail STARTTLS) |
| `sql/riders_mapping_teams_summary.sql` | Query join `sejutajiwa_riders_hubs_mapping` + `outlet_mapping_directory` on `jilid`, filter hari ini, group by `sejuta_jiwa_type` |

## Sistem Eksternal

- **KSJ Link PostgreSQL** — `d_transaction`, `d_rider` (source DAG 1); `d_transaction`, `d_location` (target DAG 2, cluster terpisah untuk `d_location`).
- **MySQL Data Lake & Data Warehouse** — `sejutajiwa_riders_hubs_mapping` (target DAG 1, source DAG 2).
- **MS Teams via Power Automate** & **Gmail SMTP** — hanya di DAG 1.

## Connections / Variables

- Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` (fallback env var) — dipakai DAG 1.
- Tidak ada Airflow Connection — kredensial dari `config/config_db_datawarehouse.py` dan `config_db_ksj_link.py`.
- Kredensial SMTP hardcoded di `teams_report_riders_mapping.py` (sender `saputra.christabel20@gmail.com`).

## Pola Kode Khas

- Logging emoji-prefixed (`🚀 📥 🆕 📋 🏢 ⚙️ ✅ 📤`) menandai fase ETL — sama gayanya dengan `maintenance/log_cleanup_dag.py`.
- Komentar eksplisit `# PRODUCTION!` di dekat nama tabel — gaya peringatan inline soal tabel produksi.
- Timestamp Jakarta pakai `pendulum.now("Asia/Jakarta")` dengan suffix manual `" WIB"` — komentar menjelaskan ini untuk menghindari klien app salah interpretasi string ISO8601 sebagai UTC.
- `retry_with_backoff` generic dipakai konsisten di kedua DAG, granularitas per-database (misal DAG 2 retry `d_transaction` dan `d_location` secara independen, supaya kegagalan transient di satu target tidak memaksa baca ulang MySQL / truncate ulang target lain).
