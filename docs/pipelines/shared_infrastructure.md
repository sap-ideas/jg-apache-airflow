# Shared Infrastructure — `plugins/common/` & `config/`

Modul-modul ini dipakai bersama oleh (hampir) semua DAG di project. Di-mount ke
container Airflow lewat `docker-compose.yaml` (`PYTHONPATH` mencakup
`/opt/airflow/dags:/opt/airflow/config:/opt/airflow/plugins`), sehingga DAG bisa
`import` langsung sebagai `common.db_helpers`, `common.teams_powerautomate_report`,
dsb — bukan `plugins.common...`.

## `plugins/common/db_helpers.py`

Satu sumber kebenaran untuk semua koneksi database di project.

### MySQL (Data Lake & Data Warehouse) — dipasangkan dengan `config_db_datawarehouse.py`

| Fungsi | Fungsi |
|---|---|
| `_mysql_tcp_host(hostname)` | Resolve hostname ke literal IPv4 lewat `socket.getaddrinfo` sebelum connect — menghindari masalah DNS Docker yang flaky. |
| `_is_transient_mysql_network_err(exc)` | Klasifikasi error koneksi yang bersifat transient (errno `-3`, `2003`, `2005`, "name resolution", "gaierror", dsb). |
| `get_connection(config_dw, db_name, max_retries=8, base_delay_sec=2.0)` | Koneksi raw `mysql.connector`, retry dengan exponential backoff (`base_delay_sec * 2**attempt`) **hanya** untuk error transient di atas. |
| `_build_mysql_uri(config_dw, db_name)` | Bangun URI `mysql+pymysql://user:pw@host/db?charset=utf8mb4`. |
| `get_engine(config_dw)` | SQLAlchemy engine untuk **Data Warehouse** (`config_dw.db_target`), `pool_pre_ping=True`, `pool_recycle=3600`. |
| `get_engine_datalake(config_dw)` | SQLAlchemy engine untuk **Data Lake** (`config_dw.db_resource`). |

### PostgreSQL (KSJ Link & JiwaPlus)

| Fungsi | Fungsi |
|---|---|
| `ksj_link_db_conn(config_ksj, db_name)` | `psycopg2.connect` ke database KSJ Link (`d_transaction`, `d_rider`, dll) di host utama (`config_ksj.hostname`). |
| `ksj_link_location_db_conn(config_ksj, db_name)` | Sama, tapi ke cluster **terpisah** khusus service lokasi (`config_ksj.hostname_location`) — dipakai untuk database `d_location`. |
| `jiwaplus_db_conn(config_jiwaplus)` | `psycopg2.connect` ke database JiwaPlus (consumer app). |

### Utilitas audit redump

| Fungsi | Fungsi |
|---|---|
| `get_summary_before(config_dw, table_name, sum_columns, start_date, end_date, sources=None)` | `SELECT COALESCE(SUM(col),0)... FROM table WHERE date BETWEEN ... [AND sources=...]`, dipakai untuk snapshot nilai sebelum delete. |
| `print_before_after(before, after)` | Cetak tabel perbandingan BEFORE/AFTER/selisih ke stdout — dipakai di hampir semua pipeline redump untuk audit console + email report. |

> Catatan: `get_summary_before` membangun SQL lewat f-string interpolation (bukan parameterized query) — risiko rendah karena input selalu berasal dari tanggal/parameter internal Airflow, bukan input eksternal, tapi pola ini konsisten dipakai di banyak tempat lain juga.

## `plugins/common/teams_powerautomate_report.py`

Helper untuk mengirim notifikasi **Adaptive Card** ke Microsoft Teams lewat webhook
**Power Automate**. Modul ini sengaja dibuat generic — SQL/konten spesifik per-DAG
harus ditulis di modul lokal DAG masing-masing (`dags/<dag>/teams_report_*.py`), bukan di sini.

Fungsi utama:

- `resolve_power_automate_webhook_url()` — resolusi URL webhook: (1) Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL`, fallback (2) environment variable dengan nama sama. Return `None` kalau keduanya tidak ada (dibungkus try/except agar tetap jalan di luar konteks Airflow).
- `require_power_automate_webhook_url()` — sama, tapi raise `ValueError` kalau kosong.
- `text_block(...)`, `column_set(...)`, `column(...)` — builder elemen Adaptive Card level rendah (TextBlock/ColumnSet/Column).
- `section_column_table(*, section_title, column_headers, ..., rows, empty_message)` — builder tabel section (header + baris data), menampilkan `empty_message` kalau `rows` kosong.
- `build_etl_report_adaptive_card(*, report_title, report_date, dag_id, run_id, executed_at_iso, status, section_blocks)` — rakit card lengkap: title, tanggal, `FactSet` (DAG/Run ID/Executed at/Status), lalu section spesifik DAG.
- `build_power_automate_payload(adaptive_card)` — bungkus jadi `{"adaptiveCard": adaptive_card}`, sesuai bentuk yang diharapkan Power Automate.
- `post_to_power_automate(webhook_url, payload, timeout_sec=30.0)` — `requests.post(...)` + `raise_for_status()`.

**Konvensi pemakaian di tiap DAG:**
- 1 Airflow Variable global untuk semua DAG: `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` — **tidak boleh** di-hardcode di kode DAG.
- Task terakhir DAG biasanya `PythonOperator` dengan `trigger_rule="all_success"` yang memanggil fungsi lokal `send_*_teams_report(...)` — jadi notifikasi Teams saat ini terkirim pada **sukses**, bukan pada gagal (kecuali modul lokal DAG menambahkan logic sendiri untuk kasus gagal, seperti di `iseller_pusat_dwh`).
- Pemanggilan `post_to_power_automate` selalu dibungkus try/except best-effort di level DAG (mis. `post_teams_safe` di `iseller_pusat_dwh/teams_report_iseller_pusat_unfulfilled.py`) — kegagalan Teams tidak pernah menggagalkan pipeline.

## `config/config_db_*.py`

Semua tiga modul mengikuti pola sama: kelas `Config` yang membaca `os.getenv(VAR, 'default_hardcoded')`, lalu re-export sebagai variabel module-level "for backward compatibility".

| Modul | Target | Variabel penting |
|---|---|---|
| `config_db_datawarehouse.py` | MySQL **Data Lake** + **Data Warehouse** (host sama, nama DB beda) | `hostname_lake`/`hostname` (env `DB_LAKE_HOST`/`DB_WAREHOUSE_HOST`, default `dblake.jiwa.prod`), `username`/`username_lake`, `password`/`password_lake`, `db_lake`/`db_resource` (`DB_LAKE_NAME`, default `data_lake_jiwa`), `db_target` (`DB_WAREHOUSE_NAME`, default `data_warehouse_jiwa`) |
| `config_db_ksj_link.py` | PostgreSQL **KSJ Link** (2 cluster) | `hostname` (env `KSJ_LINK_HOST`, default `db-ksj.jiwa.prod`), `hostname_location` (env `KSJ_LINK_HOST_LOCATION`, cluster terpisah khusus `d_location`), `username`/`password`/`port`, plus nama-per-database: `d_transaction`, `d_location`, `d_payment`, `d_rider`, `d_cms`, `d_jiwa_ksj` (ada guard self-correcting untuk salah konfigurasi lama di mana `KSJ_LINK_DB_JIWA_KSJ` ke-set sebagai literal string `"d_jiwa_ksj"` alih-alih nama DB asli `"jiwa-ksj"`) |
| `config_db_jiwaplus.py` | PostgreSQL **JiwaPlus** (consumer app) | `hostname` (env `JIWAPLUS_HOST`, default `db.jiwa.prod`), `database` (env `JIWAPLUS_DATABASE`, default `jiwaplus`), `username`, `password`, `port` |

Nilai default hardcoded di ketiga file ini termasuk **hostname produksi asli, username, dan password plaintext** — berfungsi sebagai secret yang ter-commit kalau `.env` tidak di-set. Lihat catatan keamanan di [README index](./README.md#catatan-keamanan-untuk-diketahui-bukan-bagian-dari-dokumentasi-flow).

## Konvensi Struktur Per-DAG

- Setiap DAG hidup di `dags/<nama_dag>/` dengan subpackage `pipelines/` (`extract.py`, `transform.py`, `load.py` opsional, `etl_orchestrator.py`).
- Kalau DAG mengirim laporan Teams, biasanya ada file SQL lokal `sql/<tujuan>_teams_summary.sql` + modul `teams_report_<nama>.py` yang mengekspos fungsi `send_*_teams_report()`.
- Kalau DAG punya tabel tujuan sendiri (bukan tabel shared lintas-DAG), schema-nya didokumentasikan sebagai `sql/create_table.sql` (atau `sql/create_table_<nama_tabel>.sql` kalau lebih dari satu tabel) di folder DAG itu — `CREATE TABLE IF NOT EXISTS`, dijalankan **manual** (bukan lewat migration tool/Airflow task), sekadar biar schema-nya ada satu sumber kebenaran yang di-track Git. Perubahan schema lintas-DAG/tabel shared tetap masuk `migrations/` di root.
- `config/config_db_*.py` hanya menyimpan parameter koneksi (host/user/pass/port/nama DB); semua logic pembuatan koneksi/engine ada di `plugins/common/db_helpers.py` — DAG tidak pernah membangun connection string sendiri.
