# Project Conventions

Project ETL Airflow. Lihat [`README.md`](README.md) untuk overview umum dan
[`docs/pipelines/`](docs/pipelines/README.md) untuk detail tiap DAG.

File ini adalah konvensi kode yang **wajib diikuti** untuk setiap kontribusi baru —
baik ditulis manual maupun dengan bantuan Claude session lain. Isinya migrasi dari
`.cursor/rules/` (sudah dihapus, repo ini tidak lagi memakai Cursor).

**Daftar isi:**
1. [Arsitektur DAG & Pipeline](#1-arsitektur-dag--pipeline)
2. [Secrets & Konfigurasi](#2-secrets--konfigurasi)
3. [Menulis ke Database](#3-menulis-ke-database)
4. [Notifikasi Teams / Power Automate](#4-notifikasi-teams--power-automate)

---

## 1. Arsitektur DAG & Pipeline

Semua DAG produksi (kecuali `maintenance`) mengikuti pola yang sama. Ikuti pola ini
persis untuk DAG baru — jangan improvisasi walau pendekatan lain terlihat "lebih
proper" secara Airflow pada umumnya. Detail naratif + contoh per-DAG ada di
[`docs/pipelines/README.md`](docs/pipelines/README.md) bagian "Arsitektur Umum".

### 1.1 Struktur file

Setiap pipeline **selalu** dipecah jadi file terpisah di `dags/<nama_dag>/pipelines/`
— jangan digabung jadi satu file walau logic-nya terlihat pendek:

| File | Isi |
|---|---|
| `extract.py` | Query/ambil data dari source (DB, API, CSV) |
| `transform.py` | Pandas: cleaning, join, agregasi, derive kolom |
| `load.py` | Tulis ke target (DB, API eksternal) |
| `etl_orchestrator.py` | `run_all()` — memanggil extract → transform → load secara berurutan, plus notifikasi (email/Teams) |

DAG file (`dag_*.py`) tetap tipis: hanya load config + satu `PythonOperator` yang
memanggil `run_all()`.

### 1.2 Mencegah tabrakan nama modul (WAJIB)

Nama file `extract.py` / `transform.py` / `load.py` / `etl_orchestrator.py` berulang
di **semua** folder DAG. Airflow memuat seluruh `dag_*.py` dalam satu proses saat
membangun DagBag, jadi `sys.path.insert(0, pipelines_dir)` yang diikuti langsung
`from etl_orchestrator import run_all` bisa **diam-diam mengimpor modul milik DAG
lain** yang kebetulan nama file-nya sama (modul ter-cache lebih dulu oleh DAG lain,
lalu "bocor" dipakai DAG ini).

> **Insiden nyata:** `sjw_riders_mapping` sempat gagal total di production
> (2026-09-23) gara-gara ini.

Solusinya, selalu pakai helper yang sudah ada — jangan `sys.path.insert` manual:

```python
from common.pipeline_loader import load_pipeline_dir
load_pipeline_dir(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pipelines'))

from etl_orchestrator import run_all
```

Lihat `plugins/common/pipeline_loader.py` untuk penjelasan lengkap.

### 1.3 Aturan wajib lainnya

- **Config loading tidak pernah lewat Airflow Connections.** Semua kredensial DB
  di-load dinamis dari `config/config_db_*.py` via
  `importlib.util.spec_from_file_location` — bukan
  `airflow.hooks.base.BaseHook.get_connection` atau Connections UI. Airflow
  Connections **sama sekali tidak dipakai** di project ini; jangan sarankan atau
  buat Connection baru.
- **Satu task per DAG (umumnya).** Orkestrasi extract → transform → load → notify
  dikendalikan kode Python biasa di dalam `etl_orchestrator.run_all()`, bukan oleh
  graf task Airflow (`>>`). DAG hanya punya satu `PythonOperator` yang memanggil
  `run_all()`. Pengecualian: `sjw_riders_mapping` (2 task berantai) dan
  `maintenance_log_cleanup` (7 task). Jangan buat DAG multi-task baru kecuali ada
  alasan sekuat itu.
- **Date window "kemarin" (H-1) di Asia/Jakarta**, dihitung via helper
  `_target_date_yesterday_jakarta()` (atau `_target_end_date_yesterday_jakarta()`):
  ambil `data_interval_end` (fallback `logical_date`), convert ke `Asia/Jakarta`,
  kurangi 1 hari. DAG yang jalan beberapa kali sehari boleh punya pola berbeda —
  lihat `f_ksj_ondemand_order_summary` (hari ini + lookback) atau
  `f_rider_mangkal_summary` (hari ini, scope di SQL) sebagai contoh valid.
- **Idempotensi via DELETE-lalu-INSERT**, bukan `if_exists='replace'`: hitung
  `df_final` penuh di memory dulu → `DELETE` baris untuk range/batch yang sama →
  `INSERT` / `to_sql(if_exists='append')`. Ini yang membuat retry Airflow
  (`retries=2`, delay 5 menit) aman dijalankan ulang tanpa duplikasi atau data
  hilang.
- **Notifikasi**: email (Gmail SMTP, `GMAIL_SMTP_PASSWORD`) adalah default/wajib.
  Teams (Power Automate) **opsional dan selalu best-effort** — bungkus try/except,
  kegagalan kirim Teams hanya `logging.warning`, tidak boleh menggagalkan DAG.
  Detail lengkap: [bagian 4](#4-notifikasi-teams--power-automate).
- **Airflow Variables dipakai seminimal mungkin**, hanya 2 yang legit saat ini:
  `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` dan `access_token_pusat` (keduanya fallback ke
  environment variable). Jangan buat Variable baru untuk hal yang cukup lewat
  `.env`.
- **Gaya logging**: kode pipeline (`extract.py` / `transform.py` / `load.py` /
  `etl_orchestrator.py`) memakai `print()` dengan banner ASCII (`"="*65`,
  `"[n/N] nama_step"`) — bukan modul `logging`. `logging.getLogger(__name__)` hanya
  dipakai di modul shared `plugins/common/`. Ikuti yang sesuai dengan lokasi file
  barumu.

---

## 2. Secrets & Konfigurasi

Semua kredensial (database, Gmail SMTP, API token) dibaca dari environment variable
via `.env` — lihat `.env.example` untuk daftar lengkap key yang tersedia.

**Jangan** hardcode default credential apa pun di `config/config_db_*.py` atau file
lain mana pun.

Detail lengkap: [`docs/AIRFLOW_SECRETS_AND_VARIABLES.md`](docs/AIRFLOW_SECRETS_AND_VARIABLES.md)
dan [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

---

## 3. Menulis ke Database

### 3.1 Tag audit `NEXUS_AIRFLOW` (dags/**/*.py, plugins/**/*.py)

Setiap baris yang ditulis ke MySQL/PostgreSQL oleh **DAG atau pipeline Airflow**
wajib ditandai `load_data_by` / `updated_by` = **`'NEXUS_AIRFLOW'`**. Tag ini yang
membedakan load otomatis (Airflow/Nexus) dari load manual oleh manusia lewat
notebook/ad-hoc script (yang memakai email operator).

- Default value **harus** `'NEXUS_AIRFLOW'` — jangan personal email (mis.
  `'lahiardhan@jiwagroup.com'`).
- Definisikan sekali sebagai konstanta module-level: `UPDATED_BY = "NEXUS_AIRFLOW"`
  (untuk kode baru) atau `DEFAULT_LOAD_DATA_BY = "NEXUS_AIRFLOW"` (kalau nilainya
  mengalir lewat default parameter fungsi).
- Lempar konstanta lewat parameter (biar testable) — jangan hardcode literal
  string di tiap call site.

```python
# BAD — personal email leaks into production data warehouse
DEFAULT_LOAD_DATA_BY = "lahiardhan@jiwagroup.com"
df['load_data_by'] = "someone@jiwagroup.com"

# GOOD — clear "this row was written by automation"
UPDATED_BY = "NEXUS_AIRFLOW"

def run(config_dw, updated_by=UPDATED_BY):
    df['load_data_by'] = updated_by
    df.to_sql(..., con=engine, if_exists='append')
```

**Pengecualian:** notebook manual (`manual_run_*.ipynb`) bersifat human-triggered,
jadi tetap memakai email operator sebagai `load_data_by`. Aturan `NEXUS_AIRFLOW` di
atas hanya berlaku untuk file `.py` yang berjalan di scheduler Airflow.

### 3.2 Provisioning tabel database

**Pipeline tidak pernah membuat tabel sendiri** — tidak ada `CREATE TABLE` yang
dieksekusi dari kode Python/Airflow. Tabel target dibuat **manual oleh data team di
DBeaver** sebelum pipeline pertama kali dijalankan. Pipeline hanya `DELETE` (untuk
range/batch yang sama) lalu `INSERT` / `to_sql(if_exists='append')` — tidak pernah
`if_exists='replace'` atau `CREATE TABLE IF NOT EXISTS` yang berjalan otomatis.

File `sql/create_table*.sql` yang ada di beberapa folder DAG (kalau ada) hanyalah
**referensi schema**, bukan migration yang dieksekusi otomatis. File itu tetap harus
dijalankan manual (atau direplikasi manual di DBeaver) oleh data team, dan itu
terjadi **sebelum** DAG-nya diaktifkan — bukan bagian dari alur run pipeline.

---

## 4. Notifikasi Teams / Power Automate (Adaptive Cards)

Tanggung jawab dipecah menjadi tiga lapis (`dags/**/*.py`, `dags/**/*.sql`,
`plugins/common/teams_powerautomate_report.py`):

1. **`plugins/common/teams_powerautomate_report.py`** (shared, long-lived)
   - HTTP: `post_to_power_automate`, `build_power_automate_payload`.
   - Card shell: `build_etl_report_adaptive_card` (title, date, success container,
     FactSet).
   - Layout helper reusable: `text_block`, `column`, `column_set`,
     `section_column_table`, `format_int_commas`.
   - **Jangan** taruh SQL spesifik-DAG, copy bisnis, atau dataclass one-off di
     sini — modul ini harus tetap kecil dan stabil.

2. **Per folder DAG**, di `dags/<...>/`:
   - **`sql/<purpose>_teams_summary.sql`** — SQL summary untuk Teams card (satu
     file per query/report, nama jelas).
   - **`teams_report_<dag_or_area>.py`** — load SQL dari `sql/`, jalankan via
     SQLAlchemy `Engine`, map row ke string, panggil
     `build_etl_report_adaptive_card` + `post_to_power_automate`.
   - DAG meng-import `send_*_teams_report` dari modul lokal itu. Contoh:
     `sjw_riders_mapping/teams_report_riders_mapping.py`,
     `iseller_mitra_dwh/teams_report_iseller_mitra.py`,
     `iseller_pusat_dwh/teams_report_iseller_pusat_unfulfilled.py`.

3. **Webhook secret**:
   - Pakai **satu webhook URL global**, resolve lewat
     `common.teams_powerautomate_report.resolve_power_automate_webhook_url()`.
   - Konfigurasi sebagai Airflow Variable **`POWER_AUTOMATE_TEAMS_WEBHOOK_URL`**
     (lebih disarankan) atau environment variable dengan nama sama.
   - **Jangan** hardcode webhook URL (termasuk secret `sig=`) di dalam modul
     DAG/report.

**Kontrak Power Automate:** body POST harus punya object **`adaptiveCard`** (root
`type: AdaptiveCard`, `$schema`, `version`). Flow harus pakai **Post adaptive
card** (bukan plain Post message), bind ke `triggerBody()?['adaptiveCard']` (atau
setara).

**Menambah report DAG baru:**
1. Tambahkan `dags/<your_dag>/sql/<name>.sql`.
2. Tambahkan `dags/<your_dag>/teams_report_<your_dag>.py` dengan fetch +
   `send_*_teams_report`.
3. Di DAG: `PythonOperator` terakhir dengan `trigger_rule="all_success"` yang
   memanggil `send_*`.
4. Pakai `section_column_table` untuk grid sederhana; untuk layout custom, compose
   dengan `text_block` / `column_set` hanya di modul lokal DAG.
