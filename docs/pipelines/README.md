# Pipeline Documentation — Index & Architecture

Dokumentasi ini menjelaskan seluruh DAG Airflow di `dags/`, dibuat dari hasil membaca
langsung source code (bukan asumsi). Untuk quick start / operasional (start/stop,
troubleshooting, tech stack), lihat [`README.md`](../../README.md) di root project.
Untuk cara mengelola Airflow Variables & secrets, lihat
[`AIRFLOW_SECRETS_AND_VARIABLES.md`](../AIRFLOW_SECRETS_AND_VARIABLES.md).

## Daftar DAG

| Dokumen | DAG ID | Jadwal (Asia/Jakarta) | Ringkasan |
|---|---|---|---|
| [iseller_pusat_dwh](./iseller_pusat_dwh.md) | `etl_iseller_pusat_dwh` | `0 9 * * *` (09:00) | Backfill unfulfilled orders via iSeller API, lalu redump 8 tabel summary DWH |
| [iseller_mitra_dwh](./iseller_mitra_dwh.md) | `etl_iseller_mitra_dwh_summary` | `30 8 * * *` (08:30) | Deteksi & bersihkan duplikat di Data Lake, redump 5 tabel summary secara selektif |
| [sjw_riders_mapping](./sjw_riders_mapping.md) | `etl_sejutajiwa_riders_ksj_link_mapping` | `0 8 * * *` (08:00) | Mapping rider→hub dari KSJ Link, insert ke MySQL Lake+DWH |
| [sjw_riders_mapping](./sjw_riders_mapping.md) | `sync_sejutajiwa_riders_hubs_to_postgresql` | Dipicu via Airflow **Dataset** (bukan cron) | Sync full-table mapping MySQL → 2 database PostgreSQL |
| [f_hourly_rider_performance_tito](./f_hourly_rider_performance_tito.md) | `f_hourly_rider_performance_tito` | `0 8 * * *` (08:00) | Performa rider per jam berbasis check-in/out (TI-TO) |
| [f_rider_mangkal_summary](./f_rider_mangkal_summary.md) | `f_rider_mangkal_summary` | `0 8,11,15 * * *` (3x/hari) | Deteksi sesi "mangkal" (loitering/parkir) rider dari log lokasi |
| [f_daily_ksj_app_user_session](./f_daily_ksj_app_user_session.md) | `f_daily_ksj_app_user_session` | `0 6 * * *` (06:00) | Gabungkan sesi user app JiwaPlus + KSJ (rider-found rate, klik WA) |
| [api_integration_landlord_01065](./api_integration_landlord_01065.md) | `api_landlord_revenue_sharing_01065` | `0 8 * * *` (08:00) | Kirim revenue sharing harian ke API landlord (Lippo Mall tenant portal) |
| [f_daily_estimated_income_per_store](./f_daily_estimated_income_per_store.md) | `f_daily_estimated_income_per_store` | `0 10 * * *` (10:00) | Estimasi uang masuk harian per outlet per payment channel, dikurangi commission cost, untuk landlord |
| [f_ksj_ondemand_order_summary](./f_ksj_ondemand_order_summary.md) | `f_ksj_ondemand_order_summary` | `0 9,12,15,17 * * *` (4x/hari) | Jarak straight-line vs rute (OSRM) & timing paid→delivered per order on-demand KSJ |
| [maintenance](./maintenance.md) | `maintenance_log_cleanup` | `0 2 * * 0` (Minggu 02:00) | Bersihkan log lama & optimasi database metadata Airflow |

Lihat juga [shared_infrastructure.md](./shared_infrastructure.md) untuk modul yang dipakai bersama oleh semua DAG di atas (`plugins/common/`, `config/`).

## Arsitektur Umum

> Versi ringkas & wajib-diikuti dari poin-poin di bawah ini ada di
> [`../../CLAUDE.md`](../../CLAUDE.md) bagian "Pola arsitektur DAG" — itu yang
> auto-loaded tiap sesi Claude Code. Bagian di sini versi naratif/lebih detail buat
> dibaca manusia; kalau ada yang bikin DAG baru pakai Claude session lain, pastikan
> dia baca `CLAUDE.md`, bukan cuma halaman ini.

Semua DAG produksi (kecuali `maintenance`) mengikuti pola yang sama:

```
dag_*.py (Airflow DAG, tipis)
   └─ 1x PythonOperator "run_pipeline"
        └─ pipelines/etl_orchestrator.py :: run_all(...)
             ├─ extract.py   — query sumber data (MySQL Data Lake / PostgreSQL KSJ Link / JiwaPlus)
             ├─ transform.py — pandas: join, cleaning, agregasi
             ├─ load.py (atau inline di etl_orchestrator) — DELETE+INSERT ke MySQL Data Warehouse
             └─ notifikasi   — email (Gmail SMTP) dan/atau MS Teams (Power Automate webhook)
```

Karakteristik penting:

- **Orkestrasi bukan di graf task Airflow.** Hampir semua DAG hanya punya **1 task** (`PythonOperator`) yang memanggil satu fungsi Python besar (`run_all`/`run_pipeline`). Urutan extract→transform→load→notify dikendalikan oleh kode Python biasa, bukan oleh dependency `>>` antar task Airflow. Pengecualian: `sjw_riders_mapping` (2 task: `run_pipeline >> send_reports_task`) dan `maintenance_log_cleanup` (7 task berantai).
- **Date window "yesterday" di Asia/Jakarta.** Sebagian besar DAG jalan pagi hari dan memproses data **H-1** (kemarin), dihitung via helper `_target_date_yesterday_jakarta()` / `_target_end_date_yesterday_jakarta()` yang mengambil `data_interval_end` (fallback `logical_date`) lalu dikonversi ke `Asia/Jakarta`. `iseller_pusat_dwh` malah memproses **rolling 9 hari** (H-9 s.d. H-1) untuk menangkap data fulfillment yang telat masuk. `f_rider_mangkal_summary` scope-nya "hari ini" penuh di dalam SQL (bukan parameter Python), karena jalan 3x sehari.
- **Idempotensi via DELETE-lalu-INSERT.** Semua load ke tabel summary DWH memakai pola: hitung `df_final` penuh di memory dulu → `DELETE` baris untuk rentang tanggal/batch yang sama → `INSERT`/`to_sql(if_exists='append')`. Komentar yang berulang di banyak file: *"DELETE — dilakukan setelah df_final siap di memory"*. Ini membuat retry Airofw (`retries=2`, delay 5 menit) aman untuk dijalankan ulang tanpa duplikasi atau data hilang.
- **Config loading tidak lewat Airflow Connections.** Semua kredensial database berasal dari modul `config/config_db_*.py` (kelas `Config` yang membaca `os.getenv(...)` dengan **fallback default hardcoded**), di-load secara dinamis oleh tiap DAG via:
  ```python
  spec = importlib.util.spec_from_file_location("config", "/opt/airflow/config/config_db_datawarehouse.py")
  ```
  bukan `import` package biasa maupun `airflow.hooks.base.BaseHook.get_connection`. Airflow **Connections** native tidak dipakai sama sekali di project ini.
- **Airflow Variables** hanya dipakai untuk 3 hal: `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` (webhook Teams), `access_token_pusat` (token API iSeller Pusat), dan `access_token_mitra` (token API iSeller Mitra) — semuanya fallback ke environment variable kalau Variable tidak di-set.
- **Notifikasi dua jalur**: sebagian besar DAG mengirim **email HTML via Gmail SMTP** (kredensial hardcoded di source, akun pengirim `saputra.christabel20@gmail.com`) dan sebagian juga mengirim **Adaptive Card ke MS Teams** lewat Power Automate webhook (`plugins/common/teams_powerautomate_report.py`). Tidak semua DAG memakai kedua jalur — `f_daily_ksj_app_user_session` dan `api_integration_landlord_01065` cuma pakai email.
- **Notifikasi Teams selalu best-effort.** Kegagalan kirim ke Teams (webhook belum di-set, network error, dsb.) selalu dibungkus try/except dan hanya di-`logging.warning`, tidak pernah menggagalkan DAG. Kegagalan kirim email juga demikian.
- **Gaya logging campuran.** Kode pipeline (extract/transform/load/orchestrator) dominan pakai `print()` dengan banner ASCII (`"="*65`, `"[n/N] nama_step"`, kadang emoji 🚀📊✅) — bukan modul `logging`. Modul shared di `plugins/common/` (yaitu `db_helpers.py`, `teams_powerautomate_report.py`) konsisten pakai `logging.getLogger(__name__)`.
- **Komentar & pesan bilingual.** Campuran Bahasa Indonesia dan Inggris di komentar kode maupun isi email — gaya yang konsisten di seluruh project.
- **Naming convention pipeline function**: hampir semua fungsi pipeline redump mengikuti signature `run_<nama>(start_date, end_date, config_dw) -> (deleted, dumped, before, after)`, dipanggil di dalam loop/step orchestrator yang membungkus tiap panggilan dengan try/except sendiri-sendiri (satu tabel gagal tidak menghentikan tabel lain).
- **`sys.path` manipulation.** DAG file dan `etl_orchestrator.py` sering melakukan `sys.path.insert(0, ...)` ke folder `pipelines/` agar modul bisa di-import sebagai top-level module — pola defensif supaya kode yang sama juga bisa dijalankan manual di luar Airflow (banyak DAG punya notebook `.ipynb` pendamping untuk manual run/backfill).

## Catatan Keamanan (untuk diketahui, bukan bagian dari dokumentasi flow)

> **Update (pre-handover cleanup):** ketiga poin di bawah — password DB hardcoded di
> `config/`, password Gmail SMTP hardcoded di 16 lokasi (`etl_orchestrator.py` /
> `teams_report_*.py` / 2 notebook), dan `get_token.csv` plaintext — **sudah di-scrub**
> sebelum repo ini diserahkan ke devops. Semua sekarang baca dari environment variable
> (`.env`, tidak masuk git). Ditinggalkan di sini sebagai jejak riwayat, bukan isu
> terbuka. Lihat [`docs/DEPLOYMENT.md`](../DEPLOYMENT.md) untuk cara serah-terima
> secret yang benar.

Beberapa hal yang ditemukan selama riset dan layak diperhatikan tim (di luar scope dokumentasi arsitektur ini):

- ~~Semua `config/config_db_*.py` menyimpan password database sebagai **fallback default di source code**, bukan hanya lewat env var.~~ Sudah diperbaiki — password tidak lagi punya default, wajib dari `.env`.
- ~~Password Gmail SMTP di-hardcode berulang di banyak file `etl_orchestrator.py` / `teams_report_*.py` alih-alih disimpan sebagai Airflow Variable/Connection.~~ Sudah diperbaiki — sekarang `os.getenv("GMAIL_SMTP_PASSWORD")`. Masih **tidak** dipindah ke Airflow Connection/Variable (masih via env), jadi ini tetap area yang bisa ditingkatkan lagi ke depannya kalau mau ikut pola `AIRFLOW_SECRETS_AND_VARIABLES.md`.
- File `.env` di root menyimpan token API iSeller dan URL webhook Teams (dengan `sig=` secret) secara plaintext. **Ini memang disengaja** — `.env` di-gitignore dan tidak pernah masuk repo; itulah tempat semestinya secret plaintext disimpan di luar Git.

Detail cara mengelola secret dengan benar ada di [`AIRFLOW_SECRETS_AND_VARIABLES.md`](../AIRFLOW_SECRETS_AND_VARIABLES.md).

## Catatan Lain

- `sjw_riders_mapping` awalnya bersumber dari Google Sheets, sekarang dari KSJ Link
  PostgreSQL — kalau nemu referensi Google Sheets/`gspread`/`keys/` di dokumen lama atau
  histori, itu sudah tidak berlaku, integrasinya sudah dihapus total dari repo ini.
