# Deployment Guide (for DevOps)

Panduan ini untuk tim yang akan men-deploy dan mengoperasikan stack Airflow ini di
server/production. Untuk pengetahuan soal isi tiap DAG/pipeline, lihat
[`docs/pipelines/`](pipelines/README.md). Untuk cara kerja sehari-hari (dev quick
start, struktur folder), lihat [`README.md`](../README.md) di root. Untuk alur kerja
tim data nambah/ubah pipeline setelah production live (git branch flow, local vs
production), lihat [`docs/CONTRIBUTING.md`](CONTRIBUTING.md).

## 1. Apa yang dijalankan

- **Apache Airflow 2.8.1** (Python 3.11), `LocalExecutor`, single node.
- **Docker Compose** — 5 service: `postgres` (metadata DB Airflow), `airflow-webserver`,
  `airflow-scheduler`, `airflow-triggerer`, `airflow-init` (jalan sekali saat bootstrap).
- Image custom `airflow-nexus:2.8.1-python3.11` = image resmi Airflow + dependency di
  `requirements.txt` (di-build dari `Dockerfile.airflow`).
- 10 folder DAG / 11 `dag_id` produksi (ETL data warehouse/data lake, riders mapping,
  revenue sharing, maintenance) — jadwal lengkap ada di
  [`docs/pipelines/README.md`](pipelines/README.md).

Tidak ada Kubernetes/Helm/cloud-managed Airflow di sini — murni Docker Compose di satu
host. Kalau target infra devops berbeda (K8s, managed Airflow/Composer/MWAA), stack ini
perlu diadaptasi, bukan tinggal `docker-compose up`.

## 2. Prasyarat server

- Linux (Ubuntu/Debian direkomendasikan) atau WSL2, dengan **Docker** + **Docker
  Compose** terpasang.
  - ⚠️ Repo ini pakai `docker-compose` (v1 binary). Kalau server hanya punya Docker
    Compose v2 (`docker compose`, tanpa strip), ganti semua perintah `docker-compose ...`
    di script/dokumen ini jadi `docker compose ...` — sintaks command sama.
- Resource minimum yang disarankan: **2 vCPU / 4 GB RAM dedicated untuk Airflow**
  (naikkan kalau DAG makin banyak).
- **Akses jaringan keluar** (VPN/firewall rule) ke:
  - `dblake.jiwa.prod` — MySQL Data Lake & Data Warehouse (port 3306)
  - `db-ksj.jiwa.prod` dan `db-ksj-location-service-database.jiwa.prod` — PostgreSQL KSJ Link (port 5432)
  - `db.jiwa.prod` — PostgreSQL JiwaPlus (port 5432)
  - Internet HTTPS keluar untuk: iSeller Pusat API, API revenue landlord Lippo Mall
    (`revenueapi.lippomalls.com:8443`), `smtp.gmail.com:587`, webhook Power
    Automate/Teams, dan `router.project-osrm.org` (routing API dipakai
    `f_ksj_ondemand_order_summary` untuk hitung jarak rute — gagal connect ke sini
    tidak menggagalkan DAG, tapi kolom jarak rute jadi NULL semua).
  - **Koordinasikan dengan tim DE/DBA** sebelum deploy — kalau host devops tidak satu
    jaringan/VPN dengan DB di atas, semua DAG akan gagal connect.
- `docker-compose.yaml` menambahkan entry `/etc/hosts` statis untuk `dblake.jiwa.prod`
  (workaround DNS yang kadang flaky di lingkungan lama). Kalau di server devops DNS
  normal, ini harmless; kalau IP-nya sudah tidak valid, override lewat
  `DBLAKE_EXTRA_HOST_IP` di `.env` atau hapus baris `extra_hosts` di `docker-compose.yaml`.
- Port yang perlu terbuka: **8080** (Airflow Web UI) — satu-satunya port yang
  di-expose ke luar container. Semua service lain (postgres, scheduler, triggerer)
  tidak di-expose keluar host.

## 3. Secrets — diserahkan terpisah, TIDAK lewat Git/GitHub

Satu-satunya file yang sengaja **tidak** ada di repo (lihat `.gitignore`) adalah `.env`
— berisi SEMUA kredensial (lihat `.env.example` untuk daftar lengkap key-nya, termasuk
kredensial API revenue landlord per outlet `LANDLORD_OUTLET_<jilid>_EMAIL/_PASSWORD`).
Harus diserahkan lewat kanal aman (password manager / vault internal / secure file
transfer — bukan chat/email biasa): `cp .env.example .env` lalu isi manual, atau terima
file `.env` langsung dari data team.

Tanpa `.env` terisi, container akan **gagal start** (docker-compose akan menolak start
kalau ada `${VAR:?set_in_env}` yang kosong) atau DAG akan gagal connect ke database/API.

**Soal outlet landlord (`jilid`)**: folder `dags/api_integration_landlord_01065/` sengaja
khusus untuk outlet **01065 saja** (nama folder sudah eksplisit menyebut jilid-nya, DAG
tidak generik). Keputusan tim data: setiap outlet baru dapat **DAG & folder pipeline
sendiri** (bukan 1 DAG generik yang loop banyak outlet) — karena tiap outlet berpotensi
punya data source/skema extract yang berbeda, bukan cuma filter `outlet_code` yang
beda. Format `.env` (`LANDLORD_TOKEN_API_URL` + `LANDLORD_OUTLET_<kode>_EMAIL`/`_PASSWORD`)
tetap generik per-kode supaya siap dipakai outlet berikutnya begitu pipeline-nya dibuat
— outlet **00570** kredensial-nya sudah ada di `.env`, tapi pipeline-nya **belum dibuat**
(masih menunggu detail data source-nya, per 2026-09-22).

### 3a. Migrasi tabel target

Beberapa DAG punya file `sql/create_table*.sql` di folder DAG masing-masing (`CREATE
TABLE IF NOT EXISTS`, tidak dieksekusi otomatis oleh Airflow — murni referensi DDL
manual):

- `dags/f_daily_estimated_income_per_store/sql/create_table.sql` → tabel
  `daily_est_income_per_store_for_landlord`
- `dags/f_ksj_ondemand_order_summary/sql/create_table.sql` → tabel target DAG itu

  **Kedua tabel di atas sudah dibuat & sudah di-dump data testing-nya di production**
  (dikonfirmasi tim data, 2026-09-22) — tidak perlu dijalankan ulang saat deploy.
  Disimpan di sini sebagai referensi schema kalau perlu rebuild/restore di environment
  lain.

- `dags/api_integration_landlord_01065/sql/create_table_logs.sql` dan
  `create_table_log_detail.sql` → tabel `api_landlord_logs` & `api_landlord_log_detail`
  (log request/response ke API landlord). DAG ini sudah lama berjalan di production,
  jadi tabelnya kemungkinan besar sudah ada — **belum eksplisit dikonfirmasi seperti 2
  di atas**, cek dulu ke tim data kalau ragu sebelum deploy pertama kali di server baru.

## 4. Instalasi (first-time deploy)

```bash
# 1. Clone repo
git clone <repo-url> repository
cd repository

# 2. Taruh secrets (lihat bagian 3 di atas)
cp .env.example .env        # lalu isi semua value

# 3. Build image custom (Airflow + dependency di requirements.txt)
docker-compose build

# 4. Bootstrap (inisialisasi metadata DB Airflow + buat user admin) — sekali saja
docker-compose up airflow-init

# 5. Start semua service
docker-compose up -d

# 6. Cek semua service sehat
docker-compose ps
```

Atau pakai script yang sudah disiapkan (menjalankan build+init+up otomatis kalau
`logs/` masih kosong):

```bash
./start_airflow.sh
```

## 5. Verifikasi setelah deploy

```bash
# Semua container harus "healthy" / "Up"
docker-compose ps

# Web UI harus bisa diakses
curl -f http://localhost:8080/health

# Semua 11 dag_id harus muncul, tanpa import error
docker-compose exec airflow-scheduler airflow dags list
docker-compose exec airflow-scheduler airflow dags list-import-errors

# Login ke Web UI pakai kredensial dari .env
#   _AIRFLOW_WWW_USER_USERNAME / _AIRFLOW_WWW_USER_PASSWORD
```

Kalau mau smoke-test satu DAG secara manual tanpa menunggu jadwal:

```bash
docker-compose exec airflow-scheduler \
  airflow dags test <dag_id> $(date +%Y-%m-%d)
```

Daftar `dag_id` ada di [`docs/pipelines/README.md`](pipelines/README.md).

## 6. Operasional sehari-hari

| Perintah | Fungsi |
|---|---|
| `./start_airflow.sh` | Start semua service (+ install ulang `requirements.txt` di container) |
| `./stop_airflow.sh` | Stop semua service |
| `./status_airflow.sh` | Cek status service & volume |
| `./logs_airflow.sh [service]` | Tail log (semua service, atau salah satu) |
| `./scripts/monitor_airflow_logs.sh [--alert-threshold-gb N]` | Cek ukuran folder `logs/`, alert kalau lewat threshold |
| `docker-compose restart airflow-scheduler airflow-webserver` | Restart tanpa downtime metadata DB (dipakai setelah ganti `.env`, mis. update `access_token_pusat`) |
| `docker-compose build && docker-compose up -d` | Rebuild image setelah `requirements.txt` berubah |

## 7. Update / redeploy setelah ada perubahan kode

```bash
git pull
# Kalau requirements.txt berubah:
docker-compose build
docker-compose up -d
# Kalau cuma perubahan DAG (.py), tidak perlu rebuild/restart —
# scheduler otomatis re-scan folder dags/ tiap AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL (300 detik)
```

## 8. Logs & retensi

Diatur di `docker-compose.yaml` env `AIRFLOW__LOGGING__LOG_RETENTION_DAYS` (saat ini
**7 hari**) dan dieksekusi oleh DAG `maintenance_log_cleanup`
(`dags/maintenance/log_cleanup_dag.py`, jadwal Minggu 02:00 WIB), yang juga membersihkan
DAG run history di metadata DB (`DAG_RUN_RETENTION_DAYS = 30` hari, hardcoded di file
tersebut — ubah manual di kode kalau mau beda dari nilai di compose). Detail lengkap ada
di [`docs/LOG_MANAGEMENT_DRD.md`](LOG_MANAGEMENT_DRD.md).

Folder `logs/` bisa tumbuh besar (pernah tercatat ~500 MB) — pastikan disk host punya
ruang cukup, dan jangan hapus `logs/` manual saat container jalan.

## 9. Backup & data yang perlu di-backup

- **Metadata Airflow** ada di Docker volume `postgres-db-volume` (bukan file di repo) —
  ini yang menyimpan DAG run history, Connections, Variables (termasuk
  `access_token_pusat`, `access_token_mitra`, dan webhook Teams kalau disimpan sebagai Airflow Variable, bukan
  cuma `.env`). Backup dengan `docker-compose exec postgres pg_dump -U airflow airflow > backup.sql`,
  atau backup volume-nya langsung.
- **`.env`** — backup terpisah di password manager/vault, BUKAN di dalam repo/volume
  yang sama dengan kode.
- **`AIRFLOW__CORE__FERNET_KEY`** — kalau hilang/berubah, semua Variable/Connection
  terenkripsi yang sudah tersimpan jadi tidak terbaca. Simpan sama amannya dengan
  password database.
- `dags/`, `config/`, `plugins/` — sudah ada di Git, tidak perlu backup terpisah.

## 10. Known limitations (bawaan arsitektur project ini, bukan bug baru)

- **Single node, tanpa HA.** `LocalExecutor` + 1 Postgres metadata DB di satu host —
  kalau host down, semua DAG berhenti. Tidak ada multi-worker/Celery/Kubernetes executor.
- **Kredensial database via `config/config_db_*.py`, bukan Airflow Connections.** Semua
  DAG baca kredensial dari modul `config/` (env var, sudah tidak ada hardcode default
  lagi setelah cleanup ini) — bukan lewat UI Airflow **Admin → Connections**. Kalau
  devops terbiasa mengelola kredensial lewat Connections UI, pola di project ini
  berbeda — jangan kaget.
- **Notifikasi email pakai 1 akun Gmail bersama** (`saputra.christabel20@gmail.com`,
  app password di `.env` sebagai `GMAIL_SMTP_PASSWORD`) untuk hampir semua DAG. Kalau
  app password ini di-revoke/berubah di sisi Google, semua notifikasi email dari semua
  DAG berhenti sekaligus — beri tahu tim data kalau itu terjadi.
- **Notifikasi Teams (Power Automate webhook) best-effort** — kegagalan kirim ke Teams
  tidak pernah menggagalkan DAG (`try/except` + `logging.warning`). Jangan jadikan "tidak
  ada notif Teams" sebagai satu-satunya sinyal DAG gagal — cek Airflow UI juga.
- **`docker-compose` v1 vs v2.** Kalau muncul error `KeyError: 'ContainerConfig'` saat
  recreate container, itu known issue mismatch `docker-compose` v1 + Docker versi baru
  — pindah ke `docker compose` (v2 plugin) atau bersihkan container lama dulu. Detail:
  [`docs/AIRFLOW_SECRETS_AND_VARIABLES.md`](AIRFLOW_SECRETS_AND_VARIABLES.md) bagian 7.

## 11. Rollback

Karena tidak ada state di image (semua kode di-mount sebagai volume dari `dags/`,
`config/`, `plugins/`), rollback = `git checkout <commit-lama>` lalu:

```bash
docker-compose down
git checkout <commit/tag lama>
docker-compose build   # cuma perlu kalau requirements.txt ikut berubah
docker-compose up -d
```

Metadata Airflow (history run, Variables) **tidak ikut ter-rollback** — itu ada di
volume Postgres terpisah dari kode. Kalau perlu rollback data juga, restore dari backup
`pg_dump` di bagian 9.

## 12. Troubleshooting

Lihat bagian **Troubleshooting** di [`README.md`](../README.md) untuk kasus umum (DAG
tidak muncul, import error, connection error, permission error pada folder yang
di-mount).

## 13. Contact

Owner project: **data-team** — lahiardhan@jiwagroup.com
