# Airflow ETL Pipeline - Data Team

Automated ETL pipelines untuk iSeller, KSJ Link riders mapping, dan maintenance — berjalan di Apache Airflow 2.8.1 dengan Docker Compose.

> Mau nambah/ubah pipeline setelah production live? Baca
> [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md) dulu (git branch flow, local vs
> production). Untuk deploy/operate infra: [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md).

---

## Prerequisites

- **OS:** WSL2 Ubuntu 24.04 (atau Linux server)
- **Docker** & **Docker Compose** (v2+)
- Akses network ke database server `dblake.jiwa.prod` (MySQL), `db-ksj.jiwa.prod` (PostgreSQL), `db.jiwa.prod` (PostgreSQL)
- File `.env` terisi (lihat `.env.example`) — diserahkan terpisah di luar repo git, lihat [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md)

---

## Quick Start

```bash
# 1. Clone repo (kalau belum)
cd /root
git clone <repo-url> repository
cd repository

# 2. Siapkan .env (nilai asli diserahkan terpisah, lihat docs/DEPLOYMENT.md)
cp .env.example .env   # lalu isi semua value

# 3. Start Airflow (auto init kalau pertama kali)
./start_airflow.sh

# 4. Buka Web UI
#    URL  : http://localhost:8080
#    User : (sesuai _AIRFLOW_WWW_USER_USERNAME di .env)
#    Pass : (sesuai _AIRFLOW_WWW_USER_PASSWORD di .env)
```

Selesai. DAGs akan muncul di UI dalam beberapa menit.

---

## Management Commands

| Perintah | Fungsi |
|----------|--------|
| `./start_airflow.sh` | Start semua service + install dependencies |
| `./stop_airflow.sh` | Stop semua service |
| `./status_airflow.sh` | Cek status service & volumes |
| `./logs_airflow.sh` | Live log semua service |
| `./logs_airflow.sh airflow-scheduler` | Live log scheduler saja |
| `docker-compose restart airflow-scheduler airflow-webserver` | Restart service tertentu |

---

## Project Structure

```
repository/
├── .env                            # Environment variables (credentials, tokens)
├── docker-compose.yaml             # Docker stack definition
├── requirements.txt                # Python dependencies (auto-install)
├── start_airflow.sh                # Start script
├── stop_airflow.sh                 # Stop script
├── status_airflow.sh               # Status check
├── logs_airflow.sh                 # Log viewer
│
├── config/                         # DB config modules (mounted ke /opt/airflow/config)
│   ├── config_db_datawarehouse.py  #   MySQL Data Lake & Warehouse
│   ├── config_db_ksj_link.py       #   PostgreSQL KSJ Link
│   └── config_db_jiwaplus.py       #   PostgreSQL JiwaPlus (consumer app)
│
├── dags/                           # Airflow DAGs (mounted ke /opt/airflow/dags), 1 folder = 1 DAG
│   ├── iseller_pusat_dwh/          #   ETL iSeller Pusat (DWH)
│   ├── iseller_mitra_dwh/          #   ETL iSeller Mitra (DWH)
│   ├── sjw_riders_mapping/         #   KSJ Link riders↔hubs mapping + sync PostgreSQL
│   ├── f_hourly_rider_performance_tito/  # Performa rider per jam (check-in/out)
│   ├── f_rider_mangkal_summary/    #   Deteksi sesi "mangkal" rider
│   ├── f_daily_ksj_app_user_session/  # Sesi user app JiwaPlus + KSJ
│   ├── f_ksj_ondemand_order_summary/  # Summary order on-demand KSJ
│   ├── api_integration_landlord_01065/  # Revenue sharing → API landlord Lippo Mall (jilid 01065 only)
│   ├── f_daily_estimated_income_per_store/  # Estimasi uang masuk harian per outlet (landlord)
│   └── maintenance/                #   Cleanup log & metadata Airflow
│
├── logs/                           # Runtime logs (auto-generated, gitignored)
├── plugins/                        # Modul shared (plugins/common/)
├── migrations/                     # SQL migration scripts
└── scripts/                        # Monitoring scripts
```

Rincian tiap DAG (DAG ID, jadwal, fungsi) ada di [`docs/pipelines/README.md`](docs/pipelines/README.md)
— dijaga sinkron dengan source code, jadi itu yang jadi rujukan utama, bukan file ini.

### Enable / Disable DAG

1. Login ke Airflow UI → http://localhost:8080
2. Cari DAG yang diinginkan
3. Toggle switch on/off
4. Untuk manual run: klik **Trigger DAG**

---

## Database Connections

Semua kredensial ada di `.env` (lihat bagian [Environment Variables](#environment-variables)
di bawah), dibaca sebagai environment variable oleh config module di `config/`.

| Database | Host | Module |
|----------|------|--------|
| MySQL Data Lake | `dblake.jiwa.prod` / `data_lake_jiwa` | `config_db_datawarehouse.py` |
| MySQL Data Warehouse | `dblake.jiwa.prod` / `data_warehouse_jiwa` | `config_db_datawarehouse.py` |
| PostgreSQL KSJ Link | `db-ksj.jiwa.prod` | `config_db_ksj_link.py` |
| PostgreSQL JiwaPlus | `db.jiwa.prod` | `config_db_jiwaplus.py` |

Config module di-load oleh DAGs menggunakan `importlib`:
```python
spec = importlib.util.spec_from_file_location("config", "/opt/airflow/config/config_db_datawarehouse.py")
```

---

## iSeller Pusat Pipeline — Detail

### Flow

```
1. Detect unfulfilled orders di Data Lake
        ↓
2. Call iSeller API (GetProducts, GetOrders)
        ↓
3. Process & insert details ke Data Lake
        ↓
4. Update fulfillment_status di Data Lake
        ↓
5. Re-dump 8 DWH summary tables (berurutan):
     a. daily_transaction_summary_iseller_pusat
     b. daily_sales_type_summary_iseller_pusat
     c. hourly_transaction_summary_iseller_pusat
     d. daily_items_summary_iseller_pusat
     e. daily_bundling_summary_iseller_pusat
     f. daily_items_salestype_summary_pusat
     g. daily_outlet_transaction_summary_companywide_new
     h. hourly_outlet_transaction_summary_companywide_new
        ↓
6. Send email report
```

### API Token (`access_token_pusat`)

Pipeline ini membutuhkan API token iSeller yang **expired secara periodik** dan harus di-update manual.

**Lokasi token:** file `.env` di root project

```bash
# .env
access_token_pusat=YOUR_TOKEN_HERE
```

**Cara update token:**

1. Dapatkan token baru dari iSeller
2. Edit file `.env`, ganti value `access_token_pusat`
3. Restart Airflow supaya environment variable ter-load ulang:
   ```bash
   docker-compose restart airflow-scheduler airflow-webserver
   ```

**Kalau token expired saat pipeline berjalan:**
- Pipeline akan **berhenti total** (tidak lanjut ke DWH update)
- Email alert terkirim dengan daftar unfulfilled orders dan keterangan token expired
- Subject email: `[ALERT] ETL iSeller Pusat - STOPPED - API Token Expired (...)`

### Manual Run (Notebook)

File: `dags/iseller_pusat_dwh/manual_run_iseller_pusat_dwh.ipynb`

Parameter yang bisa diatur:

| Parameter | Type | Default | Keterangan |
|-----------|------|---------|------------|
| `AUTO_MODE` | bool | `False` | `True` = otomatis pakai kemarin, `False` = manual isi tanggal |
| `MANUAL_START_DATE` | str | - | Format `YYYY-MM-DD`, isi jika `AUTO_MODE = False` |
| `MANUAL_END_DATE` | str | - | Format `YYYY-MM-DD`, isi jika `AUTO_MODE = False` |
| `SKIP_DL_CHECK` | bool | `False` | `True` = skip backfill fulfillment, langsung DWH update |

---

## Environment Variables

**Semua kredensial ada di `.env`** (di-gitignore, tidak pernah masuk repo). Template
lengkap dengan semua key ada di `.env.example` — copy jadi `.env` lalu isi nilainya.
`docker-compose.yaml` membaca semuanya lewat `${VAR}` — tidak ada credential yang
di-hardcode langsung di file itu.

| Variable | Keterangan |
|----------|------------|
| `AIRFLOW_UID` | User ID untuk container (default: `50000`, jangan diubah kecuali ada permission issue) |
| `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | Login Web UI — **ganti password default sebelum deploy production** |
| `AIRFLOW__CORE__FERNET_KEY` | Key enkripsi Variables/Connections — harus sama persis di semua service, jangan diganti setelah production jalan |
| `access_token_pusat` | API token iSeller Pusat (expired berkala, update manual atau via Airflow Variable) |
| `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` | Webhook Teams untuk notifikasi/alert |
| `GMAIL_SMTP_PASSWORD` | App password Gmail (`saputra.christabel20@gmail.com`) dipakai hampir semua DAG untuk kirim email report |
| `DB_LAKE_*`, `DB_WAREHOUSE_*` | Kredensial MySQL Data Lake & Data Warehouse |
| `KSJ_LINK_*` | Kredensial PostgreSQL KSJ Link |
| `JIWAPLUS_*` | Kredensial PostgreSQL JiwaPlus |

Detail lengkap soal secrets & Airflow Variables ada di [`docs/AIRFLOW_SECRETS_AND_VARIABLES.md`](docs/AIRFLOW_SECRETS_AND_VARIABLES.md).

---

## Troubleshooting

### DAG tidak muncul di UI

```bash
# Cek scheduler log untuk import error
docker-compose logs -f airflow-scheduler

# Restart scheduler
docker-compose restart airflow-scheduler
```

### Import error / module not found

```bash
# Re-install dependencies di kedua container
docker-compose exec airflow-webserver pip install -r /opt/airflow/requirements.txt
docker-compose exec airflow-scheduler pip install -r /opt/airflow/requirements.txt

# Restart
docker-compose restart airflow-scheduler airflow-webserver
```

### Database connection error

```bash
# Test MySQL dari dalam container (pakai config module, bukan hardcode credential)
docker-compose exec airflow-scheduler python -c "
import sys
sys.path.insert(0, '/opt/airflow/config')
import mysql.connector
from config_db_datawarehouse import Config

conn = mysql.connector.connect(
    host=Config.hostname_lake, user=Config.username_lake,
    password=Config.password_lake, database=Config.db_lake,
)
print('Connected:', conn.is_connected())
conn.close()
"
```

### Permission error pada logs/dags

```bash
sudo chown -R 50000:0 logs/ dags/ plugins/ config/
```

### API Token expired (iSeller Pusat)

Gejala: Email alert `[ALERT] ... API Token Expired` atau error `401 Unauthorized` di log.

```bash
# 1. Update token di .env
nano .env
# Ganti value access_token_pusat

# 2. Restart supaya env variable ter-load
docker-compose restart airflow-scheduler airflow-webserver

# 3. Trigger ulang DAG dari UI atau CLI
docker-compose exec airflow-scheduler airflow dags trigger etl_iseller_pusat_dwh
```

### Container tidak mau start

```bash
# Cek Docker running
sudo service docker start

# Full reset (data Airflow metadata hilang, DAG history reset)
docker-compose down -v
./start_airflow.sh
```

---

## Tech Stack

| Component | Version |
|-----------|---------|
| Apache Airflow | 2.8.1 |
| Python | 3.11 |
| PostgreSQL (metadata) | 13 |
| MySQL (Data Lake & DWH) | Remote server |
| Docker Compose | 3.8 |
| Executor | LocalExecutor |

---

**Owner:** data-team
**Maintained by:** lahiardhan@jiwagroup.com
