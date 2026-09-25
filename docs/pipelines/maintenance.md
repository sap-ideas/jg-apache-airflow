# `maintenance` — Log Cleanup

Satu-satunya DAG yang bukan ETL bisnis — ini pipeline housekeeping internal
Airflow: bersihkan log lama & optimasi database metadata.

Lihat juga [`IMPLEMENTATION_SUMMARY.md`](../../IMPLEMENTATION_SUMMARY.md) di root
untuk latar belakang & rasional retention policy DAG ini.

## Metadata DAG

File: `log_cleanup_dag.py`

| Field | Nilai |
|---|---|
| `dag_id` | `maintenance_log_cleanup` |
| `schedule_interval` | `'0 2 * * 0'` — tiap Minggu 02:00 Asia/Jakarta |
| `start_date` | `datetime(2024, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `['maintenance', 'cleanup', 'monitoring', 'system']` |
| `default_args` | `owner='data-team'`, `depends_on_past=False`, `email_on_failure=False`, `email_on_retry=False`, `retries=1`, `retry_delay=timedelta(minutes=5)` |
| `doc_md` | pakai module docstring |

Berbeda dari DAG lain, DAG ini **bukan** single-task — ada **7 task berantai** (`BashOperator` + 1 `PythonOperator`), murni linear tanpa branching:

```
pre_cleanup_disk_check
  >> cleanup_old_log_files
  >> cleanup_empty_directories
  >> cleanup_old_dag_runs_database
  >> optimize_airflow_database
  >> post_cleanup_disk_check
  >> generate_cleanup_report
```

## Detail Tiap Task

| Task | Tipe | Fungsi |
|---|---|---|
| `pre_cleanup_disk_check` | `BashOperator` | `du`/`find` untuk log disk usage, jumlah file log, file log tertua — murni informasional |
| `cleanup_old_log_files` | `BashOperator` | Hapus file di `/opt/airflow/logs` lebih tua dari `LOG_RETENTION_DAYS=7` hari (`find ... -mtime +7 -delete`), laporkan jumlah sebelum/sesudah |
| `cleanup_empty_directories` | `BashOperator` | Hapus direktori kosong di `/opt/airflow/logs` |
| `cleanup_old_dag_runs_database` | `BashOperator` | `airflow db clean --clean-before-timestamp <cutoff> --skip-archive --yes --verbose` — purge baris metadata DB lebih tua dari `DAG_RUN_RETENTION_DAYS=30` hari |
| `optimize_airflow_database` | `BashOperator` | `psql $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN -c "VACUUM ANALYZE;"` terhadap Postgres metadata Airflow |
| `post_cleanup_disk_check` | `BashOperator` | Ulangi cek disk/log-file plus `df -h` free space sistem, untuk perbandingan sebelum/sesudah |
| `generate_cleanup_report` | `PythonOperator` | `subprocess.run` shell out ke `du`/`find` lagi, log ringkasan terformat via `logging.info` |

**Sistem eksternal**: hanya filesystem lokal (`/opt/airflow/logs`) dan database metadata Postgres milik Airflow sendiri — **tidak** ada Teams/Email/API di DAG ini.

## Retention Policy

| Data | Retensi | Lokasi |
|---|---|---|
| Task Logs | 30 hari (docker-compose `AIRFLOW__LOGGING__LOG_RETENTION_DAYS`) — **catatan**: konstanta di DAG (`LOG_RETENTION_DAYS=7`) berbeda dari docker-compose (30). Komentar di kode menyatakan konstanta ini "should match docker-compose.yaml" tapi tidak ada validasi otomatis — worth dicek/disinkronkan kalau retensi mau diubah. | File system |
| DAG Runs | `DAG_RUN_RETENTION_DAYS=30` hari | PostgreSQL DB metadata |

## Pola Kode Khas

- Satu file self-contained — tidak ada extract/transform/load split; semua logic inline sebagai heredoc bash string di `BashOperator`, plus 1 helper Python.
- Logging emoji-decorated (`🔍 📊 🧹 ✅`) di `echo`/`logging.info` — gaya yang berulang di `sjw_riders_mapping` juga.
- Try/except di sekitar tiap `subprocess.run` dalam `generate_cleanup_report`, log warning kalau gagal — tidak menggagalkan task.
- Konstanta retensi dideklarasikan sebagai module-level uppercase constant dengan komentar peringatan soal sinkronisasi manual ke `docker-compose.yaml`.
- Tiap task punya `doc_md` sendiri (muncul di Airflow UI → Task Instance Details).

## Connections / Variables

- Tidak ada Connection/Variable custom — hanya mengandalkan env var `$AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (connection string metadata DB milik Airflow sendiri) dan CLI internal `airflow db clean`.
