# `ksj_rider_commission_staging` — Komisi Rider KSJ (Staging)

Menghitung komisi rider Sejuta Jiwa (KSJ) untuk **minggu berjalan** (Senin–Minggu dari
tanggal H-1) dan menulisnya ke database **KSJ Link staging** milik tim tech, supaya rider
app staging bisa menampilkannya. Port dari notebook `etl_rider_apps_weekly_incentive_stg.ipynb`
(notebook disimpan di folder DAG sebagai referensi, kredensial sudah dibuang).

## Metadata DAG

File: `dags/ksj_rider_commission/ksj_rider_commission_staging/dag_etl_ksj_rider_commission_staging.py`

| Field | Nilai |
|---|---|
| `dag_id` | `ksj_rider_commission_staging` |
| `schedule` | `"0 2 * * *"` — harian 02:00 Asia/Jakarta |
| `catchup` / `max_active_runs` | `False` / `1` |
| `tags` | `["etl", "riders", "commission", "ksj-link", "staging"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |
| Config | `config/config_db_ksj_link_staging.py` (env `KSJ_LINK_STG_*`, wajib diisi — DAG gagal jelas kalau kosong) |

**Date window**: `target_date` = kemarin di Asia/Jakarta. Run manual boleh override lewat
Trigger DAG w/ config `{"target_date": "YYYY-MM-DD"}`. Minggu = Senin..Minggu dari `target_date`.

## Alur (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py) — KSJ Link staging, db d_transaction
  ├─ rider_daily_transaction_summaries / _summary_products (rider ownership_status = 'KBN')
  └─ commission_schemes, commission_scheme_by_weekly_sales, commission_scheme_by_prod_solds,
     commission_extra_schemes, commission_extra_scheme_by_prod_solds

TRANSFORM (transform.py)
  daily products → daily summaries → weekly summaries (1 baris per rider)

LOAD (load.py) — 1 transaksi, DELETE-lalu-INSERT untuk minggu berjalan
  ├─ rider_weekly_commission_summaries            (DELETE start_date/end_date = minggu ini)
  ├─ rider_daily_commission_summaries             (DELETE transaction_date di dalam minggu)
  └─ rider_daily_commission_summary_products      (DELETE transaction_date di dalam minggu)
  (tiap tabel: restart sequence id dari MAX(id)+1, insert batch 1.000)

EMAIL: report statistik + total payout (best-effort), subject diawali [STAGING]
```

## Catatan
- Tabel milik tim tech, **tidak punya kolom `load_data_by`**, jadi tag `NEXUS_AIRFLOW` tidak diterapkan.
- `created_at`/`updated_at` hanya di-set di tabel weekly (waktu run asli, Asia/Jakarta); tabel daily memakai default DB — sama seperti notebook.
- `run_all(..., dry_run=True)` menjalankan extract + transform dan mencetak total, tanpa load dan tanpa email.
