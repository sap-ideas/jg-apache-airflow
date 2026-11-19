# `ksj_rider_commission_production` — Komisi Rider KSJ (Production)

Menghitung komisi rider Sejuta Jiwa (KSJ) untuk **minggu berjalan** (Senin–Minggu dari
tanggal H-1) dan menulisnya ke database **KSJ Link production** (`d_transaction`), supaya rider
app bisa menampilkannya. Port dari notebook `etl_rider_apps_weekly_incentive_prod_v3.ipynb`
(notebook disimpan di folder DAG sebagai referensi, kredensial sudah dibuang).

## Metadata DAG

File: `dags/ksj_rider_commission/ksj_rider_commission_production/dag_etl_ksj_rider_commission_production.py`

| Field | Nilai |
|---|---|
| `dag_id` | `ksj_rider_commission_production` |
| `schedule` | `MultipleCronTriggerTimetable("0 2 * * *", "0 18 * * 1")` — 02:00 setiap hari **+ 18:00 setiap Senin** (menangkap transaksi Minggu yang telat sync), Asia/Jakarta |
| `catchup` / `max_active_runs` | `False` / `1` |
| `tags` | `["etl", "riders", "commission", "ksj-link", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |
| Config | `config/config_db_ksj_link.py` (env `KSJ_LINK_*`, sama dengan DAG KSJ Link lain) |

**Date window**: `target_date` = kemarin (H-1) di Asia/Jakarta (Senin 18:00 → Minggu kemarin). Run manual boleh override lewat
Trigger DAG w/ config `{"target_date": "YYYY-MM-DD"}`. Minggu = Senin..Minggu dari `target_date`.

## Aturan H-1 & run manual

- Data diambil **Senin s/d `target_date`** (bukan sampai Minggu), jadi hari H (real-time, belum final) tidak pernah ikut dihitung. Baris weekly tetap dikunci ke minggu penuh (`start_date`=Senin, `end_date`=Minggu) dan isinya akumulasi sampai `target_date`.
- DELETE tabel daily/produk hanya sampai `target_date`; baris setelahnya tidak disentuh.
- Guard di `run_all` (gagal jelas, tidak ada yang ditulis):
  - `target_date` >= hari ini (Asia/Jakarta) **ditolak**.
  - Minggu yang sudah lewat **harus** dihitung ulang dengan `target_date` = tanggal **Minggu** minggu itu (tanggal tengah minggu akan menimpa weekly dengan akumulasi parsial).
- Trigger manual tanpa conf: `target_date` = H-1 dari waktu trigger. Dengan conf `{"target_date": "YYYY-MM-DD"}`: tunduk pada guard di atas.


## Alur (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py) — KSJ Link production, db d_transaction
  ├─ rider_daily_transaction_summaries / _summary_products (rider ownership_status = 'KBN')
  └─ commission_schemes, commission_scheme_by_weekly_sales, commission_scheme_by_prod_solds,
     commission_extra_schemes, commission_extra_scheme_by_prod_solds

TRANSFORM (transform.py)
  daily products → daily summaries → weekly summaries (1 baris per rider)

LOAD (load.py) — 1 transaksi, DELETE-lalu-INSERT (weekly: minggu penuh; daily/produk: Senin..target_date)
  ├─ rider_weekly_commission_summaries            (DELETE start_date/end_date = minggu ini)
  ├─ rider_daily_commission_summaries             (DELETE transaction_date Senin..target_date)
  └─ rider_daily_commission_summary_products      (DELETE transaction_date Senin..target_date)
  (tiap tabel: restart sequence id dari MAX(id)+1, insert batch 1.000)

EMAIL: report statistik + total payout (best-effort), penerima `athens.jiwagroup@gmail.com`
```

## Catatan
- Tabel dibaca rider app, **tidak punya kolom `load_data_by`**, jadi tag `NEXUS_AIRFLOW` tidak diterapkan.
- `created_at`/`updated_at` hanya di-set di tabel weekly (waktu run asli, Asia/Jakarta); tabel daily memakai default DB — sama seperti notebook. Trik notebook lama (`base_date` hardcode 2025-08-21 + jam random 03:xx) dibuang.
- Beda dari notebook: `weekly_commission_scheme_id` bernilai `'NaN'` diganti `NULL` sebelum insert (sama seperti staging).
- **Jangan jalankan notebook ini manual di hari yang sama setelah DAG aktif** (DELETE-INSERT minggu yang sama, hasilnya saling timpa).
- `run_all(..., dry_run=True)` menjalankan extract + transform dan mencetak total, tanpa load dan tanpa email.
