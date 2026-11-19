# `ksj_rider_commission_additional` — Komisi Mingguan Rider BLITZ/HMS (Additional)

Menghitung komisi **minggu berjalan** (Senin–Minggu dari tanggal H-1) untuk rider
`BLITZ%` / `HMS%` memakai skema weekly_sales **vendor JW**, lalu menulis ke
`rider_weekly_commission_additional` di KSJ Link production (`d_transaction`). Port dari notebook
`etl_rider_apps_weekly_incentive_additional.ipynb` (disimpan di folder DAG, kredensial sudah dibuang).

## Metadata DAG

File: `dags/ksj_rider_commission/ksj_rider_commission_additional/dag_etl_ksj_rider_commission_additional.py`

| Field | Nilai |
|---|---|
| `dag_id` | `ksj_rider_commission_additional` |
| `schedule` | `MultipleCronTriggerTimetable("0 2 * * *", "0 18 * * 1")` — 02:00 setiap hari **+ 18:00 setiap Senin** (menangkap transaksi Minggu yang telat sync), Asia/Jakarta |
| `catchup` / `max_active_runs` | `False` / `1` |
| `tags` | `["etl", "riders", "commission", "ksj-link", "production", "additional"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |
| Config | `config/config_db_ksj_link.py` (env `KSJ_LINK_*`) |

**Date window**: `target_date` = kemarin (H-1) di Asia/Jakarta (Senin 18:00 → Minggu kemarin). Run manual boleh override lewat
Trigger DAG w/ config `{"target_date": "YYYY-MM-DD"}`.

## Aturan H-1 & run manual

- Data diambil **Senin s/d `target_date`** (bukan sampai Minggu), jadi hari H (real-time, belum final) tidak pernah ikut dihitung. Baris weekly tetap dikunci ke minggu penuh (`start_date`=Senin, `end_date`=Minggu) dan isinya akumulasi sampai `target_date`.
- Hanya `rider_weekly_commission_additional` yang ditulis (DELETE kunci `start_date`/`end_date` minggu itu).
- Guard di `run_all` (gagal jelas, tidak ada yang ditulis):
  - `target_date` >= hari ini (Asia/Jakarta) **ditolak**.
  - Minggu yang sudah lewat **harus** dihitung ulang dengan `target_date` = tanggal **Minggu** minggu itu (tanggal tengah minggu akan menimpa weekly dengan akumulasi parsial).
- Trigger manual tanpa conf: `target_date` = H-1 dari waktu trigger. Dengan conf `{"target_date": "YYYY-MM-DD"}`: tunduk pada guard di atas.


## Alur (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py) — KSJ Link production, db d_transaction
  ├─ rider_daily_transaction_summaries (+ _summary_products) khusus rider_code LIKE 'BLITZ%' / 'HMS%'
  ├─ commission_schemes                  WHERE vendor_code = 'JW'
  ├─ commission_scheme_by_weekly_sales   WHERE scheme milik vendor JW
  └─ commission_scheme_by_prod_solds, commission_extra_schemes, commission_extra_scheme_by_prod_solds

TRANSFORM (transform.py)
  semua rider dipaksa memakai skema JW pertama (scheme_id dari commission_scheme_by_weekly_sales[0],
  type dari commission_schemes[0]; gagal jelas kalau kosong) → daily products → daily summaries
  → weekly summaries. Tanpa kolom jiwaplus.

LOAD (load.py) — 1 transaksi, DELETE-lalu-INSERT minggu berjalan
  └─ rider_weekly_commission_additional   (DELETE start_date/end_date = minggu ini; restart sequence id)
  (tabel daily dihitung sebagai bahan weekly, TIDAK ditulis)

EMAIL: report statistik + total payout (best-effort), subject "[REPORT] KSJ Rider Commission (ADDITIONAL)"
```

## Catatan
- Tabel tidak punya kolom `load_data_by`, jadi tag `NEXUS_AIRFLOW` tidak diterapkan.
- `created_at`/`updated_at` weekly = waktu run asli (Asia/Jakarta).
- `weekly_commission_scheme_id` bernilai `'NaN'` diganti `NULL` sebelum insert.
- Skema dipilih dengan indeks `[0]` tanpa `ORDER BY`: kalau vendor JW punya lebih dari satu skema, pilihannya bisa berubah. Jumlah skema dicetak di log tiap run.
- `run_all(..., dry_run=True)` menjalankan extract + transform dan mencetak total, tanpa load dan tanpa email.
