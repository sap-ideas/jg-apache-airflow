# `f_order_estimated_income_per_store` — Estimasi Uang Masuk Per Order (untuk Landlord)

Menghitung estimasi uang masuk **per order** dari transaksi iSeller Pusat (satu baris per
`order_id` + `transaction_id`; split payment = lebih dari satu baris) dengan commission per payment type,
ke tabel `order_est_income_for_landlord`. Dipakai landlord yang meminta revenue sharing per order
(awal: outlet `00570`, lihat DRD 04). Hasilnya divalidasi terhadap `daily_est_income_per_store_for_landlord`
sebelum di-load. Nominal yang dibagi ke landlord (`subtotal - sales_type_commission`) tidak disimpan.

## Metadata DAG

File: `dag_etl_f_order_estimated_income_per_store.py`

| Field | Nilai |
|---|---|
| `dag_id` | `f_order_estimated_income_per_store` |
| `schedule` | `[Asset("f_daily_estimated_income_per_store://daily_est_income_per_store_for_landlord")]` — tanpa jam tetap, jalan tepat setelah `f_daily_estimated_income_per_store` (10:00) sukses |
| `outlets` (Asset yang di-emit) | `Asset("f_order_estimated_income_per_store://order_est_income_for_landlord")` |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "landlord", "income", "iseller", "dwh", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |

**Kenapa Asset-trigger, bukan cron pagi**: validasi wajib vs tabel daily, dan tabel daily H-1 baru ter-load
jam 10:00 (setelah `etl_iseller_pusat_fulfillment` @ 09:00 refresh source table).

**Date window**: run otomatis = satu hari, tanggal yang diproses run upstream `f_daily_estimated_income_per_store`
(H-1 Asia/Jakarta, dibaca dari `context["triggering_asset_events"]` — key Airflow 3.4, bukan `triggering_dataset_events`).
Run manual: param `start_date` / `end_date` (maks 31 hari), `load_data_by` = email user yang men-trigger;
run otomatis = `NEXUS_AIRFLOW`.

## Alur Pipeline (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py)
  extract_order_transactions : transactions_iseller_pusat (Data Lake), outlet_code IN OUTLET_CODES ('00570'),
    fulfillment_status='fulfilled', status='completed', payment_status='paid', transactions_status='success';
    row level, tanpa GROUP BY
  extract_daily_summary      : daily_est_income_per_store_for_landlord (DWH) di range & outlet yang sama (untuk validasi)

TRANSFORM (transform.py)
  build_order_income: map_payment_type (PAYMENT_TYPE_MAP, NULL -> 'Unknown'), commission_pct per baris,
    sales_type_commission = subtotal * commission_pct (tidak dibulatkan di kode; DECIMAL(20,4))
  check_duplicates : raise kalau (order_id, transaction_id) dobel di sumber
  validate_vs_daily: jumlah per date + outlet + payment type harus sama dengan tabel daily
    (selisih subtotal / tax_amount / total_sales / sales_type_commission < 0.01, total_order = COUNT DISTINCT order_id);
    kalau beda -> raise, load tidak jalan
  PAYMENT_TYPE_MAP & COMMISSION_PCT di-copy dari DAG daily dan HARUS identik (validasi = pengaman kalau drift)

LOAD (load.py :: load_order_income)
  Atomic DELETE (date BETWEEN start AND end AND outlet_code IN OUTLET_CODES) + INSERT (engine.begin())
  ke order_est_income_for_landlord. Tidak ada transaksi -> tabel tidak disentuh.

NOTIFIKASI (_send_report_email, inline di etl_orchestrator.py)
  Email HTML via Gmail SMTP (sukses & gagal). Gagal kirim email hanya di-logging.warning.
```

## Sistem Eksternal

- **MySQL Data Lake** — source `transactions_iseller_pusat`.
- **MySQL Data Warehouse** — validasi `daily_est_income_per_store_for_landlord`, target `order_est_income_for_landlord` (DDL referensi: `sql/create_table.sql`, dibuat manual, tidak dieksekusi Airflow).
- **Email (Gmail SMTP)** — kredensial sama pola DAG lain. Tidak memakai Teams.

## Menambah outlet

Tambahkan kode outlet ke `OUTLET_CODES` di `pipelines/extract.py` (satu-satunya tempat; dipakai extract, validasi, dan DELETE).
Outlet itu juga harus ada di `OUTLET_CODES` DAG `f_daily_estimated_income_per_store`, kalau tidak validasi gagal.

## Catatan

- Header ganda di Data Lake (`transaction_id` sama, mis. 00570 pada 2026-03-21, 2026-05-27, 2026-09-29) membuat ETL berhenti (fail loud) di tanggal tersebut.
- Belum ada DAG pengirim API per order; DAG itu nanti subscribe ke Asset `order_est_income_for_landlord`.
