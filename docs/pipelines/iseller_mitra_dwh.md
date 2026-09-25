# `iseller_mitra_dwh` — ETL iSeller Mitra

DAG dengan pendekatan berbeda dari `iseller_pusat_dwh`: alih-alih selalu redump,
DAG ini **mendeteksi duplikat data lebih dulu** dan hanya melakukan cleanup +
redump kalau memang ada masalah — hemat resource kalau data sudah bersih.

Catatan: task ini menyebut file orkestrator sebagai `etl_orchestrator.py`, tapi
di folder ini nama filenya adalah **`pipelines/redump_etl_functions.py`**
(docstring-nya sendiri menyebut diri "Master Orchestrator - Re-Dump All DWH
Summary Tables (iSeller Mitra)"). Ada juga file `.xlsx` nyasar
(`query_result_2026-06-12T08_41_09....xlsx`) di root folder DAG — tidak dipakai kode apa pun.

## Metadata DAG

File: `dag_etl_iseller_mitra_dwh.py`

| Field | Nilai |
|---|---|
| `dag_id` | `etl_iseller_mitra_dwh_summary` |
| `schedule_interval` | `"30 8 * * *"` — harian 08:30 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "iseller", "mitra", "dwh", "production"]` |
| `default_args` | sama dengan DAG lain — `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |

**Date window**: `start_date == end_date` = "kemarin" di Asia/Jakarta (single-day window, beda dari `iseller_pusat_dwh` yang rolling 9 hari).

## Alur Pipeline (`redump_etl_functions.run_all`)

```
STEP 1 — check_duplicates.check_duplicates()
   ├─ Query transactions_iseller_mitra (header) & transactions_items_iseller_mitra (detail)
   │  untuk cari duplicate key di Data Lake, rentang tanggal target
   ├─ Kalau TIDAK ADA duplikat → generate_report_outlet_01154(), kirim email "data aman" (hijau),
   │  kirim Teams "clean" card → SELESAI, tidak lanjut ke STEP 2/3
   └─ Kalau ADA duplikat → generate report, kirim email alert (merah), lanjut ke STEP 2

STEP 2 — pipeline_clean_duplicates.run_clean_duplicates()  [hanya kalau ada duplikat]
   ├─ Load hanya tabel yang kotor (header dan/atau detail, sesuai flag)
   ├─ drop_duplicates() di pandas
   ├─ DELETE rentang tanggal dari Data Lake
   └─ Insert ulang data yang sudah bersih

STEP 3 — Redump tabel summary (SELEKTIF):
   ├─ daily_items_summary_iseller_pusat      (pipeline_daily_items.py)      — SELALU jalan
   ├─ daily_bundling_summary_iseller_pusat   (pipeline_daily_bundlings.py)  — SELALU jalan
   ├─ daily_transaction_summary_iseller_pusat(pipeline_daily_transactions.py)— hanya kalau header duplikat
   ├─ hourly_transaction_summary_iseller_pusat(pipeline_hourly_transactions.py)— hanya kalau header duplikat
   └─ daily_sales_type_summary_iseller_pusat (pipeline_daily_sales_type.py)  — hanya kalau header duplikat

STEP 4 — Email HTML "Re-Dump Completed" (before/after per tabel)

STEP 5 — Teams Adaptive Card "duplicate flow" (before vs after cleaning), kecuali skip_dl_check
```

**Kenapa items/bundlings selalu redump** meski cuma detail yang duplikat (bukan header)? Karena keduanya JOIN header+detail sekaligus (dijelaskan di komentar `redump_etl_functions.py:257-271`), jadi harus selalu fresh; 3 pipeline lain murni agregat dari header saja, jadi bisa di-skip kalau header tidak bermasalah.

Semua 4 tabel di sini ditulis sebagai `sources='iseller_mitra'` ke tabel yang **sama** dengan `iseller_pusat_dwh` (mis. `daily_transaction_summary_iseller_pusat`), dibedakan lewat kolom `sources` — jadi kedua DAG berbagi tabel target tapi dengan filter `sources` yang berbeda saat delete/redump.

## Sistem Eksternal

- **MySQL Data Lake** — source: `transactions_iseller_mitra` (header), `transactions_items_iseller_mitra` (detail), filter umum `payment_status='paid' AND fulfillment_status='fulfilled' AND transactions_status='success'`, exclude `sales_type LIKE '%jiwa+%'`.
- **MySQL Data Warehouse** — 5 tabel summary bersuffix `_iseller_pusat`, disaring `sources='iseller_mitra'`.
- **Reference/dimension**: `outlet_mapping`, `outlet_brand_mapping`, `kbn_transition_outlets` (sama seperti di `iseller_pusat_dwh`, termasuk logic transisi ownership berbasis tanggal).
- **Email (Gmail SMTP)** — 3 jenis: "duplicated data alert" (merah), "data aman" (hijau), dan "Re-Dump Completed" (STEP 4). Kredensial hardcoded, sama dengan DAG lain (`saputra.christabel20@gmail.com` → `athens.jiwagroup@gmail.com`, `lahia.ardhanlahia@gmail.com`).
- **MS Teams via Power Automate** — `teams_report_iseller_mitra.py`: `send_mitra_dwh_teams_report_clean()` (hijau) dan `send_mitra_dwh_teams_report_duplicate_flow()` (merah, before/after + statistik cleanup). Dibungkus try/except non-fatal.

## Connections / Variables

- Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` (fallback env var).
- Tidak ada Airflow Connection — kredensial DB dari `config/config_db_datawarehouse.py`.
- Kredensial SMTP hardcoded, **diduplikasi** di 2 file (`check_duplicates.py` dan `redump_etl_functions.py`) alih-alih disentralkan.

## Pola Kode Khas DAG Ini

- Setiap pipeline redump punya signature seragam `run_<nama>(start_date, end_date, config_dw) -> (deleted, dumped, before, after)`.
- Try/except per-pipeline di orchestrator — status `SUCCESS`/`FAILED` per tabel dicatat independen, satu tabel gagal tidak menghentikan yang lain.
- Semua query SQL dibangun via f-string interpolation langsung (bukan parameterized) — pola berulang di seluruh project, risiko rendah karena input hanya dari tanggal yang dihitung internal Airflow.
