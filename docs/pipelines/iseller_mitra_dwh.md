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
| Param | `run_mode`: `full` (default, run terjadwal) / `datalake_only` (cek + bersihkan DL saja, DWH tidak disentuh) / `dwh_only` (skip DL, re-dump 5 tabel DWH). Dipilih saat Trigger DAG w/ config |

**Date window**: `start_date == end_date` = "kemarin" di Asia/Jakarta (single-day window, beda dari `iseller_pusat_dwh` yang rolling 9 hari).

## Alur Pipeline (`redump_etl_functions.run_all`)

```
STEP 1 — check_duplicates.check_duplicates()
   ├─ Query transactions_iseller_mitra (header) & transactions_items_iseller_mitra (detail)
   │  untuk cari duplicate key di Data Lake, rentang tanggal target
   ├─ Kalau TIDAK ADA duplikat → generate_report_outlet_01154(), kirim email "data aman" (hijau),
   │  kirim Teams "clean" card → SELESAI, tidak lanjut ke STEP 2/3
   │  (pemicu lanjut: header dobel ATAU detail NON-combo dobel ATAU key combo dobel; key combo dobel
   │   belum tentu duplikat karena komponen identik bisa sah, jadi diputuskan master di STEP 3)
   ├─ Ada type details di luar standard/variant/comboset → STOP (alert), DL tidak disentuh
   └─ Kalau ADA duplikat → generate report, kirim email alert (merah), lanjut ke STEP 2

STEP 2 — master_bundlings.load_bundling_slot_count()  [hanya kalau ada duplikat]
   ├─ Tarik master bundling dari API iSeller Mitra GetProducts (jumlah slot per bundling)
   └─ GAGAL (token tidak ada / expired / API error) → STOP: email + Teams alert, task gagal,
      Data Lake & DWH TIDAK disentuh. Tidak ada fallback/cadangan.

STEP 3 — pipeline_clean_duplicates.run_clean_duplicates()
   ├─ Load header + detail, dedup header (order_id, transaction_id) dan detail combo-aware
   │  (dedup_details.py: dup_factor combo = row combo / jumlah slot master; tidak bisa dipastikan
   │   → tidak dipotong + needs_manual_check)
   ├─ Hanya tabel yang terdeteksi dobel di STEP 1 dan berubah yang ditulis ulang
   ├─ DELETE + insert ulang dalam 1 transaksi (gagal → rollback, Data Lake utuh)
   └─ Kalau hanya key combo dobel dan master memutuskan tidak ada row yang dibuang → email "aman" + Teams, tanpa re-dump DWH

STEP 4 — Redump tabel summary (SELEKTIF):
   ├─ daily_items_summary_iseller_pusat      (pipeline_daily_items.py)      — SELALU jalan
   ├─ daily_bundling_summary_iseller_pusat   (pipeline_daily_bundlings.py)  — SELALU jalan
   ├─ daily_transaction_summary_iseller_pusat(pipeline_daily_transactions.py)— hanya kalau header duplikat
   ├─ hourly_transaction_summary_iseller_pusat(pipeline_hourly_transactions.py)— hanya kalau header duplikat
   └─ daily_sales_type_summary_iseller_pusat (pipeline_daily_sales_type.py)  — hanya kalau header duplikat

STEP 5 — Email HTML "Re-Dump Completed" (before/after per tabel + ringkasan dedup) dan
         Teams Adaptive Card "duplicate flow" (before vs after cleaning), kecuali skip_dl_check.
         Kalau ada pipeline DWH FAILED, task Airflow digagalkan SETELAH report terkirim.
```

Notebook manual per step: `manual_run_iseller_mitra_cleaning.ipynb` (folder DAG; `DRY_RUN = True` bawaan, `DWH_ONLY` untuk skip DL).

Param `run_all(..., dry_run=True)`: jalankan STEP 1-3 tanpa email, tanpa DELETE/re-dump, lalu berhenti.
Detail logic dedup, daftar `reason` error master, dan hasil uji: [`FLOW_dedup_details_comboset.md`](../../dags/iseller_mitra_dwh/FLOW_dedup_details_comboset.md)
dan [`KNOWLEDGE_comboset_komponen_identik_vs_duplicate.md`](../../dags/iseller_mitra_dwh/KNOWLEDGE_comboset_komponen_identik_vs_duplicate.md).

**Kenapa items/bundlings selalu redump** meski cuma detail yang duplikat (bukan header)? Karena keduanya JOIN header+detail sekaligus (dijelaskan di komentar `redump_etl_functions.py:257-271`), jadi harus selalu fresh; 3 pipeline lain murni agregat dari header saja, jadi bisa di-skip kalau header tidak bermasalah.

Semua 4 tabel di sini ditulis sebagai `sources='iseller_mitra'` ke tabel yang **sama** dengan `iseller_pusat_dwh` (mis. `daily_transaction_summary_iseller_pusat`), dibedakan lewat kolom `sources` — jadi kedua DAG berbagi tabel target tapi dengan filter `sources` yang berbeda saat delete/redump.

## Sistem Eksternal

- **MySQL Data Lake** — source: `transactions_iseller_mitra` (header), `transactions_items_iseller_mitra` (detail), filter umum `payment_status='paid' AND fulfillment_status='fulfilled' AND transactions_status='success'`, exclude `sales_type LIKE '%jiwa+%'`.
- **MySQL Data Warehouse** — 5 tabel summary bersuffix `_iseller_pusat`, disaring `sources='iseller_mitra'`.
- **Reference/dimension**: `outlet_mapping`, `outlet_brand_mapping`, `kbn_transition_outlets` (sama seperti di `iseller_pusat_dwh`, termasuk logic transisi ownership berbasis tanggal).
- **API iSeller Mitra** — `GetProducts` (master bundling), token `access_token_mitra`.
- **Email (Gmail SMTP)** — 4 jenis: "duplicated data alert" (merah), "data aman" (hijau), "Re-Dump Completed" (STEP 5), dan "STOPPED" (master/token gagal). Password dari env `GMAIL_SMTP_PASSWORD`; pengirim `saputra.christabel20@gmail.com` → `athens.jiwagroup@gmail.com`, `lahia.ardhanlahia@gmail.com`.
- **MS Teams via Power Automate** — `teams_report_iseller_mitra.py`: `send_mitra_dwh_teams_report_clean()` (hijau), `send_mitra_dwh_teams_report_duplicate_flow()` (merah, before/after + statistik cleanup), dan `send_mitra_dwh_teams_report_master_error()` (merah, STOPPED). Dibungkus try/except non-fatal.

## Connections / Variables

- Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL` (fallback env var).
- Airflow Variable `access_token_mitra` (fallback env var, lalu file `.env`). Wajib ada: tanpa token valid DAG berhenti.
- Tidak ada Airflow Connection — kredensial DB dari `config/config_db_datawarehouse.py`.
- Kode kirim email SMTP **diduplikasi** di 2 file (`check_duplicates.py` dan `redump_etl_functions.py`) alih-alih disentralkan.

## Pola Kode Khas DAG Ini

- Setiap pipeline redump punya signature seragam `run_<nama>(start_date, end_date, config_dw) -> (deleted, dumped, before, after)`.
- Try/except per-pipeline di orchestrator — status `SUCCESS`/`FAILED` per tabel dicatat independen, satu tabel gagal tidak menghentikan yang lain. Setelah semua pipeline dan report selesai, task di-raise kalau ada yang FAILED (jadi task Airflow tidak terlihat sukses).
- Semua query SQL dibangun via f-string interpolation langsung (bukan parameterized) — pola berulang di seluruh project, risiko rendah karena input hanya dari tanggal yang dihitung internal Airflow.
