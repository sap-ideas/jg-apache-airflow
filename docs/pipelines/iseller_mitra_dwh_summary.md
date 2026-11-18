# Rantai harian iSeller Mitra + 6 DAG `etl_iseller_mitra_dwh_summary_*`

Mulai 2026-11-19 agregasi DWH Mitra (sebelumnya CRON di luar Airflow) dikerjakan 6 DAG Airflow, satu
DAG per tabel. Semua DAG Mitra membentuk satu rantai; **hanya ingestion yang punya jadwal** (06:00 WIB),
DAG lain jalan begitu DAG sebelumnya selesai. Kode rantai: `plugins/common/mitra_chain.py`.

## Alur

```
06:00  etl_iseller_mitra_ingestion (H-1)
          ├─ gagal  -> STOP. Rerun manual dengan trigger_next dicentang -> seluruh rantai jalan.
          └─ sukses
                ▼
       etl_iseller_mitra_backfilling (H-8..H-7)        lanjut apa pun hasilnya
                ▼
       etl_iseller_mitra_fulfillment (H-8..H-1)        lanjut apa pun hasilnya
                ▼
       etl_iseller_mitra_cleaning (run_mode datalake_only)
          window H-8..H-1 kalau backfilling menambah order / gagal, atau fulfillment membuat ≥1 order fulfilled;
          selain itu H-1.                               lanjut apa pun hasilnya (DWH dobel lebih mudah dicek daripada kosong)
                ▼
       DWH (H-8..H-1), satu tabel gagal -> tabel berikutnya tetap jalan
          1 items -> 2 bundlings -> 3 transactions -> 4 hourly -> 5 sales_type -> 6 companywide
                                        └─ gagal -> 6 companywide TIDAK dipicu (menyalin tabel transactions)
```

Window paling jauh H-8 di semua DAG (H-1 = kemarin Asia/Jakarta dari jadwal ingestion, dibawa lewat conf
`chain_h1`). Durasi DWH 8 hari (diukur dry_run 2026-11-18): items ~96 dtk, bundlings ~63, transactions ~12,
hourly ~42, sales_type ~24, companywide ~3.

## 6 DAG DWH

| # | dag_id | Tabel DWH | Baris yang dipegang |
|---|---|---|---|
| 1 | `etl_iseller_mitra_dwh_summary_items` | `daily_items_summary_iseller_pusat` | `sources='iseller_mitra'` |
| 2 | `etl_iseller_mitra_dwh_summary_bundlings` | `daily_bundling_summary_iseller_pusat` | `sources='iseller_mitra'` |
| 3 | `etl_iseller_mitra_dwh_summary_transactions` | `daily_transaction_summary_iseller_pusat` | `sources='iseller_mitra'` |
| 4 | `etl_iseller_mitra_dwh_summary_hourly` | `hourly_transaction_summary_iseller_pusat` | `sources='iseller_mitra'` |
| 5 | `etl_iseller_mitra_dwh_summary_sales_type` | `daily_sales_type_summary_iseller_pusat` | `sources='iseller_mitra'` |
| 6 | `etl_iseller_mitra_dwh_summary_companywide` | `daily_outlet_transaction_summary_companywide_new` | `datasource='ISELLER' AND ownership_model='MITRA'` (sumber: tabel DWH #3) |

SQL dan transform sama persis dengan pipeline re-dump di `iseller_mitra_cleaning/pipelines/pipeline_daily_*.py`
(subquery mapping outlet ada sekali di `plugins/common/mitra_dwh_summary.py`). Parity terhadap isi DWH hasil
CRON, 2026-11-10 s/d 2026-11-17: 6 tabel, 8 tanggal, jumlah baris dan kolom nominal identik.

Tiap DAG: extract → transform → load (`replace_rows`: DELETE `BETWEEN start AND end` + baris milik pipeline,
lalu INSERT, **satu transaksi**; gagal di tengah = rollback, DWH tidak berubah). Hasil hitung 0 baris padahal
DWH punya baris di window = gagal tanpa menghapus. Email `[OK]` / `[FAILED]` per tabel (gagal hanya di
percobaan terakhir; `retries=2`, aman karena idempotent).

## Task tiap DAG di rantai

```
run_pipeline ──(apa pun hasilnya)──> next_conf ──> trigger_<DAG berikutnya>
     └──────────────────────────────> status_akhir (merah kalau run_pipeline gagal)
```

`status_akhir` perlu karena tanpa task ini run terlihat hijau walau `run_pipeline` gagal (task terakhirnya
sukses). `next_conf` menentukan conf DAG berikutnya: `start_date`, `end_date`, `trigger_next`, `chain_h1`,
`failed` (dag_id yang gagal di rantai ini), dan khusus `dl_changed` (backfilling → fulfillment) serta
`run_mode` (fulfillment → cleaning). `companywide` adalah DAG terakhir: hanya `run_pipeline`.

## Trigger manual

| Param | Isi |
|---|---|
| `start_date` / `end_date` | Date picker, wajib, inklusif. Default DAG DWH: H-8 s/d H-1 saat file di-parse. |
| `trigger_next` | Default **mati**: hanya DAG ini yang jalan. Centang = lanjut ke DAG berikutnya. DAG DWH meneruskan window yang sama; DAG Data Lake di tengah rantai memakai H-1 hari ini. |
| `dry_run` (DWH) | Hitung lalu bandingkan per tanggal dengan isi DWH sekarang (baris + kolom nominal). Tidak menulis, tidak email, tidak memicu DAG berikutnya. |

Contoh: re-dump satu tabel = trigger DAG itu saja. Re-dump semua tabel = trigger `..._items` dengan
`trigger_next` dicentang. Companywide untuk window tertentu: pastikan DAG transactions untuk window itu sudah sukses.

## Catatan

- Jangan pause DAG di tengah rantai: run yang dipicu menunggu (queued) sampai DAG itu dinyalakan lagi.
  Untuk melewati satu langkah, trigger manual DAG berikutnya.
- `max_active_runs=1` tiap DAG: rantai kedua di hari yang sama antre, tidak bentrok.
- Cleaning `run_mode=full` (trigger manual) masih me-redump DWH dengan kode lamanya sendiri; rantai harian
  memakai `datalake_only` sehingga DWH hanya ditulis 6 DAG ini.
