# `iseller_mitra_backfilling` — ETL iSeller Mitra (Backfilling order yang tidak ada di Data Lake)

Menarik ulang order **H-8 s/d H-7** dari API iSeller Mitra dan memasukkan order yang **tidak ada sama sekali**
di Data Lake (ingest harian tidak menariknya: outlet baru, panggilan API gagal, dst.). Berhenti di Data Lake — tidak
ada redump DWH.

Beda dengan [`iseller_mitra_fulfillment`](./iseller_mitra_fulfillment.md): fulfillment hanya memperbaiki order yang
sudah ada di header dengan `fulfillment_status = 'none'`. Backfilling ini mengisi order yang belum punya header sama sekali.
Tabel summary DWH tidak disentuh (sama dengan `etl_iseller_mitra_fulfillment`): setelah backfilling memasukkan order, redump manual lewat `etl_iseller_mitra_cleaning` dengan `run_mode = dwh_only` untuk tanggal yang sama.

**Urutan dengan DAG lain**: jalan 06:10, setelah ingest harian (~06:08, menulis H-1) dan sebelum `etl_iseller_mitra_fulfillment` (07:50) dan `etl_iseller_mitra_cleaning` (07:51). DAG ini menulis H-8 s/d H-7, jadi tidak beririsan dengan ingest harian. Fulfillment dan cleaning **tidak menunggu** DAG ini; run biasanya selesai dalam hitungan menit, tapi jangan trigger manual yang panjang di sekitar 08:00-09:00.

## Metadata DAG

File: `dag_etl_iseller_mitra_backfilling.py`

| Field | Nilai |
|---|---|
| `dag_id` | `etl_iseller_mitra_backfilling` |
| `schedule_interval` | `"10 6 * * *"` — harian 06:10 Asia/Jakarta (setelah ingest harian ~06:08) |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "iseller", "mitra", "backfilling", "datalake", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=timedelta(minutes=5)` |
| Task | 1x `PythonOperator("run_pipeline")`, `execution_timeout=timedelta(hours=3)` |
| Param | `start_date`, `end_date`, `dry_run` — hanya dipakai saat **Trigger DAG w/ config** (manual) |

**Date window**: run terjadwal menarik **H-8 s/d H-7** (Asia/Jakarta, dari `data_interval_end`). Cek order yang sudah ada
memakai window yang sama ditambah 1 hari (transaksi/refund yang tercatat di hari berikutnya). Trigger manual memakai
`start_date`/`end_date` dari form (isi awal = H-8 s/d H-7; start setelah end ditolak). `dry_run = True` (manual saja) = tarik +
proses lengkap tapi **tidak menulis Data Lake dan tidak kirim email/Teams**; run terjadwal selalu menulis.

## Alur Pipeline

```
1. Token: Airflow Variable access_token_mitra (fallback env var) → kosong = MissingAccessTokenError (tanpa retry)
2. order_id yang sudah ada di transactions_iseller_mitra (transaction_date window + 1 hari)
3. Extract: getstoreinfo (semua outlet aktif, bukan dari Data Lake) + GetProducts + GetOrders
   per (outlet x jendela 4 jam), jendela berakhir xx:59:59.999, 10 worker, retry 5x (jeda 2/4/8/16 detik)
   ├─ Panggilan yang tetap gagal diulang dengan 3 worker
   └─ Respons 0 order diulang sekali (menangkap kegagalan diam-diam)
4. Filter: hanya order yang belum ada di Data Lake (1 order_id diambil sekali walau tertarik di 2 jendela)
5. Transform: logika sama dengan iseller_mitra_fulfillment / notebook ingest v2 (lihat bawah)
6. Load: header + details dalam 1 TRANSAKSI; order_id existing dicek ulang di dalam transaksi
7. Email + Teams. Kalau ada panggilan API yang TETAP gagal: data yang berhasil tetap di-load, lalu task digagalkan
   (email/Teams "PARTIAL") supaya Airflow retry. Run ulang hanya memasukkan order yang belum ada.

   ⛔ Berhenti: MissingAccessTokenError / TokenExpiredError (API 401) → task GAGAL tanpa retry;
      exception lain → task GAGAL, Airflow retry (aman, idempoten)
```

## Detail

- **Order tanpa transaksi (belum dibayar) tidak dimasukkan.** Order yang saat ditarik belum punya transaksi tidak punya
  baris header; details tanpa header akan masuk dengan kolom header kosong (`transactions_type`, `sales_type`, dst.) dan
  meleset dari akurasi (pernah terjadi di tgl 18 dan 22 Okt 2026). Order itu tertarik lagi di run berikutnya setelah transaksinya ada.
- **Transform** (`pipelines/transform.py`) disalin dari `iseller_mitra_fulfillment/pipelines/pipeline_backfill_fulfillment.py`
  (sudah diuji terhadap notebook ingest `transaction_janji_jiwa_mitra_v2`): sumber diskon **`discounts[]`** (`discount_amount`; khusus
  Mitra, karena diskon "Custom" kasir tidak masuk `discount_order_amount`), pajak dari `tax_percentage` API, `combo_share`, group promo/diskon per
  `(order_detail_id, transactions_type)`, sales type 7 kondisi, list SJW 40 ID. **Kalau transform diubah, ubah juga di file pasangannya.**
  Details tidak di-`drop_duplicates` (komponen combo identik itu sah).
- **Kenapa 1 transaksi:** kalau header berhasil ditulis tapi details gagal, order terlihat "sudah ada" di run berikutnya dan
  details-nya tidak pernah terisi.
- **Error deadlock iSeller** (`Transaction ... was deadlocked ... chosen as the deadlock victim`) bersifat sementara dan bisa muncul di
  `GetOrders`, `GetProducts`, maupun `getstoreinfo`; semuanya di-retry 5x. Panggilan `GetOrders` yang tetap gagal tidak
  membuang seluruh tarikan (lihat alur langkah 3 dan 7).
- **Order tanpa details** (header ada, `fulfillment_status='none'`) bukan tugas DAG ini — itu tugas `iseller_mitra_fulfillment`.

## Sistem Eksternal

- **iSeller Mitra REST API** — `https://janjijiwamitra.isellershop.com/api/v2/{getstoreinfo,GetProducts,GetOrders}` (Bearer token).
- **MySQL Data Lake** (`config_dw.db_resource`) — `transactions_iseller_mitra` dan `transactions_items_iseller_mitra` (append).
- **Email (Gmail SMTP)** — pengirim `saputra.christabel20@gmail.com` (password dari env `GMAIL_SMTP_PASSWORD`).
- **MS Teams via Power Automate** — `teams_report_iseller_mitra_backfilling.py`: 🟢 completed (termasuk "sudah lengkap"),
  🟡 partial (panggilan API gagal), 🔴 pipeline berhenti. Best-effort via `post_teams_safe`.

## Connections / Variables

- Airflow Variable `access_token_mitra` — token API iSeller Mitra (**expired berkala**; Variable didahulukan atas env var,
  jadi update Variable-nya kalau token diganti).
- Airflow Variable `POWER_AUTOMATE_TEAMS_WEBHOOK_URL`; password SMTP dari env `GMAIL_SMTP_PASSWORD`.

## Manual Run

- **Airflow UI → Trigger DAG w/ config**: isi `start_date`/`end_date`; centang `dry_run` untuk uji tanpa menulis. Untuk periode
  panjang, pecah per bulan (`execution_timeout` 3 jam; sekitar 5.600 panggilan API per hari yang ditarik).
- Notebook referensi `backfilling_janji_jiwa_mitra_v3.ipynb` ada di folder yang sama (alur yang sama untuk dijalankan manual).
