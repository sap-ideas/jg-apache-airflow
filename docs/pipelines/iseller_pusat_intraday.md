# `iseller_pusat_intraday` — ETL iSeller Pusat (data hari H, tiap jam, tabel intraday Data Lake)

Menarik order **hari ini** (00:00 WIB s/d sekarang) dari API iSeller Pusat tiap jam **06:00–21:00 WIB** dan memperbarui dua
tabel khusus di Data Lake yang hanya berisi **data hari ini**: `transactions_iseller_pusat_intraday` (header) dan
`transactions_items_iseller_pusat_intraday` (details). Tujuannya data semi real-time untuk analisis di hari H tanpa menunggu
ingest H-1 + fulfillment 09:00 besoknya. Data kemarin dihapus otomatis di run pertama hari berikutnya.

**Tabel Data Lake utama (`transactions_iseller_pusat*`) dan DWH tidak disentuh.** SOP "order closed tidak berubah" tetap
berlaku di sana; tabel intraday adalah salinan kerja yang boleh berubah mengikuti API.

## Metadata DAG

File: `dag_etl_iseller_pusat_intraday.py`

| Field | Nilai |
|---|---|
| `dag_id` | `etl_iseller_pusat_intraday` |
| `schedule` | `"0 6-21 * * *"` — tiap jam 06:00–21:00 Asia/Jakarta (16 run/hari) |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["etl", "iseller", "pusat", "intraday", "datalake", "production"]` |
| `default_args` | `owner='data-team'`, `retries=1`, `retry_delay=timedelta(minutes=5)` |
| Task | 1x `PythonOperator("run_pipeline")`, `execution_timeout=timedelta(minutes=30)` |
| Param | `dry_run` — hanya dipakai saat **Trigger DAG w/ config** (manual) |

**Date window**: selalu **hari ini** (00:00 WIB s/d saat run jalan), dihitung dari jam saat task berjalan — tidak ada param tanggal.
`dry_run = True` (manual saja) = tarik + proses lengkap + hitung yang akan ditulis, tapi **tidak menulis tabel dan tidak kirim email**;
run terjadwal selalu menulis. Rencana waktu: run jam 21:00 sekitar 4–5 menit; `execution_timeout` 30 menit sebagai pengaman.

## Alur Pipeline

```
1. Token: Airflow Variable access_token_pusat (fallback env var) → kosong = MissingAccessTokenError (tanpa retry)
   + getstoreinfo (semua outlet aktif) + baca isi tabel intraday saat ini (1 hari, dibaca utuh)
2. Extract: GetOrders 1 call per outlet untuk 00:00 WIB s/d sekarang (akhir jendela xx:xx:xx.999), 10 worker, retry 5x
   ├─ Respons sebanyak 200 order (batas API) = terpotong → jendela dibelah dua, rekursif, tanpa celah/tumpang tindih
   └─ Outlet yang tetap gagal diulang dengan 3 worker; call kosong TIDAK di-re-fetch (lihat Detail)
3. Saring: hanya order BARU / BERUBAH dibanding tabel yang lanjut (lihat Aturan upsert)
4. Transform (hanya order hasil saring): salinan transform iseller_pusat_backfilling + GetProducts
5. Load per order dalam 1 TRANSAKSI: rollover H-1 → insert order baru → ganti order berubah → tahan order yang menyusut
6. Email ringkasan (sukses dan gagal, isi sama). Kalau ada outlet yang TETAP gagal: data yang berhasil tetap di-load,
   email "PARTIAL", lalu task digagalkan supaya Airflow retry.

   ⛔ Berhenti: MissingAccessTokenError / TokenExpiredError (API 401) → task GAGAL tanpa retry (email STOPPED);
      exception lain → task GAGAL, Airflow retry (aman, idempoten)
```

## Aturan upsert (per order)

| Kondisi order | Yang terjadi |
|---|---|
| Tidak ketarik di run ini (outlet gagal / kosong) | **Tidak disentuh**, versi jam sebelumnya tetap ada |
| Baru | Insert header + details |
| Berubah: status / payment_status / fulfillment_status beda, ada transaksi baru (mis. refund), status transaksi beda, atau details sudah ada di API tapi belum di tabel | Versi lama dihapus (header + details) lalu versi terbaru di-insert |
| Berubah tapi versi baru **menyusut** (baris header atau details lebih sedikit dari yang di tabel) | **Ditahan**: versi lama dipertahankan, jumlah + contoh `order_reference` muncul di email |
| Tidak berubah | Dilewati (tidak ditulis, tidak di-transform) |
| Belum punya transaksi (belum dibayar) | Dilewati, ketarik lagi di run berikutnya |

Satu-satunya penghapusan massal = **rollover H-1**: baris dengan `order_date` (UTC) sebelum 00:00 WIB hari ini dihapus di
transaksi yang sama (efektif di run 06:00; pakai `order_date` karena GetOrders menarik berdasarkan tanggal pembuatan order).

## Detail

- **Kenapa upsert per order, bukan DELETE semua lalu dump:** kalau 1 tarikan kebetulan tidak membawa detail/order tertentu
  (call gagal, outlet kosong, order belum fulfilled), delete-all membuatnya hilang dari tabel. Upsert per order tidak pernah
  menghapus order yang tidak ketarik, dan order yang tadinya `fulfillment_status='none'` (details kosong) ikut terisi begitu fulfilled.
- **Kenapa hapus-lalu-insert per order, bukan UPDATE kolom:** details tidak punya key unik (komponen combo identik itu sah),
  jadi cara aman mengganti isi 1 order = hapus semua baris order itu lalu insert versi terbaru.
- **Batas 200 order per call GetOrders** (diukur 2026-11-23): outlet tersibuk kemarin 1 call = 200 order vs 349 order dengan 6 jendela
  4 jam. Maka respons 200 order dianggap terpotong dan dibelah dua. Jendela < 60 detik yang masih 200 order = gagal keras
  (tidak boleh diam-diam terpotong). Dengan batas yang sama (`created_before` sama), 1 call dan jendela 4 jam menghasilkan order identik
  (0 selisih di 5 outlet tersibuk).
- **Call kosong tidak di-re-fetch:** 694 dari 937 outlet aktif memang tidak punya order (re-fetch memulihkan 0; hanya 1 dari 694 yang
  kemarin ada order). Call yang kosong atau gagal diam-diam hanya berarti order outlet itu belum ter-update jam ini; run berikutnya
  menarik ulang seluruh hari.
- **Ukuran run (diukur 2026-11-23 ±14:45 WIB):** 937 outlet × 1 call = 156 detik (6 call/s), GetProducts 18 detik, transform 8.548 order
  47 detik. Penyaringan "baru/berubah" membuat transform di run-run berikutnya jauh lebih kecil.
- **Transform** (`pipelines/transform.py`) adalah salinan identik `iseller_pusat_backfilling/pipelines/transform.py` (yang identik
  dengan `iseller_pusat_fulfillment/pipelines/pipeline_backfill_fulfillment.py` dan notebook `transaction_janji_jiwa_pusat_v4`):
  split payment (`plugins/common/iseller_split_payment.py`), `combo_share`, pajak dari `tax_percentage` API, sales type, list SJW.
  **Kalau transform diubah, ubah juga di file-file pasangannya.**
- **Perubahan yang tidak terdeteksi:** hanya status/transaksi/details yang dibandingkan, bukan nominal. Order closed tidak berubah nominal
  (SOP), jadi ini disengaja.
- **Order yang dihapus total di iSeller** (bukan void) tidak ikut terhapus di tabel intraday sampai rollover besok; jarang terjadi.
- **Data = snapshot jam run terakhir:** lihat `MAX(load_data_at)` (WIB) untuk tahu seberapa baru. Order belum fulfilled belum punya details,
  jadi revenue hari H pakai header (`transactions_iseller_pusat_intraday`), item mix pakai details.
- **Zona waktu kolom:** `load_data_at` = **WIB** (diisi `load.py`, 1 nilai untuk header + details satu run; beda dari tabel Data Lake utama yang UTC karena
  container Airflow berjalan di UTC), `transaction_date` = WIB, `order_date` dan `closed_date` = UTC (sama dengan tabel utama). Jangan join dengan
  tabel utama berdasarkan jam load.
- **Tabel tanpa kolom `load_data_by`:** sama dengan tabel Data Lake iSeller lain (hanya `load_data_at`).

## Email

Satu format untuk sukses (`[OK]`), parsial (`[ALERT] ... PARTIAL`), dan gagal (`[ALERT] ... STOPPED`; ditambah pesan error). Isi: tanggal data,
jam tarik, durasi, outlet aktif (API), outlet yang punya order, total order (DISTINCT `order_id`), order belum fulfilled, total amount
header dan details, order dari API, order baru, order ter-update, order tidak berubah, order ditahan, baris H-1 dihapus, outlet gagal.
Tidak ada kartu Teams (sengaja: 16 run/hari).

## Sistem Eksternal

- **iSeller Pusat REST API** — `https://janjijiwapusat.isellershop.com/api/v2/{getstoreinfo,GetProducts,GetOrders}` (Bearer token).
- **MySQL Data Lake** (`config_dw.db_resource`) — `transactions_iseller_pusat_intraday` dan `transactions_items_iseller_pusat_intraday`.
  Schema + index (`order_id`, `transaction_date`) ada di [`sql/create_table_intraday.sql`](../../dags/iseller_pusat/iseller_pusat_intraday/sql/create_table_intraday.sql)
  (referensi; dibuat manual di DBeaver sebelum DAG diaktifkan). User DB Airflow butuh SELECT/INSERT/DELETE di kedua tabel.
- **Email (Gmail SMTP)** — pengirim `saputra.christabel20@gmail.com` (password dari env `GMAIL_SMTP_PASSWORD`).

## Connections / Variables

- Airflow Variable `access_token_pusat` — token API iSeller Pusat (**expired berkala**; Variable didahulukan atas env var,
  jadi update Variable-nya kalau token diganti). Token expired = semua run intraday berhenti (email STOPPED tiap jam).
- Password SMTP dari env `GMAIL_SMTP_PASSWORD`.

## Manual Run

- **Airflow UI → Trigger DAG w/ config**: centang `dry_run` untuk uji tanpa menulis; tanpa dry run = tarik hari ini dan upsert sekarang juga.
- Notebook referensi `transaction_janji_jiwa_pusat_v4.ipynb` ada di folder yang sama (versi notebook; **token tidak boleh di-hardcode**,
  notebook ini membacanya dari env var `access_token_pusat` atau input).
