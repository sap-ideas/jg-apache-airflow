# Dedup Data Lake iSeller Mitra: Implementation Knowledge

**Status:** sudah diimplementasikan di `pipelines/` (logic dedup 2026-10-05, diperbarui 2026-10-07).
Perubahan 2026-10-07: cadangan `items / batch / headers` **dihapus** (dup_factor hanya dari master bundling API),
token API gagal = **pipeline berhenti**, DELETE + re-dump di DL dibuat **atomik** (1 transaksi), task
Airflow **gagal** kalau ada pipeline DWH yang FAILED, **key combo dobel ikut memicu cek master** (bukan hanya
info), dan **type details tidak dikenal = pipeline berhenti**.
Logic dedup sudah dijalankan read-only (in-memory) ke data DL 1–7 Okt 2026 dan dry run ke DL 2–4 Okt 2026.
Run asli (DELETE + re-dump) belum pernah dijalankan.

> **Untuk agent yang deploy ke Airflow:** logic di dokumen ini **sudah final dan sudah teruji**.
> Jangan membuat logic dedup baru dan jangan mengubah aturan di bagian 2. Tugasnya cukup deploy file
> di bagian 5, set konfigurasi di bagian 6, lalu verifikasi sesuai bagian 8.

> **Scope:** pipeline ini membersihkan data **H-1** (DAG harian). Untuk data lama, lihat batasan di bagian 9.

---

## 1. Masalah

- Tabel DL `transactions_iseller_mitra` (headers) dan `transactions_items_iseller_mitra` (details) sering
  **dobel satu batch harian penuh**: dataframe yang sama ter-append 2×. Headers dan details bisa dobel
  sendiri-sendiri, misalnya 2 Okt 2026 details dobel tapi headers tidak.
- Logic lama `drop_duplicates(composite key)` menyisakan 1 row per key. Ini **menghapus komponen combo
  yang sah**, karena combo bisa berisi produk yang sama di beberapa slot (contoh "Hangout Drink Combo" 5
  minuman yang sama) dan row-nya identik di semua kolom key.
- `load_data_at` **tidak bisa** membedakan row asli dan duplikatnya, karena nilainya sama persis.

## 2. Aturan yang tidak boleh diubah

1. Dedup key headers: `(order_id, transaction_id)`. Row sale dan refund punya `transaction_id` beda, jadi keduanya tetap ada.
2. Dedup key details: `order_id, order_detail_id, type, bundling_id, product_id, sku, status, payment_status, transactions_type`.
   `status`, `payment_status`, dan `transactions_type` sengaja ada di key supaya row sale/refund tetap ada.
3. Jangan pakai `load_data_at` untuk memisahkan row asli dan duplikat. Kolom ini hanya dipakai untuk mengelompokkan batch.
4. Combo tidak boleh dipotong kalau dobelnya tidak bisa dipastikan. Lebih baik data kelebihan + dilaporkan
   daripada data sah terhapus.
5. Tabel yang tidak dobel **tidak boleh disentuh** (tidak di-DELETE, tidak di-re-dump), demi menjaga keaslian data.
   Tabel hanya ditulis ulang kalau terdeteksi dobel di STEP 1 **dan** ada row yang benar-benar dibuang.
6. `dup_factor` combo **hanya** boleh dari master bundling (API `GetProducts`). **Tidak ada cadangan.**
   Kalau token API tidak ada / expired / API error, pipeline **berhenti** sebelum Data Lake disentuh,
   dan email + Teams wajib menampilkan penyebabnya (bagian 4 step 2).
   *(Diubah 2026-10-07; sebelumnya pipeline tetap jalan pakai cadangan.)*

## 3. Logic dedup

### Istilah
| Nama | Arti |
|---|---|
| `rows_per_key` | jumlah row dengan dedup key details yang sama |
| `bundling_slot_count` | jumlah slot combo di master = jumlah `category` unik di `bundles` (API `GetProducts`). Setiap slot quantity 1 |
| `combo_group_rows` | jumlah row 1 order_detail combo di DL (semua komponen) |
| `dup_factor` | berapa kali data ke-load ke DL (1 = normal, 2 = dobel) |
| `dup_factor_source` | `bundling` (dari master) / `not_in_master` / `slot_mismatch` (dua terakhir: dup_factor = 1, tidak dipotong) |
| `rows_to_keep` | jumlah row yang disimpan per key |
| `needs_manual_check` | combo yang tidak dipotong karena tidak bisa dipastikan, dilaporkan untuk dicek manual |

Fakta master: ketiga jenis combo (single item, produk identik berulang, multi produk pilihan customer)
punya model yang sama, yaitu N slot. Row normal per order_detail combo = N. Paket dibeli lebih dari 1 tidak
menambah row (yang berubah kolom `quantity`).

### Aturan keep
- **Headers:** `drop_duplicates(['order_id', 'transaction_id'])`
- **Details non-combo:** 1 row per key
- **Details combo:** `rows_per_key / dup_factor` row per key. Kalau tidak habis dibagi, simpan semua + `needs_manual_check`.

### Cara menentukan `dup_factor` combo (per order_detail)
Hanya dari master:

`dup_factor = combo_group_rows / bundling_slot_count`, dipakai kalau hasilnya bilangan bulat ≥ 1 (source `bundling`).

Selain itu **tidak dipotong** (`dup_factor = 1`) dan masuk `needs_manual_check`:
- `not_in_master`: `bundling_id` tidak ada di master (mis. paket sudah dinonaktifkan).
- `slot_mismatch`: `combo_group_rows` bukan kelipatan bulat dari `bundling_slot_count` (mis. jumlah slot paket diubah setelah order).

### Contoh
Order berisi 1 Spunbond (non-combo) + 1 Double Toast & Drink Combo (4 slot). Di DL: Spunbond 2 row, tiap
komponen combo 4 row (dobel di DL ×2, plus fan-out promo di ETL ×2 khusus combo).

| | dup_factor | Hasil |
|---|---|---|
| Bundling ada di master | 16 row / 4 slot = 4 | tiap komponen 4/4 = **1** → 4 row ✅ |
| Bundling tidak ada di master | tidak bisa ditentukan → 1 | tiap komponen tetap 4 row → 16 row (kelebihan, masuk `needs_manual_check`) |
| Logic lama | - | 1 row per key (benar di order ini, tapi salah untuk combo produk identik) |

---

## 4. Flow pipeline (`run_all`)

| Step | Isi | File |
|---|---|---|
| 1 | Check duplicates: log current rows, hitung key dobel headers & details (total / non-combo / combo), dan cek `type` details yang tidak dikenal. **Pemicu lanjut ke STEP 2-3 = headers dobel ATAU details non-combo dobel ATAU key combo dobel.** Key combo dobel belum tentu duplikat (komponen identik yang sah), jadi diputuskan oleh master di STEP 3. Email alert STEP 1 hanya untuk headers / non-combo dobel. Tidak ada duplikat sama sekali → email "aman" + Teams, selesai. `type` tidak dikenal → STOP (tabel di bawah) | `check_duplicates.py` |
| 2 | Load `bundling_slot_count` dari API `GetProducts` (semua page sampai page kosong). **Gagal → STOP** (tabel di bawah): email + Teams + task gagal, DL & DWH tidak disentuh | `master_bundlings.py`, `redump_etl_functions.py` |
| 3 | Load DL, dedup headers & details, log [3.1]–[3.5], simpan report CSV, lalu DELETE + re-dump **hanya tabel yang terdeteksi dobel di step 1 dan berubah**, dalam **1 transaksi** (gagal → rollback, DL tetap utuh). Kalau STEP 1 hanya menemukan key combo dobel dan master memutuskan tidak ada row yang dibuang: email "aman" + Teams, **tanpa re-dump DWH**, selesai | `pipeline_clean_duplicates.py` + `dedup_details.py` |
| 4 | Re-dump DWH: hanya details dobel → 2 tabel (items, bundlings). Selain itu → 5 tabel. Logic redump tidak diubah | `redump_etl_functions.py` |
| 5 | Email report (tabel ringkasan dedup, tanpa lampiran) + Teams. Kalau ada pipeline DWH FAILED: subject `[FAILED PIPELINE]` dan task di-raise **setelah** report terkirim | `redump_etl_functions.py`, `teams_report_iseller_mitra.py` |

### Run mode (parameter DAG `run_mode`, 2026-10-07)

Dipilih saat Trigger DAG w/ config (default `full`, dipakai run terjadwal). Pola sama dengan `etl_iseller_pusat_dwh`.

| `run_mode` | `run_all(...)` | Yang dikerjakan |
|---|---|---|
| `full` | default | STEP 1–5 |
| `datalake_only` | `skip_dwh=True` | STEP 1–3 (DL benar-benar dibersihkan), STEP 4 dilewati: DWH tidak disentuh. Email + Teams versi Data Lake saja |
| `dwh_only` | `skip_dl_check=True` | Lewati STEP 1–3, langsung re-dump **5** tabel DWH dari DL apa adanya. Hanya email report (tanpa Teams) |

`datalake_only` tanpa duplikat sama dengan `full` tanpa duplikat (email + Teams "aman", selesai). `skip_dl_check` dan
`skip_dwh` tidak boleh True bersamaan (`ValueError`). Kegunaan utama `dwh_only`: DL sudah bersih (mis. run `full`
sebelumnya macet di STEP 4) tapi DWH belum dibangun ulang. Trigger `full` biasa **tidak** akan me-re-dump DWH, karena STEP 1 sudah
melihat DL bersih.

Parameter `run_all`:
- `skip_dl_check=True`: lewati step 1–3.
- `skip_dwh=True`: berhenti setelah step 3 (run mode `datalake_only`).
- `dry_run=True`: jalankan step 1–3 tanpa email, tanpa DELETE/re-dump, lalu berhenti (return `clean_stats`). Default `False`.

### Step 2 gagal: STOP, bukan cadangan

Notifikasi (email `[ALERT] ETL iSeller Mitra - STOPPED - <judul>` + card Teams merah) berisi pesan error asli dari API,
`reason`, dan langkah tindak lanjut. Data Lake & DWH **tidak disentuh**, jadi aman di-rerun. Dengan `dry_run=True`
pipeline tetap berhenti dengan error yang sama, tapi tanpa email dan Teams.

| `reason` | Kondisi | Retry otomatis |
|---|---|---|
| `token_not_found` | `access_token_mitra` tidak ada di Variable / env / .env | Tidak |
| `token_expired` | API balas 401, atau HTTP 200 dengan body error token | Tidak |
| `token_invalid` | API balas 403 | Tidak |
| `server_error` | API balas 5xx | Ya |
| `connection_error` | timeout / koneksi gagal | Ya |
| `invalid_response` | response bukan JSON / format `GetProducts` tidak dikenal | Tidak |
| `http_error` | HTTP error lain | Tidak |
| `unexpected_error` | error lain yang tidak terduga | Tidak |
| `empty` | API tidak mengembalikan bundling | Tidak |
| `truncated` | paging belum habis setelah 200 page, master bisa terpotong | Tidak |

Tidak ada retry untuk reason yang tidak sementara: error dilempar sebagai `AirflowFailException`.
`run_clean_duplicates` juga menolak berjalan (`ValueError`) kalau master kosong / gagal, sebagai pengaman kedua.

### Pengaman: `type` details tidak dikenal → STOP

`KNOWN_DETAIL_TYPES = ('standard', 'variant', 'comboset')` di `dedup_details.py`. Kalau ada row dengan `type` lain
(atau NULL) di periode itu, pipeline berhenti dengan `reason = unknown_detail_type` (email + Teams, tanpa retry),
DL & DWH tidak disentuh. Alasannya: type di luar `comboset` dianggap non-combo, jadi komponen combo identik yang sah
akan dipotong jadi 1 row per key. `dedup_details_v2` juga raise `ValueError` sebagai pengaman kedua.
Nilai `type` sejak 1 Sep 2026: `comboset` 26.658, `standard` 76.976, `variant` 176.996 (tidak ada type lain).
Kalau iSeller memakai type baru: cek apakah itu jenis paket, lalu tambahkan ke aturan combo.

### Step 5: pipeline DWH FAILED
Task Airflow gagal setelah email/Teams terkirim. Kalau DL sudah dibersihkan di run yang sama, error dilempar sebagai
`AirflowFailException` (tanpa retry), karena retry akan melihat "tidak ada duplikat" dan tidak me-re-dump DWH.
Re-dump DWH ulang dengan Trigger DAG `run_mode = dwh_only` (atau manual `run_all(start, end, CONFIG_DW, skip_dl_check=True)`).

## 5. File

| File | Status | Isi |
|---|---|---|
| `pipelines/master_bundlings.py` | baru | `get_access_token_mitra()`, `load_bundling_slot_count()` → `(Series bundling_id→slot, status dict dengan ok/reason/message)`, judul & tindak lanjut notifikasi per `reason` |
| `pipelines/dedup_details.py` | baru | `dedup_details_v2()`, `compute_dup_factor()`, `build_dedup_reports()` |
| `pipelines/pipeline_clean_duplicates.py` | diubah | `run_clean_duplicates(start, end, config, clean_headers, clean_details, bundling_slot_count, master_status, dry_run)`, DELETE + re-dump atomik |
| `pipelines/check_duplicates.py` | diubah | log current rows, split non-combo/combo, pemicu combo-only (`combo_only`), cek type tidak dikenal, param `send_email` |
| `pipelines/redump_etl_functions.py` | diubah | urutan step 1–5, `dry_run`, STOP kalau master gagal, ringkasan dedup di email, task gagal kalau pipeline DWH FAILED |
| `teams_report_iseller_mitra.py` | diubah | card error master/token (`send_mitra_dwh_teams_report_master_error`), teks key non-combo |
| `manual_run_iseller_mitra_cleaning.ipynb` | baru | Notebook manual, 1 sel per step (cek DL → master → clean DL → re-dump DWH → email → review). `DRY_RUN = True` bawaan; `DWH_ONLY` setara run mode `dwh_only`; berhenti setelah STEP 3 setara `datalake_only`. Pola sama dengan `manual_run_iseller_pusat_dwh.ipynb` |
| `dag_etl_iseller_mitra_dwh.py` | diubah (kecil) | Airflow `Param` `run_mode` (`full` / `datalake_only` / `dwh_only`) dipetakan ke `skip_dl_check` / `skip_dwh`. Selebihnya tetap: `run_all(target_date, target_date, CONFIG_DW, ...)` |

Modul baru di-import sebagai sibling di folder `pipelines/` (pola import sama seperti file lain di folder itu).
Password SMTP tetap dari environment variable `GMAIL_SMTP_PASSWORD`, tidak boleh di-hardcode.

## 6. Konfigurasi deploy Airflow

1. **Token API**: set `access_token_mitra`. Urutan baca:
   1. Airflow Variable `access_token_mitra` (disarankan, mudah di-rotate dari UI)
   2. environment variable `access_token_mitra`
   3. file `.env` di `<project>/.env`, `<parent>/.env`, atau `/opt/airflow/.env`

   Token tidak ada / expired → pipeline berhenti + email & Teams (bagian 4 step 2). Perbaiki token, lalu clear task.
2. **Folder report**: `<airflow base_log_folder>/iseller_mitra_dedup_reports/<start>_<end>_<YYYYMMDDTHHmmss>/`
   (dibaca dari `airflow.configuration.conf`). Ikut terhapus saat maintenance log. Run lokal → `<project>/reports/`.
3. **Dependency**: tidak ada yang baru (`pandas`, `numpy`, `requests`, `pendulum` sudah dipakai).
4. **Engine tabel DL**: rollback atomik (bagian 4 step 3) hanya berlaku kalau tabel DL memakai InnoDB.
   Cek: `SHOW TABLE STATUS WHERE Name IN ('transactions_iseller_mitra','transactions_items_iseller_mitra');`

## 7. Output

**Log** (urutan): STEP 1 current rows + hasil check → STEP 2 status master → STEP 3:
- [3.1] RAW → CLEAN (headers, details, non-combo, combo), jumlah `needs_manual_check`
- [3.2] sumber `dup_factor` (`bundling` / `not_in_master` / `slot_mismatch`)
- [3.3] % combo sesuai slot, bundling tidak ada di master, slot mismatch
- [3.4] akurasi `tgl, headers_raw, details_raw, headers_clean, details_clean, acc_details_%`
- [3.5] list `needs_manual_check`
- rencana tabel yang ditulis ulang

Lalu STEP 4 redump DWH, STEP 5 report.

**Report CSV** (hanya dibuat kalau tidak kosong, path dicetak di log): `needs_manual_check.csv`,
`slot_mismatch.csv`, `bundling_not_in_master.csv`.

**Cara membaca:** amount combo di mitra dihitung ETL dari harga master, jadi akurasi amount bukan patokan
pasti untuk combo. Patokan utamanya **% combo sesuai slot** di [3.3].

## 8. Verifikasi setelah deploy

1. Jalankan `run_all(start, end, config_dw, dry_run=True)` (misal dari `manual_run_iseller_mitra_cleaning.ipynb`) untuk periode yang dobel.
   Cek: STEP 2 `Status : OK`, [3.3] sesuai slot ~100%, [3.5] kosong / wajar.
2. Run asli untuk 1 tanggal. Cek: tabel yang tidak dobel tertulis "TIDAK disentuh", email berisi tabel
   "Dedup Details (combo-aware)", Teams terkirim, tabel DWH terisi.
3. Tes token salah (Variable kosong / salah) dengan `dry_run=True` → log `Status : GAGAL ...` + `Reason`, lalu pipeline
   **berhenti dengan error** (tanpa email/Teams karena dry run). Tanpa `dry_run`: email `[ALERT] ... STOPPED` + card Teams
   terkirim, Data Lake tidak berubah.

## 9. Hasil uji & batasan

**Data 2–4 Okt 2026, dry run ke DL (master OK):**

| | Logic lama | Logic baru |
|---|---|---|
| Headers | 22.840 → 13.631 | 22.840 → 13.631 |
| Details | 52.660 → 25.970 | 52.660 → **26.325** (+355 row combo sah) |
| Combo sesuai slot | - | **100%** (1.140 order_detail) |
| Akurasi amount | 98,72% | **99,44%** |
| Order memburuk vs header | 108 | **0** |

**Data 1–7 Okt 2026 (H-1), dedup in-memory read-only dari DL:** details 48.515 → 48.193.
2.271 order_detail combo; 2.209 dipotong/dicek lewat master (`bundling`), 12 `not_in_master`, 50 `slot_mismatch`.
Semua 62 kasus itu **tanggal 1 Okt saja** dan tidak ada yang duplikat (tiap key 1 row); 2–7 Okt bersih.
- `not_in_master` (12): "Jiwa Toast Combo" (11) dan "Combo Jiwa Toast NC" (1), kemungkinan paket dinonaktifkan setelah 1 Okt.
- `slot_mismatch` (50): jumlah row di DL lebih sedikit dari jumlah slot master (mis. Hangout Drink Combo 5 slot, 1–4 row).
  Kemungkinan paket diubah setelah transaksi (dugaan dari pola data, belum dikonfirmasi).
- Keduanya tidak dipotong dan muncul di `needs_manual_check` (60 order). Itu perilaku yang diharapkan.

**Data 6 Okt 2026 (kasus combo-only), dry run `run_all` ke DL, master OK:** headers dobel 0, non-combo dobel 0, key combo
dobel 354. Details 7.636 → 7.314 (dibuang 322 row, semuanya combo, 184 order_detail dengan `dup_factor = 2`); non-combo
dibuang 0; `needs_manual_check` 0; combo sesuai slot 100% dari 385.
Cek per row: 322/322 row yang dibuang punya kembaran identik (semua kolom kecuali `load_data_at`) yang tetap disimpan;
tidak ada key non-dobel yang tersentuh; hasil tiap combo yang dipotong = jumlah slot master (184/184).
Amount vs header untuk 174 order terdampak: 184,3% → 92,2% (168 dari 174 lebih dekat ke header). 6 order tidak
membaik, semuanya bundling "Triple Drinks Offer Delivery": amount tersimpan tepat separuh amount mentah (mis. 40.000 → 20.000),
jadi itu pembuangan salinan, bukan data lain. Selisih ke header sudah ada sebelum cleaning: di 2–5 Okt (tanpa dobel) 184 dari 210
order bundling itu details-nya di bawah header (77,2% total), jadi ini ketidakpresisian amount ETL, bukan akibat cleaning.
Asal kasusnya: run lama (sebelum perubahan ini) tanpa token membersihkan non-combo tapi menahan 184 combo karena
cadangan tidak setuju (`batch` ×2, `headers` ×1); setelah itu STEP 1 melihat "aman" dan combo tersisa tidak pernah dibersihkan.
Pemicu combo mengatasi ini. Hari dengan key combo sah (mis. 5 Okt: 68 key, 0 dibuang) berakhir sebagai "aman".

**Catatan keputusan (2026-10-07): kenapa cadangan dihapus.**
- Untuk H-1, hampir semua combo ada di master (1–7 Okt: 12 dari 2.271 tidak ada, semuanya 1 Okt), jadi cadangan nyaris tidak pernah terpakai.
- Sumber `items` dan `batch` tidak independen (nilai `batch` dihitung dari `items`), jadi kesepakatan "2 sumber" lebih lemah dari kelihatannya.
- Keputusan: pipeline hanya bergantung pada master dari API. Master tidak bisa di-load → berhenti, bukan menebak.

**Batasan:**
- `GetProducts` hanya berisi bundling **aktif**. Untuk data lama, 20–50% order_detail combo per bulan
  (Jan–Sep 2026) tidak ada di master. Tanpa cadangan, combo itu **tidak dipotong** dan masuk `needs_manual_check`,
  jadi pipeline ini **tidak cocok untuk cleaning ulang periode lama** (data tetap kelebihan sampai dicek manual).
  Kalau tetap perlu, jalankan **per bulan** (tabel DL tidak punya index) dan siapkan penanganan manual untuk
  combo non-master.
- Kalau slot sebuah bundling pernah diubah setelah order terjadi → muncul di `slot_mismatch` / `needs_manual_check`, tidak dihapus diam-diam.
- Order `needs_manual_check` (combo `slot_mismatch` / `not_in_master`) tidak dipotong dan tidak ada langkah otomatis
  lanjutan; report CSV ikut terhapus saat maintenance log. Untuk H-1 normal seharusnya kosong.
- Di hari ketika hanya headers yang dobel dan tidak ada key combo dobel, details tidak disentuh (aturan 5).

**Risiko sisa yang diterima (2026-10-07):**
- **Slot master berubah setelah transaksi.** `dup_factor` dihitung dari `jumlah row / slot master hari ini`. Contoh nyata
  6 Okt `#307-12809` (Triple Drinks Offer Delivery, master 3 slot): 6 row identik di DL, 6 / 3 = 2, disimpan 3 (benar).
  Kalau master kemudian bilang 2 slot, 6 / 2 = 3 dan disimpan 2: **1 row sah terhapus tanpa peringatan**. Kalau master bilang 6
  slot, 6 / 6 = 1 dan duplikat tidak terhapus (aman tapi kotor). Kalau 4 slot, 6 / 4 = 1,5 → `slot_mismatch`, tidak dipotong.
  Hanya kasus "slot turun ke pembagi jumlah row" yang diam-diam menghapus data sah. Kasus slot berubah memang ada (1 Okt: 50
  combo `slot_mismatch`, row lebih sedikit dari slot master, arahnya naik sehingga tidak dipotong), tapi untuk H-1 master ditarik
  sehari setelah transaksi sehingga peluangnya kecil.
- **Tidak ada pengaman independen dari data.** `load_data_at` tidak bisa dipakai: semua row dobel 6 Okt punya `load_data_at`
  yang sama (1 batch, `2026-10-07 06:07:49`), jadi syarat "dobel harus terbelah ≥ 2 batch" akan menolak membersihkan duplikat asli.
  Bukti lain (non-combo ikut dobel, header dobel) adalah logic `items` / `headers` yang sengaja sudah dihapus. Diputuskan diterima.
- Jejak audit: ringkasan dedup di email menunjukkan jumlah combo dipotong dan sumbernya, dan log STEP 3 mencatat tiap
  perubahan, supaya slot yang janggal terlihat kalau suatu hari terjadi.

## 10. Di luar scope (belum dikerjakan)
- **Fan-out promo/diskon di notebook ETL DL mitra** (merge menggandakan row order_detail yang punya > 1
  promo/diskon). Dedup hanya membersihkan akibatnya.
- **Tabel snapshot master bundling di DL (opsi B)**, supaya bundling nonaktif tetap tercatat. Butuh approval manager.
- **Dump DL idempotent di ETL** (hapus dulu sebelum append), supaya duplikat tidak terjadi lagi.
- **Penanganan combo `slot_mismatch` / `not_in_master`**: saat ini hanya dilaporkan.
- **Pengaman slot master berubah** (lihat risiko sisa di bagian 9): butuh sumber bukti dobel yang independen dari master.
