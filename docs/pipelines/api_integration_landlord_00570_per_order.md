# `api_integration_landlord_00570_per_order` — Kirim Revenue Sharing PER ORDER ke API Landlord (00570)

Mengirim uang masuk (estimated income) **per order** outlet `00570` (DISTRICT 2 MEIKARTA) ke Lippo Revenue API,
mulai sales tanggal **2026-11-23** (cutover; sampai 2026-11-22 dikirim per payment type oleh
`api_landlord_revenue_sharing_00570`). Sumber: `order_est_income_for_landlord` (DAG `f_order_estimated_income_per_store`).
Dokumen bisnis: DRD 05.

## Metadata DAG

File: `dag_api_landlord_revenue_sharing_00570_per_order.py`

| Field | Nilai |
|---|---|
| `dag_id` | `api_landlord_revenue_sharing_00570_per_order` |
| `schedule` | `[Asset("f_order_estimated_income_per_store://order_est_income_for_landlord")]` — tanpa jam tetap |
| `max_active_runs` | `1` |
| `default_args` | `retries=2` hanya untuk error sebelum POST; setelah POST → `AirflowFailException` (tanpa retry) |
| Params (manual) | `start_date`, `end_date` (maks 31 hari, ≥ 2026-11-23), `dry_run` (default **True**), `resend_uncertain` (default False) |

**Date window**: run otomatis = rentang di `extra` asset event (`start_date` / `end_date` yang baru di-load DAG ETL order;
event menumpuk → gabungan). Tanggal < `MIN_SEND_DATE` (2026-11-23) dibuang. Run otomatis selalu live (bukan dry run).
`load_data_by`: otomatis `NEXUS_AIRFLOW`, manual = email user yang trigger.

## Payload

| Field | Isi |
|---|---|
| `TransactionNumber` | `<jilid>_<order_reference>_<PaymentType>_<HHMMSS>` (HHMMSS = jam run WIB; maks 50 char, mall memotong yang lebih panjang) |
| `TransactionDate` | tanggal transaksi |
| `Amount` | `ROUND(subtotal - sales_type_commission, 4)` — uang masuk penuh, 10% dihitung landlord |
| `Remarks` | `<jilid>_<order_reference>_<PaymentType>_<YYYY-MM-DD>_<HHMMSS>` |

Baris `total_sales = 0` tidak dikirim. Nomor dobel (split payment dengan payment type sama di 1 order) atau > 50 char → raise sebelum POST.

## Guard anti-dobel (sebelum POST pertama)

Membaca `api_landlord_log_detail` jilid 00570. Kunci = `TransactionNumber` tanpa `_HHMMSS`.

| Status di log untuk kunci | Aksi |
|---|---|
| `success`, atau `error` + `TRIM(error_message)='duplicate'` | skip (sudah ada di mall) |
| `timeout_error` / `pending` (tanpa success/duplicate) | skip + peringatan; kirim hanya dengan `resend_uncertain=True` setelah cek manual |
| `http_error` / `error` lain | kirim ulang (mall menolak, belum masuk) |
| tidak ada di log | kirim |

Tanggal yang sudah diterima mall dalam format lama (per payment type, `Janji_Jiwa_...`) → raise (dry run: peringatan saja).

## Alur per tanggal (`etl_orchestrator.run_all`)

```
insert_pending_logs : baris detail status 'pending' (commit sendiri)      ← jejak SEBELUM POST
post_one_date       : POST 1 batch (retry 2x, timeout 90 s, 401 -> refresh token sekali)
finalize_logs       : insert api_landlord_logs + UPDATE 'pending' -> status akhir (1 transaksi)
cek TotalSuccess    : TotalSuccess mall == jumlah success yang cocok di log
```

- Simpan log gagal / proses mati di tengah POST → **STOP**, tanggal berikutnya tidak dikirim, baris tetap `pending`.
- `TotalSuccess` beda → log sudah tersimpan, lalu **STOP**.
- `http_error` / `timeout_error` di satu tanggal → log tersimpan, lanjut tanggal berikutnya, akhir run `AirflowFailException`.
- Email HTML (Gmail SMTP) tiap run; dry run diberi tanda `[DRY RUN]`.

## Runbook: baris masih `pending` setelah run selesai

`pending` = POST **mungkin** sudah sampai ke mall, tapi hasilnya tidak tercatat (DB putus saat simpan log, atau proses
mati di tengah POST). Guard tidak akan mengirim ulang baris ini otomatis.

1. Cari barisnya:
   ```sql
   SELECT id, transaction_number, transaction_date, amount, created_at
   FROM api_landlord_log_detail WHERE jilid = '00570' AND status = 'pending' ORDER BY id;
   ```
2. Buka log task Airflow run tersebut (jam = `created_at`). Kalau POST sempat balik, tabel hasil
   (`transaction_number | amount | status | mall_transaction_id | error_message`) tercetak sebelum pesan STOP.
3. Betulkan status sesuai bukti:
   - Log task menunjukkan `success` + `mall_transaction_id` → `UPDATE ... SET status='success', mall_transaction_id=<id>, revenue_batch_id=<batch> WHERE id IN (...)`.
   - Log task menunjukkan `http_error`, atau landlord konfirmasi belum masuk → `UPDATE ... SET status='error', error_message='manual: belum masuk mall' WHERE id IN (...)` → run berikutnya kirim ulang otomatis.
   - Tidak ada bukti (proses mati di tengah POST) → tanya landlord / cek rekap Lippo dulu, lalu pilih salah satu di atas.
     Alternatif tanpa UPDATE: trigger manual dengan `resend_uncertain=True` **hanya** kalau sudah pasti belum masuk.
4. Trigger manual tanggal yang tersisa (`dry_run=False`); guard hanya mengirim yang belum ada di mall.

## Revisi

Selisih (kurang/lebih bayar) tidak lewat DAG ini (guard akan skip): kirim lewat flow revisi dengan nomor dasar sama +
HHMMSS baru + `Remarks` diawali `Revise-`.
