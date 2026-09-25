# `api_integration_landlord_01065` — Revenue Sharing ke Landlord (jilid 01065)

Kirim laporan revenue sharing harian ke API pihak ketiga (portal tenant Lippo Mall),
dan simpan log request/response secara idempoten untuk audit.

## Metadata DAG

File: `dag_api_landlord_revenue_sharing_01065.py`

| Field | Nilai |
|---|---|
| `dag_id` | `api_landlord_revenue_sharing_01065` |
| `schedule_interval` | `"0 8 * * *"` — harian 08:00 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `tags` | `["api", "landlord", "revenue-sharing", "lippomall", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |

**Date window**: `target_date` = "kemarin" di Asia/Jakarta (helper duplikat di dalam DAG file ini, sama logic dengan DAG lain).

DAG ini spesifik untuk outlet **jilid `01065`** — nama DAG mengandung kode jilid ini.
⚠️ Nambah outlet lain (mis. `00570`) belum berarti otomatis jalan — lihat
[`docs/DEPLOYMENT.md`](../DEPLOYMENT.md) bagian 3 untuk status & catatan-nya.

## Alur Pipeline (`etl_orchestrator.run_all`)

```
EXTRACT
  ├─ get_access_token(jilid) — baca kredensial dari environment variable
  │    (LANDLORD_TOKEN_API_URL + LANDLORD_OUTLET_<jilid>_EMAIL/_PASSWORD, lihat .env.example),
  │    POST grant_type=password ke LANDLORD_TOKEN_API_URL
  │    → Bearer access_token. Raise RuntimeError kalau env var belum di-set / token
  │    kosong / response bukan 200.
  └─ extract_sales() — MySQL Data Lake: transactions_iseller_pusat, outlet_code=01065,
       filter fulfillment_status='fulfilled', status='completed', payment_status='paid',
       transactions_status='success'; normalisasi payment_type
       ("iSeller Pay QRIS"/"iSellerpay (Sejutajiwariders)" → 'iSeller QRIS');
       aggregate total_sales/total_tax/net_sales per date/jilid/payment_type

TRANSFORM (transform.py :: transform_sales)
  Group by (jilid, transaction_date, payment_type) → sum TotalSales/TotalTax/Amount;
  bangun TransactionNumber & Remarks (prefix "Janji_Jiwa_" + jilid + payment_type +
  tanggal compact + HHMMSS); round Amount 4 desimal.
  Kalau input kosong → return DataFrame kosong bertipe (short-circuit di orchestrator).

LOAD — dua langkah:
  ├─ post_to_api() (load.py) — POST JSON {"revenueDatas": [...]} Bearer-auth ke
  │    REVENUE_API_URL (.../revenue/api/revenue/v1/save), timeout 60s.
  │    Bangun 2 DataFrame log:
  │      df_log_summary — batch stats (batch_id, total_sent/success/error, HTTP status,
  │                        request/response JSON, timestamp)
  │      df_log_detail  — per-transaksi, merge dengan transactionReturnDatas API,
  │                        derive status (success/error/http_error) + error_message
  │    Handle response non-200 dan body non-JSON dengan graceful.
  └─ dump_logs_idempotent() (load.py) — via common.db_helpers.get_engine_datalake:
       untuk masing2 dari 2 tabel log (api_landlord_logs, api_landlord_log_detail):
       DELETE WHERE jilid=:jilid AND transaction_date=:td → to_sql(append),
       tiap tabel dalam transaksi engine.begin() sendiri

SHORT-CIRCUIT tanpa data: kalau transform_sales() kosong → skip POST API & log dump,
  kirim email "0 rows", return SUCCESS_NO_DATA

NOTIFIKASI: Email HTML (Gmail SMTP) — 3 varian: sukses, 0 data, gagal — statistik
  rows extracted/transformed, api_success/api_error, delete/insert count per tabel log
```

## Sistem Eksternal

- **API eksternal (Revenue API Lippo Mall)** — endpoint token (`LANDLORD_TOKEN_API_URL`)
  & save revenue (`REVENUE_API_URL`, hardcode di `load.py`:
  `https://revenueapi.lippomalls.com:8443/api/revenue/v1/save`). Kredensial per outlet
  ada di `.env` sebagai `LANDLORD_OUTLET_<jilid>_EMAIL` / `_PASSWORD` (dulunya file CSV
  plaintext `get_token.csv` di folder DAG — sudah dipindah ke env,
  2026-09-22). `.env` sendiri sekarang punya kredensial untuk jilid `01065` dan `00570`;
  ada catatan dokumentasi lama yang menyebut jilid ketiga `00797` juga pernah
  direncanakan — **belum dikonfirmasi masih relevan atau tidak**, cek ke data team
  kalau mau tahu statusnya.
- **MySQL Data Lake** — source `transactions_iseller_pusat`; target log `api_landlord_logs`, `api_landlord_log_detail`.
- **Email (Gmail SMTP)** — kredensial dari `GMAIL_SMTP_PASSWORD`, sama pola & akun dengan DAG lain.
- Tidak memakai Teams/Power Automate.

## Connections / Variables

- Tidak ada Airflow Connection/Variable native untuk DB — hanya `config/config_db_datawarehouse.py` (MySQL Data Lake/Warehouse) yang di-load dinamis.
- **Kredensial API landlord dari environment variable, bukan Airflow Connection/Variable
  maupun file CSV lagi** — `LANDLORD_TOKEN_API_URL` (shared) +
  `LANDLORD_OUTLET_<jilid>_EMAIL`/`_PASSWORD` (per outlet) di `.env`. Ditambahkan lewat
  `python-dotenv` di `extract.py` supaya tetap kebaca kalau dijalankan manual di luar
  Docker.
- Kredensial SMTP Gmail dari `GMAIL_SMTP_PASSWORD`, inline di `etl_orchestrator.py` (akun sama dengan DAG lain).
- URL Revenue API (save) di-hardcode di `load.py`; URL token dari `LANDLORD_TOKEN_API_URL`.

## Pola Kode Khas

- Struktur 4 langkah: `extract.py` → `transform.py` → `load.py` (DAG ini yang paling lengkap punya `load.py` terpisah, membedah POST API dan dump log jadi 2 fungsi bernama) → email inline di `etl_orchestrator.py`.
- Konstanta modul-level untuk endpoint/nama tabel di puncak tiap file pipeline (`JILID`, `REVENUE_API_URL`, `LOG_SUMMARY_TABLE`, `LOG_DETAIL_TABLE`) — nilai `JILID` terduplikasi di 3 file berbeda (`extract.py`, `load.py`, `etl_orchestrator.py`).
- Type hint union Python 3.10+ (`str | None`).
- Ada typo kecil di SQL literal `extract.py` (`THEN 'iSeller QRIS''` — extra single-quote) — tidak mempengaruhi hasil query karena posisinya di akhir string literal, tapi worth diketahui.
- Sama seperti DAG lain: try/except di level `run_all`, kegagalan email hanya `logging.warning`, exception asli tetap di-raise ulang untuk memicu retry Airflow.

## Manual Run

Ada notebook `dags/api_integration_landlord_01065/revenue_sharing.ipynb` sebagai precursor/dev notebook.
