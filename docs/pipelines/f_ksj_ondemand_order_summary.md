# `f_ksj_ondemand_order_summary` — KSJ On-Demand Order Distance & Timing Summary

Membangun tabel `ksj_on_demand_order_summary` di Data Warehouse dari order on-demand
KSJ Link: jarak straight-line vs jarak rute jalan sesungguhnya (OSRM) per order, metrik
blast (berapa rider di-ping, berapa ronde), dan breakdown waktu paid → accepted → OTW →
delivered. Dipakai untuk analisis performa dispatch on-demand (seberapa jauh rider
harus menjangkau, seberapa lama tiap tahap memakan waktu).

## Metadata DAG

File: `dag_etl_f_ksj_ondemand_order_summary.py`

| Field | Nilai |
|---|---|
| `dag_id` | `f_ksj_ondemand_order_summary` |
| `schedule_interval` | `"0 9,12,15,17 * * *"` — 4x/hari, 09:00 / 12:00 / 15:00 / 17:00 Asia/Jakarta |
| `start_date` | `datetime(2026, 1, 1, tzinfo=local_tz)` |
| `catchup` | `False` |
| `max_active_runs` | `1` |
| `execution_timeout` | `2 jam` (task, bukan DAG) — jaga-jaga kalau panggilan OSRM lambat/macet supaya tidak nge-block batch berikutnya |
| `tags` | `["etl", "ksj-link", "on-demand", "order-distance", "dwh", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` |
| Task | 1x `PythonOperator("run_pipeline")` |

**Date window** — beda pola dari DAG harian lain (lihat docstring `etl_orchestrator.py`):
bukan H-1, tapi **hari ini + `LOOKBACK_DAYS=1` hari ke belakang** (jadi: hari ini &
kemarin), dihitung ulang dari wall-clock setiap kali run (bukan dari
`data_interval`/`logical_date` Airflow) — karena DAG ini jalan 4x/hari dan tiap run
me-refresh window yang sama seiring order hari itu selesai satu-satu. Overwrite-per-date
di step LOAD membuat ini aman dijalankan berulang (idempotent).

## Alur Pipeline (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py) — 3 database TERPISAH, tidak bisa di-JOIN di SQL:
  jiwa-order-dispatcher (PostgreSQL, config_ksj.d_jiwa_ondemand)
    get_pending_orders            — 1 baris per order on-demand (grain tabel akhir)
    get_blast_rider_metrics       — agregat respons rider per order (ACCEPTED/REJECTED/TIMEOUT/...)
    get_blast_round_metrics       — jumlah ronde blast & radius per order
    get_accept_context            — di ronde ke berapa order akhirnya di-accept
    get_nearest_blasted_rider     — posisi rider TERDEKAT yang di-blast (fallback origin utk order gagal)
    get_blast_rider_positions     — posisi SEMUA rider yang di-blast (utk OSRM per rider)
    get_blasted_rider_codes       — daftar rider_code yang di-blast, urut dari terdekat
    get_first_dispatch_reason     — alasan dispatch paling awal (order_status_detail)
  d_transaction (PostgreSQL, config_ksj.d_transaction)
    get_delivery_detail           — transactions x on_demand_order_deliveries (transaction_type='ON_DEMAND')
  jiwaplus (PostgreSQL, config_jiwaplus)
    get_jiwaplus_transaction_detail — nama/telepon customer, status transaksi sisi JiwaPlus
    get_markassajiwa_user_ids       — user_id pemegang voucher membership aktif HARI INI

TRANSFORM (transform.py)
  build_blast_route_agg()  — jarak OSRM tiap rider yang di-blast -> agregat
                              (nearest/avg/median/max) per order
  build_order_summary()    — left-merge semua source, tentukan origin_type
                              (ACCEPTED_RIDER / NEAREST_BLASTED_RIDER / NO_RIDER_BLASTED /
                              NO_RIDER_COORDINATE), hitung straight_line_distance_m
                              (haversine, tetap jadi baseline) + actual_route_distance_m
                              (OSRM, live call ke router.project-osrm.org — di-cache per
                              koordinat, gagal -> NULL + route_source='FAILED', TIDAK
                              pernah menggagalkan pipeline), turunkan 4 kolom durasi
                              (minutes_paid_to_completed, accept_to_otw, otw_to_delivered,
                              accept_to_delivered)

LOAD (etl_orchestrator._overwrite_for_dates)
  Atomic DELETE + INSERT per order_date, SATU transaksi (engine.begin()):
  DELETE FROM ksj_on_demand_order_summary WHERE order_date IN (...)
  → to_sql(if_exists='append', method='multi', chunksize=500, dtype=DTYPE_MAP)
  Kalau df_summary kosong: DELETE **DISKIP** (bukan cuma insert yang diskip) — supaya
  hasil kosong di 1 batch tidak menghapus baris baik yang sudah ke-load batch
  sebelumnya di hari yang sama.

NOTIFIKASI (_send_report_email, inline di etl_orchestrator.py)
  Email HTML via Gmail SMTP, termasuk breakdown order_status & route_source.
  Kegagalan kirim email hanya di-logging.warning — tidak menggagalkan task.
```

## Sistem Eksternal

- **PostgreSQL KSJ Link** — 2 database dipakai: `d_jiwa_ondemand` (dispatcher) dan
  `d_transaction` (transaksi/delivery).
- **PostgreSQL JiwaPlus** — detail customer + voucher membership.
- **MySQL Data Warehouse** — target `ksj_on_demand_order_summary`.
- **OSRM (router.project-osrm.org)** — routing API publik, tanpa API key, dipanggil
  langsung dari `transform.py` (`ROUTE_PROVIDER='OSRM'`). Ada jalur `GOOGLE` (butuh
  `GOOGLE_MAPS_API_KEY`, saat ini `None` — tidak aktif) dan `NONE` (matikan routing) yang
  tinggal ganti konstanta kalau perlu pindah provider. Gagal panggil = `route_source`
  jadi `'FAILED'`/`'NO_ROUTE'`, kolom jarak NULL, pipeline tetap lanjut.
- **Email (Gmail SMTP)** — password dari `GMAIL_SMTP_PASSWORD`, sama pola DAG lain.
- Tidak memakai Teams/Power Automate.

## Connections / Variables

- Tidak ada Airflow Connection/Variable native. 3 config di-load dinamis via
  `importlib.util`: `config_db_datawarehouse.py`, `config_db_ksj_link.py`,
  `config_db_jiwaplus.py`.
- Koneksi via `plugins/common/db_helpers.py`: `ksj_link_db_conn(config_ksj, db_name)`
  (raw psycopg2, per-database — dipanggil dengan `config_ksj.d_jiwa_ondemand` atau
  `config_ksj.d_transaction`), `jiwaplus_db_conn(config_jiwaplus)` (raw psycopg2), dan
  `get_engine(config_dw)` (SQLAlchemy, untuk step LOAD ke Data Warehouse).
- `GMAIL_SMTP_PASSWORD` dari environment (lihat `.env.example`).

## Pola Kode Khas

- **Tidak ada DDL di repo untuk versi lama** — tabel dulunya dibuat otomatis oleh
  `to_sql()` dari notebook. `sql/create_table.sql` (`CREATE TABLE IF NOT EXISTS`) adalah
  penambahan baru supaya schema-nya eksplisit & terdokumentasi; **tidak dijalankan
  otomatis oleh Airflow** — lihat [`docs/DEPLOYMENT.md`](../DEPLOYMENT.md) bagian 3a.
  `etl_orchestrator.py` juga tetap membawa `DTYPE_MAP` sebagai fallback kalau suatu saat
  tabelnya perlu dibuat ulang dari nol lewat `to_sql`.
- **3 database terpisah, merge di pandas** — bukan satu keputusan gaya, tapi keharusan:
  `d_jiwa_ondemand`, `d_transaction`, dan `jiwaplus` tidak bisa di-JOIN lewat SQL, jadi
  `build_order_summary()` melakukan serangkaian left-merge di pandas
  (`_left_merge` helper menangani kasus source frame kosong yang bikin merge gagal
  karena dtype `object` vs `int64`).
- **OSRM call di-cache per pasangan koordinat** (`_route_cache`, module-level dict) —
  order yang berbagi titik yang sama (mis. kantor pusat, alamat langganan) tidak
  memanggil API dua kali. Cache ini juga dipakai bersama antara
  `build_blast_route_agg()` dan `build_order_summary()`.
- **Banyak catatan verifikasi data di docstring** (`extract.py`, `transform.py`) — mis.
  `transaction_datetime_gmt` di JiwaPlus sebenarnya WIB meski namanya GMT,
  `blast_rider_lat/long` baru mulai terisi sejak 11 Agustus 2026. Baca docstring
  fungsi terkait sebelum mengubah query — ini hasil verifikasi manual terhadap data
  live, bukan asumsi.
- Print-based progress logging dengan banner `"[N/3] STEP_NAME"` dan timer elapsed via
  `time.time()`, sama seperti DAG lain di repo ini.
- Try/except membungkus seluruh `run_all`; kalau exception, print + kirim email FAILED
  (merah), lalu di-raise ulang supaya Airflow tetap menandai gagal dan retry sesuai
  `default_args`.

## Manual Run

Ada 2 notebook dev/precursor di folder DAG ini:
`etl_ksj_on_demand_order_distance_NEW.ipynb` (versi terbaru — jadi acuan port ke
`extract.py`/`transform.py`, lihat docstring `extract.py`) dan
`etl_ksj_on_demand_order_distance_old.ipynb` (versi sebelumnya, ditinggal sebagai
referensi histori). Kode produksi ada di `pipelines/`, bukan di notebook — notebook
dipakai untuk eksplorasi/debug manual, bukan dijalankan terjadwal.
