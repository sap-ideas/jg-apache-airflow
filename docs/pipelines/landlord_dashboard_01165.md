# `landlord_dashboard_01165` — Dashboard HTML Landlord 01165 → `custom_menus`

Generate ulang dashboard penjualan outlet 01165 (SUMMARECON EMERALD KARAWANG) untuk landlord, lalu
**UPDATE kolom `html_content` + `updated_at`** di tabel `custom_menus` (database `data_report`, PostgreSQL JiwaPlus),
row `id = 80` (`OPR - Summarecon Emerald Karawang Sales`). File HTML tidak disimpan di disk. Menu ini yang
ditampilkan di ai.jiwa.app.

## Metadata DAG

File: `dag_landlord_dashboard_01165.py`

| Field | Nilai |
|---|---|
| `dag_id` | `landlord_dashboard_01165` |
| `schedule` | Asset `f_daily_estimated_income_per_store://daily_est_income_per_store_for_landlord` — jalan setelah `f_daily_estimated_income_per_store` sukses (paralel dengan `api_landlord_revenue_sharing_00570`), tanpa jam tetap |
| `catchup` / `max_active_runs` | `False` / `1` |
| `tags` | `["landlord", "dashboard", "html", "custom-menus", "production"]` |
| `default_args` | `owner='data-team'`, `retries=2`, `retry_delay=5 menit` (aman: UPDATE idempotent) |
| Task | 1x `PythonOperator("run_pipeline")`, tanpa params |

**Data scope**: 60 hari terakhir dari tanggal run, paling awal `2026-11-01` (outlet buka 1 Nov 2026; transaksi 29–31 Okt = data testing).

## Alur Pipeline (`etl_orchestrator.run_all`)

```
EXTRACT (extract.py :: extract_income)
  MySQL DWH — daily_est_income_per_store_for_landlord, outlet_code='01165', date >= start.
  Grain date x outlet_name x payment bucket (payment type tak dikenal / NULL -> 'other');
  income = SUM(subtotal - COALESCE(sales_type_commission, 0)), total_order = SUM(total_order)

TRANSFORM (transform.py :: pivot_detail, build_html)
  Pivot 1 baris per (date, outlet_name), kolom per payment bucket, daily_income = jumlah bucket.
  Data di-embed sebagai JSON ke templates/dashboard_template_jiwa_light.html (tema Jiwa Light) -> string HTML.
  Data kosong -> raise (dashboard tidak ditimpa HTML kosong).

LOAD (load.py :: update_custom_menu)
  PostgreSQL data_report, satu transaksi:
  UPDATE custom_menus SET html_content = %s, updated_at = (NOW() AT TIME ZONE 'UTC')
   WHERE id = 80 AND name = 'OPR - Summarecon Emerald Karawang Sales'
  rowcount != 1 -> rollback + raise. Kolom/row lain tidak disentuh.

NOTIFIKASI (_send_report_email, inline di etl_orchestrator.py)
  Email HTML via Gmail SMTP (sukses/gagal). Kegagalan kirim email hanya print WARNING.
```

## Catatan

- `updated_at` bertipe `timestamp without time zone` dan berisi **UTC** (nilai existing 10:53 WIB tersimpan `03:53`), jadi di-set UTC.
- Tabel tidak punya kolom `updated_by`, jadi tag audit `NEXUS_AIRFLOW` tidak berlaku.
- Tabel `custom_menus` dibuat/dikelola di luar repo ini; pipeline hanya UPDATE row yang sudah ada.
- Template ada di `templates/` (salinan dari folder pengembangan dashboard). Ubah desain di template, bukan di `transform.py`.
- Hasil HTML memakai line ending LF (file lama dari notebook Windows memakai CRLF); tidak berpengaruh di browser.

## Koneksi / Config

- `config_db_datawarehouse.py` (MySQL DWH, `get_connection` dari `common.db_helpers`) dan `config_db_jiwaplus.py`
  (`database_data_report`, `psycopg2` langsung karena `jiwaplus_db_conn` hardcode DB `jiwaplus`).
- SMTP password dari `GMAIL_SMTP_PASSWORD`.

## Manual Run

`run_all(config_dw, config_jiwaplus, dry_run_path="/path/out.html")` menulis HTML ke file dan **tidak** menyentuh DB
(untuk cek tampilan). Tanpa `dry_run_path` = UPDATE sungguhan. Tidak ada form trigger; trigger manual dari UI Airflow
langsung meng-update row 80.

Notebook referensi: `landlord_dashboard_01165.ipynb` (di folder DAG; cara lama, generate file HTML manual).
