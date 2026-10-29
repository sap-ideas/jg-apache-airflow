-- ============================================================
-- Table: daily_est_income_per_store_for_landlord
-- Database: MySQL Data Warehouse (config_dw.db_target)  -- MySQL 5.7
-- Written by: dags/f_daily_estimated_income_per_store/pipelines/load.py
--   DELETE FROM daily_est_income_per_store_for_landlord WHERE date = :d
--   then to_sql(if_exists='append') — overwrite-per-date, safe to retry.
-- Read by: dags/api_integration_landlord_00570 (extract.py)
--
-- Format long: satu baris per date + outlet + payment_type.
-- Payment type baru TIDAK perlu kolom baru (cukup entry di transform.py).
-- Nominal yang dibagi ke landlord = subtotal - sales_type_commission
-- (dihitung saat dipakai, tidak disimpan).
--
-- Referensi schema: dijalankan manual di DBeaver (lihat migrate_wide_to_long.sql
-- untuk urutan cutover dari format wide lama).
-- ============================================================

CREATE TABLE daily_est_income_per_store_for_landlord (
    `date` DATE NOT NULL,
    outlet_code VARCHAR(10) NOT NULL,
    outlet_name VARCHAR(150) NULL,
    payment_type VARCHAR(100) NOT NULL COMMENT 'Payment type resmi (mis. Go Food, iSeller QRIS); yang belum dikenal = nama asli dari iSeller',

    subtotal DECIMAL(20,4) NOT NULL DEFAULT 0 COMMENT 'Net sales (SUM subtotal)',
    tax_amount DECIMAL(20,4) NOT NULL DEFAULT 0 COMMENT 'Pajak (SUM total_tax_amount, apa adanya dari iSeller)',
    total_sales DECIMAL(20,4) NOT NULL DEFAULT 0 COMMENT 'Gross sales (SUM total_amount)',
    sales_type_commission DECIMAL(20,4) NOT NULL DEFAULT 0 COMMENT 'Komisi payment type dalam rupiah = subtotal x pct (tidak dibulatkan di kode)',

    load_data_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    load_data_by VARCHAR(250) NULL,

    UNIQUE KEY uq_date_outlet_payment (`date`, outlet_code, payment_type),
    KEY idx_outlet_date (outlet_code, `date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  COMMENT='Daily estimated income per store per payment type for landlord (est. income = subtotal - sales_type_commission)';
