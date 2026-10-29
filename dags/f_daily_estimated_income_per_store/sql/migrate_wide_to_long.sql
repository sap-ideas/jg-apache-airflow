-- ============================================================
-- Cutover: daily_est_income_per_store_for_landlord  wide -> long
-- Jalankan MANUAL di DBeaver (MySQL 5.7), database data warehouse (config_dw.db_target).
-- Pipeline tidak pernah menjalankan DDL (lihat CLAUDE.md 3.2).
--
-- Kenapa tidak di-unpivot dari tabel lama: tabel lama tidak punya pajak per channel, dan
-- payment type yang dulu hilang (Mandiri QRIS, isellerpay_qris) tidak ada di kolom channel.
-- Histori dibangun ulang dari sumber (transactions_iseller_pusat) pakai notebook backfill.
-- ============================================================

-- ------------------------------------------------------------
-- STEP 0 (di Airflow UI): PAUSE dulu kedua DAG, pastikan tidak ada run yang sedang jalan
--   - f_daily_estimated_income_per_store
--   - api_landlord_revenue_sharing_00570
-- ------------------------------------------------------------

-- STEP 1: simpan tabel lama sebagai backup (rename, bukan drop)
RENAME TABLE daily_est_income_per_store_for_landlord
          TO daily_est_income_per_store_for_landlord_old;

-- STEP 2: buat tabel baru (sama dengan create_table.sql)
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

-- ------------------------------------------------------------
-- STEP 3: deploy kode baru (merge / live-mount), lalu BACKFILL dari notebook:
--   dags/f_daily_estimated_income_per_store/daily_est_income_per_store_for_landlord.ipynb
--   start_date = 2026-01-01, end_date = kemarin (00570 dan 00989).
--   Notebook meng-DELETE range itu dulu, jadi aman di-run ulang.
-- ------------------------------------------------------------

-- STEP 4a: rekonsiliasi per outlet-hari: tabel baru vs header tabel lama.
--   Selisih HANYA boleh muncul di hari yang dulu punya payment type unmapped
--   (Mandiri QRIS: 2026-04-06 (00989), 2026-04-18 (00570), 2026-08-14 (00570);
--    isellerpay_qris: 2026-10-17 (00570) kalau saat itu belum ikut terhitung).
SELECT o.`date`, o.outlet_code,
       o.total_sales AS old_total_sales,   n.total_sales AS new_total_sales,
       n.total_sales - o.total_sales       AS diff_total_sales,
       o.net_sales   AS old_net_sales,     n.subtotal    AS new_subtotal,
       n.subtotal - o.net_sales            AS diff_subtotal,
       o.tax_amount  AS old_tax,           n.tax_amount  AS new_tax,
       n.tax_amount - o.tax_amount         AS diff_tax
FROM daily_est_income_per_store_for_landlord_old o
LEFT JOIN (
    SELECT `date`, outlet_code,
           SUM(total_sales) total_sales, SUM(subtotal) subtotal, SUM(tax_amount) tax_amount
    FROM daily_est_income_per_store_for_landlord
    GROUP BY `date`, outlet_code
) n ON n.`date` = o.`date` AND n.outlet_code = o.outlet_code
WHERE n.`date` IS NULL                                   -- hari yang belum ke-backfill
   OR ABS(n.total_sales - o.total_sales) > 0.01
   OR ABS(n.subtotal    - o.net_sales)   > 0.01
   OR ABS(n.tax_amount  - o.tax_amount)  > 0.01
ORDER BY o.`date`, o.outlet_code;

-- STEP 4b: komisi total harus sama dengan total_commission_cost lama (selisih hanya di hari unmapped)
SELECT o.`date`, o.outlet_code, o.total_commission_cost AS old_commission,
       n.commission AS new_commission, n.commission - o.total_commission_cost AS diff
FROM daily_est_income_per_store_for_landlord_old o
JOIN (
    SELECT `date`, outlet_code, SUM(sales_type_commission) commission
    FROM daily_est_income_per_store_for_landlord
    GROUP BY `date`, outlet_code
) n ON n.`date` = o.`date` AND n.outlet_code = o.outlet_code
WHERE ABS(n.commission - o.total_commission_cost) > 0.01
ORDER BY o.`date`, o.outlet_code;

-- STEP 4c: payment type yang ada di tabel baru (cek tidak ada nama aneh / duplikat varian)
SELECT outlet_code, payment_type, COUNT(*) AS days, SUM(total_sales) AS total_sales
FROM daily_est_income_per_store_for_landlord
GROUP BY outlet_code, payment_type
ORDER BY outlet_code, total_sales DESC;

-- ------------------------------------------------------------
-- STEP 5 (di Airflow UI): UNPAUSE kedua DAG.
--
-- ROLLBACK (kalau ada masalah sebelum _old di-drop):
--   DROP TABLE daily_est_income_per_store_for_landlord;
--   RENAME TABLE daily_est_income_per_store_for_landlord_old
--             TO daily_est_income_per_store_for_landlord;
--   lalu revert kode ke commit sebelumnya.
--
-- Setelah beberapa hari aman, hapus backup:
--   DROP TABLE daily_est_income_per_store_for_landlord_old;
-- ------------------------------------------------------------
