-- ============================================================
-- Tambah kolom total_order ke daily_est_income_per_store_for_landlord
-- Jalankan MANUAL di DBeaver (MySQL 5.7), database data warehouse (config_dw.db_target).
-- Pipeline tidak pernah menjalankan DDL (lihat CLAUDE.md 3.2).
--
-- total_order = COUNT(DISTINCT order_id) per date + outlet + payment_type (dari transactions_iseller_pusat,
-- filter yang sama dengan nominal uang). Order split payment dihitung sekali per payment type yang dipakai,
-- jadi SUM(total_order) antar payment type bisa lebih besar dari jumlah order unik outlet-hari.
--
-- URUTAN (penting):
--   1. Jalankan STEP 1 (ALTER) DULU, sebelum kode baru dijalankan Airflow.
--      Aman untuk kode lama: kolom ber-default 0, jadi INSERT lama tetap jalan (tidak perlu pause DAG).
--   2. Deploy / merge kode baru. Kalau DAG producer jalan SEBELUM STEP 1, INSERT gagal
--      (kolom total_order belum ada). Transaksi di-rollback, data lama aman, tapi run-nya error.
--   3. Backfill histori lewat notebook (daily_est_income_per_store_for_landlord.ipynb),
--      start_date = 2026-01-01, end_date = kemarin, 3 outlet (00570, 00989, 01165).
--   4. Jalankan query cek STEP 2 dan STEP 3.
-- ============================================================

-- STEP 1: tambah kolom
ALTER TABLE daily_est_income_per_store_for_landlord
    ADD COLUMN total_order INT NOT NULL DEFAULT 0
    COMMENT 'Jumlah order unik (COUNT DISTINCT order_id) per payment type, order split payment dihitung sekali per payment type yang dipakai, jadi jangan dijumlah antar payment type'
    AFTER total_sales;

-- STEP 2 (setelah backfill): baris yang belum terisi total_order. Hasil harus KOSONG.
--   Ada total_sales tapi total_order = 0 -> baris itu belum ke-backfill / belum terhitung ulang.
SELECT `date`, outlet_code, payment_type, total_sales, total_order, load_data_at, load_data_by
FROM daily_est_income_per_store_for_landlord
WHERE total_sales <> 0 AND total_order = 0
ORDER BY `date`, outlet_code, payment_type;

-- STEP 3 (setelah backfill): rekonsiliasi per outlet-hari, total_order tabel ini vs jumlah order unik langsung dari Data Lake.
--   Selisih hanya boleh muncul di hari yang punya order split payment (order dihitung di >1 payment type).
--   Ganti nama database kalau beda (default: data_warehouse_jiwa dan data_lake_jiwa) dan sesuaikan rentang tanggal.
--   Catatan: scan tabel Data Lake tanpa index, batasi rentang tanggalnya.
SELECT w.`date`, w.outlet_code,
       w.orders_dw, l.orders_lake, w.orders_dw - IFNULL(l.orders_lake, 0) AS diff
FROM (
    SELECT `date`, outlet_code, SUM(total_order) AS orders_dw
    FROM data_warehouse_jiwa.daily_est_income_per_store_for_landlord
    WHERE `date` BETWEEN '2026-10-01' AND '2026-10-31'
    GROUP BY `date`, outlet_code
) w
LEFT JOIN (
    SELECT DATE_FORMAT(transaction_date, '%Y-%m-%d') AS d, outlet_code, COUNT(DISTINCT order_id) AS orders_lake
    FROM data_lake_jiwa.transactions_iseller_pusat
    WHERE DATE_FORMAT(transaction_date, '%Y-%m-%d') BETWEEN '2026-10-01' AND '2026-10-31'
        AND outlet_code IN ('00570', '00989', '01165')
        AND fulfillment_status = 'fulfilled'
        AND status = 'completed'
        AND payment_status = 'paid'
        AND transactions_status = 'success'
    GROUP BY 1, 2
) l ON l.d = DATE_FORMAT(w.`date`, '%Y-%m-%d') AND l.outlet_code = w.outlet_code
WHERE w.orders_dw <> IFNULL(l.orders_lake, 0)
ORDER BY w.`date`, w.outlet_code;

-- ROLLBACK (kalau perlu, sebelum / sesudah deploy kode baru):
--   ALTER TABLE daily_est_income_per_store_for_landlord DROP COLUMN total_order;
--   lalu revert kode producer ke commit sebelumnya.
