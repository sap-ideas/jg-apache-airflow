-- ============================================================
-- Table: daily_est_income_per_store_for_landlord
-- Database: MySQL Data Warehouse (config_dw.db_target)
-- Written by: dags/f_daily_estimated_income_per_store/pipelines/load.py
--   DELETE FROM daily_est_income_per_store_for_landlord WHERE date = :d
--   then to_sql(if_exists='append') — overwrite-per-date, safe to retry.
-- ============================================================

CREATE TABLE IF NOT EXISTS daily_est_income_per_store_for_landlord (
    date DATE NOT NULL,
    outlet_code VARCHAR(20) NOT NULL,
    outlet_name VARCHAR(255) NULL,
    net_sales DECIMAL(18,2) NOT NULL DEFAULT 0,
    tax_amount DECIMAL(18,2) NOT NULL DEFAULT 0,
    total_sales DECIMAL(18,2) NOT NULL DEFAULT 0,

    cash DECIMAL(18,2) NOT NULL DEFAULT 0,
    cash_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    cash_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    bri DECIMAL(18,2) NOT NULL DEFAULT 0,
    bri_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    bri_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    go_food DECIMAL(18,2) NOT NULL DEFAULT 0,
    go_food_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    go_food_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    grab_food DECIMAL(18,2) NOT NULL DEFAULT 0,
    grab_food_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    grab_food_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    grab_dine_out DECIMAL(18,2) NOT NULL DEFAULT 0,
    grab_dine_out_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    grab_dine_out_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    shopee_food DECIMAL(18,2) NOT NULL DEFAULT 0,
    shopee_food_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    shopee_food_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    tokopedia_go DECIMAL(18,2) NOT NULL DEFAULT 0,
    tokopedia_go_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    tokopedia_go_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    jiwaplus DECIMAL(18,2) NOT NULL DEFAULT 0,
    jiwaplus_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    jiwaplus_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    iseller_qris DECIMAL(18,2) NOT NULL DEFAULT 0,
    iseller_qris_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    iseller_qris_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    qpon DECIMAL(18,2) NOT NULL DEFAULT 0,
    qpon_subtotal DECIMAL(18,2) NOT NULL DEFAULT 0,
    qpon_commission DECIMAL(18,2) NOT NULL DEFAULT 0,

    total_commission_cost DECIMAL(18,2) NOT NULL DEFAULT 0,

    load_data_at DATETIME NOT NULL,
    load_data_by VARCHAR(100) NOT NULL DEFAULT 'NEXUS_AIRFLOW',

    UNIQUE KEY uq_date_outlet (date, outlet_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
