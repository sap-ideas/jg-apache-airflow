-- ============================================================
-- Table: daily_rider_mangkal_session_summary_logs
-- Database: MySQL Data Warehouse (config_dw.db_target)
-- Written by: dags/f_rider_mangkal_summary/pipelines/etl_orchestrator.py
--   Full audit history, append-only (keeps every batch_label run of the
--   day: 08AM/11AM/03PM). PRIMARY KEY below matches the code comment's
--   stated intent — a vanilla append would hit a duplicate-key error if
--   the same batch is replayed, which is how retries stay safe.
-- ============================================================

CREATE TABLE IF NOT EXISTS daily_rider_mangkal_session_summary_logs (
    operating_date DATE NOT NULL,
    rider_code VARCHAR(50) NOT NULL,
    jilid VARCHAR(20) NULL,
    hub_name VARCHAR(255) NULL,
    business_unit VARCHAR(50) NULL,
    region VARCHAR(100) NULL,
    kota VARCHAR(100) NULL,
    provinsi VARCHAR(100) NULL,
    total_working_minutes FLOAT NULL,
    total_mangkal_time FLOAT NULL,
    total_mangkal_session FLOAT NULL,
    mangkal_mins_pct FLOAT NULL,
    total_sales FLOAT NULL,
    total_order FLOAT NULL,
    total_qty FLOAT NULL,
    total_click_by_user BIGINT NULL,
    batch_label VARCHAR(8) NOT NULL,
    load_data_by VARCHAR(100) NOT NULL DEFAULT 'NEXUS_AIRFLOW',
    load_data_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (operating_date, rider_code, batch_label)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
