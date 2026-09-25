-- ============================================================
-- Table: daily_rider_mangkal_session_summary
-- Database: MySQL Data Warehouse (config_dw.db_target)
-- Written by: dags/f_rider_mangkal_summary/pipelines/etl_orchestrator.py
--   Overwrite-per-day snapshot (freshest batch of the day wins):
--   DELETE FROM daily_rider_mangkal_session_summary WHERE operating_date IN (...)
--   then to_sql(if_exists='append'). Same columns as
--   daily_rider_mangkal_session_summary_logs minus batch_label.
-- ============================================================

CREATE TABLE IF NOT EXISTS daily_rider_mangkal_session_summary (
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
    load_data_by VARCHAR(100) NOT NULL DEFAULT 'NEXUS_AIRFLOW',
    load_data_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (operating_date, rider_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
