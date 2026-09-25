-- ============================================================
-- Table: api_landlord_log_detail
-- Database: MySQL Data Lake (config_dw.db_resource, via get_engine_datalake)
-- Written by: dags/api_integration_landlord_01065/pipelines/load.py :: dump_logs_idempotent()
--   DELETE FROM api_landlord_log_detail WHERE jilid=:jilid AND transaction_date=:td
--   then to_sql(if_exists='append') — one row per transaction sent to the API.
-- ============================================================

CREATE TABLE IF NOT EXISTS api_landlord_log_detail (
    jilid VARCHAR(10) NOT NULL,
    transaction_number VARCHAR(100) NULL,
    transaction_date DATE NOT NULL,
    payment_type VARCHAR(50) NULL,
    total_sales DECIMAL(18,2) NULL,
    total_tax DECIMAL(18,2) NULL,
    amount DECIMAL(18,4) NULL,
    remarks VARCHAR(255) NULL,
    mall_transaction_id VARCHAR(50) NULL,
    revenue_batch_id VARCHAR(50) NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT NULL,
    created_at DATETIME NOT NULL,
    load_data_by VARCHAR(100) NOT NULL DEFAULT 'NEXUS_AIRFLOW',

    KEY idx_jilid_date (jilid, transaction_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
