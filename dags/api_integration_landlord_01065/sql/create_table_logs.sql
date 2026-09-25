-- ============================================================
-- Table: api_landlord_logs
-- Database: MySQL Data Lake (config_dw.db_resource, via get_engine_datalake)
-- Written by: dags/api_integration_landlord_01065/pipelines/load.py :: dump_logs_idempotent()
--   DELETE FROM api_landlord_logs WHERE jilid=:jilid AND transaction_date=:td
--   then to_sql(if_exists='append') — one row per POST batch.
-- ============================================================

CREATE TABLE IF NOT EXISTS api_landlord_logs (
    revenue_batch_id VARCHAR(50) NULL,
    jilid VARCHAR(10) NOT NULL,
    transaction_date DATE NOT NULL,
    total_records_sent INT NOT NULL DEFAULT 0,
    total_success INT NULL,
    total_error INT NULL,
    http_status_code INT NULL,
    api_code VARCHAR(20) NULL,
    api_message TEXT NULL,
    api_url VARCHAR(255) NULL,
    request_payload LONGTEXT NULL,
    response_payload LONGTEXT NULL,
    created_at DATETIME NOT NULL,
    load_data_by VARCHAR(100) NOT NULL DEFAULT 'NEXUS_AIRFLOW',

    KEY idx_jilid_date (jilid, transaction_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
