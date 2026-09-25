-- ============================================================
-- Table: hourly_sejutajiwa_rider_performance
-- Database: MySQL Data Warehouse (config_dw.db_target)
-- Written by: dags/f_hourly_rider_performance_tito/pipelines/etl_orchestrator.py
--   DELETE FROM hourly_sejutajiwa_rider_performance WHERE date = :d
--   then to_sql(if_exists='append') — overwrite-per-date, safe to retry.
-- ============================================================

CREATE TABLE IF NOT EXISTS hourly_sejutajiwa_rider_performance (
    rider_id           VARCHAR(50)     NOT NULL,
    jilid              VARCHAR(20)     NULL,
    hub_name           VARCHAR(255)    NULL,
    region             VARCHAR(100)    NULL,
    provinsi           VARCHAR(100)    NULL,
    kota               VARCHAR(100)    NULL,
    sejuta_jiwa_type   VARCHAR(50)     NULL,
    date               DATE            NOT NULL,
    checkin            DATETIME        NULL,
    checkout           DATETIME        NULL,
    hour_slot          INT             NOT NULL,
    week               INT             NULL,
    hour               INT             NULL,
    payment_type       VARCHAR(50)     NOT NULL,
    total_orders       BIGINT          NOT NULL DEFAULT 0,
    total_qty          BIGINT          NOT NULL DEFAULT 0,
    total_sales        DECIMAL(18,2)   NOT NULL DEFAULT 0,
    load_data_by       VARCHAR(100)    NOT NULL DEFAULT 'NEXUS_AIRFLOW',

    UNIQUE KEY uq_rider_date_hour_payment (rider_id, date, hour_slot, payment_type),
    KEY idx_date (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
