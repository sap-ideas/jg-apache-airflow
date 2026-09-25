-- ============================================================
-- Table: ksj_on_demand_order_summary
-- Database: MySQL Data Warehouse (config_dw.db_target)
-- Written by: dags/f_ksj_ondemand_order_summary/pipelines/etl_orchestrator.py
--   DELETE FROM ksj_on_demand_order_summary WHERE order_date IN (...)
--   then to_sql(if_exists='append') — overwrite-per-date, safe to retry.
-- ============================================================

CREATE TABLE IF NOT EXISTS ksj_on_demand_order_summary (
    transaction_id                      BIGINT          NOT NULL,
    transaction_datetime_gmt            DATETIME        NULL,
    order_date                          DATE            NOT NULL,
    order_hour                          INT             NULL,
    pending_order_id                    BIGINT          NOT NULL,
    latest_transaction_status           VARCHAR(50)     NULL,
    order_status                        VARCHAR(20)     NULL,
    order_status_detail                 VARCHAR(255)    NULL,
    hub_code                            VARCHAR(50)     NULL,
    customer_id                         BIGINT          NULL,
    customer_name                       VARCHAR(255)    NULL,
    customer_phone                      VARCHAR(50)     NULL,
    have_markassajiwa                   TINYINT(1)      NULL,
    order_qty                           INT             NULL,
    total_price                         DECIMAL(18,2)   NULL,
    payment_type                        VARCHAR(50)     NULL,
    minutes_paid_to_completed           FLOAT           NULL,
    seconds_to_accept                   FLOAT           NULL,
    minutes_accept_to_otw               FLOAT           NULL,
    minutes_otw_to_delivered            FLOAT           NULL,
    minutes_accept_to_delivered         FLOAT           NULL,
    origin_type                         VARCHAR(30)     NULL,
    origin_rider_code                   VARCHAR(50)     NULL,
    origin_lat                          FLOAT           NULL,
    origin_lng                          FLOAT           NULL,
    delivery_lat                        FLOAT           NULL,
    delivery_lng                        FLOAT           NULL,
    straight_line_distance_m            FLOAT           NULL,
    actual_route_distance_m             FLOAT           NULL,
    route_duration_seconds              FLOAT           NULL,
    delivered_distance_m                FLOAT           NULL,
    nearest_blasted_rider_distance_m    FLOAT           NULL,
    avg_blasted_rider_distance_m        FLOAT           NULL,
    median_blasted_rider_distance_m     FLOAT           NULL,
    max_blasted_rider_distance_m        FLOAT           NULL,
    route_source                        VARCHAR(20)     NULL,
    total_rider_blasted                 INT             NULL,
    total_rider_blasted_until_accept    INT             NULL,
    total_blast_rounds                  INT             NULL,
    accepted_blast_round                INT             NULL,
    blasted_rider_codes                 TEXT            NULL,
    load_data_at                        DATETIME        NOT NULL,
    load_data_by                        VARCHAR(100)    NOT NULL DEFAULT 'NEXUS_AIRFLOW',

    UNIQUE KEY uq_pending_order_id (pending_order_id),
    KEY idx_order_date (order_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
