-- ============================================================
-- Tables: d_sejutajiwa_riders_hubs_mapping, d_sejutajiwa_riders_hubs_mapping_mitra
-- Database: PostgreSQL KSJ Link, d_transaction (config_ksj.d_transaction)
-- Written by: dags/sjw_riders_mapping/dag_sync_postgresql.py
--   pipelines/sync_postgresql.py :: TRUNCATE + to_sql(if_exists='append')
--   — full mirror copy of the MySQL sejutajiwa_riders_hubs_mapping(_mitra)
--   tables (see sql/create_table_mysql.sql), minus `updated_by`, plus
--   `load_data_at`/`load_data_by`.
--
-- No UNIQUE constraint needed for correctness (TRUNCATE+INSERT doesn't
-- upsert/conflict) — a previous migration added one here on the
-- assumption the pipeline used ON CONFLICT, which is no longer true; see
-- git history. Indexes below are kept for downstream query performance
-- only, not as a duplicate-prevention mechanism.
-- ============================================================

CREATE TABLE IF NOT EXISTS d_sejutajiwa_riders_hubs_mapping (
    hub_id VARCHAR(20),
    jilid VARCHAR(20),
    hub_name VARCHAR(100),
    rider_id VARCHAR(50),
    cart_id VARCHAR(50),
    rider_name VARCHAR(150),
    status VARCHAR(10),
    scheme VARCHAR(20),
    phone_number VARCHAR(20),
    cashier_id VARCHAR(100),
    load_data_at TIMESTAMP,
    load_data_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_rider_id_d_sejutajiwa ON d_sejutajiwa_riders_hubs_mapping (rider_id);
CREATE INDEX IF NOT EXISTS idx_hub_id_d_sejutajiwa ON d_sejutajiwa_riders_hubs_mapping (hub_id);

CREATE TABLE IF NOT EXISTS d_sejutajiwa_riders_hubs_mapping_mitra (
    hub_id VARCHAR(20),
    jilid VARCHAR(20),
    hub_name VARCHAR(100),
    rider_id VARCHAR(50),
    cart_id VARCHAR(50),
    rider_name VARCHAR(150),
    status VARCHAR(10),
    scheme VARCHAR(20),
    phone_number VARCHAR(20),
    cashier_id VARCHAR(100),
    load_data_at TIMESTAMP,
    load_data_by VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_rider_id_d_sejutajiwa_mitra ON d_sejutajiwa_riders_hubs_mapping_mitra (rider_id);
CREATE INDEX IF NOT EXISTS idx_hub_id_d_sejutajiwa_mitra ON d_sejutajiwa_riders_hubs_mapping_mitra (hub_id);
