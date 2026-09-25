-- ============================================================
-- Tables: sejutajiwa_riders_hubs_mapping, sejutajiwa_riders_hubs_mapping_mitra
-- Database: MySQL Data Lake (config_dwh.db_lake)
-- Written by: dags/sjw_riders_mapping/dag_riders_ksj_link_mapping.py
--   pipelines/load.py :: append_only_ignore_duplicates() does
--   "INSERT IGNORE INTO ... VALUES (...)" — correctness depends entirely
--   on the UNIQUE KEY below (INSERT IGNORE silently no-ops on a
--   duplicate rider_id instead of erroring, which is how "append only
--   new riders" is implemented). Without this key, every run would
--   insert duplicate rows.
--
-- Same schema for both tables — _mitra just holds ownership_status='MITRA'
-- rows instead of 'KBN'.
-- ============================================================

CREATE TABLE IF NOT EXISTS sejutajiwa_riders_hubs_mapping (
    hub_id VARCHAR(20) NULL,
    jilid VARCHAR(20) NULL,
    hub_name VARCHAR(100) NULL,
    rider_id VARCHAR(50) NOT NULL,
    cart_id VARCHAR(50) NULL,
    rider_name VARCHAR(150) NULL,
    status VARCHAR(10) NULL,
    scheme VARCHAR(20) NULL,
    phone_number VARCHAR(20) NULL,
    cashier_id VARCHAR(100) NULL,
    updated_by VARCHAR(50) NOT NULL DEFAULT 'NEXUS_AIRFLOW',

    UNIQUE KEY uq_rider_id (rider_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sejutajiwa_riders_hubs_mapping_mitra (
    hub_id VARCHAR(20) NULL,
    jilid VARCHAR(20) NULL,
    hub_name VARCHAR(100) NULL,
    rider_id VARCHAR(50) NOT NULL,
    cart_id VARCHAR(50) NULL,
    rider_name VARCHAR(150) NULL,
    status VARCHAR(10) NULL,
    scheme VARCHAR(20) NULL,
    phone_number VARCHAR(20) NULL,
    cashier_id VARCHAR(100) NULL,
    updated_by VARCHAR(50) NOT NULL DEFAULT 'NEXUS_AIRFLOW',

    UNIQUE KEY uq_rider_id (rider_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
