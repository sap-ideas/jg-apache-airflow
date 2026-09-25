"""
Database configuration for Airflow DAGs
Reads from environment variables — set via docker-compose.yaml (container) or
`.env` (loaded automatically here for manual/notebook runs outside Docker).
"""
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    # Data Lake Database
    hostname_lake = os.getenv('DB_LAKE_HOST', 'dblake.jiwa.prod')
    username_lake = os.getenv('DB_LAKE_USER')
    password_lake = os.getenv('DB_LAKE_PASSWORD')
    db_lake = os.getenv('DB_LAKE_NAME', 'data_lake_jiwa')

    # Data Warehouse Database (default connection)
    hostname = os.getenv('DB_WAREHOUSE_HOST', 'dblake.jiwa.prod')
    username = os.getenv('DB_WAREHOUSE_USER')
    password = os.getenv('DB_WAREHOUSE_PASSWORD')
    db_resource = os.getenv('DB_LAKE_NAME', 'data_lake_jiwa')  # Data Lake for MYSQL_URI_DATALAKE
    db_target = os.getenv('DB_WAREHOUSE_NAME', 'data_warehouse_jiwa')  # Data Warehouse for MYSQL_URI_WAREHOUSE


# Export module-level variables for backward compatibility
hostname = Config.hostname
username = Config.username
password = Config.password
db_resource = Config.db_resource
db_target = Config.db_target

hostname_lake = Config.hostname_lake
username_lake = Config.username_lake
password_lake = Config.password_lake
db_lake = Config.db_lake


