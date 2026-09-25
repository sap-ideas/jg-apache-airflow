"""
Database configuration for KSJ Link PostgreSQL
Reads from environment variables — set via docker-compose.yaml (container) or
`.env` (loaded automatically here for manual/notebook runs outside Docker).
"""
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    # KSJ Link PostgreSQL Connection
    hostname = os.getenv('KSJ_LINK_HOST', 'db-ksj.jiwa.prod')
    # d_location runs on a separate cluster; other DBs (d_transaction, d_rider, …) use hostname above
    hostname_location = os.getenv(
        'KSJ_LINK_HOST_LOCATION',
        'db-ksj-location-service-database.jiwa.prod',
    )
    username = os.getenv('KSJ_LINK_USER', 'data-team')
    password = os.getenv('KSJ_LINK_PASSWORD')
    port = os.getenv('KSJ_LINK_PORT', '5432')
    
    # KSJ Link Databases
    d_transaction = os.getenv('KSJ_LINK_DB_TRANSACTION', 'd_transaction')
    d_location = os.getenv('KSJ_LINK_DB_LOCATION', 'd_location')
    d_payment = os.getenv('KSJ_LINK_DB_PAYMENT', 'd_payment')
    d_rider = os.getenv('KSJ_LINK_DB_RIDER', 'd_rider')
    d_cms = os.getenv('KSJ_LINK_DB_CMS', 'd_cms')
    # NOTE:
    # Some deployments mistakenly set KSJ_LINK_DB_JIWA_KSJ to the *variable name*
    # "d_jiwa_ksj" instead of the actual PostgreSQL database name "jiwa-ksj".
    # To keep pipelines resilient, we auto-correct that specific misconfiguration.
    _d_jiwa_ksj_raw = os.getenv('KSJ_LINK_DB_JIWA_KSJ', 'jiwa-ksj')
    d_jiwa_ksj = 'jiwa-ksj' if _d_jiwa_ksj_raw == 'd_jiwa_ksj' else _d_jiwa_ksj_raw
    d_jiwa_ondemand = os.getenv('KSJ_LINK_DB_JIWA_ONDEMAND', 'jiwa-order-dispatcher')


# Export module-level variables for backward compatibility
hostname = Config.hostname
hostname_location = Config.hostname_location
username = Config.username
password = Config.password
port = Config.port
d_transaction = Config.d_transaction
d_location = Config.d_location
d_payment = Config.d_payment
d_rider = Config.d_rider
d_cms = Config.d_cms
d_jiwa_ksj = Config.d_jiwa_ksj
d_jiwa_ondemand = Config.d_jiwa_ondemand
