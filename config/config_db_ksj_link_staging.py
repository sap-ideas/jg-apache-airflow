"""
Database configuration for KSJ Link PostgreSQL **STAGING** (milik tim tech).
Reads from environment variables — set via docker-compose.yaml (container) or
`.env` (loaded automatically here for manual/notebook runs outside Docker).

Host / user / password sengaja TANPA default (jangan hardcode kredensial).
"""
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    hostname = os.getenv('KSJ_LINK_STG_HOST')
    username = os.getenv('KSJ_LINK_STG_USER')
    password = os.getenv('KSJ_LINK_STG_PASSWORD')
    port = os.getenv('KSJ_LINK_STG_PORT', '5432')
    d_transaction = os.getenv('KSJ_LINK_STG_DB_TRANSACTION', 'd_transaction')
    d_rider = os.getenv('KSJ_LINK_STG_DB_RIDER', 'd_rider')


# Export module-level variables (sama dengan config_db_ksj_link.py)
hostname = Config.hostname
username = Config.username
password = Config.password
port = Config.port
d_transaction = Config.d_transaction
d_rider = Config.d_rider
