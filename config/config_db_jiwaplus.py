"""
Database configuration for JiwaPlus PostgreSQL
Reads from environment variables — set via docker-compose.yaml (container) or
`.env` (loaded automatically here for manual/notebook runs outside Docker).
"""
import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    hostname = os.getenv('JIWAPLUS_HOST', 'db.jiwa.prod')
    database = os.getenv('JIWAPLUS_DATABASE', 'jiwaplus')
    username = os.getenv('JIWAPLUS_USER')
    password = os.getenv('JIWAPLUS_PASSWORD')
    port     = os.getenv('JIWAPLUS_PORT', '5432')


# Export module-level variables for backward compatibility
hostname = Config.hostname
database = Config.database
username = Config.username
password = Config.password
port     = Config.port
