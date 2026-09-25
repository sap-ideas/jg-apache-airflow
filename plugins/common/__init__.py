"""
common - shared cross-DAG utilities.

Resolved on PYTHONPATH because /opt/airflow/plugins is added in docker-compose.yaml
(and also auto-mounted by Airflow's plugin manager).

Import from any DAG/pipeline file as:
    from common.db_helpers import get_connection, get_engine, ...
"""
