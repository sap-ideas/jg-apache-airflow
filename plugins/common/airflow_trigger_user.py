"""Resolve who triggered a manual DAG run from the Airflow UI.

Airflow 2.8 does not store the triggering user on `dag_run`, but the UI writes an audit row to
the `log` table (event='trigger', owner=<username>, execution_date=<run's logical_date>).
The username is mapped to an email through FAB's `ab_user`.

Used to fill `load_data_by` on manual runs, so human-triggered loads are attributable
(automatic runs keep the 'NEXUS_AIRFLOW' tag).

Importable from any DAG file as:
    from common.airflow_trigger_user import get_manual_trigger_user
"""

import logging

from airflow.models import Log
from airflow.utils.session import create_session
from sqlalchemy import select, text

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK = "MANUAL_AIRFLOW"


def get_manual_trigger_user(dag_run, fallback: str = DEFAULT_FALLBACK) -> str:
    """Email of the Airflow user who triggered `dag_run` from the UI.

    Order: email from `ab_user` -> username (if the user has no email) -> `fallback`
    (run triggered through CLI / REST API, audit row missing, or any lookup error).
    Never raises: a failed lookup must not fail the DAG.
    """
    try:
        with create_session() as session:
            owner = session.execute(
                select(Log.owner)
                .where(
                    Log.dag_id == dag_run.dag_id,
                    Log.event == "trigger",
                    Log.execution_date == dag_run.logical_date,
                    Log.owner.is_not(None),
                )
                .order_by(Log.dttm.desc())
                .limit(1)
            ).scalar()
            if not owner:
                print(f"  load_data_by: no trigger audit row for {dag_run.run_id}, fallback {fallback}")
                return fallback

            email = session.execute(
                text("SELECT email FROM ab_user WHERE username = :u"), {"u": owner}
            ).scalar()
            return (email or "").strip() or owner
    except Exception as e:  # noqa: BLE001 — see docstring
        logger.warning("Could not resolve trigger user for %s: %s", getattr(dag_run, "run_id", dag_run), e)
        return fallback
