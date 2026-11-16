"""Resolve who triggered a manual DAG run from the Airflow UI.

Airflow 3 records the triggering user on the DAG run itself (`dag_run.triggering_user_name`):
the user's display name ("<first name> <last name>") for UI / REST API triggers, the unix user
for CLI triggers. Airflow users have their email as first name and an empty last name, so a UI
trigger yields that email. (Tasks can no longer read the metadata DB, so the old `log` / `ab_user`
lookup is gone.)

Used to fill `load_data_by` on manual runs, so human-triggered loads are attributable
(automatic runs keep the 'NEXUS_AIRFLOW' tag).

Importable from any DAG file as:
    from common.airflow_trigger_user import get_manual_trigger_user
"""

import logging

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK = "MANUAL_AIRFLOW"


def get_manual_trigger_user(dag_run, fallback: str = DEFAULT_FALLBACK) -> str:
    """Email of the Airflow user who triggered `dag_run` from the UI.

    Returns `fallback` when the triggering user is missing or is not an email
    (e.g. CLI trigger -> unix user name). Never raises: a failed lookup must not fail the DAG.
    """
    try:
        user = (getattr(dag_run, "triggering_user_name", None) or "").strip()
        if "@" not in user:
            print(f"  load_data_by: triggering user {user!r} is not an email for {dag_run.run_id}, fallback {fallback}")
            return fallback
        return user
    except Exception as e:  # noqa: BLE001 — see docstring
        logger.warning("Could not resolve trigger user for %s: %s", getattr(dag_run, "run_id", dag_run), e)
        return fallback
