"""Isolate a DAG's own pipeline modules from other DAGs' same-named modules.

Every DAG folder reuses generic filenames across the repo — `extract.py`,
`transform.py`, `load.py`, `etl_orchestrator.py`, and (between `iseller_pusat_dwh`
and `iseller_mitra_dwh`) even table-specific names like `pipeline_daily_items.py`.
Airflow parses every `dag_*.py` file into a single process to build/refresh the
DagBag — both on scheduler heartbeat and on `airflow tasks run` (which reloads the
whole DAGS_FOLDER to locate the target DAG). A bare
`sys.path.insert(0, dir); from extract import X` in one DAG can therefore end up
importing a DIFFERENT DAG's `extract.py` if that module name was already cached in
`sys.modules` by whichever DAG file happened to be parsed first in that process —
silently, with no import error, since the module name still resolves to *something*.
This bit `sjw_riders_mapping` in production on 2026-09-23 after a scheduler restart
changed the parse order.

`load_pipeline_dir()` closes that gap: it evicts any already-cached module that
shares a basename with a `.py` file in `pipelines_dir`, then puts `pipelines_dir`
first on `sys.path` — so the very next `import <name>` in this DAG is guaranteed to
read from THIS directory, not a stale module from another DAG.
"""

import glob
import os
import sys


def load_pipeline_dir(pipelines_dir: str) -> None:
    """Call once, immediately before importing this DAG's own pipeline modules.

    Safe to call more than once per process (e.g. once for a DAG folder and again
    for its `pipelines/` subfolder) and safe even when nothing actually collides —
    it only evicts modules whose name matches a `.py` file physically present in
    `pipelines_dir`.
    """
    local_module_names = {
        os.path.splitext(os.path.basename(f))[0]
        for f in glob.glob(os.path.join(pipelines_dir, "*.py"))
        if not os.path.basename(f).startswith("__")
    }
    for name in local_module_names:
        sys.modules.pop(name, None)

    if pipelines_dir in sys.path:
        sys.path.remove(pipelines_dir)
    sys.path.insert(0, pipelines_dir)
