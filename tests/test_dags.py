"""Structural tests for Airflow DAGs — no AWS calls, no network."""
import os
from unittest.mock import MagicMock, patch

import pytest

# Point Airflow at a temp home so it doesn't need an existing installation
os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-test")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "true")


# ---------------------------------------------------------------------------
# DagBag — import-level errors
# ---------------------------------------------------------------------------

def test_dag_bag_no_import_errors():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert dagbag.import_errors == {}, (
        f"DAG import errors: {dagbag.import_errors}"
    )


def test_all_three_dags_present():
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert "sync_catalog" in dagbag.dags
    assert "ingest_datasets" in dagbag.dags
    assert "load_to_db" in dagbag.dags


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_dag(dag_id: str):
    from airflow.models import DagBag

    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    return dagbag.dags[dag_id]


# ---------------------------------------------------------------------------
# sync_catalog
# ---------------------------------------------------------------------------

def test_sync_catalog_schedule():
    dag = _get_dag("sync_catalog")
    assert dag.schedule_interval == "0 6 * * 1-5"


def test_sync_catalog_max_active_runs():
    dag = _get_dag("sync_catalog")
    assert dag.max_active_runs == 1


def test_sync_catalog_has_expected_tasks():
    dag = _get_dag("sync_catalog")
    task_ids = {t.task_id for t in dag.tasks}
    assert "fetch_category_slugs" in task_ids
    assert "fetch_and_store_category" in task_ids


def test_sync_catalog_no_cycles():
    dag = _get_dag("sync_catalog")
    assert dag.test_cycle() is False


def test_sync_catalog_has_sla_miss_callback():
    dag = _get_dag("sync_catalog")
    assert dag.sla_miss_callback is not None


# ---------------------------------------------------------------------------
# ingest_datasets
# ---------------------------------------------------------------------------

def test_ingest_datasets_schedule():
    dag = _get_dag("ingest_datasets")
    assert dag.schedule_interval == "0 7 * * 1-5"


def test_ingest_datasets_max_active_runs():
    dag = _get_dag("ingest_datasets")
    assert dag.max_active_runs == 1


def test_ingest_datasets_has_expected_tasks():
    dag = _get_dag("ingest_datasets")
    task_ids = {t.task_id for t in dag.tasks}
    assert "wait_for_catalog" in task_ids
    assert "get_category_slugs" in task_ids
    assert "ingest_category" in task_ids
    assert "trigger_glue_crawler" in task_ids


def test_ingest_datasets_sensor_is_upstream_of_slugs():
    dag = _get_dag("ingest_datasets")
    sensor = dag.get_task("wait_for_catalog")
    slugs_task = dag.get_task("get_category_slugs")
    assert slugs_task.task_id in {t.task_id for t in sensor.downstream_list}


def test_ingest_datasets_no_cycles():
    dag = _get_dag("ingest_datasets")
    assert dag.test_cycle() is False


def test_ingest_datasets_has_sla_miss_callback():
    dag = _get_dag("ingest_datasets")
    assert dag.sla_miss_callback is not None


# ---------------------------------------------------------------------------
# load_to_db
# ---------------------------------------------------------------------------

def test_load_to_db_schedule():
    dag = _get_dag("load_to_db")
    assert dag.schedule_interval == "30 8 * * 1-5"


def test_load_to_db_max_active_runs():
    dag = _get_dag("load_to_db")
    assert dag.max_active_runs == 1


def test_load_to_db_has_expected_tasks():
    dag = _get_dag("load_to_db")
    task_ids = {t.task_id for t in dag.tasks}
    assert "wait_for_ingest" in task_ids
    assert "get_category_slugs" in task_ids
    assert "load_category" in task_ids
    assert "log_summary" in task_ids
    assert "validate_load" in task_ids


def test_load_to_db_sensor_is_upstream_of_slugs():
    dag = _get_dag("load_to_db")
    sensor = dag.get_task("wait_for_ingest")
    slugs_task = dag.get_task("get_category_slugs")
    assert slugs_task.task_id in {t.task_id for t in sensor.downstream_list}


def test_load_to_db_no_cycles():
    dag = _get_dag("load_to_db")
    assert dag.test_cycle() is False


def test_load_to_db_has_sla_miss_callback():
    dag = _get_dag("load_to_db")
    assert dag.sla_miss_callback is not None
