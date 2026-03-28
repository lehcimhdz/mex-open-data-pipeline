"""Unit tests for dags/utils/callbacks.py."""
from __future__ import annotations

import os
from unittest.mock import MagicMock, call, patch

import pytest

os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-test")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "false")
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "true")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context(dag_id: str = "test_dag", task_id: str = "test_task") -> dict:
    dag = MagicMock()
    dag.dag_id = dag_id
    task_instance = MagicMock()
    task_instance.task_id = task_id
    return {
        "dag": dag,
        "run_id": "manual__2026-01-01",
        "task_instance": task_instance,
        "exception": ValueError("boom"),
        "logical_date": "2026-01-01T00:00:00+00:00",
    }


# ---------------------------------------------------------------------------
# on_dag_failure
# ---------------------------------------------------------------------------

class TestOnDagFailure:
    def test_logs_structured_error(self):
        from utils.callbacks import on_dag_failure

        with patch("utils.callbacks.log") as mock_log, \
             patch("utils.callbacks.Variable.get", return_value=None), \
             patch("utils.callbacks._send_slack"):
            on_dag_failure(_make_context())

        mock_log.error.assert_called_once()
        args = mock_log.error.call_args[0]
        assert "test_dag" in str(args)
        assert "test_task" in str(args)

    def test_skips_email_when_no_alert_email(self):
        from utils.callbacks import on_dag_failure

        with patch("utils.callbacks.Variable.get", return_value=None), \
             patch("utils.callbacks.log"), \
             patch("utils.callbacks._send_slack"), \
             patch("airflow.utils.email.send_email") as mock_email:
            on_dag_failure(_make_context())

        mock_email.assert_not_called()

    def test_sends_email_when_alert_email_set(self):
        from utils.callbacks import on_dag_failure

        def _var_get(key, default_var=None):
            return "alerts@example.com" if key == "ALERT_EMAIL" else default_var

        with patch("utils.callbacks.Variable.get", side_effect=_var_get), \
             patch("utils.callbacks.log"), \
             patch("utils.callbacks._send_slack"), \
             patch("airflow.utils.email.send_email") as mock_email:
            on_dag_failure(_make_context())

        mock_email.assert_called_once()
        _, kwargs = mock_email.call_args
        assert "test_dag" in kwargs["subject"]

    def test_does_not_raise_when_email_fails(self):
        from utils.callbacks import on_dag_failure

        def _var_get(key, default_var=None):
            return "alerts@example.com" if key == "ALERT_EMAIL" else default_var

        with patch("utils.callbacks.Variable.get", side_effect=_var_get), \
             patch("utils.callbacks.log"), \
             patch("utils.callbacks._send_slack"), \
             patch("airflow.utils.email.send_email", side_effect=RuntimeError("SMTP down")):
            on_dag_failure(_make_context())  # must not propagate

    def test_always_calls_send_slack(self):
        from utils.callbacks import on_dag_failure

        with patch("utils.callbacks.Variable.get", return_value=None), \
             patch("utils.callbacks.log"), \
             patch("utils.callbacks._send_slack") as mock_slack:
            on_dag_failure(_make_context("my_dag", "my_task"))

        mock_slack.assert_called_once()
        text = mock_slack.call_args[0][0]
        assert "my_dag" in text
        assert "my_task" in text


# ---------------------------------------------------------------------------
# on_sla_miss
# ---------------------------------------------------------------------------

class TestOnSlaMiss:
    def _make_sla(self, task_id: str = "slow_task"):
        sla = MagicMock()
        sla.task_id = task_id
        return sla

    def test_logs_warning(self):
        from utils.callbacks import on_sla_miss

        dag = MagicMock()
        dag.dag_id = "ingest_datasets"
        sla = self._make_sla("ingest_category")

        with patch("utils.callbacks.log") as mock_log, \
             patch("utils.callbacks._send_slack"):
            on_sla_miss(dag, [], [], [sla], [])

        mock_log.warning.assert_called_once()
        assert "ingest_datasets" in str(mock_log.warning.call_args)

    def test_calls_send_slack_with_dag_and_task(self):
        from utils.callbacks import on_sla_miss

        dag = MagicMock()
        dag.dag_id = "sync_catalog"
        sla = self._make_sla("fetch_and_store_category")

        with patch("utils.callbacks.log"), \
             patch("utils.callbacks._send_slack") as mock_slack:
            on_sla_miss(dag, [], [], [sla], [])

        mock_slack.assert_called_once()
        text = mock_slack.call_args[0][0]
        assert "sync_catalog" in text
        assert "fetch_and_store_category" in text

    def test_handles_multiple_missed_tasks(self):
        from utils.callbacks import on_sla_miss

        dag = MagicMock()
        dag.dag_id = "ingest_datasets"
        slas = [self._make_sla("task_a"), self._make_sla("task_b")]

        with patch("utils.callbacks.log"), \
             patch("utils.callbacks._send_slack") as mock_slack:
            on_sla_miss(dag, [], [], slas, [])

        text = mock_slack.call_args[0][0]
        assert "task_a" in text
        assert "task_b" in text


# ---------------------------------------------------------------------------
# _send_slack
# ---------------------------------------------------------------------------

class TestSendSlack:
    def test_skips_when_no_webhook_url(self):
        from utils.callbacks import _send_slack

        with patch("utils.callbacks.Variable.get", return_value=None), \
             patch("urllib.request.urlopen") as mock_urlopen:
            _send_slack("hello")

        mock_urlopen.assert_not_called()

    def test_posts_to_webhook(self):
        from utils.callbacks import _send_slack

        mock_response = MagicMock()
        mock_response.__enter__ = MagicMock(return_value=MagicMock(status=200))
        mock_response.__exit__ = MagicMock(return_value=False)

        with patch("utils.callbacks.Variable.get", return_value="https://hooks.slack.com/T/B/x"), \
             patch("urllib.request.urlopen", return_value=mock_response) as mock_urlopen:
            _send_slack("test message")

        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        assert b"test message" in req.data

    def test_does_not_raise_on_network_error(self):
        from utils.callbacks import _send_slack

        with patch("utils.callbacks.Variable.get", return_value="https://hooks.slack.com/T/B/x"), \
             patch("urllib.request.urlopen", side_effect=OSError("network down")), \
             patch("utils.callbacks.log"):
            _send_slack("hello")  # must not propagate
