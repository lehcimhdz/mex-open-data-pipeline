"""
Shared Airflow callbacks for all DAGs in this project.

on_dag_failure  — logs a structured error + sends email + sends Slack alert.
on_sla_miss     — logs a warning + sends Slack alert when a task misses its SLA.
_send_slack     — internal helper; posts to SLACK_WEBHOOK_URL Airflow Variable.

Slack setup
-----------
No extra packages required — uses stdlib urllib only.
1. Create an Incoming Webhook in your Slack workspace.
2. Set the webhook URL as an Airflow Variable:
       airflow variables set SLACK_WEBHOOK_URL https://hooks.slack.com/services/...
Alerts will be sent automatically on failure and SLA miss.
"""

from __future__ import annotations

import json
import logging
import urllib.request

from airflow.models import Variable

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal Slack helper
# ---------------------------------------------------------------------------

def _send_slack(text: str) -> None:
    """POST *text* to the configured Slack webhook (best-effort, never raises)."""
    webhook_url = Variable.get("SLACK_WEBHOOK_URL", default_var=None)
    if not webhook_url:
        return

    payload = json.dumps({"text": text}).encode()
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:  # noqa: S310
            if resp.status != 200:
                log.warning("Slack webhook returned HTTP %d", resp.status)
    except Exception as exc:
        log.warning("Failed to send Slack alert: %s", exc)


# ---------------------------------------------------------------------------
# DAG failure callback
# ---------------------------------------------------------------------------

def on_dag_failure(context: dict) -> None:
    """Structured-log a DAG/task failure, send email, and post a Slack alert."""
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "N/A"
    exception = context.get("exception", "unknown error")
    logical_date = context.get("logical_date") or context.get("execution_date")

    log.error(
        "DAG FAILURE | dag=%s | task=%s | run=%s | date=%s | error=%s",
        dag_id,
        task_id,
        run_id,
        logical_date,
        exception,
    )

    # ── Email alert ──────────────────────────────────────────────────────────
    # Requires AIRFLOW__SMTP__SMTP_HOST, AIRFLOW__SMTP__SMTP_USER,
    # AIRFLOW__SMTP__SMTP_PASSWORD, AIRFLOW__SMTP__SMTP_MAIL_FROM in .env
    alert_email = Variable.get("ALERT_EMAIL", default_var=None)
    if alert_email:
        try:
            from airflow.utils.email import send_email

            send_email(
                to=alert_email,
                subject=f"[Airflow] {dag_id} failed — task {task_id}",
                html_content=(
                    f"<b>DAG:</b> {dag_id}<br>"
                    f"<b>Task:</b> {task_id}<br>"
                    f"<b>Run:</b> {run_id}<br>"
                    f"<b>Date:</b> {logical_date}<br>"
                    f"<b>Error:</b> <pre>{exception}</pre>"
                ),
            )
        except Exception as exc:
            log.warning("Failed to send failure email: %s", exc)

    # ── Slack alert ──────────────────────────────────────────────────────────
    _send_slack(
        f":red_circle: *DAG failure* — `{dag_id}`\n"
        f">Task: `{task_id}` | Run: `{run_id}`\n"
        f">```{exception}```"
    )


# ---------------------------------------------------------------------------
# SLA miss callback
# ---------------------------------------------------------------------------

def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:  # noqa: ANN001
    """Called by Airflow when one or more tasks miss their SLA.

    Logs a structured warning and posts a Slack alert so the on-call team
    can investigate before the downstream DAG starts.
    """
    dag_id = dag.dag_id
    missed_tasks = [sla.task_id for sla in slas]

    log.warning(
        "SLA MISS | dag=%s | tasks=%s",
        dag_id,
        missed_tasks,
    )

    _send_slack(
        f":warning: *SLA missed* — DAG `{dag_id}`\n"
        f">Tasks: `{', '.join(missed_tasks)}`\n"
        f">Check the Airflow UI for details."
    )
