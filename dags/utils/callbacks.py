"""
Shared Airflow callbacks for all DAGs in this project.

on_dag_failure logs a structured error message and optionally sends an email
alert when AIRFLOW__SMTP__SMTP_HOST is configured in the environment.

To add Slack alerts, install apache-airflow-providers-slack and uncomment the
SlackWebhookHook block below, then set a SLACK_CONN_ID Airflow connection.
"""

from __future__ import annotations

import logging

from airflow.models import Variable

log = logging.getLogger(__name__)


def on_dag_failure(context: dict) -> None:
    """Structured-log a DAG/task failure and send an email alert if SMTP is configured."""
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

    # Email alert — requires SMTP vars in airflow.cfg or env:
    #   AIRFLOW__SMTP__SMTP_HOST, AIRFLOW__SMTP__SMTP_USER, AIRFLOW__SMTP__SMTP_PASSWORD
    #   AIRFLOW__SMTP__SMTP_MAIL_FROM
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

    # Slack alert — uncomment after installing apache-airflow-providers-slack
    # and creating a SLACK_CONN_ID connection in Airflow with the webhook URL.
    # try:
    #     from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    #     SlackWebhookHook(slack_webhook_conn_id="slack_default").send(
    #         text=(
    #             f":red_circle: *{dag_id}* failed\n"
    #             f">Task: `{task_id}` | Run: `{run_id}`\n"
    #             f">```{exception}```"
    #         )
    #     )
    # except Exception as exc:
    #     log.warning("Failed to send Slack alert: %s", exc)
