import os

from typing import Sequence

from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime


def create_email_notification(email: Sequence, name: str, namespace: str, dag):
    return EmailOperator(
                task_id="send-email-on-error",
                to=email,
                subject=f"Airflow task {name} error",
                html_content=f"<p> Airflow task {name} feiler i namespace {namespace} "
                f"kl. {datetime.now().isoformat()}. "
                f"Logger: {os.environ['AIRFLOW__WEBSERVER__BASE_URL']} </p>",
                dag=dag,
            )


def create_slack_notification(slack_channel: str, name: str, namespace: str):
    return SlackWebhookOperator(
        task_id="airflow_task_failed",
        webhook_token=os.environ["SLACK_WEBHOOK_TOKEN"],
        message=f"@here DAG {name} feilet i namespace {namespace} kl. {datetime.now().isoformat()}. "
                f'Logger: {os.environ["AIRFLOW__WEBSERVER__BASE_URL"]}',
        channel=slack_channel,
        link_names=True,
        icon_emoji=":sadpanda:",
        proxy=os.environ["HTTPS_PROXY"],
    )
