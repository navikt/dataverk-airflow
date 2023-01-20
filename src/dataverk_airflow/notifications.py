import os

from typing import Sequence, Union, List

from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.email import EmailOperator
from datetime import datetime


def create_email_notification(dag_id: str, email: Union[List[str], str], name: str, namespace: str, dag):
    return EmailOperator(
                task_id="send-email-on-error",
                to=email,
                subject=f"Airflow task {name} error",
                html_content=f"<p> Airflow task {name} i DAG {dag_id} feiler i namespace {namespace} "
                f"kl. {datetime.now().isoformat()}. ",
                dag=dag,
            )


def create_slack_notification(dag_id: str, slack_channel: str, name: str, namespace: str):
    return SlackWebhookOperator(
        task_id="airflow_task_failed",
        http_conn_id=None,
        webhook_token=os.environ["SLACK_TOKEN"],
        message=f"@here Airflow task {name} i DAG {dag_id} feilet i namespace {namespace} kl. {datetime.now().isoformat()}. ",
        channel=slack_channel,
        link_names=True,
        icon_emoji=":sadpanda:",
    )
