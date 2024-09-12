import os
from datetime import datetime
from typing import List, Union

from airflow.operators.email import EmailOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator


def create_email_notification(dag, email: Union[List[str], str], name: str, namespace: str):
    return EmailOperator(
                dag=dag,
                task_id="airflow_task_failed_email",
                to=email,
                subject=f"Airflow task {name} error",
                html_content=f"<p> Airflow task {name} i DAG {dag._dag_id} feiler i namespace {namespace} "
                f"kl. {datetime.now().isoformat()}. ",
            )


def create_slack_notification(dag, slack_channel: str, name: str, namespace: str):
    return SlackAPIPostOperator(
            dag=dag,
            task_id="airflow_task_failed_slack",
            slack_conn_id="slack_connection",
            text=f":error: DAG *{dag._dag_id}* feilet :error: \n Sjekk task _{name}_ i namespace {namespace}",
            channel=slack_channel,
    )
