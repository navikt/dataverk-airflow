import os
import kubernetes.client as k8s

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.email_operator import EmailOperator


def create_knada_nb_pod_operator(
    dag: DAG,
    name: str,
    repo: str,
    nb_path: str,
    namespace: str,
    email: str = None,
    slack_channel: str = None,
    branch: str = "master",
    log_output: bool = False,
    resources: dict = None,
    retries: int = 3,
    retry_delay: timedelta = timedelta(seconds=5),
):
    """ Factory function for creating KubernetesPodOperator for executing knada jupyter notebooks

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param nb_path: str: Path to notebook in repo
    :param namespace: str: K8S namespace for pod
    :param email: str: Email of owner
    :param slack_channel: Name of slack channel, default None (no slack notification)
    :param branch: str: Branch in repo, default "master"
    :param log_output: bool: Write logs from notebook to stdout, default False
    :param resources: dict: Specify required cpu and memory requirements (keys in dict: request_memory, request_cpu, limit_memory, limit_cpu), default None
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :return: KubernetesPodOperator
    """

    def on_failure(context):
        if email:
            send_email = EmailOperator(
                task_id="send-email-on-error",
                to=email,
                subject=f"Airflow task {name} error",
                html_content=f"<p> Airflow task {name} feiler i namespace {namespace} "
                f"kl. {datetime.now().isoformat()}. "
                f"Logger: {os.environ['AIRFLOW__WEBSERVER__BASE_URL']} </p>",
                dag=dag,
            )

            send_email.execute(context)

        if slack_channel:
            slack_notification = SlackWebhookOperator(
                task_id="airflow_task_failed",
                webhook_token=os.environ["SLACK_WEBHOOK_TOKEN"],
                message=f"@here DAG {name} feilet i namespace {namespace} kl. {datetime.now().isoformat()}. "
                f'Logger: {os.environ["AIRFLOW__WEBSERVER__BASE_URL"]}',
                channel=slack_channel,
                link_names=True,
                icon_emoji=":sadpanda:",
                proxy=os.environ["HTTPS_PROXY"],
            )

            slack_notification.execute(context)

    envs = [
        {"name": "HTTPS_PROXY", "value": os.environ["HTTPS_PROXY"]},
        {"name": "https_proxy", "value": os.environ["HTTPS_PROXY"]},
    ]

    git_clone_init_container = k8s.V1Container(
        name="clone-repo",
        image="navikt/knada-git-sync:2020-10-23-98963f6",
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", mount_path="/repo", sub_path=None, read_only=False
            ),
            k8s.V1VolumeMount(
                name="git-clone-secret",
                mount_path="/keys",
                sub_path=None,
                read_only=False,
            ),
        ],
        env=envs,
        command=["/bin/sh", "/git-clone.sh"],
        args=[repo, branch, "/repo"],
    )

    return KubernetesPodOperator(
        init_containers=[git_clone_init_container],
        dag=dag,
        on_failure_callback=on_failure,
        name=name,
        namespace=namespace,
        task_id=name,
        is_delete_operator_pod=True,
        image="navikt/knada-airflow-nb:6",
        env_vars={
            "LOG_ENABLED": "true" if log_output else "false",
            "NOTEBOOK_PATH": f"/repo/{Path(nb_path).parent}",
            "NOTEBOOK_NAME": Path(nb_path).name,
            "DATAVERK_API_ENDPOINT": os.environ["DATAVERK_API_ENDPOINT"],
            "DATAVERK_BUCKET_ENDPOINT": os.environ["DATAVERK_BUCKET_ENDPOINT"],
            "HTTPS_PROXY": os.environ["HTTPS_PROXY"],
            "https_proxy": os.environ["HTTPS_PROXY"],
            "NO_PROXY": os.environ["NO_PROXY"],
            "no_proxy": os.environ["NO_PROXY"],
        },
        volume_mounts=[
            VolumeMount(
                name="dags-data", mount_path="/repo", sub_path=None, read_only=True
            )
        ],
        service_account_name="airflow",
        volumes=[
            Volume(name="dags-data", configs={}),
            Volume(
                name="git-clone-secret",
                configs={
                    "secret": {
                        "defaultMode": 448,
                        "secretName": os.environ["K8S_GIT_CLONE_SECRET"],
                    }
                },
            ),
        ],
        annotations={"sidecar.istio.io/inject": "false"},
        resources=resources,
        retries=retries,
        retry_delay=retry_delay,
    )


def create_knada_python_pod_operator(
    dag: DAG,
    name: str,
    repo: str,
    script_path: str,
    namespace: str,
    email: str = None,
    slack_channel: str = None,
    branch: str = "master",
    resources: dict = None,
    retries: int = 3,
    retry_delay: timedelta = timedelta(seconds=5),
):
    """ Factory function for creating KubernetesPodOperator for executing knada jupyter notebooks

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param script_path: str: Path to python script in repo
    :param namespace: str: K8S namespace for pod
    :param email: str: Email of owner
    :param slack_channel: Name of slack channel, default None (no slack notification)
    :param branch: str: Branch in repo, default "master"
    :param resources: dict: Specify required cpu and memory requirements (keys in dict: request_memory, request_cpu, limit_memory, limit_cpu), default None
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :return: KubernetesPodOperator
    """

    def on_failure(context):
        if email:
            send_email = EmailOperator(
                task_id="send-email-on-error",
                to=email,
                subject=f"Airflow task {name} error",
                html_content=f"<p> Airflow task {name} feiler i namespace {namespace} "
                f"kl. {datetime.now().isoformat()}. "
                f"Logger: {os.environ['AIRFLOW__WEBSERVER__BASE_URL']} </p>",
                dag=dag,
            )

            send_email.execute(context)

        if slack_channel:
            slack_notification = SlackWebhookOperator(
                task_id="airflow_task_failed",
                webhook_token=os.environ["SLACK_WEBHOOK_TOKEN"],
                message=f"@here DAG {name} feilet i namespace {namespace} kl. {datetime.now().isoformat()}. "
                f'Logger: {os.environ["AIRFLOW__WEBSERVER__BASE_URL"]}',
                channel=slack_channel,
                link_names=True,
                icon_emoji=":sadpanda:",
                proxy=os.environ["HTTPS_PROXY"],
            )

            slack_notification.execute(context)

    envs = [
        {"name": "HTTPS_PROXY", "value": os.environ["HTTPS_PROXY"]},
        {"name": "https_proxy", "value": os.environ["HTTPS_PROXY"]},
    ]

    git_clone_init_container = k8s.V1Container(
        name="clone-repo",
        image="navikt/knada-git-sync:2020-10-23-98963f6",
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", mount_path="/repo", sub_path=None, read_only=False
            ),
            k8s.V1VolumeMount(
                name="git-clone-secret",
                mount_path="/keys",
                sub_path=None,
                read_only=False,
            ),
        ],
        env=envs,
        command=["/bin/sh", "/git-clone.sh"],
        args=[repo, branch, "/repo"],
    )

    return KubernetesPodOperator(
        init_containers=[git_clone_init_container],
        dag=dag,
        on_failure_callback=on_failure,
        name=name,
        namespace=namespace,
        task_id=name,
        is_delete_operator_pod=True,
        image="navikt/knada-airflow-python:1",
        env_vars={
            "SCRIPT_PATH": f"/repo/{Path(script_path).parent}",
            "SCRIPT_NAME": Path(script_path).name,
            "DATAVERK_API_ENDPOINT": os.environ["DATAVERK_API_ENDPOINT"],
            "DATAVERK_BUCKET_ENDPOINT": os.environ["DATAVERK_BUCKET_ENDPOINT"],
            "HTTPS_PROXY": os.environ["HTTPS_PROXY"],
            "https_proxy": os.environ["HTTPS_PROXY"],
            "NO_PROXY": os.environ["NO_PROXY"],
            "no_proxy": os.environ["NO_PROXY"],
        },
        volume_mounts=[
            VolumeMount(
                name="dags-data", mount_path="/repo", sub_path=None, read_only=False
            )
        ],
        service_account_name="airflow",
        volumes=[
            Volume(name="dags-data", configs={}),
            Volume(
                name="git-clone-secret",
                configs={
                    "secret": {
                        "defaultMode": 448,
                        "secretName": os.environ["K8S_GIT_CLONE_SECRET"],
                    }
                },
            ),
        ],
        annotations={"sidecar.istio.io/inject": "false"},
        resources=resources,
        retries=retries,
        retry_delay=retry_delay,
    )
