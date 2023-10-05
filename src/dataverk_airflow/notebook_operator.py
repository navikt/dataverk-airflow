import os
from datetime import timedelta
from pathlib import Path
from typing import Callable

from airflow import DAG
from kubernetes import client

from dataverk_airflow.kubernetes_operator import kubernetes_operator


def notebook_operator(
    dag: DAG,
    name: str,
    repo: str,
    nb_path: str,
    email: str | None = None,
    slack_channel: str | None = None,
    branch: str = "main",
    resources: client.V1ResourceRequirements | None = None,
    allowlist: list = [],
    log_output: bool = False,
    retries: int = 3,
    extra_envs: dict | None = None,
    delete_on_finish: bool = True,
    image: str | None = None,
    startup_timeout_seconds: int = 360,
    retry_delay: timedelta = timedelta(seconds=5),
    do_xcom_push: bool = False,
    on_success_callback: Callable | None = None,
):
    """ Factory function for creating KubernetesPodOperator for executing Jupyter notebooks

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param nb_path: str: Path to notebook in repo
    :param email: str: Email of owner
    :param slack_channel: Name of Slack channel, default None (no Slack notification)
    :param branch: str: Branch in repo, default "main"
    :param resources: dict: Specify required cpu and memory requirements (keys in dict: request_memory, request_cpu, limit_memory, limit_cpu), default None
    :param allowlist: list: list of hosts and port the task needs to reach on the format host:port
    :param log_output: bool: Write logs from notebook to stdout, default False
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param image: str: Dockerimage the pod should use
    :param startup_timeout_seconds: int: pod startup timeout
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param do_xcom_push: bool: Enable xcom push of content in file '/airflow/xcom/return.json', default False
    :param on_success_callback: Callable

    :return: KubernetesPodOperator
    """

    env_vars = {
        "NOTEBOOK_PATH": f"/workspace/{Path(nb_path).parent}",
        "NOTEBOOK_NAME": Path(nb_path).name,
    }

    if extra_envs:
        env_vars = dict(env_vars, **extra_envs)

    if not image:
        image = os.getenv("KNADA_AIRFLOW_NOTEBOOK_IMAGE")

    cmds = ["/bin/bash", "/execute_notebook.sh"]

    return kubernetes_operator(repo, branch, dag, name, email, slack_channel,
                               resources, allowlist, startup_timeout_seconds, retries,
                               retry_delay, on_success_callback, delete_on_finish,
                               image, env_vars, do_xcom_push, cmds)
