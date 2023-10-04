import os
from datetime import timedelta
from typing import Callable

from airflow import DAG
from kubernetes import client

from dataverk_airflow import kubernetes_operator


def python_operator(
    dag: DAG,
    name: str,
    repo: str,
    script_path: str,
    email: str = None,
    slack_channel: str = None,
    branch: str = "main",
    resources: client.V1ResourceRequirements = None,
    allowlist: list = [],
    log_output: bool = False,
    retries: int = 3,
    extra_envs: dict = None,
    delete_on_finish: bool = True,
    image: str = None,
    startup_timeout_seconds: int = 360,
    retry_delay: timedelta = timedelta(seconds=5),
    do_xcom_push: bool = False,
    nls_lang: str = "NORWEGIAN_NORWAY.AL32UTF8",
    on_success_callback: Callable = None,
):
    """ Factory function for creating KubernetesPodOperator for executing Python scripts

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param script_path: str: Path to python script in repo
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
    :param nls_lang: str: Configure locale and character sets with NLS_LANG environment variable in k8s pod, defaults to Norwegian
    :param on_success_callback: Callable

    :return: KubernetesPodOperator
    """

    if not image:
        image = os.getenv("KNADA_AIRFLOW_PYTHON_IMAGE")

    cmds = ["/bin/bash", "/execute_python.sh"]
  
    return kubernetes_operator(repo, branch, dag, name, email, slack_channel,
                               resources, allowlist, startup_timeout_seconds, retries,
                               retry_delay, on_success_callback, delete_on_finish,
                               image, extra_envs, do_xcom_push, cmds)
