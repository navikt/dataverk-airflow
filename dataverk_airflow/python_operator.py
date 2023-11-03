import os
from datetime import timedelta
from pathlib import Path
from typing import Callable

from airflow import DAG
from kubernetes import client

from dataverk_airflow.kubernetes_operator import kubernetes_operator


def python_operator(
        dag: DAG,
        name: str,
        script_path: str,
        repo: str = None,
        image: str = None,
        branch: str = "main",
        email: str = None,
        slack_channel: str = None,
        extra_envs: dict = {},
        allowlist: list = [],
        requirements_path: str = None,
        resources: client.V1ResourceRequirements = None,
        startup_timeout_seconds: int = None,
        retries: int = None,
        delete_on_finish: bool = True,
        retry_delay: timedelta = None,
        do_xcom_push: bool = False,
        on_success_callback: Callable = None,
):
    """Operator for executing Python scripts.

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param script_path: str: Path to script in repo
    :param image: str: Dockerimage the pod should use
    :param branch: str: Branch in repo, default "main"
    :param email: str: Email of owner
    :param slack_channel: str: Name of Slack channel, default None (no Slack notification)
    :param extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
    :param allowlist: list: list of hosts and port the task needs to reach on the format host:port
    :param requirements_path: bool: Path (including filename) to your requirements.txt
    :param resources: dict: Specify required cpu and memory requirements (keys in dict: request_memory, request_cpu, limit_memory, limit_cpu), default None
    :param startup_timeout_seconds: int: pod startup timeout
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param do_xcom_push: bool: Enable xcom push of content in file '/airflow/xcom/return.json', default False
    :param on_success_callback: Callable

    :return: KubernetesPodOperator
    """
    if not image:
        image = os.getenv("KNADA_AIRFLOW_OPERATOR_IMAGE")

    cmds = [f"python {Path(script_path).name}"]

    kwargs = {
        "dag": dag, "name": name, "repo": repo, "image": image, "cmds": cmds, "branch": branch, "email": email,
        "slack_channel": slack_channel, "extra_envs": extra_envs, "allowlist": allowlist, "requirements_path": requirements_path,
        "resources": resources, "startup_timeout_seconds": startup_timeout_seconds, "retries": retries,
        "delete_on_finish": delete_on_finish, "retry_delay": retry_delay, "do_xcom_push": do_xcom_push,
        "on_success_callback": on_success_callback, "working_dir": str(Path(script_path).parent)
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    return kubernetes_operator(**kwargs)
