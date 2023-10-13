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
        log_output: bool = False,
        image: str | None = None,
        branch: str = "main",
        email: str | None = None,
        slack_channel: str | None = None,
        extra_envs: dict = {},
        allowlist: list = [],
        requirements_path: str | None = None,
        resources: client.V1ResourceRequirements | None = None,
        startup_timeout_seconds: int | None = None,
        retries: int | None = None,
        delete_on_finish: bool = True,
        retry_delay: timedelta | None = None,
        do_xcom_push: bool = False,
        on_success_callback: Callable | None = None,
):
    """Operator for executing Jupyter notebooks.

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param nb_path: str: Path to notebook in repo
    :param log_output: bool: Write logs from notebook to stdout, default False
    :param image: str: Dockerimage the pod should use
    :param branch: str: Branch in repo, default "main"
    :param email: str: Email of owner
    :param slack_channel: Name of Slack channel, default None (no Slack notification)
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

    cmds = ["papermill", Path(nb_path).name, "output.ipynb"]
    if log_output:
        cmds.append("--log-output")

    kwargs = {
        "dag": dag, "name": name, "repo": repo, "image": image, "cmds": cmds, "branch": branch, "email": email,
        "slack_channel": slack_channel, "extra_envs": extra_envs, "allowlist": allowlist,  "requirements_path": requirements_path,
        "resources": resources, "startup_timeout_seconds": startup_timeout_seconds, "retries": retries,
        "delete_on_finish": delete_on_finish, "retry_delay": retry_delay, "do_xcom_push": do_xcom_push,
        "on_success_callback": on_success_callback, "working_dir": str(Path(nb_path).parent)
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    return kubernetes_operator(**kwargs)
