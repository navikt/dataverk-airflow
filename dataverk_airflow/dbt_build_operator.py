import os
from datetime import timedelta
from typing import Callable

from airflow import DAG
from kubernetes import client

from dataverk_airflow.kubernetes_operator import kubernetes_operator

def dbt_operator(
        dag: DAG,
        name: str,
        dbt_cmd: str = "dbt build --profiles-dir . --project-dir .",
        repo: str = None,
        image: str = None,
        branch: str = "main",
        email: str = None,
        slack_channel: str = None,
        extra_envs: dict = {},
        allowlist: list = [],
        requirements_path: str = None,
        resources: client.V1ResourceRequirements = None,
        startup_timeout_seconds: int = 360,
        retries: int = 3,
        delete_on_finish: bool = True,
        retry_delay: timedelta = timedelta(seconds=5),
        do_xcom_push: bool = False,
        container_uid: int = 50000,
        on_success_callback: Callable = None,
        use_uv_pip_install: bool = False,
        env_from_secrets: list = [],
):
    """Operator for running dbt build

    The working directory in the container environment will be the root folder of the cloned repo specified
    by the repo input argument. One must therefore correctly configure the --profiles-dir and --project-dir options
    in the dbt command as relative paths from the root folder of the cloned repo.

    If you use the slack_channel argument, the following host will also be added:
    - hooks.slack.com

    If you use the email argument, the following host will also be added:
    - smtp-address

    If you use the requirements_path argument, the following hosts will also be added:
    - pypi.org
    - files.pythonhosted.org
    - pypi.python.org

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param dbt_cmd: str: The dbt command to be executed by the worker.
    :param image: str: Dockerimage the pod should use
    :param branch: str: Branch in repo, default "main"
    :param email: str: Email of owner
    :param slack_channel: str: Name of Slack channel, default None (no Slack notification)
    :param extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
    :param allowlist: list: list of hosts and port the task needs to reach on the format host:port
    :param requirements_path: bool: Path (including filename) to your requirements.txt
    :param resources: dict: Specify cpu and memory resource usage (dict: request/limit: {"memory": "", "cpu": "", "ephemeral-storage": ""}), default ephemeral-storage: 100Mi
    :param startup_timeout_seconds: int: pod startup timeout
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param do_xcom_push: bool: Enable xcom push of content in file '/airflow/xcom/return.json', default False
    :param container_uid: int: User ID for the container image. Root (id = 0) is not allowed, defaults to 50000 (standard uid for airflow).
    :param on_success_callback: a function or list of functions to be called when a task instance
        of this task fails. a context dictionary is passed as a single
        parameter to this function. Context contains references to related
        objects to the task instance and is documented under the macros
        section of the API.
    :param working_dir: str: Path to working directory
    :param use_uv_pip_install: bool: Use uv pip install, default False
    :param env_from_secrets: list: List of kubernetes secrets to mount environment variables from

    :return: KubernetesPodOperator
    """
    if not image:
        image = os.getenv("KNADA_AIRFLOW_OPERATOR_IMAGE")

    kwargs = {
        "dag": dag, "name": name, "repo": repo, "image": image, "cmds": [dbt_cmd], "branch": branch, "email": email,
        "slack_channel": slack_channel, "extra_envs": extra_envs, "allowlist": allowlist, "requirements_path": requirements_path,
        "resources": resources, "startup_timeout_seconds": startup_timeout_seconds, 
        "retries": retries, "delete_on_finish": delete_on_finish, "retry_delay": retry_delay, "do_xcom_push": do_xcom_push,
        "on_success_callback": on_success_callback, "working_dir": ".", "container_uid": container_uid, "use_uv_pip_install": use_uv_pip_install,
        "env_from_secrets": env_from_secrets,
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return kubernetes_operator(**kwargs)