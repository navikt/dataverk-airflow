import os

from datetime import timedelta
from typing import Callable
from deprecated import deprecated

from airflow import DAG
from kubernetes import client
from dataverk_airflow import notebook_operator, python_operator


@deprecated("Will be removed in next release, use notebook_operator instead")
def create_knada_nb_pod_operator(
    dag: DAG,
    name: str,
    repo: str,
    nb_path: str,
    namespace: str = None,
    email: str = None,
    slack_channel: str = None,
    branch: str = "master",
    log_output: bool = False,
    resources: client.V1ResourceRequirements = None,
    retries: int = 3,
    extra_envs: dict = None,
    delete_on_finish: bool = True,
    image: str = None,
    startup_timeout_seconds: int = 360,
    retry_delay: timedelta = timedelta(seconds=5),
    do_xcom_push: bool = False,
    on_success_callback: Callable = None,
    nls_lang: str = "NORWEGIAN_NORWAY.AL32UTF8",
    allowlist: list = [],
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
    :param extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param image: str: Dockerimage the pod should use
    :param startup_timeout_seconds: int: pod startup timeout
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param do_xcom_push: bool: Whether to push xcom pod output, default False
    :param nls_lang: str: Configure locale and character sets with NLS_LANG environment variable in k8s pod, defaults to Norwegian
    :param on_success_callback: Callable
    :param allowlist: list: list of hosts and port the task needs to reach on the format host:port
    :return: KubernetesPodOperator
    """

    if not image:
        image = os.getenv("KNADA_PYTHON_POD_OP_IMAGE",
                          "europe-west1-docker.pkg.dev/knada-gcp/knada/airflow:2023-03-08-d3684b7")

    kwargs = {
        "dag": dag, "name": name, "repo": repo, "image": image, "branch": branch, "email": email,
        "slack_channel": slack_channel, "extra_envs": extra_envs, "allowlist": allowlist,
        "resources": resources, "startup_timeout_seconds": startup_timeout_seconds, "retries": retries,
        "delete_on_finish": delete_on_finish, "retry_delay": retry_delay, "do_xcom_push": do_xcom_push,
        "on_success_callback": on_success_callback, "nb_path": nb_path
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    return notebook_operator(**kwargs)


@deprecated("Will be removed in next release, use python_operator instead")
def create_knada_python_pod_operator(
    dag: DAG,
    name: str,
    repo: str,
    script_path: str,
    namespace: str = None,
    email: str = None,
    slack_channel: str = None,
    branch: str = "master",
    resources: client.V1ResourceRequirements = None,
    retries: int = 3,
    extra_envs: dict = None,
    delete_on_finish: bool = True,
    image: str = None,
    startup_timeout_seconds: int = 360,
    retry_delay: timedelta = timedelta(seconds=5),
    nls_lang: str = "NORWEGIAN_NORWAY.AL32UTF8",
    do_xcom_push: bool = False,
    allowlist: list = [],


):
    """ Factory function for creating KubernetesPodOperator for executing knada python scripts

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
    :param extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param image: str: Dockerimage the pod should use
    :param startup_timeout_seconds: int: pod startup timeout
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param nls_lang: str: Configure locale and character sets with NLS_LANG environment variable in k8s pod, defaults to Norwegian
    :param do_xcom_push: bool: Enable xcom push of content in file '/airflow/xcom/return.json'
    :param allowlist: list: list of hosts and port the task needs to reach on the format host:port
    :return: KubernetesPodOperator
    """

    if not image:
        image = os.getenv("KNADA_PYTHON_POD_OP_IMAGE",
                          "europe-west1-docker.pkg.dev/knada-gcp/knada/airflow:2023-03-08-d3684b7")

    kwargs = {
        "dag": dag, "name": name, "repo": repo, "image": image, "branch": branch, "email": email,
        "slack_channel": slack_channel, "extra_envs": extra_envs, "allowlist": allowlist, "resources": resources, "startup_timeout_seconds": startup_timeout_seconds, "retries": retries,
        "delete_on_finish": delete_on_finish, "retry_delay": retry_delay, "do_xcom_push": do_xcom_push,
        "script_path": script_path
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}

    return python_operator(**kwargs)
