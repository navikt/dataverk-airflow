import os

from datetime import timedelta
from pathlib import Path
from typing import Callable
from deprecated import deprecated

from airflow import DAG
from kubernetes.client.models import V1Volume, V1SecretVolumeSource, V1ConfigMapVolumeSource, V1VolumeMount, V1PodSecurityContext, V1SeccompProfile
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes import client

from dataverk_airflow.init_containers import create_git_clone_init_container
from dataverk_airflow.notifications import create_email_notification, create_slack_notification


POD_WORKSPACE_DIR = "/workspace"
CA_BUNDLE_PATH = "/etc/pki/tls/certs/ca-bundle.crt"


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

    env_vars = {
        "LOG_ENABLED": "true" if log_output else "false",
        "NOTEBOOK_PATH": f"{POD_WORKSPACE_DIR}/{Path(nb_path).parent}",
        "NOTEBOOK_NAME": Path(nb_path).name,
        "TZ": os.environ["TZ"],
        "REQUESTS_CA_BUNDLE": CA_BUNDLE_PATH,
        "NLS_LANG": nls_lang,
        "KNADA_TEAM_SECRET": os.environ["KNADA_TEAM_SECRET"]
    }

    allowlist_str = "hooks.slack.com"

    if len(allowlist):
        allowlist_str += "," + ",".join(allowlist)

    namespace = namespace if namespace else os.getenv("NAMESPACE")

    if extra_envs:
        env_vars = dict(env_vars, **extra_envs)

    def on_failure(context):
        if email:
            send_email = create_email_notification(
                dag, email, name, namespace, dag)
            send_email.execute(context)

        if slack_channel:
            slack_notification = create_slack_notification(
                dag, slack_channel, name, namespace)
            slack_notification.execute()

    if not image:
        image = os.getenv("KNADA_PYTHON_POD_OP_IMAGE", "europe-west1-docker.pkg.dev/knada-gcp/knada/airflow:2023-03-08-d3684b7")

    return KubernetesPodOperator(
        init_containers=[create_git_clone_init_container(
            repo, branch, POD_WORKSPACE_DIR)],
        image_pull_secrets=os.environ["K8S_IMAGE_PULL_SECRETS"],
        dag=dag,
        labels={"component": "worker", "release": "airflow"},
        on_failure_callback=on_failure,
        name=name,
        cmds=["/bin/bash", "/execute_notebook.sh"],
        namespace=namespace,
        task_id=name,
        startup_timeout_seconds=startup_timeout_seconds,
        is_delete_operator_pod=delete_on_finish,
        image=image,
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": allowlist_str})
            )
        },
        env_vars=env_vars,
        do_xcom_push=do_xcom_push,
        volume_mounts=[
            V1VolumeMount(
                name="dags-data",
                mount_path=POD_WORKSPACE_DIR,
                sub_path=None,
                read_only=False
            ),
            V1VolumeMount(
                name="ca-bundle-pem",
                mount_path=CA_BUNDLE_PATH,
                read_only=True,
                sub_path="ca-bundle.pem"
            )
        ],
        service_account_name=os.getenv("TEAM", "airflow"),
        volumes=[
            V1Volume(
                name="dags-data"
            ),
            V1Volume(
                name="airflow-git-secret",
                secret=V1SecretVolumeSource(
                    default_mode=448,
                    secret_name=os.getenv("K8S_GIT_CLONE_SECRET", "github-app-secret"),
                )
            ),
            V1Volume(
                name="ca-bundle-pem",
                config_map=V1ConfigMapVolumeSource(
                    default_mode=420,
                    name="ca-bundle-pem",
                )
            ),
        ],
        container_resources=resources,
        retries=retries,
        on_success_callback=on_success_callback,
        retry_delay=retry_delay,
        security_context=V1PodSecurityContext(
            fs_group=0,
            seccomp_profile=V1SeccompProfile(
                type="RuntimeDefault"
            )
        ),
    )

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

    env_vars = {
        "NOTEBOOK_PATH": f"{POD_WORKSPACE_DIR}/{Path(script_path).parent}",
        "NOTEBOOK_NAME": Path(script_path).name,
        "TZ": os.environ["TZ"],
        "REQUESTS_CA_BUNDLE": CA_BUNDLE_PATH,
        "NLS_LANG": nls_lang,
        "KNADA_TEAM_SECRET": os.environ["KNADA_TEAM_SECRET"]
    }

    allowlist_str = "hooks.slack.com"

    if len(allowlist):
        allowlist_str += "," + ",".join(allowlist)

    namespace = namespace if namespace else os.getenv("NAMESPACE")

    if extra_envs:
        env_vars = dict(env_vars, **extra_envs)

    def on_failure(context):
        if email:
            send_email = create_email_notification(
                dag, email, name, namespace, dag)
            send_email.execute(context)

        if slack_channel:
            slack_notification = create_slack_notification(
                dag, slack_channel, name, namespace)
            slack_notification.execute()
            
    if not image:
        image = os.getenv("KNADA_PYTHON_POD_OP_IMAGE", "europe-west1-docker.pkg.dev/knada-gcp/knada/airflow:2023-03-08-d3684b7")

    return KubernetesPodOperator(
        init_containers=[create_git_clone_init_container(
            repo, branch, POD_WORKSPACE_DIR)],
        image_pull_secrets=os.environ["K8S_IMAGE_PULL_SECRETS"],
        dag=dag,
        labels={"component": "worker", "release": "airflow"},
        on_failure_callback=on_failure,
        startup_timeout_seconds=startup_timeout_seconds,
        name=name,
        cmds=["/bin/bash", "/execute_python.sh"],
        namespace=namespace,
        task_id=name,
        is_delete_operator_pod=delete_on_finish,
        image=image,
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(annotations={"allowlist": allowlist_str})
            )
        },
        env_vars=env_vars,
        volume_mounts=[
            V1VolumeMount(
                name="dags-data",
                mount_path=POD_WORKSPACE_DIR,
                sub_path=None,
                read_only=False
            ),
            V1VolumeMount(
                name="ca-bundle-pem",
                mount_path=CA_BUNDLE_PATH,
                read_only=True,
                sub_path="ca-bundle.pem"
            )
        ],
        service_account_name=os.getenv("TEAM", "airflow"),
        volumes=[
            V1Volume(
                name="dags-data"
            ),
            V1Volume(
                name="airflow-git-secret",
                secret=V1SecretVolumeSource(
                    default_mode=448,
                    secret_name=os.getenv("K8S_GIT_CLONE_SECRET", "github-app-secret"),
                )
            ),
            V1Volume(
                name="ca-bundle-pem",
                config_map=V1ConfigMapVolumeSource(
                    default_mode=420,
                    name="ca-bundle-pem",
                )
            ),
        ],
        container_resources=resources,
        retries=retries,
        retry_delay=retry_delay,
        do_xcom_push=do_xcom_push,
        security_context=V1PodSecurityContext(
            fs_group=0,
            seccomp_profile=V1SeccompProfile(
                type="RuntimeDefault"
            )
        ),
    )
