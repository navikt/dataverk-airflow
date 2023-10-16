import os
from datetime import timedelta
from typing import Callable

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
from kubernetes import client
from kubernetes.client.models import (
    V1ConfigMapVolumeSource,
    V1PodSecurityContext,
    V1SeccompProfile,
    V1SecretVolumeSource,
    V1Volume,
    V1VolumeMount,
)

from dataverk_airflow import git_clone
from dataverk_airflow.notifications import (
    create_email_notification,
    create_slack_notification,
)

CA_BUNDLE_PATH = "/etc/pki/tls/certs/ca-bundle.crt"
POD_WORKSPACE_DIR = "/workspace"


class MissingValueException(Exception):
    """Raised when a required value is missing."""


def kubernetes_operator(
        dag: DAG,
        name: str,
        repo: str,
        image: str,
        cmds: list | None = None,
        branch: str = "main",
        email: str | None = None,
        slack_channel: str | None = None,
        extra_envs: dict = {},
        allowlist: list = [],
        requirements_path: str | None = None,
        resources: client.V1ResourceRequirements | None = None,
        startup_timeout_seconds: int = 360,
        retries: int = 3,
        delete_on_finish: bool = True,
        retry_delay: timedelta = timedelta(seconds=5),
        do_xcom_push: bool = False,
        on_success_callback: Callable | None = None,
        working_dir: str | None = None,
):
    """Simplified operator for creating KubernetesPodOperator.

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param image: str: Dockerimage the pod should use
    :param cmds: str: Command to run in pod
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
    :param working_dir: str: Path to working directory

    :return: KubernetesPodOperator
    """
    if repo == "":
        raise MissingValueException("repo cannot be empty")

    if image == "":
        raise MissingValueException("image cannot be empty")

    env_vars = {
        "NLS_LANG": "NORWEGIAN_NORWAY.AL32UTF8",
        "TZ": os.getenv("TZ", "Europe/Oslo"),
        "REQUESTS_CA_BUNDLE": CA_BUNDLE_PATH,
        "KNADA_TEAM_SECRET": os.environ["KNADA_TEAM_SECRET"],
        **extra_envs
    }

    namespace = os.environ["NAMESPACE"]

    if slack_channel:
        allowlist.append("slack.com")

    def on_failure(context):
        if email:
            send_email = create_email_notification(dag, email, name, namespace)
            send_email.execute(context)

        if slack_channel:
            slack_notification = create_slack_notification(
                dag, slack_channel, name, namespace)
            slack_notification.execute()

    on_finish_action = OnFinishAction.DELETE_POD if delete_on_finish else OnFinishAction.KEEP_POD

    if working_dir:
        working_dir = POD_WORKSPACE_DIR + "/" + working_dir
    else:
        working_dir = POD_WORKSPACE_DIR

    if requirements_path:
        cmds = ["pip", "install", "-r", f"{POD_WORKSPACE_DIR}/{requirements_path}", "--user", "&&"] + cmds

    return KubernetesPodOperator(
        dag=dag,
        on_failure_callback=on_failure,
        name=name,
        task_id=name,
        startup_timeout_seconds=startup_timeout_seconds,
        on_finish_action=on_finish_action,
        annotations={"allowlist": allowlist},
        image=image,
        env_vars=env_vars,
        do_xcom_push=do_xcom_push,
        container_resources=resources,
        retries=retries,
        on_success_callback=on_success_callback,
        retry_delay=retry_delay,
        full_pod_spec=client.V1Pod(
            spec=client.V1PodSpec(
                containers=[
                    client.V1Container(
                        name="base",
                        working_dir=working_dir,
                    )
                ]
            )
        ),
        init_containers=[
            git_clone(repo, branch, POD_WORKSPACE_DIR)
        ],
        image_pull_secrets=os.environ["K8S_IMAGE_PULL_SECRETS"],
        labels={
            "component": "worker",
            "release": "airflow"
        },
        cmds=cmds,
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
                    secret_name=os.getenv(
                        "K8S_GIT_CLONE_SECRET", "github-app-secret"),
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
        security_context=V1PodSecurityContext(
            fs_group=0,
            seccomp_profile=V1SeccompProfile(
                type="RuntimeDefault"
            )
        ),
    )
