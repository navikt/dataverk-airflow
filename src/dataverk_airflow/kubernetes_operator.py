import os

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from kubernetes import client
from kubernetes.client.models import (
    V1ConfigMapVolumeSource,
    V1PodSecurityContext,
    V1SeccompProfile,
    V1SecretVolumeSource,
    V1Volume,
    V1VolumeMount,
)

import dataverk_airflow.git_clone as git_clone
from dataverk_airflow.notifications import (
    create_email_notification,
    create_slack_notification,
)

CA_BUNDLE_PATH = "/etc/pki/tls/certs/ca-bundle.crt"
POD_WORKSPACE_DIR = "/workspace"


def kubernetes_operator(dag, repo, branch, name, email, slack_channel, log_output, resources, allowlist, startup_timeout_seconds, retries, retry_delay, on_success_callback, delete_on_finish, image, extra_envs, do_xcom_push, cmds):
    """Simplified operator for creating KubernetesPodOperator.

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
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
        "LOG_ENABLED": "true" if log_output else "false",
        "NLS_LANG": "NORWEGIAN_NORWAY.AL32UTF8",
        "TZ": os.environ["TZ"],
        "REQUESTS_CA_BUNDLE": CA_BUNDLE_PATH,
        "KNADA_TEAM_SECRET": os.environ["KNADA_TEAM_SECRET"]
    }

    namespace = os.getenv("NAMESPACE", "unknown namespace")

    def on_failure(context):
        if email:
            send_email = create_email_notification(dag, email, name, namespace)
            send_email.execute(context)

    if slack_channel:
        allowlist += "hooks.slack.com"
        slack_notification = create_slack_notification(
            dag, slack_channel, name, namespace)
        slack_notification.execute()

        if not image:
            image = os.getenv("KNADA_AIRFLOW_OPERATOR_IMAGE")

    return KubernetesPodOperator(
        dag=dag,
        on_failure_callback=on_failure,
        name=name,
        task_id=name,
        startup_timeout_seconds=startup_timeout_seconds,
        is_delete_operator_pod=delete_on_finish,
        image=image,
        env_vars=env_vars,
        do_xcom_push=do_xcom_push,
        container_resources=resources,
        retries=retries,
        on_success_callback=on_success_callback,
        retry_delay=retry_delay,
        executor_config={
            "pod_override": client.V1Pod(
                metadata=client.V1ObjectMeta(
                    annotations={"allowlist": allowlist})
            )
        },
        init_containers=[
            git_clone.container(repo, branch, POD_WORKSPACE_DIR)
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
