import os
from datetime import timedelta
from typing import Callable, List

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import OnFinishAction
from kubernetes import client
from kubernetes.client.models import (
    V1ConfigMapVolumeSource,
    V1Container,
    V1PodSecurityContext,
    V1SeccompProfile,
    V1SecretVolumeSource,
    V1Volume,
    V1VolumeMount,
)

from dataverk_airflow import git_clone, bucket_read
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
        image: str,
        repo: str = None,
        cmds: list = None,
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
        on_success_callback: Callable = None,
        working_dir: str = None,
):
    """Simplified operator for creating KubernetesPodOperator.

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
    :param image: str: Dockerimage the pod should use
    :param cmds: str: Command to run in pod
    :param branch: str: Branch in repo, default "main"
    :param email: str: Email of owner
    :param slack_channel: str: Name of Slack channel, default None (no Slack notification)
    :param extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
    :param allowlist: list: list of hosts and port the task needs to reach on the format host:port
    :param requirements_path: bool: Path (including filename) to your requirements.txt
    :param resources: dict: Specify cpu and memory resource usage (dict: request/limit: {"memory": "", "cpu": "", "ephemeral-storage": ""}), default None
    :param startup_timeout_seconds: int: pod startup timeout
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param do_xcom_push: bool: Enable xcom push of content in file '/airflow/xcom/return.json', default False
    :param on_success_callback: a function or list of functions to be called when a task instance
        of this task fails. a context dictionary is passed as a single
        parameter to this function. Context contains references to related
        objects to the task instance and is documented under the macros
        section of the API.
    :param working_dir: str: Path to working directory

    :return: KubernetesPodOperator
    """
    is_composer = True if os.getenv("GCS_BUCKET") else False

    if not is_composer and repo in [None, ""]:
        raise MissingValueException("repo cannot be empty")

    if image == "":
        raise MissingValueException("image cannot be empty")

    if slack_channel:
        allowlist.append("hooks.slack.com")

    if email:
        allowlist.append(os.getenv("AIRFLOW__SMTP__SMTP_HOST") +
                         ":"+os.getenv("AIRFLOW__SMTP__SMTP_PORT"))

    namespace = get_namespace(is_composer)

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
        cmds = [
            f"pip install -r {POD_WORKSPACE_DIR}/{requirements_path} --user --no-cache-dir"] + cmds

        allowlist.append("pypi.org")
        allowlist.append("files.pythonhosted.org")
        allowlist.append("pypi.python.org")

    return KubernetesPodOperator(
        dag=dag,
        on_failure_callback=on_failure,
        name=name,
        task_id=name,
        startup_timeout_seconds=startup_timeout_seconds,
        on_finish_action=on_finish_action,
        annotations={"allowlist": ",".join(allowlist)},
        image=image,
        env_vars=env_vars(is_composer, extra_envs),
        config_file=config_file(is_composer),
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
        init_containers=init_containers(is_composer, repo, branch),
        image_pull_secrets=os.getenv("K8S_IMAGE_PULL_SECRETS"),
        labels={
            "component": "worker",
            "release": "airflow"
        },
        cmds=["/bin/sh", "-c"],
        arguments=[" && ".join(cmds)] if cmds is not None else None,
        volume_mounts=volume_mounts(is_composer),
        service_account_name=os.getenv("TEAM", "default"),
        volumes=volumes(is_composer),
        security_context=V1PodSecurityContext(
            fs_group=0,
            seccomp_profile=V1SeccompProfile(
                type="RuntimeDefault"
            )
        ),
    )


def get_namespace(is_composer: bool) -> str:
    if is_composer:
        return "composer-user-workloads"
    else:
        return os.getenv("NAMESPACE")


def env_vars(is_composer: bool, extra_envs: dict) -> dict:
    env_vars = {
        "NLS_LANG": "NORWEGIAN_NORWAY.AL32UTF8",
        "TZ": os.getenv("TZ", "Europe/Oslo"),
        **extra_envs
    }

    if not is_composer:
        if not os.getenv("INTEGRATION_TEST"):
            env_vars["REQUESTS_CA_BUNDLE"] = CA_BUNDLE_PATH

        env_vars["KNADA_TEAM_SECRET"] = os.environ["KNADA_TEAM_SECRET"]

    return env_vars


def config_file(is_composer: bool) -> str:
    if not os.getenv("INTEGRATION_TEST"):
        return "/home/airflow/composer_kube_config" if is_composer else None


def init_containers(is_composer: bool, repo: str, branch: str) -> List[V1Container]:
    if is_composer:
        return [
            bucket_read(POD_WORKSPACE_DIR)
        ]
    else:
        return [
            git_clone(repo, branch, POD_WORKSPACE_DIR)
        ]


def volume_mounts(is_composer: bool) -> List[V1VolumeMount]:
    volume_mounts = [
        V1VolumeMount(
            name="dags-data",
            mount_path=POD_WORKSPACE_DIR,
            sub_path=None,
            read_only=False
        )
    ]

    if not is_composer:
        volume_mounts.append(
            V1VolumeMount(
                name="ca-bundle-pem",
                mount_path=CA_BUNDLE_PATH,
                read_only=True,
                sub_path="ca-bundle.pem"
            )
        )

    return volume_mounts


def volumes(is_composer: bool) -> List[V1Volume]:
    volumes = [
        V1Volume(
            name="dags-data"
        )
    ]

    if not is_composer:
        volumes.append(
            V1Volume(
                name="airflow-git-secret",
                secret=V1SecretVolumeSource(
                    default_mode=448,
                    secret_name=os.getenv(
                        "K8S_GIT_CLONE_SECRET", "github-app-secret"),
                )
            )
        )
        volumes.append(
            V1Volume(
                name="ca-bundle-pem",
                config_map=V1ConfigMapVolumeSource(
                    default_mode=420,
                    name="ca-bundle-pem",
                )
            )
        )

    return volumes
