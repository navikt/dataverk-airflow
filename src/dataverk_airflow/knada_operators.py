import os

from datetime import timedelta
from pathlib import Path
from airflow import DAG
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from dataverk_airflow.init_containers import create_git_clone_init_container, change_permissions_init_container, \
    dbt_read_gcs_bucket
from dataverk_airflow.notifications import create_email_notification, create_slack_notification


def create_knada_nb_pod_operator(
    dag: DAG,
    name: str,
    repo: str,
    nb_path: str,
    namespace: str,
    email: str = None,
    slack_channel: str = None,
    branch: str = "master",
    log_output: bool = False,
    resources: dict = None,
    retries: int = 3,
    delete_on_finish: bool = True,
    startup_timeout_seconds: int = 120,
    retry_delay: timedelta = timedelta(seconds=5),
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
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param startup_timeout_seconds: int: pod startup timeout
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :return: KubernetesPodOperator
    """

    def on_failure(context):
        if email:
            send_email = create_email_notification(email, name, namespace, dag)
            send_email.execute(context)

        if slack_channel:
            slack_notification = create_slack_notification(slack_channel, name, namespace)
            slack_notification.execute(context)

    return KubernetesPodOperator(
        init_containers=[create_git_clone_init_container(repo, branch, "/repo"),
                         change_permissions_init_container("/repo")],
        dag=dag,
        on_failure_callback=on_failure,
        name=name,
        namespace=namespace,
        task_id=name,
        startup_timeout_seconds=startup_timeout_seconds,
        is_delete_operator_pod=delete_on_finish,
        image=os.getenv("KNADA_NOTEBOOK_OP_IMAGE", "navikt/knada-airflow-nb:6"),
        env_vars={
            "LOG_ENABLED": "true" if log_output else "false",
            "NOTEBOOK_PATH": f"/repo/{Path(nb_path).parent}",
            "NOTEBOOK_NAME": Path(nb_path).name,
            "DATAVERK_API_ENDPOINT": os.environ["DATAVERK_API_ENDPOINT"],
            "DATAVERK_BUCKET_ENDPOINT": os.environ["DATAVERK_BUCKET_ENDPOINT"],
            "HTTPS_PROXY": os.environ["HTTPS_PROXY"],
            "https_proxy": os.environ["HTTPS_PROXY"],
            "NO_PROXY": os.environ["NO_PROXY"],
            "no_proxy": os.environ["NO_PROXY"],
        },
        volume_mounts=[
            VolumeMount(
                name="dags-data", mount_path="/repo", sub_path=None, read_only=False
            )
        ],
        service_account_name="airflow",
        volumes=[
            Volume(name="dags-data", configs={}),
            Volume(
                name="git-clone-secret",
                configs={
                    "secret": {
                        "defaultMode": 448,
                        "secretName": os.environ["K8S_GIT_CLONE_SECRET"],
                    }
                },
            ),
        ],
        annotations={"sidecar.istio.io/inject": "false"},
        resources=resources,
        retries=retries,
        retry_delay=retry_delay,
    )


def create_knada_python_pod_operator(
    dag: DAG,
    name: str,
    repo: str,
    script_path: str,
    namespace: str,
    email: str = None,
    slack_channel: str = None,
    branch: str = "master",
    resources: dict = None,
    retries: int = 3,
    delete_on_finish: bool = True,
    startup_timeout_seconds: int = 120,
    retry_delay: timedelta = timedelta(seconds=5),
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
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param startup_timeout_seconds: int: pod startup timeout
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :return: KubernetesPodOperator
    """

    def on_failure(context):
        if email:
            send_email = create_email_notification(email, name, namespace, dag)
            send_email.execute(context)

        if slack_channel:
            slack_notification = create_slack_notification(slack_channel, name, namespace)
            slack_notification.execute(context)

    return KubernetesPodOperator(
        init_containers=[create_git_clone_init_container(repo, branch, "/repo"),
                         change_permissions_init_container("/repo")],
        dag=dag,
        on_failure_callback=on_failure,
        startup_timeout_seconds=startup_timeout_seconds,
        name=name,
        namespace=namespace,
        task_id=name,
        is_delete_operator_pod=delete_on_finish,
        image=os.getenv("KNADA_PYTHON_POD_OP_IMAGE", "navikt/knada-airflow-python:1"),
        env_vars={
            "SCRIPT_PATH": f"/repo/{Path(script_path).parent}",
            "SCRIPT_NAME": Path(script_path).name,
            "DATAVERK_API_ENDPOINT": os.environ["DATAVERK_API_ENDPOINT"],
            "DATAVERK_BUCKET_ENDPOINT": os.environ["DATAVERK_BUCKET_ENDPOINT"],
            "HTTPS_PROXY": os.environ["HTTPS_PROXY"],
            "https_proxy": os.environ["HTTPS_PROXY"],
            "NO_PROXY": os.environ["NO_PROXY"],
            "no_proxy": os.environ["NO_PROXY"],
        },
        volume_mounts=[
            VolumeMount(
                name="dags-data", mount_path="/repo", sub_path=None, read_only=False
            )
        ],
        service_account_name="airflow",
        volumes=[
            Volume(name="dags-data", configs={}),
            Volume(
                name="git-clone-secret",
                configs={
                    "secret": {
                        "defaultMode": 448,
                        "secretName": os.environ["K8S_GIT_CLONE_SECRET"],
                    }
                },
            ),
        ],
        annotations={"sidecar.istio.io/inject": "false"},
        resources=resources,
        retries=retries,
        retry_delay=retry_delay,
    )


def create_knada_dbt_seed_operator(
    dag: DAG,
    name: str,
    repo: str,
    namespace: str,
    dbt_dir: str,
    seed_source: dict = None,
    email: str = None,
    slack_channel: str = None,
    branch: str = "master",
    resources: dict = None,
    retries: int = 3,
    delete_on_finish: bool = True,
    startup_timeout_seconds: int = 120,
    retry_delay: timedelta = timedelta(seconds=5),
):
    """ Factory function for creating KubernetesPodOperator for executing knada dbt seed

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param dbt_dir: str: Path to dbt directory
    :param seed_source: dict: Dictionary with keys for bucket_name (name of gcs bucket name) and blob_name (name of blob in bucket)
    :param namespace: str: K8S namespace for pod
    :param email: str: Email of owner
    :param slack_channel: Name of slack channel, default None (no slack notification)
    :param branch: str: Branch in repo, default "master"
    :param resources: dict: Specify required cpu and memory requirements (keys in dict: request_memory, request_cpu, limit_memory, limit_cpu), default None
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param startup_timeout_seconds: int: pod startup timeout
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :return: KubernetesPodOperator
    """

    def on_failure(context):
        if email:
            send_email = create_email_notification(email, name, namespace, dag)
            send_email.execute(context)

        if slack_channel:
            slack_notification = create_slack_notification(slack_channel, name, namespace)
            slack_notification.execute(context)

    return KubernetesPodOperator(
        init_containers=[create_git_clone_init_container(repo, branch, "/repo"),
                         dbt_read_gcs_bucket("/repo", seed_source, dbt_dir),
                         change_permissions_init_container("/repo")],
        dag=dag,
        on_failure_callback=on_failure,
        startup_timeout_seconds=startup_timeout_seconds,
        name=name,
        namespace=namespace,
        task_id=name,
        is_delete_operator_pod=delete_on_finish,
        image=os.getenv("KNADA_DBT_IMAGE", "navikt/knada-dbt:1"),
        env_vars={
            "GOOGLE_APPLICATION_CREDENTIALS": "/home/jovyan/creds.json",
            "HTTPS_PROXY": os.environ["HTTPS_PROXY"],
            "https_proxy": os.environ["HTTPS_PROXY"],
            "NO_PROXY": os.environ["NO_PROXY"],
            "no_proxy": os.environ["NO_PROXY"],
        },
        cmds=["dbt", "seed"],
        arguments=["--profiles-dir", f"/repo/{dbt_dir}", "--project-dir", f"/repo/{dbt_dir}"],
        volume_mounts=[
            VolumeMount(
                name="dags-data", mount_path="/repo", sub_path=None, read_only=False
            ),
            VolumeMount(
                name="google-creds",
                mount_path="/home/jovyan",
                sub_path=None,
                read_only=False,
            ),
            VolumeMount(
                name="ca-bundle-pem",
                mount_path="/etc/pki/tls/certs/ca-bundle.crt",
                read_only=True,
                sub_path="ca-bundle.pem"
            ),
        ],
        service_account_name="airflow",
        volumes=[
            Volume(name="dags-data", configs={}),
            Volume(name="ca-bundle-pem",
                   configs={
                        "configMap": {
                            "defaultMode": 420,
                            "name": "ca-bundle-pem"
                        }
                   }),
            Volume(
                name="git-clone-secret",
                configs={
                    "secret": {
                        "defaultMode": 448,
                        "secretName": os.environ["K8S_GIT_CLONE_SECRET"],
                    }
                },
            ),
            Volume(
                name="google-creds",
                configs={
                    "secret": {
                        "defaultMode": 448,
                        "secretName": os.getenv("K8S_GOOGLE_CREDS", "google-creds"),
                    }
                },
            ),
        ],
        annotations={"sidecar.istio.io/inject": "false"},
        resources=resources,
        retries=retries,
        retry_delay=retry_delay,
    )

