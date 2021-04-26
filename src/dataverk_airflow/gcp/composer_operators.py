import os

from enum import Enum
from airflow.kubernetes.volume import Volume
from airflow.kubernetes.volume_mount import VolumeMount
from airflow.contrib.operators import kubernetes_pod_operator
from dataverk_airflow.gcp.init_containers import create_read_bucket_init_container


class DBTCommand(Enum):
    DEBUG = "debug"
    TEST = "test"
    RUN = "run"


def create_dbt_pod_operator(
    task_id: str,
    bucket_dbt_path: str,
    dbt_cmd: DBTCommand = DBTCommand.DEBUG,
    namespace: str = "default",
):
    read_bucket_init_container = create_read_bucket_init_container(bucket_dbt_path)
    return kubernetes_pod_operator.KubernetesPodOperator(
        init_containers=[read_bucket_init_container],
        task_id=task_id,
        name=task_id,
        cmds=["dbt", dbt_cmd.value],
        arguments=["--profiles-dir", "/dbt-data", "--project-dir", "/dbt-data"],
        env_vars={"REQUESTS_CA_BUNDLE": "/etc/ssl/certs/ca-certificates.crt"},
        namespace=namespace,
        volume_mounts=[
            VolumeMount(
                name="dbt-data", mount_path="dbt-data", sub_path=None, read_only=False
            )
        ],
        volumes=[Volume(name="dbt-data", configs={})],
        image=os.getenv("KNADA_DBT_IMAGE", "navikt/knada-dbt:4"),
    )
