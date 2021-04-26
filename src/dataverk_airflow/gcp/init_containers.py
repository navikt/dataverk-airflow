import kubernetes.client as k8s


def create_read_bucket_init_container(bucket_dbt_path: str):
    return k8s.V1Container(
        name="read-bucket",
        image="navikt/knada-dbt:4",
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dbt-data", mount_path="dbt-data", sub_path=None, read_only=False
            )
        ],
        command=["/bin/sh", "-c"],
        args=[
            f"gsutil cp -r {bucket_dbt_path}/* dbt-data/; chmod -R 777 dbt-data"
        ],
    )
