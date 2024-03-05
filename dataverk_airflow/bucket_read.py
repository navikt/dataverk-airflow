import os
from kubernetes.client.models import V1SecurityContext, V1SeccompProfile
import kubernetes.client as k8s

def bucket_read(
    mount_path: str,
) -> k8s.V1Container:
    return k8s.V1Container(
        name="read-bucket",
        image="google/cloud-sdk:alpine",
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data",
                mount_path=mount_path,
                sub_path=None,
                read_only=False
            ),
        ],
        command=["/bin/sh", "-c"],
        args=[
            f"gsutil rsync -r -x '^logs/*|^.*-build/*' gs://{os.environ['GCS_BUCKET']} {mount_path} && chmod -R 777 {mount_path}"
        ],
        resources=k8s.V1ResourceRequirements(
            requests={"memory": "128Mi"},
            limits={"memory": "128Mi"}
        ),
    )
