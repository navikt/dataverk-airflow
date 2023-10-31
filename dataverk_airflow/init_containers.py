import os

from typing import List
import kubernetes.client as k8s


def create_git_clone_init_container(
    repo: str,
    branch: str,
    mount_path: str
) -> List(k8s.V1Container):
    return k8s.V1Container(
        name="clone-repo",
        image=os.getenv("CLONE_REPO_IMAGE", "europe-west1-docker.pkg.dev/knada-gcp/knada/git-sync:2023-03-09-bfc0f3e"),
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", mount_path=mount_path, sub_path=None, read_only=False
            ),
            k8s.V1VolumeMount(
                name="airflow-git-secret",
                mount_path="/keys",
            ),
        ],
        command=["/bin/sh", "-c"],
        args=[f"/git-clone.sh {repo} {branch} {mount_path}"]
    )
