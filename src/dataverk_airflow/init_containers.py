import os
import kubernetes.client as k8s


def create_git_clone_init_container(
    repo: str,
    branch: str,
    mount_path: str
):
    return k8s.V1Container(
        name="clone-repo",
        image=os.getenv("CLONE_REPO_IMAGE", "navikt/knada-git-sync:2022-01-14-710e533"),
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", mount_path=mount_path, sub_path=None, read_only=False
            ),
            k8s.V1VolumeMount(
                name="airflow-git-secret",
                mount_path="/keys",
                sub_path=None,
                read_only=False,
            ),
        ],
        command=["/bin/sh", "-c"],
        args=[f"/git-clone.sh {repo} {branch} {mount_path}; chmod -R 777 {mount_path}"],
    )
