import os

import kubernetes.client as k8s


def git_clone(
    repo: str,
    branch: str,
    mount_path: str
):
    return k8s.V1Container(
        name="clone-repo",
        image=os.getenv("CLONE_REPO_IMAGE"),
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data",
                mount_path=mount_path,
                sub_path=None,
                read_only=False
            ),
            k8s.V1VolumeMount(
                name="airflow-git-secret",
                mount_path="/keys",
            ),
        ],
        command=["/bin/sh", "-c"],
        args=[f"/git-clone.sh"],
        env=[
            k8s.V1EnvVar(
                name="GIT_SYNC_ONE_TIME",
                value="true",
            ),
            k8s.V1EnvVar(
                name="GIT_SYNC_REPO",
                value=repo,
            ),
            k8s.V1EnvVar(
                name="GIT_SYNC_BRANCH",
                value=branch,
            ),
            k8s.V1EnvVar(
                name="GIT_SYNC_ROOT",
                value=mount_path,
            ),
        ]
    )
