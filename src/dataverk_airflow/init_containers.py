import os
import kubernetes.client as k8s


envs = [
    {"name": "HTTPS_PROXY", "value": os.environ["HTTPS_PROXY"]},
    {"name": "https_proxy", "value": os.environ["HTTPS_PROXY"]},
]


def create_git_clone_init_container(
    repo: str,
    branch: str,
    mount_path: str
):
    return k8s.V1Container(
        name="clone-repo",
        image=os.getenv("CLONE_REPO_IMAGE", "navikt/knada-git-sync:2020-10-23-98963f6"),
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", mount_path=mount_path, sub_path=None, read_only=False
            ),
            k8s.V1VolumeMount(
                name="git-clone-secret",
                mount_path="/keys",
                sub_path=None,
                read_only=False,
            ),
        ],
        env=envs,
        command=["/bin/sh", "/git-clone.sh"],
        args=[repo, branch, "/repo"],
    )


def change_permissions_init_container(
    mount_path: str
):
    return k8s.V1Container(
        name="change-permissions",
        image="busybox",
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", mount_path=mount_path, sub_path=None, read_only=False
            ),
        ],
        command=['sh', '-c', f'chmod -R 777 {mount_path}'],
    )


def dbt_read_gcs_bucket(
    mount_path: str,
    seed_source: dict,
    profiles_dir: str
):
    gcs_envs = envs.copy()
    gcs_envs.append({"name": "GOOGLE_APPLICATION_CREDENTIALS", "value": "/var/run/secrets/google-creds/creds.json"})
    return k8s.V1Container(
        name="read-gcs-blob",
        image=os.getenv("KNADA_READ_BLOB_IMAGE", "navikt/knada-read-blob:1"),
        env=gcs_envs,
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", mount_path=mount_path, sub_path=None, read_only=False
            ),
            k8s.V1VolumeMount(
                name="google-creds",
                mount_path="/var/run/secrets/google-creds",
                sub_path=None,
                read_only=False,
            ),
            k8s.V1VolumeMount(
                name="ca-bundle-pem",
                mount_path="/etc/pki/tls/certs/ca-bundle.crt",
                read_only=True,
                sub_path="ca-bundle.pem"
            ),
        ],
        command=["python3", "/read-gcs-blob.py"],
        args=[seed_source["gcs_bucket"], seed_source["blob_name"], f"{mount_path}/{profiles_dir}/data"]
    )


def dbt_read_s3_bucket(
    mount_path: str,
    seed_source: dict,
    profiles_dir: str
):
    return k8s.V1Container(
        name="read-gcs-blob",
        image=os.getenv("KNADA_READ_BLOB_IMAGE", "navikt/knada-read-blob:1"),
        env=envs,
        volume_mounts=[
            k8s.V1VolumeMount(
                name="dags-data", mount_path=mount_path, sub_path=None, read_only=False
            ),
            k8s.V1VolumeMount(
                name="ca-bundle-pem",
                mount_path="/etc/pki/tls/certs/ca-bundle.crt",
                read_only=True,
                sub_path="ca-bundle.pem"
            ),
        ],
        command=["python3", "/read-s3-blob.py"],
        args=[seed_source["bucket"], seed_source["blob_name"], f"{mount_path}/{profiles_dir}/data"]
    )
