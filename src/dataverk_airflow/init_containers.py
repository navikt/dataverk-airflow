import os
import kubernetes.client as k8s


envs = [
    {"name": "HTTPS_PROXY", "value": os.environ["HTTPS_PROXY"]},
    {"name": "https_proxy", "value": os.environ["HTTPS_PROXY"]},
    {"name": "NO_PROXY", "value": os.environ["NO_PROXY"]},
    {"name": "no_proxy", "value": os.environ["NO_PROXY"]},
]


def create_git_clone_init_container(
    repo: str,
    branch: str,
    mount_path: str
):
    return k8s.V1Container(
        name="clone-repo",
        image=os.getenv("CLONE_REPO_IMAGE", "navikt/knada-git-sync:2020-12-02-df3d995"),
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
        command=["/bin/sh", "-c"],
        args=[f"/git-clone.sh {repo} {branch} {mount_path}; chmod -R 777 {mount_path}"],
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
        args=[seed_source["bucket"], seed_source["blob_name"], f"{mount_path}/{profiles_dir}/data"]
    )


def dbt_read_s3_bucket(
    mount_path: str,
    seed_source: dict,
    profiles_dir: str
):
    s3_envs = envs.copy()
    s3_envs.append({"name": "VKS_VAULT_ADDR", "value": os.environ["VKS_VAULT_ADDR"]})
    s3_envs.append({"name": "VKS_AUTH_PATH", "value": os.environ["VKS_AUTH_PATH"]})
    s3_envs.append({"name": "VKS_KV_PATH", "value": os.environ["VKS_KV_PATH"]})
    s3_envs.append({"name": "K8S_SERVICEACCOUNT_PATH", "value": os.environ["K8S_SERVICEACCOUNT_PATH"]})
    return k8s.V1Container(
        name="read-s3-blob",
        image=os.getenv("KNADA_READ_BLOB_IMAGE", "navikt/knada-read-blob:1"),
        env=s3_envs,
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
