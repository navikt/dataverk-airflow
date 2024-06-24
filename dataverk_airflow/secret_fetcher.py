import os
import json
import kubernetes.client as k8s
from dataverk_airflow.kubernetes_operator import SECRETS_PATH


def fetch_gsm_secrets(
    secrets: list,
    run_as_user: str = 5000,
):
    return k8s.V1Container(
        name="read-secrets",
        image="europe-north1-docker.pkg.dev/knada-gcp/knada-north/dataverk-airflow-mantest:v5",
        volume_mounts=[
            k8s.V1VolumeMount(
                name="secret",
                mount_path=SECRETS_PATH,
                sub_path=None,
                read_only=False
            ),
        ],
        command=["python", "/scripts/secretfetch.py"],
        args=[SECRETS_PATH, json.dumps(secrets)],
        security_context=k8s.V1SecurityContext(
            run_as_user=run_as_user,
            allow_privilege_escalation=False,
        )
    )
