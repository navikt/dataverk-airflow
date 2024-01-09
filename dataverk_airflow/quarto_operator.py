import os
from datetime import timedelta
from pathlib import Path
from typing import Callable

from airflow import DAG
from kubernetes import client

from dataverk_airflow.kubernetes_operator import kubernetes_operator


def quarto_operator(
        dag: DAG,
        name: str,
        quarto: dict,
        repo: str = None,
        image: str = None,
        branch: str = "main",
        email: str = None,
        slack_channel: str = None,
        extra_envs: dict = {},
        allowlist: list = [],
        requirements_path: str = None,
        resources: client.V1ResourceRequirements = None,
        startup_timeout_seconds: int = None,
        retries: int = None,
        delete_on_finish: bool = True,
        retry_delay: timedelta = None,
        do_xcom_push: bool = False,
        on_success_callback: Callable = None,
):
    """Operator for rendering Quarto.

    This operator will automatically add Datamarkedsplassen, and cdnjs.cloudflare.com to the allow list, as these are required to render and publish a Quarto story.

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param quarto: dict: Dict of Quarto configuration, needs the following values {"path": "path/to/index.qmd", "env": "dev/prod", "id":"uuid", "token": "quarto-token", "format": "html"}
    :param image: str: Dockerimage the pod should use
    :param branch: str: Branch in repo, default "main"
    :param email: str: Email of owner
    :param slack_channel: str: Name of Slack channel, default None (no Slack notification)
    :param extra_envs: dict: Dict with environment variables, example: {"key": "value", "key2": "value2"}
    :param allowlist: list: List of hosts and port the task needs to reach on the format host:port
    :param requirements_path: bool: Path (including filename) to your requirements.txt
    :param resources: dict: Specify cpu and memory resource usage (dict: request/limit: {"memory": "", "cpu": "", "ephemeral-storage": ""}), default None
    :param startup_timeout_seconds: int: Pod startup timeout
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param do_xcom_push: bool: Enable xcom push of content in file '/airflow/xcom/return.json', default False
    :param on_success_callback: Callable

    :return: KubernetesPodOperator
    """
    if not image:
        image = os.getenv("KNADA_AIRFLOW_OPERATOR_IMAGE")

    working_dir = None
    try:
        if os.getenv("MARKEDSPLASSEN_HOST"):
            host = os.getenv("MARKEDSPLASSEN_HOST")
        elif quarto['env'] == "prod":
            host = "datamarkedsplassen.intern.nav.no"
        else:
            host = "datamarkedsplassen.intern.dev.nav.no"

        working_dir = Path(quarto['path']).parent
        url = f"https://{host}/quarto/update/{quarto['id']}"
        quarto_format = quarto.get('format', "html")

        cmds = [
            f"quarto render {Path(quarto['path']).name} --to {quarto_format} --execute --output index.html -M self-contained:True",
            f"""curl --fail-with-body -X PUT -F index.html=@index.html {url} -H "Authorization:Bearer {quarto['token']}" """
        ]
    except KeyError as err:
        raise KeyError(
            f"path, environment, id and token must be provided in the Quarto configuration. Missing  {err}")

    allowlist.append(host)
    allowlist.append("cdnjs.cloudflare.com")

    kwargs = {
        "dag": dag, "name": name, "repo": repo, "image": image, "cmds": cmds, "branch": branch, "email": email,
        "slack_channel": slack_channel, "extra_envs": extra_envs, "allowlist": allowlist,  "requirements_path": requirements_path,
        "resources": resources, "startup_timeout_seconds": startup_timeout_seconds, "retries": retries,
        "delete_on_finish": delete_on_finish, "retry_delay": retry_delay, "do_xcom_push": do_xcom_push,
        "on_success_callback": on_success_callback, "working_dir": str(working_dir)
    }

    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return kubernetes_operator(**kwargs)
