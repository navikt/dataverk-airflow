import os
from datetime import timedelta
from pathlib import Path
from typing import Callable

from airflow import DAG
from kubernetes import client

from dataverk_airflow.kubernetes_operator import kubernetes_operator, VALID_PYTHON_VERSIONS


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
        python_version: str = "3.11",
        resources: client.V1ResourceRequirements = None,
        startup_timeout_seconds: int = None,
        retries: int = None,
        delete_on_finish: bool = True,
        retry_delay: timedelta = None,
        do_xcom_push: bool = False,
        container_uid: int = 50000,
        on_success_callback: Callable = None,
        use_uv_pip_install: bool = False,
):
    """Operator for rendering Quarto.

    This operator will automatically add Datamarkedsplassen, and cdnjs.cloudflare.com to the allow list, as these are required to render and publish a Quarto story.

    :param dag: DAG: owner DAG
    :param name: str: Name of task
    :param repo: str: Github repo
    :param quarto: dict: Dict of Quarto configuration, needs the following values {"path": "path/to/index.qmd", "folder": "path/to/folder", "env": "dev/prod", "id":"uuid", "token": "quarto-token", "format": "html"}
    :param image: str: Dockerimage the pod should use
    :param branch: str: Branch in repo, default "main"
    :param email: str: Email of owner
    :param slack_channel: str: Name of Slack channel, default None (no Slack notification)
    :param extra_envs: dict: Dict with environment variables, example: {"key": "value", "key2": "value2"}
    :param allowlist: list: List of hosts and port the task needs to reach on the format host:port
    :param requirements_path: str: Path (including filename) to your requirements.txt
    :param python_version: str: desired Python version for the environment your code will be running in when using the default image. We offer only supported versions of Python, and default to the latest version if this parameter is omitted. See https://devguide.python.org/versions/ for available versions.
    :param resources: dict: Specify cpu and memory resource usage (dict: request/limit: {"memory": "", "cpu": "", "ephemeral-storage": ""}), default None
    :param startup_timeout_seconds: int: Pod startup timeout
    :param retries: int: Number of retries for task before DAG fails, default 3
    :param delete_on_finish: bool: Whether to delete pod on completion
    :param retry_delay: timedelta: Time inbetween retries, default 5 seconds
    :param do_xcom_push: bool: Enable xcom push of content in file '/airflow/xcom/return.json', default False
    :param container_uid: int: User ID for the container image. Root (id = 0) is not allowed, defaults to 50000 (standard uid for airflow).
    :param on_success_callback: Callable
    :param use_uv_pip_install: bool: Use uv pip install, default False

    :return: KubernetesPodOperator
    """
    if not image:
        if python_version not in VALID_PYTHON_VERSIONS:
            raise ValueError(
                f"When using the default image the python_version argument must be set to " \
                f"{', '.join(VALID_PYTHON_VERSIONS[:-1])} or {VALID_PYTHON_VERSIONS[-1]}: value '{python_version}' is not supported" 
            )
        try:
            image = os.environ["DATAVERK_IMAGE_PYTHON_" + python_version.replace(".","")]
        except KeyError:
            image = os.getenv("KNADA_AIRFLOW_OPERATOR_IMAGE")


    working_dir = None
    try:
        if quarto.get("path"):
            working_dir = Path(quarto['path']).parent
        else:
            working_dir = Path(quarto['folder'])

        if os.getenv("MARKEDSPLASSEN_HOST"):
            host = os.getenv("MARKEDSPLASSEN_HOST")
        elif quarto['env'] == "prod":
            host = "datamarkedsplassen.intern.nav.no"
        else:
            host = "datamarkedsplassen.intern.dev.nav.no"

        cmds = create_quarto_cmds(quarto, host)
    except KeyError as err:
        raise KeyError(
            f"path or folder and environment, id and token must be provided in the Quarto configuration. Missing  {err}")

    allowlist.append(host)
    allowlist.append("cdnjs.cloudflare.com")

    kwargs = {
        "dag": dag, "name": name, "repo": repo, "image": image, "cmds": cmds, "branch": branch, "email": email,
        "slack_channel": slack_channel, "extra_envs": extra_envs, "allowlist": allowlist, "requirements_path": requirements_path,
        "resources": resources, "startup_timeout_seconds": startup_timeout_seconds, 
        "retries": retries, "delete_on_finish": delete_on_finish, "retry_delay": retry_delay, "do_xcom_push": do_xcom_push,
        "on_success_callback": on_success_callback, "working_dir": str(working_dir), "container_uid": container_uid, "use_uv_pip_install": use_uv_pip_install,
    }

    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    return kubernetes_operator(**kwargs)


def create_quarto_cmds(quarto: dict, host: str) -> list:
    url = f"https://{host}/quarto/update/{quarto['id']}"

    if quarto.get("path"):
        quarto_format = quarto.get('format', "html")
        return [
            f"quarto render {Path(quarto['path']).name} --to {quarto_format} --execute --output index.html -M self-contained:True",
            f"""curl --fail-with-body --retry 2 -X PUT -F index.html=@index.html {url} -H "Authorization:Bearer {quarto['token']}" """
        ]
    else:
        return [
            f"quarto render --execute --output-dir output || true",
            f"knatch {quarto['id']} output {quarto['token']} --host {host}"
        ]
