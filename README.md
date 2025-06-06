# Dataverk airflow

Enkelt wrapperbibliotek rundt [KubernetesPodOperator](https://airflow.apache.org/docs/stable/kubernetes.html) som lager Airflow task som kjører i en Kubernetes pod.

## Våre operators

Alle våre operators lar deg klone et annet repo enn der DAGene er definert, bare legg det til med `repo="navikt/<repo>`.

Vi har også støtte for å installere Python pakker ved oppstart av Airflow task, spesifiser `requirements.txt`-filen din med `requirements_path="/path/to/requirements.txt"`.
Merk at hvis du kombinerer `repo` og `requirements_path`, må `requirements.txt` ligge i repoet nevnt i `repo`.

Ønsker du å benytte [UV](https://github.com/astral-sh/uv) for `pip install`, så kan du sette `use_uv_pip_install=True`.

| :exclamation:  `UV` kjører i et virtual environment og man må da spesifisere alle pakker man trenger i `requirements.txt`, da man ikke har tilgang på de som finnes i default image. F.eks `pyyaml`, `nbformat`, `nbclient` |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|

### Quarto operator (datafortelling)

Denne kjører `quarto render` for deg, som lager en HTML-fil som kan lastes opp til Datamarkedsplassen.

Vi har støtte for enkeltfiler, og kataloger, dette kan du spesifisere med `path` for enkeltfiler, og `folder` hvis du har et Quarto prosjekt i en katalog.
Quarto prosjekter brukes hovedsakelig for [book](https://quarto.org/docs/books/), [website](https://quarto.org/docs/websites/), eller [dashboard](https://quarto.org/docs/dashboards/).
Enkeltfiler bygges `self-contained`, som betyr at HTML-filen blir bygd med alle sine eksterne avhengighter (Javascript, CSS, og bilder).

For å laste opp filer til Datamarkedsplassen må man ha et Quarto-token, som er unikt per team.
Dette finner man under [Mine teams token](https://data.ansatt.nav.no/user/tokens) i menyen.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from dataverk_airflow import quarto_operator


with DAG('navn-dag', start_date=days_ago(1), schedule_interval="*/10 * * * *") as dag:
    t1 = quarto_operator(dag=dag,
                         name="<navn-på-task>",
                         repo="navikt/<repo>",
                         quarto={
                             "path": "/path/to/index.qmd",
                             "env": "dev/prod",
                             "id":"uuid",
                             "token": Variable.get("quarto_token"),
                         },
                         slack_channel="<#slack-alarm-kanal>")
```


Har du behov for å rendre noe annet enn `html`, kan du bruke verdien `format`.
Dette må du for eksempel gjøre hvis du ønsker å lage et dashboard.

```
with DAG('navn-dag', start_date=days_ago(1), schedule_interval="*/10 * * * *") as dag:
    t1 = quarto_operator(dag=dag,
                         name="<navn-på-task>",
                         repo="navikt/<repo>",
                         quarto={
                             "folder": "/path/to/book",
                             "format": "dashboard",
                             "env": "dev/prod",
                             "id":"uuid",
                             "token": Variable.get("quarto_token"),
                         },
                         slack_channel="<#slack-alarm-kanal>")
```

I eksemplene over lagrer vi tokenet i en Airflow variable som så brukes i DAG tasken under.
Se offisiell [Airflow dokumentasjon](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) for hvordan man bruker `Variable.get()´ i en task.

### Notebook operator

Denne lar deg kjøre en Jupyter notebook.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import notebook_operator


with DAG('navn-dag', start_date=days_ago(1), schedule_interval="*/10 * * * *") as dag:
    t1 = notebook_operator(dag=dag,
                           name="<navn-på-task>",
                           repo="navikt/<repo>",
                           nb_path="/path/to/notebook.ipynb",
                           slack_channel="<#slack-alarm-kanal>")
```

### Python operator

Denne lar deg kjøre vilkårlig Python-scripts.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('navn-dag', start_date=days_ago(1), schedule_interval="*/10 * * * *") as dag:
    t1 = python_operator(dag=dag,
                         name="<navn-på-task>",
                         repo="navikt/<repo>",
                         script_path="/path/to/script.py",
                         slack_channel="<#slack-alarm-kanal>")
```

### Kubernetes operator

Vi tilbyr også vår egen Kubernetes operator som kloner et valg repo inn i containeren.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import kubernetes_operator


with DAG('navn-dag', start_date=days_ago(1), schedule_interval="*/10 * * * *") as dag:
    t1 = kubernetes_operator(dag=dag,
                             name="<navn-på-task>",
                             repo="navikt/<repo>",
                             cmds=["/path/to/bin/", "script-name.sh", "argument1", "argument2"],
                             image="europe-north1-docker.pkg.dev/nais-management-233d/ditt-team/ditt-image:din-tag",
                             slack_channel="<#slack-alarm-kanal>")
```

Denne operatoren har støtte for to ekstra flagg som ikke er tilgjengelig fra de andre.

```
cmds: str: Command to run in pod
working_dir: str: Path to working directory
```

### Allow list

Alle operators støtter å sette allow list, men det er noen adresser som blir lagt til av Dataverk Airflow.

Hvis du bruker `slack_channel` argumentet, vil vi legge til:
- hooks.slack.com

Hvis du bruker `email` argumentet, vil vi legge til:
- Riktig SMTP-adresse

Hvis du bruker `requirements_path` argumentet, vil vi legge til:
- pypi.org
- files.pythonhosted.org
- pypi.python.org

For `quarto_operator` vil vi legge til:
- Adressen til riktig Datamarkedsplass
- cdnjs.cloudflare.com

### Felles argumenter

Alle operatorene våre har støtte for disse argumentene i funksjonskallet.

```
dag: DAG: owner DAG
name: str: Name of task
repo: str: Github repo
image: str: Dockerimage the pod should use
branch: str: Branch in repo, default "main"
email: str: Email of owner
slack_channel: str: Name of Slack channel, default None (no Slack notification)
extra_envs: dict: dict with environment variables example: {"key": "value", "key2": "value2"}
allowlist: list: list of hosts and port the task needs to reach on the format host:port
requirements_path: bool: Path (including filename) to your requirements.txt
python_version: str: Desired Python version for the environment your code will be running in when using the default image. We offer only supported versions of Python, and default to the latest version if this parameter is omitted. See https://devguide.python.org/versions/ for available versions.
resources: dict: Specify required cpu and memory requirements (keys in dict: request_memory, request_cpu, limit_memory, limit_cpu), default None
startup_timeout_seconds: int: pod startup timeout
retries: int: Number of retries for task before DAG fails, default 3
delete_on_finish: bool: Whether to delete pod on completion
retry_delay: timedelta: Time inbetween retries, default 5 seconds
do_xcom_push: bool: Enable xcom push of content in file "/airflow/xcom/return.json", default False
on_success_callback:: func: a function to be called when a task instance of this task succeeds
```

## Sette resource requirements

Vi har støtte for å sette `requests` og `limits` for hver operator.
Merk at man ikke trenger å sette `limits` på CPU da dette blir automatisk løst av plattformen.

Ved å bruke `ephemeral-storage` kan man be om ekstra diskplass for lagring i en task.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('navn-dag', start_date=days_ago(1), schedule_interval="*/10 * * * *") as dag:
    t1 = python_operator(dag=dag,
                         name="<navn-på-task>",
                         repo="navikt/<repo>",
                         script_path="/path/to/script.py",
                         resources={
                             "requests": {
                                 "memory": "50Mi",
                                 "cpu": "100m",
                                 "ephemeral-storage": "1Gi"
                             },
                             "limits": {
                                 "memory": "100Mi"
                             }
                         })
```
