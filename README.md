# Dataverk airflow

Enkelt wrapperbibliotek rundt [KubernetesPodOperator](https://airflow.apache.org/docs/stable/kubernetes.html) som lager Airflow task som kjører i en Kubernetes pod.

## Våre operators

Alle våre operators lar deg klone et repo på forhånd, bare legg det til med `repo="navikt/<repo>`.
Vi har også støtte for å installere Python pakker ved oppstart av Airflow task, spesifiser `requirements.txt`-filen din med `requirements_path="/path/to/requirements.txt"`.

### Quarto operator

Denne kjører Quarto render for deg.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import quarto_operator


with DAG('navn-dag', start_date=days_ago(1), schedule_interval="*/10 * * * *") as dag:
    t1 = quarto_operator(dag=dag,
                         name="<navn-på-task>",
                         repo="navikt/<repo>",
                         quarto={
                             "path": "/path/to/index.qmd",
                             "env": "dev/prod",
                             "id":"uuid",
                             "token":
                             "quarto-token"
                         },
                         slack_channel="<#slack-alarm-kanal>")
```

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

## Kubernetes operator

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
