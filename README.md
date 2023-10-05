# Dataverk airflow

Enkelt wrapperbibliotek rundt [KubernetesPodOperator](https://airflow.apache.org/docs/stable/kubernetes.html) som lager Airflow task som kjører i en Kubernetes pod.

## Våre operators

### Python operator

Vi tilbyr en operator som lar deg kjøre både Jupyter notebooks, eller Python-scripts.
Begge operatorene lar deg klone et repo og kjøre en notebook fil eller et Python-script.

Internt i operatoren finner den ut om det er et Python-script eller en notebook som skal kjøres.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from dataverk_airflow import python_operator


with DAG('navn-dag', start_date=days_ago(1), schedule_interval="*/10 * * * *") as dag:
    t1 = python_operator(dag=dag,
                         name="<navn-på-task>",
                         repo="navikt/<repo>",
                         script_path="<sti-til-fil-i-repo>",
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
