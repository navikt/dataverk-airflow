# Dataverk airflow
Enkel wrapperbibliotek rundt [KubernetesPodOperator](https://airflow.apache.org/docs/stable/kubernetes.html) som lager 
airflow tasker som kjører i separate kubernetes podder.

## Knada pod notebook operator
Lager en kubernetes pod operator som kjører en jupyter notebook. Tar seg av kloning av ønsket repo og varsling ved feil
på epost og/eller slack.

### Eksempel på bruk
````python

from airflow import DAG
from datetime import datetime
from dataverk_airflow.knada_operators import create_knada_nb_pod_operator


with DAG('navn-pod-dag', start_date=datetime(2020, 10, 28), schedule_interval="*/10 * * * *") as dag:
    t1 = create_knada_nb_pod_operator(dag=dag,
                                      email="<epost@epost.no>",
                                      slack_channel="<#slack-alarm-kanal>",
                                      name="<navn-på-task>",
                                      repo="navikt/<repo>",
                                      nb_path="<sti-til-notebook-i-repo>",
                                      namespace="<kubernetes-namespace>",
                                      branch="<branch-i-repo>",
                                      log_output=False)
````

## Knada python pod operator
Lager en kubernetes pod operator som kjører et python skript. Tar seg av kloning av ønsket repo og varsling ved feil
på epost og/eller slack.

````python
  
from airflow import DAG
from datetime import datetime
from dataverk_airflow.knada_operators import create_knada_python_pod_operator


with DAG('navn-pod-dag', start_date=datetime(2020, 10, 28), schedule_interval="*/10 * * * *") as dag:
    t1 = create_knada_python_pod_operator(dag=dag,
                                          email="<epost@epost.no>",
                                          slack_channel="<#slack-alarm-kanal>",
                                          name="<navn-på-task>",
                                          repo="navikt/<repo>",
                                          script_path="<sti-til-notebook-i-repo>",
                                          namespace="<kubernetes-namespace>",
                                          branch="<branch-i-repo>")
````
