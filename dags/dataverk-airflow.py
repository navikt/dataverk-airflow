from airflow import DAG
from datetime import datetime
from kubernetes.client import models as k8s
from airflow.models import Variable
from dataverk_airflow import notebook_operator, python_operator, quarto_operator


with DAG('DataverkAirflow', start_date=datetime(2023, 2, 15), schedule=None) as dag:
    py_op = python_operator(
        dag = dag,
        name = "python-op",
        repo = "navikt/dataverk-airflow",
        branch="integration-tests",
        script_path = "dags/notebooks/script.py",
        requirements_path="dags/notebooks/requirements.txt",
        retries=0,
    )

    nb_op = notebook_operator(
        dag = dag,
        name = "nb-op",
        repo = "navikt/dataverk-airflow",
        branch="integration-tests",
        nb_path = "dags/notebooks/mynb.ipynb",
        requirements_path="dags/notebooks/requirements.txt",
        retries=0,
    )

    quarto_op = quarto_operator(
        dag=dag,
        name="quarto-op",
        repo="navikt/dataverk-airflow",
        branch="integration-tests",
        quarto={
            "path": "dags/notebooks/quarto.ipynb",
            "env": "dev",
            "id": "bf48d8a4-05ca-47a5-a360-bc24171baf62",
            "token": Variable.get("quarto_token"),
        },
        requirements_path="dags/notebooks/requirements.txt",
        retries=0,
    )

    py_op
    nb_op
    quarto_op
