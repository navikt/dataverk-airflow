from airflow import DAG
from datetime import datetime
from kubernetes.client import models as k8s
from airflow.models import Variable
from dataverk_airflow import notebook_operator, python_operator, quarto_operator


with DAG('DataverkAirflow', start_date=datetime(2023, 2, 15), schedule=None) as dag:
    nb_op = notebook_operator(
        dag = dag,
        name = "nb-op",
        repo = "navikt/dataverk-airflow",
        nb_path = "dags/notebooks/mynb.ipynb",
        requirements_path="dags/notebooks/requirements.txt",
    )

    py_op = python_operator(
        dag = dag,
        name = "python-op",
        repo = "navikt/dataverk-airflow",
        script_path = "dags/notebooks/script.py",
        requirements_path="dags/notebooks/requirements.txt",
    )

    quarto_op = quarto_operator(
        dag=dag,
        name="quarto-op",
        repo="navikt/dataverk-airflow",
        quarto={
            "path": "dags/notebooks/quarto.ipynb",
            "env": "dev",
            "id": "4bdcde31-5a0d-4e90-8335-8d6b8134deb1",
            "token": Variable.get("quarto_token"),
        },
        requirements_path="dags/notebooks/requirements.txt",
    )

    nb_op
    py_op
    quarto_op