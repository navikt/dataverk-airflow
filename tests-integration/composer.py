import os
from airflow import DAG
from datetime import datetime
from kubernetes.client import models as k8s
from airflow.models import Variable
from dataverk_airflow import python_operator, notebook_operator, quarto_operator

# Env variable for GCP bucket is automatically set for composer instances
# For the test we must set it manually
os.environ["GCS_BUCKET"] = "dataverk-airflow-tests"

with DAG('DataverkAirflowComposer', start_date=datetime(2023, 2, 15), schedule=None) as dag:
    py_op = python_operator(
        dag = dag,
        is_composer=True,
        name = "python-op",
        script_path = "tests-integration/notebooks/script.py",
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,        
        startup_timeout_seconds=60,
    )

    nb_op = notebook_operator(
        dag = dag,
        is_composer=True,
        name = "nb-op",
        nb_path = "tests-integration/notebooks/mynb.ipynb",
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
    )

    quarto_op = quarto_operator(
        dag=dag,
        is_composer=True,
        name="quarto-op",
        quarto={
            "path": "tests-integration/notebooks/quarto.ipynb",
            "env": "dev",
            "id": "bf48d8a4-05ca-47a5-a360-bc24171baf62",
            "token": Variable.get("quarto_token"),
        },
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
    )

    py_op
    nb_op
    quarto_op
