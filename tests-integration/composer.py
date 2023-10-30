import os
from airflow import DAG
from datetime import datetime
from kubernetes.client import models as k8s
from airflow.models import Variable
from dataverk_airflow import python_operator, notebook_operator

# Env variable for GCP bucket is automatically set for composer instances
# For the test we must set it manually
os.environ["GCS_BUCKET"] = "dataverk-airflow-tests"

with DAG('DataverkAirflowComposer', start_date=datetime(2023, 2, 15), schedule=None) as dag:
    py_op = python_operator(
        dag = dag,
        name = "python-op",
        script_path = "notebooks/script.py",
        requirements_path="notebooks/requirements.txt",
        retries=0,
        is_composer=True,
        startup_timeout_seconds=60,
    )

    nb_op = notebook_operator(
        dag = dag,
        name = "nb-op",
        nb_path = "notebooks/mynb.ipynb",
        requirements_path="notebooks/requirements.txt",
        retries=0,
        is_composer=True,
        startup_timeout_seconds=60,
    )

    py_op
    nb_op
