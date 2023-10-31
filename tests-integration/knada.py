from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from dataverk_airflow import python_operator, notebook_operator, quarto_operator
from dataverk_airflow.knada_operators import create_knada_nb_pod_operator, create_knada_python_pod_operator


with DAG('DataverkAirflowKnada', start_date=datetime(2023, 2, 15), schedule=None) as dag:
    py_op = python_operator(
        dag=dag,
        name="python-op",
        repo="navikt/dataverk-airflow",
        script_path="tests-integration/notebooks/script.py",
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
    )

    nb_op = notebook_operator(
        dag=dag,
        name="nb-op",
        repo="navikt/dataverk-airflow",
        nb_path="tests-integration/notebooks/mynb.ipynb",
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
    )

    quarto_op = quarto_operator(
        dag=dag,
        name="quarto-op",
        repo="navikt/dataverk-airflow",
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

with DAG('LegacyOperators', start_date=datetime(2023, 2, 15), schedule=None) as dag:
    py_op = create_knada_python_pod_operator(
        dag=dag,
        name="python-op",
        repo="navikt/dataverk-airflow",
        script_path="tests-integration/notebooks/script.py",
    )

    nb_op = create_knada_nb_pod_operator(
        dag=dag,
        name="nb-op",
        repo="navikt/dataverk-airflow",
        nb_path="tests-integration/notebooks/mynb.ipynb",
    )

    py_op
    nb_op
