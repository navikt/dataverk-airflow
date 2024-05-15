from airflow import DAG
from datetime import datetime
from airflow.models import Variable
from dataverk_airflow import python_operator, notebook_operator, quarto_operator


with DAG('KnadaOperators', start_date=datetime(2023, 2, 15), schedule=None) as dag:
    py_op = python_operator(
        dag=dag,
        name="python-op",
        repo="navikt/dataverk-airflow",
        script_path="tests-integration/notebooks/script.py",
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
    )
    py_op_uv = python_operator(
        dag=dag,
        name="python-op-uv",
        repo="navikt/dataverk-airflow",
        script_path="tests-integration/notebooks/script.py",
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
        use_uv_pip_install=True,
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

    nb_op_uv = notebook_operator(
        dag=dag,
        name="nb-op-uv",
        repo="navikt/dataverk-airflow",
        nb_path="tests-integration/notebooks/mynb.ipynb",
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
        use_uv_pip_install=True,
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

    quarto_op_uv = quarto_operator(
        dag=dag,
        name="quarto-op-uv",
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
        use_uv_pip_install=True,
    )

    quarto_book_op = quarto_operator(
        dag=dag,
        name="quarto-book-op",
        repo="navikt/dataverk-airflow",
        quarto={
            "folder": "tests-integration/notebooks/quartobook",
            "env": "dev",
            "id": "757da08e-031e-4fac-a5f0-fffe6d2d96b6",
            "token": Variable.get("quarto_token"),
        },
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
    )

    quarto_book_op_uv = quarto_operator(
        dag=dag,
        name="quarto-book-op-uv",
        repo="navikt/dataverk-airflow",
        quarto={
            "folder": "tests-integration/notebooks/quartobook",
            "env": "dev",
            "id": "757da08e-031e-4fac-a5f0-fffe6d2d96b6",
            "token": Variable.get("quarto_token"),
        },
        requirements_path="tests-integration/notebooks/requirements.txt",
        retries=0,
        startup_timeout_seconds=60,
        use_uv_pip_install=True,
    )

    py_op
    py_op_uv
    nb_op
    nb_op_uv
    quarto_op
    quarto_op_uv
    quarto_book_op
    quarto_book_op_uv
