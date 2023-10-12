import datetime
import os
from unittest import mock

from airflow import DAG
from dataverk_airflow.python_operator import python_operator


@mock.patch.dict(os.environ, {"KNADA_TEAM_SECRET": "team-secret",
                              "NAMESPACE": "namespace",
                              "KNADA_AIRFLOW_OPERATOR_IMAGE": "operator-image",
                              "K8S_IMAGE_PULL_SECRETS": "image-pull-secret"})
class TestPythonOperator:
    """Test python_operator.py"""

    def test_that_knakda_operator_image_is_used(self):
        dag = DAG("dag_id", start_date=datetime.datetime(2023, 10, 11))
        container = python_operator(dag, "name", "repo", "script_path")
        assert container.image == "operator-image"

    def test_that_personal_operator_image_is_used(self):
        dag = DAG("dag_id", start_date=datetime.datetime(2023, 10, 11))
        container = python_operator(dag, "name", "repo", "script_path", image="personal-image")
        assert container.image == "personal-image"

    def test_that_cmds_are_correct(self):
        dag = DAG("dag_id", start_date=datetime.datetime(2023, 10, 11))
        container = python_operator(dag, "name", "repo", "script_path")
        assert container.cmds[-1] == "script_path"
