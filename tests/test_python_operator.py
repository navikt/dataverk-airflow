import datetime
import os
from unittest import mock

import pytest
from airflow import DAG
from dataverk_airflow.python_operator import python_operator


@mock.patch.dict(os.environ, {"KNADA_TEAM_SECRET": "team-secret",
                              "NAMESPACE": "namespace",
                              "KNADA_AIRFLOW_OPERATOR_IMAGE": "operator-image",
                              "DATAVERK_IMAGE_PYTHON_38": "operator-image-python-3.8",
                              "DATAVERK_IMAGE_PYTHON_39": "operator-image-python-3.9",
                              "DATAVERK_IMAGE_PYTHON_310": "operator-image-python-3.10",
                              "DATAVERK_IMAGE_PYTHON_311": "operator-image-python-3.11",
                              "DATAVERK_IMAGE_PYTHON_312": "operator-image-python-3.12",
                              "K8S_IMAGE_PULL_SECRETS": "image-pull-secret"})
class TestPythonOperator:
    """Test python_operator.py"""

    @pytest.fixture
    def dag(self):
        return DAG("dag_id", start_date=datetime.datetime(2023, 10, 10))

    def test_that_knada_operator_image_is_used(self, dag):
        container = python_operator(dag, "name", "script_path", "repo")
        assert container.image == "operator-image-python-3.12"

    def test_select_different_python_version_for_knada_operator_image(self, dag):
        container = python_operator(dag, "name", "script_path", "repo", python_version="3.11")
        assert container.image == "operator-image-python-3.11"

    def test_select_invalid_python_version_for_knada_operator_image(self, dag):
        with pytest.raises(ValueError) as err:
            python_operator(dag, "name", "script_path", "repo", python_version="3.7")
        assert err

    def test_that_personal_operator_image_is_used(self, dag):
        container = python_operator(dag, "name", "script_path", "repo", image="personal-image")
        assert container.image == "personal-image"

    def test_that_cmds_are_correct(self, dag):
        container = python_operator(dag, "name", "script_path", "repo")
        assert container.arguments == ["python script_path"]
