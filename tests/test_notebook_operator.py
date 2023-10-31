import datetime
import os
from unittest import mock

import pytest
from airflow import DAG
from dataverk_airflow.notebook_operator import notebook_operator


@mock.patch.dict(os.environ, {"KNADA_TEAM_SECRET": "team-secret",
                              "NAMESPACE": "namespace",
                              "KNADA_AIRFLOW_OPERATOR_IMAGE": "operator-image",
                              "K8S_IMAGE_PULL_SECRETS": "image-pull-secret"})
class TestNotebookOperator:
    """Test notebook_operator.py"""

    @pytest.fixture
    def dag(self):
        return DAG("dag_id", start_date=datetime.datetime(2023, 10, 10))

    def test_that_knada_operator_image_is_used(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo")
        assert container.image == "operator-image"

    def test_that_personal_operator_image_is_used(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo", image="personal-image")
        assert container.image == "personal-image"

    def test_that_cmds_are_correct(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo")
        assert container.arguments == ["papermill notebook_path output.ipynb"]

    def test_that_log_output_is_added_to_cmds(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo", log_output=True)
        assert "--log-output" in container.arguments[-1]
