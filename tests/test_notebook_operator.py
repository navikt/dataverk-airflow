import datetime
import os
from unittest import mock

import pytest
from airflow import DAG
from dataverk_airflow.notebook_operator import notebook_operator


@mock.patch.dict(os.environ, {"KNADA_TEAM_SECRET": "team-secret",
                              "NAMESPACE": "namespace",
                              "KNADA_AIRFLOW_OPERATOR_IMAGE": "operator-image",
                              "DATAVERK_IMAGE_PYTHON_38": "operator-image-python-3.8",
                              "DATAVERK_IMAGE_PYTHON_39": "operator-image-python-3.9",
                              "DATAVERK_IMAGE_PYTHON_310": "operator-image-python-3.10",
                              "DATAVERK_IMAGE_PYTHON_311": "operator-image-python-3.11",
                              "DATAVERK_IMAGE_PYTHON_312": "operator-image-python-3.12",
                              "K8S_IMAGE_PULL_SECRETS": "image-pull-secret"})
class TestNotebookOperator:
    """Test notebook_operator.py"""

    @pytest.fixture
    def dag(self):
        return DAG("dag_id", start_date=datetime.datetime(2023, 10, 10))

    def test_that_knada_operator_image_is_used(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo")
        assert container.image == "operator-image-python-3.11"

    def test_select_different_python_version_for_knada_operator_image(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo", python_version="3.12")
        assert container.image == "operator-image-python-3.12"

    def test_select_invalid_python_version_for_knada_operator_image(self, dag):
        with pytest.raises(ValueError) as err:
            notebook_operator(dag, "name", "notebook_path", "repo", python_version="3.7")
        assert err

    def test_that_personal_operator_image_is_used(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo", image="personal-image")
        assert container.image == "personal-image"

    def test_that_cmds_are_correct(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo")
        assert container.arguments == ["papermill notebook_path output.ipynb --kernel python3"]

    def test_disable_notebook_kernel_override(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo", override_notebook_kernelspec=False)
        assert container.arguments == ["papermill notebook_path output.ipynb"]

    def test_that_log_output_is_added_to_cmds(self, dag):
        container = notebook_operator(dag, "name", "notebook_path", "repo", log_output=True)
        assert "--log-output" in container.arguments[-1]
