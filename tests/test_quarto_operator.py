import datetime
import os
from unittest import mock

import pytest
from airflow import DAG
from dataverk_airflow.quarto_operator import quarto_operator


@mock.patch.dict(os.environ, {"KNADA_TEAM_SECRET": "team-secret",
                              "NAMESPACE": "namespace",
                              "KNADA_AIRFLOW_OPERATOR_IMAGE": "operator-image",
                              "K8S_IMAGE_PULL_SECRETS": "image-pull-secret"})
class TestQuartoOperator:
    """Test quarto_operator.py"""

    @pytest.fixture
    def dag(self):
        return DAG("dag_id", start_date=datetime.datetime(2023, 10, 10))

    @pytest.fixture
    def quarto(self):
        return {
            "path": "/path/to/quarto.qmd",
            "env": "dev",
            "id": "uuid",
            "token": "quarto-token"
        }

    def test_that_knada_operator_image_is_used(self, dag, quarto):
        container = quarto_operator(dag, "name", quarto, "repo")
        assert container.image == "operator-image"

    def test_that_personal_operator_image_is_used(self, dag, quarto):
        container = quarto_operator(
            dag, "name", quarto, "repo", image="personal-image")
        assert container.image == "personal-image"

    def test_that_cmds_are_correct(self, dag, quarto):
        container = quarto_operator(dag, "name", quarto, "repo")
        correct_cmds = ["quarto render quarto.qmd --to html --execute --output index.html -M self-contained:True && "
                        f"""curl --fail-with-body -X PUT -F index.html=@index.html https://datamarkedsplassen.intern.dev.nav.no/quarto/update/{quarto['id']} -H "Authorization:Bearer {quarto['token']}" """]
        assert container.arguments == correct_cmds

    def test_that_quarto_deps_is_added_to_allowlist(self, dag, quarto):
        container = quarto_operator(dag, "name", quarto, "repo")

        annotations = container.annotations
        assert "allowlist" in annotations
        assert "datamarkedsplassen.intern.dev.nav.no" in annotations["allowlist"]
        assert "cdnjs.cloudflare.com" in annotations["allowlist"]

    def test_that_log_output_is_added_to_cmds(self, dag, quarto):
        with pytest.raises(KeyError) as err:
            quarto_operator(dag, "name", {}, "repo")
        assert err
