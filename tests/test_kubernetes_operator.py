import datetime
import os
from unittest import mock

import pytest
from airflow import DAG
from dataverk_airflow import MissingValueException, kubernetes_operator


@mock.patch.dict(os.environ, {"KNADA_TEAM_SECRET": "team-secret",
                              "NAMESPACE": "namespace",
                              "K8S_IMAGE_PULL_SECRETS": "image-pull-secret"})
class TestKubernetesOperator:
    """Test kubernetes_operator.py"""

    @pytest.fixture
    def dag(self):
        return DAG("dag_id", start_date=datetime.datetime(2023, 10, 10))

    def test_that_exception_is_raised_for_empty_image(self, dag):
        with pytest.raises(MissingValueException) as info:
            kubernetes_operator(dag, "name", "repo", "")
        assert "image cannot be empty" in str(info.value)

    def test_that_exception_is_raised_for_empty_repo(self, dag):
        with pytest.raises(MissingValueException) as info:
            kubernetes_operator(dag, "name", "", "image")
        assert "repo cannot be empty" in str(info.value)

    def test_that_extra_envs_are_merged_in(self, dag):
        container = kubernetes_operator(dag, "name", "repo", "image",
                                        extra_envs={"key": "value"})
        test_value = None
        for env in container.env_vars:
            if env.name == "key":
                test_value = env.value
        assert test_value == "value"

    def test_that_slack_is_added_to_allowlist(self, dag):
        container = kubernetes_operator(dag, "name", "repo", "image",
                                        slack_channel="slack")

        annotations = container.executor_config["pod_override"].metadata.annotations
        assert "allowlist" in annotations
        assert "slack.com" in annotations["allowlist"]