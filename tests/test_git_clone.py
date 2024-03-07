from pathlib import Path

import pytest
import yaml
from dataverk_airflow import git_clone

FIXTURE_DIR = Path(__file__).parent.resolve() / 'test_files'


@pytest.mark.datafiles(FIXTURE_DIR / "git_clone.yaml")
def test_git_clone(datafiles):
    container = git_clone("repo", "branch", "mount", 50000)
    for yml in datafiles.iterdir():
        with open(yml) as f:
            assert container.to_dict() == yaml.safe_load(f.read())
