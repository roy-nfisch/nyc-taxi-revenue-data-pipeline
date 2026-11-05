"""ETL entry-point compatibility and independence from the optional engine."""

import subprocess
import sys

import pytest

pytestmark = pytest.mark.integration


def test_etl_stage_imports_are_passive(check_passive_imports):
    """ETL imports must not start work, create storage or load DuckDB."""
    check_passive_imports(
        "green_taxi.etl",
        "green_taxi.etl.revenue",
        "green_taxi.etl.pipeline",
        "green_taxi.__main__",
    )


def test_existing_prefect_command_delegates_to_etl():
    """The compatibility module exposes the same command/flow, not a second copy."""
    from green_taxi import flow as delegate
    from green_taxi.etl import flow as implementation

    assert delegate.main is implementation.main
    assert delegate.taxi_flow is implementation.taxi_flow


def test_etl_command_does_not_load_duckdb(tmp_path):
    """Even with the extra installed, ordinary imports/settings must not use it."""
    code = """
import importlib.abc
import sys

class BlockEngine(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'duckdb', '_duckdb'}:
            raise AssertionError('ETL imported the optional engine')

sys.meta_path.insert(0, BlockEngine())
from green_taxi.__main__ import main
from click.testing import CliRunner
result = CliRunner().invoke(main)
assert result.exit_code == 0, result.output
assert 'reporting_period' in result.output
"""
    subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert list(tmp_path.iterdir()) == []


def test_existing_prefect_module_command_still_shows_help(tmp_path):
    """Running the documented module from another directory needs no API server."""
    result = subprocess.run(
        [sys.executable, "-m", "green_taxi.flow", "--help"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    assert all(flag in result.stdout for flag in ("--config", "--data-dir", "--force"))
