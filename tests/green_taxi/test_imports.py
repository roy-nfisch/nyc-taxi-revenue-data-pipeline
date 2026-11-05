"""A fresh Python process can import the package without starting services."""

import subprocess
import sys

import pytest

pytestmark = pytest.mark.integration


def test_imports_do_not_access_network_or_create_storage(tmp_path):
    """Block network and directory writes while importing in a clean directory."""
    code = """
from unittest.mock import patch
import requests
with (
    patch("requests.sessions.Session.request", side_effect=AssertionError("HTTP")),
    patch("socket.create_connection", side_effect=AssertionError("network")),
    patch("pathlib.Path.mkdir", side_effect=AssertionError("mkdir")),
):
    import green_taxi.settings
    import green_taxi.logging
    import green_taxi.__main__
    import green_taxi.download
    import green_taxi.reporting
    import green_taxi.etl
    import green_taxi.etl.revenue
    import green_taxi.etl.pipeline
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    assert result.stdout == ""
    assert list(tmp_path.iterdir()) == []


def test_existing_prefect_command_delegates_to_etl():
    """The compatibility module exposes the same command/flow, not a second copy."""
    from green_taxi import flow as delegate
    from green_taxi.etl import flow as implementation

    assert delegate.main is implementation.main
    assert delegate.taxi_flow is implementation.taxi_flow


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
