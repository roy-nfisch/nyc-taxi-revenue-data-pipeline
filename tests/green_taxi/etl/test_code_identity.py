"""Verify explicit unknown code provenance instead of inventing a clean revision."""

import subprocess

import pytest

from green_taxi.etl import pipeline

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "failure", [FileNotFoundError(), subprocess.TimeoutExpired("git", 2)]
)
def test_unavailable_git_is_explicit(monkeypatch, failure):
    """Missing Git or a bounded-call timeout permits a run with unknown provenance."""

    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(subprocess, "run", fail)
    assert pipeline.code_identity() == {"revision": "unknown", "dirty": "unknown"}


def test_wheel_does_not_inspect_callers_repository(monkeypatch, tmp_path):
    """A package outside a checkout never attributes work to an unrelated cwd."""
    monkeypatch.delenv("GREEN_TAXI_BUILD_REVISION", raising=False)
    monkeypatch.delenv("GREEN_TAXI_BUILD_DIRTY", raising=False)
    monkeypatch.setattr(
        pipeline, "__file__", str(tmp_path / "src/green_taxi/etl/pipeline.py")
    )
    assert pipeline.code_identity() == {"revision": "unknown", "dirty": "unknown"}


def test_nested_etl_module_uses_the_checkout_root(monkeypatch, tmp_path):
    """The new folder depth must not lose provenance or inspect the src directory."""
    (tmp_path / ".git").mkdir()
    monkeypatch.setattr(
        pipeline, "__file__", str(tmp_path / "src/green_taxi/etl/pipeline.py")
    )
    calls = []

    def run(command, **kwargs):
        calls.append(kwargs["cwd"])
        return subprocess.CompletedProcess(
            command, 0, "reviewed-revision\n" if command[1] == "rev-parse" else ""
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert pipeline.code_identity() == {"revision": "reviewed-revision", "dirty": False}
    assert calls == [tmp_path, tmp_path]


@pytest.mark.parametrize(
    "value, expected",
    [("true", True), ("false", False), ("unknown", "unknown"), ("garbage", "unknown")],
)
def test_installed_package_reports_build_identity(
    monkeypatch, tmp_path, value, expected
):
    """An image build stamp identifies its source without pretending Git is present."""
    monkeypatch.setattr(
        pipeline, "__file__", str(tmp_path / "src/green_taxi/etl/pipeline.py")
    )
    monkeypatch.setenv("GREEN_TAXI_BUILD_REVISION", "reviewed-build")
    monkeypatch.setenv("GREEN_TAXI_BUILD_DIRTY", value)
    assert pipeline.code_identity() == {"revision": "reviewed-build", "dirty": expected}


@pytest.mark.parametrize("status, dirty", [("", False), (" M src/example.py\n", True)])
def test_git_identity_records_revision_and_changes(monkeypatch, status, dirty):
    """Both Git calls use the package checkout, bounded execution, and real output."""
    from pathlib import Path

    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        return subprocess.CompletedProcess(
            command, 0, "abc123\n" if command[1] == "rev-parse" else status
        )

    monkeypatch.setattr(subprocess, "run", run)
    assert pipeline.code_identity() == {"revision": "abc123", "dirty": dirty}
    assert len(calls) == 2
    for _, options in calls:
        assert options == {
            "cwd": Path(pipeline.__file__).resolve().parents[3],
            "check": True,
            "capture_output": True,
            "text": True,
            "timeout": 2,
        }
