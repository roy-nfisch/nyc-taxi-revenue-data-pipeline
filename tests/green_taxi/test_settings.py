"""Boundary tests for the run settings, using no network or source dataset."""

import pytest
from pydantic import ValidationError

from green_taxi.settings import PipelineSettings


@pytest.mark.parametrize("months", [("2025-01",), ("2025-03", "2025-01")])
def test_month_selection(months):
    """Keep an explicit valid selection; do not silently add months."""
    assert PipelineSettings(months=months).months == months


@pytest.mark.parametrize(
    "values",
    [
        {"months": ()},
        {"months": ("2025-01", "2025-01")},
        {"months": ("2025-04",)},
        {"months": ("../2025-01",)},
        {"data_dir": " "},
        {"timeout_seconds": 0},
        {"timeout_seconds": -1},
        {"timeout_seconds": 301},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": float("nan")},
        {"log_level": "QUIET"},
        {"unexpected": True},
    ],
)
def test_invalid_settings(values):
    """Reject invalid settings before downloads or directory creation."""
    with pytest.raises(ValidationError):
        PipelineSettings(**values)


def test_paths_are_resolved_without_creating_directories(tmp_path, monkeypatch):
    """Use caller-relative or absolute mount paths without creating storage."""
    monkeypatch.chdir(tmp_path)
    settings = PipelineSettings()
    assert settings.raw_dir == tmp_path / "data/raw"
    assert settings.processed_dir == tmp_path / "data/processed"
    assert not settings.data_dir.exists()
    assert PipelineSettings(data_dir=tmp_path).data_dir == tmp_path


def test_settings_are_immutable():
    """Prevent configuration changing halfway through a run."""
    with pytest.raises(ValidationError):
        PipelineSettings().months = ("2025-03",)


@pytest.mark.parametrize("timeout", [0.1, 300])
def test_valid_timeout_boundaries(timeout):
    """Accept positive bounded HTTP timeouts."""
    assert PipelineSettings(timeout_seconds=timeout).timeout_seconds == timeout
