"""Boundary tests for the run settings, using no network or source dataset."""

import pytest
from pydantic import ValidationError

from green_taxi.settings import PipelineSettings

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("months", [("2025-01",), ("2025-03", "2025-01"), ("2026-12",)])
def test_month_selection(months):
    """Keep an explicit valid selection; do not silently add months."""
    assert PipelineSettings(months=months).months == months


@pytest.mark.parametrize(
    "values",
    [
        {"months": ()},
        {"months": ("2025-01", "2025-01")},
        {"months": ("2025-13",)},
        {"months": ("0000-01",)},
        {"months": ("2025-1",)},
        {"months": ("../2025-01",)},
        {"data_dir": " "},
        {"timeout_seconds": 0},
        {"timeout_seconds": -1},
        {"timeout_seconds": 301},
        {"timeout_seconds": float("inf")},
        {"timeout_seconds": float("nan")},
        {"log_level": "QUIET"},
        {"unexpected": True},
        {"source_url": "http://example.org/data"},
        {"source_url": "https://user:password@example.org/data"},
        {"source_url": "https://example.org/data?token=secret"},
        {"source_url": "https://example.org/data#fragment"},
        {"source_url": "not a URL"},
    ],
)
def test_invalid_settings(values):
    """Reject invalid settings before downloads or directory creation."""
    with pytest.raises(ValidationError):
        PipelineSettings(**({"months": ("2025-01",)} | values))


def test_paths_are_resolved_without_creating_directories(tmp_path, monkeypatch):
    """Use caller-relative or absolute mount paths without creating storage."""
    monkeypatch.chdir(tmp_path)
    settings = PipelineSettings.from_file()
    assert settings.raw_dir == tmp_path / "data/raw"
    assert settings.processed_dir == tmp_path / "data/processed"
    assert not settings.data_dir.exists()
    assert PipelineSettings.from_file(data_dir=tmp_path).data_dir == tmp_path


def test_settings_are_immutable():
    """Prevent configuration changing halfway through a run."""
    with pytest.raises(ValidationError):
        PipelineSettings.from_file().months = ("2025-03",)


def test_period_derives_months_without_mutating_configuration():
    """Derivation belongs to the settings object and leaves caller input untouched."""
    values = {"reporting_period": {"start_month": "2031-12", "duration_months": 2}}
    settings = PipelineSettings.model_validate(values)
    assert settings.months == ("2031-12", "2032-01")
    assert "months" not in values
    assert isinstance(values["reporting_period"], dict)


@pytest.mark.parametrize("months", [None, [], ["2031-12", "2031-12"]])
def test_invalid_explicit_months_are_not_silently_derived(months):
    """An invalid advanced override must fail, rather than run a different selection."""
    with pytest.raises(ValidationError):
        PipelineSettings(
            months=months,
            reporting_period={"start_month": "2031-12", "duration_months": 2},
        )


@pytest.mark.parametrize("timeout", [0.1, 300])
def test_valid_timeout_boundaries(timeout):
    """Accept positive bounded HTTP timeouts."""
    assert (
        PipelineSettings(months=("2025-01",), timeout_seconds=timeout).timeout_seconds
        == timeout
    )
