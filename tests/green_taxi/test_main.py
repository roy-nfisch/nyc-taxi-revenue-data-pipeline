"""Check JSON loading and the terminal entry point without running data work."""

import json
from datetime import date

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from green_taxi.__main__ import main
from green_taxi.settings import DEFAULT_CONFIG, PipelineSettings

pytestmark = pytest.mark.integration


def test_packaged_defaults_are_loaded_without_writes(tmp_path, monkeypatch):
    """JSON supplies default months even outside the checkout directory."""
    monkeypatch.chdir(tmp_path)
    settings = PipelineSettings.from_file()
    assert settings.months == ("2025-01", "2025-02", "2025-03")
    assert "months" not in json.loads(DEFAULT_CONFIG.read_text())
    assert (settings.reporting_period.start, settings.reporting_period.end) == (
        date(2025, 1, 1),
        date(2025, 4, 1),
    )
    assert not settings.data_dir.exists()


def test_custom_file_and_storage_override(tmp_path):
    """Read another month; a mount override wins over JSON storage."""
    config = tmp_path / "run.json"
    config.write_text(
        json.dumps({"months": ["2026-07"], "data_dir": "old", "log_level": "DEBUG"})
    )
    settings = PipelineSettings.from_file(config, data_dir=tmp_path / "mounted")
    assert settings.months == ("2026-07",)
    assert settings.data_dir == tmp_path / "mounted"
    assert settings.log_level == "DEBUG"


@pytest.mark.parametrize(
    "text",
    [
        "{",
        "[]",
        "{}",
        '{"months": ["2025-00"]}',
        '{"months": ["2025-01"], "unexpected": 1}',
    ],
)
def test_bad_file_fails_without_default_fallback(tmp_path, text):
    """Reject malformed or invalid configuration instead of hiding it."""
    config = tmp_path / "bad.json"
    config.write_text(text)
    with pytest.raises(ValueError):
        PipelineSettings.from_file(config)


def test_missing_file_and_missing_constructor_months(tmp_path):
    """Direct construction requires months; an unreadable file is not ignored."""
    with pytest.raises(FileNotFoundError):
        PipelineSettings.from_file(tmp_path / "missing.json")
    with pytest.raises(ValidationError):
        PipelineSettings()


def test_command_loads_config_and_prints_settings(tmp_path):
    """Click passes file/path arguments to our settings loader."""
    config = tmp_path / "run.json"
    config.write_text('{"months": ["2025-02"]}')
    storage = tmp_path / "storage"
    result = CliRunner().invoke(
        main, ["--config", str(config), "--data-dir", str(storage)]
    )
    assert result.exit_code == 0, result.output
    values = json.loads(result.output)
    assert values["months"] == ["2025-02"]
    assert values["data_dir"] == str(storage)
    assert not storage.exists()


@pytest.mark.parametrize(
    "start, duration, expected",
    [
        ("2025-03", 3, ["2025-03", "2025-04", "2025-05"]),
        ("2031-12", 2, ["2031-12", "2032-01"]),
        ("2032-02", 1, ["2032-02"]),
    ],
)
def test_period_only_json_derives_download_months(tmp_path, start, duration, expected):
    """One edit controls source selection and report dates, including year rollover."""
    config = tmp_path / "run.json"
    config.write_text(
        json.dumps(
            {
                "reporting_period": {
                    "start_month": start,
                    "duration_months": duration,
                }
            }
        )
    )
    storage = tmp_path / "data"
    result = CliRunner().invoke(
        main, ["--config", str(config), "--data-dir", str(storage)]
    )
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["months"] == expected
    assert not storage.exists()


@pytest.mark.parametrize("duration", [0, -1, 1.0, "1", True, None])
def test_invalid_period_cannot_generate_months(tmp_path, duration):
    """Validate strict positive integers before using duration to build filenames."""
    config = tmp_path / "bad.json"
    config.write_text(
        json.dumps(
            {
                "reporting_period": {
                    "start_month": "2025-01",
                    "duration_months": duration,
                }
            }
        )
    )
    with pytest.raises(ValidationError):
        PipelineSettings.from_file(config)


def test_command_default_and_help():
    """No arguments are needed; help explains the optional file override."""
    runner = CliRunner()
    result = runner.invoke(main)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["months"] == list(
        PipelineSettings.from_file().months
    )
    help_result = runner.invoke(main, ["--help"])
    assert help_result.exit_code == 0
    assert "--config" in help_result.output


def test_command_reports_config_error(tmp_path):
    """Invalid JSON produces an error instead of usable-looking defaults."""
    config = tmp_path / "bad.json"
    config.write_text("[]")
    result = CliRunner().invoke(main, ["--config", str(config)])
    assert result.exit_code != 0
    assert "Configuration must be a JSON object" in result.output


def test_command_loads_custom_period_without_changing_source_months(tmp_path):
    """Load a nested period through JSON/CLI, keeping source selection separate."""
    config = tmp_path / "run.json"
    values = {
        "months": ["2031-01"],
        "reporting_period": {"start_month": "2030-12", "duration_months": 2},
    }
    config.write_text(json.dumps(values))
    settings = PipelineSettings.from_file(config, data_dir=tmp_path / "data")
    assert (settings.reporting_period.start, settings.reporting_period.end) == (
        date(2030, 12, 1),
        date(2031, 2, 1),
    )
    result = CliRunner().invoke(main, ["--config", str(config)])
    assert result.exit_code == 0, result.output
    actual = json.loads(result.output)
    assert actual["reporting_period"] == values["reporting_period"]
    assert actual["months"] == values["months"]
    assert not settings.data_dir.exists()


@pytest.mark.parametrize("duration", [0, -1, -2, True, 1.0, "1", None])
def test_command_rejects_invalid_period_before_any_work(tmp_path, duration):
    """JSON numbers must be positive integers; invalid periods create no storage."""
    config = tmp_path / "bad.json"
    config.write_text(
        json.dumps(
            {
                "months": ["2031-01"],
                "reporting_period": {
                    "start_month": "2031-01",
                    "duration_months": duration,
                },
            }
        )
    )
    storage = tmp_path / "data"
    result = CliRunner().invoke(
        main, ["--config", str(config), "--data-dir", str(storage)]
    )
    assert result.exit_code != 0
    assert "duration_months" in result.output
    assert not storage.exists()
