"""Check retry decisions and sequential delegation without starting a server."""

from unittest.mock import MagicMock

import pytest
import requests
from click.testing import CliRunner

from green_taxi.flow import main, retry_download, taxi_flow

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "error, expected",
    [
        (requests.Timeout(), True),
        (requests.ConnectionError(), True),
        (requests.exceptions.ChunkedEncodingError(), True),
        (requests.exceptions.SSLError(), False),
        (ValueError(), False),
        (PermissionError(), False),
    ],
)
def test_only_transient_errors_retry(error, expected):
    """Data/storage/certificate errors need correction, not repeated attempts."""
    state = MagicMock()
    state.result.side_effect = error
    assert retry_download(None, None, state) is expected


@pytest.mark.parametrize(
    "status, expected",
    [
        (429, True),
        (500, True),
        (503, True),
        (599, True),
        (600, False),
        (400, False),
        (401, False),
        (404, False),
    ],
)
def test_retryable_http_status(status, expected):
    """Retry throttling/server failures, not bad requests or missing source months."""
    response = requests.Response()
    response.status_code = status
    state = MagicMock()
    state.result.side_effect = requests.HTTPError(response=response)
    assert retry_download(None, None, state) is expected


def test_flow_cli_requires_explicit_server(monkeypatch):
    """No API URL means an actionable error, not an accidental temporary server."""
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = CliRunner().invoke(main)
    assert result.exit_code != 0
    assert "PREFECT_API_URL" in result.output


def test_flow_delegates_in_order_without_copying_rules(monkeypatch, tmp_path):
    """Exercise the flow body with task doubles; orchestration adds no calculation."""
    import green_taxi.flow as module

    calls = []
    pipeline = MagicMock()
    monkeypatch.setattr(module.TaxiPipeline, "from_settings", lambda settings: pipeline)

    def step(name, value):
        def run(*args):
            calls.append(name)
            return value

        return run

    monkeypatch.setattr(module, "download_inputs", step("download", ()))
    monkeypatch.setattr(module, "calculate_report", step("calculate", "report"))
    monkeypatch.setattr(module, "save_report", step("save", "metadata.json"))
    assert taxi_flow.fn(data_dir=str(tmp_path)) == "metadata.json"
    assert calls == ["download", "calculate", "save"]


def test_flow_cli_passes_config_storage_and_refresh(monkeypatch, tmp_path):
    """Click must forward explicit run options unchanged to the orchestration layer."""
    config = tmp_path / "run.json"
    config.write_text("{}")
    monkeypatch.setenv("PREFECT_API_URL", "http://127.0.0.1:4205/api")
    run = MagicMock(return_value="metadata.json")
    monkeypatch.setattr("green_taxi.flow.taxi_flow", run)
    result = CliRunner().invoke(
        main, ["--config", str(config), "--data-dir", str(tmp_path), "--force"]
    )
    assert result.exit_code == 0, result.output
    run.assert_called_once_with(str(config), str(tmp_path), True)
