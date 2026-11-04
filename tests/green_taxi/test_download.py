"""Exercise real file staging with controlled HTTP responses and failures."""

import hashlib
import json
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import requests
from click.testing import CliRunner

from green_taxi.download import TaxiDownloader, main
from green_taxi.settings import PipelineSettings

pytestmark = [pytest.mark.integration, pytest.mark.data]


def test_download_identity_and_request(staging, parquet_bytes):
    """Lineage matches saved bytes, and requests have a bounded timeout."""
    downloader, session, _ = staging
    (item,) = downloader.download()
    assert item.path.read_bytes() == parquet_bytes()
    assert item.size_bytes == len(parquet_bytes())
    assert item.sha256 == hashlib.sha256(item.path.read_bytes()).hexdigest()
    assert item.month == downloader.settings.months[0] and item.downloaded
    session.get.assert_called_once_with(
        item.source_url, stream=True, timeout=downloader.settings.timeout_seconds
    )
    assert list(item.path.parent.glob("*.part")) == []
    with pytest.raises(FrozenInstanceError):
        item.sha256 = "changed"


def test_valid_input_is_reused_without_http(staging):
    """A second run revalidates existing data and does not request it again."""
    downloader, session, _ = staging
    (first,) = downloader.download()
    session.reset_mock()
    (second,) = downloader.download()
    session.get.assert_not_called()
    assert not second.downloaded
    assert second.sha256 == first.sha256
    session.close.assert_not_called()


@pytest.mark.parametrize(
    "failure", [requests.HTTPError("500"), requests.Timeout("timeout")]
)
def test_http_failure_never_publishes_partial_file(staging, failure):
    """HTTP errors propagate and leave no destination or temporary file."""
    downloader, _, response = staging
    response.raise_for_status.side_effect = failure
    with pytest.raises(type(failure)):
        downloader.download()
    assert list(downloader.settings.raw_dir.iterdir()) == []


def test_interrupted_refresh_preserves_previous_file(staging):
    """An interrupted stream cannot overwrite a previously usable input."""
    downloader, _, response = staging
    (first,) = downloader.download()
    original = first.path.read_bytes()

    def interrupted(**kwargs):
        yield b"partial bytes"
        raise requests.ConnectionError("disconnected")

    response.iter_content.side_effect = interrupted
    with pytest.raises(requests.ConnectionError):
        downloader.download(force=True)
    assert first.path.read_bytes() == original
    assert list(first.path.parent.glob("*.part")) == []


@pytest.mark.parametrize(
    "kind", ["html", "empty", "missing_date", "missing_amount", "truncated"]
)
def test_bad_download_is_not_published(staging, parquet_bytes, kind):
    """A successful HTTP response does not prove the bytes are usable input."""
    downloader, _, response = staging
    payloads = {
        "html": b"<html>not a dataset</html>",
        "empty": parquet_bytes(empty=True),
        "missing_date": parquet_bytes(missing="lpep_pickup_datetime"),
        "missing_amount": parquet_bytes(missing="total_amount"),
        "truncated": parquet_bytes()[:30],
    }
    response.iter_content.side_effect = lambda **kwargs: iter([payloads[kind]])
    with pytest.raises(ValueError):
        downloader.download()
    assert list(downloader.settings.raw_dir.iterdir()) == []


def test_corrupt_existing_file_fails_instead_of_being_trusted(staging, sample_filename):
    """Existence alone is not validation; repair requires explicit force."""
    downloader, session, _ = staging
    downloader.settings.raw_dir.mkdir()
    destination = downloader.settings.raw_dir / sample_filename
    destination.write_bytes(b"broken")
    with pytest.raises(ValueError):
        downloader.download()
    session.get.assert_not_called()
    assert destination.read_bytes() == b"broken"
    assert downloader.download(force=True)[0].downloaded


def test_refresh_changes_identity_under_same_filename(staging, parquet_bytes):
    """Checksums distinguish an updated source even if its name stays the same."""
    downloader, _, response = staging
    (old,) = downloader.download()
    response.iter_content.side_effect = lambda **kwargs: iter([parquet_bytes(99)])
    (new,) = downloader.download(force=True)
    assert old.path == new.path
    assert old.sha256 != new.sha256


def test_publish_failure_preserves_old_file_and_cleans_temporary(staging, monkeypatch):
    """A filesystem publication error stays visible and does not destroy old data."""
    downloader, _, _ = staging
    (old,) = downloader.download()
    original = old.path.read_bytes()

    def fail_replace(*args):
        raise OSError("cannot replace")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="cannot replace"):
        downloader.download(force=True)
    assert old.path.read_bytes() == original
    assert not list(old.path.parent.glob("*.part"))


def test_all_months_and_owned_session_cleanup(
    tmp_path, staging, monkeypatch, sample_months
):
    """The downloader closes sessions it creates and stages selected files once."""
    _, session, _ = staging
    session.__enter__.return_value = session
    monkeypatch.setattr(requests, "Session", lambda: session)
    downloader = TaxiDownloader(
        PipelineSettings(data_dir=tmp_path, months=sample_months)
    )
    results = downloader.download()
    assert [item.month for item in results] == list(sample_months)
    assert session.get.call_count == len(sample_months)
    session.__exit__.assert_called_once()


def test_later_month_failure_keeps_completed_month(
    tmp_path, staging, sample_months, sample_filename
):
    """A failed batch does not erase completed inputs; retry can reuse them."""
    _, session, response = staging
    settings = PipelineSettings(data_dir=tmp_path, months=sample_months[:2])
    response.raise_for_status.side_effect = [None, requests.HTTPError("503")]
    with pytest.raises(requests.HTTPError):
        TaxiDownloader(settings, session).download()
    assert [p.name for p in settings.raw_dir.iterdir()] == [sample_filename]


def test_connection_failure_removes_temporary_file(staging):
    """A failure before a response exists must also clean up staging files."""
    downloader, session, _ = staging
    session.get.side_effect = requests.Timeout("connection timed out")
    with pytest.raises(requests.Timeout):
        downloader.download()
    assert list(downloader.settings.raw_dir.iterdir()) == []


def test_cli_download_and_invalid_month(
    tmp_path, staging, monkeypatch, sample_months, sample_filename
):
    """The public command uses the same object and rejects unsafe month inputs."""
    _, session, _ = staging
    session.__enter__.return_value = session
    monkeypatch.setattr(requests, "Session", lambda: session)
    runner = CliRunner()
    config = tmp_path / "run.json"
    config.write_text(json.dumps({"months": [sample_months[0]]}))
    result = runner.invoke(main, ["--data-dir", str(tmp_path), "--config", str(config)])
    assert result.exit_code == 0, result.output
    assert sample_filename in result.output
    session.reset_mock()
    config.write_text(json.dumps({"months": [f"../{sample_months[0]}"]}))
    invalid = runner.invoke(main, ["--config", str(config)])
    assert invalid.exit_code != 0
    session.get.assert_not_called()


def test_network_failure_logs_context_without_exception_secrets(staging, download_logs):
    """Log the failing input, status and retry advice, not raw URLs or secrets."""
    downloader, _, response = staging
    failed_response = requests.Response()
    failed_response.status_code = 503
    response.raise_for_status.side_effect = requests.HTTPError(
        "sensitive-token-in-error", response=failed_response
    )
    with pytest.raises(requests.HTTPError):
        downloader.download()
    assert "Starting download" in download_logs.text
    assert "status=503" in download_logs.text
    assert downloader.settings.months[0] in download_logs.text
    assert "retry the command" in download_logs.text
    assert "sensitive-token-in-error" not in download_logs.text


def test_configured_source_is_used(staging, tmp_path):
    """The endpoint comes from validated settings, never a second code constant."""
    original, session, _ = staging
    settings = PipelineSettings(
        months=original.settings.months,
        data_dir=tmp_path,
        source_url="https://example.org/taxi/",
    )
    (item,) = TaxiDownloader(settings, session).download()
    assert item.source_url == f"https://example.org/taxi/{item.path.name}"
    assert session.get.call_args.args[0] == item.source_url
