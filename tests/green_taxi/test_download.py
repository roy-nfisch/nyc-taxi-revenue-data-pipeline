"""Exercise real file staging with controlled HTTP responses and failures."""

import hashlib
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
import requests
from click.testing import CliRunner

from green_taxi.download import TaxiDownloader, main
from green_taxi.settings import PipelineSettings


def test_download_identity_and_request(staging, parquet_bytes):
    """Lineage matches saved bytes, and requests have a bounded timeout."""
    downloader, session, _ = staging
    (item,) = downloader.download()
    assert item.path.read_bytes() == parquet_bytes()
    assert item.size_bytes == len(parquet_bytes())
    assert item.sha256 == hashlib.sha256(item.path.read_bytes()).hexdigest()
    assert item.month == "2025-01" and item.downloaded
    session.get.assert_called_once_with(item.source_url, stream=True, timeout=60)
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


def test_corrupt_existing_file_fails_instead_of_being_trusted(staging):
    """Existence alone is not validation; repair requires explicit force."""
    downloader, session, _ = staging
    downloader.settings.raw_dir.mkdir()
    destination = downloader.settings.raw_dir / "green_tripdata_2025-01.parquet"
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


def test_all_months_and_owned_session_cleanup(tmp_path, staging, monkeypatch):
    """The downloader closes sessions it creates and stages selected files once."""
    _, session, _ = staging
    session.__enter__.return_value = session
    monkeypatch.setattr(requests, "Session", lambda: session)
    downloader = TaxiDownloader(PipelineSettings(data_dir=tmp_path))
    results = downloader.download()
    assert [item.month for item in results] == ["2025-01", "2025-02", "2025-03"]
    assert session.get.call_count == 3
    session.__exit__.assert_called_once()


def test_later_month_failure_keeps_completed_month(tmp_path, staging):
    """A failed batch does not erase completed inputs; retry can reuse them."""
    _, session, response = staging
    settings = PipelineSettings(data_dir=tmp_path, months=("2025-01", "2025-02"))
    response.raise_for_status.side_effect = [None, requests.HTTPError("503")]
    with pytest.raises(requests.HTTPError):
        TaxiDownloader(settings, session).download()
    assert [p.name for p in settings.raw_dir.iterdir()] == [
        "green_tripdata_2025-01.parquet"
    ]


def test_connection_failure_removes_temporary_file(staging):
    """A failure before a response exists must also clean up staging files."""
    downloader, session, _ = staging
    session.get.side_effect = requests.Timeout("connection timed out")
    with pytest.raises(requests.Timeout):
        downloader.download()
    assert list(downloader.settings.raw_dir.iterdir()) == []


def test_cli_download_and_invalid_month(tmp_path, staging, monkeypatch):
    """The public command uses the same object and rejects unsafe month inputs."""
    _, session, _ = staging
    session.__enter__.return_value = session
    monkeypatch.setattr(requests, "Session", lambda: session)
    runner = CliRunner()
    result = runner.invoke(main, ["--data-dir", str(tmp_path), "--month", "2025-01"])
    assert result.exit_code == 0, result.output
    assert "green_tripdata_2025-01.parquet" in result.output
    session.reset_mock()
    invalid = runner.invoke(main, ["--month", "../2025-01"])
    assert invalid.exit_code == 2
    session.get.assert_not_called()
