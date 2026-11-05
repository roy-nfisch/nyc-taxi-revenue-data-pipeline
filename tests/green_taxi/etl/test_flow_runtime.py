"""Run real Prefect tasks against an isolated temporary server with fake HTTP."""

import json
from pathlib import Path

import pandas as pd
import pytest
import requests
from pandas.testing import assert_frame_equal
from prefect import flow
from prefect.client.orchestration import get_client
from prefect.testing.utilities import prefect_test_harness

from green_taxi.etl.flow import download_inputs, taxi_flow

pytestmark = [pytest.mark.integration, pytest.mark.data]


@flow(name="Download retry verification", persist_result=False)
def run_download(pipeline, force=False):
    """Give retry tests a real flow context; return task status, ID, and row count."""
    state = download_inputs.with_options(retry_delay_seconds=0)(
        pipeline, force, return_state=True
    )
    count = len(state.result()) if state.is_completed() else None
    return state.is_completed(), state.state_details.task_run_id, count


@pytest.fixture(scope="module", autouse=True)
def prefect_server():
    """Start/stop a temporary Prefect server; never use the user's normal server."""
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("DO_NOT_TRACK", "1")
        patch.setenv("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
        with prefect_test_harness():
            yield


def test_prefect_matches_plain_python_and_rebuilds(pipeline_setup, monkeypatch):
    """Both execution paths save equivalent real tables; second flow reuses inputs."""
    from green_taxi.etl.pipeline import TaxiPipeline

    pipeline, session = pipeline_setup
    marker = pipeline.run()
    expected = pd.read_parquet(marker.parent / "fact_daily_revenue.parquet")
    monkeypatch.setattr(TaxiPipeline, "from_settings", lambda settings: pipeline)
    first = json.loads(Path(taxi_flow()).read_text())
    second = json.loads(Path(taxi_flow()).read_text())
    assert first["run_id"] != second["run_id"]
    assert first["counts"] == second["counts"]
    assert session.get.call_count == len(pipeline.settings.months)
    assert_frame_equal(
        expected, pd.read_parquet(marker.parent / "fact_daily_revenue.parquet")
    )


def test_transient_failure_retries_and_reuses_completed_month(pipeline_setup):
    """A timeout on month two retries the batch without redownloading month one."""
    pipeline, session = pipeline_setup
    attempts = []

    def get(*args, **kwargs):
        attempts.append(args[0])
        if len(attempts) == 2:
            raise requests.Timeout("temporary")
        return session.get.return_value

    session.get.side_effect = get
    completed, task_id, count = run_download(pipeline)
    assert completed
    assert count == len(pipeline.settings.months)
    assert len(attempts) == len(pipeline.settings.months) + 1
    with get_client(sync_client=True) as client:
        assert client.read_task_run(task_id).run_count == 2


def test_corrupt_data_is_not_retried(pipeline_setup):
    """Unreadable Parquet fails after one attempt despite configured network retries."""
    pipeline, session = pipeline_setup
    response = session.get.return_value.__enter__.return_value
    response.iter_content.side_effect = lambda **kwargs: iter([b"not parquet"])
    completed, task_id, count = run_download(pipeline)
    assert not completed
    assert count is None
    assert session.get.call_count == 1
    assert not pipeline.settings.processed_dir.exists()


def test_network_retry_limit_is_three_attempts(pipeline_setup):
    """Persistent network failure exhausts two retries without publishing outputs."""
    pipeline, session = pipeline_setup
    session.get.side_effect = requests.Timeout("still unavailable")
    completed, task_id, count = run_download(pipeline)
    assert not completed and count is None
    assert session.get.call_count == 3
    with get_client(sync_client=True) as client:
        assert client.read_task_run(task_id).run_count == 3
    assert not pipeline.settings.processed_dir.exists()


def test_forced_retry_refreshes_all_selected_sources(pipeline_setup, parquet_bytes):
    """Force never reuses a stale preexisting month after a partial refresh fails."""
    pipeline, session = pipeline_setup
    pipeline.run()
    session.get.reset_mock()
    response = session.get.return_value.__enter__.return_value
    response.iter_content.side_effect = lambda **kwargs: iter(
        [parquet_bytes(amount=20.5)]
    )
    attempts = []

    def get(*args, **kwargs):
        attempts.append(args[0])
        if len(attempts) == 2:
            raise requests.Timeout("temporary")
        return session.get.return_value

    session.get.side_effect = get
    completed, _, count = run_download(pipeline, force=True)
    assert completed and count == len(pipeline.settings.months)
    assert attempts.count(attempts[0]) == 2
    for path in pipeline.settings.raw_dir.glob("*.parquet"):
        assert pd.read_parquet(path).total_amount.tolist() == [20.5]


@pytest.mark.parametrize("stage", ["download", "calculate", "save"])
def test_full_flow_fails_without_running_later_stages(
    pipeline_setup, monkeypatch, stage
):
    """Actual flow failure must preserve prior reports and stop later operations."""
    from unittest.mock import MagicMock

    from green_taxi.etl.pipeline import TaxiPipeline

    pipeline, _ = pipeline_setup
    marker = pipeline.run()
    before = {path.name: path.read_bytes() for path in marker.parent.iterdir()}
    download = MagicMock(wraps=pipeline.downloader.download)
    calculate = MagicMock(wraps=pipeline.calculator.calculate)
    save = MagicMock(wraps=pipeline.writer.write)
    operations = {"download": download, "calculate": calculate, "save": save}
    operations[stage].side_effect = ValueError("review failure")
    monkeypatch.setattr(pipeline.downloader, "download", download)
    monkeypatch.setattr(pipeline.calculator, "calculate", calculate)
    monkeypatch.setattr(pipeline.writer, "write", save)
    monkeypatch.setattr(TaxiPipeline, "from_settings", lambda settings: pipeline)
    state = taxi_flow(return_state=True)
    assert state.is_failed()
    assert operations[stage].call_count == 1
    if stage == "download":
        calculate.assert_not_called()
    if stage != "save":
        save.assert_not_called()
    assert {path.name: path.read_bytes() for path in marker.parent.iterdir()} == before
