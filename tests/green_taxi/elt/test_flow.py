"""Exercise the four ELT stages on an isolated Prefect test server."""

import json
from pathlib import Path

import pytest
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import FlowRunFilter, FlowRunFilterId
from prefect.testing.utilities import prefect_test_harness

from green_taxi.elt import flow as module
from green_taxi.elt.pipeline import ELTPipeline

pytestmark = pytest.mark.integration
pytest.importorskip("duckdb")


def test_prefect_uses_same_elt_pipeline_and_four_stages(pipeline_setup, monkeypatch):
    """The real flow must match plain output and record four completed tasks."""
    etl, _ = pipeline_setup
    pipeline = ELTPipeline(etl.settings, etl.downloader)
    expected = json.loads(pipeline.run().read_text())
    monkeypatch.setattr(module, "ELTPipeline", lambda settings: pipeline)
    monkeypatch.setenv("DO_NOT_TRACK", "1")
    monkeypatch.setenv("PREFECT_SERVER_ANALYTICS_ENABLED", "false")
    with prefect_test_harness(server_startup_timeout=90):
        state = module.taxi_flow(return_state=True)
        actual = json.loads(Path(state.result()).read_text())
        with get_client(sync_client=True) as client:
            tasks = client.read_task_runs(
                flow_run_filter=FlowRunFilter(
                    id=FlowRunFilterId(any_=[state.state_details.flow_run_id])
                )
            )
        assert len(tasks) == 4
        assert all(task.state.is_completed() for task in tasks)
    assert actual["counts"] == expected["counts"]
    assert actual["total_amount"] == expected["total_amount"]


def test_cli_requires_an_explicit_server(monkeypatch):
    """The ELT command must not silently start a local Prefect server."""
    from click.testing import CliRunner

    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = CliRunner().invoke(module.main)
    assert result.exit_code != 0
    assert "PREFECT_API_URL" in result.output


@pytest.mark.parametrize("failed", ["download", "load", "calculate", "save"])
def test_flow_stops_before_later_stages(pipeline_setup, monkeypatch, failed):
    """A failure at any stage must not be followed by later work or publication."""
    etl, _ = pipeline_setup
    pipeline = ELTPipeline(etl.settings, etl.downloader)
    monkeypatch.setattr(module, "ELTPipeline", lambda settings: pipeline)
    calls = []

    def stage(name):
        def run(*args):
            calls.append(name)
            if name == failed:
                raise ValueError("injected stage failure")
            return ()

        return run

    for name, attribute in [
        ("download", "download_inputs"),
        ("load", "load_sources"),
        ("calculate", "calculate_report"),
        ("save", "save_report"),
    ]:
        monkeypatch.setattr(module, attribute, stage(name))
    with pytest.raises(ValueError, match="injected"):
        module.taxi_flow.fn()
    order = ["download", "load", "calculate", "save"]
    assert calls == order[: order.index(failed) + 1]
