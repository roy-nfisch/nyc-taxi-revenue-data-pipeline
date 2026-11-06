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
