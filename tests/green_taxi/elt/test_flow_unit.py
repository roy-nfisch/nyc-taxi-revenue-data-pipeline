"""Check ELT flow delegation without DuckDB or a running Prefect server."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from green_taxi.elt import flow as module
from green_taxi.elt.pipeline import ELTPipeline

pytestmark = pytest.mark.unit


def test_cli_requires_an_explicit_server(monkeypatch):
    """The ELT command must not silently start a local Prefect server."""
    monkeypatch.delenv("PREFECT_API_URL", raising=False)
    result = CliRunner().invoke(module.main)
    assert result.exit_code != 0
    assert "PREFECT_API_URL" in result.output


@pytest.mark.parametrize("failed", ["download", "load", "calculate", "save"])
def test_flow_stops_before_later_stages(monkeypatch, failed):
    """A failed stage stops the flow body without opening storage or a server."""
    pipeline = MagicMock(spec=ELTPipeline)
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


@pytest.mark.parametrize("fails", [False, True])
def test_save_task_uses_pipeline_boundary(fails):
    """Forward inputs unchanged; return the path as text or propagate failure."""
    pipeline = MagicMock(spec=ELTPipeline)
    pipeline.save.return_value = Path("metadata.json")
    inputs, result, context = (), object(), {}
    if fails:
        pipeline.save.side_effect = OSError("storage failure")
        with pytest.raises(OSError, match="storage failure"):
            module.save_report.fn(pipeline, inputs, result, context)
    else:
        assert (
            module.save_report.fn(pipeline, inputs, result, context) == "metadata.json"
        )
    pipeline.save.assert_called_once_with(inputs, result, context)
