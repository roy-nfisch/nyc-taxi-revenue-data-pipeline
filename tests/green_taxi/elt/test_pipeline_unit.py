"""Check the ELT publication boundary without loading the database engine."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from green_taxi.elt.pipeline import ELTPipeline
from green_taxi.run_report import RunReport

pytestmark = pytest.mark.unit


def test_batch_uses_pipeline_save_step(reporting_settings, monkeypatch):
    """The plain batch uses the same save boundary as the Prefect task."""
    pipeline = ELTPipeline(reporting_settings)
    for name in ("start_run", "load", "calculate", "save"):
        monkeypatch.setattr(pipeline, name, MagicMock())
    pipeline.downloader = MagicMock()
    assert pipeline.run() is pipeline.save.return_value
    pipeline.save.assert_called_once_with(
        pipeline.downloader.download.return_value,
        pipeline.calculate.return_value,
        pipeline.start_run.return_value,
    )


@pytest.mark.parametrize("fails", [False, True])
def test_save_delegates_to_shared_reporting(reporting_settings, fails):
    """Keep metadata arguments, returned path and writer failures unchanged."""
    pipeline = ELTPipeline(reporting_settings)
    pipeline.reporting = MagicMock(spec=RunReport)
    expected = Path("metadata.json")
    pipeline.reporting.save.return_value = expected
    inputs, result, context = (), object(), {}
    if fails:
        pipeline.reporting.save.side_effect = OSError("storage failure")
        with pytest.raises(OSError, match="storage failure"):
            pipeline.save(inputs, result, context)
    else:
        assert pipeline.save(inputs, result, context) is expected
    pipeline.reporting.save.assert_called_once_with(inputs, result, context)
