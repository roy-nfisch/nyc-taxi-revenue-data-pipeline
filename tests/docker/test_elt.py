"""Verify the installed ELT image on tiny data, as its default non-root user."""

import os
import subprocess

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("GREEN_TAXI_DOCKER_TESTS") != "1",
        reason="Opt into built-image checks.",
    ),
]


def test_elt_image_runs_and_reuses_sources_without_network():
    """Use disposable container storage; verify two reports and untouched ETL output."""
    code = """
import json
import os
from pathlib import Path
import pandas as pd
from green_taxi.settings import PipelineSettings
from green_taxi.elt.pipeline import ELTPipeline
assert os.getuid() == 10001
settings = PipelineSettings(reporting_period={'start_month': '2031-01', 'duration_months': 1}, data_dir=Path('/tmp/taxi-smoke'))
settings.raw_dir.mkdir(parents=True)
pd.DataFrame({'lpep_pickup_datetime': ['2031-01-01'], 'total_amount': [1.25]}).to_parquet(settings.raw_dir / 'green_tripdata_2031-01.parquet', index=False)
pipeline = ELTPipeline(settings)
for _ in range(2):
    metadata = json.loads(pipeline.run().read_text())
    assert metadata['counts']['rows_used'] == 1
    assert metadata['total_amount'] == '1.25'
assert not settings.processed_dir.exists()
"""
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "green-taxi-elt:local",
            "python",
            "-c",
            code,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
