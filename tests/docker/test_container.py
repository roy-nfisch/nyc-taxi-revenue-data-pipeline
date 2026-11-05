"""Opt-in image checks using tiny local inputs, never the public taxi download."""

import json
import os
import subprocess
from decimal import Decimal

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from green_taxi.settings import PipelineSettings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("GREEN_TAXI_DOCKER_TESTS") != "1",
        reason="Set GREEN_TAXI_DOCKER_TESTS=1 after building green-taxi:local.",
    ),
]


def test_image_defaults_to_nonroot_user():
    """The production image must not need root to run Python or the batch."""
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "green-taxi:local",
            "python",
            "-c",
            "import os; print(os.getuid())",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert int(result.stdout.strip()) != 0


def test_container_rebuilds_without_appending(tmp_path):
    """Use the host UID for writable temporary bind storage; app logic is unchanged."""
    settings = PipelineSettings.from_file(data_dir=tmp_path)
    settings.raw_dir.mkdir()
    for month in settings.months:
        pd.DataFrame(
            {
                "lpep_pickup_datetime": [settings.reporting_period.start],
                "total_amount": [1.25],
            }
        ).to_parquet(settings.raw_dir / f"green_tripdata_{month}.parquet", index=False)
    command = [
        "docker",
        "run",
        "--rm",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--env",
        "UV_CACHE_DIR=/tmp/uv-cache",
        "--volume",
        f"{tmp_path}:/data",
        "green-taxi:local",
    ]
    previous = None
    previous_facts = None
    for _ in range(2):
        subprocess.run(command, check=True, capture_output=True, text=True, timeout=120)
        metadata = json.loads((settings.processed_dir / "metadata.json").read_text())
        assert metadata["counts"]["rows_used"] == len(settings.months)
        assert not any(item["downloaded"] for item in metadata["inputs"])
        facts = pd.read_parquet(settings.processed_dir / "fact_daily_revenue.parquet")
        assert facts.to_dict("records") == [
            {
                "service_date": settings.reporting_period.start,
                "trip_count": len(settings.months),
                "daily_revenue": Decimal("1.25") * len(settings.months),
            }
        ]
        assert Decimal(metadata["total_amount"]) == facts.daily_revenue.iloc[0]
        if previous:
            assert metadata["run_id"] != previous
            assert_frame_equal(previous_facts, facts)
        previous = metadata["run_id"]
        previous_facts = facts
