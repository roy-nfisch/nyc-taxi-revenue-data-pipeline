"""Opt-in image checks using tiny local inputs, never the public taxi download."""

import json
import os
import subprocess
from decimal import Decimal
from uuid import uuid4

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


@pytest.fixture
def custom_config(tmp_path):
    """Prepare a synthetic month separate from packaged defaults and production data."""
    config = tmp_path / "run.json"
    values = {
        "months": ["2032-02"],
        "reporting_period": {
            "start_month": "2032-02",
            "duration_months": 1,
        },
    }
    config.write_text(json.dumps(values))
    settings = PipelineSettings.model_validate(
        values | {"data_dir": tmp_path / "storage"}
    )
    settings.raw_dir.mkdir(parents=True)
    pd.DataFrame(
        {
            "lpep_pickup_datetime": [settings.reporting_period.start],
            "total_amount": [1.25],
        }
    ).to_parquet(settings.raw_dir / "green_tripdata_2032-02.parquet", index=False)
    return config, settings, values


@pytest.fixture
def compose_batch(custom_config):
    """Isolate the Compose project and bind temporary data; remove its network after."""
    config, settings, _ = custom_config
    base = [
        "docker",
        "compose",
        "-p",
        f"taxi-config-test-{uuid4().hex}",
        "-f",
        "compose.yaml",
        "-f",
        "compose.config.yaml",
    ]
    env = os.environ | {"TAXI_CONFIG_FILE": str(config)}
    run = base + [
        "run",
        "--rm",
        "--no-deps",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "-e",
        "UV_CACHE_DIR=/tmp/uv-cache",
        "-v",
        f"{settings.data_dir}:/data",
        "pipeline",
    ]
    try:
        yield run, env
    finally:
        subprocess.run(
            base + ["down"],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )


def test_compose_custom_config_is_used_and_cannot_be_written(
    custom_config, compose_batch
):
    """Run a custom February batch and verify the mounted file rejects writes."""
    config, settings, values = custom_config
    run, env = compose_batch
    probe = """
import errno
from pathlib import Path
p = Path('/run-config/run.json')
assert p.is_file()
try:
    p.write_text('must not change')
except OSError as error:
    assert error.errno == errno.EROFS
else:
    raise AssertionError('Config was writable')
"""
    subprocess.run(
        run + ["python", "-c", probe],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    subprocess.run(
        run, env=env, check=True, capture_output=True, text=True, timeout=120
    )
    metadata = json.loads((settings.processed_dir / "metadata.json").read_text())
    assert metadata["reporting_period"]["start_month"] == "2032-02"
    assert metadata["outputs"]["dim_date.parquet"]["rows"] == 29
    assert metadata["counts"]["rows_used"] == 1
    assert json.loads(config.read_text()) == values


def test_missing_custom_file_is_not_created(custom_config, compose_batch):
    """A typo fails at mount setup; Docker must not create an empty host directory."""
    config, settings, _ = custom_config
    run, env = compose_batch
    config.unlink()
    result = subprocess.run(run, env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode != 0
    assert not config.exists()
    assert not settings.processed_dir.exists()
