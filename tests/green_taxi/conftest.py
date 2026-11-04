"""Small real Parquet payloads and an HTTP double; no live network in tests."""

import logging
from datetime import datetime
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from green_taxi.download import TaxiDownloader
from green_taxi.settings import PipelineSettings


@pytest.fixture
def download_logs(caplog):
    """Capture downloader logs independently of previous CLI logger configuration."""
    logger = logging.getLogger("green_taxi.download")
    previous = logger.level, logger.propagate
    logger.addHandler(caplog.handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)
        logger.setLevel(previous[0])
        logger.propagate = previous[1]


@pytest.fixture
def sample_months():
    """Central synthetic months, independent of the production download selection."""
    return ("2031-01", "2031-02", "2031-03")


@pytest.fixture
def sample_filename(sample_months):
    """Name of the first synthetic source file used in staging tests."""
    return f"green_tripdata_{sample_months[0]}.parquet"


@pytest.fixture
def parquet_bytes(sample_months):
    """Return a builder for tiny Parquet files with optional missing columns."""

    def build(amount=10.5, missing=None, empty=False):
        values = {
            "lpep_pickup_datetime": [datetime.fromisoformat(f"{sample_months[0]}-01")],
            "total_amount": [amount],
        }
        if missing:
            values.pop(missing)
        table = pa.table(values)
        if empty:
            table = table.slice(0, 0)
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink)
        return sink.getvalue().to_pybytes()

    return build


@pytest.fixture
def staging(tmp_path, parquet_bytes, sample_months):
    """Return downloader, fake session, and response with real readable bytes."""
    session = MagicMock()
    response = session.get.return_value.__enter__.return_value
    response.iter_content.side_effect = lambda **kwargs: iter([b"", parquet_bytes()])
    settings = PipelineSettings(data_dir=tmp_path, months=(sample_months[0],))
    return TaxiDownloader(settings, session), session, response


@pytest.fixture
def reporting_settings(tmp_path):
    """Use the agreed reporting period; isolate all input files in temporary storage."""
    return PipelineSettings.from_file(data_dir=tmp_path)


@pytest.fixture
def trip_file(tmp_path):
    """Return a writer for small real Parquet inputs, not a duplicate course dataset."""
    import pandas as pd

    def write(name, pickups, amounts):
        destination = tmp_path / name
        pd.DataFrame(
            {
                "lpep_pickup_datetime": pickups,
                "total_amount": amounts,
            }
        ).to_parquet(destination, index=False)
        return destination

    return write
