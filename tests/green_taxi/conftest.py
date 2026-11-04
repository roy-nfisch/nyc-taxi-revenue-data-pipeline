"""Small real Parquet payloads and an HTTP double; no live network in tests."""

from datetime import datetime
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from green_taxi.download import TaxiDownloader
from green_taxi.settings import PipelineSettings


@pytest.fixture
def parquet_bytes():
    """Return a builder for tiny Parquet files with optional missing columns."""

    def build(amount=10.5, missing=None, empty=False):
        values = {
            "lpep_pickup_datetime": [datetime(2025, 1, 1)],
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
def staging(tmp_path, parquet_bytes):
    """Return downloader, fake session, and response with real readable bytes."""
    session = MagicMock()
    response = session.get.return_value.__enter__.return_value
    response.iter_content.side_effect = lambda **kwargs: iter([b"", parquet_bytes()])
    settings = PipelineSettings(data_dir=tmp_path, months=("2025-01",))
    return TaxiDownloader(settings, session), session, response
