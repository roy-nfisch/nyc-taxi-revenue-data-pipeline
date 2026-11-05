"""Check the optional registry contract in memory; no taxi loading or SQL rules."""

from importlib.resources import files

import pyarrow as pa
import pytest

pytestmark = [pytest.mark.integration, pytest.mark.data]


@pytest.fixture
def database():
    """Create and close an isolated engine; skip when the extra is not installed."""
    duckdb = pytest.importorskip("duckdb")
    with duckdb.connect(":memory:") as connection:
        schema = files("green_taxi.elt").joinpath("sql/schema.sql").read_text()
        connection.execute(schema)
        yield connection


@pytest.fixture
def source_record():
    """Provide a registry row, including original Arrow type metadata, not rides."""
    schema = pa.schema(
        [
            ("lpep_pickup_datetime", pa.timestamp("ns", tz="Europe/Berlin")),
            ("total_amount", pa.bool_()),
        ]
    )
    return (
        "green_tripdata_2031-01.parquet",
        "source_2031_01",
        "a" * 64,
        100,
        2,
        schema.serialize().to_pybytes(),
    )


def test_registry_retains_schema_and_is_safe_to_initialise_again(
    database, source_record
):
    """Reapplying DDL preserves registered identities and the original Arrow schema."""
    database.execute(
        "INSERT INTO loaded_sources VALUES (?, ?, ?, ?, ?, ?)", source_record
    )
    database.execute(files("green_taxi.elt").joinpath("sql/schema.sql").read_text())
    assert database.execute("SELECT * FROM loaded_sources").fetchall() == [
        source_record
    ]


@pytest.mark.parametrize(
    "index,value", [(0, ""), (1, "unsafe;drop"), (2, "bad"), (3, 0), (4, -1), (5, b"")]
)
def test_registry_rejects_invalid_identity_metadata(
    database, source_record, index, value
):
    """Invalid metadata must fail visibly rather than registering an unusable source."""
    duckdb = pytest.importorskip("duckdb")
    values = list(source_record)
    values[index] = value
    with pytest.raises(duckdb.Error):
        database.execute("INSERT INTO loaded_sources VALUES (?, ?, ?, ?, ?, ?)", values)


def test_different_sources_may_share_a_checksum(database, source_record):
    """Content equality must not collapse distinct source-file identities."""
    database.execute(
        "INSERT INTO loaded_sources VALUES (?, ?, ?, ?, ?, ?)", source_record
    )
    other = ("green_tripdata_2031-02.parquet", "source_2031_02", *source_record[2:])
    database.execute("INSERT INTO loaded_sources VALUES (?, ?, ?, ?, ?, ?)", other)
    assert database.execute("SELECT count(*) FROM loaded_sources").fetchone() == (2,)


@pytest.mark.parametrize("index", [0, 1])
def test_registry_requires_distinct_source_and_table(database, source_record, index):
    """Only the source's own registry entry may point at its raw table."""
    duckdb = pytest.importorskip("duckdb")
    database.execute(
        "INSERT INTO loaded_sources VALUES (?, ?, ?, ?, ?, ?)", source_record
    )
    duplicate = list(source_record)
    duplicate[1 - index] = (
        "source_2031_02" if index == 0 else "green_tripdata_2031-02.parquet"
    )
    with pytest.raises(duckdb.ConstraintException):
        database.execute(
            "INSERT INTO loaded_sources VALUES (?, ?, ?, ?, ?, ?)", duplicate
        )
