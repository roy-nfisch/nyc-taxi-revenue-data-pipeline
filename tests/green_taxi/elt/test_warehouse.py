"""Prove raw loading preserves bytes, schemas, duplicate rows and safe reruns."""

import hashlib
from dataclasses import replace
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from green_taxi.download import StagedFile
from green_taxi.elt.warehouse import TaxiWarehouse

pytestmark = [pytest.mark.integration, pytest.mark.data]
pytest.importorskip("duckdb")


@pytest.fixture
def warehouse(tmp_path):
    """Open one isolated local warehouse and always close its connection."""
    with TaxiWarehouse(tmp_path / "elt" / "warehouse.duckdb") as warehouse:
        yield warehouse


@pytest.fixture
def staged(tmp_path, parquet_bytes, sample_months):
    """Reuse the shared tiny source builder, with optional custom Arrow tables."""

    def write(month=sample_months[0], *, table=None, content=None):
        path = tmp_path / f"green_tripdata_{month}.parquet"
        if table is not None:
            pq.write_table(table, path)
        else:
            path.write_bytes(content if content is not None else parquet_bytes())
        payload = path.read_bytes()
        return StagedFile(
            month,
            path,
            "https://example.invalid/source",
            len(payload),
            hashlib.sha256(payload).hexdigest(),
            False,
        )

    return write


def registry(warehouse):
    """Read all source metadata in a stable order for transaction assertions."""
    return warehouse._connection.execute(
        "SELECT * FROM loaded_sources ORDER BY source_id"
    ).fetchall()


def test_context_has_no_constructor_io_and_closes_on_failure(tmp_path):
    """The connection is bounded by the context, including exceptional exits."""
    path = tmp_path / "not-created" / "warehouse.duckdb"
    warehouse = TaxiWarehouse(path)
    assert not path.parent.exists()
    with pytest.raises(RuntimeError, match="context"):
        warehouse.load([])
    with pytest.raises(ValueError, match="intentional"):
        with warehouse:
            connection = warehouse._connection
            raise ValueError("intentional")
    assert warehouse._connection is None
    with pytest.raises(Exception, match="closed"):
        connection.execute("SELECT 1")
    with warehouse:
        with pytest.raises(RuntimeError, match="already open"):
            warehouse.__enter__()


def test_load_preserves_duplicates_metadata_and_unusual_column_names(warehouse, staged):
    """No bookkeeping column overwrites source data and no duplicate disappears."""
    schema = pa.schema(
        [
            pa.field(
                "lpep_pickup_datetime",
                pa.timestamp("ns"),
                nullable=False,
                metadata={b"meaning": b"recorded pickup"},
            ),
            pa.field("total_amount", pa.float64()),
            pa.field("source_id; DROP TABLE loaded_sources;--", pa.string()),
        ],
        metadata={b"source": b"original"},
    )
    table = pa.Table.from_arrays(
        [
            pa.array([1, 1, None], type=pa.timestamp("ns")),
            pa.array([10.5, 10.5, None]),
            pa.array(["same", "same", "other"]),
        ],
        schema=schema,
    )
    # Parquet enforces the original field's non-nullability.
    table = table.slice(0, 2)
    item = staged(table=table)
    warehouse.load([item])
    original = pq.read_schema(item.path)
    record = registry(warehouse)[0]
    assert record[:5] == (
        item.path.name,
        "source_2031_01",
        item.sha256,
        item.size_bytes,
        2,
    )
    assert pa.ipc.read_schema(pa.BufferReader(record[5])).equals(
        original, check_metadata=True
    )
    actual = warehouse._connection.sql('FROM "raw"."source_2031_01"').to_arrow_table()
    assert actual.cast(original).equals(table, check_metadata=True)


def test_unchanged_reuses_and_changed_replaces_without_append(
    warehouse, staged, monkeypatch
):
    """The checksum cache is source-scoped, and replacements never append old rows."""
    item = staged()
    warehouse.load([item])
    previous = registry(warehouse)
    original_replace = warehouse._replace_source

    def unexpected(*args):
        raise AssertionError("unchanged source was rewritten")

    monkeypatch.setattr(warehouse, "_replace_source", unexpected)
    warehouse.load([replace(item, downloaded=True)])
    assert registry(warehouse) == previous
    monkeypatch.setattr(warehouse, "_replace_source", original_replace)
    changed = staged(
        table=pa.table(
            {
                "lpep_pickup_datetime": ["2031-01-01", "invalid", "2031-01-01"],
                "total_amount": ["1.23", "bad", "1.23"],
            }
        )
    )
    warehouse.load([changed])
    assert registry(warehouse)[0][2:5] == (changed.sha256, changed.size_bytes, 3)
    assert warehouse._connection.execute(
        "SELECT count(*) FROM raw.source_2031_01"
    ).fetchone() == (3,)


def test_equal_bytes_in_distinct_source_files_both_contribute(warehouse, staged):
    """Identical bytes do not collapse different filenames into a global cache."""
    first = staged()
    second = staged("2031-02", content=first.path.read_bytes())
    assert first.sha256 == second.sha256
    warehouse.load([first, second])
    assert len(registry(warehouse)) == 2
    assert warehouse._connection.execute(
        "SELECT (SELECT count(*) FROM raw.source_2031_01) + "
        "(SELECT count(*) FROM raw.source_2031_02)"
    ).fetchone() == (2,)


def test_mixed_source_types_keep_boolean_and_aware_distinctions(warehouse, staged):
    """Invalid row types load unchanged, including timezone-aware nanoseconds."""
    numeric = staged()
    boolean = staged(
        "2031-02",
        table=pa.table(
            {
                "lpep_pickup_datetime": ["2031-02-01", "invalid"],
                "total_amount": [True, False],
            }
        ),
    )
    aware = staged(
        "2031-03",
        table=pa.table(
            {
                "lpep_pickup_datetime": pa.array(
                    [1, None], type=pa.timestamp("ns", "Europe/Berlin")
                ),
                "total_amount": [Decimal("1.20"), None],
            }
        ),
    )
    warehouse.load([numeric, boolean, aware])
    assert len(registry(warehouse)) == 3
    assert (
        warehouse._connection.sql("FROM raw.source_2031_02")
        .to_arrow_table()
        .schema.field("total_amount")
        .type
        == pa.bool_()
    )
    stored = warehouse._connection.sql("FROM raw.source_2031_03").to_arrow_table()
    assert stored.schema.field("lpep_pickup_datetime").type.tz is not None
    assert stored.cast(pq.read_schema(aware.path)).equals(pq.read_table(aware.path))


@pytest.mark.parametrize(
    "extra",
    [
        pa.array([None, None], type=pa.null()),
        pa.array([2**64 - 1, None], type=pa.uint64()),
        pa.array([float("nan"), -0.0], type=pa.float32()),
        pa.array([b"\x00\xff", None], type=pa.binary()),
        pa.array([b"ab", None], type=pa.binary(2)),
        pa.array(["long", None], type=pa.large_string()),
        pa.array([0, None], type=pa.date32()),
        pa.array([Decimal("12345678901234567890.12"), None], type=pa.decimal128(24, 2)),
    ],
)
def test_supported_extra_scalar_columns_round_trip(warehouse, staged, extra):
    """Extra raw fields retain integer limits, binary, null and exact decimal values."""
    item = staged(
        table=pa.table(
            {
                "lpep_pickup_datetime": ["2031-01-01", "invalid"],
                "total_amount": [1.2, None],
                "extra": extra,
            }
        )
    )
    warehouse.load([item])
    assert registry(warehouse)[0][4] == 2


@pytest.mark.parametrize("change", ["checksum", "size", "filename", "month"])
def test_invalid_staged_identity_makes_no_source_changes(warehouse, staged, change):
    """Reject stale bytes and unsafe identities rather than trusting a filename."""
    item = staged()
    changes = {
        "checksum": {"sha256": "a" * 64},
        "size": {"size_bytes": item.size_bytes + 1},
        "filename": {"path": item.path.with_name("unsafe;drop.parquet")},
        "month": {"month": "2031-01; DROP TABLE loaded_sources"},
    }
    with pytest.raises(ValueError):
        warehouse.load([replace(item, **changes[change])])
    assert registry(warehouse) == []


def test_reused_source_still_checks_current_file_bytes(warehouse, staged):
    """A registered checksum never permits changed local bytes to pass validation."""
    item = staged()
    warehouse.load([item])
    before = registry(warehouse)
    item.path.write_bytes(item.path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="Staged bytes changed"):
        warehouse.load([item])
    assert registry(warehouse) == before


def test_duplicate_batch_identity_and_resolved_path_fail_before_loading(
    warehouse, staged, tmp_path
):
    """Reject aliases and repeated source names before committing the first source."""
    first = staged()
    with pytest.raises(ValueError, match="distinct"):
        warehouse.load([first, first])
    alias = tmp_path / "green_tripdata_2031-02.parquet"
    alias.symlink_to(first.path)
    with pytest.raises(ValueError, match="distinct"):
        warehouse.load([first, replace(first, month="2031-02", path=alias)])
    assert registry(warehouse) == []


@pytest.mark.parametrize("failure", ["verification", "registry", "changed_bytes"])
def test_failed_replacement_restores_old_table_and_registry(
    warehouse, staged, monkeypatch, failure
):
    """Failures after table replacement or registry insertion roll back both objects."""
    item = staged()
    warehouse.load([item])
    before = registry(warehouse)
    changed = staged(
        table=pa.table({"lpep_pickup_datetime": ["bad"], "total_amount": [False]})
    )
    original_verify = warehouse._verify_table

    def fail(current, source):
        original_verify(current, source)
        if failure == "verification":
            raise ValueError("Injected lossless verification failure")
        if failure == "registry":
            warehouse._connection.execute("DROP TABLE loaded_sources")
        if failure == "changed_bytes":
            current.path.write_bytes(current.path.read_bytes() + b"changed")

    monkeypatch.setattr(warehouse, "_verify_table", fail)
    with pytest.raises(Exception):
        warehouse.load([changed])
    assert registry(warehouse) == before
    assert warehouse._connection.execute(
        "SELECT total_amount FROM raw.source_2031_01"
    ).fetchone() == (10.5,)


def test_later_failure_stops_batch_but_keeps_completed_source(warehouse, staged):
    """The load boundary is per source, not a misleading all-batch transaction."""
    first = staged()
    second = staged("2031-02", content=b"not parquet")
    third = staged("2031-03")
    with pytest.raises(pa.ArrowException):
        warehouse.load([first, second, third])
    assert [row[0] for row in registry(warehouse)] == [first.path.name]


@pytest.mark.parametrize(
    "table",
    [
        pa.table({"lpep_pickup_datetime": ["2031-01-01"]}),
        pa.table(
            {
                "lpep_pickup_datetime": pa.array([], type=pa.string()),
                "total_amount": pa.array([], type=pa.float64()),
            }
        ),
        pa.table({"lpep_pickup_datetime": ["2031-01-01"], "total_amount": [[1.0]]}),
        pa.table({"lpep_pickup_datetime": ["2031-01-01"], "total_amount": [b"1.20"]}),
        pa.table(
            {
                "lpep_pickup_datetime": ["2031-01-01"],
                "total_amount": [1],
                "TOTAL_AMOUNT": [2],
            }
        ),
    ],
)
def test_invalid_or_unsupported_schema_fails_explicitly(warehouse, staged, table):
    """No missing field, nested value or ambiguous native name is silently repaired."""
    with pytest.raises(ValueError):
        warehouse.load([staged(table=table)])
    assert registry(warehouse) == []


def test_lossy_engine_mapping_is_rejected_before_commit(warehouse, staged, monkeypatch):
    """Even a declared scalar type must pass actual value round-trip verification."""
    from green_taxi.elt import warehouse as module

    monkeypatch.setattr(module, "_equal_values", lambda left, right: False)
    with pytest.raises(ValueError, match="Lossy raw value"):
        warehouse.load([staged()])
    assert registry(warehouse) == []
    assert warehouse._connection.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'raw'"
    ).fetchone() == (0,)


def test_empty_input_is_not_a_successful_noop(warehouse):
    """A selected batch must contain at least one actual source."""
    with pytest.raises(ValueError, match="at least one"):
        warehouse.load([])


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
@pytest.mark.parametrize("timezone", [None, "Europe/Berlin"])
def test_timestamp_precision_and_timezone_survive_reopen(
    staged, tmp_path, unit, timezone
):
    """Persistent tables retain instants and type distinctions after reconnecting."""
    table = pa.table(
        {
            "lpep_pickup_datetime": pa.array(
                [1, None], type=pa.timestamp(unit, timezone)
            ),
            "total_amount": pa.array([None, None], type=pa.null()),
        }
    )
    item = staged(table=table)
    path = tmp_path / "persisted.duckdb"
    with TaxiWarehouse(path) as first:
        first.load([item])
        before = registry(first)
    with TaxiWarehouse(path) as reopened:
        reopened.load([item])
        assert registry(reopened) == before


def test_missing_cached_table_is_not_reported_as_reused(warehouse, staged):
    """Registry metadata alone cannot hide missing or externally damaged storage."""
    item = staged()
    warehouse.load([item])
    warehouse._connection.execute("DROP TABLE raw.source_2031_01")
    with pytest.raises(Exception, match="does not exist"):
        warehouse.load([item])
