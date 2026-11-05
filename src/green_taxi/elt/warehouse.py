"""Load checked source files unchanged into a local, optional DuckDB warehouse."""

import hashlib
import logging
import re
from collections.abc import Iterable
from importlib.resources import files
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from green_taxi.download import REQUIRED_COLUMNS, StagedFile

logger = logging.getLogger("green_taxi.elt.warehouse")
_MONTH = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")
_CHECKSUM = re.compile(r"[0-9a-f]{64}")


def _raw_identifier(month: str) -> str:
    """Return a quoted raw table identifier only for a validated source month."""
    if not _MONTH.fullmatch(month) or month.startswith("0000"):
        raise ValueError(f"Invalid source month: {month!r}")
    return f'"raw"."source_{month.replace("-", "_")}"'


def _supported_type(kind: pa.DataType) -> bool:
    """Limit loading to scalar types whose values we can check without guessing."""
    checks = (
        pa.types.is_null,
        pa.types.is_boolean,
        pa.types.is_integer,
        pa.types.is_floating,
        pa.types.is_string,
        pa.types.is_large_string,
        pa.types.is_binary,
        pa.types.is_large_binary,
        pa.types.is_fixed_size_binary,
        pa.types.is_date,
        pa.types.is_timestamp,
        pa.types.is_decimal128,
    )
    return any(check(kind) for check in checks)


def _validate_schema(schema: pa.Schema) -> None:
    """Reject missing, ambiguous or unsupported columns before changing storage."""
    names = schema.names
    if not REQUIRED_COLUMNS.issubset(names):
        raise ValueError("Input needs pickup datetime and total_amount columns.")
    if len({name.casefold() for name in names}) != len(names) or any(
        not name or "\0" in name for name in names
    ):
        raise ValueError("Input column names must be nonempty and unambiguous.")
    for field in schema:
        if not _supported_type(field.type):
            raise ValueError(f"Unsupported raw type for {field.name}: {field.type}")
        kind = field.type
        numeric = pa.types.is_integer(kind) or pa.types.is_floating(kind)
        numeric = numeric or pa.types.is_decimal128(kind)
        common = numeric or pa.types.is_boolean(kind) or pa.types.is_null(kind)
        common = common or pa.types.is_string(kind) or pa.types.is_large_string(kind)
        pickup = pa.types.is_date(kind) or pa.types.is_timestamp(kind)
        if field.name in REQUIRED_COLUMNS and not (
            common or (field.name == "lpep_pickup_datetime" and pickup)
        ):
            raise ValueError(f"Unsupported required-column type: {field.name}: {kind}")


def _verify_bytes(item: StagedFile, content: bytes) -> None:
    """Require the exact byte identity supplied by the downloader."""
    if (
        len(content) != item.size_bytes
        or hashlib.sha256(content).hexdigest() != item.sha256
    ):
        raise ValueError(f"Staged bytes changed: {item.path.name}")


def _equal_values(left: pa.Table, right: pa.Table) -> bool:
    """Compare every value, including duplicates and floating-point NaN bits."""
    if left.num_rows != right.num_rows or left.schema != right.schema:
        return False
    for field, first, second in zip(left.schema, left.columns, right.columns):
        first, second = first.combine_chunks(), second.combine_chunks()
        if pa.types.is_floating(field.type):
            bits = {16: pa.uint16(), 32: pa.uint32(), 64: pa.uint64()}
            first = first.view(bits[field.type.bit_width])
            second = second.view(bits[field.type.bit_width])
        if not first.equals(second):
            return False
    return True


class TaxiWarehouse:
    """Own one connection and transactional source loads, without cleaning rides.

    Construction performs no I/O. Enter the context before loading and use one
    writer per database. Each source commits separately; a later failure stops
    the batch but does not remove earlier successful source loads.
    """

    def __init__(self, database_path: Path) -> None:
        """Store the database location without importing the optional engine."""
        self.database_path = Path(database_path)
        self._connection = None

    def __enter__(self):
        """Open and initialise the database, closing it if initialisation fails."""
        if self._connection is not None:
            raise RuntimeError("Warehouse context is already open.")
        import duckdb

        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(self.database_path))
        try:
            connection.execute("SET preserve_insertion_order = true")
            connection.execute(
                files("green_taxi.elt").joinpath("sql/schema.sql").read_text()
            )
        except BaseException:
            connection.close()
            raise
        self._connection = connection
        return self

    def __exit__(self, exception_type, exception, traceback) -> None:
        """Close the connection after success or failure; never swallow errors."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def load(self, inputs: Iterable[StagedFile]) -> None:
        """Verify and register all selected sources, or propagate the first failure.

        Reject duplicate identities and paths before any source changes. Equal
        bytes in distinct source files remain distinct. Matching sources are
        checked and reused; changed sources replace their table and registry
        together. Unsupported or lossy schemas fail rather than being coerced.
        """
        if self._connection is None:
            raise RuntimeError("Enter the warehouse context before loading.")
        for item in self._validate_inputs(inputs):
            try:
                self._load_source(item)
            except Exception:
                logger.exception("Raw loading failed for %s", item.path.name)
                raise

    @staticmethod
    def _validate_inputs(inputs: Iterable[StagedFile]) -> tuple[StagedFile, ...]:
        """Check all source names, metadata and aliases before any database writes."""
        selected = tuple(inputs)
        if not selected:
            raise ValueError("Loading requires at least one staged source.")
        identities, paths = set(), set()
        for item in selected:
            _raw_identifier(item.month)
            expected = f"green_tripdata_{item.month}.parquet"
            if item.path.name != expected:
                raise ValueError(f"Source filename does not match month: {expected}")
            if item.size_bytes <= 0 or not _CHECKSUM.fullmatch(item.sha256):
                raise ValueError(f"Invalid staged identity: {expected}")
            resolved = item.path.resolve(strict=True)
            if expected in identities or resolved in paths:
                raise ValueError(
                    "Loading requires distinct source identities and paths."
                )
            identities.add(expected)
            paths.add(resolved)
        return selected

    def calculate(self, inputs, period):
        """Return SQL results for exactly these checked sources, without publishing.

        Require a matching registry entry and unchanged staged bytes. A read
        transaction keeps all selected tables consistent; query failures roll back.
        """
        from green_taxi.elt.queries import WarehouseQueries

        if self._connection is None:
            raise RuntimeError("Enter the warehouse context before calculating.")
        selected = self._validate_inputs(inputs)
        connection = self._connection
        connection.execute("BEGIN TRANSACTION")
        try:
            sources = []
            for item in selected:
                _verify_bytes(item, item.path.read_bytes())
                rows = connection.execute(
                    "SELECT sha256, size_bytes, arrow_schema FROM loaded_sources WHERE source_id = ?",
                    [item.path.name],
                ).fetchall()
                if not rows or rows[0][:2] != (item.sha256, item.size_bytes):
                    raise ValueError(
                        f"Source is not loaded with this identity: {item.path.name}"
                    )
                schema = pa.ipc.read_schema(pa.BufferReader(rows[0][2]))
                sources.append((_raw_identifier(item.month), schema))
            result = WarehouseQueries(connection).calculate(sources, period)
            connection.execute("COMMIT")
            return result
        except BaseException:
            connection.execute("ROLLBACK")
            raise

    def _load_source(self, item: StagedFile) -> None:
        """Read one checked snapshot, then reuse or transactionally replace it."""
        content = item.path.read_bytes()
        _verify_bytes(item, content)
        source = pq.read_table(pa.BufferReader(content))
        _validate_schema(source.schema)
        if source.num_rows == 0:
            raise ValueError(f"Input contains no ride records: {item.path.name}")
        metadata = (
            f"source_{item.month.replace('-', '_')}",
            item.sha256,
            item.size_bytes,
            source.num_rows,
            source.schema.serialize().to_pybytes(),
        )
        records = self._connection.execute(
            "SELECT raw_table, sha256, size_bytes, row_count, arrow_schema "
            "FROM loaded_sources WHERE source_id = ?",
            [item.path.name],
        ).fetchall()
        existing = records[0] if records else None
        if existing == metadata:
            self._verify_table(item, source)
            _verify_bytes(item, item.path.read_bytes())
            logger.info("Reused %s (%s rows)", item.path.name, source.num_rows)
            return
        self._replace_source(item, source, metadata)
        logger.info("Loaded %s (%s rows)", item.path.name, source.num_rows)

    def _replace_source(self, item: StagedFile, source: pa.Table, metadata: tuple):
        """Publish the source table and registry atomically, rolling back failures."""
        connection = self._connection
        connection.execute("BEGIN TRANSACTION")
        registered = False
        try:
            connection.register("_staged_source", source)
            registered = True
            connection.execute(
                f"CREATE OR REPLACE TABLE {_raw_identifier(item.month)} "
                'AS SELECT * FROM "_staged_source"'
            )
            self._verify_table(item, source)
            connection.execute(
                "DELETE FROM loaded_sources WHERE source_id = ?", [item.path.name]
            )
            connection.execute(
                "INSERT INTO loaded_sources VALUES (?, ?, ?, ?, ?, ?)",
                (item.path.name, *metadata),
            )
            _verify_bytes(item, item.path.read_bytes())
            connection.execute("COMMIT")
        except BaseException:
            connection.execute("ROLLBACK")
            raise
        finally:
            if registered:
                connection.unregister("_staged_source")

    def _verify_table(self, item: StagedFile, source: pa.Table) -> None:
        """Prove native storage preserves source values and required type distinctions."""
        stored = self._connection.sql(
            f"SELECT * FROM {_raw_identifier(item.month)}"
        ).to_arrow_table()
        for original, native in zip(source.schema, stored.schema):
            if pa.types.is_timestamp(original.type) and (
                not pa.types.is_timestamp(native.type)
                or bool(original.type.tz) != bool(native.type.tz)
            ):
                raise ValueError(f"Lossy timestamp mapping: {original.name}")
            if pa.types.is_boolean(original.type) and not pa.types.is_boolean(
                native.type
            ):
                raise ValueError(f"Lossy boolean mapping: {original.name}")
        try:
            restored = stored.cast(source.schema, safe=True)
        except (pa.ArrowException, ValueError) as error:
            raise ValueError(f"Lossy raw schema mapping: {item.path.name}") from error
        if not _equal_values(source, restored):
            raise ValueError(f"Lossy raw value mapping: {item.path.name}")
