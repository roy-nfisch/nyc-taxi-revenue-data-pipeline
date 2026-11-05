"""Execute packaged SQL on one warehouse connection; return checked report values."""

from importlib.resources import files

import pandas as pd
import pyarrow as pa

from green_taxi.reporting import (
    COUNT_FIELDS,
    RevenueResult,
    build_date_dimension,
    decimal_amount,
)


def column_kind(kind: pa.DataType) -> str:
    """Describe an original Arrow column without changing its values."""
    if pa.types.is_boolean(kind):
        return "boolean"
    if pa.types.is_null(kind):
        return "null"
    if pa.types.is_timestamp(kind):
        return "timestamp_aware" if kind.tz else "timestamp_naive"
    if pa.types.is_date(kind):
        return "date"
    if pa.types.is_string(kind) or pa.types.is_large_string(kind):
        return "text"
    if (
        pa.types.is_integer(kind)
        or pa.types.is_floating(kind)
        or pa.types.is_decimal128(kind)
    ):
        return "numeric"
    raise ValueError(f"Unsupported required-column type: {kind}")


class WarehouseQueries:
    """Run classification and aggregation without copying the SQL rules into Python."""

    def __init__(self, connection):
        """Borrow the context-owned connection; do not open or close another one."""
        self.connection = connection

    @staticmethod
    def sql(name: str) -> str:
        """Read a trusted packaged resource, independent of the current directory."""
        return (
            files("green_taxi.elt")
            .joinpath(f"sql/{name}.sql")
            .read_text(encoding="utf-8")
        )

    def classify(self, table: str, schema: pa.Schema, period) -> None:
        """Append one selected source's classifications to temporary run storage.

        table must already be validated and quoted by the warehouse. Values are
        bound separately. The caller owns a transaction and clears temporary data.
        """
        parameters = dict(
            period_start=period.start,
            period_end=period.end,
            pickup_kind=column_kind(schema.field("lpep_pickup_datetime").type),
            amount_kind=column_kind(schema.field("total_amount").type),
        )
        query = self.sql("classify").replace("__SOURCE_TABLE__", table)
        self.connection.execute(
            "INSERT INTO _elt_classified " + query, parameters
        ).fetchall()

    def calculate(self, sources, period) -> RevenueResult:
        """Classify selected sources in SQL and adapt exact results for the writer.

        sources contains validated (quoted table, original schema) pairs. No source
        is deduplicated. Sub-cents or no usable rides fail before publication.
        """
        connection = self.connection
        connection.execute(self.sql("normalize_money"))
        connection.execute(
            "CREATE TEMP TABLE _elt_classified (service_date DATE, status VARCHAR, amount_cents BIGNUM)"
        )
        for table, schema in sources:
            self.classify(table, schema, period)
        result = self.result(period)
        connection.execute("DROP TABLE _elt_classified")
        return result

    def result(self, period) -> RevenueResult:
        """Validate counters first, then convert final facts without rounding."""
        prefix = "WITH classified AS (SELECT * FROM _elt_classified) "
        values = self.connection.execute(prefix + self.sql("counts")).fetchone()
        counts = dict(zip(COUNT_FIELDS, map(int, values[:-1]), strict=True))
        if values[-1]:
            raise ValueError("Sub-cent total_amount requires a reviewed rounding rule.")
        if not counts["rows_used"]:
            raise ValueError("No usable rides in the reporting period.")
        rows = self.connection.execute(prefix + self.sql("daily_revenue")).fetchall()
        facts = pd.DataFrame(
            [
                (day, int(count), decimal_amount(int(cents)))
                for day, count, cents in rows
            ],
            columns=["service_date", "trip_count", "daily_revenue"],
        )
        total = self.connection.execute(
            "SELECT sum(amount_cents) FROM _elt_classified WHERE status = 'accepted'"
        ).fetchone()[0]
        return RevenueResult(
            facts, build_date_dimension(period), counts, decimal_amount(int(total))
        )
