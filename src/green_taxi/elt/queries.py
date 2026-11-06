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
    """Return the SQL kind label for an original Arrow column type.

    Preserve boolean and timezone distinctions; unsupported types raise
    ValueError. This helper performs no I/O or conversion of ride values.
    """
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
    """Run SQL and adapt its result to the shared report contract.

    Borrow a connection owned by TaxiWarehouse. The warehouse controls its
    transaction and rollback; this object neither opens nor closes connections.
    """

    def __init__(self, connection):
        """Borrow the context-owned connection; do not open or close another one."""
        self.connection = connection

    @staticmethod
    def sql(name: str) -> str:
        """Return SQL text from a trusted packaged resource name.

        Names are internal constants, not user input. Missing resources and read
        failures propagate; no SQL is executed by this method.
        """
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
        Temporary classifications are dropped on success; the caller must roll
        back the transaction on failure. No report files are written here.
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
        """Return RevenueResult from temporary classifications for this period.

        Reject sub-cents or no usable rows before adapting final facts. SQL owns
        aggregation; Python converts integer cents to Decimal and builds the
        shared calendar. SQL, conversion and validation failures propagate.
        """
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
