"""Execute packaged transformation SQL against raw tables, without a loader."""

import re
from datetime import date, datetime, timezone
from decimal import Decimal
from importlib.resources import files
from itertools import product

import pytest

from green_taxi.etl.revenue import DailyRevenueCalculator, recorded_date
from green_taxi.reporting import COUNT_FIELDS, recorded_cents
from green_taxi.settings import ReportingPeriod

pytestmark = [pytest.mark.integration, pytest.mark.data]
PERIOD = ReportingPeriod(start_month="2025-01", duration_months=1)


def sql_resource(name):
    """Read installed resources rather than depending on the working directory."""
    return files("green_taxi.elt").joinpath(f"sql/{name}.sql").read_text()


def classification_query(table):
    """Model the coordinator's mandatory allowlist/quoting boundary."""
    if not re.fullmatch(r"source_[0-9]{4}_(0[1-9]|1[0-2])", table):
        raise ValueError("Invalid raw table identifier.")
    return sql_resource("classify").replace("__SOURCE_TABLE__", f'"raw"."{table}"')


@pytest.fixture
def sql_database():
    """Use the optional engine in memory and close it after each test."""
    duckdb = pytest.importorskip("duckdb")
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE SCHEMA raw")
        yield connection


def create_source(
    connection, rows, pickup_type="VARCHAR", amount_type="VARCHAR", month="01"
):
    """Create tiny raw tables directly; SQL types here are trusted test constants."""
    table = f"source_2025_{month}"
    connection.execute(
        f'CREATE TABLE raw."{table}" ('
        f"lpep_pickup_datetime {pickup_type}, total_amount {amount_type})"
    )
    if rows:
        connection.executemany(f'INSERT INTO raw."{table}" VALUES (?, ?)', rows)
    return table


def classify(connection, table, pickup_kind="text", amount_kind="text"):
    """Materialize the common shape for aggregation, never Python ride totals."""
    parameters = dict(
        period_start=PERIOD.start,
        period_end=PERIOD.end,
        pickup_kind=pickup_kind,
        amount_kind=amount_kind,
    )
    connection.execute(
        f'CREATE TEMP TABLE "{table}_classified" AS ' + classification_query(table),
        parameters,
    )
    return connection.execute(f'SELECT * FROM "{table}_classified"').fetchall()


def classified_union(connection, tables):
    """Only union normalized classifications, not differently typed raw columns."""
    body = " UNION ALL ".join(f'SELECT * FROM "{table}_classified"' for table in tables)
    connection.execute("CREATE TEMP VIEW classified AS " + body)


def expected_row(pickup, amount):
    """Use the existing ETL scalar rules as the independent row oracle."""
    day = recorded_date(pickup)
    if day is None:
        return None, "invalid_pickup", None
    if not PERIOD.contains(day):
        return day, "outside_period", None
    try:
        cents = recorded_cents(amount)
    except ValueError:
        return day, "sub_cent", None
    return (
        day,
        "invalid_amount" if cents is None else "accepted",
        (None if cents is None else str(cents)),
    )


def test_daily_facts_and_all_counters_match_etl(sql_database, trip_file):
    """Preserve duplicates, negatives, zero and pickup-first exclusion priority."""
    rows = [
        ("2025-01-01", 0.1),
        ("2025-01-01", 0.2),
        ("2025-01-02T12:30:00", -3.5),
        ("2025-01-02", 0.0),
        ("2025-01-02", 0.0),
        ("bad", 0.001),
        ("2024-12-31", 0.001),
        ("2025-02-01", float("nan")),
        ("2025-01-03", float("inf")),
        ("2025-01-03", None),
    ]
    table = create_source(sql_database, rows, amount_type="DOUBLE")
    assert classify(sql_database, table, amount_kind="numeric") == [
        expected_row(*row) for row in rows
    ]
    classified_union(sql_database, [table])
    path = trip_file("sql-parity.parquet", *zip(*rows, strict=True))
    result = DailyRevenueCalculator(PERIOD).calculate([path])
    facts = sql_database.execute(sql_resource("daily_revenue")).fetchall()
    assert facts == [
        (row.service_date, row.trip_count, str(recorded_cents(row.daily_revenue)))
        for row in result.facts.itertuples(index=False)
    ]
    counts = sql_database.execute(sql_resource("counts")).fetchone()
    assert counts == (*[result.counts[name] for name in COUNT_FIELDS], 0)
    assert [row[1:] for row in facts] == [(2, "30"), (3, "-350")]


@pytest.mark.parametrize(
    "pickup_type,pickup_kind,pickup",
    [
        ("DATE", "date", date(2025, 1, 1)),
        ("TIMESTAMP", "timestamp_naive", datetime(2025, 1, 1, 23, 59)),
        ("TIMESTAMP_NS", "timestamp_naive", datetime(2025, 1, 1, 23, 59)),
        ("TIMESTAMPTZ", "timestamp_aware", datetime(2025, 1, 1, tzinfo=timezone.utc)),
        ("BIGINT", "numeric", 1735689600),
        ("BOOLEAN", "boolean", True),
        ("INTEGER", "null", None),
    ],
)
def test_native_pickup_kinds_are_binder_safe(
    sql_database, pickup_type, pickup_kind, pickup
):
    """Unsupported row kinds are exclusions, not binder errors or epoch guesses."""
    rows = [(pickup, 2), (None, 3)]
    table = create_source(sql_database, rows, pickup_type, "INTEGER")
    assert classify(sql_database, table, pickup_kind, "numeric") == [
        expected_row(*row) for row in rows
    ]


@pytest.mark.parametrize(
    "amount_type,amount_kind,amount",
    [
        ("BOOLEAN", "boolean", True),
        ("BOOLEAN", "boolean", False),
        ("INTEGER", "null", None),
        ("BIGINT", "numeric", 2),
        ("DECIMAL(38, 10)", "numeric", Decimal("12345678901234567890.12")),
        ("DOUBLE", "numeric", 0.1),
        ("DOUBLE", "numeric", float("nan")),
        ("DOUBLE", "numeric", float("inf")),
        ("DOUBLE", "numeric", -0.0),
        ("FLOAT", "numeric", 0.5),
    ],
)
def test_native_amount_kinds_preserve_etl_rules(
    sql_database, amount_type, amount_kind, amount
):
    """Never accept a boolean as one/zero, or round a numeric fixed-point value."""
    rows = [("2025-01-01", amount)]
    table = create_source(sql_database, rows, amount_type=amount_type)
    assert classify(sql_database, table, amount_kind=amount_kind) == [
        expected_row(*rows[0])
    ]


def test_float32_is_not_shortened_before_decimal_parsing(sql_database):
    """Python float exposure of an Arrow float32 0.1 is a finite sub-cent value."""
    import pyarrow as pa

    amount = pa.table({"amount": pa.array([0.1], type=pa.float32())}).to_pandas()
    python_amount = next(amount.itertuples(index=False, name=None))[0]
    rows = [("2025-01-01", python_amount)]
    table = create_source(sql_database, rows, amount_type="FLOAT")
    assert (
        classify(sql_database, table, amount_kind="numeric")
        == [expected_row(*rows[0])]
        == [(date(2025, 1, 1), "sub_cent", None)]
    )


def test_iso_text_matches_fromisoformat_without_guessing(sql_database):
    """Basic/week dates, fractional clocks and one-character separators are ISO."""
    pickups = [
        "2025-01-01",
        "20250101",
        "2025-W01",
        "2025W01",
        "2025-W01-3",
        "2025W013",
        "2025-W02-1",
        "2025W021",
        "2025-01-01X12",
        "20250101T1234",
        "2025-01-01T12:34:56.1234567",
        "2025-01-01T12.5",
        "2025-01-01T12:34,5",
        "2025-01-01\n12:34:56",
        "2025-01-01T12345678",
        "2025-01-01T1234567",
        "2025-01-01T12:34:56:1",
        "2025-01-01T12:34:56:123456789",
        "2025-01-01T12:34:56x12",
        "2025-01-01🙂12",
        "2025-01-01+12",
        "2025-W01-3T12:00",
        "2025-01-01T24:00",
        "2025-01-01T12:60",
        "2025-01-01T12:34:60",
        "2025-01-01T12:3456",
        "2025-01-01T1234:56",
        "2025-01-01T1",
        "2025-01-01T",
        "2025-01-01  12",
        " 2025-01-01",
        "2025-01-01 ",
        "2025-01-01T00:00Z",
        "2025-01-01T12:00+00:00",
        "2025-01-01T12:00-00:00",
        "2025-01-01T12:00+01:00:00.5",
        "2025-02-29",
        "2024-02-29",
        "2025-W53-1",
        "2020-W53-7",
        "2025-0001",
        "0000-01-01",
        "0001-01-01",
        "9999-12-31",
        "2025-01-01T12:00:00.",
        "2025-01-01T12:00:00.٢",
        "٢٠٢٥-01-01",
        None,
        "",
        "invalid",
        "01/02/2025",
        "1735689600",
    ]
    # Python resolves ambiguous week-date/digit separators by fixed precedence.
    pickups += [
        base + separator + clock
        for base, separator, clock in product(
            [
                "2025-01-01",
                "20250101",
                "2025-W01",
                "2025W01",
                "2025-W01-3",
                "2025W013",
                "0001-W01-1",
                "9999-W52-7",
            ],
            ["", "T", " ", "🙂", "1", ":", "+", "Z", "\n"],
            [
                "",
                "00",
                "12",
                "24",
                "1234",
                "123456",
                "12:34",
                "12:34:56",
                "12.5",
                "12:34.5",
                "1234.5",
                "12:34:56+00",
                "12:34:56Z",
            ],
        )
    ]
    rows = [(pickup, "0.001") for pickup in pickups]
    table = create_source(sql_database, rows)
    assert classify(sql_database, table) == [expected_row(*row) for row in rows]


def test_money_text_matches_decimal_without_rounding(sql_database):
    """String parsing preserves exact coefficients/exponents, not DECIMAL width."""
    amounts = [
        "0.10",
        "0.20",
        "-3.50",
        "-0.00",
        "+.50",
        "1.",
        "001.2300",
        "1.2300e2",
        "12300e-4",
        "100e-4",
        "1e-2",
        "1e-3",
        "-0.001",
        "0.0000",
        "0e-999999999999999999",
        "0e999999999999999999",
        "0e-1000000000000000000",
        "0e-1999999999999999997",
        "0.0e1000000000000000000",
        "0.0e-1999999999999999996",
        "0e-1999999999999999998",
        "0.0e-1999999999999999997",
        "11e999999999999999999",
        "1e1000000000000000000",
        "1e-9223372036854775808",
        "1e-170141183460469231731687303715884105728",
        "0e9999999999999999999",
        "1e00000000000000000000000000000000000000002",
        " \t+_1_._2_0_e_+_2_ \n",
        "_ 1",
        "1__2",
        "١٢.٣٠",
        "１２.３０",
        "\u20071.20\u2007",
        "\x1c1.20\x1f",
        "1\u20072",
        "1,20",
        "1x20",
        "",
        ".",
        "+",
        "NaN",
        "sNaN12",
        "Inf",
        "-Infinity",
        "true",
        None,
        "123456789012345678901234567890123456789012345678901234567890.12",
    ]
    rows = [("2025-01-01", amount) for amount in amounts]
    table = create_source(sql_database, rows)
    assert classify(sql_database, table) == [expected_row(*row) for row in rows]


def test_bignum_sums_are_exact_beyond_fixed_decimal_precision(sql_database):
    """Carry, cancellation and negative totals retain exact arbitrary-width cents."""
    huge = "9" * 80
    rows = [
        ("2025-01-01", huge + ".99"),
        ("2025-01-01", "0.01"),
        ("2025-01-01", "-" + huge + ".99"),
        ("2025-01-02", "-" + huge + ".99"),
        ("2025-01-02", "-0.01"),
    ]
    table = create_source(sql_database, rows)
    classify(sql_database, table)
    classified_union(sql_database, [table])
    assert sql_database.execute(sql_resource("daily_revenue")).fetchall() == [
        (date(2025, 1, 1), 3, "1"),
        (date(2025, 1, 2), 2, "-" + str((10**80) * 100)),
    ]
    assert sql_database.execute(
        "SELECT typeof(amount_cents), typeof(sum(amount_cents)) "
        "FROM classified GROUP BY typeof(amount_cents)"
    ).fetchone() == ("BIGNUM", "BIGNUM")


def test_separate_sources_combine_after_classification(sql_database, trip_file):
    """Mixed types and equal rows in different sources retain source multiplicity."""
    sources = [
        (
            "TIMESTAMP",
            "DOUBLE",
            "timestamp_naive",
            "numeric",
            [(datetime(2025, 1, 1), 0.5)] * 2,
        ),
        ("DATE", "BOOLEAN", "date", "boolean", [(date(2025, 1, 1), True)]),
        (
            "TIMESTAMPTZ",
            "DOUBLE",
            "timestamp_aware",
            "numeric",
            [(datetime(2025, 1, 1, tzinfo=timezone.utc), 2.0)],
        ),
        (
            "VARCHAR",
            "VARCHAR",
            "text",
            "text",
            [("invalid", "0.001"), ("2025-02-01", "0.001")],
        ),
    ]
    tables, paths = [], []
    for i, (pickup_type, amount_type, pickup_kind, amount_kind, rows) in enumerate(
        sources, 1
    ):
        table = create_source(sql_database, rows, pickup_type, amount_type, f"{i:02}")
        classify(sql_database, table, pickup_kind, amount_kind)
        tables.append(table)
        paths.append(trip_file(f"source-{i}.parquet", *zip(*rows, strict=True)))
    # Another source with exactly the first source's bytes is not deduplicated.
    table = create_source(sql_database, sources[0][-1], "TIMESTAMP", "DOUBLE", "05")
    classify(sql_database, table, "timestamp_naive", "numeric")
    tables.append(table)
    duplicate_path = paths[0].with_name("equal-bytes.parquet")
    duplicate_path.write_bytes(paths[0].read_bytes())
    result = DailyRevenueCalculator(PERIOD).calculate([*paths, duplicate_path])
    classified_union(sql_database, tables)
    assert sql_database.execute(sql_resource("counts")).fetchone() == (
        *[result.counts[name] for name in COUNT_FIELDS],
        0,
    )
    assert sql_database.execute(sql_resource("daily_revenue")).fetchall() == [
        (date(2025, 1, 1), 4, "200")
    ]


def test_sub_cent_is_separate_failure_not_excluded_money(sql_database, trip_file):
    """The query exposes the condition the integration coordinator must reject."""
    rows = [
        ("invalid", "0.001"),
        ("2025-02-01", "0.001"),
        ("2025-01-01", "0.001"),
        ("2025-01-01", "1.00"),
    ]
    table = create_source(sql_database, rows)
    classify(sql_database, table)
    classified_union(sql_database, [table])
    assert sql_database.execute(sql_resource("counts")).fetchone() == (
        4,
        1,
        1,
        1,
        0,
        0,
        0,
        1,
    )
    with pytest.raises(ValueError, match="Sub-cent"):
        DailyRevenueCalculator(PERIOD).calculate(
            [trip_file("sub-cent.parquet", *zip(*rows, strict=True))]
        )


def test_empty_classification_has_zero_counters_and_no_facts(sql_database):
    """No SQL null totals are mistaken for an accepted zero-value ride."""
    table = create_source(sql_database, [])
    classify(sql_database, table)
    classified_union(sql_database, [table])
    assert sql_database.execute(sql_resource("counts")).fetchone() == (0,) * 8
    assert sql_database.execute(sql_resource("daily_revenue")).fetchall() == []


def test_huge_exponent_fails_before_allocating_unbounded_money(sql_database):
    """Resource failures are explicit, never rounded or reported as invalid money."""
    duckdb = pytest.importorskip("duckdb")
    rows = [("2025-01-01", "1e100000000000")]
    table = create_source(sql_database, rows)
    with pytest.raises(duckdb.Error, match="resource limit"):
        classify(sql_database, table)


def test_resource_guard_does_not_parse_money_on_excluded_pickups(sql_database):
    """Excluded pickups and exponential zero do not allocate exponent-size strings."""
    rows = [
        ("invalid", "1e100000000000"),
        ("2025-02-01", "1e100000000000"),
        ("2025-01-01", "0e100000000000"),
        ("2025-01-01", "1e-999999999999999999"),
    ]
    table = create_source(sql_database, rows)
    assert classify(sql_database, table) == [
        (None, "invalid_pickup", None),
        (date(2025, 2, 1), "outside_period", None),
        (date(2025, 1, 1), "accepted", "0"),
        (date(2025, 1, 1), "sub_cent", None),
    ]


@pytest.mark.parametrize("arrow_type", ["int64", "uint64"])
def test_nullable_integer_parquet_preserves_original_cents(
    sql_database, tmp_path, arrow_type
):
    """SQL must not introduce lossy pandas float promotion for nullable integers."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    amount = 9007199254740993
    source = pa.table(
        {
            "lpep_pickup_datetime": [date(2025, 1, 1)] * 2,
            "total_amount": pa.array([amount, None], type=getattr(pa, arrow_type)()),
        }
    )
    path = tmp_path / "nullable-integer.parquet"
    pq.write_table(source, path)
    sql_database.execute(
        "CREATE TABLE raw.source_2025_01 AS SELECT * FROM read_parquet(?)",
        [str(path)],
    )
    rows = classify(sql_database, "source_2025_01", "date", "numeric")
    assert rows == [
        (date(2025, 1, 1), "accepted", str(amount * 100)),
        (date(2025, 1, 1), "invalid_amount", None),
    ]
    assert source["total_amount"][0].as_py() == amount


def test_float32_pandas_metadata_does_not_hide_sub_cents(sql_database, tmp_path):
    """Extension metadata must not change the classification of original float bits."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    frame = pd.DataFrame(
        {
            "lpep_pickup_datetime": ["2025-01-01"] * 2,
            "total_amount": pd.array([0.1, None], dtype="Float32"),
        }
    )
    source = pa.Table.from_pandas(frame, preserve_index=False)
    path = tmp_path / "extension-float.parquet"
    pq.write_table(source, path)
    sql_database.execute(
        "CREATE TABLE raw.source_2025_01 AS SELECT * FROM read_parquet(?)",
        [str(path)],
    )
    assert classify(sql_database, "source_2025_01", "text", "numeric") == [
        (date(2025, 1, 1), "sub_cent", None),
        (date(2025, 1, 1), "invalid_amount", None),
    ]
    assert source["total_amount"][0].as_py() != 0.1


@pytest.mark.parametrize(
    "table",
    [
        "raw.source_2025_01",
        "source_2025_13",
        'source_2025_01";DROP TABLE raw.x',
        "source_2025_1",
        "source_2025_01 --",
    ],
)
def test_identifier_boundary_rejects_unvalidated_names(table):
    """Identifiers cannot be bound as values, so validate and quote them separately."""
    with pytest.raises(ValueError, match="identifier"):
        classification_query(table)
