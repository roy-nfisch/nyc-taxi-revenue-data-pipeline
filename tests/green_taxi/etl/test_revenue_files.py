"""Read actual tiny Parquet files and verify agreed data rules and aggregation."""

from datetime import datetime, timedelta
from decimal import Decimal, localcontext

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pandas.testing import assert_frame_equal

from green_taxi.etl.revenue import DailyRevenueCalculator

pytestmark = [pytest.mark.integration, pytest.mark.data]


def test_known_daily_totals_across_files(reporting_settings, trip_file):
    """Combine repeated pickup days across differently named monthly files."""
    start = reporting_settings.reporting_period.start
    first = trip_file("first.parquet", [start, start], [0.1, 0.2])
    second = trip_file(
        "later-month.parquet", [start, start + timedelta(days=1)], [-0.05, 0.0]
    )
    with localcontext() as context:
        context.prec = 2
        result = DailyRevenueCalculator(reporting_settings.reporting_period).calculate(
            [first, second]
        )
    assert result.facts.to_dict("records") == [
        {"service_date": start, "trip_count": 3, "daily_revenue": Decimal("0.25")},
        {
            "service_date": start + timedelta(days=1),
            "trip_count": 1,
            "daily_revenue": Decimal("0.00"),
        },
    ]
    assert result.total_amount == Decimal("0.25")
    assert result.counts["negative_amount"] == 1
    assert result.counts["zero_amount"] == 1


def test_period_boundaries(reporting_settings, trip_file):
    """Include start and the final instant before end; reject adjacent dates."""
    period = reporting_settings.reporting_period
    start = datetime.combine(period.start, datetime.min.time())
    end = datetime.combine(period.end, datetime.min.time())
    path = trip_file(
        "boundaries.parquet",
        [
            start - timedelta(microseconds=1),
            start,
            end - timedelta(microseconds=1),
            end,
        ],
        [1.0] * 4,
    )
    result = DailyRevenueCalculator(period).calculate([path])
    assert result.counts["rows_used"] == 2
    assert result.counts["outside_period"] == 2
    assert result.total_amount == Decimal("2.00")


def test_exclusion_reasons_do_not_overlap(reporting_settings, trip_file):
    """Assign date, period, and amount exclusions in order, counting each row once."""
    period = reporting_settings.reporting_period
    path = trip_file(
        "invalid.parquet",
        [None, "bad-date", str(period.end), str(period.start), str(period.start)],
        [None, "bad", "bad", "Infinity", "10.00"],
    )
    result = DailyRevenueCalculator(period).calculate([path])
    assert result.counts == {
        "rows_read": 5,
        "rows_used": 1,
        "invalid_pickup": 2,
        "outside_period": 1,
        "invalid_amount": 1,
        "negative_amount": 0,
        "zero_amount": 0,
    }
    assert (
        sum(
            result.counts[key]
            for key in [
                "rows_used",
                "invalid_pickup",
                "outside_period",
                "invalid_amount",
            ]
        )
        == result.counts["rows_read"]
    )


@pytest.mark.parametrize("missing", ["total_amount", "lpep_pickup_datetime"])
def test_missing_required_column_stops_run(reporting_settings, tmp_path, missing):
    """Do not fall back to fare components or silently skip malformed file schemas."""
    path = tmp_path / "missing.parquet"
    pd.DataFrame(
        {
            "lpep_pickup_datetime": ["2025-01-01"],
            "total_amount": [1],
            "fare_amount": [1],
        }
    ).drop(columns=missing).to_parquet(path)
    with pytest.raises(ValueError, match="columns"):
        DailyRevenueCalculator(reporting_settings.reporting_period).calculate([path])


@pytest.mark.parametrize("case", ["empty", "bad_date", "bad_amount", "outside"])
def test_no_usable_batch_fails(reporting_settings, trip_file, case):
    """An empty fact table is not a successfully calculated revenue report."""
    start = str(reporting_settings.reporting_period.start)
    pickups, amounts = {
        "empty": ([], []),
        "bad_date": ([None], [1]),
        "bad_amount": ([start], [None]),
        "outside": ([str(reporting_settings.reporting_period.end)], [1]),
    }[case]
    path = trip_file("unusable.parquet", pickups, amounts)
    with pytest.raises(ValueError, match="No usable"):
        DailyRevenueCalculator(reporting_settings.reporting_period).calculate([path])


def test_same_file_alias_is_rejected(reporting_settings, trip_file):
    """Reject the same source twice even under a symlink path."""
    source = trip_file(
        "source.parquet", [reporting_settings.reporting_period.start], [1]
    )
    alias = source.with_name("alias.parquet")
    alias.symlink_to(source)
    with pytest.raises(ValueError, match="distinct"):
        DailyRevenueCalculator(reporting_settings.reporting_period).calculate(
            [source, alias]
        )


def test_empty_input_list_fails(reporting_settings):
    """Missing inputs cannot produce a successful empty report."""
    with pytest.raises(ValueError, match="nonempty"):
        DailyRevenueCalculator(reporting_settings.reporting_period).calculate([])


def test_subcent_stops_instead_of_rounding(reporting_settings, trip_file):
    """Finite money needing a new rounding rule must fail explicitly."""
    path = trip_file(
        "precision.parquet", [reporting_settings.reporting_period.start], [0.001]
    )
    with pytest.raises(ValueError, match="Sub-cent"):
        DailyRevenueCalculator(reporting_settings.reporting_period).calculate([path])


@pytest.mark.parametrize("pandas_metadata", [False, True])
@pytest.mark.parametrize(
    "value, arrow_type, pandas_type",
    [(2**53 + 1, pa.int64(), "Int64"), (2**64 - 1, pa.uint64(), "UInt64")],
)
def test_nullable_integer_amount_is_not_promoted_to_float(
    reporting_settings, tmp_path, value, arrow_type, pandas_type, pandas_metadata
):
    """Preserve integer cents beyond float64 precision, with or without metadata."""
    pickups = [reporting_settings.reporting_period.start] * 2
    if pandas_metadata:
        table = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "lpep_pickup_datetime": pickups,
                    "total_amount": pd.array([value, None], dtype=pandas_type),
                }
            ),
            preserve_index=False,
        )
    else:
        table = pa.table(
            {
                "lpep_pickup_datetime": pickups,
                "total_amount": pa.array([value, None], type=arrow_type),
            }
        )
    path = tmp_path / "nullable-integer.parquet"
    pq.write_table(table, path)
    result = DailyRevenueCalculator(reporting_settings.reporting_period).calculate(
        [path]
    )
    assert result.total_amount == Decimal(f"{value}.00")
    assert result.counts["rows_read"] == 2
    assert result.counts["rows_used"] == 1
    assert result.counts["invalid_amount"] == 1


@pytest.mark.parametrize("pandas_metadata", [False, True])
def test_float32_subcent_decision_does_not_depend_on_pandas_metadata(
    reporting_settings, tmp_path, pandas_metadata
):
    """A binary32 0.1 is not cent-exact; extension metadata must not hide it."""
    pickups = [reporting_settings.reporting_period.start] * 2
    if pandas_metadata:
        table = pa.Table.from_pandas(
            pd.DataFrame(
                {
                    "lpep_pickup_datetime": pickups,
                    "total_amount": pd.array([0.1, None], dtype="Float32"),
                }
            ),
            preserve_index=False,
        )
    else:
        table = pa.table(
            {
                "lpep_pickup_datetime": pickups,
                "total_amount": pa.array([0.1, None], type=pa.float32()),
            }
        )
    path = tmp_path / "float32.parquet"
    pq.write_table(table, path)
    with pytest.raises(ValueError, match="Sub-cent"):
        DailyRevenueCalculator(reporting_settings.reporting_period).calculate([path])


@pytest.mark.parametrize(
    "amounts, expected, invalid, negative, zero",
    [
        (pa.array([0.1, None], type=pa.float64()), Decimal("0.10"), 1, 0, 0),
        (
            pa.array([Decimal("0.00"), Decimal("-1.25"), None]),
            Decimal("-1.25"),
            1,
            1,
            1,
        ),
        (
            pa.array([0.0, -1.25, float("nan"), float("inf"), -float("inf")]),
            Decimal("-1.25"),
            3,
            1,
            1,
        ),
    ],
)
def test_arrow_backed_read_preserves_existing_money_rules(
    reporting_settings, tmp_path, amounts, expected, invalid, negative, zero
):
    """Keep float64 text semantics, decimal signs/zero and nonfinite accounting."""
    path = tmp_path / "money-rules.parquet"
    pq.write_table(
        pa.table(
            {
                "lpep_pickup_datetime": [reporting_settings.reporting_period.start]
                * len(amounts),
                "total_amount": amounts,
            }
        ),
        path,
    )
    result = DailyRevenueCalculator(reporting_settings.reporting_period).calculate(
        [path]
    )
    assert result.total_amount == expected
    assert result.counts["invalid_amount"] == invalid
    assert result.counts["rows_used"] == len(amounts) - invalid
    assert result.counts["negative_amount"] == negative
    assert result.counts["zero_amount"] == zero


def test_arrow_backed_boolean_amounts_are_still_invalid(reporting_settings, tmp_path):
    """Never treat native nullable booleans as integer revenue."""
    path = tmp_path / "booleans.parquet"
    pq.write_table(
        pa.table(
            {
                "lpep_pickup_datetime": [reporting_settings.reporting_period.start] * 3,
                "total_amount": pa.array([True, False, None], type=pa.bool_()),
            }
        ),
        path,
    )
    with pytest.raises(ValueError, match="No usable"):
        DailyRevenueCalculator(reporting_settings.reporting_period).calculate([path])


def test_repeat_calls_do_not_retain_state_or_write_reports(
    reporting_settings, trip_file
):
    """Reusing the calculator rebuilds facts without duplication or output I/O."""
    source = trip_file(
        "source.parquet", [reporting_settings.reporting_period.start] * 2, [1, 1]
    )
    before = source.read_bytes()
    calculator = DailyRevenueCalculator(reporting_settings.reporting_period)
    first = calculator.calculate([source])
    second = calculator.calculate([source])
    assert_frame_equal(first.facts, second.facts)
    assert first.counts == second.counts
    assert second.counts["rows_used"] == 2  # Do not deduplicate similar rides.
    assert source.read_bytes() == before
    assert not reporting_settings.processed_dir.exists()


def test_calendar_does_not_invent_facts_and_join_preserves_totals(
    reporting_settings, trip_file
):
    """Dates without records stay only in the dimension; fact joins are one-to-one."""
    source = trip_file(
        "source.parquet", [reporting_settings.reporting_period.start], [2]
    )
    result = DailyRevenueCalculator(reporting_settings.reporting_period).calculate(
        [source]
    )
    assert len(result.dates) == 90
    assert len(result.facts) == 1
    joined = result.facts.merge(result.dates, on="service_date", validate="one_to_one")
    assert len(joined) == len(result.facts)
    assert joined.daily_revenue.tolist() == result.facts.daily_revenue.tolist()
