"""Small rule checks: no source downloads, report writes, or Prefect runtime."""

from datetime import date, timedelta
from decimal import Decimal, localcontext
from fractions import Fraction

import pandas as pd
import pytest

from green_taxi.reporting import (
    build_date_dimension,
    decimal_amount,
    recorded_cents,
)
from green_taxi.settings import ReportingPeriod

pytestmark = [pytest.mark.unit, pytest.mark.data]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("0.10", 10),
        (0.1, 10),
        (0, 0),
        ("-1.25", -125),
        (Decimal("1.2300"), 123),
        ("1e2", 10000),
    ],
)
def test_exact_cents(value, expected):
    """Accept finite cent-exact numeric representations, including zero/negative."""
    assert recorded_cents(value) == expected


@pytest.mark.parametrize(
    "value",
    [None, pd.NA, "bad", "", True, False, float("nan"), float("inf"), "-Infinity"],
)
def test_unknown_amount_is_not_zero(value):
    """Missing, nonnumeric, boolean, and non-finite amounts cannot become revenue."""
    assert recorded_cents(value) is None


@pytest.mark.parametrize("value", ["0.001", "-0.001", 0.1 + 0.2])
def test_subcent_values_require_review(value):
    """Do not round unexpected precision, including binary-arithmetic artefacts."""
    with pytest.raises(ValueError, match="Sub-cent"):
        recorded_cents(value)


@pytest.mark.parametrize("cents", [0, 1, -1, 123, -123, 10**35 + 1])
def test_decimal_result_is_exact_under_small_caller_precision(cents):
    """Money conversion must not change when another library changes decimal context."""
    with localcontext() as context:
        context.prec = 2
        actual = decimal_amount(cents)
        assert Fraction(actual) == Fraction(cents, 100)
        assert actual.as_tuple().exponent == -2


def test_calendar_uses_configured_period(reporting_settings):
    """The dimension covers the entire period, regardless of whether rides exist."""
    period = reporting_settings.reporting_period
    calendar = build_date_dimension(period)
    assert len(calendar) == (period.end - period.start).days == 90
    assert calendar.service_date.iloc[0] == period.start
    assert calendar.service_date.iloc[-1] == period.end - timedelta(days=1)
    assert calendar.service_date.is_unique
    assert calendar.isna().sum().sum() == 0
    assert calendar.columns.tolist() == [
        "service_date",
        "year",
        "month",
        "day_of_month",
    ]


def test_short_period_has_matching_calendar_attributes():
    """A whole leap-year February has 29 days and correct calendar attributes."""
    result = build_date_dimension(
        ReportingPeriod(start_month="2024-02", duration_months=1)
    )
    assert len(result) == 29
    assert result.iloc[-1].to_dict() == {
        "service_date": date(2024, 2, 29),
        "year": 2024,
        "month": 2,
        "day_of_month": 29,
    }
