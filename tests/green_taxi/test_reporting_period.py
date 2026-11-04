"""Check strict month counts and derived calendar boundaries without file I/O."""

from datetime import date

import pytest
from pydantic import ValidationError

from green_taxi.settings import ReportingPeriod

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "duration",
    [-2, -1, 0, True, False, 1.0, 1.5, "3", "1.0", "", None, [], {}],
)
def test_duration_requires_a_positive_integer(duration):
    """Do not coerce strings/floats/booleans or interpret nonpositive modes."""
    with pytest.raises(ValidationError):
        ReportingPeriod(start_month="2025-01", duration_months=duration)


@pytest.mark.parametrize(
    "month",
    ["2025-00", "2025-13", "0000-01", "2025-1", "2025-01-01", "0", 0, None],
)
def test_start_month_must_be_a_real_calendar_month(month):
    """Reject bad formats, impossible years, and numeric dates instead of guessing."""
    with pytest.raises(ValidationError):
        ReportingPeriod(start_month=month, duration_months=1)


@pytest.mark.parametrize(
    "values",
    [
        {},
        {"start_month": "2025-01"},
        {"duration_months": 1},
        {"start_month": "2025-01", "duration_months": 1, "end": "2025-04-01"},
        {"start": "2025-01-01", "end": "2025-04-01"},
    ],
)
def test_period_requires_only_its_two_configuration_fields(values):
    """Missing fields and separately editable date boundaries fail validation."""
    with pytest.raises(ValidationError):
        ReportingPeriod.model_validate(values)


@pytest.mark.parametrize(
    "month, duration, start, end",
    [
        ("2025-01", 1, date(2025, 1, 1), date(2025, 2, 1)),
        ("2024-02", 1, date(2024, 2, 1), date(2024, 3, 1)),
        ("2025-12", 2, date(2025, 12, 1), date(2026, 2, 1)),
        ("2025-01", 12, date(2025, 1, 1), date(2026, 1, 1)),
        ("9999-10", 2, date(9999, 10, 1), date(9999, 12, 1)),
    ],
)
def test_month_count_derives_exact_boundaries(month, duration, start, end):
    """Count complete calendar months, handling leap years and year transitions."""
    period = ReportingPeriod(start_month=month, duration_months=duration)
    assert (period.start, period.end) == (start, end)
    assert period.contains(start)
    assert not period.contains(end)


@pytest.mark.parametrize("duration", [1, 10**30])
def test_period_must_fit_supported_calendar_years(duration):
    """Reject overflow at construction, not later while processing ride records."""
    with pytest.raises(ValidationError, match="supported calendar years"):
        ReportingPeriod(start_month="9999-12", duration_months=duration)


def test_period_is_immutable():
    """Prevent the report range changing halfway through a calculation."""
    period = ReportingPeriod(start_month="2025-01", duration_months=1)
    with pytest.raises(ValidationError):
        period.duration_months = 2
