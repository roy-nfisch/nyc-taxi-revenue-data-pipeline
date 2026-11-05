"""Check ETL pickup parsing without guessing dates or timezone conversions."""

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from green_taxi.etl.revenue import recorded_date

pytestmark = [pytest.mark.unit, pytest.mark.data]


@pytest.mark.parametrize(
    "value",
    [
        None,
        pd.NaT,
        pd.NA,
        1735689600,
        True,
        "31.02.2025",
        "01/02/2025",
        "invalid",
        datetime(2025, 1, 1, tzinfo=timezone.utc),
    ],
)
def test_invalid_pickup_is_not_guessed(value):
    """Reject missing, invalid, ambiguous, numeric-epoch and timezone-aware input."""
    assert recorded_date(value) is None


@pytest.mark.parametrize(
    "value",
    [
        "2025-02-03",
        "2025-02-03T23:59:59",
        date(2025, 2, 3),
        datetime(2025, 2, 3, 12),
        pd.Timestamp("2025-02-03"),
    ],
)
def test_supported_pickup_representations(value):
    """Native dates and ISO strings preserve the recorded day without shifting it."""
    assert recorded_date(value) == date(2025, 2, 3)
