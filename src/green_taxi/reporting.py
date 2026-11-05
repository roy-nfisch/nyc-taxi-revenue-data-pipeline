"""Shared report result, calendar and exact-money helpers; no file or network I/O."""

from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal, InvalidOperation

import pandas as pd

from green_taxi.settings import ReportingPeriod

COUNT_FIELDS = (
    "rows_read",
    "rows_used",
    "invalid_pickup",
    "outside_period",
    "invalid_amount",
    "negative_amount",
    "zero_amount",
)


def build_date_dimension(period: ReportingPeriod) -> pd.DataFrame:
    """Return one row per calendar day, independently of available ride records.

    Input is a validated period. Dates are Python date values, with integer year,
    month and day_of_month. A calendar row does not assert that any rides existed.
    This pure helper does not create zero-revenue facts or perform file I/O.
    """
    days = [
        period.start + timedelta(days=i)
        for i in range((period.end - period.start).days)
    ]
    return pd.DataFrame(
        {
            "service_date": days,
            "year": [day.year for day in days],
            "month": [day.month for day in days],
            "day_of_month": [day.day for day in days],
        }
    )


def recorded_cents(value: object) -> int | None:
    """Return exact integer cents, or None for unknown/non-numeric/non-finite money.

    Convert via decimal text rather than Decimal(float)'s binary expansion.
    Reject booleans and never replace unknown amounts with zero. Raise ValueError
    for a finite sub-cent amount: rounding would change an unapproved data rule.
    Integer arithmetic avoids dependence on the caller's decimal precision.
    """
    if isinstance(value, bool):
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if not amount.is_finite():
        return None
    numerator, denominator = amount.as_integer_ratio()
    cents, remainder = divmod(numerator * 100, denominator)
    if remainder:
        raise ValueError("Sub-cent total_amount requires a reviewed rounding rule.")
    return cents


def decimal_amount(cents: int) -> Decimal:
    """Convert integer cents to an exact two-place Decimal without float arithmetic."""
    whole, fraction = divmod(abs(cents), 100)
    sign = "-" if cents < 0 else ""
    return Decimal(f"{sign}{whole}.{fraction:02d}")


@dataclass(frozen=True)
class RevenueResult:
    """Return calculated tables, row accounting, and exact total; no files saved.

    The container is frozen, but its DataFrames/dictionary are caller-owned mutable
    results. The calculator retains no reference to this run's mutable state.
    """

    facts: pd.DataFrame
    dates: pd.DataFrame
    counts: dict[str, int]
    total_amount: Decimal
