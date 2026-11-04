"""Calculate daily facts and a calendar dimension; do not download or save files."""

import logging
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

from green_taxi.settings import ReportingPeriod

PICKUP = "lpep_pickup_datetime"
AMOUNT = "total_amount"
COUNT_FIELDS = (
    "rows_read",
    "rows_used",
    "invalid_pickup",
    "outside_period",
    "invalid_amount",
    "negative_amount",
    "zero_amount",
)
logger = logging.getLogger(__name__)


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


def recorded_date(value: object) -> date | None:
    """Parse an ISO string or native date/datetime; return None for unusable input.

    Do not guess numeric epochs, ambiguous day/month strings, or timezone shifts.
    Timezone-aware values are unusable under the agreed naive-source contract.
    Invalid or missing values are counted by the caller, not silently replaced.
    """
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value)
        if not isinstance(value, date) or pd.isna(value):
            return None
        stamp = pd.Timestamp(value)
        return stamp.date() if stamp.tzinfo is None else None
    except (ValueError, TypeError, OverflowError):
        return None


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


class DailyRevenueCalculator:
    """Own approved record rules and aggregation, not downloads or output writes.

    Supply a validated ReportingPeriod. calculate() reads one Parquet file at a time and
    returns a RevenueResult. Repeated calls rebuild results from scratch. Missing
    columns/files, duplicate resolved paths, sub-cent money, and an entirely
    unusable batch fail explicitly. Rows are not deduplicated by appearance.
    """

    def __init__(self, period: ReportingPeriod) -> None:
        """Store the agreed period without reading inputs or creating directories."""
        self.period = period

    def calculate(self, input_paths: Iterable[Path]) -> RevenueResult:
        """Read selected paths and return daily facts/calendar plus accounting.

        Resolve paths before rejecting repeated inputs, including symlink aliases.
        Exclusion priority is invalid pickup, outside period, then invalid amount;
        every row is counted once. A file's name never assigns its revenue day.
        A file with no usable rows may contribute exclusions; a batch with none
        fails. Other I/O or Parquet errors propagate. No partial report is returned.
        """
        paths = tuple(Path(path).resolve() for path in input_paths)
        if not paths or len(paths) != len(set(paths)):
            raise ValueError("Supply nonempty, distinct input paths.")
        daily = {}
        counts = Counter(dict.fromkeys(COUNT_FIELDS, 0))
        for path in paths:
            frame = pd.read_parquet(path)
            self._add_frame(frame, daily, counts)
            logger.info("Read %s (%s rows)", path.name, len(frame))
        if not counts["rows_used"]:
            raise ValueError("No usable ride records remain in the reporting period.")
        logger.info("Accepted %s of %s rows", counts["rows_used"], counts["rows_read"])
        return self._result(daily, counts)

    def _result(self, daily: dict, counts: Counter) -> RevenueResult:
        """Build independent output tables and exact totals; perform no writes."""
        facts = pd.DataFrame(
            [
                {
                    "service_date": day,
                    "trip_count": count,
                    "daily_revenue": decimal_amount(cents),
                }
                for day, (count, cents) in sorted(daily.items())
            ]
        )
        return RevenueResult(
            facts,
            build_date_dimension(self.period),
            dict(counts),
            decimal_amount(sum(cents for _, cents in daily.values())),
        )

    def _add_frame(self, frame: pd.DataFrame, daily: dict, counts: Counter) -> None:
        """Accumulate one monthly frame without mutating it or retaining ride rows.

        Required column names must be present and unique. Update exact integer
        totals for accepted rows, plus counts of negative and zero accepted money.
        Missing payment type and other unused columns do not exclude a ride.
        """
        if not frame.columns.is_unique or not {PICKUP, AMOUNT}.issubset(frame.columns):
            raise ValueError(
                "Input needs unique pickup datetime and total_amount columns."
            )
        for pickup, amount in frame[[PICKUP, AMOUNT]].itertuples(
            index=False, name=None
        ):
            counts["rows_read"] += 1
            day = recorded_date(pickup)
            if day is None:
                counts["invalid_pickup"] += 1
                continue
            if not self.period.contains(day):
                counts["outside_period"] += 1
                continue
            cents = recorded_cents(amount)
            if cents is None:
                counts["invalid_amount"] += 1
                continue
            count, total = daily.get(day, (0, 0))
            daily[day] = count + 1, total + cents
            counts["rows_used"] += 1
            counts["negative_amount"] += cents < 0
            counts["zero_amount"] += cents == 0
