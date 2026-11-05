"""Calculate daily facts from staged files; do not download or save files."""

import logging
from collections import Counter
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from green_taxi.reporting import (
    COUNT_FIELDS,
    RevenueResult,
    build_date_dimension,
    decimal_amount,
    recorded_cents,
)
from green_taxi.settings import ReportingPeriod

PICKUP = "lpep_pickup_datetime"
AMOUNT = "total_amount"
logger = logging.getLogger("green_taxi.revenue")


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
        Arrow-backed reads preserve nullable numeric types before money validation.
        """
        paths = tuple(Path(path).resolve() for path in input_paths)
        if not paths or len(paths) != len(set(paths)):
            raise ValueError("Supply nonempty, distinct input paths.")
        daily = {}
        counts = Counter(dict.fromkeys(COUNT_FIELDS, 0))
        for path in paths:
            frame = pd.read_parquet(path, dtype_backend="pyarrow")
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
