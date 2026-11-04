"""Validate and save calculated tables; never download or recalculate rides."""

import json
import logging
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
from pandas.testing import assert_frame_equal

from green_taxi.revenue import (
    COUNT_FIELDS,
    RevenueResult,
    build_date_dimension,
    decimal_amount,
    recorded_cents,
)
from green_taxi.settings import ReportingPeriod

OUTPUT_FILES = (
    "fact_daily_revenue.parquet",
    "fact_daily_revenue.csv",
    "dim_date.parquet",
)
logger = logging.getLogger(__name__)


class ReportWriter:
    """Own checked output publication in one directory, not data transformations.

    Supply the processed directory. write() checks the result before touching
    existing reports, stages all files, and publishes metadata last. Each file
    replacement is atomic, not the whole bundle. Use one writer at a time per
    directory. A failed publication has no success metadata; errors propagate.
    """

    def __init__(self, directory: Path) -> None:
        """Store the destination without creating it or writing files."""
        self.directory = directory

    def write(
        self, result: RevenueResult, period: ReportingPeriod, metadata: dict
    ) -> Path:
        """Save fact Parquet/CSV, date Parquet, and JSON; return metadata path.

        Inputs are calculated tables, their validated period, and run lineage
        supplied by the pipeline. Validate before staging. A staging failure
        preserves the previous report; a replacement failure may leave mixed
        table versions but removes the success marker. No append behaviour.
        """
        self._validate(result, period)
        self.directory.mkdir(parents=True, exist_ok=True)
        marker = self.directory / "metadata.json"
        with TemporaryDirectory(dir=self.directory, prefix=".report-") as folder:
            staged = Path(folder)
            result.facts.to_parquet(staged / OUTPUT_FILES[0], index=False)
            result.facts.to_csv(staged / OUTPUT_FILES[1], index=False)
            result.dates.to_parquet(staged / OUTPUT_FILES[2], index=False)
            self._check_saved_tables(staged, result)
            document = self._metadata(result, period, metadata)
            (staged / marker.name).write_text(
                json.dumps(document, indent=2, allow_nan=False) + "\n",
                encoding="utf-8",
            )
            marker.unlink(missing_ok=True)
            for name in (*OUTPUT_FILES, marker.name):
                (staged / name).replace(self.directory / name)
        logger.info("Saved checked report: %s", marker)
        return marker

    @staticmethod
    def _validate(result: RevenueResult, period: ReportingPeriod) -> None:
        """Reject inconsistent schemas, dates, counts, or money before saving.

        Check calendar coverage, unique fact dates, and the one-to-one date join.
        This validates outputs from our calculator, not arbitrary uploaded tables.
        Raises ValueError for a broken report contract; performs no writes.
        """
        facts = result.facts
        if facts.columns.tolist() != ["service_date", "trip_count", "daily_revenue"]:
            raise ValueError("Unexpected fact schema.")
        try:
            assert_frame_equal(result.dates, build_date_dimension(period))
        except AssertionError as error:
            raise ValueError(
                "Date dimension does not match the reporting period."
            ) from error
        if facts.empty or facts.isna().any().any() or not facts.service_date.is_unique:
            raise ValueError("Facts need nonempty, non-null, unique dates and values.")
        if (
            not pd.api.types.is_integer_dtype(facts.trip_count)
            or (facts.trip_count <= 0).any()
        ):
            raise ValueError("Trip counts must be positive integers.")
        joined = facts.merge(result.dates, on="service_date", validate="one_to_one")
        if len(joined) != len(facts):
            raise ValueError("Every fact must reference a date in the dimension.")
        ReportWriter._validate_totals(result)

    @staticmethod
    def _validate_totals(result: RevenueResult) -> None:
        """Require exact decimal money and consistent nonnegative integer counts."""
        facts = result.facts
        money = [*facts.daily_revenue, result.total_amount]
        if not all(isinstance(value, Decimal) and value.is_finite() for value in money):
            raise ValueError("Money must use finite Decimal values.")
        cents = [recorded_cents(value) for value in facts.daily_revenue]
        if None in cents or decimal_amount(sum(cents)) != result.total_amount:
            raise ValueError("Fact money does not reconcile with the run total.")
        counts = result.counts
        if set(counts) != set(COUNT_FIELDS) or any(
            type(value) is not int or value < 0 for value in counts.values()
        ):
            raise ValueError("Run counts must be nonnegative integers with known keys.")
        if counts["negative_amount"] + counts["zero_amount"] > counts["rows_used"]:
            raise ValueError("Negative and zero counts exceed accepted rides.")
        if facts.trip_count.sum() != counts["rows_used"] or counts["rows_read"] != sum(
            counts[key]
            for key in (
                "rows_used",
                "invalid_pickup",
                "outside_period",
                "invalid_amount",
            )
        ):
            raise ValueError("Fact trip counts or exclusion counts do not reconcile.")

    @staticmethod
    def _check_saved_tables(staged: Path, result: RevenueResult) -> None:
        """Read all staged tables back and compare exact values before publication.

        Only daily summaries are read, not the raw dataset. CSV date and decimal
        converters preserve the output contract instead of guessing float money.
        Mismatches raise ValueError; read/parse failures also stop publication.
        """
        facts = pd.read_parquet(staged / OUTPUT_FILES[0])
        dates = pd.read_parquet(staged / OUTPUT_FILES[2])
        csv = pd.read_csv(
            staged / OUTPUT_FILES[1],
            converters={
                "service_date": date.fromisoformat,
                "daily_revenue": Decimal,
            },
        )
        try:
            assert_frame_equal(facts, result.facts, check_exact=True)
            assert_frame_equal(dates, result.dates, check_exact=True)
            assert_frame_equal(csv, result.facts, check_exact=True)
        except AssertionError as error:
            raise ValueError(
                "Saved tables do not match the calculated report."
            ) from error

    @staticmethod
    def _metadata(result: RevenueResult, period: ReportingPeriod, run: dict) -> dict:
        """Describe the checked tables and resolved dates, using JSON-safe values."""
        return run | {
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "reporting_period": period.model_dump()
            | {
                "start_inclusive": period.start.isoformat(),
                "end_exclusive": period.end.isoformat(),
            },
            "counts": result.counts,
            "total_amount": str(result.total_amount),
            "currency": "USD",
            "outputs": {
                OUTPUT_FILES[0]: {"rows": len(result.facts)},
                OUTPUT_FILES[1]: {"rows": len(result.facts)},
                OUTPUT_FILES[2]: {"rows": len(result.dates)},
            },
        }
