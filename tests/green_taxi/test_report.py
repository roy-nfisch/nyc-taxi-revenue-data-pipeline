"""Read saved outputs and exercise publication failures using temporary storage."""

import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from green_taxi.report import OUTPUT_FILES, ReportWriter

pytestmark = [pytest.mark.integration, pytest.mark.data]


def test_outputs_keep_schemas_values_and_decimal_money(
    calculated_report, reporting_settings
):
    """Read both Parquet tables and CSV back; writing must not change their values."""
    writer = ReportWriter(reporting_settings.processed_dir)
    writer.write(calculated_report, reporting_settings.reporting_period, {})
    directory = writer.directory
    facts = pd.read_parquet(directory / OUTPUT_FILES[0])
    dates = pd.read_parquet(directory / OUTPUT_FILES[2])
    assert_frame_equal(facts, calculated_report.facts)
    assert_frame_equal(dates, calculated_report.dates)
    csv = pd.read_csv(
        directory / OUTPUT_FILES[1], converters={"daily_revenue": Decimal}
    )
    csv["service_date"] = pd.to_datetime(csv.service_date).dt.date
    assert_frame_equal(csv, facts)
    assert facts.daily_revenue.tolist() == [Decimal("0.30")]


@pytest.mark.parametrize(
    "broken", ["duplicate", "unknown_date", "calendar", "count", "total", "schema"]
)
def test_invalid_result_is_not_published(calculated_report, reporting_settings, broken):
    """Reject broken output contracts before creating storage or replacing reports."""
    result = calculated_report
    if broken == "duplicate":
        result.facts.loc[1] = result.facts.iloc[0]
    elif broken == "unknown_date":
        result.facts.loc[0, "service_date"] = reporting_settings.reporting_period.end
    elif broken == "calendar":
        result = replace(result, dates=result.dates.drop(index=0))
    elif broken == "count":
        result.counts["rows_used"] += 1
    elif broken == "total":
        result.facts.loc[0, "daily_revenue"] = Decimal("0.31")
    else:
        result = replace(
            result, facts=result.facts.rename(columns={"trip_count": "trips"})
        )
    writer = ReportWriter(reporting_settings.processed_dir)
    with pytest.raises(ValueError):
        writer.write(result, reporting_settings.reporting_period, {})
    assert not writer.directory.exists()


def test_staging_failure_preserves_prior_report(
    calculated_report, reporting_settings, monkeypatch
):
    """A failed CSV write leaves every previous output and metadata untouched."""
    writer = ReportWriter(reporting_settings.processed_dir)
    writer.write(calculated_report, reporting_settings.reporting_period, {})
    before = {path.name: path.read_bytes() for path in writer.directory.iterdir()}

    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_csv", fail)
    with pytest.raises(OSError, match="disk full"):
        writer.write(calculated_report, reporting_settings.reporting_period, {})
    assert {
        path.name: path.read_bytes() for path in writer.directory.iterdir()
    } == before


@pytest.mark.parametrize("failed_name", (*OUTPUT_FILES, "metadata.json"))
def test_replacement_failure_removes_success_marker(
    calculated_report, reporting_settings, monkeypatch, failed_name
):
    """Do not leave old metadata claiming that a partially replaced bundle is valid."""
    writer = ReportWriter(reporting_settings.processed_dir)
    marker = writer.write(calculated_report, reporting_settings.reporting_period, {})
    original = Path.replace

    def fail_second(path, target):
        if path.name == failed_name:
            raise OSError("replacement failed")
        return original(path, target)

    monkeypatch.setattr(Path, "replace", fail_second)
    with pytest.raises(OSError, match="replacement failed"):
        writer.write(calculated_report, reporting_settings.reporting_period, {})
    assert not marker.exists()
    assert not list(writer.directory.glob(".report-*"))


def test_metadata_describes_written_outputs(calculated_report, reporting_settings):
    """JSON row counts, resolved period, and totals describe actual saved tables."""
    writer = ReportWriter(reporting_settings.processed_dir)
    path = writer.write(
        calculated_report, reporting_settings.reporting_period, {"run_id": "test"}
    )
    metadata = json.loads(path.read_text())
    assert metadata["run_id"] == "test"
    assert metadata["total_amount"] == "0.30"
    assert metadata["counts"] == calculated_report.counts
    period = reporting_settings.reporting_period
    assert metadata["reporting_period"]["start_inclusive"] == period.start.isoformat()
    assert metadata["reporting_period"]["end_exclusive"] == period.end.isoformat()
    for name, info in metadata["outputs"].items():
        read = pd.read_csv if name.endswith(".csv") else pd.read_parquet
        assert info["rows"] == len(read(writer.directory / name))


def test_output_path_must_be_a_directory(calculated_report, reporting_settings):
    """An unusable destination fails explicitly instead of returning success."""
    destination = reporting_settings.processed_dir
    destination.write_text("not a directory")
    with pytest.raises(FileExistsError):
        ReportWriter(destination).write(
            calculated_report, reporting_settings.reporting_period, {}
        )
    assert destination.read_text() == "not a directory"


@pytest.mark.parametrize("value", [0.3, "0.30"])
def test_money_must_remain_decimal(calculated_report, reporting_settings, value):
    """Convertible values must not silently change the stored decimal schema."""
    calculated_report.facts["daily_revenue"] = [value]
    with pytest.raises(ValueError, match="Decimal"):
        ReportWriter(reporting_settings.processed_dir).write(
            calculated_report, reporting_settings.reporting_period, {}
        )


@pytest.mark.parametrize("value", [-1, True, 0.0, "0", 3])
def test_invalid_auxiliary_counts_fail(calculated_report, reporting_settings, value):
    """Counts must be nonnegative integers and cannot exceed accepted rides."""
    calculated_report.counts["negative_amount"] = value
    with pytest.raises(ValueError, match="counts"):
        ReportWriter(reporting_settings.processed_dir).write(
            calculated_report, reporting_settings.reporting_period, {}
        )


@pytest.mark.parametrize("filename", OUTPUT_FILES)
def test_changed_saved_table_preserves_prior_report(
    calculated_report, reporting_settings, monkeypatch, filename
):
    """Detect successful-but-wrong serialization before replacing any old files."""
    writer = ReportWriter(reporting_settings.processed_dir)
    writer.write(calculated_report, reporting_settings.reporting_period, {})
    before = {path.name: path.read_bytes() for path in writer.directory.iterdir()}
    method = "to_csv" if filename.endswith(".csv") else "to_parquet"
    original = getattr(pd.DataFrame, method)

    def corrupt(frame, path, **kwargs):
        changed = frame.iloc[:0] if Path(path).name == filename else frame
        return original(changed, path, **kwargs)

    monkeypatch.setattr(pd.DataFrame, method, corrupt)
    with pytest.raises(ValueError, match="Saved tables"):
        writer.write(calculated_report, reporting_settings.reporting_period, {})
    assert {
        path.name: path.read_bytes() for path in writer.directory.iterdir()
    } == before
