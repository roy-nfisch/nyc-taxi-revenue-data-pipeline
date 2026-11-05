"""Run real staging, SQL and publication together using existing tiny fixtures."""

import json

import pandas as pd
import pytest
from click.testing import CliRunner
from pandas.testing import assert_frame_equal

from green_taxi.elt.pipeline import ELTPipeline
from green_taxi.elt.warehouse import TaxiWarehouse
from green_taxi.etl.revenue import DailyRevenueCalculator

pytestmark = [pytest.mark.integration, pytest.mark.data]
pytest.importorskip("duckdb")


@pytest.fixture
def elt_setup(pipeline_setup):
    """Reuse the ETL stage/settings fixtures, replacing only the coordinator."""
    etl, session = pipeline_setup
    return etl, ELTPipeline(etl.settings, etl.downloader), session


def test_elt_matches_etl_and_rebuilds_separate_reports(elt_setup):
    """Reruns preserve every fact and counter, without touching ETL outputs."""
    etl, elt, session = elt_setup
    etl_marker = etl.run()
    original = etl_marker.read_bytes()
    expected = pd.read_parquet(etl_marker.parent / "fact_daily_revenue.parquet")
    previous = None
    for _ in range(2):
        marker = elt.run()
        metadata = json.loads(marker.read_text())
        assert_frame_equal(
            expected, pd.read_parquet(marker.parent / "fact_daily_revenue.parquet")
        )
        assert metadata["counts"] == json.loads(original)["counts"]
        assert marker.parent == etl.settings.data_dir / "elt" / "processed"
        assert metadata["run_id"] != previous
        previous = metadata["run_id"]
    assert etl_marker.read_bytes() == original
    assert session.get.call_count == len(etl.settings.months)


@pytest.mark.parametrize("amount", [0.001, None])
def test_failed_sql_does_not_publish_or_poison_next_run(
    elt_setup, parquet_bytes, amount
):
    """Sub-cents and no usable rows fail; a later corrected batch can succeed."""
    etl, elt, session = elt_setup
    response = session.get.return_value.__enter__.return_value
    response.iter_content.side_effect = lambda **kwargs: iter(
        [parquet_bytes(amount=amount)]
    )
    with pytest.raises(ValueError, match="Sub-cent|No usable"):
        elt.run()
    assert not (elt.settings.data_dir / "elt/processed/metadata.json").exists()
    response.iter_content.side_effect = lambda **kwargs: iter([parquet_bytes()])
    assert elt.run(force=True).exists()


def test_only_selected_loaded_sources_contribute(elt_setup):
    """A warehouse can contain other periods, but this report selects its own inputs."""
    _, elt, _ = elt_setup
    inputs = elt.downloader.download()
    with TaxiWarehouse(elt.database_path) as warehouse:
        warehouse.load(inputs)
        result = warehouse.calculate(inputs[:1], elt.settings.reporting_period)
    expected = DailyRevenueCalculator(elt.settings.reporting_period).calculate(
        [inputs[0].path]
    )
    assert_frame_equal(result.facts, expected.facts)
    assert result.counts == expected.counts


def test_missing_or_changed_registry_identity_fails(elt_setup):
    """Calculation must never use an unregistered source or a stale checksum."""
    _, elt, _ = elt_setup
    inputs = elt.downloader.download()
    with TaxiWarehouse(elt.database_path) as warehouse:
        with pytest.raises(ValueError, match="not loaded"):
            warehouse.calculate(inputs, elt.settings.reporting_period)
        warehouse.load(inputs)
        warehouse._connection.execute(
            "UPDATE loaded_sources SET sha256 = repeat('a', 64)"
        )
        with pytest.raises(ValueError, match="not loaded"):
            warehouse.calculate(inputs, elt.settings.reporting_period)


def test_manual_elt_command_reuses_real_pipeline(elt_setup, monkeypatch):
    """The CLI prints the successful metadata path; there is no second pipeline."""
    from green_taxi.elt import __main__ as command

    _, elt, _ = elt_setup
    monkeypatch.setattr(command, "ELTPipeline", lambda settings: elt)
    result = CliRunner().invoke(
        command.main, ["--data-dir", str(elt.settings.data_dir)]
    )
    assert result.exit_code == 0, result.output
    assert str(elt.settings.data_dir / "elt/processed/metadata.json") in result.output
