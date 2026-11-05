"""Exercise all batch stages together without calling the live data provider."""

import hashlib
import json
from datetime import datetime

import pandas as pd
import pytest
from click.testing import CliRunner
from pandas.testing import assert_frame_equal

from green_taxi.__main__ import main

pytestmark = [pytest.mark.integration, pytest.mark.data]


def test_rebuild_reuses_inputs_and_replaces_equivalent_tables(pipeline_setup):
    """Two runs keep equal facts/calendar, fresh IDs, and no additional HTTP calls."""
    pipeline, session = pipeline_setup
    first_path = pipeline.run()
    first = json.loads(first_path.read_text())
    facts = pd.read_parquet(first_path.parent / "fact_daily_revenue.parquet")
    dates = pd.read_parquet(first_path.parent / "dim_date.parquet")
    second_path = pipeline.run()
    second = json.loads(second_path.read_text())
    assert session.get.call_count == len(pipeline.settings.months)
    assert first["run_id"] != second["run_id"]
    assert not any(item["downloaded"] for item in second["inputs"])
    assert_frame_equal(
        facts, pd.read_parquet(second_path.parent / "fact_daily_revenue.parquet")
    )
    assert_frame_equal(dates, pd.read_parquet(second_path.parent / "dim_date.parquet"))


def test_lineage_matches_actual_staged_inputs(pipeline_setup):
    """Record real sizes/checksums, configured URLs, UTC times, and code identity."""
    pipeline, _ = pipeline_setup
    metadata = json.loads(pipeline.run().read_text())
    start = datetime.fromisoformat(metadata["started_at"])
    finish = datetime.fromisoformat(metadata["finished_at"])
    assert start.utcoffset().total_seconds() == finish.utcoffset().total_seconds() == 0
    assert start <= finish
    assert set(metadata["code"]) == {"revision", "dirty"}
    for item in metadata["inputs"]:
        source = pipeline.settings.raw_dir / item["filename"]
        assert item["size_bytes"] == source.stat().st_size
        assert item["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
        assert (
            item["source_url"]
            == f"{str(pipeline.settings.source_url).rstrip('/')}/{source.name}"
        )
    assert metadata["counts"]["rows_used"] == len(pipeline.settings.months)


@pytest.mark.parametrize("force", [False, True])
def test_changed_source_does_not_relabel_cached_origin(pipeline_setup, force):
    """Cached bytes have unknown origin; only a real fetch verifies the new URL."""
    from green_taxi.settings import PipelineSettings

    pipeline, session = pipeline_setup
    pipeline.run()
    session.get.reset_mock()
    settings = PipelineSettings.model_validate(
        pipeline.settings.model_dump() | {"source_url": "https://example.org/taxi/"}
    )
    pipeline.settings = settings
    pipeline.downloader.settings = settings
    metadata = json.loads(pipeline.run(force=force).read_text())
    assert session.get.call_count == (len(settings.months) if force else 0)
    for item in metadata["inputs"]:
        configured = f"https://example.org/taxi/{item['filename']}"
        assert item["configured_source_url"] == configured
        assert item["source_url"] == (configured if force else None)


def test_cli_runs_existing_stages_and_force_refresh(pipeline_setup, monkeypatch):
    """CLI uses real reused inputs; force explicitly refreshes the downloader."""
    pipeline, session = pipeline_setup
    pipeline.run()
    config = pipeline.settings.data_dir.parent / "run.json"
    config.write_text(pipeline.settings.model_dump_json())
    result = CliRunner().invoke(main, ["--config", str(config), "--run"])
    assert result.exit_code == 0, result.output
    assert str(pipeline.settings.processed_dir / "metadata.json") in result.output
    monkeypatch.setattr(
        "green_taxi.etl.pipeline.TaxiDownloader", lambda settings: pipeline.downloader
    )
    refreshed = CliRunner().invoke(main, ["--config", str(config), "--run", "--force"])
    assert refreshed.exit_code == 0, refreshed.output
    assert session.get.call_count == 2 * len(pipeline.settings.months)


def test_stage_failure_does_not_publish_report(pipeline_setup):
    """A broken input stops calculation/publication rather than claiming success."""
    pipeline, session = pipeline_setup
    session.get.side_effect = OSError("offline")
    with pytest.raises(OSError, match="offline"):
        pipeline.run()
    assert not pipeline.settings.processed_dir.exists()


def test_force_requires_run():
    """A refresh flag alone must not silently turn settings inspection into data I/O."""
    result = CliRunner().invoke(main, ["--force"])
    assert result.exit_code != 0
    assert "--force requires --run" in result.output


def test_cli_does_not_report_success_on_write_failure(pipeline_setup, monkeypatch):
    """Real staged inputs reach the writer; its error becomes a failed CLI run."""
    from green_taxi.report import ReportWriter

    pipeline, _ = pipeline_setup
    pipeline.downloader.download()
    config = pipeline.settings.data_dir.parent / "run.json"
    config.write_text(pipeline.settings.model_dump_json())

    def fail(*args, **kwargs):
        raise OSError("output unavailable")

    monkeypatch.setattr(ReportWriter, "write", fail)
    result = CliRunner().invoke(main, ["--config", str(config), "--run"])
    assert result.exit_code != 0
    assert isinstance(result.exception, OSError)
    assert not pipeline.settings.processed_dir.exists()


def test_calculation_failure_preserves_previous_report(pipeline_setup):
    """Readable inputs with no usable amounts fail before replacing the last report."""
    pipeline, _ = pipeline_setup
    marker = pipeline.run()
    before = {path.name: path.read_bytes() for path in marker.parent.iterdir()}
    for path in pipeline.settings.raw_dir.glob("*.parquet"):
        frame = pd.read_parquet(path)
        frame["total_amount"] = float("nan")
        frame.to_parquet(path, index=False)
    with pytest.raises(ValueError, match="No usable"):
        pipeline.run()
    assert {path.name: path.read_bytes() for path in marker.parent.iterdir()} == before
