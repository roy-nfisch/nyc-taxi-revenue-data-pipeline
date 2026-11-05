"""Show the same ELT objects as four sequential Prefect stages; no schedule."""

import os
from pathlib import Path

import click
from prefect import flow, task

from green_taxi.elt.pipeline import ELTPipeline
from green_taxi.etl.flow import retry_download
from green_taxi.logging import configure_logging
from green_taxi.settings import PipelineSettings


@task(
    name="Download ELT inputs",
    retries=2,
    retry_delay_seconds=[1, 3],
    retry_condition_fn=retry_download,
    persist_result=False,
    cache_policy=None,
)
def download_inputs(pipeline, force):
    """Reuse checked staging; retry only transient HTTP failures."""
    return pipeline.downloader.download(force=force)


@task(name="Load raw sources", retries=0, persist_result=False, cache_policy=None)
def load_sources(pipeline, inputs):
    """Commit checked source versions; never retry schema or storage failures."""
    pipeline.load(inputs)


@task(name="Transform with SQL", retries=0, persist_result=False, cache_policy=None)
def calculate_report(pipeline, inputs):
    """Return checked SQL results; sub-cents fail before saving."""
    return pipeline.calculate(inputs)


@task(name="Save ELT report", retries=0, persist_result=False, cache_policy=None)
def save_report(pipeline, inputs, result, context):
    """Publish through the shared writer and lineage code."""
    return str(pipeline.reporting.save(inputs, result, context))


@flow(
    name="Green Taxi ELT",
    flow_run_name="green-taxi-elt",
    retries=0,
    persist_result=False,
)
def taxi_flow(config_path=None, data_dir=None, force=False):
    """Run sequential stages with plain values, not serialized connections."""
    settings = PipelineSettings.from_file(
        Path(config_path) if config_path else None,
        data_dir=Path(data_dir) if data_dir else None,
    )
    configure_logging(settings)
    pipeline = ELTPipeline(settings)
    context = pipeline.start_run()
    inputs = download_inputs(pipeline, force)
    load_sources(pipeline, inputs)
    result = calculate_report(pipeline, inputs)
    return save_report(pipeline, inputs, result, context)


@click.command()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--data-dir", default=None, type=click.Path())
@click.option("--force", is_flag=True)
def main(config_path, data_dir, force):
    """Run against an explicit Prefect server; do not start a hidden server."""
    if not os.environ.get("PREFECT_API_URL"):
        raise click.ClickException(
            "Set PREFECT_API_URL or use the Compose elt-flow service."
        )
    click.echo(taxi_flow(config_path, data_dir, force))


if __name__ == "__main__":
    main()
