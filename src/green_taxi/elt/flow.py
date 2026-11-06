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
    """Return staged inputs from the supplied pipeline's shared downloader.

    force selects deliberate refresh. Partial downloads remain reusable on retry;
    only transient HTTP failures qualify for the bounded retry policy above.
    """
    return pipeline.downloader.download(force=force)


@task(name="Load raw sources", retries=0, persist_result=False, cache_policy=None)
def load_sources(pipeline, inputs):
    """Load the staged inputs through the pipeline and return None.

    The pipeline owns connection cleanup. Schema, identity and storage errors
    propagate without retry, preventing transformation or publication.
    """
    pipeline.load(inputs)


@task(name="Transform with SQL", retries=0, persist_result=False, cache_policy=None)
def calculate_report(pipeline, inputs):
    """Return RevenueResult from the pipeline's SQL calculation for these inputs.

    No rules are copied into this task. Sub-cent and no-usable-row failures
    propagate without retry, preventing the save stage.
    """
    return pipeline.calculate(inputs)


@task(name="Save ELT report", retries=0, persist_result=False, cache_policy=None)
def save_report(pipeline, inputs, result, context):
    """Publish inputs, result and run context; return the metadata path as text.

    The shared reporting object validates and writes outputs. Writer failures
    propagate; this task does not calculate revenue or claim success early.
    """
    return str(pipeline.reporting.save(inputs, result, context))


@flow(
    name="Green Taxi ELT",
    flow_run_name="green-taxi-elt",
    retries=0,
    persist_result=False,
)
def taxi_flow(config_path=None, data_dir=None, force=False):
    """Run download, load, transform and save in order; return metadata path text.

    Inputs select trusted JSON/storage and deliberate refresh. The same pipeline
    objects serve the plain command; no schedule or .submit() calls are added.
    Connections are opened inside stages, never passed between tasks.
    """
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
    """Run one flow against an explicit server and print its metadata path.

    Require PREFECT_API_URL before calling the flow. Command options select
    JSON, storage and refresh. Missing server configuration raises ClickException;
    runtime failures fail the process. No hidden server is started by this CLI.
    """
    if not os.environ.get("PREFECT_API_URL"):
        raise click.ClickException(
            "Set PREFECT_API_URL or use the Compose elt-flow service."
        )
    click.echo(taxi_flow(config_path, data_dir, force))


if __name__ == "__main__":
    main()
