"""Manually run existing stage objects with visible Prefect tasks; no schedule."""

import os
from pathlib import Path

import click
import requests
from prefect import flow, get_run_logger, task

from green_taxi.logging import configure_logging
from green_taxi.pipeline import TaxiPipeline
from green_taxi.settings import PipelineSettings


def retry_download(task, task_run, state) -> bool:
    """Retry transient HTTP failures only, within the download task's retry limit.

    Prefect supplies the failed state. Never retry certificate, invalid-data,
    filesystem, authentication, or missing-source errors. This function schedules
    nothing itself; it returns a decision for Prefect's bounded retry policy.
    """
    try:
        state.result()
    except requests.exceptions.SSLError:
        return False
    except (
        requests.Timeout,
        requests.ConnectionError,
        requests.exceptions.ChunkedEncodingError,
    ):
        return True
    except requests.HTTPError as error:
        return error.response is not None and (
            error.response.status_code == 429 or 500 <= error.response.status_code < 600
        )
    except Exception:
        return False
    return False


@task(
    name="Download taxi files",
    task_run_name="download-taxi-files",
    retries=2,
    retry_delay_seconds=[1, 3],
    retry_condition_fn=retry_download,
    persist_result=False,
    cache_policy=None,
)
def download_inputs(pipeline: TaxiPipeline, force: bool) -> tuple:
    """Stage inputs using the shared downloader; partial successes survive retry."""
    return pipeline.downloader.download(force=force)


@task(
    name="Calculate daily revenue",
    task_run_name="calculate-daily-revenue",
    retries=0,
    persist_result=False,
    cache_policy=None,
)
def calculate_report(pipeline: TaxiPipeline, inputs: tuple):
    """Use the shared calculator; invalid data fails immediately, without retry."""
    return pipeline.calculator.calculate(item.path for item in inputs)


@task(
    name="Save checked report",
    task_run_name="save-checked-report",
    retries=0,
    persist_result=False,
    cache_policy=None,
)
def save_report(pipeline: TaxiPipeline, inputs: tuple, result, context: dict) -> str:
    """Save using shared code; log verified totals in Prefect after publication."""
    path = pipeline.save(inputs, result, context)
    period = pipeline.settings.reporting_period
    get_run_logger().info(
        "Report %s to %s (end exclusive): %s accepted rides, %s days, USD %s; saved %s",
        period.start,
        period.end,
        result.counts["rows_used"],
        len(result.facts),
        result.total_amount,
        path,
    )
    return str(path)


@flow(
    name="Green Taxi Batch",
    flow_run_name="green-taxi-batch",
    retries=0,
    persist_result=False,
)
def taxi_flow(
    config_path: str | None = None, data_dir: str | None = None, force: bool = False
) -> str:
    """Manually stage, calculate, and save in order using the same Python objects.

    Inputs select trusted JSON/storage and deliberate refresh. No .submit(),
    schedule, result pickle, or copied business rules. The CLI below requires an
    explicit Prefect API URL; direct Python callers manage Prefect settings.
    """
    settings = PipelineSettings.from_file(
        Path(config_path) if config_path else None,
        data_dir=Path(data_dir) if data_dir else None,
    )
    configure_logging(settings)
    pipeline = TaxiPipeline.from_settings(settings)
    context = pipeline.start_run()
    inputs = download_inputs(pipeline, force)
    result = calculate_report(pipeline, inputs)
    return save_report(pipeline, inputs, result, context)


@click.command()
@click.option("--config", "config_path", type=click.Path(exists=True, dir_okay=False))
@click.option("--data-dir", default=None, type=click.Path())
@click.option("--force", is_flag=True)
def main(config_path: str | None, data_dir: str | None, force: bool) -> None:
    """Start one flow against a configured server; print the saved metadata path.

    Requires PREFECT_API_URL before flow invocation. The plain Python command
    remains available without any server. Runtime errors fail the process.
    """
    if not os.environ.get("PREFECT_API_URL"):
        raise click.ClickException(
            "Set PREFECT_API_URL or use the Compose flow service."
        )
    click.echo(taxi_flow(config_path, data_dir, force))


if __name__ == "__main__":
    main()
