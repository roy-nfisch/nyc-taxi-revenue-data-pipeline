"""Inspect settings or explicitly run the batch with python -m green_taxi.

Click parses terminal arguments and provides --help. Importing this module
does not execute the command. Data work requires the --run option.
"""

from pathlib import Path

import click

from green_taxi.logging import configure_logging
from green_taxi.pipeline import TaxiPipeline
from green_taxi.settings import PipelineSettings


@click.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
)
@click.option("--data-dir", default=None, type=click.Path(path_type=Path))
@click.option(
    "--run",
    "run_pipeline",
    is_flag=True,
    help="Download, calculate, and save the report.",
)
@click.option(
    "--force", is_flag=True, help="With --run, refresh selected source files."
)
def main(
    config_path: Path | None, data_dir: Path | None, run_pipeline: bool, force: bool
) -> None:
    """Print settings, or compose and run the existing objects when --run is set.

    Args:
        config_path: JSON file selected by --config, or packaged defaults.json.
        data_dir: Optional --data-dir override; otherwise use JSON or ./data.
            Relative storage paths resolve from the terminal's current directory.
        run_pipeline: Run all stages only when explicitly requested with --run.
        force: Refresh selected inputs; requires --run.
    Returns:
        None. Prints settings JSON or the saved metadata path to standard output.
    Raises:
        ClickException: Configuration cannot be read or validated. Click displays
        a clear error and exits unsuccessfully. It also handles parsing errors.

    Without --run, creates no storage or network traffic. With --run, configures
    logging, stages inputs, and replaces reports. Runtime errors fail the command.
    Does not start Prefect; the separate flow reuses the same objects.
    """
    if force and not run_pipeline:
        raise click.UsageError("--force requires --run.")
    try:
        settings = PipelineSettings.from_file(config_path, data_dir=data_dir)
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    if not run_pipeline:
        click.echo(settings.model_dump_json(indent=2))
        return
    configure_logging(settings)
    pipeline = TaxiPipeline.from_settings(settings)
    click.echo(str(pipeline.run(force=force)))


if __name__ == "__main__":
    main()
