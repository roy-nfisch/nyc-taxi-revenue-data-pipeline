"""Run a manual ELT batch without changing the default ETL command."""

from pathlib import Path

import click

from green_taxi.elt.pipeline import ELTPipeline
from green_taxi.logging import configure_logging
from green_taxi.settings import PipelineSettings


@click.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
)
@click.option("--data-dir", type=click.Path(path_type=Path))
@click.option("--force", is_flag=True)
def main(config_path, data_dir, force):
    """Stage, load, calculate and publish one batch; print its metadata path."""
    try:
        settings = PipelineSettings.from_file(config_path, data_dir=data_dir)
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    configure_logging(settings)
    click.echo(str(ELTPipeline(settings).run(force=force)))


if __name__ == "__main__":
    main()
