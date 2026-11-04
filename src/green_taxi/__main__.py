"""Inspect settings without starting downloads or a Prefect server."""

from pathlib import Path

import click

from green_taxi.settings import PipelineSettings


@click.command()
@click.option("--data-dir", default="data", type=click.Path(path_type=Path))
def main(data_dir: Path) -> None:
    """Print validated storage settings; the data pipeline is not connected yet."""
    settings = PipelineSettings(data_dir=data_dir)
    click.echo(settings.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
