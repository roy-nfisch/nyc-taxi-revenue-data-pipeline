"""Expose a settings-inspection command through python -m green_taxi.

Click parses terminal arguments and provides --help. Importing this module
does not execute the command. The entry point does not run the data pipeline.
"""

from pathlib import Path

import click

from green_taxi.settings import PipelineSettings


@click.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
)
@click.option("--data-dir", default=None, type=click.Path(path_type=Path))
def main(config_path: Path | None, data_dir: Path | None) -> None:
    """Print validated settings without running downloads or other pipeline work.

    Args:
        config_path: JSON file selected by --config, or packaged defaults.json.
        data_dir: Optional --data-dir override; otherwise use JSON or ./data.
            Relative storage paths resolve from the terminal's current directory.
    Returns:
        None. Prints the settings as JSON to standard output.
    Raises:
        ClickException: Configuration cannot be read or validated. Click displays
        a clear error and exits unsuccessfully. It also handles parsing errors.

    Does not configure logging, create storage directories, read taxi files,
    or start Prefect. Other settings can be supplied in JSON rather than adding
    a terminal option for every field.
    """
    try:
        settings = PipelineSettings.from_file(config_path, data_dir=data_dir)
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(settings.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
