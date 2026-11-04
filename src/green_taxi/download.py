"""Stage complete Green Taxi files without trusting filenames alone."""

import hashlib
import logging
from contextlib import nullcontext
from dataclasses import dataclass
from pathlib import Path
from tempfile import NamedTemporaryFile

import click
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from green_taxi.logging import configure_logging
from green_taxi.settings import PipelineSettings

REQUIRED_COLUMNS = {"lpep_pickup_datetime", "total_amount"}
DOWNLOAD_CHUNK_BYTES = 1024 * 1024
PARQUET_BATCH_ROWS = 65536
logger = logging.getLogger("green_taxi.download")


@dataclass(frozen=True)
class StagedFile:
    """Identify one staged input for later run metadata; downloaded marks reuse."""

    month: str
    path: Path
    source_url: str
    size_bytes: int
    sha256: str
    downloaded: bool


class TaxiDownloader:
    """Own staging and HTTP I/O, never revenue calculation or Prefect startup.

    Supply validated settings and optionally a caller-owned HTTP session.
    download() returns immutable input records or propagates an explicit failure.
    """

    def __init__(
        self, settings: PipelineSettings, session: requests.Session | None = None
    ) -> None:
        """Store dependencies without creating directories or network connections."""
        self.settings = settings
        self.session = session

    def download(self, *, force: bool = False) -> tuple[StagedFile, ...]:
        """Stage selected months, reusing readable files unless force is true.

        Completed months survive a later month's failure. Retry revalidates them.
        The caller owns an injected session; internally created sessions close here.
        force refreshes selected files. Returns one StagedFile per selected month
        in configuration order. HTTP, filesystem, and validation errors propagate;
        no success result is returned for a failed batch. Creates raw_dir only
        when called, not during object construction.
        """
        self.settings.raw_dir.mkdir(parents=True, exist_ok=True)
        context = (
            nullcontext(self.session)
            if self.session is not None
            else requests.Session()
        )
        with context as session:
            return tuple(
                self._stage(month, session, force) for month in self.settings.months
            )

    def _stage(self, month: str, session: requests.Session, force: bool) -> StagedFile:
        """Validate or replace one input, then return its actual content identity.

        month comes from validated settings, session performs HTTP requests, and
        force selects refresh rather than reuse. Reads staged bytes to calculate
        SHA-256 and size. Existing corrupt files raise ValueError instead of
        silently triggering a download; force allows deliberate repair.
        """
        filename = f"green_tripdata_{month}.parquet"
        destination = self.settings.raw_dir / filename
        url = f"{str(self.settings.source_url).rstrip('/')}/{filename}"
        downloaded = force or not destination.exists()
        action = "download" if downloaded else "reuse"
        logger.info("Starting %s for %s", action, filename)
        try:
            result = self._stage_input(month, destination, url, session, downloaded)
        except requests.RequestException as error:
            response = getattr(error, "response", None)
            status = response.status_code if response is not None else "unavailable"
            logger.error(
                "Staging failed for %s: network=%s status=%s timeout=%ss; "
                "no replacement published; retry the command "
                "when connectivity recovers",
                filename,
                type(error).__name__,
                status,
                self.settings.timeout_seconds,
            )
            raise
        except (OSError, ValueError) as error:
            logger.error(
                "Staging failed for %s: %s; check local file/storage",
                filename,
                type(error).__name__,
            )
            raise
        logger.info(
            "%s %s (%s bytes)",
            "Downloaded" if downloaded else "Reused",
            filename,
            result.size_bytes,
        )
        return result

    def _stage_input(
        self,
        month: str,
        destination: Path,
        url: str,
        session: requests.Session,
        downloaded: bool,
    ) -> StagedFile:
        """Publish or validate one input and return its content identity.

        Called by _stage so logging stays separate from the file operations.
        Failures propagate to _stage; a result is returned only after validation
        and checksum calculation. Does not suppress network or storage errors.
        """
        if downloaded:
            self._fetch(url, destination, session)
        else:
            self._validate(destination)
        with destination.open("rb") as stream:
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
        return StagedFile(
            month, destination, url, destination.stat().st_size, checksum, downloaded
        )

    def _fetch(self, url: str, destination: Path, session: requests.Session) -> None:
        """Publish a fully validated temporary file; preserve old data on failure.

        Inputs are the configured source URL, destination, and HTTP session. Streams
        bytes into the destination directory, validates them, then replaces the
        destination. Returns None. HTTP/filesystem/validation errors propagate.
        Removes this call's temporary file on normal exception paths; a killed
        process may leave a .part file, which is never selected as a staged input.
        """
        with NamedTemporaryFile(
            dir=destination.parent, suffix=".part", delete=False
        ) as stream:
            temporary = Path(stream.name)
        try:
            with session.get(
                url, stream=True, timeout=self.settings.timeout_seconds
            ) as response:
                response.raise_for_status()
                with temporary.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                        if chunk:
                            output.write(chunk)
            self._validate(temporary)
            temporary.replace(destination)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _validate(path: Path) -> None:
        """Read every Parquet batch and require nonempty input with key columns.

        This checks file readability and structure, not the correctness of fares.
        Row-level acceptance rules belong to the later calculation stage.
        """
        try:
            with pq.ParquetFile(path) as parquet:
                if not REQUIRED_COLUMNS.issubset(parquet.schema_arrow.names):
                    raise ValueError(
                        "Input needs pickup datetime and total_amount columns."
                    )
                if parquet.metadata.num_rows == 0:
                    raise ValueError("Input contains no ride records.")
                for _ in parquet.iter_batches(batch_size=PARQUET_BATCH_ROWS):
                    pass
        except pa.ArrowException as error:
            raise ValueError(f"Unreadable Parquet input: {path.name}") from error


@click.command()
@click.option(
    "--config",
    "config_path",
    type=click.Path(path_type=Path, exists=True, dir_okay=False),
)
@click.option("--data-dir", default=None, type=click.Path(path_type=Path))
@click.option(
    "--force",
    is_flag=True,
    help="Refresh selected files; preserve existing files if refresh fails.",
)
def main(config_path: Path | None, data_dir: Path | None, force: bool) -> None:
    """Download raw inputs selected by JSON; no revenue reports are produced.

    config_path chooses a JSON file, or defaults.json when omitted. data_dir
    optionally overrides storage for a local path or container mount. force
    refreshes selected months. Prints filenames, sizes, and checksums; returns
    None. Configuration failures become Click errors before any HTTP request;
    runtime download failures propagate with an unsuccessful process exit.
    """
    try:
        settings = PipelineSettings.from_file(config_path, data_dir=data_dir)
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    configure_logging(settings)
    for item in TaxiDownloader(settings).download(force=force):
        click.echo(f"{item.path.name}: {item.size_bytes} bytes, sha256={item.sha256}")


if __name__ == "__main__":
    main()
