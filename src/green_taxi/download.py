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

SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
REQUIRED_COLUMNS = {"lpep_pickup_datetime", "total_amount"}
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
        """Validate or replace one input, then return its actual content identity."""
        filename = f"green_tripdata_{month}.parquet"
        destination = self.settings.raw_dir / filename
        url = f"{SOURCE_URL}/{filename}"
        downloaded = force or not destination.exists()
        if downloaded:
            self._fetch(url, destination, session)
        else:
            self._validate(destination)
        with destination.open("rb") as stream:
            checksum = hashlib.file_digest(stream, "sha256").hexdigest()
        logger.info("%s %s", "Downloaded" if downloaded else "Reused", filename)
        return StagedFile(
            month, destination, url, destination.stat().st_size, checksum, downloaded
        )

    def _fetch(self, url: str, destination: Path, session: requests.Session) -> None:
        """Publish a fully validated temporary file; preserve old data on failure."""
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
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
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
                for _ in parquet.iter_batches(batch_size=65536):
                    pass
        except pa.ArrowException as error:
            raise ValueError(f"Unreadable Parquet input: {path.name}") from error


@click.command()
@click.option("--data-dir", default="data", type=click.Path(path_type=Path))
@click.option(
    "--month",
    "months",
    multiple=True,
    type=click.Choice(["2025-01", "2025-02", "2025-03"]),
)
@click.option(
    "--force",
    is_flag=True,
    help="Refresh selected files; preserve existing files if refresh fails.",
)
def main(data_dir: Path, months: tuple[str, ...], force: bool) -> None:
    """Download raw inputs only; default to all three months. No reports yet."""
    settings = PipelineSettings(
        data_dir=data_dir, **({"months": months} if months else {})
    )
    configure_logging(settings)
    for item in TaxiDownloader(settings).download(force=force):
        click.echo(f"{item.path.name}: {item.size_bytes} bytes, sha256={item.sha256}")


if __name__ == "__main__":
    main()
