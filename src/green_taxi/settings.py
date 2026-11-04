"""Define one configuration contract for local Python and future containers.

This module validates settings, not taxi records. It does not load environment
variables or .env files, create storage, download data, or start Prefect.
"""

import json
from datetime import date
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

Month = Annotated[str, Field(pattern=r"^[0-9]{4}-(0[1-9]|1[0-2])$")]
DEFAULT_CONFIG = Path(__file__).with_name("defaults.json")


def default_source_url() -> str:
    """Read the public source endpoint from packaged JSON, not a Python constant.

    Used only when constructing settings without an explicit source_url. Missing
    or malformed packaged configuration fails visibly; imports do not read it.
    """
    return json.loads(DEFAULT_CONFIG.read_text(encoding="utf-8"))["source_url"]


class PipelineSettings(BaseModel):
    """Hold validated run settings that cannot be reassigned after construction.

    Inputs:
        months: One or more distinct calendar months in YYYY-MM format. Required
            for direct construction; from_file() reads the selection from JSON.
        data_dir: Storage root, defaulting to ./data relative to the caller's
            working directory. An absolute path can point to a container mount.
        timeout_seconds: Positive HTTP timeout up to 300 seconds, default 60.
            This defines a setting; constructing it does not make an HTTP call.
        log_level: DEBUG, INFO, WARNING, or ERROR; defaults to INFO.
        source_url: HTTPS dataset directory, defaulting to packaged JSON. Must
            not contain credentials, a query, or a fragment. Supply only a trusted
            source: syntax validation is not a network access-control policy.

    Result:
        A settings object with an absolute data_dir and derived raw_dir and
        processed_dir paths. Unknown fields and invalid values raise Pydantic
        ValidationError. Compatible input types may be converted by Pydantic.

    Side effects:
        Path resolution can inspect the filesystem and resolve symlinks, but no
        directories are created and writability is not checked. from_file()
        explicitly reads configuration; importing this module does not read it.
        Direct construction also reads the packaged URL if source_url is omitted.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", validate_default=True)

    months: tuple[Month, ...] = Field(min_length=1)
    data_dir: Path = Path("data")
    timeout_seconds: float = Field(default=60, gt=0, le=300, allow_inf_nan=False)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    source_url: HttpUrl = Field(default_factory=default_source_url)

    @field_validator("source_url")
    @classmethod
    def validate_source_url(cls, value: HttpUrl) -> HttpUrl:
        """Return a clean HTTPS base URL; reject embedded secrets and suffixes."""
        if value.scheme != "https" or value.username or value.password:
            raise ValueError("Source URL must use HTTPS without credentials.")
        if value.query is not None or value.fragment is not None:
            raise ValueError("Source URL must not contain a query or fragment.")
        return value

    @classmethod
    def from_file(
        cls, config_path: Path | None = None, *, data_dir: Path | None = None
    ) -> "PipelineSettings":
        """Load a JSON object, then return validated, immutable settings.

        Args:
            config_path: User-supplied JSON file, or the packaged defaults.json.
            data_dir: Optional override of JSON storage, useful for Docker mounts.
        Returns:
            PipelineSettings; relative data paths use the current working directory,
            not the JSON file's directory. Month order is preserved.
        Raises:
            OSError: The file cannot be read.
            ValueError: JSON is malformed or its top-level value is not an object.
            ValidationError: Missing months, unknown keys, or invalid settings.

        Reads configuration only. Never silently falls back after a supplied
        file fails, and never creates storage or starts a network connection.
        """
        values = json.loads((config_path or DEFAULT_CONFIG).read_text(encoding="utf-8"))
        if not isinstance(values, dict):
            raise ValueError("Configuration must be a JSON object.")
        if data_dir is not None:
            values["data_dir"] = data_dir
        return cls.model_validate(values)

    @field_validator("months")
    @classmethod
    def unique_months(cls, months: tuple[Month, ...]) -> tuple[Month, ...]:
        """Check month uniqueness and real calendar years after format validation.

        Args:
            months: The validated selection supplied to PipelineSettings.
        Returns:
            The same tuple in the same order; no months are added or removed.
        Raises:
            ValueError: If a month appears twice. Pydantic includes this in a
            ValidationError, preventing the same source from being selected twice.
        """
        if len(months) != len(set(months)):
            raise ValueError("Select each month only once.")
        for month in months:
            date.fromisoformat(f"{month}-01")
        return months

    @field_validator("data_dir", mode="before")
    @classmethod
    def resolve_storage(cls, value: object) -> Path:
        """Resolve a supplied storage root before Pydantic validates the Path.

        Args:
            value: A nonblank string or pathlib.Path; ./data is the default.
        Returns:
            An absolute Path with the user-home shortcut and symlinks resolved.
            Relative paths are interpreted from the current working directory.
        Raises:
            ValueError: If the input is blank or is not a string/Path.
            OSError: If filesystem path resolution fails.

        Resolving may inspect the filesystem. It does not create directories,
        require the destination to exist, or prove that the process can write there.
        """
        if not isinstance(value, (str, Path)) or not str(value).strip():
            raise ValueError("Supply a non-empty data directory.")
        return Path(value).expanduser().resolve()

    @property
    def raw_dir(self) -> Path:
        """Return data_dir/raw, where the downloader will stage source files.

        Reads the object's resolved data_dir; accepts no additional input.
        This only constructs a Path. It does not create or validate the directory.
        """
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        """Return data_dir/processed, where reporting will write generated files.

        Reads the same storage root as raw_dir but keeps outputs separate from
        original inputs. This property performs no writes or directory creation.
        """
        return self.data_dir / "processed"
