"""Validated run configuration shared by local Python and containers."""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

Month = Literal["2025-01", "2025-02", "2025-03"]


class PipelineSettings(BaseModel):
    """Validate months, storage, timeout, and logging before any work starts.

    Relative data paths resolve from the caller's working directory. A container
    can supply an absolute mounted path. Construction never creates directories.
    """

    model_config = ConfigDict(frozen=True, extra="forbid", validate_default=True)

    months: tuple[Month, ...] = Field(
        default=("2025-01", "2025-02", "2025-03"), min_length=1, max_length=3
    )
    data_dir: Path = Path("data")
    timeout_seconds: float = Field(default=60, gt=0, le=300, allow_inf_nan=False)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator("months")
    @classmethod
    def unique_months(cls, months: tuple[Month, ...]) -> tuple[Month, ...]:
        """Return selected months; reject repeats that would double input."""
        if len(months) != len(set(months)):
            raise ValueError("Select each month only once.")
        return months

    @field_validator("data_dir", mode="before")
    @classmethod
    def resolve_storage(cls, value: object) -> Path:
        """Return an absolute storage path; reject blank strings without I/O."""
        if not isinstance(value, (str, Path)) or not str(value).strip():
            raise ValueError("Supply a non-empty data directory.")
        return Path(value).expanduser().resolve()

    @property
    def raw_dir(self) -> Path:
        """Return the staging directory without creating it."""
        return self.data_dir / "raw"

    @property
    def processed_dir(self) -> Path:
        """Return the report directory without creating it."""
        return self.data_dir / "processed"
