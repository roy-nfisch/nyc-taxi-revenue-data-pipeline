"""Connect staging, calculation, and report writing without copying their rules."""

import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from green_taxi.download import StagedFile, TaxiDownloader
from green_taxi.report import ReportWriter
from green_taxi.revenue import DailyRevenueCalculator, RevenueResult
from green_taxi.settings import PipelineSettings

logger = logging.getLogger(__name__)


def code_identity() -> dict:
    """Return checkout revision/dirty status, or explicit unknowns when unavailable.

    Inspect only this package's checkout, not an unrelated caller's repository.
    Installed wheels report supplied build metadata, or explicit unknowns. Git calls are
    bounded and read-only; unavailable Git does not prevent data processing.
    """
    root = Path(__file__).resolve().parents[2]
    unknown = {"revision": "unknown", "dirty": "unknown"}
    if not (root / ".git").exists():
        dirty = {"true": True, "false": False}.get(
            os.environ.get("GREEN_TAXI_BUILD_DIRTY", ""), "unknown"
        )
        return {
            "revision": os.environ.get("GREEN_TAXI_BUILD_REVISION") or "unknown",
            "dirty": dirty,
        }
    try:
        revision = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            timeout=2,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return unknown
    return {"revision": revision, "dirty": bool(status)}


class TaxiPipeline:
    """Coordinate existing objects; run manually in Python or the Prefect flow.

    Dependencies are supplied, so tests can replace network I/O without replacing
    the calculation or writer. No work runs during construction. Concurrent runs
    targeting the same storage directory are not supported.
    """

    def __init__(
        self,
        settings: PipelineSettings,
        downloader: TaxiDownloader,
        calculator: DailyRevenueCalculator,
        writer: ReportWriter,
    ) -> None:
        """Store validated configuration and the three stage objects."""
        self.settings = settings
        self.downloader = downloader
        self.calculator = calculator
        self.writer = writer

    def run(self, *, force: bool = False) -> Path:
        """Stage inputs, calculate, and save; return the successful metadata path.

        force refreshes inputs; otherwise validated files are reused. Record run
        UUID, UTC times, code identity, and actual staged content identities.
        Failures propagate without returning success. Outputs replace old tables,
        never append. A prior report remains a prior run, not evidence of this one.
        """
        context = self.start_run()
        inputs = self.downloader.download(force=force)
        result = self.calculator.calculate(item.path for item in inputs)
        return self.save(inputs, result, context)

    @classmethod
    def from_settings(cls, settings: PipelineSettings) -> "TaxiPipeline":
        """Compose the same stage objects for plain Python and Prefect callers."""
        return cls(
            settings,
            TaxiDownloader(settings),
            DailyRevenueCalculator(settings.reporting_period),
            ReportWriter(settings.processed_dir),
        )

    def start_run(self) -> dict:
        """Create UTC start time, UUID, and code identity before any stage runs."""
        context = {
            "run_id": str(uuid4()),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "code": code_identity(),
        }
        logger.info("Starting run %s", context["run_id"])
        return context

    def save(
        self, inputs: tuple[StagedFile, ...], result: RevenueResult, context: dict
    ) -> Path:
        """Add input lineage, publish checked outputs, and return metadata path.

        Both execution paths call this method; neither copies the metadata rules.
        Writer failures propagate without a completed-run log message.
        """
        metadata = context | {
            "inputs": [
                {
                    "filename": item.path.name,
                    "month": item.month,
                    "source_url": item.source_url,
                    "size_bytes": item.size_bytes,
                    "sha256": item.sha256,
                    "downloaded": item.downloaded,
                }
                for item in inputs
            ],
        }
        path = self.writer.write(result, self.settings.reporting_period, metadata)
        logger.info("Completed run %s", context["run_id"])
        return path
