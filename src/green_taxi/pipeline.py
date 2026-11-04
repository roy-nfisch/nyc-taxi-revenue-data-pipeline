"""Connect staging, calculation, and report writing without copying their rules."""

import logging
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from green_taxi.download import TaxiDownloader
from green_taxi.report import ReportWriter
from green_taxi.revenue import DailyRevenueCalculator
from green_taxi.settings import PipelineSettings

logger = logging.getLogger(__name__)


def code_identity() -> dict:
    """Return checkout revision/dirty status, or explicit unknowns when unavailable.

    Inspect only this package's checkout, not an unrelated caller's repository.
    Installed wheels without a checkout have no assumed revision. Git calls are
    bounded and read-only; unavailable Git does not prevent data processing.
    """
    root = Path(__file__).resolve().parents[2]
    unknown = {"revision": "unknown", "dirty": "unknown"}
    if not (root / ".git").exists():
        return unknown
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
    """Coordinate existing objects; run manually in Python or a later Prefect flow.

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
        started = datetime.now(timezone.utc).isoformat()
        run_id = str(uuid4())
        logger.info("Starting run %s", run_id)
        inputs = self.downloader.download(force=force)
        result = self.calculator.calculate(item.path for item in inputs)
        metadata = {
            "run_id": run_id,
            "started_at": started,
            "code": code_identity(),
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
        logger.info("Completed run %s", run_id)
        return path
