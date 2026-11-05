"""Shared run identity and checked report publication for ETL and ELT."""

import logging
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from green_taxi.download import StagedFile
from green_taxi.report import ReportWriter
from green_taxi.reporting import RevenueResult
from green_taxi.settings import PipelineSettings

logger = logging.getLogger("green_taxi.pipeline")


def code_identity(root: Path) -> dict:
    """Return checkout revision/dirty status, or explicit unknowns when unavailable.

    Inspect only this package's checkout, not an unrelated caller's repository.
    Installed wheels report supplied build metadata, or explicit unknowns. Git calls are
    bounded and read-only; unavailable Git does not prevent data processing.
    """
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


class RunReport:
    """Own run metadata and publication; never download or calculate rides."""

    def __init__(self, settings: PipelineSettings, writer: ReportWriter):
        """Store settings and the writer without performing I/O."""
        self.settings = settings
        self.writer = writer

    def start_run(self, code: dict) -> dict:
        """Create UTC start time, UUID, and code identity before any stage runs."""
        context = {
            "run_id": str(uuid4()),
            "started_at": datetime.now(timezone.utc).isoformat(),
            "code": code,
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
                    "source_url": item.source_url if item.downloaded else None,
                    "configured_source_url": item.source_url,
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
