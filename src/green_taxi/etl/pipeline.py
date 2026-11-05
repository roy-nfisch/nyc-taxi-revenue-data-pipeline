"""Connect staging, calculation, and report writing without copying their rules."""

import logging
from pathlib import Path

from green_taxi.download import TaxiDownloader
from green_taxi.etl.revenue import DailyRevenueCalculator
from green_taxi.report import ReportWriter
from green_taxi.run_report import RunReport
from green_taxi.run_report import code_identity as run_code_identity
from green_taxi.settings import PipelineSettings

logger = logging.getLogger("green_taxi.pipeline")


def code_identity() -> dict:
    """Return provenance for this checkout or the installed build."""
    return run_code_identity(Path(__file__).resolve().parents[3])


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
        self.reporting = RunReport(settings, writer)

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
        """Start a run using the shared metadata contract."""
        return self.reporting.start_run(code_identity())

    def save(self, inputs, result, context) -> Path:
        """Publish through the shared writer and lineage contract."""
        return self.reporting.save(inputs, result, context)
