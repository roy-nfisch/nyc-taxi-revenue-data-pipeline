"""Connect checked staging, warehouse SQL and separate ELT report publication."""

from importlib.metadata import version
from pathlib import Path

from green_taxi.download import TaxiDownloader
from green_taxi.elt.warehouse import TaxiWarehouse
from green_taxi.report import ReportWriter
from green_taxi.run_report import RunReport, code_identity


class ELTPipeline:
    """Compose existing objects; keep all SQL and money rules in their owners."""

    def __init__(self, settings, downloader=None):
        """Store settings and collaborators without opening storage or a database."""
        self.settings = settings
        self.downloader = (
            downloader if downloader is not None else TaxiDownloader(settings)
        )
        self.database_path = settings.data_dir / "elt" / "warehouse.duckdb"
        self.reporting = RunReport(
            settings, ReportWriter(settings.data_dir / "elt" / "processed")
        )

    def start_run(self):
        """Capture run identity before staging, using the shared report contract."""
        return self.reporting.start_run(
            code_identity(Path(__file__).resolve().parents[3])
        ) | {
            "execution_mode": "elt",
            "engine": {"name": "duckdb", "version": version("duckdb")},
        }

    def load(self, inputs):
        """Load checked snapshots and close the connection before returning."""
        with TaxiWarehouse(self.database_path) as warehouse:
            warehouse.load(inputs)

    def calculate(self, inputs):
        """Reopen the warehouse and calculate only the selected source versions."""
        with TaxiWarehouse(self.database_path) as warehouse:
            return warehouse.calculate(inputs, self.settings.reporting_period)

    def run(self, *, force=False):
        """Run one manual ELT batch; failures never publish success metadata."""
        context = self.start_run()
        inputs = self.downloader.download(force=force)
        self.load(inputs)
        result = self.calculate(inputs)
        return self.reporting.save(inputs, result, context)
