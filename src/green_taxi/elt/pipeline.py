"""Connect checked staging, warehouse SQL and separate ELT report publication."""

from importlib.metadata import version
from pathlib import Path

from green_taxi.download import TaxiDownloader
from green_taxi.elt.warehouse import TaxiWarehouse
from green_taxi.report import ReportWriter
from green_taxi.run_report import RunReport, code_identity


class ELTPipeline:
    """Coordinate staging, raw loading, SQL calculation and checked publication.

    Supply validated settings and optionally a downloader for tests. Construction
    performs no network or database work. Use one writer per storage directory;
    the warehouse and report writer retain their own validation responsibilities.
    """

    def __init__(self, settings, downloader=None):
        """Store settings and stage objects without opening storage or a database.

        downloader defaults to TaxiDownloader(settings). Raw files stay shared
        with ETL; the database and report destinations are under data_dir/elt.
        """
        self.settings = settings
        self.downloader = (
            downloader if downloader is not None else TaxiDownloader(settings)
        )
        self.database_path = settings.data_dir / "elt" / "warehouse.duckdb"
        self.reporting = RunReport(
            settings, ReportWriter(settings.data_dir / "elt" / "processed")
        )

    def start_run(self):
        """Return run UUID, UTC start time, code identity and DuckDB version.

        Read build/package metadata before staging. Does not open DuckDB or
        create storage; a missing optional engine distribution fails here.
        """
        return self.reporting.start_run(
            code_identity(Path(__file__).resolve().parents[3])
        ) | {
            "execution_mode": "elt",
            "engine": {"name": "duckdb", "version": version("duckdb")},
        }

    def load(self, inputs):
        """Load staged inputs and return None after closing the connection.

        The warehouse verifies source identity and commits each source separately.
        A failure propagates and stops the batch; no report is published here.
        """
        with TaxiWarehouse(self.database_path) as warehouse:
            warehouse.load(inputs)

    def calculate(self, inputs):
        """Return RevenueResult for the selected staged inputs and reporting period.

        Reopen the warehouse, verify registered versions and run SQL. Close the
        connection on success or failure. This method never writes report files.
        """
        with TaxiWarehouse(self.database_path) as warehouse:
            return warehouse.calculate(inputs, self.settings.reporting_period)

    def run(self, *, force=False):
        """Stage, load, calculate and save; return the successful metadata path.

        force refreshes source files; otherwise checked files are reused. Reports
        replace previous outputs rather than append. Errors propagate before a
        successful return; an older report is not evidence this run succeeded.
        """
        context = self.start_run()
        inputs = self.downloader.download(force=force)
        self.load(inputs)
        result = self.calculate(inputs)
        return self.reporting.save(inputs, result, context)
