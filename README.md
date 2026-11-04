# Green Taxi Daily Revenue Pipeline

We are planning a small, object-oriented Python **batch ETL pipeline** that turns NYC Green
Taxi rides into a daily revenue report. The focus is reliable downloads, clear
data rules, useful tests, and a process someone else can run.

**Status: settings, raw-file downloading, and daily calculation are available.**
Saving reports and connecting the complete pipeline are still planned. The accepted
data rules, simple OOD plan, and fact/date layout are in
[ADR 001](docs/adr/001-pipeline-design.md). Docker and Prefect are required.
Follow our progress in [TASKS.md](TASKS.md).

## What we want to demonstrate

Build a small taxi-data pipeline and test what happens when files are incomplete,
records are invalid, and processing runs again.

- Download January–March 2025 [NYC Green Taxi records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
  and keep the original Parquet files.
- Apply agreed data rules and produce daily revenue facts with a date dimension.
- Run the shared Python objects through Prefect inside Docker.
- Check calculations, failure handling, and repeat runs with automated tests.

These are our goals, not claims of completed functionality.

## Planned flow

```mermaid
flowchart LR
    A[Download three monthly files] --> B[Keep raw Parquet files]
    B --> C[Validate and apply agreed data rules]
    C --> D[Group rides by pickup day]
    D --> E[Save daily facts and the date dimension]
```

The input has **one row per ride**; the fact table will have **one row per pickup
day with accepted records**. This uses ETL: Python transforms the staged files before loading the
finished report into output files. A database is not needed. It is **batch
processing** because each run handles a fixed set of monthly files and finishes;
it does not continuously process arriving events.

Runs will be started manually through Prefect in Docker, with no automatic
schedule initially. Rebuilding with the same inputs and rules must reproduce
the reporting tables without doubling the results.

## Planned reporting tables

| Table | What it contains | Saved as |
|---|---|---|
| `fact_daily_revenue` | Pickup date, accepted ride count, and recorded total amount in USD, excluding cash tips | Parquet and CSV |
| `dim_date` | All 90 calendar dates in January–March 2025, with year, month, and day of month | Parquet |

The tables connect through `service_date`. Facts hold the measurements; the
dimension describes the day. Calendar dates without facts do not become invented
zero-revenue days. See the [layout and diagram](docs/adr/001-pipeline-design.md#fact-and-dimension-layout)
for fields and checks. Run metadata will record which exact inputs, period,
and code version produced the outputs, alongside included/excluded counts.
See [run history and rebuilds](docs/adr/001-pipeline-design.md#run-history-and-rebuilds)
for the agreed lineage fields and launch behaviour.

## Technology choices

| Technology | Why we plan to use it |
|---|---|
| Python, pandas, and PyArrow | Read Parquet and calculate daily totals using tools already in the project. |
| Requests | Download source files with explicit timeouts and HTTP error handling. |
| Pydantic | Validate run settings, such as months and paths; table checks stay in pandas. |
| pytest and GitHub Actions | Check calculations and a small end-to-end run without downloading the full dataset in CI. |
| uv | Reuse the project's environment and lock file. |
| Prefect — required | Coordinate downloading, calculation, and saving with visible task states and bounded retries for transient failures. |
| Docker and Compose — required | Run the pipeline and local Prefect services with configured paths, persistent data, and checked ports. |

No ML model, API, dashboard, dbt, streaming broker, or cloud deployment is planned.
Docker, Prefect, and automated checks are part of our deliverable. We will design for container
execution from the start, not add a separate container-only implementation.

## Repository map

```text
src/green_taxi/       Pipeline objects, configuration, and commands
tests/               Mirrored tests and shared fixtures
docs/                Design decisions and data rules
.github/workflows/   Automated checks
solution/            Supplied reference; remove before public release
README.md            Project overview and run instructions
TASKS.md             Progress and completion checks
pyproject.toml       Package, dependencies, and tool configuration
uv.lock              Locked dependency versions
```

Report writing, Docker, and Prefect delivery remain planned.

## Setup

Use [uv](https://docs.astral.sh/uv/getting-started/installation/) 0.13.5 or newer
and Python 3.13.17 or a newer 3.13 patch release, as defined in
[`.python-version`](.python-version).

### Prepare the environment

This installs all dependencies and creates a virtual environment in `.venv/`.

```bash
git clone https://github.com/roy-nfisch/mle-data-pipeline-project.git
cd mle-data-pipeline-project
uv sync --locked
```

Inspect validated settings without starting downloads or services:

```bash
uv run --locked python -m green_taxi
uv run --locked pytest
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
uv build
```

The command reads [defaults.json](src/green_taxi/defaults.json), validates it, and
prints the resulting settings. The default months are defined only in that file,
not repeated in a Python Literal and default tuple. Month validation checks real
`YYYY-MM` values and uniqueness. The default project still covers Jan–Mar 2025.

To load your own JSON file, use `--config path/to/run.json`. It must contain
`months`; it can also set `reporting_period`, `source_url`, `data_dir`,
`timeout_seconds`, and `log_level`. This is a local file, not a web upload.
Invalid/missing files fail instead of falling back.
Changing source-month selection does not automatically change the agreed report
date boundaries; that would require a separate data-rule decision.

### Reporting months

We configure the first month and how many **whole calendar months** to include.
`ReportingPeriod` validates these values and derives the start/end dates; we do
not maintain a second editable end date.

```json
{
  "months": ["2025-01", "2025-02", "2025-03"],
  "reporting_period": {
    "start_month": "2025-01",
    "duration_months": 3
  }
}
```

The example includes January, February, and March: 01.01.2025 up to, but not
including, 01.04.2025.

| `duration_months` | Meaning |
|---|---|
| `1` | The whole starting month, not the current month or thirty days |
| `3` | The starting month and the following two months |
| `0` or a negative number | Invalid; no special “until today” meaning |
| `"3"`, `3.0`, `true`, or `null` | Invalid; only positive integers pass |

Invalid month formats, missing fields, extra boundary fields, and dates beyond
the supported calendar also fail before processing starts. The range does not
depend on today's date. Download months and reporting months remain separate:
the pickup date, not the filename, determines whether a ride belongs in the report.

Click translates terminal options into arguments to our Python function. It does
not run GitHub Actions or orchestrate tasks. Python/Prefect callers can bypass it:

```python
from green_taxi.settings import PipelineSettings

settings = PipelineSettings.from_file()  # Packaged JSON defaults
```

For a future container, `--data-dir /data` overrides JSON storage with a mount.
Configuration does not create directories. Tests use temporary paths, not our
real dataset. CI also checks the supplied reference tests separately.

### Test categories and parallel checks

**Fixtures prepare things; markers classify tests.** Shared fixtures provide
synthetic months, filenames, Parquet bytes, HTTP responses, and isolated log capture.
They do not use the real downloaded data or live internet.

| Marker | What it covers |
|---|---|
| `unit` | Settings validation, logging, money/date rules, and calendar generation in isolation |
| `integration` | File loading, CLI behaviour, imports, downloads, and calculation across real tiny Parquet files |
| `data` | Additional label for file structure, calculation rules, and output-table checks; not a duplicate CI run |

Every project test must have exactly one unit/integration marker. Test folders
still mirror the source. Intentional invalid/boundary dates remain explicit test
cases; repeated valid downloader setup lives in fixtures instead of each test.

```bash
uv run --locked pytest -m unit
uv run --locked pytest -m integration -n 2 --dist loadfile
uv run --locked pytest -m data
uv run --locked pytest -n 2 --dist loadfile
```

CI runs unit and integration jobs concurrently after lint/build/reference checks.
Integration uses two workers grouped by file; unit tests are serial within their
job to avoid worker-startup overhead. Reference tests run once in the checks job.
A three-run local comparison measured integration wall time around 0.96 seconds
serial and 0.93 seconds with two workers: effectively similar at this small size,
not evidence of a substantial speedup. These are local measurements, not CI promises.

### Download the raw files

From the repository root:

```bash
uv run --locked python -m green_taxi.download
```

This downloads January–March 2025 to ignored `data/raw/`. Run it again to reuse
validated files. To choose other months, put the selection in a local JSON file,
for example `{"months": ["2025-01"]}` in `run.json`, then run:

```bash
uv run --locked python -m green_taxi.download --config run.json --data-dir ./data
```

The same settings loader validates both commands; there is no separate hardcoded
month list in the downloader. `--data-dir` overrides JSON storage when supplied.
The public dataset directory comes from `source_url` in `defaults.json`, not a
Python URL constant. A custom JSON file can override it with a trusted HTTPS
directory without credentials/query/fragment. No .env file is required; never put
secrets in committed JSON. Syntax validation does not make an arbitrary endpoint safe.
Use `--force` to deliberately refresh selected files. Downloads use temporary
files, read every Parquet batch, require the pickup/amount columns and at least
one row, then replace the staged file. Failed refreshes keep the previous file.
A corrupt existing file stops the run; use `--force` to repair it. Already completed
months survive a later month's failure and can be reused on retry.

The `TaxiDownloader` returns `StagedFile` records containing source URLs, filenames,
sizes, checksums, and whether each file was downloaded or reused. The command
prints sizes/checksums; saving run metadata belongs to the report-writing stage.
Checksums identify content, not trusted fares. Row-level rules are not applied yet.
Use one writer per data directory; concurrent publication is not supported.
Logs identify the selected file, download/reuse start, successful size, and failures.
Network failures include exception type, available HTTP status, timeout, and retry
advice. Raw exception text and source URLs are not logged. Errors still propagate;
logging does not retry automatically or hide a failed run.

## Acknowledgements

This project started from a neue fische course brief and supplied reference
implementation, currently in [solution/](solution/README.md). Our implementation
will stay separate; the reference folder is planned for removal before public
release. The original notice remains in [LICENSE](LICENSE).
