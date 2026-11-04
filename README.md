# Green Taxi Daily Revenue Pipeline

We are planning a small, object-oriented Python **batch ETL pipeline** that turns NYC Green
Taxi rides into a daily revenue report. The focus is reliable downloads, clear
data rules, useful tests, and a process someone else can run.

**Status: Python foundation added; the full pipeline is not implemented yet.** The accepted
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
README.md                         Project goal, scope, and setup
TASKS.md                          Work and completion checks
docs/adr/001-pipeline-design.md    Accepted rules, object responsibilities, and table layout
pyproject.toml / uv.lock          Installable package and locked environment
src/green_taxi/                   Settings, explicit logging, and settings entry point
src/green_taxi/defaults.json      Default source-month selection, loaded at runtime
tests/green_taxi/                 Mirrored settings, logging, and import checks
.github/workflows/ci.yml          Feature-branch and PR lint, tests, and build
solution/                         Supplied reference; remove before public release
```

Revenue calculation, report writing, Docker, and Prefect delivery remain planned.

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
`months`; it can also set `data_dir`, `timeout_seconds`, and `log_level`. This is
a local file, not a web upload. Invalid/missing files fail instead of falling back.
Changing source-month selection does not automatically change the agreed report
date boundaries; that would require a separate data-rule decision.

Click translates terminal options into arguments to our Python function. It does
not run GitHub Actions or orchestrate tasks. Python/Prefect callers can bypass it:

```python
from green_taxi.settings import PipelineSettings

settings = PipelineSettings.from_file()  # Packaged JSON defaults
```

For a future container, `--data-dir /data` overrides JSON storage with a mount.
Configuration does not create directories. Tests use temporary paths, not our
real dataset. CI also checks the supplied reference tests separately.

## Acknowledgements

This project started from a neue fische course brief and supplied reference
implementation, currently in [solution/](solution/README.md). Our implementation
will stay separate; the reference folder is planned for removal before public
release. The original notice remains in [LICENSE](LICENSE).
