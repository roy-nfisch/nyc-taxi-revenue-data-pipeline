# Green Taxi Daily Revenue Pipeline

We are building a small, object-oriented Python **batch ETL pipeline** that turns NYC Green
Taxi rides into a daily revenue report. The focus is reliable downloads, clear
data rules, useful tests, and a process someone else can run.

**Status: the local batch command downloads, calculates, and saves checked reports.**
The [local real-data review](docs/data-review.md) reconciles 146,475 accepted rides
and $3,395,290.56 in recorded charges across 90 days. Docker and Prefect execution
are implemented for local review. The accepted
data rules, simple OOD plan, and fact/date layout are in
[ADR 001](docs/adr/001-pipeline-design.md). Docker and Prefect are required.
Follow our progress in [TASKS.md](TASKS.md).

## See it run

The terminal demo shows the batch results: accepted/excluded ride counts,
daily revenue, and saved files. Select the animation to open it separately.

<p align="center">
  <a href="docs/assets/pipeline-demo.gif"><img src="docs/assets/pipeline-demo.gif" width="620" alt="Taxi pipeline run showing accepted rides, daily revenue, and saved reports"></a>
</p>

Compressed GIF, about 55 KB. The terminal title, prompt, local paths, and scrollbar are cropped.
The recording used a temporary summary script; the supported batch command is:

```bash
uv run --locked python -m green_taxi --run
```

<details>
<summary>Example run output</summary>

From the recorded demo, including the temporary script's printed summary.
The standard command prints the logs and metadata path; it does not print the
extra summary/table below. Local paths are shortened and run IDs replaced with
`<run-id>` here. Each run gets a new ID, and paths depend on the chosen storage.

```text
INFO green_taxi.pipeline: Starting run <run-id>
INFO green_taxi.download: Starting reuse for green_tripdata_2025-01.parquet
INFO green_taxi.download: Reused green_tripdata_2025-01.parquet (1178451 bytes)
INFO green_taxi.download: Starting reuse for green_tripdata_2025-02.parquet
INFO green_taxi.download: Reused green_tripdata_2025-02.parquet (1117474 bytes)
INFO green_taxi.download: Starting reuse for green_tripdata_2025-03.parquet
INFO green_taxi.download: Reused green_tripdata_2025-03.parquet (1253510 bytes)
INFO green_taxi.revenue: Read green_tripdata_2025-01.parquet (48326 rows)
INFO green_taxi.revenue: Read green_tripdata_2025-02.parquet (46621 rows)
INFO green_taxi.revenue: Read green_tripdata_2025-03.parquet (51539 rows)
INFO green_taxi.revenue: Accepted 146475 of 146486 rows
INFO green_taxi.report: Saved checked report: data/processed/metadata.json
INFO green_taxi.pipeline: Completed run <run-id>
data/processed/metadata.json

===== PIPELINE COMPLETED =====
Rides read:       146,486
Rides accepted:   146,475
Rides excluded:   11
Days reported:    90
Recorded charges: $3,395,290.56 USD

===== FIRST 10 DAYS =====
service_date  trip_count daily_revenue
  2025-01-01        1001      25076.51
  2025-01-02        1516      34256.83
  2025-01-03        1525      36517.69
  2025-01-04        1249      27869.20
  2025-01-05        1129      25823.69
  2025-01-06        1541      34049.13
  2025-01-07        1648      36844.24
  2025-01-08        1680      37905.12
  2025-01-09        1868      42532.87
  2025-01-10        1633      37417.57

===== FILES SAVED =====
  fact_daily_revenue.parquet
  fact_daily_revenue.csv
  dim_date.parquet
  metadata.json
```

</details>

## What we want to demonstrate

Build a small taxi-data pipeline and test what happens when files are incomplete,
records are invalid, and processing runs again.

- Download January–March 2025 [NYC Green Taxi records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
  and keep the original Parquet files.
- Apply agreed data rules and produce daily revenue facts with a date dimension.
- Run the shared Python objects through Prefect inside Docker.
- Check calculations, failure handling, and repeat runs with automated tests.

See TASKS.md for reviewed milestones and work awaiting approval.

## Batch flow

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

Start runs manually through Prefect in Docker, with no automatic
schedule initially. Rebuilding with the same inputs and rules must reproduce
the reporting tables without doubling the results.

## Reporting tables

| Table | What it contains | Saved as |
|---|---|---|
| `fact_daily_revenue` | Pickup date, accepted ride count, and recorded total amount in USD, excluding cash tips | Parquet and CSV |
| `dim_date` | All 90 calendar dates in January–March 2025, with year, month, and day of month | Parquet |

The tables connect through `service_date`. Facts hold the measurements; the
dimension describes the day. Calendar dates without facts do not become invented
zero-revenue days. See the [layout and diagram](docs/adr/001-pipeline-design.md#fact-and-dimension-layout)
for fields and checks. Run metadata records which exact inputs, period,
and code version produced the outputs, alongside included/excluded counts.
See [run history and rebuilds](docs/adr/001-pipeline-design.md#run-history-and-rebuilds)
for the agreed lineage fields and launch behaviour.

## Technology choices

| Technology | Why we use it |
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
src/green_taxi/       Shared pipeline objects, JSON defaults, CLI, and Prefect flow
tests/green_taxi/     Mirrored unit/integration tests and shared fixtures
tests/docker/        Opt-in image and Compose contract checks
docs/                Design decisions, data review, Docker/Prefect guide, and demo
data/                Local raw files, reports, and original demo media — ignored
config/local/        Optional local JSON configurations — ignored
Dockerfile           Non-root image for the batch and local Prefect server
compose.yaml         Project network, persistent volumes, and manual run services
compose.config.yaml  Optional read-only custom JSON mount for batch services
.env.example         Template for local Compose settings; copy to ignored .env
.dockerignore        Files excluded from the image build context
.github/workflows/   Automated checks
solution/            Supplied reference; remove before public release
README.md            Project overview and run instructions
TASKS.md             Progress and completion checks
pyproject.toml       Package, dependencies, and tool configuration
uv.lock              Locked dependency versions
LICENSE              Project license and attribution
```

`TaxiPipeline` connects the downloader, calculator, and `ReportWriter`; each keeps
its own responsibility. The Prefect flow reuses these objects and the same
lineage/publication methods; it does not copy the calculation rules.

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
prints the resulting settings. The default period is defined only in that file;
the source months are generated from it. Month validation checks real
`YYYY-MM` values and uniqueness. The default project still covers Jan–Mar 2025.

To load your own JSON file, use `--config path/to/run.json`. Normally it contains
`reporting_period`, which also generates the source-month list. It can set `source_url`, `data_dir`,
`timeout_seconds`, and `log_level`. This is a local file, not a web upload.
Invalid/missing files fail instead of falling back.
An optional explicit `months` list overrides source selection only. Use it only
when deliberately reading different files, such as extra months for late records.

### Reporting months

We configure the first month and how many **whole calendar months** to include.
`ReportingPeriod` validates these values and derives the start/end dates; we do
not maintain a second editable end date.

```json
{
  "reporting_period": {
    "start_month": "2025-01",
    "duration_months": 3
  }
}
```

The example downloads January, February, and March and reports pickups from
01.01.2025 up to, but not including, 01.04.2025. Edit the period once; you do not
need to maintain a second month list. CLI settings inspection shows the derived list.

| `duration_months` | Meaning |
|---|---|
| `1` | The whole starting month, not the current month or thirty days |
| `3` | The starting month and the following two months |
| `0` or a negative number | Invalid; no special “until today” meaning |
| `"3"`, `3.0`, `true`, or `null` | Invalid; only positive integers pass |

Invalid month formats, missing fields, extra boundary fields, and dates beyond
the supported calendar also fail before processing starts. The range does not
depend on today's date. Source filenames normally follow the period, but the
pickup date still determines whether a ride belongs in the report. This does not
guarantee that the selected files contain every ride recorded elsewhere.

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

### Docker and Prefect

From your Mac/host terminal at the repository root (edit an existing `.env`
instead of overwriting it):

```bash
cp .env.example .env
docker compose up --build -d --wait prefect
docker compose run --rm flow
```

Open **http://127.0.0.1:4205** to see completed flow/task runs. Container data lives
in named volumes, separate from host `./data`. Plain Python also runs in the image:
`docker compose run --rm --no-deps pipeline`. Run one batch at a time.

See [Docker and Prefect](docs/docker-prefect.md) for ports, retries, storage,
inspection, tests, and safe stop/restart commands.
Packaged JSON defaults remain available; use the documented
[optional config override](docs/docker-prefect.md#custom-json-configuration-optional)
to mount your own JSON read-only without rebuilding the image.
Compose settings belong in `.env`; pipeline settings stay in JSON. No Python
dotenv loader is used. The `.env` template does not enable custom JSON by itself.

### Download the raw files

From the repository root:

```bash
uv run --locked python -m green_taxi.download
```

This downloads January–March 2025 to ignored `data/raw/`. Run it again to reuse
validated files. To choose other months, put the selection in a local JSON file,
for example `{"reporting_period": {"start_month": "2025-01", "duration_months": 1}}`
in ignored `config/local/run.json` (create
the directory first), then run:

```bash
uv run --locked python -m green_taxi.download --config config/local/run.json --data-dir ./data
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
Checksums identify content, not trusted fares. This download-only command does
not apply the calculator's row-level rules.
Use one writer per data directory; concurrent publication is not supported.
Logs identify the selected file, download/reuse start, successful size, and failures.
Network failures include exception type, available HTTP status, timeout, and retry
advice. Raw exception text and source URLs are not logged. Errors still propagate;
logging does not retry automatically or hide a failed run.

### Run the complete batch

```bash
uv run --locked python -m green_taxi --run
# Optional custom configuration or container storage:
uv run --locked python -m green_taxi --run --config config/local/run.json --data-dir ./data
```

Without `--run`, the command still only prints validated settings. Add `--force`
with `--run` to deliberately refresh downloads. Otherwise, reruns reuse checked
raw files and rebuild the reports without appending or doubling totals.

The command prints the saved `metadata.json` path. In ignored `data/processed/`:

| File | What it contains |
|---|---|
| `fact_daily_revenue.parquet` | One row per accepted pickup day, with exact decimal revenue and ride count |
| `fact_daily_revenue.csv` | A readable copy of those facts |
| `dim_date.parquet` | Every calendar day in the reporting period |
| `metadata.json` | Run ID, UTC times, resolved period, input URLs/sizes/SHA-256, code revision/dirty status, totals, and output row counts |

Code identity uses Git in a checkout or an optional image build stamp. It stays
explicitly `unknown` when neither is available; see the container guide. The finish
timestamp is captured after staging the tables, just before publication. Run IDs,
times, and download/reuse flags may differ between otherwise equivalent rebuilds.

The writer checks schemas, dates, integer counts, decimal money, and the date join.
It reads staged Parquet/CSV files back and compares exact values before replacing
any previous outputs, then publishes metadata last. A staging
failure preserves the previous report. A failure during replacement may leave
mixed table versions, but removes success metadata: do not use that bundle; fix
the failure and rerun. This is not a multi-file transaction. Run only one process
per data directory. A report left after an earlier-stage failure describes its
previous run, not the failed attempt. Errors exit unsuccessfully.

Tests use tiny temporary inputs and fake HTTP responses. The separate
[real-data review](docs/data-review.md) records the full-data reconciliation and
fresh-environment rebuild; those results are not established by synthetic tests.

## Acknowledgements

This project started from a neue fische course brief and supplied reference
implementation, currently in [solution/](solution/README.md). Our implementation
will stay separate; the reference folder is planned for removal before public
release. The original notice remains in [LICENSE](LICENSE).
