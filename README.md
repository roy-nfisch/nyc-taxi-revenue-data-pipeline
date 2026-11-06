# NYC Taxi Revenue Data Pipeline

[![CI on main](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/actions/workflows/ci.yml?query=branch%3Amain)
[![Tests: unit, integration, data](https://img.shields.io/badge/tests-unit%20%7C%20integration%20%7C%20data-blue)](#run-the-checks)
[![Python 3.13](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](#run-locally)
[![Processing: batch ETL and ELT](https://img.shields.io/badge/processing-batch%20ETL%20%7C%20ELT-blue)](#the-flow)
[![Prefect: local orchestration](https://img.shields.io/badge/Prefect-local%20orchestration-blue)](#follow-a-run)
[![Docker: ETL and ELT](https://img.shields.io/badge/Docker-ETL%20%7C%20ELT-2496ED?logo=docker&logoColor=white)](#run-with-docker-and-prefect)
[![GHCR: public ETL image](https://img.shields.io/badge/GHCR-public%20ETL%20image-blue)](https://github.com/users/roy-nfisch/packages/container/package/nyc-taxi-revenue-data-pipeline)
[![GHCR: public ELT image](https://img.shields.io/badge/GHCR-public%20ELT%20image-blue)](https://github.com/users/roy-nfisch/packages/container/package/nyc-taxi-revenue-data-pipeline-elt)

We built a small, object-oriented Python pipeline that turns NYC Green Taxi
records into daily revenue reports. Our focus was reliable downloads, clear data
rules and a report we can rebuild without doubling the results.
We then added a DuckDB/SQL path to compare **ETL** (transform, then load the
report) with **ELT** (load raw data, then transform it).

This project began during the neue fische MLE course, which supplied the brief
and a reference implementation. We built our own pipeline, tests and delivery setup.

[Run locally](#run-locally) · [Run with Docker](#run-with-docker-and-prefect) · [Technology choices](#technologies-and-why-we-used-them) · [Tests](#run-the-checks)

<p align="center">
  <a href="docs/assets/pipeline-demo.gif"><img src="docs/assets/pipeline-demo.gif" width="620" alt="Taxi pipeline demo showing accepted rides, daily revenue and saved reports"></a>
</p>

*The recording includes a temporary summary script. The commands below print
logs and the metadata path, not the presentation table.*

## What we built

- Checked downloads of the January–March 2025 files, with safe reuse and refresh.
- Daily revenue in exact cents, with a date dimension and run metadata.
- Prefect task states, logs and retries around the same Python objects.
- Automated checks for data rules, failures, reruns and Docker delivery.
- A separate ELT path, checked against ETL using the same source files.

The ETL pipeline runs without a notebook or database.

## Run locally

Use Python 3.13.17 or a newer 3.13 patch and
[uv](https://docs.astral.sh/uv/getting-started/installation/) 0.13.5 or newer.
From a fresh clone:

```bash
git clone https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline.git
cd nyc-taxi-revenue-data-pipeline
uv sync --locked
uv run --locked python -m green_taxi --run
```

The first run downloads the public files and needs internet access. Later runs
reuse checked files in `data/raw/` and rebuild the reports in `data/processed/`.
No local configuration file or API key is needed.

Read the result:

```bash
cat data/processed/metadata.json
```

Omit `--run` to inspect settings without processing data.

To run the ELT comparison instead:

```bash
uv run --locked --extra elt python -m green_taxi.elt
cat data/elt/processed/metadata.json
```

It reuses `data/raw/`, keeps its warehouse in `data/elt/warehouse.duckdb`, and
saves reports to `data/elt/processed/`. See the [ELT run guide](docs/elt.md)
for Docker and Prefect commands. Separate ETL and ELT images are listed below.

## Run with Docker and Prefect

With Docker and Compose available, use the same checkout:

```bash
docker compose up --build -d --wait prefect
docker compose run --rm flow
```

Check that host port **4205** is free first. Open
[local Prefect](http://127.0.0.1:4205), select a run and inspect its tasks or logs.
No `.env` setup is required.

Docker keeps raw files and reports in a named volume, **not** the host's
`data/` folder. Read the saved metadata from that volume:

```bash
docker compose run --rm --no-deps pipeline python -c \
  "from pathlib import Path; print(Path('/data/processed/metadata.json').read_text())"
```

The plain batch also runs in the image with
`docker compose run --rm --no-deps pipeline`, without Prefect.
Stop the server with `docker compose down`; reports and run history are retained.

### Run the published image

The [public GHCR image](https://github.com/users/roy-nfisch/packages/container/package/nyc-taxi-revenue-data-pipeline)
is available as `2026.11.05` and `latest`. No registry login or local build is needed:

```bash
docker run --rm --platform linux/amd64 \
  --mount type=volume,source=nyc-taxi-release-data,target=/data \
  ghcr.io/roy-nfisch/nyc-taxi-revenue-data-pipeline:2026.11.05
```

This runs the batch, not a public service. Reports persist in the named volume.
The ETL image is an earlier release; its metadata does not include the execution
mode and engine version recorded by the current source code.
The image is Linux amd64; Apple Silicon uses emulation. Native local builds remain
available. See the [release guide](docs/docker.md#publish-to-ghcr)
for verification and Compose instructions.

To run the separate [public ELT image](https://github.com/users/roy-nfisch/packages/container/package/nyc-taxi-revenue-data-pipeline-elt):

```bash
docker run --rm --platform linux/amd64 \
  --mount type=volume,source=nyc-taxi-elt-data,target=/data \
  ghcr.io/roy-nfisch/nyc-taxi-revenue-data-pipeline-elt:2026.11.06
```

Its reports are saved in `/data/elt/processed/` inside the named volume.

### Follow a run

ETL uses three sequential tasks: **download → calculate → save**.
ELT uses four: **download → load raw → transform with SQL → save**.
Each flow reuses its plain Python pipeline's objects and rules.

<p align="center">
  <a href="docs/assets/prefect-pipeline-run.jpg"><img src="docs/assets/prefect-pipeline-run.jpg" width="850" alt="Completed Prefect run showing download, calculation and report-saving tasks"></a>
</p>

*An actual ETL run: bars show timings and arrows show dependencies.*

<p align="center">
  <a href="docs/assets/prefect-elt-run.jpg"><img src="docs/assets/prefect-elt-run.jpg" width="850" alt="Completed Prefect ELT run with download, raw loading, SQL transformation and report saving tasks"></a>
</p>

*An actual ELT run: the raw load comes before the SQL transformation.*

Runs start manually. Logs show each stage and the final totals; temporary download
failures get up to three attempts. Prefect is a local run view, not a revenue dashboard.

### Change the reporting period

Edit `start_month` and `duration_months` in
[defaults.json](src/green_taxi/defaults.json). For example, March 2025 and a
duration of `3` select March–May. Duration must be a positive integer.

Local Python reads the edited file on the next run. To make Docker read it
without rebuilding the image:

```bash
docker compose -f compose.yaml -f compose.config.yaml run --rm flow
```

The override mounts the file read-only. Without it, Docker uses the defaults
included in the image. Keep the January–March baseline when submitting changes;
the [configuration guide](docs/docker.md#custom-json-configuration-optional)
explains separate local settings and refresh commands.

Reruns replace reports rather than appending them. Run only one batch per data directory.

## Technologies and why we used them

| Technology | Why we used it here |
|---|---|
| Python, pandas and PyArrow | Read monthly Parquet files, apply the data rules and write daily reports. |
| DuckDB and SQL | Store raw rides and calculate the ELT report, keeping ETL as the comparison baseline. |
| Requests | Download source files with explicit timeouts and HTTP error handling. |
| Pydantic | Validate the reporting period, source URL and storage settings before processing. |
| Prefect | Show stage states, retries and logs while reusing the plain Python pipeline. |
| Docker and Compose | Package a non-root runtime and keep data in persistent volumes. |
| pytest and pytest-xdist | Check individual rules, component integration and reruns using small synthetic files. |
| Ruff | Keep formatting consistent and catch common Python mistakes. |
| uv | Install locked dependencies for local work, containers and CI. |
| GitHub Actions | Run quality, unit, integration and Docker checks on changes. |

## The flow

![ETL transforms staged taxi files in Python; ELT loads them into DuckDB and transforms with SQL. Both use the shared writer and separate report folders.](docs/assets/etl-elt-flow.svg)

Both are manual batches, not continuous streams. The diagram shows two choices,
not branches that automatically run in parallel. Downloading and report writing
are shared; each path owns its calculation.

`TaxiPipeline` and `ELTPipeline` connect the steps. `TaxiDownloader` checks and
stages the source files. `DailyRevenueCalculator` applies the rules in Python;
`TaxiWarehouse` loads raw records and runs SQL. Both paths use `ReportWriter`
to check and save the reports.

See [a ride-to-daily-total example](docs/elt.md#what-happens-to-the-data) or
the [SQL flow](docs/elt.md#sql-flow) for a closer look.

## Results and limitations

The January–March 2025 [real-data review](docs/data-review.md) recorded:

| Measure | Result |
|---|---:|
| Input rides | 146,486 |
| Accepted rides | 146,475 |
| Excluded rides | 11, outside the reporting period |
| Daily facts / calendar dates | 90 / 90 |
| Recorded charges | $3,395,290.56 USD |

ELT matched every daily fact, calendar row and counter on the same reviewed
source files. Repeated runs produced the same totals without doubling results.

Outputs are `fact_daily_revenue.parquet`, a CSV copy, `dim_date.parquet` and
`metadata.json`. Daily facts and the date dimension join through `service_date`.

- Revenue means recorded `total_amount` charges, not profit; cash tips are not
  included. We retain zero and negative amounts rather than assuming they are errors.
- Invalid dates/amounts and out-of-period pickups are excluded and counted.
  Results describe these source files, not every ride in the city.
- Parquet numeric types are preserved before money validation; finite sub-cent
  amounts stop the run rather than being rounded. See the
  [numeric precision notes](docs/etl-parquet-precision.md), including float32.
- Checksums and metadata identify the inputs and run; they do not prove fares are correct.
- Reports are not saved as one transaction. A failed save can leave mixed files;
  resolve the error and rerun. Existing reports do not prove a new run succeeded.

See the [data rules and reliability design](docs/adr/001-pipeline-design.md)
for details. This is a local educational pipeline, not a production reporting service.

## Run the checks

From the project root, after installing the development dependencies:

```bash
uv sync --locked
uv run --locked ruff check src tests
uv run --locked ruff format --check src tests
uv run --locked pytest -n 2 --dist loadfile
uv build
```

| Category | What it checks |
|---|---|
| Unit | Settings, logging, date/money rules and calendar generation. |
| Integration | CLI, file I/O, pipeline wiring, Prefect behavior and reruns. |
| Data | An additional label for file structure and output rules, not a separate duplicate CI job. |

CI runs quality, unit, integration and Docker checks. Tests use tiny synthetic
inputs and fake HTTP responses, not public downloads. Add `--extra elt` to the
`uv run` test command to include DuckDB checks; otherwise those tests skip.
Docker checks are opt-in locally; see the
[Docker guide](docs/docker.md#retries-and-checks).

## Repository layout

```text
nyc-taxi-revenue-data-pipeline/
├── src/green_taxi/
│   ├── settings.py, defaults.json  # Validated run settings
│   ├── download.py                 # Download and check raw files
│   ├── reporting.py                # Shared result, calendar and money helpers
│   ├── run_report.py               # Shared run identity and publication
│   ├── report.py                   # Check and save the report
│   ├── flow.py                     # Delegate for the existing Prefect command
│   ├── __main__.py, logging.py     # Terminal entry point and logs
│   ├── etl/
│   │   ├── revenue.py              # Apply rules and calculate daily totals
│   │   ├── pipeline.py             # Connect the existing stage objects
│   │   └── flow.py                 # Prefect tasks and flow
│   └── elt/                        # Warehouse, SQL, batch command and Prefect flow
├── tests/
│   ├── green_taxi/                 # Shared component tests and fixtures
│   │   ├── etl/                    # Mirrors the Python ETL modules
│   │   └── elt/                    # Loading, SQL, parity and Prefect checks
│   ├── packaging/                  # Installed ETL/ELT wheel and SQL checks
│   └── docker/                     # Image and Compose checks
├── notebooks/                      # Optional data exploration
├── docs/                           # Design, data review and run guide
│   └── assets/                     # Pipeline diagram, terminal GIF and Prefect screenshots
├── data/                           # Generated locally; ignored by Git
│   ├── raw/                        # Staged source files
│   ├── processed/                  # ETL facts, date dimension and metadata
│   └── elt/                        # DuckDB warehouse and separate ELT reports
├── .github/workflows/              # Automated checks
├── .github/scripts/                # Release-tag safety check
├── Dockerfile, compose.yaml        # Application and local Prefect runtime
├── compose.config.yaml             # Read-only host JSON override
├── compose.elt.yaml                # ELT batch, Prefect flow and image checks
├── .env.example                    # Optional Compose settings
└── pyproject.toml, uv.lock         # Package and locked dependencies
```

The tree shows the main areas, not every file. Docker's generated data lives in
its named volume rather than the host `data/` directory.

## Challenges and decisions

- **Agree on data rules first.** Negative charges are not automatically errors.
- **Check results, not just exit codes.** Reconcile totals and verify reruns.
- **Share the logic.** Plain Python and Prefect use the same stage objects.
- **Make storage clear.** Host files and Docker volumes are different locations.

## Project notes

- [Explore ELT](notebooks/02-explore-elt.ipynb): follow a batch from raw files to a daily report.
- [Design decisions](docs/adr/001-pipeline-design.md): scope, data rules and object responsibilities.
- [ELT guide](docs/elt.md) and [design decisions](docs/adr/002-elt-comparison.md): the database-first comparison.
- [Real-data review](docs/data-review.md): source identities, reconciliation and repeat-run evidence.
- [Docker](docs/docker.md): configuration, storage and troubleshooting.
- [Image publishing](docs/docker.md#publish-to-ghcr): manual release steps and approval gates.

The project uses [NYC TLC Green Taxi records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
The original software notice remains in [LICENSE](LICENSE).
