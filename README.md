# NYC Taxi Revenue Data Pipeline

[![CI on main](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/actions/workflows/ci.yml?query=branch%3Amain)
[![Tests: unit, integration, data](https://img.shields.io/badge/tests-unit%20%7C%20integration%20%7C%20data-blue)](#run-the-checks)
[![Python 3.13](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)](#run-locally)
[![Processing: batch ETL](https://img.shields.io/badge/processing-batch%20ETL-blue)](#the-flow)
[![Prefect: local orchestration](https://img.shields.io/badge/Prefect-local%20orchestration-blue)](#follow-a-run)
[![Docker: local](https://img.shields.io/badge/Docker-local-2496ED?logo=docker&logoColor=white)](#run-with-docker-and-prefect)
[![GHCR: public image](https://img.shields.io/badge/GHCR-public%20image-blue)](https://github.com/users/roy-nfisch/packages/container/package/nyc-taxi-revenue-data-pipeline)

We built a small, object-oriented Python pipeline that turns NYC Green Taxi
records into daily revenue reports. Our focus was reliable downloads, clear data
rules and a report we can rebuild without doubling the results.

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
The image is Linux amd64; Apple Silicon uses emulation. Native local builds remain
available. See the [release guide](docs/docker-prefect.md#publish-to-ghcr)
for verification and Compose instructions.

### Follow a run

One Prefect flow calls three tasks in order: **download → calculate → save**.
It reuses the plain Python pipeline's objects and rules.

<p align="center">
  <a href="docs/assets/prefect-pipeline-run.jpg"><img src="docs/assets/prefect-pipeline-run.jpg" width="850" alt="Completed Prefect run showing download, calculation and report-saving tasks"></a>
</p>

*An actual local run: bars show timings and arrows show dependencies.*

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
the [configuration guide](docs/docker-prefect.md#custom-json-configuration-optional)
explains separate local settings and refresh commands.

Reruns replace reports rather than appending them. Run only one batch per data directory.

## Technologies and why we used them

| Technology | Why we used it here |
|---|---|
| Python, pandas and PyArrow | Read monthly Parquet files, apply the data rules and write daily reports. |
| Requests | Download source files with explicit timeouts and HTTP error handling. |
| Pydantic | Validate the reporting period, source URL and storage settings before processing. |
| Prefect | Show stage states, retries and logs while reusing the plain Python pipeline. |
| Docker and Compose | Package a non-root runtime and keep data in persistent volumes. |
| pytest and pytest-xdist | Check individual rules, component integration and reruns using small synthetic files. |
| Ruff | Keep formatting consistent and catch common Python mistakes. |
| uv | Install locked dependencies for local work, containers and CI. |
| GitHub Actions | Run quality, unit, integration and Docker checks on changes. |

## The flow

```mermaid
flowchart LR
    A[NYC Green Taxi files] --> B[Download and stage raw Parquet]
    B --> C[Validate dates and amounts]
    C --> D[Group accepted rides by day]
    D --> E[Save daily facts and date dimension]
    E --> F[Record counts and source identities]
```

This is **batch ETL**: Python transforms staged files, then saves the results.
It does not process a continuous stream.

`TaxiPipeline` connects `TaxiDownloader`, `DailyRevenueCalculator` and
`ReportWriter`, keeping each responsibility separate.

ELT is being developed separately. The [ELT Docker image](docs/elt-docker.md)
checks DuckDB and packaged SQL; there is no end-to-end ELT command yet.

## Results and limitations

The January–March 2025 [real-data review](docs/data-review.md) recorded:

| Measure | Result |
|---|---:|
| Input rides | 146,486 |
| Accepted rides | 146,475 |
| Excluded rides | 11, outside the reporting period |
| Daily facts / calendar dates | 90 / 90 |
| Recorded charges | $3,395,290.56 USD |

Outputs are `fact_daily_revenue.parquet`, a CSV copy, `dim_date.parquet` and
`metadata.json`. Daily facts and the date dimension join through `service_date`.

- Revenue means recorded `total_amount` charges, not profit; cash tips are not
  included. We retain zero and negative amounts rather than assuming they are errors.
- Invalid dates/amounts and out-of-period pickups are excluded and counted.
  Results describe these source files, not every ride in the city.
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
[Docker guide](docs/docker-prefect.md#retries-and-checks).

## Repository layout

```text
nyc-taxi-revenue-data-pipeline/
├── src/green_taxi/
│   ├── settings.py, defaults.json  # Validated run settings
│   ├── download.py                 # Download and check raw files
│   ├── reporting.py                # Shared result, calendar and money helpers
│   ├── report.py                   # Check and save the report
│   ├── flow.py                     # Delegate for the existing Prefect command
│   ├── __main__.py, logging.py     # Terminal entry point and logs
│   ├── etl/
│   │   ├── revenue.py              # Apply rules and calculate daily totals
│   │   ├── pipeline.py             # Connect the existing stage objects
│   │   └── flow.py                 # Prefect tasks and flow
│   └── elt/sql/schema.sql          # Optional registry DDL; no ELT pipeline yet
├── tests/
│   ├── green_taxi/                 # Shared component tests and fixtures
│   │   ├── etl/                    # Mirrors the Python ETL modules
│   │   └── elt/                    # Optional registry contract checks
│   ├── packaging/                  # Installed wheel, SQL resource and ETL smoke checks
│   └── docker/                     # Image and Compose checks
├── notebooks/                      # Optional data exploration
├── docs/                           # Design, data review and run guide
│   └── assets/                     # Terminal GIF and Prefect screenshot
├── data/                           # Generated locally; ignored by Git
│   ├── raw/                        # Staged source files
│   └── processed/                  # Daily facts, date dimension and metadata
├── .github/workflows/              # Automated checks
├── .github/scripts/                # Release-tag safety check
├── Dockerfile, compose.yaml        # Application and local Prefect runtime
├── compose.config.yaml             # Read-only host JSON override
├── compose.elt.yaml                # Separate ELT image checks
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

- [Design decisions](docs/adr/001-pipeline-design.md): scope, data rules and object responsibilities.
- [ELT design](docs/adr/002-elt-comparison.md) and [contracts](docs/etl-elt-contract.md): the database-first comparison.
- [Real-data review](docs/data-review.md): source identities, reconciliation and repeat-run evidence.
- [Docker and Prefect](docs/docker-prefect.md): configuration, storage and troubleshooting.
- [Image publishing](docs/docker-prefect.md#publish-to-ghcr): manual release steps and approval gates.

The project uses [NYC TLC Green Taxi records](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
The original software notice remains in [LICENSE](LICENSE).
