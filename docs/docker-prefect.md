# Run with Docker and Prefect

Docker packages our existing Python application. Prefect adds task states and
bounded download retries; it does not replace the data rules. Both entry points
reuse `TaxiPipeline.from_settings()` and the same downloader/calculator/writer.

## Start and run

Run all commands in this guide in your **Mac/host terminal**, from the repository
root, not inside a container. Set up Compose settings once:

```bash
cp .env.example .env
```

If `.env` already exists, edit it instead of overwriting it. Compose automatically
reads this local file; Python does not load it. It contains only:

```dotenv
PREFECT_PORT=4205
TAXI_CONFIG_FILE=./run.local.json
```

`.env` chooses the host UI port and the optional JSON file to mount. Pipeline
settings (months, reporting period, timeouts, and paths) stay in JSON, not `.env`.
These values are used by Compose interpolation, not automatically forwarded to
container environment variables. `.env` is excluded from Git and image builds;
only `.env.example` is committed. No new Python dependency is needed.

Check your chosen port is free, then:

```bash
docker compose up --build -d --wait prefect
docker compose run --rm flow
```

Open **http://127.0.0.1:4205** (or your chosen port). Under Runs, select
`green-taxi-batch` to see:

1. Download taxi files.
2. Calculate daily revenue.
3. Save checked report.

These are sequential: each step needs the previous result. A failed stage stops
later stages. Flow runs are started manually; no schedule, worker, or deployment
is created. Run the second command again to rebuild from checked staged inputs.
To refresh deliberately, pass the complete command because Compose overrides
the service command, not just its arguments:

```bash
docker compose run --rm flow python -m green_taxi.flow --data-dir /data --force
```

The normal Python command also works in the same image, without Prefect:

```bash
docker compose run --rm --no-deps pipeline
```

To use another host port, edit `.env`, for example:

```dotenv
PREFECT_PORT=4206
```

Then run the same Compose startup/flow commands above. An exported shell variable
with the same name takes precedence over `.env`; unset it if an edit seems ignored.

## Custom JSON configuration (optional)

Normal Compose runs use the packaged `defaults.json`; no host config file is
required. For another start month, duration, or source selection, copy the defaults
and edit the JSON. `run.local.json` is excluded from Git and the Docker build context:

```bash
cp src/green_taxi/defaults.json run.local.json
```

Keep `duration_months` a positive integer, not a quoted string or float. `months`
selects source files; `reporting_period` selects pickup dates. No values are
duplicated in Compose. `TAXI_CONFIG_FILE` is already set in `.env`; change it there
if you choose another JSON filename. Then opt into the override:

```bash
docker compose -f compose.yaml -f compose.config.yaml run --rm flow
```

For the plain batch without Prefect:

```bash
docker compose -f compose.yaml -f compose.config.yaml run --rm --no-deps pipeline
```

Start the server with the normal command above before running a flow. Only the
batch services mount the file, at `/run-config/run.json`, read-only. Missing
paths fail instead of becoming empty directories. The non-root container user
must be able to read the file; use trusted JSON, never secrets. `--data-dir /data`
still overrides any JSON storage path so reports use the existing named volume.
Custom runs replace the current report in that volume; they are not a separate
report archive. Run only one batch at a time. To return to packaged defaults,
omit `-f compose.config.yaml` again; no image rebuild or Python change is needed.
Setting `TAXI_CONFIG_FILE` in `.env` alone does not enable a custom mount, so base
Compose still works even when `run.local.json` has not been created.

## Where things live

| Component | Location / purpose |
|---|---|
| Prefect UI and API | Host loopback port 4205; not publicly exposed |
| Flow-to-server connection | `http://prefect:4200/api` inside `taxi-network` |
| Raw files and report | `/data/raw` and `/data/processed` in named volume `green-taxi_taxi-data` |
| Run history | SQLite under `/home/app/.prefect` in `green-taxi_prefect-state` |

The containers do **not** use the host's `./data` directory. The first container
run downloads its own inputs; later runs reuse them. Named volumes let the
non-root application write data without changing host file permissions. The
same data volume is shared by plain Python and Prefect: run only one batch at a time.
No database service is needed for this small local Prefect server.

Inspect the report and service logs:

```bash
docker compose run --rm --no-deps pipeline python -c "from pathlib import Path; print(Path('/data/processed/metadata.json').read_text())"
docker compose logs --tail 50 prefect
docker compose ps
```

The installed package has no Git checkout, so its code revision is explicitly
`unknown`; input checksums and run IDs are still recorded. Do not interpret this
as a clean Git revision. Result tables are not stored as Prefect pickles or cached
between runs. Prefect stores task/run states; our writer owns the report files.

## Retries and checks

Download gets at most three attempts (two retries, with one- and three-second
delays). Retry timeouts, connection interruptions, HTTP 429 and 5xx responses.
Certificate errors, missing files/HTTP 404, invalid Parquet, and storage failures
fail without retry. Calculation and publication are never automatically retried.
In normal reuse mode, completed monthly downloads survive a transient failure
and are reused on retry. With `--force`, every retry deliberately refreshes the
selected files again, including already refreshed months; stale old files must
not be mistaken for a completed refresh. This can make extra HTTP requests.

```bash
uv run --locked pytest -n 2 --dist loadfile
GREEN_TAXI_DOCKER_TESTS=1 uv run --locked pytest tests/docker/ -q
```

The Docker tests require a built `green-taxi:local` image. They check non-root
execution, known saved values and rebuilds, plus the rendered Compose contracts
for loopback ports, health dependencies, persistent volumes, and manual profiles.
They use tiny temporary inputs, not the public dataset; ordinary tests skip them
unless explicitly enabled. CI validates Compose with the manual profile enabled.
Prefect integration tests use an isolated temporary server, not this Compose
server. Docker CI uses an Ubuntu 24.04 VM because `ubuntu-slim` is not our Docker
build/test runner; unit and integration jobs retain their existing slim runner.

## Stop safely

```bash
docker compose down
```

This removes only this project's containers/network, preserving named volumes.
Starting again keeps reports and run history. Do not add `--volumes` unless you
deliberately want to delete them. Demo Radar and other projects are unaffected.
The server has no authentication and is for local use only; do not expose its port
publicly or mount the Docker socket. No cloud deployment or image publication.

References: [Prefect retries](https://docs.prefect.io/v3/how-to-guides/workflows/retries),
[Prefect result caching](https://docs.prefect.io/v3/concepts/caching),
[Compose .env interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/).
