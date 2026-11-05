# Run with Docker and Prefect

Docker packages our existing Python application. Prefect adds task states and
bounded download retries; it does not replace the data rules. Both entry points
reuse `TaxiPipeline.from_settings()` and the same downloader/calculator/writer.

## Start and run

From the repository root, check port 4205 is free, then:

```bash
docker compose up --build -d --wait prefect
docker compose run --rm flow
```

Open **http://127.0.0.1:4205**. Under Runs, select `green-taxi-batch` to see:

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

To use another host port, set it when starting the server and flow:

```bash
PREFECT_PORT=4206 docker compose up -d --wait prefect
PREFECT_PORT=4206 docker compose run --rm flow
```

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
[Prefect result caching](https://docs.prefect.io/v3/concepts/caching).
