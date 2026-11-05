# Run with Docker and Prefect

Docker packages our existing Python application. Prefect adds task states and
bounded download retries; it does not replace the data rules. Both entry points
reuse `TaxiPipeline.from_settings()` and the same downloader/calculator/writer.

## Start and run

Run all commands in this guide in your **Mac/host terminal**, from the repository
root, not inside a container. No local configuration setup is required. To change
the default UI port, optionally copy the Compose template:

```bash
cp .env.example .env
```

If `.env` already exists, edit it instead of overwriting it. Compose automatically
reads this local file; Python does not load it. It contains only:

```dotenv
PREFECT_PORT=4205
# TAXI_CONFIG_FILE=./src/green_taxi/defaults.json
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

Normal Compose runs use the defaults packaged when the image was built. To read
the current file from your checkout without rebuilding, edit the existing file:

```bash
code src/green_taxi/defaults.json
```

Keep `duration_months` a positive integer, not a quoted string or float. Set only
`reporting_period.start_month` and `duration_months`: source filenames are derived
automatically, including year changes. An explicit `months` list is optional for
advanced source overrides; remove an old list to enable automatic selection.
No new directory, copied JSON, or `.env` is needed. Save the file, then run:

```bash
docker compose -f compose.yaml -f compose.config.yaml run --rm flow
```

To refresh the custom period deliberately, keep the config argument when
replacing the service command:

```bash
docker compose -f compose.yaml -f compose.config.yaml run --rm flow \
  python -m green_taxi.flow --config /run-config/run.json --data-dir /data --force
```

The earlier refresh command without the override uses defaults from the image,
not edits made after the image was built.

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
omit `-f compose.config.yaml` again. The `-f` arguments select Compose files;
the override mounts `src/green_taxi/defaults.json` read-only inside the container.
Edits to this tracked JSON will appear in Git. For a separate private config,
optionally set `TAXI_CONFIG_FILE` in `.env` to an existing readable JSON file;
keep it outside the repository or under ignored `config/local/`.
Remove an old `TAXI_CONFIG_FILE` setting to return to the automatic default path.
An exported shell variable takes precedence over `.env`.

If Docker reports `operation not permitted`, check that the selected host file
exists and Docker Desktop may access its directory. Missing files are never
created by Compose. No privileged container or broad permission changes are needed.

Editing JSON does not launch work. Run the flow command again after saving.
`docker compose up ... prefect` starts the server/UI, not the batch, and does not
create or overwrite host JSON files. After completion, open the **Save checked
report** task's logs to see period, rides, days, and total. It is a log summary,
not an interactive report table. The report-inspection command below shows metadata.

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

The image records optional `BUILD_REVISION` and `BUILD_DIRTY` build arguments in
run metadata; the revision is also an OCI image label. CI supplies its checkout
SHA and `false` for a clean checkout. Local builds default to `unknown`, not an
invented clean revision. For a local build, supply a revision and explicitly mark
uncommitted changes, for example:

```bash
BUILD_REVISION="$(git rev-parse HEAD)" BUILD_DIRTY=true docker compose build
```

Use `false` only for a clean source checkout. This is a build stamp, not a signed
proof or an image digest. The installed package has no Git checkout to inspect.
Result tables are not stored as Prefect pickles or cached
between runs. Prefect stores task/run states; our writer owns the report files.

## Retries and checks

The server writes generated UI bundles to `/home/app/ui`, not the root-owned
installed package. Its health check requires both API health and a served HTML
page; an API-only success does not prove that the dashboard is available.

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
The end-to-end Compose check uses an isolated project, the default non-root user,
tiny synthetic input in its own named volume, and a custom read-only JSON. It
checks the real UI/JS bundle, flow/task states, rebuild, and restart persistence,
then removes only that test project's containers, network, and volumes.
Prefect integration tests use an isolated temporary server, not this Compose
server. Docker CI uses an Ubuntu 26.04 VM because `ubuntu-slim` is not our Docker
build/test runner; unit and integration jobs retain their existing slim runner.

## Stop safely

```bash
docker compose down
```

This removes only this project's containers/network, preserving named volumes.
Starting again keeps reports and run history. Do not add `--volumes` unless you
deliberately want to delete them. Demo Radar and other projects are unaffected.
The server has no authentication and is for local use only; do not expose its port
publicly or mount the Docker socket. These local commands do not deploy a public service.

## Publish to GHCR

The first release, **2026.11.05**, is published and publicly downloadable.
Publishing distributes the app, not the taxi data, and does not deploy Prefect.

| Release check | Verified result |
|---|---|
| Image | `ghcr.io/roy-nfisch/nyc-taxi-revenue-data-pipeline:2026.11.05` |
| Digest | `sha256:1a83045ae680f6ed4f69e5c58bd930bc4e99e220577cdf8053cae332c6e317cb` |
| Source commit | `f78262b77e8ea9b3a47024960de958781c9c5338` |
| Platform / user | Linux amd64 / UID 10001 |
| Anonymous access | Dated tag and `latest` pulled with empty Docker login configuration |
| Fresh run | 146,475 accepted rides, 90 daily facts, USD 3,395,290.56 |

The dated tag and `latest` matched during verification. Use the date tag or digest
when you need a fixed release. [Publication run](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/actions/runs/42076273246).
The fresh verification used disposable container storage; ordinary Compose runs
reuse their named data/history volumes.

For future owner-approved releases from reviewed `main`:

1. In GitHub Actions, select **Publish pipeline image**, choose `main`, and enter
   an unused date tag, such as `2026.11.05.1` for another release that day.
   The existing `2026.11.05` release must not be overwritten.
2. The workflow checks Python/package quality, builds the image, and runs the
   tests. Only the separate publishing job has `packages: write` permission.
   GitHub supplies `GITHUB_TOKEN`; no personal token needs to be created.
3. It rejects an existing dated tag, transfers the exact tested image, publishes
   the dated/commit tags, pulls the published digest, and verifies a tiny staged
   pipeline run. Only then does it update `latest`.
4. The owner must make the GHCR package public in its settings and verify an
   anonymous pull. A public repository does not establish public package access.

The initial workflow targets **Linux amd64** on Ubuntu 26.04. Local Docker builds
remain available for the host architecture. Multi-platform release is not claimed.
If registry authorization fails, stop and review package permissions;
never interpret 401/403 as permission to overwrite a tag. If verification fails
after a dated tag was pushed, fix the cause and use a new suffix. Do not reuse it.

The README's direct batch command stores reports in `nyc-taxi-release-data`,
separate from Compose's volume. After running that command, inspect its report
with a read-only mount:

```bash
docker run --rm --platform linux/amd64 \
  --mount type=volume,source=nyc-taxi-release-data,target=/data,readonly \
  ghcr.io/roy-nfisch/nyc-taxi-revenue-data-pipeline:2026.11.05 \
  python -c "from pathlib import Path; print(Path('/data/processed/metadata.json').read_text())"
```

To run the verified release with Compose, set its tag in a host terminal:

```bash
export TAXI_IMAGE=ghcr.io/roy-nfisch/nyc-taxi-revenue-data-pipeline:2026.11.05
export DOCKER_DEFAULT_PLATFORM=linux/amd64
docker pull "$TAXI_IMAGE"
docker compose up -d --wait --no-build --pull always prefect
docker compose run --rm --no-build --pull always flow
```

The platform setting also allows Apple Silicon to run the published amd64 image
under emulation. The same named data/history volumes are used, so this is not
automatically a fresh-data run.
The read-only JSON override still works with the published image. To return to
local builds, run `unset TAXI_IMAGE` and remove or comment out any `TAXI_IMAGE`
entry in `.env` before the normal build/start commands. Otherwise Compose will
still use the value from `.env`.
Also run `unset DOCKER_DEFAULT_PLATFORM` when returning to native local builds.

For each future release, record its link and digest here, repeat the anonymous
pull and fresh isolated run, and retain the workflow evidence. Never remove normal
user volumes for that check. See [GHCR documentation](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-container-registry).

References: [Prefect retries](https://docs.prefect.io/v3/how-to-guides/workflows/retries),
[Prefect result caching](https://docs.prefect.io/v3/concepts/caching),
[Compose .env interpolation](https://docs.docker.com/compose/how-tos/environment-variables/variable-interpolation/).
