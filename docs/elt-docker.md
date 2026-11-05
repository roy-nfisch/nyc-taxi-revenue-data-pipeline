# ELT Docker image

The ELT image includes DuckDB and the packaged SQL resources. The default ETL
image does not include DuckDB and keeps its existing behaviour.

The commands below check the engine and SQL resources only; they do not run an
ELT pipeline. Connecting loading, transformations, reports and the ELT command
and Prefect flow remains planned work. See the [contracts](etl-elt-contract.md)
and [delivery plan](../TASKS.md#t11-delivery-plan).

## Build and inspect the ELT image

From the repository root, with Docker running:

```bash
docker compose -f compose.yaml -f compose.elt.yaml build elt-tools
docker compose -f compose.yaml -f compose.elt.yaml run --rm --no-deps elt-tools
```

The second command prints the DuckDB version and confirms the registry SQL is
packaged, then exits. It starts no server, downloads no taxi data, and creates
no database or reports. Do not use `compose up` for this check: the base file
also defines the existing Prefect service. `--no-deps` keeps the check standalone.

The override adds one manual-profile `elt-tools` service and builds the separate
`green-taxi-elt:local` image (`TAXI_ELT_IMAGE` can override its name). It explicitly
sets `INSTALL_ELT=true`; the Dockerfile accepts only `true` or `false`. A normal
`docker compose build` still installs only default ETL dependencies and builds
`green-taxi:local`, with DuckDB absent. The ELT preparation service has no ports,
Prefect dependency, or automatic startup.

Both images install the project non-editably and run as UID 10001. SQL is loaded
through `importlib.resources`, not from the terminal's current directory. The
preparation service shares the existing named `taxi-data:/data` mount but does
not write to it. Future ELT storage stays in `/data/elt/warehouse.duckdb` and
`/data/elt/processed/`, separate from existing ETL `/data/processed/`. Those
paths are a contract, not outputs produced by this preparation command.

## Verify without starting a server

Build both images, then run the focused checks:

```bash
docker compose build
docker compose -f compose.yaml -f compose.elt.yaml build elt-tools
uv sync --locked
GREEN_TAXI_DOCKER_TESTS=1 uv run --locked pytest tests/docker/test_elt_preparation.py -q
```

These checks render Compose without starting services, execute the packaged
registry SQL in an in-memory DuckDB database as the non-root image user, and
confirm the default ETL image does not install DuckDB. Container probes use
`--network none` and no data mounts. The checks expect the two default local
image names shown above, independent of custom deployment image names.

The regular Docker CI job builds both variants and runs all Docker checks.
The manual ETL release workflow also builds the ELT image **for verification
only** because it runs the same tests. It still saves and publishes only the
tested default ETL image; no ELT image/tag is uploaded or substituted for ETL
`latest`. Publishing the ELT image needs separate approval after the complete
ELT pipeline has been connected and verified.
