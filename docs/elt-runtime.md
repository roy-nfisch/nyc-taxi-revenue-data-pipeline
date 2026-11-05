# Run the ELT pipeline

ETL calculates in Python. ELT first stores checked raw files in DuckDB,
then uses SQL to calculate the same daily report. Both share the downloader,
data rules, report writer and run metadata.

## Local Python

From the repository root:

```bash
uv sync --locked --extra elt
uv run --locked --extra elt python -m green_taxi.elt
```

The first run downloads the configured months; later runs check and reuse them.
Use `--config path/to/run.json` for settings, `--data-dir path` for storage,
or `--force` to deliberately refresh sources.

## Docker and Prefect

```bash
docker compose -f compose.yaml -f compose.elt.yaml build elt-tools
docker compose -f compose.yaml -f compose.elt.yaml run --rm --no-deps elt
```

For four sequential Prefect stages—download, load, transform, save:

```bash
docker compose up --build -d --wait prefect
docker compose -f compose.yaml -f compose.elt.yaml run --rm elt-flow
```

Open [local Prefect](http://127.0.0.1:4205). No schedule is created.
Use `docker compose down` when finished; volume data is retained.
Local Python can use `python -m green_taxi.elt.flow` with an explicit
`PREFECT_API_URL`.

## Files and failures

- Shared inputs: `data/raw/`.
- ELT warehouse: `data/elt/warehouse.duckdb`.
- ELT reports: `data/elt/processed/`; ETL's `data/processed/` stays separate.

Docker stores these under `/data` in its named volume, not the host folder.
Only selected source versions contribute. Reruns replace reports, not append.
Sub-cents, no usable rides or mismatched sources stop the run before saving.
Existing reports describe their earlier run; successful publication returns a
new metadata path. Use one writer per storage directory.

ELT costs extra disk for raw database tables, memory to verify each source, and
maintenance for SQL and DuckDB. It is a comparison, not a requirement for every batch.
See [SQL flow and rules](elt-sql.md) and [loading limits](elt-loading.md).
