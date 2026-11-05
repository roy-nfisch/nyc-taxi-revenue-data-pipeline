# ETL and ELT contracts

This document distinguishes the existing Python ETL from the planned database-first
ELT. ELT components are delivered separately; there is no end-to-end ELT command yet.
The task list records each component's delivery status.
The design and delivery plan remain in [ADR 002](adr/002-elt-comparison.md) and
[TASKS.md](../TASKS.md#t11-delivery-plan).

| | ETL — implemented | ELT — proposed |
|---|---|---|
| Stages | Download → Python calculation → save | Download → load raw data → SQL transformation → save |
| Code | `green_taxi.etl` | `green_taxi.elt` |
| Entry point | `python -m green_taxi --run` | Not implemented yet |
| Orchestration | `green_taxi.flow` delegates to `green_taxi.etl.flow` | Separate Prefect flow planned |
| Output | `data/processed/` | `data/elt/processed/` |

Settings, the downloader, reporting contract and writer are shared. The registry,
raw database and proposed object boundary below are specific to ELT; they do not
change ETL's current behaviour or make DuckDB a default dependency.

## Storage and source identity

Reuse `PipelineSettings` and the downloader's `tuple[StagedFile, ...]`. Do not
introduce another configuration file or duplicate the reporting period.

| Location | Purpose |
|---|---|
| `settings.raw_dir` | Existing unchanged Parquet files, shared with ETL. |
| `settings.data_dir / "elt" / "warehouse.duckdb"` | Local database for raw records and registry. |
| `settings.data_dir / "elt" / "processed"` | ELT reports; never the ETL writer's destination. |

For this downloader, `source_id` is the staged filename, such as
`green_tripdata_2025-01.parquet`. It is not the checksum or configured URL.
The raw table is `raw.source_2025_01`, derived from the validated source month,
not from arbitrary SQL supplied in a path or filename. One database holds this
single Green Taxi source family. The loader rejects duplicate source identities
or resolved paths in a batch before making changes.

The packaged `sql/schema.sql` defines `loaded_sources`: source identity, raw table
name, SHA-256, byte size, row count and the original serialized Arrow schema.
`raw_table` stores the table name without the `raw.` prefix. Schema bytes come
from `pyarrow.Schema.serialize()`; they describe types and metadata, not ride IDs.
Only the loader writes the registry. Read SQL selects the requested source IDs
whose registered checksums match the run's `StagedFile` inputs.

## Raw records stay raw

Use a separate native table for each source file, preserving every original
column, its values and duplicate rows. Do not add bookkeeping columns to these
tables: a source might already contain a column with the same name. The registry
links each table to its original schema and source identity instead.

Do not union different files into a common numeric/timestamp schema before
classifying records. Boolean amounts must remain distinguishable from numbers;
timezone-aware pickups must remain distinguishable from naive timestamps. Keep
the original Arrow timezone/type metadata when a database type does not retain
the source's full type description. Do not cast aware pickups to naive timestamps.

The loader must verify a lossless mapping for a source before publishing its
table and registry entry. Unknown or lossy mappings stop the batch explicitly;
never silently stringify, round, drop columns or strip timezone information.
Automatic schema evolution is not part of this project. This foundation does
not yet claim that every Parquet type is supported; raw loading must document
and test the supported mappings, including the mixed-type required-column fixtures.
Boolean amounts and aware pickups are invalid *rows* under our rules, not reasons
to reject an otherwise representable source. SQL counts their exclusions.

Treat source tables as multisets, not unique rides. Reuse a matching source and
checksum; replace a changed source's table and registry together in one transaction.
A failed replacement rolls back both. A failure on any selected source stops the
run before calculation/publication, even if earlier source loads committed.
Two different source filenames with the same bytes must both contribute.

## Object boundary for the next tasks

### Per-source SQL boundary

Classify one source table at a time, before combining results across files. The
loader owns native raw tables and the original Arrow schema; it does not clean
rows. The query coordinator derives the following type labels from that schema:

- `pickup_kind`: `date`, `timestamp_naive`, `timestamp_aware`, `text`, `numeric`,
  `boolean` or `null`.
- `amount_kind`: `numeric`, `text`, `boolean` or `null`.

Unsupported required-column types must be reported explicitly; do not guess a
label. These labels describe source types, not whether individual rows are valid.
Text still requires per-row parsing using the existing ETL date/money rules.

`classify.sql` reads one validated, quoted `raw.source_YYYY_MM` identifier and
binds `$period_start`, `$period_end`, `$pickup_kind` and `$amount_kind`. It returns
one row for every input row, including duplicates, with this common shape:

| Column | Type | Meaning |
|---|---|---|
| `service_date` | Nullable `DATE` | Recorded day; null for an invalid pickup. No timezone shift. |
| `status` | `VARCHAR` | Exactly one of `accepted`, `invalid_pickup`, `outside_period`, `invalid_amount`, `sub_cent`. |
| `amount_cents` | Nullable `BIGNUM` | Exact signed integer cents for accepted rows; null otherwise. Never a float or rounded decimal. |

Check pickup, period, then amount, in that order. An in-period finite sub-cent
amount produces `sub_cent`: any such result fails the run before publication;
it is not another excluded-row count. An out-of-period amount is never evaluated
as money. Accepted zero and negative amounts retain their signs.

DuckDB 2.0.0 supports signed `BIGNUM` sums. Use that representation instead of
imposing a new fixed-width money limit. Its Python result may be a decimal string;
the final adapter converts exact integer cents, not float currency values.

`daily_revenue.sql` groups accepted classifications into `service_date`,
`trip_count` and `revenue_cents`. `counts.sql` produces the existing `COUNT_FIELDS`
and a separate sub-cent failure count. Combine per-source daily totals/counters
inside DuckDB using the common classified shape; never union differently typed
raw columns or aggregate ride values in Python. The result adapter will convert
the checked results into `RevenueResult` and reuse the existing calendar helper.

SQL fixtures must exercise the same boundary independently of the loader:
ordinary numeric rides, a separate boolean-amount source, a timezone-aware source,
and invalid/out-of-period text rows. Compare all outputs and failure outcomes with
ETL. This describes the query interface, not implemented transformation SQL.

### Warehouse operations

These are proposed method contracts for review, not placeholder Python methods:

| Proposed operation | Inputs | Result and failure behaviour |
|---|---|---|
| `TaxiWarehouse(database_path)` | The database path above. | Construction stores the path only; no I/O. |
| Context entry / exit | `with TaxiWarehouse(path) as warehouse`. | Open/initialise the connection at entry and close it on exit, including failure. No connection is shared between runs. |
| `warehouse.load(inputs)` | Nonempty, distinct staged inputs. | Return `None` only after all selected sources are registered. Verify staged identity and schemas; propagate errors, never skip a failed file. |
| `warehouse.calculate(inputs, period)` | The same selected inputs and validated `ReportingPeriod`. | Return the existing `RevenueResult`; reject missing or mismatched registry entries. Only selected sources contribute. |

Raw loading owns the connection and `schema.sql`. Transformations use
classification, aggregation and counter SQL against the agreed source tables;
their fixtures can create tiny tables directly without importing an unfinished
loader. Planned integration will connect query execution and result conversion
through `calculate`, then connect Prefect and the existing report writer. Do not
add empty methods or a generic backend interface just to let branches start early.

Query inputs use bound parameters; generated table identifiers must be validated
and quoted separately. Do not interpolate arbitrary filenames or settings into SQL.
Rules, output types and acceptance checks are defined once in ADR 002.

## Install and inspect

Ordinary ETL still uses `uv sync --locked`. To install the optional engine:

```bash
uv sync --locked --extra elt
```

SQL is package data, read with `importlib.resources.files("green_taxi.elt")`
and `joinpath("sql/schema.sql").read_text(encoding="utf-8")`, not a path relative
to the terminal. No database is created just by importing the package.

Import checks follow the same separation: shared modules in
`tests/green_taxi/test_imports.py`, ETL commands/stages in
`tests/green_taxi/etl/test_entry_points.py`, and the optional package in
`tests/green_taxi/elt/test_package_import.py`. They reuse one passive-import
fixture; the ELT package check also runs without DuckDB installed.
`tests/packaging/test_installed_package.py` builds and installs a wheel outside the
checkout, checks its SQL resource and runs tiny ETL with optional-engine imports
blocked. Integration CI runs these checks; Docker verification remains separate.

For reference: [DuckDB Python installation](https://duckdb.org/docs/stable/clients/python/overview)
and [timestamp types](https://duckdb.org/docs/stable/sql/data_types/timestamp).
Source metadata and explicit mapping checks matter because engine timestamp
types need not retain the original source timezone description.
