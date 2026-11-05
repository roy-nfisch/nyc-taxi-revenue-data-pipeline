# Loading raw ELT sources

`TaxiWarehouse` implements the raw-loading part of T11.3. This is not yet an
ELT pipeline or reporting command: calculation and orchestration are separate
tasks. Existing ETL commands and storage are unchanged.

Install the optional dependency with `uv sync --locked --extra elt`. Pass the
downloader's staged inputs to the warehouse:

```python
from green_taxi.download import TaxiDownloader
from green_taxi.elt.warehouse import TaxiWarehouse

inputs = TaxiDownloader(settings).download()
with TaxiWarehouse(settings.data_dir / "elt" / "warehouse.duckdb") as warehouse:
    warehouse.load(inputs)
```

Construction and module import do not open a database or import DuckDB. Context
entry creates the parent directory and registry; context exit always closes the
connection. Use one writer per warehouse and do not modify staged files during
loading. No connection is shared between runs.

## Identity and safe reruns

The filename must match the validated source month, for example
`green_tripdata_2031-01.parquet`. Before loading any source, the batch must be
nonempty and have distinct filenames and resolved paths. Checksum and size must
match the actual staged bytes. A source file is read as a checked byte snapshot;
the current file is checked again before commit, or before reporting cache reuse.

Each source has one native `raw.source_YYYY_MM` table. The loader constructs and
quotes this identifier only after validating the month. Registry values use
bound parameters. Raw columns do not receive bookkeeping fields, and duplicate
rows remain duplicated. Identical bytes under different valid filenames create
two independent sources.

The registry retains the filename, table name, SHA-256, size, row count and
original serialized Arrow schema, including field metadata, nullability and
timezone description. Matching metadata reuses the table after checking its
values. A changed source replaces its table and registry entry in one
transaction. Failures in loading, verification or registration roll back both.
Sources commit separately: a later failure stops the batch, but earlier completed
loads remain available for retry. The caller must not calculate or publish after
a failed `load`.

## Supported raw mappings

The loader accepts scalar Arrow null, boolean, signed/unsigned integer, floating
point, string/large-string, binary/large-binary/fixed-size-binary, date, timestamp
and decimal128 fields, subject to a successful native round trip on every load.
The required pickup column supports date, timestamp, numeric, text, boolean and
null; the amount column supports numeric, text, boolean and null. Nested,
dictionary, extension, decimal256, time and duration types are not supported.
Missing required fields, empty files and column names that collide ignoring case
are rejected explicitly. Empty or NUL-containing column names are also rejected.

DuckDB stores native values, while the registry retains Arrow descriptions that
native types need not reproduce exactly, such as string offset width and field
metadata. Before commit, the loader reads the native table back, safely restores
the original Arrow schema and checks every value and row count. Floating values
are checked by their bits, including NaNs and signed zero. Native booleans must
remain booleans; aware timestamps must remain aware. Timestamps are not shifted
to naive local times. DuckDB 2.0.0 preserves the tested aware and naive timestamps
at second, millisecond, microsecond and nanosecond precision, including an aware
value one nanosecond after the epoch. Unsupported engine mappings, rounding,
truncation or any failed value comparison abort the source transaction.

Boolean amounts, invalid text and aware pickups load unchanged. They are invalid
rows under reporting rules, not loading failures; SQL classifies them later.
Loading does not parse, clean, aggregate or deduplicate ride values.

## Checks and limits

Tiny Parquet tests exercise duplicate rows, arbitrary quoted column names, full
schema metadata, mixed required-column types, unchanged/changed sources,
identical bytes under different names, persistent timestamp round trips, staged
byte changes, malformed/unsupported schemas and failures after table replacement
or registry changes. Passive-import checks block the optional engine.

Loading materializes one Parquet file and its native verification result in
memory, not the whole selected batch. This deliberately simple boundary suits
the project's monthly Green Taxi inputs; very large individual files would need
a separate bounded-memory design. No automatic schema evolution, concurrent
ingestion or repair of externally changed database tables is provided.
