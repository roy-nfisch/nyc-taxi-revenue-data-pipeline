# ELT SQL transformations

These packaged queries classify rides and calculate daily revenue in DuckDB.
They are not yet a runnable ELT pipeline. The [ETL and ELT contracts](etl-elt-contract.md)
define the source types and shared report format.

## SQL files

| File | What it does |
|---|---|
| `normalize_money.sql` | Registers a connection-local helper for whitespace, underscores and Unicode digits. |
| `classify.sql` | Gives each ride a date, status and exact integer-cent amount. |
| `daily_revenue.sql` | Counts accepted rides and sums their cents by day. |
| `counts.sql` | Counts accepted, excluded and sub-cent records. |

Normalization is separate to keep classification readable. Its character map
preserves Python 3.13's Unicode digit rules; it does not repair invalid money.

## How to execute them

Load SQL through `importlib.resources.files("green_taxi.elt").joinpath("sql/<name>.sql")`,
not paths relative to the terminal. Execute `normalize_money.sql` once on each
connection before classification. Its temporary macro creates no persistent data.

For each source, replace `__SOURCE_TABLE__` with a validated, quoted table name:
`"raw"."source_2025_01"`. Allow only `source_[0-9]{4}_(0[1-9]|1[0-2])`;
never insert arbitrary filenames or SQL. Bind `period_start`, `period_end`,
`pickup_kind` and `amount_kind` as values. The start is included; the end is not.
Derive the kinds from the original Arrow schema, without changing its types.

Materialize each source's classification before checking or aggregating it.
Combine classifications with `UNION ALL` into `classified`, preserving duplicates.
Do not combine differently typed raw columns. The final two queries read that relation.

Before saving, reject any `sub_cent` count or zero `rows_used`.
`counts.sql` returns `COUNT_FIELDS` followed by `sub_cent`.
Daily output is `service_date`, integer `trip_count` and `revenue_cents BIGNUM`.
DuckDB returns these exact sums as integer strings; convert them to integer cents,
never through a float.

## Rules and limits

- Check pickup, then reporting period, then money. Excluded pickups bypass money parsing.
- Dates follow Python's supported ISO forms. Do not guess local formats or epochs,
  or convert timezone-aware pickups to naive dates.
- Money follows `Decimal(str(value))`, including scientific notation.
  Preserve negative and zero charges; reject sub-cents rather than rounding.
- The Unicode mapping follows Python 3.13 / Unicode 15.1. Review it if Python changes.
- More than **100,000 integer-cent digits** raises a resource-limit error.
  This is a failed run, not an excluded record. Exponential zero remains zero.

## ETL comparison

The existing ETL Parquet conversion can round large nullable integers and make
Float32 handling depend on pandas metadata. SQL deliberately preserves the
original values: nullable `9007199254740993` must stay exact, and Float32 `0.1`
must not hide sub-cents.

A separate ETL representation fix addresses these differences. Full parity needs
comparison against that corrected baseline. Tests cover the shared rules,
boundaries, counters, duplicates and actual Parquet representations.
