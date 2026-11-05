# ELT SQL transformations

T11.4 adds packaged SQL resources, not a runnable ELT command or warehouse
executor. T11.6 will connect execution, result conversion and checked publication.
The source/type interface remains defined in [the contract](etl-elt-contract.md).

## Query boundary

Read each file through `importlib.resources.files("green_taxi.elt")`, then
`joinpath("sql/classify.sql").read_text(encoding="utf-8")`. SQL is installed
package data; execution does not depend on the terminal's working directory.

`classify.sql` contains exactly one `__SOURCE_TABLE__` marker. The coordinator
must validate a registered table name against `source_[0-9]{4}_(0[1-9]|1[0-2])`
and quote the schema and table separately, for example
`"raw"."source_2025_01"`. Never insert a filename, path or arbitrary SQL.
The test helper demonstrates that boundary; a production executor is T11.6 work.

Bind `period_start`, `period_end`, `pickup_kind` and `amount_kind` as values.
The period is start-inclusive/end-exclusive. Derive validated kind labels from
the original Arrow schema, not a coerced union or a guessed database value.
Do not cast an aware timestamp to naive or a boolean amount to numeric first.
Missing required columns fail visibly when the query is bound.

Materialize each source's complete classification inside DuckDB before reading
counters or aggregating. Its columns are `service_date DATE`, `status VARCHAR`
and `amount_cents BIGNUM`. Retain duplicates. Combine these normalized results
with `UNION ALL` into a relation named `classified`; do not union native raw
columns from different sources. `daily_revenue.sql` and `counts.sql` select from
that relation without modifying it. The tests create tiny raw tables directly
and do not import the unfinished loader.

`counts.sql` returns `COUNT_FIELDS` in their existing order, followed by
`sub_cent`. A positive `sub_cent` must fail the run before publication, rather
than count as excluded money. A zero `rows_used` must also fail. The coordinator
must check both before passing facts to the shared writer. Daily results are
`service_date`, integer `trip_count`, and exact `revenue_cents BIGNUM`.
DuckDB 2.0.0 returns BIGNUM values as decimal strings: convert those integer
strings to integer cents, never through a float. The SQL adds and combines
money inside DuckDB; Python is only the result adapter.

## Exact parsing and operational limit

Pickup checks precede period checks, which precede money checks. Excluded
pickups do not enter the money parser. Text dates follow Python 3.13
`datetime.fromisoformat` forms, including basic/week dates, one-character
separators and fractional clocks, without accepting timezone-aware results,
numeric epochs, guessed day/month formats or invalid calendar/clock fields.
Week separator precedence and unusual fractional forms are tested against the
actual CPython baseline. See the [CPython parser source](https://github.com/python/cpython/blob/3.13/Modules/_datetimemodule.c).

Money text follows `Decimal(str(value))`: finite values, underscore handling,
Python whitespace, Unicode 15.1 decimal digits and scientific notation.
Coefficient/exponent text arithmetic determines exact integer cents and detects
sub-cents without rounding, float multiplication or a fixed DECIMAL precision.
The Unicode table matches the project's Python 3.13 baseline and needs review
if that baseline changes. Float32 is widened before text conversion because
default Arrow-to-pandas conversion exposes these as Python binary64 floats.
Pandas extension metadata can change that exposure, as noted below.

An accepted nonzero amount requiring more than **100,000 integer-cent digits**
raises an explicit SQL resource-limit error before building an exponent-sized
string. This operational limit is not an exclusion, rounding rule or new
`invalid_amount` category. It prevents adversarial exponents from allocating
unbounded memory. Exponential zero remains exact zero, very small nonzero
values are sub-cent, and excluded pickups bypass this allocation entirely.
Tests exercise extreme exponents without asking the ETL baseline to allocate
astronomical integer ratios.

## Known baseline representation gaps — separate fix required

These queries preserve the original raw integer exactly. They deliberately
do not reproduce an existing ETL precision defect: pandas' default Parquet
conversion promotes a nullable Arrow `int64` or `uint64` column to float64.
For `[9007199254740993, null]`, ETL reports `9007199254740992.00`, while SQL
correctly reports `9007199254740993.00`. Null rows are invalid money in both.
The real-Parquet SQL regression asserts original exact cents. The separate
ETL fix owns the baseline regression; SQL tests do not require the defect to persist.

Pandas extension metadata can also change scalar strings. An Arrow float32
`0.1` without pandas metadata becomes a Python float and is sub-cent in ETL
and SQL. With pandas nullable `Float32` metadata, ETL sees a numpy float32
whose string is `0.1` and accepts 10 cents; SQL still identifies the original
float32 value as sub-cent after widening. A second real-Parquet SQL regression
checks the original float bits with that metadata present. The separate fix
therefore concerns the ETL's Arrow-to-pandas dtype/metadata exposure, not only
large integers.

The owner approved a separately scoped ETL representation fix. The SQL PR
remains draft until that fix is reviewed and combined parity is verified
against the corrected baseline. Ordinary numeric, boolean,
aware-timestamp, invalid text,
exclusion precedence, counters, duplicate sources, exact large-money and
sub-cent cases are compared with ETL. These dtype/metadata counterexamples
prevent claiming universal ETL/ELT parity or completing T11.4.
