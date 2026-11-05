# Preserve Parquet numeric precision in ETL

`DailyRevenueCalculator` reads Parquet with pandas' Arrow-backed dtype backend.
This keeps the stored nullable numeric type until existing row validation runs;
it does not add a rounding rule or change the shared money helper.

## Why this matters

The default pandas conversion can promote a nullable integer column to float64
when the file lacks pandas extension metadata. That can change large amounts
before exact-cent validation sees them. For example, an int64 amount of
`9007199254740993` beside a null previously became `9007199254740992.00` in the
report. The corrected read preserves `9007199254740993.00`. Nullable uint64
amounts are also preserved, including `18446744073709551615`.

Pandas extension metadata could also change the treatment of float32 money.
A stored binary32 `0.1` is exposed as `0.10000000149011612` when widened to a
Python float. Previously, a nullable pandas `Float32` column instead yielded a
scalar whose text was `0.1`, hiding the sub-cent precision. Arrow-backed reads
now consistently reject that binary32 amount, whether or not the file includes
pandas metadata. This intentionally tightens the formerly metadata-dependent
case to the existing no-unreviewed-rounding rule.

Float64 `0.1` still has the Python decimal text `0.1` and is accepted as ten
cents. This is not a promise that every binary floating-point fraction is an
exact decimal fraction: the agreed money rule uses decimal text, not the full
binary expansion. Decimal values, zero and negative amounts, invalid/nonfinite
amount exclusion, boolean rejection, and pickup-date rules remain unchanged.
The SQL implementation should preserve native numeric values rather than copy
the old nullable-integer conversion error.

## Verification

Real Parquet regressions cover nullable int64 and uint64 with and without
pandas extension metadata, both float32 encodings, ordinary float64 `0.1`,
Decimal zero/negative/null values, nonfinite values and native booleans. The
existing date, sub-cent, accounting and pipeline tests continue to apply.

The reviewed January–March 2025 files were calculated with both the old default
read and the Arrow-backed read. Daily facts, calendar, all accounting counts and
the total were identical: 146,486 input rides, 146,475 accepted rides, 90 daily
facts and **$3,395,290.56**. This verifies that reviewed float64 source data stays
unchanged; arbitrary future schemas still need their own data review.

No dependency versions, SQL transformations or output schemas change here.
