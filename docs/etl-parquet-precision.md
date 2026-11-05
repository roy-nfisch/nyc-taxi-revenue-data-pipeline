# Keep amounts unchanged when reading files

Reading a data file should not change its numbers. We fixed two cases where
pandas, the library we use to read Parquet files, could do that.

## What was wrong?

- **Large whole numbers could lose accuracy when the same column contained
  missing values.** For example, `9007199254740993` could become
  `9007199254740992` before we checked it.
- **Some decimal amounts were handled differently depending on how the file
  was saved.** A Float32 value displayed as `0.1` actually holds a small extra
  fraction. File metadata could hide that fraction from our checks.

## What changed?

We now read the file while keeping its original numeric types, using
`pd.read_parquet(path, dtype_backend="pyarrow")`.

Large whole numbers stay unchanged. Float32 amounts are checked consistently:
if their decimal text contains a fraction of a cent, the run stops instead of
silently rounding. Ordinary Float64 `0.1` still counts as ten cents.

The date rules and treatment of missing, zero and negative amounts stay the same.
This changes how we read numbers, not the revenue calculation or report format.

## Did the taxi results change?

**No—not for the January–March 2025 files we reviewed.** Both versions produced:

- 146,486 rides read and 146,475 accepted.
- 90 daily report rows.
- **$3,395,290.56** in recorded charges.

Tests also cover the problem cases using small Parquet files. The reviewed
results are unchanged, but other files still need their own data checks.
