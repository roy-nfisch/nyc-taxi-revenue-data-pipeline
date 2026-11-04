# Real-data review

Reviewed locally on **04.11.2026**, using the January–March 2025 source files.
This records what we checked, not just a successful command exit.

## Result

| Check | Observed result |
|---|---:|
| Raw rides read | 146,486 |
| Accepted rides | 146,475 |
| Outside the reporting period | 11 |
| Invalid pickup dates | 0 |
| Invalid amounts | 0 |
| Accepted negative amounts, retained | 478 |
| Accepted zero amounts, retained | 254 |
| Accepted rides with missing payment type, retained | 7,902 |
| Daily facts / calendar dates | 90 / 90 |
| Recorded charges | **$3,395,290.56 USD** |

The period is 01.01.2025 inclusive to 01.04.2025 exclusive. Negative/zero
counts are part of the accepted rides, not additional exclusions.

| Pickup month | Accepted rides | Recorded charges (USD) |
|---|---:|---:|
| January | 48,288 | 1,092,994.80 |
| February | 46,645 | 1,067,222.12 |
| March | 51,542 | 1,235,073.64 |

Filename month and pickup month are not always the same. These counts group by
pickup date across all three files, not simply by the source filename.

## What we checked

- Ran the existing batch command; all three staged files passed validation and
  were reused without network downloads.
- Independently read the raw files without calling the calculator's rule or
  money helpers. Used pandas for the date/amount masks and Python `Fraction`
  arithmetic for exact cents. Every accepted amount was expressible in cents.
  Compared **all 90 daily counts and amounts**, not only the grand total.
- Checked exclusion accounting against `metadata.json`; included and excluded
  rides reconcile to all input rows. Checked input byte sizes and SHA-256 values.
- Read both Parquet tables and the CSV back. Checked equal fact values, complete
  calendar coverage, non-null unique dates, and a one-to-one join with unchanged
  ride counts and money totals.
- Installed the lock file into a new temporary virtual environment and reran
  the actual command. Fact/date tables were equal; counts, totals, input identities,
  and resolved period were unchanged. The run ID changed as expected.
- In that fresh environment, all **190 application tests**, three reference
  tests, Ruff lint/format checks, and the package build passed locally.

Code revision: `eea874cd39c99a2c9e8adcd42dc4456786b57881` (merged PR #5).
Metadata correctly reported `dirty: true`: the pending task-list edit existed
at run time. This is not a claim that the entire checkout was clean.

Run IDs: `889c731f-ce68-435b-9a71-b9d470666e49` (initial run) and
`eea1308d-4669-47d5-a47b-5e4c959bf1d8` (fresh-environment rebuild).

### Source identities

These identify the files reviewed; they are evidence, not permanent test fixtures.
If the provider replaces a file, review the changed contents instead of assuming
the same expected totals still apply.

| File | Bytes | SHA-256 |
|---|---:|---|
| `green_tripdata_2025-01.parquet` | 1,178,451 | `84f3a121667157efcbf012c3566a6065df6f8e0312c678cb2f29cd72cc9c0f10` |
| `green_tripdata_2025-02.parquet` | 1,117,474 | `b3ffe16a96ba0c68c8a8f8b10f049294ae6eb8d21f2e13b3fdbd452fa2e3498e` |
| `green_tripdata_2025-03.parquet` | 1,253,510 | `d58e8d08e5e8a34c12f0d3f5ea049ae25c7c8215218a6af7bf6b8124ecde1c79` |

## Run and inspect again

From the project root, run this independent check. It reads the current report
and raw files, checks each input checksum, and calculates expected daily totals
using `Fraction` rather than the application's calculation helpers. It then
keeps the first tables in memory, runs the batch again, and compares the second
tables and run counts. The batch **replaces** the reports in `data/processed/`;
it does not modify raw files. Only one process should use this directory at a time.

```bash
uv run --locked python - <<'PY'
import hashlib
import json
import subprocess
import sys
from collections import defaultdict
from fractions import Fraction
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
from green_taxi.settings import PipelineSettings

settings = PipelineSettings.from_file()
folder = settings.processed_dir
metadata_path = folder / 'metadata.json'
first = json.loads(metadata_path.read_text())
facts = pd.read_parquet(folder / 'fact_daily_revenue.parquet')
dates = pd.read_parquet(folder / 'dim_date.parquet')
period = first['reporting_period']
start, end = pd.Timestamp(period['start_inclusive']), pd.Timestamp(period['end_exclusive'])

frames = []
for source in first['inputs']:
    path = settings.raw_dir / Path(source['filename']).name
    assert path.stat().st_size == source['size_bytes']
    with path.open('rb') as stream:
        assert hashlib.file_digest(stream, 'sha256').hexdigest() == source['sha256']
    frames.append(pd.read_parquet(path, columns=['lpep_pickup_datetime', 'total_amount']))
raw = pd.concat(frames, ignore_index=True)
pickup = pd.to_datetime(raw.lpep_pickup_datetime, errors='coerce')
assert pickup.dt.tz is None, 'This check expects timezone-naive source dates.'
in_period = pickup.notna() & (pickup >= start) & (pickup < end)
amount = pd.to_numeric(raw.total_amount, errors='coerce')
valid_amount = amount.notna() & np.isfinite(amount)
accepted = raw.loc[in_period & valid_amount]
counts = first['counts']
assert counts['rows_read'] == len(raw)
assert counts['invalid_pickup'] == int(pickup.isna().sum())
assert counts['outside_period'] == int((pickup.notna() & ~in_period).sum())
assert counts['invalid_amount'] == int((in_period & ~valid_amount).sum())
assert counts['rows_used'] == len(accepted)
assert counts['negative_amount'] == int((accepted.total_amount < 0).sum())
assert counts['zero_amount'] == int((accepted.total_amount == 0).sum())

expected = defaultdict(lambda: [0, 0])
for day, value in accepted.itertuples(index=False, name=None):
    cents = Fraction(str(value)) * 100
    assert cents.denominator == 1, 'Unexpected sub-cent source amount.'
    expected[day.date()][0] += 1
    expected[day.date()][1] += int(cents)
actual = {
    day: [count, Fraction(revenue) * 100]
    for day, count, revenue in facts.itertuples(index=False, name=None)
}
assert facts.service_date.is_unique and not facts.isna().any().any()
assert dates.service_date.is_unique and not dates.isna().any().any()
assert actual == dict(expected)
assert Fraction(first['total_amount']) * 100 == sum(row[1] for row in expected.values())
assert dates.service_date.tolist() == list(pd.date_range(start, end, inclusive='left').date)
joined = facts.merge(dates, on='service_date', validate='one_to_one')
assert len(joined) == len(facts)

subprocess.run([sys.executable, '-m', 'green_taxi', '--run'], check=True)
second = json.loads(metadata_path.read_text())
assert_frame_equal(facts, pd.read_parquet(folder / 'fact_daily_revenue.parquet'))
assert_frame_equal(dates, pd.read_parquet(folder / 'dim_date.parquet'))
assert first['run_id'] != second['run_id']
assert first['counts'] == second['counts']
assert first['total_amount'] == second['total_amount']
assert first['reporting_period'] == second['reporting_period']
for old, new in zip(first['inputs'], second['inputs'], strict=True):
    assert (old['filename'], old['size_bytes'], old['sha256']) == (
        new['filename'], new['size_bytes'], new['sha256']
    )
print(f"Verified {len(accepted):,} accepted rides across {len(facts)} days; rebuild matches.")
PY
```

Run `uv run --locked python -m green_taxi --run` first if no report exists. This
check is for the current, timezone-naive source schema; it fails if the data or
rules no longer meet its assumptions. Run IDs, timestamps, and download/reuse
flags can legitimately differ. Also run the synthetic checks:

```bash
uv run --locked pytest -n 2 --dist loadfile
```

For a new virtual environment:

```bash
taxi_review_dir=$(mktemp -d)
UV_PROJECT_ENVIRONMENT="$taxi_review_dir/.venv" uv sync --locked
UV_PROJECT_ENVIRONMENT="$taxi_review_dir/.venv" uv run --locked python -m green_taxi --run
UV_PROJECT_ENVIRONMENT="$taxi_review_dir/.venv" uv run --locked pytest -n 2 --dist loadfile
```

## Limits

This reconciles the report with these source files under our accepted rules. It
does not prove that the provider captured every ride or that charges equal profit
or verified cash collected. Cash tips are excluded from the source total.
No bad-date/amount rows occurred here; synthetic tests cover those failure cases.
Raw and generated files remain ignored. Docker/Prefect execution and GitHub CI
success are not established by this local review; container verification follows.
Only one run per data directory is supported; publication is not a multi-file
transaction. No new service or calculation implementation was needed.
