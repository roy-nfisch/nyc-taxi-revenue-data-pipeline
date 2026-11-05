# Project tasks

Goal: build a batch ETL pipeline that downloads January–March 2025 Green Taxi files and produces a checked daily
revenue report and a small date dimension. Scope comes from the [README](README.md); accepted decisions
live in [ADR 001](docs/adr/001-pipeline-design.md).

**Required delivery:** the pipeline runs in Docker and through Prefect. The
course calls Prefect a bonus, but both tools are mandatory for our project.

**To do** = not started. **In progress** = being worked on or awaiting review.
**Done** = completion checks passed and reviewed. A draft is not approval.

## Done

- [x] **T01 — Agree on the plan and data rules.** Done on 04.11.2026: inspected
  official field definitions, real example rows, and all 146,486 records in the
  three requested files using temporary storage outside the repository. The owner
  approved the data rules and fact/date layout; the OOD plan, required Docker/Prefect
  delivery, and acceptance checks are recorded in ADR 001. Expected inclusion is
  146,475 records over 90 dates. This is inspection evidence, not an implemented
  pipeline or passing application tests. No application code or staged data was created.

- [x] **T02 — Create the Python foundation.** Reviewed and merged in
  [PR #2](https://github.com/roy-nfisch/mle-data-pipeline-project/pull/2).
  Includes immutable Pydantic settings, JSON month selection, explicit file loading,
  application logging, installable packaging, mirrored tests, and feature-branch/PR
  CI. Expanded docstrings explain inputs, outputs, errors, and side effects.
  Local checks passed 34 foundation tests, three reference tests, lint/format,
  and package build. Configuration does not start downloads or Prefect.

- [x] **T03 — Download and stage the files.** Reviewed and merged in
  [PR #3](https://github.com/roy-nfisch/mle-data-pipeline-project/pull/3).
  `TaxiDownloader` uses shared JSON months/source URL, validates
  complete Parquet before reuse/publication, preserves old files on failed
  refreshes, and returns input identities for lineage. Safe failure logs and shared
  synthetic fixtures cover HTTP, interruption, corruption, and storage failures.
  Real January–March download/reuse checks passed; raw files remain ignored.
  Local verification passes 58 application tests (27 unit, 31 integration; 19
  additionally labelled data), three reference tests, lint/format, and build.
  CI separates categories and uses two integration workers without a duplicate
  data job. Latest GitHub checks remain a merge gate, not a claimed result here.
  No revenue outputs have been created.

- [x] **T04 — Build the daily calculation.** Reviewed and merged in
  [PR #4](https://github.com/roy-nfisch/mle-data-pipeline-project/pull/4).
  Apply only approved data rules and
  aggregate `fact_daily_revenue` by pickup day. Generate `dim_date` with a small
  pure helper, not a separate service. Done when small known examples cover
  boundaries, invalid rows, multiple files, exact daily counts/amounts, and the
  90-day calendar without invented zero-revenue fact rows.
  Completed; report writing and the complete command remain separate.
  Local checks pass 154 application tests, including 53 integration tests rerun
  with two workers, plus three reference tests, lint/format, and package build.
  This verifies tiny synthetic inputs, not the later full-data report review.

  Rules applied:

  - Read recorded `total_amount` only; sum exact integer cents and return decimal
    USD values. Sub-cent values fail instead of silently rounding.
  - Use recorded pickup dates without guessing ambiguous formats or converting
    timezones. Exclude invalid pickups, out-of-period dates, then invalid amounts;
    count each exclusion once. Missing required columns or no usable batch fails.
  - Keep zero/negative amounts and count them. Missing payment type does not
    exclude an otherwise usable ride. Do not deduplicate similar-looking rides.
  - Reject repeated resolved file paths; combine dates across different files.
  - Configure `start_month` and strictly positive integer `duration_months`;
    derive inclusive start/exclusive end in `ReportingPeriod`. Reject strings,
    floats, booleans, null, zero, negative durations, and calendar overflow.
    The calculator receives only this period, not unrelated download settings.
  - Keep one fact per accepted pickup day; generate every date in the period
    separately. A missing fact does not become an invented zero-revenue day.

- [x] **T05 — Save the report and connect the command.** Reviewed and merged in
  [PR #5](https://github.com/roy-nfisch/mle-data-pipeline-project/pull/5).
  Compose the objects into
  one local entry point. Done when a temporary-file integration test checks the
  saved fact/date schemas, values, and repeat-run behaviour, including explicit
  write failures. Verify unique dates, matching date references, and no row/total
  changes after joining. Save fact Parquet/CSV, date Parquet, and lineage metadata:
  run ID, UTC start/finish times, reporting period, input details, output filenames,
  and row counts. Record code revision/dirty status when available, otherwise
  explicitly unknown. Test that metadata describes the actual inputs and outputs.
  Completed. Local checks pass 190
  application tests with two workers, three reference tests, lint/format, and
  package build. Tests cover read-back values, reruns, lineage, and staging/write
  failures. Full real-data reconciliation stays in T06.
  Audit fixes: verify staged Parquet/CSV values before publication, reject
  non-decimal money and invalid run counters, and cover every replacement failure
  point, prior-report preservation, and known/unknown Git identity. No new services
  or dependencies; existing objects keep their responsibilities.

- [x] **T06 — Run the real data and review the result.** Reviewed and merged in
  [PR #6](https://github.com/roy-nfisch/mle-data-pipeline-project/pull/6).
  Process all three files;
  account for included/excluded rows and reconcile report totals with accepted rides.
  Check the saved date dimension and matching daily facts too. Done when outputs
  are inspected, limitations documented, and checks pass from
  a fresh environment. Record evidence, not just a successful exit status.
  Verify a rebuild from staged inputs produces equivalent tables without doubling
  counts or amounts; run-specific metadata may legitimately differ.
  Local verification complete: 146,486 inputs, 146,475 accepted rides,
  11 out-of-period exclusions, 90 daily facts/calendar dates, and $3,395,290.56 USD.
  All daily values reconcile independently; a fresh-environment rebuild preserves
  tables and totals. Evidence, source identities, and limits are recorded in
  [the real-data review](docs/data-review.md). No application changes were needed.
  A [compressed terminal GIF](docs/assets/pipeline-demo.gif) in the README shows
  the successful real-data summary and sample daily results, with local machine
  details cropped. The
  temporary print-only demo script was removed; the standard batch command stays
  documented in the README.
  The review includes a repeatable independent calculation and a rebuild check;
  the documented commands were rerun against the staged real data.

## In progress — required orchestration and container delivery

Work order: **T08 and T09 → T07 → T10**. Task numbers stay unchanged.

- [ ] **T08 — Prefect orchestration.** Use the shared components in extract,
  transform, and load tasks. Add bounded retries for transient failures, not bad
  data. Done when a container-run flow is visible in the local Prefect UI, failures
  and safe retries are tested, and the plain Python command still works. No copied
  transformation logic. Start runs manually; no automatic schedule initially.
  Demonstrate a manual run, rebuild, and simulated transient-failure recovery.
  Prefect is the planned tool; an alternative needs review.
  Implemented for review: three sequential tasks call shared stage objects and
  shared lineage/publication code. Download retries are limited to transient
  network/HTTP failures; real Prefect tests cover recovery, exhaustion, and corrupt
  data without retries. No schedule or result persistence is added.
- [ ] **T09 — Docker and Compose.** Package the shared command and Prefect flow;
  run local Prefect services with persistent state and raw/report storage. Check
  occupied ports before starting. Done when a fresh Compose run produces the same
  checked report, a rerun does not duplicate results, and run/inspect/stop commands
  are verified. No public hosting or image publication without separate approval.
  Implemented for review: non-root image, loopback UI on checked port 4205,
  explicit project network, and separate persistent data/history volumes. Real
  container flow and plain-container command both processed the agreed period.
  CI adds an Ubuntu 24.04 Docker job with tiny synthetic inputs; native Python
  jobs retain slim runners. See [run instructions](docs/docker-prefect.md).
  An optional Compose override mounts a trusted custom JSON file read-only for
  both run modes. Base Compose keeps packaged defaults; missing config paths fail
  without creating directories. Tests verify the mount rejects writes and a
  custom reporting month is used by a real container run.
  Compose-only settings use a committed `.env.example` and ignored host `.env`.
  Pipeline values stay in JSON; Python has no dotenv loader. Tests cover automatic
  `.env` loading, shell precedence, and packaged defaults without the override.

T08 and T09 can be developed together now that the local pipeline is verified.
Both must pass before we resume T07 so its documentation covers the actual
Docker and Prefect commands, not a planned setup.

Prior verification, before the latest follow-up: 215 native application tests passed (13 opt-in Docker tests
skipped there), all 13 Docker smoke/contract tests passed when enabled, and three reference
tests, lint/format, package/image builds, and Compose configuration passed. Real
container facts/calendar matched the reviewed host tables; a rebuild reused
inputs and preserved 146,475 rides / $3,395,290.56. Down/up preserved run history
and data. Both tasks remain awaiting review.

Review follow-up: limit retryable server responses to HTTP 500–599; validate all
Compose profiles in CI. Added full-flow failure propagation, CLI option forwarding,
forced-refresh retry behavior, saved container values, and loopback/persistence
contracts. No new service or application abstraction was needed.
Additional requested follow-up: writable UI bundle directory and HTML-aware
health check; opt-in Compose end-to-end check using private named volumes and
the default user; optional image build provenance; ignored local config folder.
These follow-up checks passed in GitHub CI for commit `9915519`; they were not
rerun locally during the PR audit.
Settings now derive source months from the reporting period when `months` is
omitted. Defaults and the local example require one period edit; an explicit
source list remains supported. Added year-rollover and invalid-duration cases;
these cases passed in GitHub CI for commit `9915519`.
The save task now logs the saved report's period, ride/day counts, and total in
Prefect after successful publication, without repeating the calculation.
CI verified on 05.11.2026: the push run
[42025803602](https://github.com/roy-nfisch/mle-data-pipeline-project/actions/runs/42025803602)
passed lint/format, reference tests, package build, unit/integration tests, and
Docker checks. In the separate PR run, checks, unit tests, and Docker checks have
passed; integration tests were still pending at this review.
Audit follow-up adds custom-config refresh instructions, updates stale container
and layout descriptions, and removes explicit months from the Compose end-to-end
fixture so it exercises automatic period selection. These latest edits have not
been tested; the next CI run must verify them before merge.
Enforced single-writer locking, digest/signed provenance, and measured image-size
optimization remain future hardening, not extra requirements for this local demo.

## After Docker and Prefect

- [ ] **T07 — Prepare the portfolio and closeout documentation.** Postponed until
  T08 and T09 are verified. Update the README with the actual file tree, working
  commands, a small output example, and course reflections. Explain why this is
  batch ETL, how to manually launch/rebuild the reporting period, and how to
  deliberately refresh inputs and trace them through metadata. Done when another
  person can follow the instructions and we have reviewed scope, tests, source
  attribution, and generated files excluded from Git. Final public release is T10.

## Final review and public release

- [ ] **T10 — Audit and prepare the project for public release.** After the
  working pipeline, Docker, and Prefect are verified:
  - Audit code, data rules, security, tests, and documentation. Keep OOD simple;
    fix confirmed findings and rerun the checks. Include an independent review
    with a different model and record any remaining limitations.
  - Choose a clear project name with the owner, rename the GitHub repository,
    and update local remotes, links, badges, workflows, and Docker/Prefect
    references where affected. Verify commands still work after the rename.
  - Check tracked files and Git history for secrets or private information;
    review licensing, attribution, and data redistribution. Remove the supplied
    reference before public release, as planned; keep raw/generated data ignored.
    Do not rewrite history without separate approval if sensitive content is found.
  - Make the repository public once release checks pass and verify anonymous
    access. This does not authorize public service hosting or image publication.
  - Assess whether the finished project adds value to grossman.engineering
    alongside King County. Record the recommendation; if agreed, add a short
    English/German project entry linking to the final repository without repeating
    its detailed documentation.
  Done when findings are resolved or clearly documented, the renamed public
  repository works for a new visitor, and the portfolio decision is recorded.

## Optional follow-up — not a release requirement

- [ ] **T11 — Explore an ELT version.** Keep the checked ETL implementation as
  the comparison baseline; this task does not block T07/T10 or public release.
  - Agree on a short ADR before implementation. Choose one local analytical
    database (DuckDB or PostgreSQL), not two implementations.
  - Extract the same taxi files, then load unchanged source records into raw
    database tables before applying business transformations. Merely staging
    Parquet files on disk does not make the current pipeline ELT.
  - Transform inside the database using SQL, optionally managed by dbt, to build
    the same daily facts and date dimension. Prefect coordinates the stages;
    reuse existing downloading, settings, and source identities where suitable.
  - Preserve the approved dates, exclusions, exact-cent money rules, and lineage.
    Make repeated raw loads and report builds safe without duplicated records.
  - Compare all daily values, counts, date references, and totals with the ETL
    baseline. Test invalid records and rebuilds, document the trade-offs, and
    keep the code simple and OOD first.
  Done when an ELT run is reproducible and equivalent under the agreed rules,
  with a clear explanation of when the extra database/SQL layer is worthwhile.

## Working agreement

- OOD first: small objects with one job, connected through composition.
  No generic framework, unnecessary inheritance, or class for every helper.
- Keep functions and methods within 50 lines where practical; do not compress
  unreadable code to meet a limit. Use simple wording and useful docstrings.
- Validate settings at the boundary; validate data where it is read and transformed.
  Log useful counts and failures. Do not silently invent missing values.
- Use small branches and PRs; update task status before opening each PR.
  Add tests with the behaviour they cover, without duplicating coverage.
- Review each task for correctness, security, and simple OOD. For major milestones,
  request an independent audit with a different model and record actual findings.
- Keep the supplied `solution/` unchanged. Keep raw/generated data out of Git;
  if we use notebooks, clear outputs, execution counts, and unnecessary metadata.
- Do not implement until the proposed plan is reviewed. Commit, push, and open
  PRs only when requested. Keep Docker and Prefect within scope; further extensions
  need review and must not delay the required deliverable.
