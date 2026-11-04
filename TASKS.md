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

- [x] **T03 — Download and stage the files.** Implementation reviewed and accepted
  on 04.11.2026; delivery is tracked in [PR #3](https://github.com/roy-nfisch/mle-data-pipeline-project/pull/3),
  not yet merged. `TaxiDownloader` uses shared JSON months/source URL, validates
  complete Parquet before reuse/publication, preserves old files on failed
  refreshes, and returns input identities for lineage. Safe failure logs and shared
  synthetic fixtures cover HTTP, interruption, corruption, and storage failures.
  Real January–March download/reuse checks passed; raw files remain ignored.
  Local verification passes 58 application tests (27 unit, 31 integration; 19
  additionally labelled data), three reference tests, lint/format, and build.
  CI separates categories and uses two integration workers without a duplicate
  data job. Latest GitHub checks remain a merge gate, not a claimed result here.
  No revenue outputs have been created.

## To do

- [ ] **T04 — Build the daily calculation.** Apply only approved data rules and
  aggregate `fact_daily_revenue` by pickup day. Generate `dim_date` with a small
  pure helper, not a separate service. Done when small known examples cover
  boundaries, invalid rows, multiple files, exact daily counts/amounts, and the
  90-day calendar without invented zero-revenue fact rows.
- [ ] **T05 — Save the report and connect the command.** Compose the objects into
  one local entry point. Done when a temporary-file integration test checks the
  saved fact/date schemas, values, and repeat-run behaviour, including explicit
  write failures. Verify unique dates, matching date references, and no row/total
  changes after joining. Save fact Parquet/CSV, date Parquet, and lineage metadata:
  run ID, UTC start/finish times, reporting period, input details, output filenames,
  and row counts. Record code revision/dirty status when available, otherwise
  explicitly unknown. Test that metadata describes the actual inputs and outputs.
- [ ] **T06 — Run the real data and review the result.** Process all three files;
  account for included/excluded rows and reconcile report totals with accepted rides.
  Check the saved date dimension and matching daily facts too. Done when outputs
  are inspected, limitations documented, and checks pass from
  a fresh environment. Record evidence, not just a successful exit status.
  Verify a rebuild from staged inputs produces equivalent tables without doubling
  counts or amounts; run-specific metadata may legitimately differ.
- [ ] **T07 — Prepare the portfolio and close out.** Update the README with the
  actual file tree, working commands, a small output example, and course reflections.
  Explain why this is batch ETL, how to manually launch/rebuild the reporting
  period, and how to deliberately refresh inputs and trace them through metadata.
  Done when another person can follow the instructions and we have reviewed scope,
  tests, source attribution, and generated files excluded from Git. Closeout waits
  for T08 and T09, including verified Docker and Prefect commands.

## Required orchestration and container delivery

- [ ] **T08 — Prefect orchestration.** Use the shared components in extract,
  transform, and load tasks. Add bounded retries for transient failures, not bad
  data. Done when a container-run flow is visible in the local Prefect UI, failures
  and safe retries are tested, and the plain Python command still works. No copied
  transformation logic. Start runs manually; no automatic schedule initially.
  Demonstrate a manual run, rebuild, and simulated transient-failure recovery.
  Prefect is the planned tool; an alternative needs review.
- [ ] **T09 — Docker and Compose.** Package the shared command and Prefect flow;
  run local Prefect services with persistent state and raw/report storage. Check
  occupied ports before starting. Done when a fresh Compose run produces the same
  checked report, a rerun does not duplicate results, and run/inspect/stop commands
  are verified. No public hosting or image publication without separate approval.

T08 and T09 can be developed together once the foundation and pipeline stages
are ready. Both must pass before final closeout; their numbers do not require
waiting until after T07 to start container work.

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
