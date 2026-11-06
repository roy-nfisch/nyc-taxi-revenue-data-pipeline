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
  [PR #2](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/2).
  Includes immutable Pydantic settings, JSON month selection, explicit file loading,
  application logging, installable packaging, mirrored tests, and feature-branch/PR
  CI. Expanded docstrings explain inputs, outputs, errors, and side effects.
  Local checks passed 34 foundation tests, three reference tests, lint/format,
  and package build. Configuration does not start downloads or Prefect.

- [x] **T03 — Download and stage the files.** Reviewed and merged in
  [PR #3](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/3).
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
  [PR #4](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/4).
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
  [PR #5](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/5).
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
  [PR #6](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/6).
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

## Done — required orchestration and container delivery

T08 and T09 were reviewed and merged in
[PR #7](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/7) on 05.11.2026.

- [x] **T08 — Prefect orchestration.** Use the shared components in extract,
  transform, and load tasks. Add bounded retries for transient failures, not bad
  data. Done when a container-run flow is visible in the local Prefect UI, failures
  and safe retries are tested, and the plain Python command still works. No copied
  transformation logic. Start runs manually; no automatic schedule initially.
  Demonstrate a manual run, rebuild, and simulated transient-failure recovery.
  Prefect is the planned tool; an alternative needs review.
  Delivered: three sequential tasks call shared stage objects and
  shared lineage/publication code. Download retries are limited to transient
  network/HTTP failures; real Prefect tests cover recovery, exhaustion, and corrupt
  data without retries. No schedule or result persistence is added.
- [x] **T09 — Docker and Compose.** Package the shared command and Prefect flow;
  run local Prefect services with persistent state and raw/report storage. Check
  occupied ports before starting. Done when a fresh Compose run produces the same
  checked report, a rerun does not duplicate results, and run/inspect/stop commands
  are verified. No public hosting or image publication without separate approval.
  Delivered: non-root image, loopback UI on checked port 4205,
  explicit project network, and separate persistent data/history volumes. Real
  container flow and plain-container command both processed the agreed period.
  CI uses an Ubuntu 26.04 Docker job with tiny synthetic inputs; native Python
  jobs retain slim runners. See [run instructions](docs/docker-prefect.md).
  An optional Compose override mounts a trusted custom JSON file read-only for
  both run modes. Base Compose keeps packaged defaults; missing config paths fail
  without creating directories. Tests verify the mount rejects writes and a
  custom reporting month is used by a real container run.
  Compose-only settings use a committed `.env.example` and ignored host `.env`.
  Pipeline values stay in JSON; Python has no dotenv loader. Tests cover automatic
  `.env` loading, shell precedence, and packaged defaults without the override.

T08 and T09 are complete. Continue with **T07 → T10b → T10 → T10a → portfolio review**;
the optional ELT comparison remains separate.

Prior verification, before the latest follow-up: 215 native application tests passed (13 opt-in Docker tests
skipped there), all 13 Docker smoke/contract tests passed when enabled, and three reference
tests, lint/format, package/image builds, and Compose configuration passed. Real
container facts/calendar matched the reviewed host tables; a rebuild reused
inputs and preserved 146,475 rides / $3,395,290.56. Down/up preserved run history
and data. Both tasks are now reviewed and merged.

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
[42025803602](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/actions/runs/42025803602)
passed lint/format, reference tests, package build, unit/integration tests, and
Docker checks. The final PR checks also all passed for commit `dc521ed`: see
[run 42031170377](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/actions/runs/42031170377).
Audit follow-up adds custom-config refresh instructions, updates stale container
and layout descriptions, and removes explicit months from the Compose end-to-end
fixture so it exercises automatic period selection. These changes are covered
by the successful final PR checks.
Enforced single-writer locking, digest/signed provenance, and measured image-size
optimization remain future hardening, not extra requirements for this local demo.

Simplification after manual review: the optional Compose config override now
mounts the existing `src/green_taxi/defaults.json` by default. No local directory,
config copy, or `.env` is required. `TAXI_CONFIG_FILE` remains an optional override;
missing files still fail and all config mounts remain read-only. Updated fallback
and dotenv contract checks passed in the final Docker CI job.

## Done — project documentation

- [x] **T07 — Prepare the portfolio and closeout documentation.** Completed on
  `main` as commit `d62790f`, after the owner reviewed the README.
  T08 and T09 are verified. Update the README with the actual file tree, working
  commands, a small output example, and course reflections. Explain why this is
  batch ETL, how to manually launch/rebuild the reporting period, and how to
  deliberately refresh inputs and trace them through metadata. Done when another
  person can follow the instructions and we have reviewed scope, tests, source
  attribution, and generated files excluded from Git. Final public release is T10.
  Started: README now opens with local/Docker run options, where reports live,
  metadata inspection, safe reruns, and a short learning summary. Removed the
  unnecessary `.env` setup from the normal Docker instructions. Existing ADR,
  data review, and Docker guide remain the detailed references. Documentation
  review completed against T07: shortened the demo result, removed duplicated
  setup/run instructions and stale timing figures, and clarified data rules,
  configuration, storage, and source tracing. Static link/structure and whitespace
  checks completed; no tests or pipeline runs were started for this task. Owner
  review is complete; a new-reader walkthrough remains in the final release review.
  Portfolio pass adds a real completed Prefect-run screenshot and explains
  sequential tasks, bounded retries, and result logs. Setup commands are
  collapsible so the landing page leads with the project and working evidence.
  Independent README review (GPT-6 Astra) confirmed the reported result against
  the data review and recommended a linked area map, less repetition, and folded
  operational details. Owner subsequently requested the King County style:
  rewrote the README with visible run commands, technology reasons, results and
  limitations, a compact annotated tree, and linked project notes. Kept the
  terminal demo and actual Prefect screenshot. No tests or runtime checks were run.

## Final review and public release

- [x] **T10 — Audit and prepare the project for public release.** Completed. After the
  working pipeline, Docker, and Prefect are verified:
  - Audit code, data rules, security, tests, and documentation. Keep OOD simple;
    fix confirmed findings and rerun the checks. Include an independent review
    with a different model and record any remaining limitations.
  - Naming decision completed during T07: the owner renamed the repository to
    `nyc-taxi-revenue-data-pipeline`; local remote, links, and package metadata
    are updated. Verify run commands after the rename during final checks.
  - Check tracked files and Git history for secrets or private information;
    review licensing, attribution, and data redistribution. Remove the supplied
    reference before public release, as planned; keep raw/generated data ignored.
    Do not rewrite history without separate approval if sensitive content is found.
  - Make the repository public once release checks pass and verify anonymous
    access. This does not authorize public service hosting or image publication.
  Done when findings are resolved or clearly documented, the renamed public
  repository works for a new visitor.
  Static audit completed with GPT-6 Astra. Fixed cached-input source attribution
  and added a changed-source reuse/refresh regression. Removed the course reference
  from the working tree and its CI/type-checker dependencies, retaining MIT notice
  and historical evidence. Targeted tracked-history checks found no matching private
  key/GitHub-token/AWS-key patterns; this is not a full secret or dependency audit.
  The owner made the repository public. Anonymous GitHub API and HTTPS Git access
  were verified on 05.11.2026. No public-service hosting was started.
  Final local verification on 05.11.2026: all 274 application, workflow-helper,
  and opt-in Docker checks passed together with two workers. Ruff lint/format,
  offline lockfile consistency, package/image builds, and actionlint validation
  passed. A fresh wheel install outside the checkout downloaded the agreed files
  and reproduced 146,475 accepted rides, 90 days and USD 3,395,290.56 on a rebuild.
  pip-audit found no known vulnerabilities in installed dependencies; it cannot
  audit this local package because it is not a PyPI release. No registry/image-OS
  vulnerability scan is claimed. PR #9 and main's CI subsequently
  passed, including the Ubuntu 26.04 Docker job.
  Owner selected `ubuntu-26.04` for Docker CI and both publishing jobs; Python-only
  jobs retain `ubuntu-slim`. GitHub's official runner list confirms the hosted
  label. The local actionlint image has an older label table, so only that unknown-
  label warning was excluded during local validation; GitHub runtime then passed.
  Full-history Gitleaks scan of an isolated GitHub mirror included all nine PR
  histories: 35 reachable commits, 33 commit diffs scanned, zero leaks found.
  Commit-author email and old course-reference commits remain visible as disclosed;
  no history rewrite was performed. The README records the course context.

- [x] **T10a — Publish the reviewed Docker image to GHCR.** Completed. After T10 is complete
  and its changes are merged, follow the King County release approach. At the
  owner's request the workflow is prepared alongside T10 for one final review;
  execution and public package verification still happen after merge:
  - Add a manual GitHub Actions publishing workflow that builds from `main`
    only after the required checks pass. Use `GITHUB_TOKEN` with the minimum
    permissions needed; do not require a personal token.
  - Use the approved repository name for the GHCR package. Tag releases as
    `year.month.day`, adding a numeric suffix for another release that day
    (for example `2026.11.05.1`), and update `latest`. Do not overwrite an
    existing dated release tag.
  - Package code and dependencies only. Exclude raw taxi data, reports, local
    configuration, credentials, caches, and the supplied reference solution.
  - Publish only after owner approval. Make the package public and verify that
    a new user can pull it without logging in. Check the published image, not
    just the locally built one, using fresh storage and the non-root user.
  - Document the versioned image link, supported platforms, pull/run commands,
    Compose image usage, configuration, and where results are stored. Keep local
    builds available and explain the difference between image defaults and
    read-only host configuration.
  Done when the approved release and `latest` refer to the verified image,
  anonymous pull and a fresh pipeline run succeed, and the README instructions
  work. Publishing distributes the image; it does not deploy the pipeline or
  expose Prefect publicly. This task does not block completion of T10.
  Prepared for review alongside T10: manual main-only workflow, minimum publishing
  permission, exact tested-image transfer, date/commit tags, fail-closed version
  guard, pulled-image checks and `latest` promotion. Independent audit added
  OCI-index/manifest-list handling to prevent replacing an existing multi-platform
  tag. Registry guard regressions use mocked responses, not real publication.
  Release `2026.11.05` passed verification/publication from main at `f78262b` in
  [run 42076273246](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/actions/runs/42076273246).
  Public package access was confirmed; dated/latest tags pulled with empty Docker
  login configuration and matched. A fresh published-image run as UID 10001 produced
  146,475 accepted rides, 90 daily facts and USD 3,395,290.56. The digest, platform
  and commands are recorded in the Docker guide. No API or Prefect service deployed.

- [x] **T10b — Add a small data exploration notebook.** Completed. Show the coach's
  exploration step in `notebooks/`, without moving pipeline logic out of `src/`:
  - Explore existing staged Green Taxi files for the selected period. An optional
    download cell reuses the existing downloader; analysis never modifies raw data.
  - Inspect pickup timestamps and `total_amount`: types, missing values, date
    and amount ranges, negative/zero amounts, and out-of-period pickups.
  - Explain how the observations support the approved cleaning rules. Link to
    the ADR and real-data review instead of duplicating their full explanations.
  - Keep the notebook short and exploratory, with simple tables and wording.
    The production pipeline must continue to run without it.
  Done when it runs top to bottom on a staged file and its conclusions match
  the observed data. Clear outputs, execution counts, and unnecessary metadata
  before committing; link it from the README. No new pipeline features required.
  Staged the three official files and executed the notebook against January in a
  disposable container with the checkout read-only. It found 48,326 rows, six
  pickups outside January–March, 153 negative and 33 zero amounts, matching the
  recorded inspection. Both selected columns had no missing values. Notebook
  outputs and execution counts are cleared for commit. The owner explored the
  notebook in Jupyter and requested dynamic period selection and optional downloads.
  Exploration now follows the selected period end to end: source files, row
  counts, date checks and labels. Verified in-memory notebook runs for March
  alone (51,539 rows; 11 outside March) and January–March (146,486 rows; 11
  outside that period). Final review checked settings, file selection, date checks,
  labels, and missing-file instructions. Saved outputs and environment metadata
  were removed; the owner's March/four-month setting is preserved. No new notebook
  execution or test-suite run was performed during this cleanup.

- [ ] **Portfolio review — Revisit the website entry.** After T10b, T10, and
  T10a are complete, assess whether the finished project adds value to
  grossman.engineering alongside King County. If agreed, add a short English and
  German entry linking to the public repository, focusing on verified results and
  how to reproduce them without repeating its documentation.

## Optional follow-up — not a release requirement

- [ ] **T11 — Explore an ELT version.** Design reviewed and merged in
  [PR #11](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/11);
  [ADR 002](docs/adr/002-elt-comparison.md): DuckDB, plain SQL, transactional raw
  loading and a separate optional execution path. Folder separation is merged;
  the optional dependency/registry foundation is in progress in T11.2, with no
  loading or transformation stages. Keep the checked ETL implementation as
  the comparison baseline; this task does not block T07/T10 or public release.
  GPT-6 Astra and GPT-6.1 Terra reviewed the proposal. The owner accepted their
  clarifications: preserve raw types without turning invalid rows into batch
  failures, verify installed-package/container execution and ETL without extras,
  avoid mandatory per-ride identity, and extract shared helpers only when needed.
  ADR 002 records the checks and why this is a learning comparison, not an upgrade.
  T11.2 and later work remain separate from the behaviour-preserving folder move.

### T11 delivery plan

Each implementation task includes its focused tests, documentation and review.
Keep objects small, use composition and reuse fixtures; do not create a new
framework or duplicate the existing ETL tests. The following are proposed tasks,
not completed work or permission to publish.

- [x] **T11.1 — Separate the folders.** Reviewed and merged in
  [PR #12](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/12).
  Move ETL-specific
  code/tests into `etl/` and extract shared reporting contracts/helpers as proposed.
  Update affected imports, CI paths and docs; preserve existing commands and
  Docker behaviour. **PR boundary:** file moves and required compatibility work
  only, with no DuckDB or ELT implementation. Done when existing ETL checks and
  output comparisons pass.
  Completed: moved calculator/coordinator/flow and their tests; kept the root
  CLI and Prefect delegate; preserved log categories and checkout provenance.
  Python verification passes 262 tests (15 Docker checks skipped), Ruff lint/format
  and package build. Moved calculation/helper bodies match main, and synthetic
  output comparisons match facts, dates, counts and exact totals. The installed
  wheel imports the nested modules/defaults and delegate outside the checkout.
  CI already discovers `tests/` recursively and setuptools discovers the new
  package, so neither configuration needed a path change. Local Docker execution
  was excluded by the owner. The merged PR's quality, unit, integration and Docker
  jobs all passed; Docker verification was performed in GitHub Actions, not locally.

- [x] **T11.2 — Define the ELT foundation.** Reviewed and merged in
  [PR #13](https://github.com/roy-nfisch/nyc-taxi-revenue-data-pipeline/pull/13)
  at `7aadb93`. Depends on merged T11.1. Agree the
  lossless raw representation, source identity/registry schema, warehouse method
  inputs/results and storage paths before parallel work. Add the optional `elt`
  dependency and SQL resource packaging. **PR boundary:** shared contracts,
  dependency/packaging setup and focused checks, not loading or business SQL.
  Done when the parallel tasks can use the same contracts and ETL still runs
  without DuckDB. Avoid placeholder methods or a generic backend abstraction.
  Completed: the [ELT guide](docs/elt.md) define one raw
  table per source, serialized Arrow schema metadata, source-scoped identity,
  storage paths and future warehouse operations. DuckDB 2.0.0 is locked behind
  the `elt` extra; registry DDL is package data. Focused checks cover registry
  identity constraints and imports without the engine. Integration CI installs
  the extra; unit and Docker jobs retain default dependencies. No loader, SQL
  business rules, coordinator, database file or Docker configuration is added.
  Local verification after review fixes: 266 tests passed without the extra
  (26 skipped), and 277
  passed with it (15 Docker checks skipped). Ruff lint/format, lock consistency
  and package build passed. Installed-wheel regression tests now build and install
  the package outside the checkout, execute its registry DDL in memory, and run
  tiny ETL with engine imports and network access blocked.
  Import checks are separated into shared, ETL and ELT tests, reusing one passive-
  import fixture. The default-installation run confirms DuckDB is absent.
  Review fixes define per-source SQL type labels and classified result columns
  before parallel work. PR #13, including its review fixes, is merged; T11.3–T11.5
  start from that shared foundation.

- [x] **T11.3 — Build raw loading.** Reviewed and merged in PR #15.
  Checked native storage, per-source registry identity, lossless verification,
  safe reuse/replacement and rollback are implemented and covered by tests.

- [x] **T11.4 — Build SQL transformations.** Reviewed and merged in PR #16.
  Packaged classification, exact-cent aggregation/counters and the money-text
  helper are covered by named boundary and real-Parquet tests. The supporting
  ETL precision fix is merged in PR #17. Runtime integration remains below.

- [x] **T11.5 — Prepare Docker delivery.** Reviewed and merged in PR #14.
  Separate ELT image, SQL resources and isolated non-root checks are available.
  The default ETL image remains unchanged; no ELT publication has occurred.

- [x] **T11.6 — Connect and verify.** Implementation verified; PR review pending.
  Depends on merged T11.3, T11.4 and T11.5.
  Connect the ELT coordinator, manual entry point, Prefect stages and conversion
  to the shared `RevenueResult`/writer contract. Keep output destinations separate.
  **PR boundary:** runtime integration, parity/failure checks and final run docs.
  Done when local Python, installed-package, Docker and Prefect runs pass; both
  paths match on the same real source bytes and repeat without doubled results.
  Reconcile every daily value and run counter, audit the changes, and document
  the extra storage/maintenance cost before release.
  Delivered: context-managed warehouse calculation, manual ELT command, four
  sequential Prefect stages, separate reports, shared RunReport metadata/writer
  lifecycle, packaged SQL and Docker batch/flow services. Mirrored tests use
  pytest importlib mode so ETL and ELT filenames do not collide.
  Verification: 486 parallel tests passed with Docker enabled; wheel/sdist,
  Ruff lint/format and passive imports passed. Reviewed source copies reconciled
  all daily values, calendar rows and counters on two SQL reruns:
  146,486 read, 146,475 accepted, 90 dates and USD 3,395,290.56.
  Storage/memory/maintenance costs and manual commands are in the run guide.
  No image has been published; owner review and merge remain separate.
  Documentation review consolidated five ELT guides into `docs/elt.md`, retaining
  the flow diagram and runnable examples. Detailed interfaces stay in ADR 002;
  ELT docstrings now describe inputs, results, side effects and failures consistently.

- [ ] **T11.7 — Publish the reviewed ELT image.** Depends on merged T11.6, passing
  release checks and explicit owner approval. Agree an ELT image variant/package
  and dated tag; do not overwrite the published ETL version or silently move its
  `latest` tag to ELT. **PR boundary:** a focused workflow/docs PR if release
  support needs changes; publication itself is a separate manual action after
  those changes are reviewed and merged. Done when the intended public image
  can be pulled without login and a fresh non-root run produces checked outputs.
  Record its digest and run instructions; publishing an image does not deploy a
  running service.

### Parallel work and PR order

T11.1 → T11.2 → T11.3 / T11.4 / T11.5 in parallel → T11.6 → T11.7.

- Give T11.1–T11.6 one focused PR each. These are task boundaries, not GitHub PR
  numbers. Keep the ADR review PR separate from implementation.
- Start each task from updated `main` after its prerequisites merge. T11.3–T11.5
  use the same foundation, not branches stacked on one another.
- T11.3 owns loader/raw schema files; T11.4 owns transformation SQL; T11.5 owns
  container/delivery configuration. Agree shared schema or interface changes
  before editing them. Avoid concurrent changes to fixtures, `pyproject.toml`
  or `uv.lock`; designate one owner for a required shared update.
- Reuse shared fixtures. Each parallel PR must pass its applicable checks and
  existing ETL regressions without depending on another unmerged implementation.
- Merge the parallel PRs independently, updating each with `main` as needed.
  T11.6 then connects and verifies the combined work; do not defer all testing
  until integration or publish merely because the image builds.

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
- The supplied reference was removed for release; preserve its MIT notice and
  historical review evidence. Keep raw/generated data out of Git;
  if we use notebooks, clear outputs, execution counts, and unnecessary metadata.
- Do not implement until the proposed plan is reviewed. Commit, push, and open
  PRs only when requested. Keep Docker and Prefect within scope; further extensions
  need review and must not delay the required deliverable.
