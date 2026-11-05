# Publish the ELT image

This is a manual release, not a deployed service. Merge and verify the runtime
and publishing changes first, then get approval for the package and an unused tag.
No ELT image has been published as part of this change.

In GitHub Actions, choose **Publish pipeline image**, branch **main**, variant
**elt** and a dated version such as `2026.11.06` or `2026.11.06.1`.

| Choice | GHCR package | Default command |
|---|---|---|
| `etl` (default) | `nyc-taxi-revenue-data-pipeline` | Python ETL |
| `elt` | `nyc-taxi-revenue-data-pipeline-elt` | DuckDB ELT |

Both variants use the existing checks, tested-image transfer, digest verification
and tag-reuse guard. Only the publishing job can write packages. Each package has
its own dated tag, commit tag and `latest`; choosing ELT does not change ETL tags.

## First public release

After the first version is published, check the new package's GitHub visibility
and explicitly make it public if needed. Do not assume repository visibility
automatically makes a new GHCR package public.

Check a published tag without registry credentials, replacing `VERSION`:

```bash
docker --config "$(mktemp -d)" pull ghcr.io/roy-nfisch/nyc-taxi-revenue-data-pipeline-elt:VERSION
docker run --rm --platform linux/amd64 \
  --mount type=volume,source=taxi-elt-release-data,target=/data \
  ghcr.io/roy-nfisch/nyc-taxi-revenue-data-pipeline-elt:VERSION
```

The run downloads the configured taxi files if they are not staged. Reports stay
in `/data/elt/processed/` inside that volume. For an isolated no-network smoke
check, run `tests/docker/test_elt.py` after tagging the pulled digest as
`green-taxi-elt:local` and enabling `GREEN_TAXI_DOCKER_TESTS=1`.

Before calling the release complete, record its immutable digest, anonymous-pull
result and fresh-run metadata. The workflow verifies the authenticated pull;
the anonymous check and first-package visibility are explicit owner steps.
