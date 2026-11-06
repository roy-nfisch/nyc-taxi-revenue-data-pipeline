"""Run manual-release input checks offline; never authenticate or publish."""

import os
import subprocess
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.unit


@pytest.fixture
def release_script():
    """Extract the trusted validation block from the actual release workflow."""
    workflow = (ROOT / ".github/workflows/publish-image.yml").read_text()
    block = workflow.split("      - name: Validate manual release request", 1)[1]
    return textwrap.dedent(
        block.split("        run: |\n", 1)[1].split("      - name:", 1)[0]
    )


@pytest.mark.parametrize(
    "variant,suffix,target,extra",
    [
        pytest.param("etl", "", "etl", "false", id="unchanged-etl-package"),
        pytest.param("elt", "-elt", "elt", "true", id="separate-elt-package"),
    ],
)
def test_variant_selects_package_target_and_dependency(
    tmp_path, release_script, variant, suffix, target, extra
):
    """Execute the real workflow validation, not a second Python implementation."""
    output = tmp_path / "outputs"
    result = subprocess.run(
        ["bash", "-e", "-c", release_script],
        capture_output=True,
        text=True,
        env=os.environ
        | {
            "VARIANT": variant,
            "VERSION": "2026.11.06.1",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_REPOSITORY": "roy-nfisch/nyc-taxi-revenue-data-pipeline",
            "GITHUB_OUTPUT": str(output),
        },
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert (
        values["image"] == "ghcr.io/roy-nfisch/nyc-taxi-revenue-data-pipeline" + suffix
    )
    assert values["target"] == target
    assert values["install_elt"] == extra


@pytest.mark.parametrize(
    "change",
    [
        {"VARIANT": "unknown"},
        {"VARIANT": "elt; echo unsafe"},
        {"GITHUB_REF": "refs/heads/feature/test"},
        {"VERSION": "2026.02.30"},
        {"VERSION": "latest"},
        {"VERSION": "2026.11.06.0"},
    ],
)
def test_invalid_release_inputs_fail_before_building(tmp_path, release_script, change):
    """Reject invalid variants, branch, date and suffix without shell injection."""
    output = tmp_path / "outputs"
    result = subprocess.run(
        ["bash", "-e", "-c", release_script],
        capture_output=True,
        text=True,
        env=os.environ
        | {
            "VARIANT": "elt",
            "VERSION": "2026.11.06",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_REPOSITORY": "roy-nfisch/nyc-taxi-revenue-data-pipeline",
            "GITHUB_OUTPUT": str(output),
        }
        | change,
        timeout=10,
    )
    assert result.returncode != 0
    assert not output.exists()


def test_release_keeps_verification_before_latest():
    """Both variants use the same tested-image handoff and guarded publication."""
    workflow = (ROOT / ".github/workflows/publish-image.yml").read_text()
    assert "default: etl" in workflow
    assert (
        'docker build --target "$TARGET" --build-arg "INSTALL_ELT=$INSTALL_ELT"'
        in workflow
    )
    assert 'docker tag "$IMAGE:test" "$LOCAL_TAG"' in workflow
    assert "uv run --locked --extra elt pytest tests/" in workflow
    assert workflow.index(
        "Check published pipeline with tiny staged data"
    ) < workflow.index("Update latest after verification")
    assert workflow.count("packages: write") == 1
