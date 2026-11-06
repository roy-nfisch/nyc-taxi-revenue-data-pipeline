"""Offline release-safety regressions; never authenticate or publish images."""

import importlib.util
import io
import json
from pathlib import Path
from unittest.mock import MagicMock
from urllib.error import HTTPError, URLError

import pytest

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.unit


@pytest.fixture
def registry(monkeypatch):
    """Load the release helper and replace all network requests with a mock."""
    spec = importlib.util.spec_from_file_location(
        "check_image_version", ROOT / ".github/scripts/check_image_version.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    network = MagicMock()
    token = MagicMock()
    token.__enter__.return_value = io.StringIO('{"token": "test-token"}')
    network.side_effect = [token, MagicMock()]
    monkeypatch.setattr(module, "urlopen", network)
    return module, network, token


def registry_error(status, codes):
    """Build a structured registry HTTP error without using a real service."""
    body = json.dumps({"errors": [{"code": code} for code in codes]}).encode()
    return HTTPError("https://ghcr.io/test", status, "test", {}, io.BytesIO(body))


@pytest.mark.parametrize("code", ["MANIFEST_UNKNOWN", "NAME_UNKNOWN"])
def test_only_explicit_missing_version_is_allowed(registry, code):
    """Allow a new tag only when GHCR explicitly reports it or its image missing.

    Fake both registry responses; check the requested tag, bearer token, and
    network timeouts without contacting GHCR or using real credentials.
    """
    module, network, token = registry
    network.side_effect = [token, registry_error(404, [code])]
    module.check_version("ghcr.io/owner/app", "2026.10.29", "actor", "secret")
    assert network.call_count == 2
    for call in network.call_args_list:
        assert call.kwargs["timeout"] == 10
    request = network.call_args.args[0]
    assert request.full_url.endswith("/owner/app/manifests/2026.10.29")
    assert request.get_header("Authorization") == "Bearer test-token"


def test_existing_version_is_rejected(registry):
    """A successful manifest lookup means the tag exists, so reject its reuse."""
    module, _, _ = registry
    with pytest.raises(RuntimeError, match="already exists"):
        module.check_version("ghcr.io/owner/app", "2026.10.29", "actor", "secret")


def test_tag_lookup_accepts_single_and_multiplatform_images(registry):
    """Existing image indexes must not be mistaken for an absent version tag."""
    module, network, _ = registry
    with pytest.raises(RuntimeError, match="already exists"):
        module.check_version("ghcr.io/owner/app", "2026.11.05", "actor", "secret")
    accept = network.call_args.args[0].get_header("Accept")
    assert "application/vnd.oci.image.index.v1+json" in accept
    assert "application/vnd.docker.distribution.manifest.list.v2+json" in accept


@pytest.mark.parametrize(
    "error",
    [
        registry_error(401, ["UNAUTHORIZED"]),
        registry_error(403, ["DENIED"]),
        registry_error(429, ["TOOMANYREQUESTS"]),
        registry_error(500, ["UNKNOWN"]),
        registry_error(404, ["DENIED"]),
        registry_error(404, []),
        URLError("offline"),
        TimeoutError("timed out"),
        HTTPError("https://ghcr.io/test", 404, "test", {}, io.BytesIO(b"bad")),
    ],
)
@pytest.mark.parametrize("phase", ["token", "manifest"])
def test_registry_failures_never_allow_publishing(registry, error, phase):
    """Errors must stop the check, not be mistaken for an unused version.

    Try each failure while obtaining a token and while looking up the tag.
    Cover access errors, rate limits, outages, timeouts, and invalid responses.
    """
    module, network, token = registry
    network.side_effect = [error] if phase == "token" else [token, error]
    with pytest.raises((HTTPError, URLError, TimeoutError, ValueError, RuntimeError)):
        module.check_version("ghcr.io/owner/app", "2026.10.29", "actor", "secret")


def test_workflow_release_guards_and_order():
    """Manual main-only release checks precede pushes; latest follows pulled-image checks."""
    workflow = (ROOT / ".github/workflows/publish-image.yml").read_text()
    assert "workflow_dispatch:" in workflow
    assert 'if [[ "$GITHUB_REF" != refs/heads/main ]]' in workflow
    assert "cancel-in-progress: false" in workflow
    check = workflow.index("run: python3 .github/scripts/check_image_version.py")
    assert check < workflow.index('docker push "$IMAGE:$VERSION"')
    assert workflow.index(
        "Check published pipeline with tiny staged data"
    ) < workflow.index("Update latest after verification")
    assert 'os.environ["GITHUB_REPOSITORY"].lower()' in workflow
    assert "TESTED_IMAGE_ID" in workflow
    assert "packages: write" in workflow
    assert workflow.count("packages: write") == 1
