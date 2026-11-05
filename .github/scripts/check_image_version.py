"""Reject an existing GHCR release tag; fail closed on registry errors."""

import base64
import json
import os
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def check_version(image: str, version: str, actor: str, credential: str) -> None:
    """Allow only an explicitly missing manifest; otherwise raise before pushing."""
    repository = image.removeprefix("ghcr.io/")
    query = urlencode({"service": "ghcr.io", "scope": f"repository:{repository}:pull"})
    basic = base64.b64encode(f"{actor}:{credential}".encode()).decode()
    request = Request(
        f"https://ghcr.io/token?{query}",
        headers={"Authorization": f"Basic {basic}"},
    )
    with urlopen(request, timeout=10) as response:
        token = json.load(response)["token"]
    request = Request(
        f"https://ghcr.io/v2/{repository}/manifests/{version}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.oci.image.manifest.v1+json, "
            "application/vnd.docker.distribution.manifest.v2+json, "
            "application/vnd.oci.image.index.v1+json, "
            "application/vnd.docker.distribution.manifest.list.v2+json",
        },
    )
    try:
        with urlopen(request, timeout=10):
            pass
    except HTTPError as error:
        with error:
            if error.code != 404:
                raise
            codes = {item["code"] for item in json.load(error)["errors"]}
        if codes and codes <= {"MANIFEST_UNKNOWN", "NAME_UNKNOWN"}:
            return
        raise RuntimeError("Registry did not confirm that the version is unused.")
    raise RuntimeError(f"Version {version} already exists. Choose a new version.")


if __name__ == "__main__":
    check_version(
        os.environ["IMAGE"],
        os.environ["VERSION"],
        os.environ["GITHUB_ACTOR"],
        os.environ["GHCR_TOKEN"],
    )
