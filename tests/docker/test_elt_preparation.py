"""Opt-in delivery checks; no public downloads, server, or user data mutations."""

import json
import os
import subprocess

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("GREEN_TAXI_DOCKER_TESTS") != "1",
        reason="Build green-taxi-elt:local and opt into Docker checks.",
    ),
]


def _services(*files):
    """Render explicit files without the developer's deployment image settings."""
    command = ["docker", "compose", "--env-file", os.devnull]
    for path in files:
        command.extend(["-f", path])
    result = subprocess.run(
        command + ["--profile", "manual", "config", "--format", "json"],
        env=os.environ
        | {"TAXI_IMAGE": "green-taxi:local", "TAXI_ELT_IMAGE": "green-taxi-elt:local"},
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return json.loads(result.stdout)["services"]


def test_elt_override_keeps_default_services_and_storage():
    """Preparation is opt-in and separate from ETL images, ports and startup."""
    baseline = _services("compose.yaml")
    services = _services("compose.yaml", "compose.elt.yaml")
    for name, settings in baseline.items():
        assert services[name] == settings
    optional = services["elt-tools"]
    assert services["pipeline"]["image"] == "green-taxi:local"
    assert optional["image"] == "green-taxi-elt:local"
    assert optional["build"]["args"]["INSTALL_ELT"] == "true"
    assert optional["profiles"] == ["manual"]
    assert not optional.get("ports") and not optional.get("depends_on")
    assert optional["volumes"] == services["pipeline"]["volumes"]
    assert optional["networks"] == services["pipeline"]["networks"]


def test_elt_image_contains_engine_and_executable_packaged_sql():
    """The opt-in image runs non-root with installed resources, without network."""
    code = """
import os
from importlib.resources import files
import duckdb
assert os.getuid() == 10001
resource = files('green_taxi.elt').joinpath('sql/schema.sql')
assert '/app/.venv/' in str(resource)
with duckdb.connect(':memory:') as connection:
    connection.execute(resource.read_text(encoding='utf-8'))
    assert connection.execute('SELECT count(*) FROM loaded_sources').fetchone() == (0,)
"""
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "green-taxi-elt:local",
            "python",
            "-c",
            code,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_default_etl_image_does_not_install_duckdb():
    """Adding optional delivery must not expand the default ETL dependencies."""
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "green-taxi:local",
            "python",
            "-c",
            "import importlib.util; assert importlib.util.find_spec('duckdb') is None",
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
