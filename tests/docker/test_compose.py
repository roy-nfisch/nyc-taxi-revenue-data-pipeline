"""Check rendered Compose contracts without starting or deleting any services."""

import json
import os
import subprocess

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("GREEN_TAXI_DOCKER_TESTS") != "1",
        reason="Compose contract checks are opt-in with the Docker tests.",
    ),
]


@pytest.fixture(scope="module")
def services():
    """Render the checked default port independent of the caller's environment."""
    result = subprocess.run(
        ["docker", "compose", "--profile", "manual", "config", "--format", "json"],
        env=os.environ | {"PREFECT_PORT": "4205"},
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return json.loads(result.stdout)["services"]


def test_api_is_loopback_only_and_waits_for_health(services):
    """Only the local server exposes a port; the flow waits for a healthy API."""
    assert services["prefect"]["ports"] == [
        {
            "mode": "ingress",
            "target": 4200,
            "published": "4205",
            "protocol": "tcp",
            "host_ip": "127.0.0.1",
        }
    ]
    assert not services["pipeline"].get("ports")
    assert not services["flow"].get("ports")
    assert services["flow"]["depends_on"]["prefect"]["condition"] == "service_healthy"
    assert (
        services["flow"]["environment"]["PREFECT_API_URL"] == "http://prefect:4200/api"
    )
    assert (
        services["flow"]["environment"]["PREFECT_SERVER_ALLOW_EPHEMERAL_MODE"]
        == "false"
    )


def test_history_and_data_use_separate_persistent_volumes(services):
    """State survives service removal; app data is shared by the two run modes."""

    def volume(service):
        (mount,) = services[service]["volumes"]
        assert mount["type"] == "volume"
        return mount["source"], mount["target"]

    assert volume("pipeline") == volume("flow") == ("taxi-data", "/data")
    assert volume("prefect") == ("prefect-state", "/home/app/.prefect")


def test_services_do_not_request_privilege_or_host_access(services):
    """Guard the local-use security boundary and prevent accidental batch startup."""
    for service in services.values():
        assert not service.get("privileged", False)
        assert service.get("network_mode") != "host"
        assert service.get("user") not in ("root", "0", "0:0")
        assert set(service["networks"]) == {"taxi-network"}
    assert (
        services["pipeline"]["profiles"] == services["flow"]["profiles"] == ["manual"]
    )
