"""Check rendered Compose contracts without starting or deleting any services."""

import json
import os
import shutil
import subprocess
from pathlib import Path

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


def test_prefect_is_loopback_only_and_waits_for_health(services):
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
    assert (
        services["prefect"]["environment"]["PREFECT_UI_STATIC_DIRECTORY"]
        == "/home/app/ui"
    )
    assert "http://127.0.0.1:4200/" in services["prefect"]["healthcheck"]["test"][-1]


@pytest.mark.parametrize("port", ["4205", "4217"])
def test_elt_flow_uses_host_url_for_ui_and_service_url_for_api(port):
    """ELT logs point to the host UI while API calls stay on the Docker network."""
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.elt.yaml",
            "--profile",
            "manual",
            "config",
            "--format",
            "json",
        ],
        env=os.environ | {"PREFECT_PORT": port},
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    environment = json.loads(result.stdout)["services"]["elt-flow"]["environment"]
    assert environment["PREFECT_API_URL"] == "http://prefect:4200/api"
    assert environment["PREFECT_UI_URL"] == f"http://127.0.0.1:{port}"


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


@pytest.mark.parametrize("service", ["pipeline", "flow"])
def test_custom_config_is_readonly_without_replacing_data_volume(tmp_path, service):
    """The override only adds a file mount; both entry points keep persistent data."""
    config = tmp_path / "run with spaces.json"
    config.write_text('{"months": ["2032-02"]}')
    result = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            "compose.yaml",
            "-f",
            "compose.config.yaml",
            "--profile",
            "manual",
            "config",
            "--format",
            "json",
        ],
        env=os.environ | {"TAXI_CONFIG_FILE": str(config)},
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    rendered = json.loads(result.stdout)["services"]
    mounts = {mount["target"]: mount for mount in rendered[service]["volumes"]}
    assert mounts["/data"]["source"] == "taxi-data"
    mounted_config = mounts["/run-config/run.json"]
    assert mounted_config["source"] == str(config)
    assert mounted_config["read_only"] is True
    assert mounted_config.get("bind", {}).get("create_host_path", False) is False
    assert "--config" in rendered[service]["command"]
    assert "/run-config/run.json" in rendered[service]["command"]
    assert len(rendered["prefect"]["volumes"]) == 1


@pytest.mark.parametrize("value", [None, ""])
def test_config_override_defaults_to_existing_json(value):
    """No local config or .env is required; unset/empty selects tracked defaults."""
    env = os.environ.copy()
    env.pop("TAXI_CONFIG_FILE", None)
    if value is not None:
        env["TAXI_CONFIG_FILE"] = value
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            os.devnull,
            "-f",
            "compose.yaml",
            "-f",
            "compose.config.yaml",
            "--profile",
            "manual",
            "config",
            "--format",
            "json",
        ],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    services = json.loads(result.stdout)["services"]
    expected = Path(__file__).resolve().parents[2] / "src/green_taxi/defaults.json"
    for name in ("pipeline", "flow"):
        mounts = {mount["target"]: mount for mount in services[name]["volumes"]}
        assert mounts["/run-config/run.json"]["source"] == str(expected)
        assert mounts["/run-config/run.json"]["read_only"] is True


@pytest.fixture
def dotenv_project(tmp_path):
    """Copy Compose/template files into isolation; never overwrite the user's .env."""
    root = Path(__file__).resolve().parents[2]
    for name in ("compose.yaml", "compose.config.yaml"):
        shutil.copyfile(root / name, tmp_path / name)
    template = (root / ".env.example").read_text()
    (tmp_path / ".env").write_text(
        template.replace("PREFECT_PORT=4205", "PREFECT_PORT=4217")
    )
    (tmp_path / "src/green_taxi").mkdir(parents=True)
    (tmp_path / "src/green_taxi/defaults.json").write_text("{}")
    excluded = {
        "PREFECT_PORT",
        "TAXI_CONFIG_FILE",
        "COMPOSE_ENV_FILES",
        "COMPOSE_DISABLE_ENV_FILE",
        "COMPOSE_FILE",
    }
    env = {key: value for key, value in os.environ.items() if key not in excluded}
    return tmp_path, env


@pytest.mark.parametrize("custom", [False, True])
def test_dotenv_is_loaded_without_exporting_compose_settings(dotenv_project, custom):
    """Compose reads .env on the host; only the optional override mounts the JSON."""
    folder, env = dotenv_project
    command = ["docker", "compose", "-f", "compose.yaml"]
    if custom:
        command += ["-f", "compose.config.yaml"]
    result = subprocess.run(
        command + ["--profile", "manual", "config", "--format", "json"],
        cwd=folder,
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    services = json.loads(result.stdout)["services"]
    assert services["prefect"]["ports"][0]["published"] == "4217"
    assert services["flow"]["environment"]["PREFECT_UI_URL"] == "http://127.0.0.1:4217"
    for name in ("pipeline", "flow"):
        assert "TAXI_CONFIG_FILE" not in (services[name].get("environment") or {})
        mounts = {mount["target"]: mount for mount in services[name]["volumes"]}
        if custom:
            assert mounts["/run-config/run.json"]["source"] == str(
                folder / "src/green_taxi/defaults.json"
            )
            assert mounts["/run-config/run.json"]["read_only"] is True
        else:
            assert "/run-config/run.json" not in mounts
            assert "--config" not in (services[name].get("command") or [])


def test_shell_setting_overrides_dotenv(dotenv_project):
    """An explicit shell port wins, matching the precedence explained in the guide."""
    folder, env = dotenv_project
    result = subprocess.run(
        ["docker", "compose", "config", "--format", "json"],
        cwd=folder,
        env=env | {"PREFECT_PORT": "4218"},
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert (
        json.loads(result.stdout)["services"]["prefect"]["ports"][0]["published"]
        == "4218"
    )
