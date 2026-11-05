"""One opt-in end-to-end check of the real Compose server, flow, and volumes."""

import base64
import json
import os
import re
import socket
import subprocess
from pathlib import Path
from urllib.parse import urljoin
from uuid import uuid4

import pandas as pd
import pytest
import requests

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("GREEN_TAXI_DOCKER_TESTS") != "1",
        reason="Requires the built image and an explicit opt-in to Docker checks.",
    ),
]


class ComposeProject:
    """Operate only a UUID-named test project, never the normal green-taxi stack."""

    def __init__(self, config: Path, port: int):
        self.command = [
            "docker",
            "compose",
            "-p",
            f"taxi-e2e-{uuid4().hex}",
            "-f",
            "compose.yaml",
            "-f",
            "compose.config.yaml",
        ]
        self.env = os.environ | {
            "TAXI_CONFIG_FILE": str(config),
            "PREFECT_PORT": str(port),
        }
        self.url = f"http://127.0.0.1:{port}"

    def run(self, *args):
        """Run a bounded Compose command and retain diagnostics on failure."""
        return subprocess.run(
            self.command + list(args),
            env=self.env,
            check=True,
            capture_output=True,
            text=True,
            timeout=180,
        )

    def start(self):
        self.run("up", "-d", "--wait", "--wait-timeout", "120", "prefect")

    def python(self, code):
        return self.run(
            "run", "--rm", "--no-deps", "pipeline", "python", "-c", code
        ).stdout

    def report(self):
        """Read actual report values as the image's default non-root user."""
        code = """
import json, os
from pathlib import Path
import pandas as pd
assert os.getuid() != 0
p = Path('/data/processed')
facts = pd.read_parquet(p/'fact_daily_revenue.parquet')
facts['daily_revenue'] = facts.daily_revenue.map(str)
print(json.dumps({'metadata': json.loads((p/'metadata.json').read_text()),
                  'facts': facts.to_dict('records')}, default=str))
"""
        return json.loads(self.python(code))

    def check_ui(self):
        """The API alone is insufficient: require HTML and a loadable JS bundle."""
        response = requests.get(
            self.url + "/", headers={"Accept": "text/html"}, timeout=10
        )
        response.raise_for_status()
        assert "text/html" in response.headers["Content-Type"]
        assets = re.findall(
            r'(?:src|href)=["\']([^"\']+\.js(?:\?[^"\']*)?)["\']', response.text
        )
        assert assets, "UI page must include a JavaScript bundle."
        bundle = requests.get(urljoin(response.url, assets[0]), timeout=10)
        bundle.raise_for_status()
        assert "javascript" in bundle.headers["Content-Type"]

    def completed_run(self):
        """Return the real flow ID after checking all three task states."""
        runs = requests.post(
            self.url + "/api/flow_runs/filter", json={"limit": 10}, timeout=10
        )
        runs.raise_for_status()
        run = runs.json()[0]
        assert run["state"]["type"] == "COMPLETED"
        tasks = requests.post(
            self.url + "/api/task_runs/filter",
            json={"flow_runs": {"id": {"any_": [run["id"]]}}},
            timeout=10,
        )
        tasks.raise_for_status()
        assert len(tasks.json()) == 3
        assert all(task["state"]["type"] == "COMPLETED" for task in tasks.json())
        return run["id"]


@pytest.fixture
def compose_project(tmp_path):
    """Use private named volumes and a free loopback port; always remove only ours."""
    config = tmp_path / "run.json"
    config.write_text(
        json.dumps(
            {
                "reporting_period": {
                    "start_month": "2032-02",
                    "duration_months": 1,
                },
            }
        )
    )
    config.chmod(0o644)  # Default UID 10001 must read this synthetic, nonsecret file.
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    project = ComposeProject(config, port)
    try:
        project.start()
        yield project
    finally:
        # UUID project owns both volumes; never remove the user's normal data.
        project.run("down", "--volumes", "--remove-orphans")


def test_real_compose_flow_ui_and_persistence(compose_project, tmp_path):
    """Exercise packaged UI/API, actual flow, custom JSON, named-volume writes/rebuild."""
    project = compose_project
    project.check_ui()
    sample = tmp_path / "tiny.parquet"
    pd.DataFrame(
        {"lpep_pickup_datetime": [pd.Timestamp("2032-02-01")], "total_amount": [1.25]}
    ).to_parquet(sample, index=False)
    encoded = base64.b64encode(sample.read_bytes()).decode()
    project.python(
        "import base64,os; from pathlib import Path; assert os.getuid()!=0; "
        "p=Path('/data/raw'); p.mkdir(parents=True,exist_ok=True); "
        f"(p/'green_tripdata_2032-02.parquet').write_bytes(base64.b64decode('{encoded}'))"
    )
    project.run("run", "--rm", "flow")
    first = project.report()
    assert first["facts"] == [
        {"service_date": "2032-02-01", "trip_count": 1, "daily_revenue": "1.25"}
    ]
    assert first["metadata"]["outputs"]["dim_date.parquet"]["rows"] == 29
    assert first["metadata"]["total_amount"] == "1.25"
    old_id = project.completed_run()
    project.run("down")  # Retain the isolated test's named volumes.
    project.start()
    old = requests.get(project.url + f"/api/flow_runs/{old_id}", timeout=10)
    old.raise_for_status()
    assert old.json()["state"]["type"] == "COMPLETED"
    project.run("run", "--rm", "flow")
    second = project.report()
    assert first["facts"] == second["facts"]
    assert first["metadata"]["run_id"] != second["metadata"]["run_id"]
    assert first["metadata"]["counts"] == second["metadata"]["counts"]
    assert not second["metadata"]["inputs"][0]["downloaded"]
