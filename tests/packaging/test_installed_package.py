"""Verify the built wheel outside the checkout, not just editable source imports."""

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def installed_package(tmp_path_factory):
    """Build and install only our wheel; reuse the runner's locked dependencies."""
    directory = tmp_path_factory.mktemp("installed-package")
    root = Path(__file__).resolve().parents[2]
    subprocess.run(
        ["uv", "build", "--wheel", "--out-dir", str(directory / "dist")],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    (wheel,) = (directory / "dist").glob("*.whl")
    installed = directory / "site"
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            sys.executable,
            "--no-deps",
            "--no-compile",
            "--target",
            str(installed),
            str(wheel),
        ],
        cwd=directory,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return installed


@pytest.fixture
def run_installed(installed_package, tmp_path):
    """Run checks in a fresh interpreter using the installed wheel before source."""
    prefix = (
        f"import sys; sys.path.insert(0, {str(installed_package)!r})\n"
        "import green_taxi\n"
        f"assert green_taxi.__file__.startswith({str(installed_package)!r})\n"
    )

    def run(code):
        return subprocess.run(
            [sys.executable, "-I", "-c", prefix + code],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )

    return run


def test_wheel_includes_executable_registry_sql(run_installed):
    """Missing package-data configuration must fail even if source tests pass."""
    pytest.importorskip("duckdb")
    run_installed("""
from importlib.resources import files
import duckdb
sql = files('green_taxi.elt').joinpath('sql/schema.sql').read_text()
with duckdb.connect(':memory:') as database:
    database.execute(sql)
    assert database.execute('SELECT count(*) FROM loaded_sources').fetchone() == (0,)
    helper = files('green_taxi.elt').joinpath('sql/normalize_money.sql').read_text()
    database.execute(helper)
    assert database.execute("SELECT normalize_money_text(' 1_2.30 ')").fetchone() == ('12.30',)
""")


def test_installed_etl_runs_with_optional_engine_blocked(run_installed):
    """A complete tiny ETL run must work without importing the optional engine."""
    run_installed("""
import importlib.abc
import json
from pathlib import Path
from unittest.mock import patch

class BlockEngine(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'duckdb', '_duckdb'}:
            raise ImportError('Optional engine is unavailable')

sys.meta_path.insert(0, BlockEngine())
import pandas as pd
from click.testing import CliRunner
from green_taxi.__main__ import main
from green_taxi.settings import PipelineSettings
settings = PipelineSettings(
    data_dir=Path.cwd() / 'data',
    reporting_period={'start_month': '2031-01', 'duration_months': 1},
)
config = Path.cwd() / 'run.json'
config.write_text(settings.model_dump_json())
settings.raw_dir.mkdir(parents=True)
for month in settings.months:
    pd.DataFrame({
        'lpep_pickup_datetime': [settings.reporting_period.start],
        'total_amount': [1.25],
    }).to_parquet(settings.raw_dir / f'green_tripdata_{month}.parquet', index=False)
with patch('requests.sessions.Session.request', side_effect=AssertionError('HTTP')):
    result = CliRunner().invoke(main, ['--run', '--config', str(config)])
assert result.exit_code == 0, result.exception
metadata = json.loads((settings.processed_dir / 'metadata.json').read_text())
assert metadata['counts']['rows_used'] == 1
assert metadata['total_amount'] == '1.25'
assert not any(item['downloaded'] for item in metadata['inputs'])
assert 'duckdb' not in sys.modules
""")
