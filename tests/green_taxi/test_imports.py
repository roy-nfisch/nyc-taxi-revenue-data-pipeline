"""A fresh Python process can import the package without starting services."""

import subprocess
import sys


def test_imports_do_not_access_network_or_create_storage(tmp_path):
    """Block network and directory writes while importing in a clean directory."""
    code = """
from unittest.mock import patch
import requests
with patch("requests.sessions.Session.request", side_effect=AssertionError("HTTP on import")), \
     patch("socket.create_connection", side_effect=AssertionError("network on import")), \
     patch("pathlib.Path.mkdir", side_effect=AssertionError("mkdir on import")):
    import green_taxi.settings
    import green_taxi.logging
    import green_taxi.__main__
    import green_taxi.download
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    assert result.stdout == ""
    assert list(tmp_path.iterdir()) == []
