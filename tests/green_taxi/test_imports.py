"""Shared package components import without starting application work."""

import pytest

pytestmark = pytest.mark.integration


def test_shared_imports_are_passive(check_passive_imports):
    """Shared components must not perform I/O or depend on the optional engine."""
    check_passive_imports(
        "green_taxi.settings",
        "green_taxi.logging",
        "green_taxi.download",
        "green_taxi.reporting",
        "green_taxi.report",
    )
