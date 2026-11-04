"""Require one test category while allowing additional data-coverage labels."""

import pytest


def pytest_collection_modifyitems(items):
    """Fail collection if a project test would be missed or repeated by CI jobs."""
    for item in items:
        categories = {mark.name for mark in item.iter_markers()} & {
            "unit",
            "integration",
        }
        if len(categories) != 1:
            raise pytest.UsageError(
                f"Select exactly one unit/integration marker for {item.nodeid}"
            )
