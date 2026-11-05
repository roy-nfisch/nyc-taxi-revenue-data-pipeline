"""The optional ELT package can be imported without its engine or storage."""

import pytest

pytestmark = pytest.mark.integration


def test_elt_package_import_is_passive(check_passive_imports):
    """Importing runtime classes must not load DuckDB or create a database."""
    check_passive_imports(
        "green_taxi.elt",
        "green_taxi.elt.warehouse",
        "green_taxi.elt.queries",
        "green_taxi.elt.pipeline",
        "green_taxi.elt.__main__",
    )
