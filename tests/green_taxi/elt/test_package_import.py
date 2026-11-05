"""The optional ELT package can be imported without its engine or storage."""

import pytest

pytestmark = pytest.mark.integration


def test_elt_package_import_is_passive(check_passive_imports):
    """Importing the foundation must not load DuckDB or create a database."""
    check_passive_imports("green_taxi.elt")
