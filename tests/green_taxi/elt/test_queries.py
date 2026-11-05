"""Check original type labels without opening the optional database engine."""

import pyarrow as pa
import pytest

from green_taxi.elt.queries import column_kind

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "kind,expected",
    [
        (pa.bool_(), "boolean"),
        (pa.null(), "null"),
        (pa.int64(), "numeric"),
        (pa.decimal128(12, 2), "numeric"),
        (pa.float32(), "numeric"),
        (pa.string(), "text"),
        (pa.date32(), "date"),
        (pa.timestamp("ns"), "timestamp_naive"),
        (pa.timestamp("ns", "Europe/Berlin"), "timestamp_aware"),
    ],
)
def test_schema_kind_is_not_guessed_from_values(kind, expected):
    """Booleans and aware timestamps retain their invalid-row distinctions."""
    assert column_kind(kind) == expected


def test_unsupported_type_is_not_silently_coerced():
    """Binary or nested input needs an explicit mapping, never a text guess."""
    with pytest.raises(ValueError, match="Unsupported"):
        column_kind(pa.binary())
