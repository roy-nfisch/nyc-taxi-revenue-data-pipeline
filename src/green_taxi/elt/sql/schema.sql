-- Metadata only: source tables and loading are implemented in the next task.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS loaded_sources (
    source_id VARCHAR PRIMARY KEY CHECK (length(source_id) > 0),
    raw_table VARCHAR NOT NULL UNIQUE
        CHECK (regexp_full_match(raw_table, 'source_[0-9]{4}_(0[1-9]|1[0-2])')),
    sha256 VARCHAR NOT NULL CHECK (regexp_full_match(sha256, '[0-9a-f]{64}')),
    size_bytes UBIGINT NOT NULL CHECK (size_bytes > 0),
    row_count UBIGINT NOT NULL,
    arrow_schema BLOB NOT NULL CHECK (octet_length(arrow_schema) > 0)
);
