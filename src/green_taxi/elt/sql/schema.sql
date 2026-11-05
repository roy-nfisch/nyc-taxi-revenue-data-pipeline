-- Create storage namespaces and a registry, not the raw ride tables themselves.
-- The loader creates one table per source file in raw. loaded_sources records
-- which version it loaded, so reruns can reuse unchanged data or replace it safely.
-- IF NOT EXISTS allows initialization to run again without deleting stored data.
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS loaded_sources (
    -- File identity and its unique, restricted raw-table name.
    source_id VARCHAR PRIMARY KEY CHECK (length(source_id) > 0),
    raw_table VARCHAR NOT NULL UNIQUE
        CHECK (regexp_full_match(raw_table, 'source_[0-9]{4}_(0[1-9]|1[0-2])')),
    -- Checksum and byte count identify the file contents; row_count records load size.
    sha256 VARCHAR NOT NULL CHECK (regexp_full_match(sha256, '[0-9a-f]{64}')),
    size_bytes UBIGINT NOT NULL CHECK (size_bytes > 0),
    row_count UBIGINT NOT NULL,
    -- Serialized Arrow schema preserves original types and metadata for verification.
    arrow_schema BLOB NOT NULL CHECK (octet_length(arrow_schema) > 0)
);
