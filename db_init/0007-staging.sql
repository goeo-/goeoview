CREATE TABLE IF NOT EXISTS staging
(
    did String,
    collection LowCardinality(String),
    key String,
    rev UInt64,
    value String
)
ENGINE = Memory;
