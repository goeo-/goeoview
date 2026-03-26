CREATE TABLE IF NOT EXISTS commit
(
    did String,
    collection LowCardinality(String),
    key String,
    rev UInt64,
    value String
)
ENGINE = MergeTree
PRIMARY KEY (did, collection, key)
ORDER BY (did, collection, key, rev);

ALTER TABLE commit ADD INDEX rev_mmix rev TYPE minmax()