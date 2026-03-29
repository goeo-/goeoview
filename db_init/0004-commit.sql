CREATE TABLE IF NOT EXISTS commit
(
    did String,
    collection LowCardinality(String),
    key String,
    rev UInt64,
    value String,
    created_month UInt32 DEFAULT toYYYYMM(now()),
    INDEX rev_mmix rev TYPE minmax() GRANULARITY 1
)
ENGINE = MergeTree
PRIMARY KEY (did, collection, key)
ORDER BY (did, collection, key, rev)
PARTITION BY created_month
SETTINGS index_granularity = 8192;
