CREATE TABLE IF NOT EXISTS gap
(
    did String,
    start_rev UInt64 DEFAULT 0,
    end_rev UInt64
)
ENGINE = MergeTree
ORDER BY did