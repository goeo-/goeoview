CREATE TABLE IF NOT EXISTS commit_recent
(
    did String,
    collection LowCardinality(String),
    key String,
    rev UInt64,
    value String
)
ENGINE = Memory
SETTINGS max_bytes_to_keep=8000000000;

CREATE MATERIALIZED VIEW IF NOT EXISTS commit_recent_mv TO commit_recent
AS SELECT *
FROM commit WHERE collection NOT IN ('app.bsky.feed.like', 'app.bsky.graph.follow', 'app.bsky.graph.block')