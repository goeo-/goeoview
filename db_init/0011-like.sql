CREATE TABLE IF NOT EXISTS like
(
    src UInt64,
    key UInt64,
    rev UInt64,
    dst_id UInt64,
    dst_collection LowCardinality(String),
    dst_key String
)
ENGINE = MergeTree
ORDER BY (src, key, rev);

CREATE MATERIALIZED VIEW IF NOT EXISTS like_mv TO like
AS SELECT
    src_id.id as src,
    parse_sb32(key) as key,
    rev,
    dst_id.id as dst_id,
    split[4] as dst_collection,
    split[5] as dst_key
FROM (
    SELECT
        did as src,
        key,
        rev,
        splitByChar('/',JSONExtractString(JSONExtractRaw(value, 'subject'),'uri'),5) as split
    FROM commit
    WHERE collection = 'app.bsky.feed.like'
) sq
INNER JOIN did_id AS src_id ON src_id.did = sq.src
LEFT JOIN did_id AS dst_id ON dst_id.did = sq.split[3]