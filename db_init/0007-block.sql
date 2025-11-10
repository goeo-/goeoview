CREATE TABLE IF NOT EXISTS block
(
    `src` UInt64,
    `key` UInt64,
    `rev` UInt64,
    `dst` UInt64
)
ENGINE = MergeTree
ORDER BY (src, key, rev);

CREATE MATERIALIZED VIEW IF NOT EXISTS block_mv TO block
AS SELECT
    src_id.id AS src,
    parse_sb32(key) as key,
    rev,
    dst_id.id AS dst
FROM commit
INNER JOIN did_id AS src_id ON src_id.did = commit.did
LEFT JOIN did_id AS dst_id ON dst_id.did = JSONExtractString(commit.value, 'subject')
WHERE collection = 'app.bsky.graph.block'
