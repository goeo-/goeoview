CREATE TABLE IF NOT EXISTS follow
(
    src UInt64,
    key UInt64,
    rev UInt64,
    dst UInt64
)
ENGINE = MergeTree
ORDER BY (src, key, rev);

CREATE MATERIALIZED VIEW IF NOT EXISTS follow_mv TO follow
AS SELECT
    src_id.id AS src,
    parse_sb32(key) as key,
    rev,
    dst_id.id AS dst
FROM commit
INNER JOIN did_id AS src_id ON src_id.did = commit.did
LEFT JOIN did_id AS dst_id ON dst_id.did = JSONExtractString(commit.value, 'subject')
WHERE collection = 'app.bsky.graph.follow'

CREATE TABLE IF NOT EXISTS new_follow
(
    src UInt64,
    key UInt64,
    rev UInt64,
    dst UInt64
)
ENGINE = Null;

CREATE MATERIALIZED VIEW IF NOT EXISTS new_follow_mv TO new_follow
AS SELECT * FROM follow;

CREATE TABLE IF NOT EXISTS follow_rn
(
    src UInt64,
    dst UInt64
)
ENGINE = MergeTree
ORDER BY src;

CREATE TABLE IF NOT EXISTS follow_rn_rev
(
    src UInt64,
    dst UInt64
)
ENGINE = MergeTree
ORDER BY dst;


CREATE MATERIALIZED VIEW IF NOT EXISTS follow_rn_rev_mv TO follow_rn_rev
AS SELECT * FROM follow_rn;