CREATE TABLE did_web
(
    `did` String,
    `fetched_at` DateTime64(9, 'UTC'),
    `doc` String
)
ENGINE = MergeTree
ORDER BY (did, fetched_at)