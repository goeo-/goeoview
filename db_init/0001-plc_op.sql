CREATE TABLE IF NOT EXISTS plc_op
(
    created_at DateTime64(9, 'UTC'),
    did String,
    cid String,
    rotation_keys Array(String),
    handle String NULL,
    pds String NULL,
    labeler String NULL,
    chat String NULL,
    feedgen String NULL,
    atproto_key String NULL,
    labeler_key String NULL,
    signed_by UInt16 NULL
)
ENGINE = MergeTree
ORDER BY (created_at, did, cid);