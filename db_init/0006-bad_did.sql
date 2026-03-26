CREATE TABLE IF NOT EXISTS bad_did
(
    did String,
    status_code UInt16,
    failed_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(failed_at)
ORDER BY did;
