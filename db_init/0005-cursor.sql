CREATE TABLE IF NOT EXISTS cursor
(
    url String,
    val UInt64
)
ENGINE = ReplacingMergeTree(val)
ORDER BY url;