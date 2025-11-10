CREATE TABLE IF NOT EXISTS following
(
    `id` UInt64,
    `following` AggregateFunction(groupBitmap, UInt64)
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY id;

CREATE TABLE IF NOT EXISTS followers
(
    `id` UInt64,
    `followers` AggregateFunction(groupBitmap, UInt64)
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY id;

CREATE TABLE IF NOT EXISTS blocking
(
    `id` UInt64,
    `blocking` AggregateFunction(groupBitmap, UInt64)
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY id;

CREATE TABLE IF NOT EXISTS blockers
(
    `id` UInt64,
    `blockers` AggregateFunction(groupBitmap, UInt64)
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY id;
