SET join_algorithm = 'direct, hash';

CREATE TABLE IF NOT EXISTS did_id
(
    `did` String,
    `id` UInt64
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY did;

CREATE TABLE IF NOT EXISTS id_did
(
    `id` UInt64,
    `did` String
)
ENGINE = EmbeddedRocksDB
PRIMARY KEY id;

CREATE MATERIALIZED VIEW IF NOT EXISTS id_did_mv TO id_did
AS SELECT id, did
FROM did_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS new_actors_mv TO did_id
AS SELECT did, generateSnowflakeID() as id 
FROM (
    SELECT DISTINCT did FROM commit
    WHERE did NOT IN (
        SELECT did FROM did_id WHERE did IN (
            SELECT DISTINCT did from commit
        )
    )
)