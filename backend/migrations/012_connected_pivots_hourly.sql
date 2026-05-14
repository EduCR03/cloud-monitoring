CREATE TABLE IF NOT EXISTS connected_pivots_hourly (
    bucket_ts INTEGER PRIMARY KEY,
    connected_count INTEGER NOT NULL,
    total_count INTEGER NOT NULL,
    created_at_ts REAL NOT NULL,
    updated_at_ts REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_connected_pivots_hourly_bucket_desc
    ON connected_pivots_hourly (bucket_ts DESC);
