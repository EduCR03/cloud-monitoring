CREATE TABLE IF NOT EXISTS summary_cards_hourly (
    bucket_ts INTEGER PRIMARY KEY,
    total_count INTEGER NOT NULL,
    connected_count INTEGER NOT NULL,
    disconnected_count INTEGER NOT NULL,
    initial_count INTEGER NOT NULL,
    quality_green_count INTEGER NOT NULL,
    quality_calculating_count INTEGER NOT NULL,
    quality_yellow_count INTEGER NOT NULL,
    quality_critical_count INTEGER NOT NULL,
    created_at_ts DOUBLE PRECISION NOT NULL,
    updated_at_ts DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_summary_cards_hourly_bucket_desc
    ON summary_cards_hourly (bucket_ts DESC);

INSERT INTO summary_cards_hourly (
    bucket_ts,
    total_count,
    connected_count,
    disconnected_count,
    initial_count,
    quality_green_count,
    quality_calculating_count,
    quality_yellow_count,
    quality_critical_count,
    created_at_ts,
    updated_at_ts
)
SELECT
    legacy.bucket_ts,
    legacy.total_count,
    legacy.connected_count,
    CASE
        WHEN legacy.total_count > legacy.connected_count THEN legacy.total_count - legacy.connected_count
        ELSE 0
    END AS disconnected_count,
    0 AS initial_count,
    0 AS quality_green_count,
    0 AS quality_calculating_count,
    0 AS quality_yellow_count,
    0 AS quality_critical_count,
    legacy.created_at_ts,
    legacy.updated_at_ts
FROM connected_pivots_hourly AS legacy
WHERE NOT EXISTS (
    SELECT 1
    FROM summary_cards_hourly
);
