CREATE TABLE IF NOT EXISTS monitoring_runs (
    run_id TEXT PRIMARY KEY,
    started_at_ts DOUBLE PRECISION NOT NULL,
    ended_at_ts DOUBLE PRECISION,
    is_active INTEGER NOT NULL DEFAULT 1,
    source TEXT NOT NULL DEFAULT 'runtime',
    label TEXT,
    metadata_json TEXT,
    created_at_ts DOUBLE PRECISION NOT NULL,
    updated_at_ts DOUBLE PRECISION NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_monitoring_runs_single_active
    ON monitoring_runs (is_active)
    WHERE is_active = 1;

CREATE INDEX IF NOT EXISTS idx_monitoring_runs_started
    ON monitoring_runs (started_at_ts DESC, updated_at_ts DESC);

ALTER TABLE monitoring_sessions ADD COLUMN IF NOT EXISTS run_id TEXT;

CREATE INDEX IF NOT EXISTS idx_monitoring_sessions_run_pivot_started
    ON monitoring_sessions (run_id, pivot_id, started_at_ts DESC);

CREATE INDEX IF NOT EXISTS idx_monitoring_sessions_run_updated
    ON monitoring_sessions (run_id, updated_at_ts DESC);

CREATE TEMP TABLE IF NOT EXISTS _tmp_legacy_run (
    run_id TEXT
);

DELETE FROM _tmp_legacy_run;

INSERT INTO _tmp_legacy_run (run_id)
SELECT md5(random()::text || clock_timestamp()::text)
WHERE EXISTS (
    SELECT 1
    FROM monitoring_sessions
    WHERE run_id IS NULL
);

INSERT INTO monitoring_runs (
    run_id,
    started_at_ts,
    ended_at_ts,
    is_active,
    source,
    label,
    metadata_json,
    created_at_ts,
    updated_at_ts
)
SELECT
    tmp.run_id,
    COALESCE(MIN(s.started_at_ts), EXTRACT(EPOCH FROM NOW())::DOUBLE PRECISION),
    CASE
        WHEN SUM(CASE WHEN s.is_active = 1 THEN 1 ELSE 0 END) > 0 THEN NULL
        ELSE MAX(COALESCE(s.ended_at_ts, s.updated_at_ts, s.started_at_ts))
    END,
    CASE
        WHEN SUM(CASE WHEN s.is_active = 1 THEN 1 ELSE 0 END) > 0 THEN 1
        ELSE 0
    END,
    'migration',
    'Legacy import',
    '{}',
    EXTRACT(EPOCH FROM NOW())::DOUBLE PRECISION,
    EXTRACT(EPOCH FROM NOW())::DOUBLE PRECISION
FROM monitoring_sessions AS s
CROSS JOIN _tmp_legacy_run AS tmp
WHERE s.run_id IS NULL
GROUP BY tmp.run_id
HAVING COUNT(*) > 0;

UPDATE monitoring_sessions
SET run_id = (
    SELECT run_id
    FROM _tmp_legacy_run
    LIMIT 1
)
WHERE run_id IS NULL
    AND EXISTS (SELECT 1 FROM _tmp_legacy_run);

DROP TABLE IF EXISTS _tmp_legacy_run;
