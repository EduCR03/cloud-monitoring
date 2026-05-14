CREATE INDEX IF NOT EXISTS idx_connectivity_events_pivot_session_ts_id_desc
    ON connectivity_events (pivot_id, session_id, ts DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_probe_events_pivot_session_ts_id_desc
    ON probe_events (pivot_id, session_id, ts DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_probe_delay_points_pivot_session_ts_id_asc
    ON probe_delay_points (pivot_id, session_id, ts ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_cloud2_events_pivot_session_ts_id_desc
    ON cloud2_events (pivot_id, session_id, ts DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_drop_events_pivot_session_ts_id_desc
    ON drop_events (pivot_id, session_id, ts DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_probe_events_pivot_ts_id_asc
    ON probe_events (pivot_id, ts ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_monitoring_sessions_run_pivot_updated_started
    ON monitoring_sessions (run_id, pivot_id, updated_at_ts DESC, started_at_ts DESC, session_id);
