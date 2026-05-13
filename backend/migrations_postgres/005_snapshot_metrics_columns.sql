ALTER TABLE pivot_snapshots ADD COLUMN IF NOT EXISTS median_ready INTEGER;
ALTER TABLE pivot_snapshots ADD COLUMN IF NOT EXISTS median_sample_count INTEGER;
ALTER TABLE pivot_snapshots ADD COLUMN IF NOT EXISTS median_cloudv2_interval_sec DOUBLE PRECISION;
ALTER TABLE pivot_snapshots ADD COLUMN IF NOT EXISTS disconnect_threshold_sec DOUBLE PRECISION;
