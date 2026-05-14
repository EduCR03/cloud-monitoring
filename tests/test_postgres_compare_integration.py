import os
import tempfile
import unittest

try:
    import psycopg
except Exception:  # pragma: no cover - optional local dependency
    psycopg = None

from backend.cloudv2_persistence import TelemetryPersistence
from scripts.validate_postgres_migration import compare_backends


TEST_POSTGRES_URL = str(os.environ.get("TEST_POSTGRES_URL", "")).strip()


@unittest.skipUnless(TEST_POSTGRES_URL and psycopg is not None, "TEST_POSTGRES_URL ausente")
class PostgresCompareIntegrationTests(unittest.TestCase):
    def setUp(self):
        with psycopg.connect(TEST_POSTGRES_URL, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA public CASCADE;")
                cursor.execute("CREATE SCHEMA public;")

    def test_compare_backends_matches_seeded_sqlite_dataset(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=sqlite_path, db_backend="sqlite")
            persistence.start()
            try:
                persistence.ensure_pivot("Pivot_A", pivot_slug="pivot-a", seen_ts=1700000000.0)
                persistence.ensure_pivot("Pivot_B", pivot_slug="pivot-b", seen_ts=1700000300.0)

                run_row = persistence.get_or_create_active_run(
                    now_ts=1700000400.0,
                    source="runtime",
                    label="Compare Validation",
                    metadata={"origin": "sqlite-compare"},
                )

                session_a = persistence.get_or_create_active_session(
                    "Pivot_A",
                    pivot_slug="pivot-a",
                    now_ts=1700000500.0,
                    source="runtime",
                    run_id=run_row["run_id"],
                )
                session_b = persistence.get_or_create_active_session(
                    "Pivot_B",
                    pivot_slug="pivot-b",
                    now_ts=1700000600.0,
                    source="runtime",
                    run_id=run_row["run_id"],
                )

                persistence.upsert_snapshot(
                    "Pivot_A",
                    session_a["session_id"],
                    {
                        "updated_at_ts": 1700000700.0,
                        "summary": {
                            "status": {"code": "connected"},
                            "quality": {"code": "green"},
                            "last_activity_ts": 1700000700.0,
                            "last_monitored_message_ts": 1700000700.0,
                            "median_ready": True,
                            "median_sample_count": 6,
                            "median_cloudv2_interval_sec": 300.0,
                            "disconnect_threshold_sec": 900.0,
                        },
                    },
                    updated_at_ts=1700000700.0,
                )
                persistence.upsert_snapshot(
                    "Pivot_B",
                    session_b["session_id"],
                    {
                        "updated_at_ts": 1700000800.0,
                        "summary": {
                            "status": {"code": "disconnected"},
                            "quality": {"code": "critical"},
                            "last_activity_ts": 1700000200.0,
                            "last_monitored_message_ts": 1700000200.0,
                            "median_ready": True,
                            "median_sample_count": 6,
                            "median_cloudv2_interval_sec": 300.0,
                            "disconnect_threshold_sec": 900.0,
                        },
                    },
                    updated_at_ts=1700000800.0,
                )

                persistence.upsert_probe_setting("Pivot_A", True, 600)
                persistence.replace_expected_pivots_pending(
                    [{"pivot_id": "Pivot_C", "added_at_ts": 1700000900.0, "source": "ui"}]
                )
                persistence.record_connected_pivots_hourly(1700001000.0, 1, 2)
                persistence.record_summary_cards_hourly(
                    1700001000.0,
                    {
                        "total": 2,
                        "connected": 1,
                        "disconnected": 1,
                        "initial": 0,
                        "green": 1,
                        "calculating": 0,
                        "yellow": 0,
                        "critical": 1,
                    },
                )
            finally:
                persistence.stop()

            result = compare_backends(
                sqlite_path=sqlite_path,
                database_url=TEST_POSTGRES_URL,
                migrate_first=True,
                sample_limit=2,
            )
            if not result.get("ok"):
                self.fail(str(result.get("failed")))


if __name__ == "__main__":
    unittest.main()
