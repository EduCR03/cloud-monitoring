import os
import unittest

try:
    import psycopg
except Exception:  # pragma: no cover - optional local dependency
    psycopg = None

from backend.cloudv2_auth import AuthService
from backend.cloudv2_persistence import TelemetryPersistence


TEST_POSTGRES_URL = str(os.environ.get("TEST_POSTGRES_URL", "")).strip()


@unittest.skipUnless(TEST_POSTGRES_URL and psycopg is not None, "TEST_POSTGRES_URL ausente")
class PostgresSmokeIntegrationTests(unittest.TestCase):
    def setUp(self):
        with psycopg.connect(TEST_POSTGRES_URL, autocommit=True) as conn:
            with conn.cursor() as cursor:
                cursor.execute("DROP SCHEMA public CASCADE;")
                cursor.execute("CREATE SCHEMA public;")

    def _build_persistence(self):
        persistence = TelemetryPersistence(
            db_backend="postgres",
            database_url=TEST_POSTGRES_URL,
        )
        persistence.start()
        self.addCleanup(persistence.stop)
        return persistence

    def test_persistence_roundtrip_on_postgres(self):
        persistence = self._build_persistence()
        persistence.ensure_pivot("Pivot_A", pivot_slug="pivot-a", seen_ts=1000.0)
        run_row = persistence.get_or_create_active_run(
            now_ts=1001.0,
            source="runtime",
            label="Smoke",
            metadata={"origin": "postgres-smoke"},
        )
        self.assertTrue(run_row)
        session_row = persistence.get_or_create_active_session(
            "Pivot_A",
            pivot_slug="pivot-a",
            now_ts=1002.0,
            source="runtime",
            run_id=run_row["run_id"],
        )
        self.assertTrue(session_row)

        persistence.upsert_snapshot(
            "Pivot_A",
            session_row["session_id"],
            {
                "updated_at_ts": 1003.0,
                "summary": {
                    "status": {"code": "connected"},
                    "quality": {"code": "green"},
                    "last_activity_ts": 1003.0,
                    "last_monitored_message_ts": 1003.0,
                    "median_ready": True,
                    "median_sample_count": 5,
                    "median_cloudv2_interval_sec": 300.0,
                    "disconnect_threshold_sec": 900.0,
                },
            },
            updated_at_ts=1003.0,
        )
        persistence.upsert_probe_setting("Pivot_A", True, 900)
        persistence.replace_expected_pivots_pending(
            [{"pivot_id": "Pivot_New", "added_at_ts": 1004.0, "source": "ui"}]
        )
        persistence.record_connected_pivots_hourly(1005.0, 1, 1)
        persistence.record_summary_cards_hourly(
            1005.0,
            {
                "total": 1,
                "connected": 1,
                "disconnected": 0,
                "initial": 0,
                "green": 1,
                "calculating": 0,
                "yellow": 0,
                "critical": 0,
            },
        )

        state = persistence.get_run_state_payload(run_id=run_row["run_id"])
        self.assertEqual(len(state.get("pivots", [])), 1)
        self.assertEqual(
            state["pivots"][0]["summary"]["status"]["code"],
            "connected",
        )

        probe_settings = persistence.load_probe_settings()
        self.assertIn("Pivot_A", probe_settings)
        self.assertEqual(int(probe_settings["Pivot_A"]["interval_sec"]), 900)

        pending = persistence.load_expected_pivots_pending()
        self.assertEqual(len(pending), 1)
        self.assertEqual(pending[0]["pivot_id"], "Pivot_New")

        connected_history = persistence.fetch_connected_pivots_hourly_history(limit=5)
        self.assertEqual(len(connected_history), 1)
        self.assertEqual(int(connected_history[0]["connected_count"]), 1)

        summary_history = persistence.fetch_summary_cards_hourly_history(limit=5)
        self.assertEqual(len(summary_history), 1)
        self.assertEqual(int(summary_history[0]["connected_count"]), 1)

    def test_auth_roundtrip_on_postgres(self):
        self._build_persistence()
        auth = AuthService(
            db_backend="postgres",
            database_url=TEST_POSTGRES_URL,
        )

        admin_result = auth.ensure_fixed_admin_account()
        self.assertTrue(admin_result.get("ok"))

        register_result = auth.register_user(
            "postgres-user@example.com",
            "12345678",
            "12345678",
            name="Teste Postgres",
            privacy_policy_accepted=True,
        )
        self.assertTrue(register_result.get("ok"))

        login_result = auth.login_user(
            "postgres-user@example.com",
            "12345678",
            ip_address="127.0.0.1",
            user_agent="postgres-smoke",
        )
        self.assertTrue(login_result.get("ok"))
        session_token = str(login_result.get("session_token") or "")
        self.assertTrue(session_token)

        session_user = auth.resolve_session(session_token)
        self.assertTrue(session_user)
        self.assertEqual(session_user["email"], "postgres-user@example.com")

        auth.logout_session(session_token)
        self.assertIsNone(auth.resolve_session(session_token))


if __name__ == "__main__":
    unittest.main()
