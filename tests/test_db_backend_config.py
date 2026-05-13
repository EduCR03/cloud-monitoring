import os
import tempfile
import unittest

from backend.cloudv2_auth import AuthService
from backend.cloudv2_db import (
    PostgresCompatConnection,
    connect_database,
    normalize_db_backend,
    resolve_database_settings,
    translate_sqlite_sql_to_postgres,
)
from backend.cloudv2_persistence import TelemetryPersistence


class _FakeRawConnection:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))
        return self

    def fetchone(self):
        return {"ok": 1}

    def fetchall(self):
        return []

    def close(self):
        return None


class DbBackendConfigTests(unittest.TestCase):
    def test_normalize_db_backend_accepts_aliases(self):
        self.assertEqual(normalize_db_backend("sqlite3"), "sqlite")
        self.assertEqual(normalize_db_backend("postgresql"), "postgres")
        self.assertEqual(normalize_db_backend("pg"), "postgres")

    def test_resolve_database_settings_defaults_to_sqlite(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            settings = resolve_database_settings(
                {"db_backend": "sqlite", "sqlite_db_path": db_path}
            )
            self.assertEqual(settings.backend, "sqlite")
            self.assertEqual(settings.sqlite_db_path, db_path)
            self.assertEqual(settings.database_url, "")

    def test_resolve_database_settings_requires_database_url_for_postgres(self):
        with self.assertRaisesRegex(ValueError, "database_url obrigatorio"):
            resolve_database_settings({"db_backend": "postgres", "sqlite_db_path": ""})

    def test_connect_database_keeps_sqlite_behavior(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            settings = resolve_database_settings({"db_backend": "sqlite", "sqlite_db_path": db_path})
            conn = connect_database(settings)
            try:
                row = conn.execute("SELECT 1 AS ok").fetchone()
                self.assertEqual(int(row["ok"]), 1)
            finally:
                conn.close()

    def test_telemetry_persistence_exposes_database_settings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            persistence = TelemetryPersistence(db_path=db_path)
            self.assertEqual(persistence.db_backend, "sqlite")
            self.assertEqual(persistence.db_path, db_path)
            self.assertEqual(persistence.database_url, "")

    def test_auth_service_exposes_database_settings(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "telemetry.sqlite3")
            service = AuthService(db_path=db_path)
            self.assertEqual(service.db_backend, "sqlite")
            self.assertEqual(service.db_path, db_path)
            self.assertEqual(service.database_url, "")

    def test_translate_sqlite_sql_to_postgres_rewrites_placeholders_and_nocase(self):
        sql = """
        SELECT value
        FROM pivots
        WHERE pivot_id = ?
        ORDER BY pivot_slug COLLATE NOCASE ASC
        LIMIT ?
        """
        translated = translate_sqlite_sql_to_postgres(sql)
        self.assertIn("pivot_id = %s", translated)
        self.assertIn("ORDER BY LOWER(pivot_slug) ASC", translated)
        self.assertIn("LIMIT %s", translated)

    def test_translate_sqlite_sql_to_postgres_ignores_sqlite_sequence(self):
        translated = translate_sqlite_sql_to_postgres("DELETE FROM sqlite_sequence WHERE name = 'events'")
        self.assertIsNone(translated)

    def test_postgres_compat_connection_rewrites_sql_before_execute(self):
        fake = _FakeRawConnection()
        conn = PostgresCompatConnection(fake)
        row = conn.execute(
            "SELECT value FROM pivots WHERE pivot_id = ? ORDER BY pivot_slug COLLATE NOCASE ASC LIMIT ?",
            ("Pivot_1", 5),
        ).fetchone()
        self.assertEqual(row["ok"], 1)
        self.assertEqual(len(fake.calls), 1)
        sql, params = fake.calls[0]
        self.assertIn("pivot_id = %s", sql)
        self.assertIn("ORDER BY LOWER(pivot_slug) ASC", sql)
        self.assertEqual(params, ("Pivot_1", 5))
