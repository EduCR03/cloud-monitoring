import os
import tempfile
import unittest

from backend.cloudv2_auth import AuthService
from backend.cloudv2_db import connect_database, normalize_db_backend, resolve_database_settings
from backend.cloudv2_persistence import TelemetryPersistence


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
