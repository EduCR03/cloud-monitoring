import os
import re
import unittest

from backend.cloudv2_persistence import (
    DEFAULT_DB_PATH,
    DEFAULT_MIGRATIONS_POSTGRES_DIR,
    DEFAULT_MIGRATIONS_SQLITE_DIR,
    TelemetryPersistence,
)


def _migration_versions(directory):
    versions = []
    for filename in os.listdir(directory):
        match = re.match(r"^(\d+)[_-].+\.sql$", str(filename))
        if not match:
            continue
        versions.append(int(match.group(1)))
    return sorted(versions)


class PostgresMigrationInventoryTests(unittest.TestCase):
    def test_postgres_inventory_matches_sqlite_inventory(self):
        sqlite_versions = _migration_versions(DEFAULT_MIGRATIONS_SQLITE_DIR)
        postgres_versions = _migration_versions(DEFAULT_MIGRATIONS_POSTGRES_DIR)
        self.assertEqual(postgres_versions, sqlite_versions)

    def test_postgres_backend_uses_postgres_migrations_dir(self):
        persistence = TelemetryPersistence(
            db_path=DEFAULT_DB_PATH,
            db_backend="postgres",
            database_url="postgresql://postgres:postgres@localhost:5432/cloudv2_test",
        )
        self.assertEqual(
            os.path.normpath(persistence.migrations_dir),
            os.path.normpath(DEFAULT_MIGRATIONS_POSTGRES_DIR),
        )


if __name__ == "__main__":
    unittest.main()
