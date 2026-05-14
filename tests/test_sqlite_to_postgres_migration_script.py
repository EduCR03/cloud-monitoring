import os
import sqlite3
import tempfile
import unittest

from scripts.migrate_sqlite_to_postgres import (
    TABLE_COPY_PLAN,
    backup_sqlite_file,
    build_insert_sql,
    build_select_sql,
    sqlite_table_count,
    sqlite_table_exists,
)


class SqliteToPostgresMigrationScriptTests(unittest.TestCase):
    def test_backup_sqlite_file_creates_copy(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            sqlite_path = os.path.join(temp_dir, "telemetry.sqlite3")
            with open(sqlite_path, "w", encoding="utf-8") as handle:
                handle.write("abc123")
            backup_path = backup_sqlite_file(sqlite_path, backup_dir=temp_dir)
            self.assertTrue(os.path.isfile(backup_path))
            with open(backup_path, "r", encoding="utf-8") as handle:
                self.assertEqual(handle.read(), "abc123")

    def test_copy_plan_contains_expected_tables(self):
        table_names = [item["table"] for item in TABLE_COPY_PLAN]
        self.assertIn("pivots", table_names)
        self.assertIn("users", table_names)
        self.assertIn("summary_cards_hourly", table_names)
        self.assertNotIn("schema_migrations", table_names)

    def test_build_sql_helpers_use_declared_columns(self):
        plan_item = {"table": "pivots", "columns": ["pivot_id", "pivot_slug"]}
        self.assertEqual(
            build_select_sql(plan_item),
            "SELECT pivot_id, pivot_slug FROM pivots ORDER BY pivot_id",
        )
        self.assertEqual(
            build_insert_sql(plan_item),
            "INSERT INTO pivots (pivot_id, pivot_slug) VALUES (%s, %s)",
        )

    def test_sqlite_table_helpers_work(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "test.sqlite3")
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, name TEXT)")
                conn.execute("INSERT INTO sample(name) VALUES ('A'), ('B')")
                self.assertTrue(sqlite_table_exists(conn, "sample"))
                self.assertEqual(sqlite_table_count(conn, "sample"), 2)
            finally:
                conn.close()


if __name__ == "__main__":
    unittest.main()
