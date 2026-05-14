import argparse
import os
import shutil
import sqlite3
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from backend.cloudv2_persistence import TelemetryPersistence
from backend.cloudv2_db import connect_database, resolve_database_settings


TABLE_COPY_PLAN = [
    {"table": "pivots", "columns": ["pivot_id", "pivot_slug", "first_seen_ts", "last_seen_ts", "created_at_ts", "updated_at_ts"]},
    {"table": "monitoring_runs", "columns": ["run_id", "started_at_ts", "ended_at_ts", "is_active", "source", "label", "metadata_json", "created_at_ts", "updated_at_ts"]},
    {"table": "monitoring_sessions", "columns": ["session_id", "pivot_id", "started_at_ts", "ended_at_ts", "is_active", "source", "label", "metadata_json", "created_at_ts", "updated_at_ts", "run_id"]},
    {"table": "pivot_snapshots", "columns": ["pivot_id", "session_id", "updated_at_ts", "status_code", "quality_code", "last_activity_ts", "last_seen_ts", "snapshot_json", "median_ready", "median_sample_count", "median_cloudv2_interval_sec", "disconnect_threshold_sec"]},
    {"table": "probe_settings", "columns": ["pivot_id", "enabled", "interval_sec", "updated_at_ts"]},
    {"table": "connectivity_events", "columns": ["id", "pivot_id", "session_id", "ts", "topic", "event_type", "summary", "details_json", "source_topic", "raw_payload", "parsed_payload_json", "event_json", "created_at_ts"]},
    {"table": "probe_events", "columns": ["id", "pivot_id", "session_id", "ts", "event_type", "topic", "latency_sec", "deadline_ts", "sent_ts", "payload", "details_json", "event_json", "created_at_ts"]},
    {"table": "probe_delay_points", "columns": ["id", "pivot_id", "session_id", "ts", "latency_sec", "avg_latency_sec", "median_latency_sec", "sample_count", "created_at_ts"]},
    {"table": "cloud2_events", "columns": ["id", "pivot_id", "session_id", "ts", "rssi", "technology", "drop_duration_raw", "drop_duration_sec", "firmware", "event_date", "event_json", "created_at_ts"]},
    {"table": "drop_events", "columns": ["id", "pivot_id", "session_id", "ts", "duration_sec", "technology", "rssi", "event_json", "created_at_ts"]},
    {"table": "ping_rssi_points", "columns": ["id", "pivot_id", "session_id", "ts", "rssi", "created_at_ts"]},
    {"table": "users", "columns": ["id", "email", "name", "password_hash", "email_verified_at", "status", "privacy_policy_version", "privacy_policy_accepted_at", "created_at", "updated_at", "last_login_at", "deleted_at", "role"]},
    {"table": "user_tokens", "columns": ["id", "user_id", "type", "token_hash", "expires_at", "used_at", "created_at"]},
    {"table": "user_sessions", "columns": ["id", "user_id", "session_hash", "created_at", "expires_at", "revoked_at", "last_seen_at", "user_agent", "ip_hash"]},
    {"table": "user_ui_preferences", "columns": ["user_id", "pivot_table_columns_json", "created_at", "updated_at"]},
    {"table": "expected_pivots_pending", "columns": ["pivot_id", "added_at_ts", "source", "updated_at_ts"]},
    {"table": "connected_pivots_hourly", "columns": ["bucket_ts", "connected_count", "total_count", "created_at_ts", "updated_at_ts"]},
    {"table": "summary_cards_hourly", "columns": ["bucket_ts", "total_count", "connected_count", "disconnected_count", "initial_count", "quality_green_count", "quality_calculating_count", "quality_yellow_count", "quality_critical_count", "created_at_ts", "updated_at_ts"]},
]

IDENTITY_TABLES = {
    "connectivity_events",
    "probe_events",
    "probe_delay_points",
    "cloud2_events",
    "drop_events",
    "ping_rssi_points",
}


def backup_sqlite_file(sqlite_path, backup_dir=None):
    safe_path = os.path.abspath(str(sqlite_path or "").strip())
    if not safe_path or not os.path.isfile(safe_path):
        raise FileNotFoundError(f"SQLite ausente: {sqlite_path}")
    target_dir = os.path.abspath(str(backup_dir or os.path.dirname(safe_path) or "."))
    os.makedirs(target_dir, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    backup_name = f"{os.path.basename(safe_path)}.bak-{timestamp}"
    backup_path = os.path.join(target_dir, backup_name)
    shutil.copy2(safe_path, backup_path)
    return backup_path


def build_select_sql(plan_item):
    columns = [str(column) for column in plan_item["columns"]]
    table_name = str(plan_item["table"])
    order_by = " ORDER BY " + ", ".join(columns[:1]) if columns else ""
    return f"SELECT {', '.join(columns)} FROM {table_name}{order_by}"


def build_insert_sql(plan_item):
    columns = [str(column) for column in plan_item["columns"]]
    table_name = str(plan_item["table"])
    placeholders = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"


def sqlite_table_exists(conn, table_name):
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type = 'table'
            AND name = ?
        LIMIT 1
        """,
        (str(table_name),),
    ).fetchone()
    return row is not None


def postgres_table_count(conn, table_name):
    row = conn.execute(f"SELECT COUNT(*) AS qty FROM {table_name}").fetchone()
    if row is None:
        return 0
    return int(row["qty"] or 0)


def sqlite_table_count(conn, table_name):
    row = conn.execute(f"SELECT COUNT(*) AS qty FROM {table_name}").fetchone()
    if row is None:
        return 0
    return int(row["qty"] or 0)


def truncate_postgres_tables(conn):
    table_names = [item["table"] for item in reversed(TABLE_COPY_PLAN)]
    conn.execute(f"TRUNCATE TABLE {', '.join(table_names)} RESTART IDENTITY CASCADE")


def copy_table(sqlite_conn, postgres_conn, plan_item, batch_size=1000):
    table_name = str(plan_item["table"])
    if not sqlite_table_exists(sqlite_conn, table_name):
        return 0
    select_sql = build_select_sql(plan_item)
    insert_sql = build_insert_sql(plan_item)
    cursor = sqlite_conn.execute(select_sql)
    total_rows = 0
    while True:
        rows = cursor.fetchmany(int(batch_size))
        if not rows:
            break
        payload = [tuple(row[column] for column in plan_item["columns"]) for row in rows]
        with postgres_conn.cursor() as pg_cursor:
            pg_cursor.executemany(insert_sql, payload)
        total_rows += len(payload)
    return total_rows


def reset_postgres_identity_sequences(conn):
    for table_name in sorted(IDENTITY_TABLES):
        conn.execute(
            f"""
            SELECT setval(
                pg_get_serial_sequence('{table_name}', 'id'),
                COALESCE((SELECT MAX(id) FROM {table_name}), 1),
                EXISTS(SELECT 1 FROM {table_name})
            )
            """
        )


def verify_counts(sqlite_conn, postgres_conn):
    results = []
    for plan_item in TABLE_COPY_PLAN:
        table_name = str(plan_item["table"])
        sqlite_count = sqlite_table_count(sqlite_conn, table_name) if sqlite_table_exists(sqlite_conn, table_name) else 0
        postgres_count = postgres_table_count(postgres_conn, table_name)
        results.append(
            {
                "table": table_name,
                "sqlite_count": sqlite_count,
                "postgres_count": postgres_count,
                "match": sqlite_count == postgres_count,
            }
        )
    return results


def bootstrap_postgres_schema(database_url):
    persistence = TelemetryPersistence(db_backend="postgres", database_url=database_url)
    persistence.start()
    persistence.stop()


def migrate(sqlite_path, database_url, backup=True, backup_dir=None, truncate_target=True, batch_size=1000):
    backup_path = backup_sqlite_file(sqlite_path, backup_dir=backup_dir) if backup else ""
    bootstrap_postgres_schema(database_url)

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    pg_settings = resolve_database_settings({"db_backend": "postgres", "database_url": database_url})
    postgres_conn = connect_database(pg_settings)
    copied = []
    try:
        if truncate_target:
            with postgres_conn:
                truncate_postgres_tables(postgres_conn)
        with postgres_conn:
            for plan_item in TABLE_COPY_PLAN:
                copied.append(
                    {
                        "table": str(plan_item["table"]),
                        "rows": copy_table(sqlite_conn, postgres_conn, plan_item, batch_size=batch_size),
                    }
                )
            reset_postgres_identity_sequences(postgres_conn)
        verification = verify_counts(sqlite_conn, postgres_conn)
    finally:
        sqlite_conn.close()
        postgres_conn.close()

    return {
        "backup_path": backup_path,
        "copied": copied,
        "verification": verification,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Migra telemetry.sqlite3 para PostgreSQL.")
    parser.add_argument("--sqlite-path", required=True, help="Caminho do telemetry.sqlite3")
    parser.add_argument("--database-url", required=True, help="URL PostgreSQL destino")
    parser.add_argument("--backup-dir", default="", help="Diretorio para backup do SQLite")
    parser.add_argument("--skip-backup", action="store_true", help="Nao gerar backup antes da migracao")
    parser.add_argument("--keep-target-data", action="store_true", help="Nao truncar tabelas destino")
    parser.add_argument("--batch-size", type=int, default=1000, help="Lote de inserts")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    result = migrate(
        sqlite_path=args.sqlite_path,
        database_url=args.database_url,
        backup=not bool(args.skip_backup),
        backup_dir=args.backup_dir or None,
        truncate_target=not bool(args.keep_target_data),
        batch_size=max(1, int(args.batch_size or 1000)),
    )
    print(f"backup: {result['backup_path'] or 'skip'}")
    for item in result["copied"]:
        print(f"copied {item['table']}: {item['rows']}")
    for item in result["verification"]:
        status = "ok" if item["match"] else "mismatch"
        print(f"verify {item['table']}: sqlite={item['sqlite_count']} postgres={item['postgres_count']} {status}")
    if any(not item["match"] for item in result["verification"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
