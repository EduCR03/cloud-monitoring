import argparse
import json
import os
import sys
from urllib.parse import urlsplit, urlunsplit

from backend.cloudv2_db import connect_database, resolve_database_settings


def redact_database_url(database_url):
    raw_url = str(database_url or "").strip()
    if not raw_url:
        return ""
    try:
        parsed = urlsplit(raw_url)
    except Exception:
        return "<invalid-url>"
    if not parsed.netloc or "@" not in parsed.netloc:
        return raw_url

    auth, host = parsed.netloc.rsplit("@", 1)
    if ":" in auth:
        user, _password = auth.split(":", 1)
        safe_auth = f"{user}:***"
    else:
        safe_auth = auth
    return urlunsplit((parsed.scheme, f"{safe_auth}@{host}", parsed.path, parsed.query, parsed.fragment))


def _check(label, fn):
    try:
        details = fn()
        return {"label": label, "ok": True, "details": details if details is not None else {}}
    except Exception as exc:
        return {"label": label, "ok": False, "error": str(exc)}


def _fetchone(conn, sql, params=None):
    if params is None:
        return conn.execute(sql).fetchone() or {}
    return conn.execute(sql, params).fetchone() or {}


def _existing_public_tables(conn):
    rows = conn.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
            AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """
    ).fetchall()
    return [str(row["table_name"]) for row in rows]


def run_preflight(database_url, require_empty=False):
    settings = resolve_database_settings({"db_backend": "postgres", "database_url": database_url})
    result = {
        "ok": False,
        "database_url": redact_database_url(settings.database_url),
        "checks": [],
    }

    conn = None
    try:
        conn = connect_database(settings)
        result["checks"].append({"label": "connect", "ok": True})

        result["checks"].append(
            _check(
                "server_identity",
                lambda: _fetchone(
                    conn,
                    """
                    SELECT
                        current_database() AS database,
                        current_user AS user,
                        current_schema() AS schema
                    """,
                ),
            )
        )
        result["checks"].append(_check("server_version", lambda: _fetchone(conn, "SHOW server_version")))
        result["checks"].append(_check("server_timezone", lambda: _fetchone(conn, "SHOW TIMEZONE")))
        result["checks"].append(
            _check(
                "schema_privileges",
                lambda: _fetchone(
                    conn,
                    """
                    SELECT
                        has_schema_privilege(current_user, 'public', 'USAGE') AS can_use_public,
                        has_schema_privilege(current_user, 'public', 'CREATE') AS can_create_public
                    """,
                ),
            )
        )

        def _temp_write():
            with conn:
                conn.execute("CREATE TEMP TABLE cloudv2_preflight_tmp(id INTEGER) ON COMMIT DROP")
                conn.execute("INSERT INTO cloudv2_preflight_tmp(id) VALUES (1)")
                row = _fetchone(conn, "SELECT COUNT(*) AS qty FROM cloudv2_preflight_tmp")
                if int(row["qty"] or 0) != 1:
                    raise RuntimeError("temp table write check failed")
                return {"temp_rows": int(row["qty"] or 0)}

        result["checks"].append(_check("temporary_write", _temp_write))

        def _empty_schema():
            tables = _existing_public_tables(conn)
            if require_empty and tables:
                raise RuntimeError("public schema possui tabelas: " + ", ".join(tables))
            return {"existing_tables": tables, "require_empty": bool(require_empty)}

        result["checks"].append(_check("public_schema_inventory", _empty_schema))
    except Exception as exc:
        result["checks"].append({"label": "connect", "ok": False, "error": str(exc)})
    finally:
        if conn is not None:
            conn.close()

    result["ok"] = all(bool(item.get("ok")) for item in result["checks"])
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Valida acesso seguro ao PostgreSQL/RDS sem migrar dados.")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="URL PostgreSQL. Tambem aceita DATABASE_URL.",
    )
    parser.add_argument(
        "--require-empty",
        action="store_true",
        help="Falha se schema public ja tiver tabelas.",
    )
    parser.add_argument("--report-json", default="", help="Arquivo opcional para salvar relatorio JSON.")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    result = run_preflight(
        database_url=args.database_url,
        require_empty=bool(args.require_empty),
    )
    if args.report_json:
        report_path = os.path.abspath(str(args.report_json))
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2)

    print(f"database: {result['database_url']}")
    for item in result["checks"]:
        status = "ok" if item.get("ok") else "fail"
        print(f"{status}: {item['label']}")
        if item.get("error"):
            print(f"  {item['error']}")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
