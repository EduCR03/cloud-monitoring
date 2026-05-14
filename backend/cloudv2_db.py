"""Database configuration and connection helpers for SQLite/PostgreSQL."""

from dataclasses import dataclass
import os
import re
import sqlite3

try:
    import psycopg
    from psycopg.rows import dict_row as psycopg_dict_row
except Exception:  # pragma: no cover - optional dependency until PostgreSQL rollout
    psycopg = None
    psycopg_dict_row = None


SUPPORTED_DB_BACKENDS = ("sqlite", "postgres")


@dataclass(frozen=True)
class DatabaseSettings:
    backend: str
    sqlite_db_path: str
    database_url: str


class EmptyResult:
    def fetchone(self):
        return None

    def fetchall(self):
        return []


class PostgresCompatConnection:
    def __init__(self, raw_connection):
        self._raw_connection = raw_connection

    def __enter__(self):
        self._raw_connection.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        return self._raw_connection.__exit__(exc_type, exc, tb)

    def close(self):
        return self._raw_connection.close()

    def commit(self):
        return self._raw_connection.commit()

    def rollback(self):
        return self._raw_connection.rollback()

    def cursor(self, *args, **kwargs):
        return self._raw_connection.cursor(*args, **kwargs)

    def execute(self, sql, params=None):
        translated_sql = translate_sqlite_sql_to_postgres(sql)
        if translated_sql is None:
            return EmptyResult()
        if params is None:
            return self._raw_connection.execute(translated_sql)
        return self._raw_connection.execute(translated_sql, tuple(params))

    def __getattr__(self, name):
        return getattr(self._raw_connection, name)


def normalize_db_backend(value, default="sqlite"):
    normalized = str(value or "").strip().lower()
    if normalized in ("postgresql", "postgres", "pg"):
        return "postgres"
    if normalized in ("sqlite", "sqlite3", ""):
        return str(default or "sqlite").strip().lower() or "sqlite"
    raise ValueError(
        f"db_backend invalido: {value!r}. Valores suportados: {', '.join(SUPPORTED_DB_BACKENDS)}"
    )


def resolve_database_settings(config=None, db_backend=None, sqlite_db_path=None, database_url=None):
    source = config if isinstance(config, dict) else {}
    backend = normalize_db_backend(
        db_backend if db_backend is not None else source.get("db_backend", "sqlite"),
        default="sqlite",
    )
    raw_sqlite_path = sqlite_db_path if sqlite_db_path is not None else source.get("sqlite_db_path", "")
    raw_database_url = database_url if database_url is not None else source.get("database_url", "")
    resolved_sqlite_path = str(raw_sqlite_path or "").strip()
    resolved_database_url = str(raw_database_url or "").strip()

    if backend == "sqlite" and not resolved_sqlite_path:
        raise ValueError("sqlite_db_path obrigatorio quando db_backend=sqlite")
    if backend == "postgres" and not resolved_database_url:
        raise ValueError("database_url obrigatorio quando db_backend=postgres")

    return DatabaseSettings(
        backend=backend,
        sqlite_db_path=resolved_sqlite_path,
        database_url=resolved_database_url,
    )


def _replace_qmark_placeholders(sql_text):
    result = []
    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    index = 0
    while index < len(sql_text):
        char = sql_text[index]
        nxt = sql_text[index + 1] if (index + 1) < len(sql_text) else ""
        if in_line_comment:
            result.append(char)
            if char == "\n":
                in_line_comment = False
            index += 1
            continue
        if in_block_comment:
            result.append(char)
            if char == "*" and nxt == "/":
                result.append(nxt)
                in_block_comment = False
                index += 2
                continue
            index += 1
            continue
        if not in_single and not in_double and char == "-" and nxt == "-":
            result.append(char)
            result.append(nxt)
            in_line_comment = True
            index += 2
            continue
        if not in_single and not in_double and char == "/" and nxt == "*":
            result.append(char)
            result.append(nxt)
            in_block_comment = True
            index += 2
            continue
        if char == "'" and not in_double:
            result.append(char)
            if in_single and nxt == "'":
                result.append(nxt)
                index += 2
                continue
            in_single = not in_single
            index += 1
            continue
        if char == '"' and not in_single:
            result.append(char)
            in_double = not in_double
            index += 1
            continue
        if char == "?" and not in_single and not in_double:
            result.append("%s")
            index += 1
            continue
        result.append(char)
        index += 1
    return "".join(result)


def translate_sqlite_sql_to_postgres(sql):
    safe_sql = str(sql or "").strip()
    if not safe_sql:
        return safe_sql
    if re.search(r"\bsqlite_sequence\b", safe_sql, flags=re.IGNORECASE):
        return None

    translated = safe_sql
    translated = re.sub(
        r"(?i)\b([a-zA-Z_][a-zA-Z0-9_\.]*)\s+COLLATE\s+NOCASE\b",
        r"LOWER(\1)",
        translated,
    )
    translated = _replace_qmark_placeholders(translated)
    return translated


def connect_database(settings, *, auth_mode=False):
    safe_settings = settings if isinstance(settings, DatabaseSettings) else resolve_database_settings(settings)
    if safe_settings.backend == "sqlite":
        directory = os.path.dirname(safe_settings.sqlite_db_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        conn = sqlite3.connect(safe_settings.sqlite_db_path, timeout=3.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 3000")
        if not auth_mode:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    if psycopg is None or psycopg_dict_row is None:
        raise RuntimeError(
            "PostgreSQL configurado, mas a dependencia psycopg nao esta instalada. "
            "Instale requirements.txt antes de usar db_backend=postgres."
        )

    raw_connection = psycopg.connect(
        safe_settings.database_url,
        autocommit=False,
        row_factory=psycopg_dict_row,
    )
    return PostgresCompatConnection(raw_connection)
