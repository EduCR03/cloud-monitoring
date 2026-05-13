"""Database configuration and connection helpers for SQLite/PostgreSQL."""

from dataclasses import dataclass
import os
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

    return psycopg.connect(
        safe_settings.database_url,
        autocommit=False,
        row_factory=psycopg_dict_row,
    )
