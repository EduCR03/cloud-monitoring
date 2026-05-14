#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
POSTGRES_PUBLIC_PORT="${POSTGRES_PUBLIC_PORT:-55432}"
POSTGRES_DB="${POSTGRES_DB:-cloudv2}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
DATABASE_URL="postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:${POSTGRES_PUBLIC_PORT}/${POSTGRES_DB}"

cd "${REPO_ROOT}"

if ! docker version >/dev/null 2>&1; then
  echo "Docker nao esta disponivel. Inicie o Docker e rode novamente." >&2
  exit 1
fi

echo "Subindo PostgreSQL local..."
export POSTGRES_PUBLIC_PORT POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD
docker compose --profile postgres up -d postgres

echo "Aguardando PostgreSQL..."
export TEST_POSTGRES_URL="${DATABASE_URL}"
python - <<'PY'
import os
import time
import psycopg

deadline = time.time() + 90
while True:
    try:
        with psycopg.connect(os.environ["TEST_POSTGRES_URL"], autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        break
    except Exception:
        if time.time() >= deadline:
            raise
        time.sleep(2)
PY

echo "Rodando validacao PostgreSQL..."
python -m pytest -q \
  tests/test_postgres_smoke_integration.py \
  tests/test_postgres_compare_integration.py \
  tests/test_postgres_migration_inventory.py \
  tests/test_sqlite_to_postgres_migration_script.py \
  tests/test_validate_postgres_migration_script.py \
  tests/test_db_backend_config.py

echo "PostgreSQL local validado."
