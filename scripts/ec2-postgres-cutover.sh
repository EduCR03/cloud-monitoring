#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/cloud-monitoring}"
BRANCH="${BRANCH:-dev}"
SQLITE_PATH="${SQLITE_PATH:-/data/telemetry.sqlite3}"
SAMPLE_LIMIT="${SAMPLE_LIMIT:-8}"
APPLY_CUTOVER="${APPLY_CUTOVER:-0}"
ROLLBACK_ONLY="${ROLLBACK_ONLY:-0}"
REPORT_PATH="${REPORT_PATH:-/data/postgres-compare-report.json}"
ENV_BACKUP=""
BACKEND_STOPPED=0
CUTOVER_DONE=0

if [ ! -d "${APP_DIR}/.git" ]; then
  echo "Repositorio ausente em ${APP_DIR}." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker nao encontrado. Rode scripts/ec2-install-docker.sh primeiro." >&2
  exit 1
fi

cd "${APP_DIR}"

if [ ! -f .env.backend ]; then
  echo "Arquivo .env.backend nao encontrado." >&2
  exit 1
fi

set_env_value() {
  local key="$1"
  local value="$2"
  local tmp_file
  tmp_file="$(mktemp)"
  awk -v key="${key}" -v value="${value}" '
    BEGIN { found = 0 }
    index($0, key "=") == 1 {
      if (!found) {
        print key "=" value
        found = 1
      }
      next
    }
    { print }
    END {
      if (!found) {
        print key "=" value
      }
    }
  ' .env.backend > "${tmp_file}"
  mv "${tmp_file}" .env.backend
}

restore_on_error() {
  if [ "${CUTOVER_DONE}" = "1" ]; then
    return 0
  fi
  if [ -n "${ENV_BACKUP}" ] && [ -f "${ENV_BACKUP}" ]; then
    cp "${ENV_BACKUP}" .env.backend
    echo "Configuracao anterior restaurada."
  fi
  if [ "${BACKEND_STOPPED}" = "1" ]; then
    docker compose up -d backend || true
    echo "Backend reiniciado com configuracao anterior."
  fi
}

trap 'restore_on_error' ERR

rollback_to_sqlite() {
  set_env_value "DB_BACKEND" "sqlite"
  set_env_value "SQLITE_DB_PATH" "${SQLITE_PATH}"
  docker compose up -d backend
  docker compose ps backend
  echo "Rollback aplicado para SQLite."
}

if [ "${ROLLBACK_ONLY}" = "1" ]; then
  rollback_to_sqlite
  exit 0
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL obrigatorio." >&2
  echo "Exemplo: DATABASE_URL=postgresql://usuario:senha@host:5432/cloudv2 bash scripts/ec2-postgres-cutover.sh" >&2
  exit 1
fi

echo "Atualizando branch ${BRANCH}..."
git fetch origin
git checkout "${BRANCH}"
git pull --ff-only origin "${BRANCH}"

echo "Construindo imagem backend..."
docker compose build backend

ENV_BACKUP=".env.backend.postgres-cutover.$(date +%Y%m%d%H%M%S).bak"
cp .env.backend "${ENV_BACKUP}"

echo "Parando backend para congelar SQLite..."
docker compose stop backend || true
BACKEND_STOPPED=1

echo "Migrando e validando PostgreSQL em container isolado..."
docker compose run --rm --no-deps \
  -e DB_BACKEND=sqlite \
  -e SQLITE_DB_PATH="${SQLITE_PATH}" \
  -e DATABASE_URL="${DATABASE_URL}" \
  backend \
  python scripts/validate_postgres_migration.py \
    --sqlite-path "${SQLITE_PATH}" \
    --database-url "${DATABASE_URL}" \
    --migrate-first \
    --sample-limit "${SAMPLE_LIMIT}" \
    --report-json "${REPORT_PATH}"

if [ "${APPLY_CUTOVER}" != "1" ]; then
  echo "Validacao concluida. Cutover nao aplicado."
  echo "Para aplicar: APPLY_CUTOVER=1 DATABASE_URL=... bash scripts/ec2-postgres-cutover.sh"
  docker compose up -d backend
  BACKEND_STOPPED=0
  docker compose ps backend
  exit 0
fi

echo "Aplicando cutover para PostgreSQL..."
set_env_value "DB_BACKEND" "postgres"
set_env_value "DATABASE_URL" "${DATABASE_URL}"
set_env_value "SQLITE_DB_PATH" "${SQLITE_PATH}"

docker compose up -d backend
BACKEND_STOPPED=0
CUTOVER_DONE=1
docker compose ps backend

echo "Cutover PostgreSQL aplicado."
echo "Rollback: ROLLBACK_ONLY=1 bash scripts/ec2-postgres-cutover.sh"
