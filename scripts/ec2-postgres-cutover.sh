#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/cloud-monitoring}"
BRANCH="${BRANCH:-dev}"
SQLITE_PATH="${SQLITE_PATH:-/data/telemetry.sqlite3}"
SAMPLE_LIMIT="${SAMPLE_LIMIT:-8}"
APPLY_CUTOVER="${APPLY_CUTOVER:-0}"
ROLLBACK_ONLY="${ROLLBACK_ONLY:-0}"
REPORT_PATH="${REPORT_PATH:-/data/postgres-compare-report.json}"
CUTOVER_ID="${CUTOVER_ID:-$(date -u +%Y%m%d%H%M%S)}"
BACKUP_DIR="${BACKUP_DIR:-/data/backups}"
SQLITE_BACKUP_PATH="${SQLITE_BACKUP_PATH:-${BACKUP_DIR}/telemetry.${CUTOVER_ID}.sqlite3}"
SQLITE_BACKUP_SHA256_PATH="${SQLITE_BACKUP_SHA256_PATH:-${SQLITE_BACKUP_PATH}.sha256}"
HEALTHCHECK_RETRIES="${HEALTHCHECK_RETRIES:-30}"
HEALTHCHECK_SLEEP_SEC="${HEALTHCHECK_SLEEP_SEC:-2}"
REQUIRE_EMPTY_POSTGRES="${REQUIRE_EMPTY_POSTGRES:-0}"
RUN_HTTP_SMOKE="${RUN_HTTP_SMOKE:-1}"
SMOKE_BASE_URL="${SMOKE_BASE_URL:-http://127.0.0.1:8008}"
SMOKE_REPORT_JSON="${SMOKE_REPORT_JSON:-/data/postgres-http-smoke-report.json}"
MANIFEST_PATH="${MANIFEST_PATH:-/data/postgres-cutover-${CUTOVER_ID}.manifest.json}"
ENV_BACKUP=""
BACKEND_STOPPED=0
CUTOVER_DONE=0
CURRENT_COMMIT=""

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
  write_cutover_manifest "failed_restored" "Falha durante cutover; configuracao anterior restaurada." || true
}

trap 'restore_on_error' ERR

wait_for_backend_health() {
  local attempt
  attempt=1
  echo "Aguardando backend saudavel..."
  while [ "${attempt}" -le "${HEALTHCHECK_RETRIES}" ]; do
    if docker compose exec -T backend curl -fsS http://127.0.0.1:8008/api/health >/dev/null 2>&1; then
      echo "Backend saudavel."
      return 0
    fi
    sleep "${HEALTHCHECK_SLEEP_SEC}"
    attempt=$((attempt + 1))
  done

  echo "Backend nao respondeu healthcheck." >&2
  docker compose logs --tail 80 backend >&2 || true
  return 1
}

rollback_to_sqlite() {
  set_env_value "DB_BACKEND" "sqlite"
  set_env_value "SQLITE_DB_PATH" "${SQLITE_PATH}"
  docker compose up -d backend
  wait_for_backend_health
  docker compose ps backend
  echo "Rollback aplicado para SQLite."
}

create_sqlite_backup() {
  echo "Criando backup SQLite congelado..."
  docker compose run --rm --no-deps \
    -e DB_BACKEND=sqlite \
    -e SQLITE_PATH="${SQLITE_PATH}" \
    -e SQLITE_BACKUP_PATH="${SQLITE_BACKUP_PATH}" \
    -e SQLITE_BACKUP_SHA256_PATH="${SQLITE_BACKUP_SHA256_PATH}" \
    backend \
    sh -lc '
      set -eu
      if [ ! -f "${SQLITE_PATH}" ]; then
        echo "SQLite ausente: ${SQLITE_PATH}" >&2
        exit 1
      fi
      mkdir -p "$(dirname "${SQLITE_BACKUP_PATH}")"
      cp "${SQLITE_PATH}" "${SQLITE_BACKUP_PATH}"
      sha256sum "${SQLITE_BACKUP_PATH}" > "${SQLITE_BACKUP_SHA256_PATH}"
      ls -lh "${SQLITE_BACKUP_PATH}"
      cat "${SQLITE_BACKUP_SHA256_PATH}"
    '
}

run_postgres_preflight() {
  echo "Validando acesso PostgreSQL/RDS..."
  local extra_args=()
  if [ "${REQUIRE_EMPTY_POSTGRES}" = "1" ]; then
    extra_args+=(--require-empty)
  fi
  docker compose run --rm --no-deps \
    -e DATABASE_URL="${DATABASE_URL}" \
    backend \
    python scripts/postgres_preflight.py \
      --database-url "${DATABASE_URL}" \
      "${extra_args[@]}"
}

run_http_smoke() {
  if [ "${RUN_HTTP_SMOKE}" != "1" ]; then
    return 0
  fi
  echo "Rodando smoke HTTP read-only..."
  docker compose exec -T \
    -e SMOKE_BASE_URL="${SMOKE_BASE_URL}" \
    -e SMOKE_EMAIL="${SMOKE_EMAIL:-}" \
    -e SMOKE_PASSWORD="${SMOKE_PASSWORD:-}" \
    -e SMOKE_REPORT_JSON="${SMOKE_REPORT_JSON}" \
    backend \
    python scripts/http_smoke_check.py
}

write_cutover_manifest() {
  local status="$1"
  local note="${2:-}"
  local apply_flag=()
  local smoke_flag=()
  if [ "${APPLY_CUTOVER}" = "1" ]; then
    apply_flag+=(--apply-cutover)
  fi
  if [ "${RUN_HTTP_SMOKE}" = "1" ]; then
    smoke_flag+=(--run-http-smoke)
  fi
  docker compose run --rm --no-deps \
    -e DATABASE_URL="${DATABASE_URL:-}" \
    backend \
    python scripts/write_cutover_manifest.py \
      --path "${MANIFEST_PATH}" \
      --cutover-id "${CUTOVER_ID}" \
      --status "${status}" \
      --branch "${BRANCH}" \
      --commit "${CURRENT_COMMIT}" \
      --database-url "${DATABASE_URL:-}" \
      --sqlite-path "${SQLITE_PATH}" \
      --sqlite-backup-path "${SQLITE_BACKUP_PATH}" \
      --sqlite-backup-sha256-path "${SQLITE_BACKUP_SHA256_PATH}" \
      --env-backup-path "${ENV_BACKUP}" \
      --compare-report-path "${REPORT_PATH}" \
      --smoke-report-path "${SMOKE_REPORT_JSON}" \
      --note "${note}" \
      "${apply_flag[@]}" \
      "${smoke_flag[@]}"
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
CURRENT_COMMIT="$(git rev-parse --short HEAD)"

echo "Construindo imagem backend..."
docker compose build backend

run_postgres_preflight

ENV_BACKUP=".env.backend.postgres-cutover.$(date +%Y%m%d%H%M%S).bak"
cp .env.backend "${ENV_BACKUP}"

echo "Parando backend para congelar SQLite..."
docker compose stop backend || true
BACKEND_STOPPED=1

create_sqlite_backup

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
  wait_for_backend_health
  run_http_smoke
  write_cutover_manifest "validated_without_cutover" "Validacao executada sem alterar DB_BACKEND."
  BACKEND_STOPPED=0
  docker compose ps backend
  exit 0
fi

echo "Aplicando cutover para PostgreSQL..."
set_env_value "DB_BACKEND" "postgres"
set_env_value "DATABASE_URL" "${DATABASE_URL}"
set_env_value "SQLITE_DB_PATH" "${SQLITE_PATH}"

docker compose up -d backend
wait_for_backend_health
run_http_smoke
write_cutover_manifest "cutover_applied" "Backend iniciado com DB_BACKEND=postgres."
BACKEND_STOPPED=0
CUTOVER_DONE=1
docker compose ps backend

echo "Cutover PostgreSQL aplicado."
echo "Backup SQLite: ${SQLITE_BACKUP_PATH}"
echo "Manifesto: ${MANIFEST_PATH}"
echo "Rollback: ROLLBACK_ONLY=1 bash scripts/ec2-postgres-cutover.sh"
