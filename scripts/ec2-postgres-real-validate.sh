#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/cloud-monitoring}"
BRANCH="${BRANCH:-dev}"
SQLITE_PATH="${SQLITE_PATH:-/data/telemetry.sqlite3}"
VALIDATION_MODE="${VALIDATION_MODE:-preflight}"
SAMPLE_LIMIT="${SAMPLE_LIMIT:-8}"
REQUIRE_EMPTY_POSTGRES="${REQUIRE_EMPTY_POSTGRES:-0}"
CONFIRM_TARGET_TRUNCATE="${CONFIRM_TARGET_TRUNCATE:-0}"
RUN_SMOKE_AFTER="${RUN_SMOKE_AFTER:-0}"
SMOKE_BASE_URL="${SMOKE_BASE_URL:-http://127.0.0.1:8008}"
VALIDATION_ID="${VALIDATION_ID:-$(date -u +%Y%m%d%H%M%S)}"
REPORT_PATH="${REPORT_PATH:-/data/postgres-real-validation-${VALIDATION_ID}.json}"
BACKEND_STOPPED=0

if [ ! -d "${APP_DIR}/.git" ]; then
  echo "Repositorio ausente em ${APP_DIR}." >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker nao encontrado. Rode scripts/ec2-install-docker.sh primeiro." >&2
  exit 1
fi

if [ -z "${DATABASE_URL:-}" ]; then
  echo "DATABASE_URL obrigatorio." >&2
  exit 1
fi

case "${VALIDATION_MODE}" in
  preflight|compare-existing|migrate-and-compare)
    ;;
  *)
    echo "VALIDATION_MODE invalido: ${VALIDATION_MODE}" >&2
    exit 1
    ;;
esac

if [ "${VALIDATION_MODE}" = "migrate-and-compare" ] && [ "${CONFIRM_TARGET_TRUNCATE}" != "1" ]; then
  echo "CONFIRM_TARGET_TRUNCATE=1 obrigatorio para migrate-and-compare." >&2
  exit 2
fi

cd "${APP_DIR}"

if [ ! -f .env.backend ]; then
  echo "Arquivo .env.backend nao encontrado." >&2
  exit 1
fi

restart_backend_if_needed() {
  if [ "${BACKEND_STOPPED}" = "1" ]; then
    docker compose up -d backend || true
    BACKEND_STOPPED=0
    wait_for_backend_health || true
  fi
}

trap 'restart_backend_if_needed' EXIT

wait_for_backend_health() {
  local attempt=1
  while [ "${attempt}" -le 30 ]; do
    if docker compose exec -T backend curl -fsS http://127.0.0.1:8008/api/health >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
    attempt=$((attempt + 1))
  done
  docker compose logs --tail 80 backend >&2 || true
  return 1
}

echo "Atualizando branch ${BRANCH}..."
git fetch origin
git checkout "${BRANCH}"
git pull --ff-only origin "${BRANCH}"

echo "Construindo imagem backend..."
docker compose build backend

extra_args=()
if [ "${REQUIRE_EMPTY_POSTGRES}" = "1" ]; then
  extra_args+=(--require-empty)
fi
if [ "${CONFIRM_TARGET_TRUNCATE}" = "1" ]; then
  extra_args+=(--confirm-target-truncate)
fi

if [ "${VALIDATION_MODE}" = "migrate-and-compare" ]; then
  echo "Parando backend para congelar SQLite durante migracao comparativa..."
  docker compose stop backend || true
  BACKEND_STOPPED=1
fi

echo "Executando validacao real PostgreSQL (${VALIDATION_MODE})..."
docker compose run --rm --no-deps \
  -e DATABASE_URL="${DATABASE_URL}" \
  backend \
  python scripts/postgres_real_validation.py \
    --sqlite-path "${SQLITE_PATH}" \
    --database-url "${DATABASE_URL}" \
    --mode "${VALIDATION_MODE}" \
    --sample-limit "${SAMPLE_LIMIT}" \
    --report-json "${REPORT_PATH}" \
    "${extra_args[@]}"

restart_backend_if_needed

if [ "${RUN_SMOKE_AFTER}" = "1" ]; then
  echo "Rodando smoke HTTP apos validacao real..."
  docker compose exec -T \
    -e SMOKE_BASE_URL="${SMOKE_BASE_URL}" \
    -e SMOKE_EMAIL="${SMOKE_EMAIL:-}" \
    -e SMOKE_PASSWORD="${SMOKE_PASSWORD:-}" \
    -e SMOKE_REPORT_JSON="${SMOKE_REPORT_JSON:-/data/postgres-real-smoke-${VALIDATION_ID}.json}" \
    backend \
    python scripts/http_smoke_check.py
fi

echo "Validacao real concluida."
echo "Relatorio: ${REPORT_PATH}"
