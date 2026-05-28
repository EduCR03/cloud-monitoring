#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/cloud-monitoring}"
BRANCH="${BRANCH:-main}"
REPO_URL="${REPO_URL:-https://github.com/Monitoramento-de-Conectividade/cloud-monitoring.git}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker nao encontrado. Rode scripts/ec2-install-docker.sh primeiro." >&2
  exit 1
fi

if [ ! -d "${APP_DIR}/.git" ]; then
  mkdir -p "${APP_DIR}"
  git clone "${REPO_URL}" "${APP_DIR}"
fi

cd "${APP_DIR}"
git fetch origin

if git show-ref --verify --quiet "refs/heads/${BRANCH}"; then
  git checkout "${BRANCH}"
else
  git checkout -b "${BRANCH}" "origin/${BRANCH}"
fi

git pull --ff-only origin "${BRANCH}"

if [ ! -f .env.backend ]; then
  echo "Arquivo .env.backend nao encontrado em ${APP_DIR}." >&2
  echo "Crie com base em .env.backend.example antes de subir o container." >&2
  exit 1
fi

mkdir -p certs logs_mqtt

read_env_value() {
  local key="$1"
  if [ ! -f .env.backend ]; then
    return 0
  fi
  grep -E "^${key}=" .env.backend | tail -n1 | cut -d '=' -f2- | tr -d '\r' || true
}

SANDBOX_PROXY_NETWORK="${SANDBOX_PROXY_NETWORK:-$(read_env_value SANDBOX_PROXY_NETWORK)}"
SANDBOX_PROXY_NETWORK="${SANDBOX_PROXY_NETWORK:-proxy-net}"
export SANDBOX_PROXY_NETWORK

if ! docker network inspect "${SANDBOX_PROXY_NETWORK}" >/dev/null 2>&1; then
  docker network create "${SANDBOX_PROXY_NETWORK}"
fi

docker compose build backend
docker compose up -d backend
docker compose ps backend

reset_db_once_if_requested() {
  # One-shot reset scoped to feat/aws-server to avoid impacting other branches.
  if [ "${BRANCH}" != "feat/aws-server" ]; then
    return 0
  fi

  local db_backend="sqlite"
  if [ -f .env.backend ]; then
    db_backend="$(grep -E '^DB_BACKEND=' .env.backend | tail -n1 | cut -d '=' -f2- | tr -d '\r' || true)"
  fi
  if [ -n "${db_backend}" ] && [ "${db_backend}" != "sqlite" ]; then
    echo "Reset one-shot ignorado para DB_BACKEND=${db_backend}."
    return 0
  fi

  local marker_path="/data/.cloud_monitoring_reset_20260211_done"
  echo "Verificando reset one-shot de dados (schema preservado)..."

  docker exec cloud-monitoring-backend sh -lc "
set -euo pipefail

if [ -f '${marker_path}' ]; then
  echo 'Reset one-shot ja aplicado anteriormente.'
  exit 0
fi

python - <<'PY'
import sqlite3

db_path = '/data/telemetry.sqlite3'
tables = [
    'connectivity_events',
    'probe_events',
    'probe_delay_points',
    'cloud2_events',
    'drop_events',
    'pivot_snapshots',
    'monitoring_sessions',
    'monitoring_runs',
    'probe_settings',
    'pivots',
]

conn = sqlite3.connect(db_path)
with conn:
    for table in tables:
        try:
            conn.execute(f'DELETE FROM {table}')
        except sqlite3.Error:
            pass
    try:
        conn.execute(
            \"DELETE FROM sqlite_sequence WHERE name IN ('connectivity_events','probe_events','probe_delay_points','cloud2_events','drop_events','pivot_snapshots','monitoring_sessions','monitoring_runs','probe_settings','pivots')\"
        )
    except sqlite3.Error:
        pass

print('Dados de monitoramento removidos. Schema preservado.')
PY

touch '${marker_path}'
echo 'Marcador de reset criado em ${marker_path}'
"
}

reset_db_once_if_requested

echo "Deploy concluido."
