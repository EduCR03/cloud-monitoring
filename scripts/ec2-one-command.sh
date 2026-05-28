#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${APP_DIR:-$HOME/cloud-monitoring}"
BRANCH="${BRANCH:-feat/aws-server}"
REPO_URL="${REPO_URL:-https://github.com/Monitoramento-de-Conectividade/cloud-monitoring.git}"

SANDBOX_PROXY_NETWORK="${SANDBOX_PROXY_NETWORK:-proxy-net}"
BROKER="${BROKER:-a19mijesri84u2-ats.iot.us-east-1.amazonaws.com}"
MQTT_PORT="${MQTT_PORT:-8883}"

# Runtime tuning defaults for small EC2 profiles.
DASHBOARD_REFRESH_SEC="${DASHBOARD_REFRESH_SEC:-20}"
API_STATE_CACHE_TTL_SEC="${API_STATE_CACHE_TTL_SEC:-5}"
API_QUALITY_CACHE_TTL_SEC="${API_QUALITY_CACHE_TTL_SEC:-5}"
MAX_EVENTS_PER_PIVOT="${MAX_EVENTS_PER_PIVOT:-800}"
MAX_EVENTS_PER_PIVOT_PANEL="${MAX_EVENTS_PER_PIVOT_PANEL:-800}"
MAX_EVENTS_PER_PIVOT_LIST="${MAX_EVENTS_PER_PIVOT_LIST:-400}"
PROBE_DEFAULT_INTERVAL_SEC="${PROBE_DEFAULT_INTERVAL_SEC:-900}"
PROBE_MIN_INTERVAL_SEC="${PROBE_MIN_INTERVAL_SEC:-180}"
DB_BACKEND="${DB_BACKEND:-sqlite}"
SQLITE_DB_PATH="${SQLITE_DB_PATH:-/data/telemetry.sqlite3}"
DATABASE_URL="${DATABASE_URL:-}"

FRONTEND_URL="${FRONTEND_URL:-https://SEU_FRONTEND.vercel.app}"
BACKEND_URL="${BACKEND_URL:-}"
ADMIN_EMAIL="${ADMIN_EMAIL:-}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-}"

if [ -z "${BACKEND_URL}" ]; then
  echo "Erro: defina BACKEND_URL (ex.: https://api.seu-dominio.com)." >&2
  exit 1
fi
if [ -z "${ADMIN_EMAIL}" ]; then
  echo "Erro: defina ADMIN_EMAIL." >&2
  exit 1
fi
if [ -z "${ADMIN_PASSWORD}" ]; then
  echo "Erro: defina ADMIN_PASSWORD." >&2
  exit 1
fi

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

mkdir -p certs logs_mqtt

cat > .env.backend <<EOF
SANDBOX_PROXY_NETWORK=${SANDBOX_PROXY_NETWORK}
BROKER=${BROKER}
MQTT_PORT=${MQTT_PORT}
CORS_ALLOWED_ORIGINS=${FRONTEND_URL}
AUTH_COOKIE_SAMESITE=None
AUTH_COOKIE_SECURE=1
AUTH_BASE_URL=${BACKEND_URL}
REQUIRE_APPLY_TO_START=0
HISTORY_MODE=merge
DB_BACKEND=${DB_BACKEND}
SQLITE_DB_PATH=${SQLITE_DB_PATH}
DATABASE_URL=${DATABASE_URL}
DASHBOARD_REFRESH_SEC=${DASHBOARD_REFRESH_SEC}
API_STATE_CACHE_TTL_SEC=${API_STATE_CACHE_TTL_SEC}
API_QUALITY_CACHE_TTL_SEC=${API_QUALITY_CACHE_TTL_SEC}
MAX_EVENTS_PER_PIVOT=${MAX_EVENTS_PER_PIVOT}
MAX_EVENTS_PER_PIVOT_PANEL=${MAX_EVENTS_PER_PIVOT_PANEL}
MAX_EVENTS_PER_PIVOT_LIST=${MAX_EVENTS_PER_PIVOT_LIST}
PROBE_DEFAULT_INTERVAL_SEC=${PROBE_DEFAULT_INTERVAL_SEC}
PROBE_MIN_INTERVAL_SEC=${PROBE_MIN_INTERVAL_SEC}
AUTH_FIXED_ADMIN_ENABLED=1
AUTH_FIXED_ADMIN_EMAIL=${ADMIN_EMAIL}
AUTH_FIXED_ADMIN_PASSWORD=${ADMIN_PASSWORD}
AUTH_EMAIL_MODE=console
EOF

if [ ! -f certs/amazon_ca.pem ] || [ ! -f certs/device.pem.crt ] || [ ! -f certs/private.pem.key ]; then
  echo "Certificados MQTT ausentes. Coloque os arquivos abaixo e rode o comando novamente:" >&2
  echo "  certs/amazon_ca.pem" >&2
  echo "  certs/device.pem.crt" >&2
  echo "  certs/private.pem.key" >&2
  exit 1
fi

export SANDBOX_PROXY_NETWORK

if ! docker network inspect "${SANDBOX_PROXY_NETWORK}" >/dev/null 2>&1; then
  docker network create "${SANDBOX_PROXY_NETWORK}"
fi

docker compose up -d --build backend
docker compose ps backend
docker logs --tail=80 cloud-monitoring-backend || true

echo
echo "Backend iniciado."
echo "Health interno: docker compose exec -T backend curl -I http://127.0.0.1:8008/login"
