#!/usr/bin/env bash
set -euo pipefail

# Prepara e valida HTTPS via Caddy para o backend cloud-monitoring.
#
# Este script NAO instala outro proxy e NAO sobrescreve o Caddy compartilhado.
# Ele gera o bloco Caddyfile correto e valida backend, HTTPS e CORS.
#
# Uso:
#   DOMAIN=back-cloud-monitor.duckdns.org bash scripts/ec2-setup-https.sh
# Ou:
#   bash scripts/ec2-setup-https.sh back-cloud-monitor.duckdns.org
#
# Variaveis opcionais:
#   BACKEND_UPSTREAM=http://127.0.0.1:8008
#   HEALTH_PATH=/login
#   FRONTEND_URL=https://cloud-monitoring.vercel.app
#   CADDY_CONFIG_PATH=/caminho/para/Caddyfile

DOMAIN="${DOMAIN:-${1:-}}"
BACKEND_UPSTREAM="${BACKEND_UPSTREAM:-http://127.0.0.1:8008}"
HEALTH_PATH="${HEALTH_PATH:-/login}"
FRONTEND_URL="${FRONTEND_URL:-https://cloud-monitoring.vercel.app}"
CADDY_CONFIG_PATH="${CADDY_CONFIG_PATH:-}"
GENERATED_CADDYFILE="${GENERATED_CADDYFILE:-Caddyfile.cloud-monitoring.generated}"

log() {
  printf '[ec2-caddy] %s\n' "$*"
}

fail() {
  printf '[ec2-caddy] ERRO: %s\n' "$*" >&2
  exit 1
}

require_cmd() {
  local cmd="$1"
  command -v "${cmd}" >/dev/null 2>&1 || fail "comando obrigatorio ausente: ${cmd}"
}

imds_get() {
  local path="$1"
  local token
  token="$(curl -fsS -m 2 -X PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" || true)"
  if [ -n "${token}" ]; then
    curl -fsS -m 2 -H "X-aws-ec2-metadata-token: ${token}" "http://169.254.169.254/${path}" || true
  else
    curl -fsS -m 2 "http://169.254.169.254/${path}" || true
  fi
}

if [ -z "${DOMAIN}" ]; then
  fail "defina DOMAIN (ex.: back-cloud-monitor.duckdns.org)."
fi

if [[ "${BACKEND_UPSTREAM}" == */ ]]; then
  BACKEND_UPSTREAM="${BACKEND_UPSTREAM%/}"
fi

if [[ "${HEALTH_PATH}" != /* ]]; then
  HEALTH_PATH="/${HEALTH_PATH}"
fi

require_cmd curl
require_cmd awk
require_cmd getent

log "Validando resolucao DNS de ${DOMAIN}..."
DOMAIN_IP="$(getent ahostsv4 "${DOMAIN}" | awk 'NR==1 {print $1}')"
[ -n "${DOMAIN_IP}" ] || fail "nao foi possivel resolver ${DOMAIN}."
log "Dominio resolve para: ${DOMAIN_IP}"

PUBLIC_IP="$(imds_get latest/meta-data/public-ipv4)"
if [ -n "${PUBLIC_IP}" ] && [ "${DOMAIN_IP}" != "${PUBLIC_IP}" ]; then
  fail "o dominio ${DOMAIN} aponta para ${DOMAIN_IP}, mas esta EC2 e ${PUBLIC_IP}."
fi

log "Verificando backend local em ${BACKEND_UPSTREAM}${HEALTH_PATH}..."
if ! curl -fsS -m 8 "${BACKEND_UPSTREAM}${HEALTH_PATH}" >/dev/null; then
  fail "backend local nao respondeu. Suba o container antes: docker compose up -d --build backend"
fi
log "Backend local respondeu."

cat > "${GENERATED_CADDYFILE}" <<EOF
${DOMAIN} {
    encode zstd gzip

    reverse_proxy ${BACKEND_UPSTREAM} {
        header_up Host {host}
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
        header_up X-Forwarded-Host {host}
    }
}
EOF

log "Bloco Caddy gerado em ${GENERATED_CADDYFILE}."

if [ -n "${CADDY_CONFIG_PATH}" ]; then
  [ -f "${CADDY_CONFIG_PATH}" ] || fail "CADDY_CONFIG_PATH nao encontrado: ${CADDY_CONFIG_PATH}"
  if command -v caddy >/dev/null 2>&1; then
    log "Validando Caddyfile informado..."
    caddy validate --config "${CADDY_CONFIG_PATH}"
  else
    log "Comando caddy ausente neste shell. Validacao local ignorada."
  fi
fi

log "Verificando endpoint HTTPS publico..."
HTTPS_STATUS="$(curl -s -o /dev/null -w "%{http_code}" -m 12 "https://${DOMAIN}${HEALTH_PATH}" || true)"
if [ "${HTTPS_STATUS}" != "200" ]; then
  log "HTTPS ainda nao respondeu 200. Copie o bloco gerado para o Caddy compartilhado e recarregue."
else
  log "HTTPS OK."
fi

log "Verificando preflight CORS para ${FRONTEND_URL}..."
CORS_HEADERS="$(
  curl -sSI -X OPTIONS -m 12 "https://${DOMAIN}/auth/login" \
    -H "Origin: ${FRONTEND_URL}" \
    -H "Access-Control-Request-Method: POST" || true
)"

CORS_STATUS="$(printf '%s\n' "${CORS_HEADERS}" | awk 'toupper($1) ~ /^HTTP\// {code=$2} END {print code}')"
CORS_ALLOW_ORIGIN="$(printf '%s\n' "${CORS_HEADERS}" | awk -F': ' 'tolower($1)=="access-control-allow-origin" {gsub("\r","",$2); print $2; exit}')"
CORS_ALLOW_CREDENTIALS="$(printf '%s\n' "${CORS_HEADERS}" | awk -F': ' 'tolower($1)=="access-control-allow-credentials" {gsub("\r","",$2); print $2; exit}')"

if [ "${HTTPS_STATUS}" = "200" ]; then
  [ "${CORS_STATUS}" = "204" ] || [ "${CORS_STATUS}" = "200" ] || fail "preflight CORS falhou (status ${CORS_STATUS})."
  [ "${CORS_ALLOW_ORIGIN}" = "${FRONTEND_URL}" ] || fail "Access-Control-Allow-Origin (${CORS_ALLOW_ORIGIN}) difere de FRONTEND_URL (${FRONTEND_URL})."
  [ "${CORS_ALLOW_CREDENTIALS}" = "true" ] || fail "Access-Control-Allow-Credentials deve ser true."
  log "CORS OK."
fi

echo
echo "Proximos passos:"
echo "1) Copie ${GENERATED_CADDYFILE} para o Caddyfile compartilhado."
echo "2) Recarregue o Caddy."
echo "3) Configure no backend:"
echo "   CORS_ALLOWED_ORIGINS=${FRONTEND_URL}"
echo "   AUTH_COOKIE_SAMESITE=None"
echo "   AUTH_COOKIE_SECURE=1"
echo "   AUTH_BASE_URL=https://${DOMAIN}"
echo "4) Configure no frontend:"
echo "   window.CLOUDV2_API_BASE_URL = \"https://${DOMAIN}\";"
