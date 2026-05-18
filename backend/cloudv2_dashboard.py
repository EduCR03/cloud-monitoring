import json
import logging
import mimetypes
import os
import re
import shutil
import threading
import gzip
from http.cookies import SimpleCookie
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlparse

from backend.cloudv2_auth import (
    FIXED_ADMIN_EMAIL,
    PRIVACY_POLICY_VERSION,
    SESSION_COOKIE_NAME,
    SESSION_TTL_SEC,
    AuthService,
    InMemoryRateLimiter,
)
from backend.cloudv2_paths import DATA_SUBDIR, DEFAULT_WEB_DIR, LEGACY_WEB_DIRS, resolve_data_dir, resolve_web_dir


DASHBOARD_DIR = resolve_web_dir()
DATA_DIR = resolve_data_dir(DASHBOARD_DIR)
MAX_BULK_PIVOT_ACTIONS = 100


def _parse_csv_env(value):
    items = []
    raw_text = str(value or "")
    for chunk in raw_text.replace("\n", ",").split(","):
        normalized = str(chunk or "").strip()
        if not normalized:
            continue
        if normalized not in items:
            items.append(normalized)
    return items


def _normalize_cookie_samesite(value, fallback="Lax"):
    normalized = str(value or "").strip().lower()
    if normalized == "strict":
        return "Strict"
    if normalized == "none":
        return "None"
    if normalized == "lax":
        return "Lax"
    return str(fallback or "Lax")


def _normalize_bulk_pivot_ids(value, limit=MAX_BULK_PIVOT_ACTIONS):
    if not isinstance(value, list):
        raise ValueError("pivot_ids obrigatorio")

    try:
        safe_limit = max(1, int(limit or MAX_BULK_PIVOT_ACTIONS))
    except (TypeError, ValueError):
        safe_limit = MAX_BULK_PIVOT_ACTIONS

    normalized = []
    seen = set()
    for raw_item in value:
        pivot_id = str(raw_item or "").strip()
        if not pivot_id or pivot_id in seen:
            continue
        seen.add(pivot_id)
        normalized.append(pivot_id)

    if not normalized:
        raise ValueError("pivot_ids obrigatorio")
    if len(normalized) > safe_limit:
        raise ValueError(f"maximo de {safe_limit} pivot_ids por requisicao")
    return normalized


def _is_admin_auth_context(auth_context):
    current_user = (auth_context or {}).get("user") or {}
    current_role = str(current_user.get("role") or "user").strip().lower()
    return current_role == "admin"


def _data_dir_has_files(path):
    if not os.path.isdir(path):
        return False
    try:
        for name in os.listdir(path):
            if name in (".gitkeep",):
                continue
            if os.path.isfile(os.path.join(path, name)):
                return True
    except OSError:
        return False
    return False


def _seed_data_dir_from_legacy():
    normalized_current = os.path.normpath(DASHBOARD_DIR)

    for legacy_dir in LEGACY_WEB_DIRS:
        normalized_legacy = os.path.normpath(legacy_dir)
        if normalized_current == normalized_legacy:
            continue

        legacy_data_dir = os.path.join(legacy_dir, DATA_SUBDIR)
        if not os.path.isdir(legacy_data_dir):
            continue
        if _data_dir_has_files(DATA_DIR):
            return

        try:
            names = os.listdir(legacy_data_dir)
        except OSError:
            continue

        for name in names:
            source_path = os.path.join(legacy_data_dir, name)
            target_path = os.path.join(DATA_DIR, name)
            if not os.path.isfile(source_path):
                continue
            try:
                shutil.copy2(source_path, target_path)
            except OSError:
                continue


def ensure_dirs():
    os.makedirs(DASHBOARD_DIR, exist_ok=True)
    os.makedirs(DATA_DIR, exist_ok=True)
    _seed_data_dir_from_legacy()


def _sync_frontend_assets_to_dashboard_dir():
    source_dir = os.path.normpath(DEFAULT_WEB_DIR)
    target_dir = os.path.normpath(DASHBOARD_DIR)

    if source_dir == target_dir:
        return
    if not os.path.isdir(source_dir):
        return

    skipped_dirs = {DATA_SUBDIR, "__pycache__"}
    skipped_files = {".gitkeep", "vercel.json"}

    for dirpath, dirnames, filenames in os.walk(source_dir):
        dirnames[:] = [name for name in dirnames if name not in skipped_dirs]
        rel_dir = os.path.relpath(dirpath, source_dir)
        rel_dir = "" if rel_dir == "." else rel_dir

        for filename in filenames:
            if filename in skipped_files:
                continue

            source_path = os.path.join(dirpath, filename)
            target_path = os.path.join(target_dir, rel_dir, filename)
            target_parent = os.path.dirname(target_path)

            try:
                os.makedirs(target_parent, exist_ok=True)
            except OSError:
                continue

            try:
                if os.path.exists(target_path) and os.path.getsize(source_path) == os.path.getsize(target_path):
                    with open(source_path, "rb") as source_file:
                        source_bytes = source_file.read()
                    with open(target_path, "rb") as target_file:
                        if source_bytes == target_file.read():
                            continue
                shutil.copy2(source_path, target_path)
            except OSError:
                continue


def slugify(value):
    slug = re.sub(r"[^a-zA-Z0-9_-]+", "_", str(value)).strip("_")
    return slug or "pivot"


def write_text_atomic(path, content):
    temp = f"{path}.tmp"
    with open(temp, "w", encoding="utf-8") as file:
        file.write(content)
    os.replace(temp, path)


def write_json_atomic(path, data):
    temp = f"{path}.tmp"
    with open(temp, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)
    os.replace(temp, path)


def _default_index_html():
    return """<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>CloudV2 Monitor</title>
  <link rel="stylesheet" href="dashboard.css"/>
</head>
<body>
  <div id="app"></div>
  <script src="dashboard.js"></script>
</body>
</html>
"""


def _default_css():
    return """body{margin:0;font-family:Arial,sans-serif;background:#f6faf7;color:#153425}#app{padding:20px}"""


def _default_js():
    return """document.getElementById('app').textContent='Dashboard carregado.';"""


def generate_dashboard_assets(refresh_sec):
    ensure_dirs()
    _sync_frontend_assets_to_dashboard_dir()
    index_path = os.path.join(DASHBOARD_DIR, "index.html")
    css_path = os.path.join(DASHBOARD_DIR, "dashboard.css")
    js_path = os.path.join(DASHBOARD_DIR, "dashboard.js")

    if not os.path.exists(index_path):
        write_text_atomic(index_path, _default_index_html())
    if not os.path.exists(css_path):
        write_text_atomic(css_path, _default_css())
    if not os.path.exists(js_path):
        write_text_atomic(js_path, _default_js())

    write_json_atomic(
        os.path.join(DATA_DIR, "ui_config.json"),
        {"refresh_sec": max(1, int(refresh_sec))},
    )


def _build_handler(telemetry_store, reload_token_getter=None):
    auth_service = AuthService(
        db_path=telemetry_store.persistence.db_path,
        db_backend=getattr(telemetry_store.persistence, "db_backend", "sqlite"),
        database_url=getattr(telemetry_store.persistence, "database_url", ""),
        logger=logging.getLogger("cloudv2.auth"),
    )
    auth_seed_result = auth_service.ensure_fixed_admin_account()
    if not auth_seed_result.get("ok"):
        logging.getLogger("cloudv2.auth").warning(
            "Conta admin fixa nao foi inicializada: %s",
            str(auth_seed_result.get("error") or "desabilitada"),
        )
    rate_limiter = InMemoryRateLimiter()
    auth_blocked = object()
    config_session_lock = threading.Lock()
    config_session_requests = {}
    config_state_by_idp = {
        "02": "network_config",
        "03": "pivot_config",
        "04": "rush_config",
        "05": "sector_config",
        "22": "physical_barrier_config",
        "24": "reboot_config",
        "26": "virtual_barrier_config",
        "31": "comm_main_mode_config",
    }

    page_aliases = {
        "/": "index.html",
        "/mapa": "mapa.html",
        "/login": "login.html",
        "/register": "register.html",
        "/verify-email": "verify-email.html",
        "/forgot-password": "forgot-password.html",
        "/reset-password": "reset-password.html",
        "/privacy-policy": "privacy-policy.html",
    }
    public_get_paths = {
        "/login",
        "/register",
        "/verify-email",
        "/forgot-password",
        "/reset-password",
        "/privacy-policy",
        "/auth.css",
        "/auth.js",
        "/auth/verify",
    }
    public_post_paths = {
        "/auth/register",
        "/auth/login",
        "/auth/resend-verification",
        "/auth/forgot-password",
        "/auth/reset-password",
    }
    unverified_allowed_get = {
        "/verify-email",
        "/login",
        "/register",
        "/forgot-password",
        "/reset-password",
        "/privacy-policy",
        "/auth.css",
        "/auth.js",
        "/auth/me",
        "/auth/verify",
    }
    unverified_allowed_post = {
        "/auth/resend-verification",
        "/auth/logout",
        "/auth/forgot-password",
        "/auth/reset-password",
        "/account/delete",
    }
    rate_limit_rules = {
        "/auth/login": (8, 300),
        "/auth/resend-verification": (5, 600),
        "/auth/forgot-password": (5, 600),
    }
    auth_rate_limit_disabled = str(os.environ.get("AUTH_DISABLE_RATE_LIMIT", "")).strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )
    cookie_samesite = _normalize_cookie_samesite(
        os.environ.get("AUTH_COOKIE_SAMESITE", "Lax"),
        fallback="Lax",
    )
    cors_origins = _parse_csv_env(os.environ.get("CORS_ALLOWED_ORIGINS", ""))
    cors_allow_any_origin = "*" in cors_origins
    cors_allowed_origins = {origin for origin in cors_origins if origin != "*"}

    class DashboardHandler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=DASHBOARD_DIR, **kwargs)

        def _client_accepts_gzip(self):
            accepted = str(self.headers.get("Accept-Encoding", "") or "").lower()
            return "gzip" in accepted

        def _encode_response_body(self, body, allow_gzip=False):
            raw_body = body if isinstance(body, (bytes, bytearray)) else bytes(body or b"")
            headers = {}
            if allow_gzip and self._client_accepts_gzip() and len(raw_body) >= 1024:
                compressed = gzip.compress(raw_body, compresslevel=5)
                if len(compressed) < len(raw_body):
                    raw_body = compressed
                    headers["Content-Encoding"] = "gzip"
                    headers["Vary"] = "Accept-Encoding"
            return raw_body, headers

        def _write_json(self, status_code, payload, extra_headers=None, cookies=None):
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            body, encoding_headers = self._encode_response_body(body, allow_gzip=True)
            self.send_response(status_code)
            self._write_cors_headers()
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            for key, value in encoding_headers.items():
                self.send_header(str(key), str(value))
            if extra_headers:
                for key, value in extra_headers.items():
                    self.send_header(str(key), str(value))
            if cookies:
                for cookie_value in cookies:
                    self.send_header("Set-Cookie", str(cookie_value))
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_text(self, status_code, content_type, body_text):
            body = str(body_text or "").encode("utf-8")
            self.send_response(status_code)
            self._write_cors_headers()
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _redirect(self, location, cookies=None):
            self.send_response(302)
            self._write_cors_headers()
            self.send_header("Location", str(location))
            self.send_header("Cache-Control", "no-store")
            if cookies:
                for cookie_value in cookies:
                    self.send_header("Set-Cookie", str(cookie_value))
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _write_html_file(self, filename):
            path = os.path.join(DASHBOARD_DIR, str(filename or "").strip())
            if not os.path.isfile(path):
                self._write_text(404, "text/plain", "arquivo nao encontrado")
                return
            with open(path, "rb") as file:
                body = file.read()

            content_type, _ = mimetypes.guess_type(path)
            if not content_type:
                content_type = "text/html"
            if content_type.startswith("text/"):
                content_type = f"{content_type}; charset=utf-8"

            self.send_response(200)
            self._write_cors_headers()
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_html_headers(self, filename):
            path = os.path.join(DASHBOARD_DIR, str(filename or "").strip())
            if not os.path.isfile(path):
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            content_type, _ = mimetypes.guess_type(path)
            if not content_type:
                content_type = "text/html"
            if content_type.startswith("text/"):
                content_type = f"{content_type}; charset=utf-8"

            self.send_response(200)
            self._write_cors_headers()
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(int(os.path.getsize(path))))
            self.end_headers()

        def _read_json_body(self):
            content_length = int(self.headers.get("Content-Length", "0"))
            if content_length <= 0:
                return {}
            raw = self.rfile.read(content_length)
            if not raw:
                return {}
            return json.loads(raw.decode("utf-8"))

        def _is_api_path(self, path):
            normalized = str(path or "").strip()
            return (
                normalized.startswith("/api/")
                or normalized.startswith("/admin/")
                or normalized in ("/auth/me", "/account/export", "/account/delete")
            )

        def _is_public_path(self, path, method):
            safe_method = str(method or "").upper()
            safe_path = str(path or "").strip()
            if safe_method == "GET":
                return safe_path in public_get_paths
            if safe_method == "POST":
                return safe_path in public_post_paths
            return False

        def _is_allowed_for_unverified(self, path, method):
            safe_method = str(method or "").upper()
            safe_path = str(path or "").strip()
            if safe_method == "GET":
                return safe_path in unverified_allowed_get
            if safe_method == "POST":
                return safe_path in unverified_allowed_post
            return False

        def _resolve_cors_origin(self):
            origin = str(self.headers.get("Origin", "")).strip()
            if not origin:
                return ""
            if cors_allow_any_origin:
                return origin
            if origin in cors_allowed_origins:
                return origin
            return ""

        def _write_cors_headers(self, resolved_origin=None):
            origin = str(resolved_origin or self._resolve_cors_origin() or "").strip()
            if not origin:
                return
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Credentials", "true")
            self.send_header("Vary", "Origin")

        def _get_raw_session_token(self):
            raw_cookie = str(self.headers.get("Cookie", "") or "")
            if not raw_cookie.strip():
                return ""
            parsed = SimpleCookie()
            try:
                parsed.load(raw_cookie)
            except Exception:
                return ""
            item = parsed.get(SESSION_COOKIE_NAME)
            return str(item.value or "").strip() if item is not None else ""

        def _resolve_auth_context(self):
            cached = getattr(self, "_auth_context_cache", None)
            if cached is not None:
                return cached

            token = self._get_raw_session_token()
            context = auth_service.resolve_session(token, touch=True) if token else None
            self._auth_context_cache = context if context is not None else False
            return context

        def _request_is_secure(self):
            forced_secure = str(os.environ.get("AUTH_COOKIE_SECURE", "")).strip().lower() in ("1", "true", "yes", "on")
            if forced_secure:
                return True
            forwarded_proto = str(self.headers.get("X-Forwarded-Proto", "")).split(",")[0].strip().lower()
            if forwarded_proto == "https":
                return True
            return bool(getattr(self.connection, "cipher", None))

        def _build_session_cookie(self, session_token, max_age=None):
            ttl = int(max_age or SESSION_TTL_SEC)
            secure_cookie = self._request_is_secure() or (cookie_samesite == "None")
            parts = [
                f"{SESSION_COOKIE_NAME}={str(session_token or '').strip()}",
                "Path=/",
                "HttpOnly",
                f"SameSite={cookie_samesite}",
                f"Max-Age={max(0, ttl)}",
            ]
            if secure_cookie:
                parts.append("Secure")
            return "; ".join(parts)

        def _build_clear_session_cookie(self):
            secure_cookie = self._request_is_secure() or (cookie_samesite == "None")
            parts = [
                f"{SESSION_COOKIE_NAME}=",
                "Path=/",
                "HttpOnly",
                f"SameSite={cookie_samesite}",
                "Max-Age=0",
            ]
            if secure_cookie:
                parts.append("Secure")
            return "; ".join(parts)

        def _client_ip(self):
            forwarded_for = str(self.headers.get("X-Forwarded-For", "")).strip()
            if forwarded_for:
                first = forwarded_for.split(",")[0].strip()
                if first:
                    return first
            return str((self.client_address or ("", 0))[0] or "unknown").strip() or "unknown"

        def _request_base_url(self):
            forwarded_proto = str(self.headers.get("X-Forwarded-Proto", "")).split(",")[0].strip().lower()
            scheme = "https" if forwarded_proto == "https" else "http"
            if not forwarded_proto and self._request_is_secure():
                scheme = "https"

            forwarded_host = str(self.headers.get("X-Forwarded-Host", "")).split(",")[0].strip()
            host = forwarded_host or str(self.headers.get("Host", "")).strip()
            if not host:
                host = f"{self.server.server_name}:{self.server.server_port}"
            return f"{scheme}://{host}"

        def _respond_auth_required(self, path):
            if self._is_api_path(path):
                self._write_json(401, {"ok": False, "code": "auth_required", "redirect": "/login"})
                return
            self._redirect("/login")

        def _respond_email_not_verified(self, path):
            if self._is_api_path(path):
                self._write_json(403, {"ok": False, "code": "email_not_verified", "redirect": "/verify-email"})
                return
            self._redirect("/verify-email")

        def _enforce_auth(self, path, method):
            if self._is_public_path(path, method):
                return self._resolve_auth_context()

            context = self._resolve_auth_context()
            if not context:
                self._respond_auth_required(path)
                return auth_blocked

            return context

        def _can_delete_pivots(self, auth_context):
            current_user = (auth_context or {}).get("user") or {}
            current_role = str(current_user.get("role") or "user").strip().lower()
            current_email = str(current_user.get("email") or "").strip().lower()
            return current_role == "admin" and current_email == FIXED_ADMIN_EMAIL

        def _can_manage_expected_pivots(self, auth_context):
            return _is_admin_auth_context(auth_context)

        def _config_session_key(self, auth_context, pivot_id, idp):
            normalized_user = str((auth_context or {}).get("session_user_id") or "").strip()
            if not normalized_user:
                normalized_user = self._get_raw_session_token()
            normalized_pivot = str(pivot_id or "").strip()
            normalized_idp = str(idp or "03").strip().zfill(2)
            if not normalized_user or not normalized_pivot:
                return ""
            return f"{normalized_user}:{normalized_pivot}:{normalized_idp}"

        def _remember_config_request(self, auth_context, pivot_id, idp, request_ts):
            key = self._config_session_key(auth_context, pivot_id, idp)
            try:
                safe_ts = float(request_ts)
            except (TypeError, ValueError):
                safe_ts = 0.0
            if not key or safe_ts <= 0:
                return
            with config_session_lock:
                config_session_requests[key] = safe_ts

        def _has_config_response_for_request(self, auth_context, pivot_id, idp):
            normalized_idp = str(idp or "03").strip().zfill(2)
            state_key = config_state_by_idp.get(normalized_idp)
            if not state_key:
                return False
            key = self._config_session_key(auth_context, pivot_id, normalized_idp)
            with config_session_lock:
                request_ts = float(config_session_requests.get(key) or 0)
            if request_ts <= 0:
                return False
            snapshot = telemetry_store.get_pivot_snapshot(str(pivot_id or "").strip())
            summary = snapshot.get("summary") if isinstance(snapshot, dict) else {}
            config_state = summary.get(state_key) if isinstance(summary, dict) else {}
            if not isinstance(config_state, dict):
                return False
            try:
                response_ts = float(config_state.get("last_response_ts") or 0)
            except (TypeError, ValueError):
                response_ts = 0.0
            return response_ts + 0.001 >= request_ts and not bool(config_state.get("pending"))

        def _check_rate_limit(self, path):
            if auth_rate_limit_disabled:
                return True
            rule = rate_limit_rules.get(str(path or "").strip())
            if rule is None:
                return True
            limit, window_sec = rule
            allowed, retry_after = rate_limiter.allow(str(path or "").strip(), self._client_ip(), limit, window_sec)
            if allowed:
                return True
            self._write_json(
                429,
                {"ok": False, "code": "rate_limited", "message": "Muitas tentativas. Tente novamente em instantes."},
                extra_headers={"Retry-After": str(int(retry_after))},
            )
            return False

        def _handle_auth_get(self, path, query, auth_context):
            if path == "/auth/verify":
                token = (query.get("token") or [""])[0]
                result = auth_service.verify_email_token(token)
                if result.get("ok"):
                    self._redirect("/login?verified=1")
                    return True

                code = str(result.get("code") or "")
                if code == "token_expired":
                    self._redirect("/verify-email?status=expired")
                    return True
                if code == "token_used":
                    self._redirect("/login?verified=1")
                    return True
                self._redirect("/verify-email?status=invalid")
                return True

            if path == "/auth/me":
                if not auth_context:
                    self._write_json(401, {"ok": False, "authenticated": False, "redirect": "/login"})
                    return True
                self._write_json(
                    200,
                    {
                        "ok": True,
                        "authenticated": True,
                        "user": (auth_context or {}).get("user"),
                        "pivot_delete_allowed": self._can_delete_pivots(auth_context),
                        "privacy_policy_version": PRIVACY_POLICY_VERSION,
                    },
                )
                return True

            if path == "/api/user/pivot-table-columns":
                if not auth_context:
                    self._write_json(401, {"ok": False, "code": "auth_required", "message": "Autenticacao obrigatoria."})
                    return True

                user_id = (auth_context or {}).get("session_user_id")
                columns = auth_service.get_pivot_table_columns(user_id)
                self._write_json(
                    200,
                    {
                        "ok": True,
                        "columns": columns,
                    },
                )
                return True

            if path == "/admin/users":
                if not auth_context:
                    self._write_json(401, {"ok": False, "code": "auth_required", "message": "Autenticacao obrigatoria."})
                    return True

                current_user = (auth_context or {}).get("user") or {}
                current_role = str(current_user.get("role") or "user").strip().lower()
                if current_role != "admin":
                    self._write_json(403, {"ok": False, "code": "admin_required", "message": "Acesso restrito ao administrador."})
                    return True

                result = auth_service.list_users_for_admin((auth_context or {}).get("session_user_id"))
                status_code = 200 if result.get("ok") else 403
                self._write_json(status_code, result)
                return True

            if path == "/account/export":
                if not auth_context:
                    self._write_json(401, {"ok": False, "code": "auth_required", "redirect": "/login"})
                    return True
                export_payload = auth_service.export_account_data((auth_context or {}).get("session_user_id"))
                if export_payload is None:
                    self._write_json(404, {"ok": False, "error": "usuario nao encontrado"})
                    return True
                self._write_json(200, {"ok": True, "data": export_payload})
                return True

            return False

        def _handle_auth_post(self, path, auth_context):
            if path in ("/auth/login", "/auth/resend-verification", "/auth/forgot-password"):
                if not self._check_rate_limit(path):
                    return True

            if path == "/auth/register":
                if not auth_context:
                    self._write_json(401, {"ok": False, "code": "auth_required", "message": "Autenticacao obrigatoria."})
                    return True

                current_user = (auth_context or {}).get("user") or {}
                current_role = str(current_user.get("role") or "user").strip().lower()
                if current_role != "admin":
                    self._write_json(403, {"ok": False, "code": "admin_required", "message": "Apenas administradores podem criar contas."})
                    return True

                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"ok": False, "error": "json invalido"})
                    return True

                result = auth_service.register_user(
                    email=body.get("email"),
                    password=body.get("password"),
                    password_confirm=body.get("password_confirm"),
                    name=body.get("name"),
                    privacy_policy_accepted=bool(body.get("privacy_policy_accepted")),
                    request_base_url=self._request_base_url(),
                )
                status_code = 200 if result.get("ok") else 400
                if result.get("code") == "email_in_use":
                    status_code = 409
                self._write_json(status_code, result)
                return True

            if path == "/auth/login":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"ok": False, "error": "json invalido"})
                    return True

                result = auth_service.login_user(
                    email=body.get("email"),
                    password=body.get("password"),
                    ip_address=self._client_ip(),
                    user_agent=self.headers.get("User-Agent", ""),
                )
                if not result.get("ok"):
                    self._write_json(401, result)
                    return True

                session_token = result.get("session_token")
                payload = dict(result)
                payload.pop("session_token", None)
                payload["redirect"] = f"{self._request_base_url()}/index.html"
                self._write_json(
                    200,
                    payload,
                    cookies=[self._build_session_cookie(session_token, max_age=payload.get("session_ttl_sec"))],
                )
                return True

            if path == "/auth/logout":
                auth_service.logout_session(self._get_raw_session_token())
                self._write_json(
                    200,
                    {"ok": True, "message": "Sessao encerrada."},
                    cookies=[self._build_clear_session_cookie()],
                )
                return True

            if path == "/auth/resend-verification":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"ok": False, "error": "json invalido"})
                    return True

                user_id = None
                if auth_context and not bool(((auth_context or {}).get("user") or {}).get("email_verified")):
                    user_id = (auth_context or {}).get("session_user_id")

                result = auth_service.resend_verification(
                    email=body.get("email"),
                    user_id=user_id,
                    request_base_url=self._request_base_url(),
                )
                self._write_json(200, result)
                return True

            if path == "/auth/forgot-password":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"ok": False, "error": "json invalido"})
                    return True

                result = auth_service.forgot_password(
                    email=body.get("email"),
                    request_base_url=self._request_base_url(),
                )
                self._write_json(200, result)
                return True

            if path == "/auth/reset-password":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"ok": False, "error": "json invalido"})
                    return True

                result = auth_service.reset_password(
                    raw_token=body.get("token"),
                    new_password=body.get("password"),
                    confirm_password=body.get("password_confirm"),
                )
                status_code = 200 if result.get("ok") else 400
                if result.get("code") == "token_expired":
                    status_code = 410
                self._write_json(status_code, result, cookies=[self._build_clear_session_cookie()])
                return True

            if path == "/account/delete":
                if not auth_context:
                    self._write_json(401, {"ok": False, "code": "auth_required", "redirect": "/login"})
                    return True
                current_user = (auth_context or {}).get("user") or {}
                current_role = str(current_user.get("role") or "user").strip().lower()
                if current_role == "admin":
                    self._write_json(
                        403,
                        {"ok": False, "code": "admin_protected", "message": "Conta de administrador nao pode ser excluida."},
                    )
                    return True
                deleted = auth_service.delete_account((auth_context or {}).get("session_user_id"))
                auth_service.logout_session(self._get_raw_session_token())
                if not deleted:
                    self._write_json(
                        404,
                        {"ok": False, "error": "conta nao encontrada"},
                        cookies=[self._build_clear_session_cookie()],
                    )
                    return True
                self._write_json(
                    200,
                    {"ok": True, "message": "Conta excluida com sucesso."},
                    cookies=[self._build_clear_session_cookie()],
                )
                return True

            if path == "/admin/users/delete":
                if not auth_context:
                    self._write_json(401, {"ok": False, "code": "auth_required", "message": "Autenticacao obrigatoria."})
                    return True

                current_user = (auth_context or {}).get("user") or {}
                current_role = str(current_user.get("role") or "user").strip().lower()
                if current_role != "admin":
                    self._write_json(403, {"ok": False, "code": "admin_required", "message": "Acesso restrito ao administrador."})
                    return True

                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"ok": False, "error": "json invalido"})
                    return True

                result = auth_service.admin_delete_user(
                    admin_user_id=(auth_context or {}).get("session_user_id"),
                    target_user_id=body.get("user_id"),
                )
                status_code = 200 if result.get("ok") else 400
                if result.get("code") == "admin_required":
                    status_code = 403
                if result.get("code") == "not_found":
                    status_code = 404
                self._write_json(status_code, result)
                return True

            return False

        def do_OPTIONS(self):
            parsed = urlparse(self.path)
            path = parsed.path

            allows_cors = (
                self._is_api_path(path)
                or self._is_public_path(path, "GET")
                or self._is_public_path(path, "POST")
                or self._is_allowed_for_unverified(path, "GET")
                or self._is_allowed_for_unverified(path, "POST")
            )
            if not allows_cors:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            origin = self._resolve_cors_origin()
            if not origin:
                self.send_response(403)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            self.send_response(204)
            self._write_cors_headers(origin)
            self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,OPTIONS")
            requested_headers = str(self.headers.get("Access-Control-Request-Headers", "")).strip()
            if requested_headers:
                self.send_header("Access-Control-Allow-Headers", requested_headers)
                self.send_header("Vary", "Access-Control-Request-Headers")
            else:
                self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.send_header("Access-Control-Max-Age", "600")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_HEAD(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path in ("/mapa", "/login", "/register", "/verify-email", "/forgot-password", "/reset-password", "/privacy-policy"):
                alias_file = page_aliases.get(path)
                if alias_file:
                    self._write_html_headers(alias_file)
                    return

            super().do_HEAD()

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path
            query = parse_qs(parsed.query or "")

            auth_context = self._enforce_auth(path, "GET")
            if auth_context is auth_blocked:
                return

            if path == "/login":
                if auth_context:
                    self._redirect("/index.html")
                    return

            if path == "/register":
                if not auth_context:
                    self._redirect("/login")
                    return
                current_user = (auth_context or {}).get("user") or {}
                current_role = str(current_user.get("role") or "user").strip().lower()
                if current_role != "admin":
                    self._redirect("/index.html")
                    return

            if self._handle_auth_get(path, query, auth_context):
                return

            if path == "/api/health":
                self._write_json(200, {"ok": True})
                return

            if path == "/api/state":
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                payload = telemetry_store.get_state_snapshot(run_id=run_id)
                selected_run_id = None
                if isinstance(payload, dict):
                    selected_run_id = str(payload.get("run_id") or "").strip() or run_id
                filter_options = telemetry_store.get_cloud2_filter_options(run_id=selected_run_id)
                if isinstance(payload, dict):
                    payload["cloud2_filter_options"] = {
                        "run_id": str(filter_options.get("run_id") or "").strip() or None,
                        "technologies": list(filter_options.get("technologies") or []),
                        "firmwares": list(filter_options.get("firmwares") or []),
                    }
                self._write_json(200, payload)
                return

            if path == "/api/quality-lite":
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                pivot_ids_raw = (query.get("pivot_ids") or [None])[0]
                pivot_ids = None
                if isinstance(pivot_ids_raw, str) and pivot_ids_raw.strip():
                    pivot_ids = _normalize_bulk_pivot_ids(pivot_ids_raw.split(","), limit=500)
                payload = telemetry_store.get_quality_cards_snapshot(run_id=run_id, pivot_ids=pivot_ids)
                self._write_json(200, payload)
                return

            if path == "/api/summary/cards-history":
                payload = telemetry_store.get_summary_cards_history_snapshot()
                self._write_json(200, payload)
                return

            if path == "/api/summary/connected-history":
                payload = telemetry_store.get_summary_cards_history_snapshot()
                self._write_json(200, payload)
                return

            if path == "/api/monitoring/runs":
                limit_raw = (query.get("limit") or [None])[0]
                try:
                    limit = int(limit_raw) if limit_raw is not None else 200
                except (TypeError, ValueError):
                    limit = 200
                runs = telemetry_store.list_monitoring_runs(limit=limit)
                self._write_json(
                    200,
                    {
                        "runs": runs,
                    },
                )
                return

            if path.startswith("/api/pivot/") and path.endswith("/sessions"):
                pivot_id = unquote(path[len("/api/pivot/") : -len("/sessions")]).strip("/").strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id invalido"})
                    return
                limit_raw = (query.get("limit") or [None])[0]
                try:
                    limit = int(limit_raw) if limit_raw is not None else 200
                except (TypeError, ValueError):
                    limit = 200
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                sessions = telemetry_store.list_monitoring_sessions(pivot_id, limit=limit, run_id=run_id)
                self._write_json(
                    200,
                    {
                        "pivot_id": pivot_id,
                        "run_id": run_id,
                        "sessions": sessions,
                    },
                )
                return

            if path.startswith("/api/pivot/") and path.endswith("/panel"):
                pivot_id = unquote(path[len("/api/pivot/") : -len("/panel")]).strip("/").strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id invalido"})
                    return
                session_id = (query.get("session_id") or [None])[0]
                if isinstance(session_id, str):
                    session_id = session_id.strip() or None
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                payload = telemetry_store.get_complete_panel(pivot_id, session_id=session_id, run_id=run_id)
                if payload is None:
                    self._write_json(404, {"error": "pivot nao encontrado"})
                    return
                self._write_json(200, payload)
                return

            if path.startswith("/api/pivot/"):
                pivot_id = unquote(path[len("/api/pivot/") :]).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id invalido"})
                    return
                session_id = (query.get("session_id") or [None])[0]
                if isinstance(session_id, str):
                    session_id = session_id.strip() or None
                run_id = (query.get("run_id") or [None])[0]
                if isinstance(run_id, str):
                    run_id = run_id.strip() or None
                payload = telemetry_store.get_pivot_snapshot(pivot_id, session_id=session_id, run_id=run_id)
                if payload is None:
                    self._write_json(404, {"error": "pivot nao encontrado"})
                    return
                self._write_json(200, payload)
                return

            if path == "/api/probe-config":
                payload = telemetry_store.get_probe_config_snapshot()
                self._write_json(200, payload)
                return

            if path == "/api/dev/reload-token":
                token = ""
                if callable(reload_token_getter):
                    token = str(reload_token_getter() or "")
                self._write_json(200, {"token": token})
                return

            alias_file = page_aliases.get(path)
            if alias_file:
                self._write_html_file(alias_file)
                return

            super().do_GET()

        def do_POST(self):
            parsed = urlparse(self.path)
            path = parsed.path

            auth_context = self._enforce_auth(path, "POST")
            if auth_context is auth_blocked:
                return

            if self._handle_auth_post(path, auth_context):
                return

            if path == "/api/admin/purge-database":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                password = body.get("password")
                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    result = telemetry_store.purge_database_records(password=password, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "result": result})
                return

            if path == "/api/monitoring/runs":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    created = telemetry_store.start_new_monitoring_run(source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "created": created})
                return

            if path == "/api/monitoring/history":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                run_id = str(body.get("run_id") or "").strip()
                if not run_id:
                    self._write_json(400, {"error": "run_id obrigatorio"})
                    return
                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    activated = telemetry_store.activate_history_run(run_id=run_id, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "activated": activated})
                return

            if path.startswith("/api/pivot/") and path.endswith("/sessions"):
                pivot_id = unquote(path[len("/api/pivot/") : -len("/sessions")]).strip("/").strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return
                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    created = telemetry_store.start_new_monitoring_session(pivot_id, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "created": created})
                return

            if path == "/api/pivots/expected":
                if not self._can_manage_expected_pivots(auth_context):
                    self._write_json(
                        403,
                        {
                            "ok": False,
                            "code": "admin_required",
                            "message": "Apenas administradores podem configurar novos pivos.",
                        },
                    )
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return
                try:
                    pivot_ids = _normalize_bulk_pivot_ids(body.get("pivot_ids"))
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return

                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    result = telemetry_store.queue_expected_pivots(pivot_ids, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return

                self._write_json(200, result)
                return

            if path == "/api/pivots/expected/remove":
                if not self._can_manage_expected_pivots(auth_context):
                    self._write_json(
                        403,
                        {
                            "ok": False,
                            "code": "admin_required",
                            "message": "Apenas administradores podem configurar novos pivos.",
                        },
                    )
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                pivot_id = str(body.get("pivot_id", "")).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return

                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    result = telemetry_store.remove_expected_pivot(pivot_id, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return

                self._write_json(200, result)
                return

            if path == "/api/pivots/delete":
                if not self._can_delete_pivots(auth_context):
                    self._write_json(
                        403,
                        {
                            "ok": False,
                            "code": "fixed_admin_required",
                            "message": "Apenas o administrador principal pode deletar pivos.",
                        },
                    )
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return
                try:
                    pivot_ids = _normalize_bulk_pivot_ids(body.get("pivot_ids"))
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return

                source = str(body.get("source", "ui")).strip() or "ui"
                results = []
                success_count = 0
                for pivot_id in pivot_ids:
                    try:
                        deleted = telemetry_store.delete_pivot(pivot_id, source=source)
                        results.append({"ok": True, "pivot_id": pivot_id, "deleted": deleted})
                        success_count += 1
                    except ValueError as exc:
                        results.append({"ok": False, "pivot_id": pivot_id, "error": str(exc)})

                error_count = len(results) - success_count
                self._write_json(
                    200,
                    {
                        "ok": error_count == 0,
                        "partial": success_count > 0 and error_count > 0,
                        "success_count": success_count,
                        "error_count": error_count,
                        "results": results,
                    },
                )
                return

            if path.startswith("/api/pivot/") and path.endswith("/delete"):
                pivot_id = unquote(path[len("/api/pivot/") : -len("/delete")]).strip("/").strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return
                if not self._can_delete_pivots(auth_context):
                    self._write_json(
                        403,
                        {
                            "ok": False,
                            "code": "fixed_admin_required",
                            "message": "Apenas o administrador principal pode deletar pivos.",
                        },
                    )
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    body = {}
                source = str(body.get("source", "ui")).strip() or "ui"
                try:
                    deleted = telemetry_store.delete_pivot(pivot_id, source=source)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                self._write_json(200, {"ok": True, "deleted": deleted})
                return

            if path == "/api/pivot-technology":
                if not self._can_delete_pivots(auth_context):
                    self._write_json(
                        403,
                        {
                            "ok": False,
                            "code": "fixed_admin_required",
                            "message": "Apenas o administrador principal pode alterar a tecnologia do pivo.",
                        },
                    )
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                pivot_id = str(body.get("pivot_id", "")).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return

                is_concentrator = bool(body.get("is_concentrator", False))
                try:
                    updated = telemetry_store.update_pivot_concentrator(
                        pivot_id,
                        is_concentrator=is_concentrator,
                    )
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return

                self._write_json(200, {"ok": True, "updated": updated})
                return

            if path == "/api/pivot-location":
                if not self._can_delete_pivots(auth_context):
                    self._write_json(
                        403,
                        {
                            "ok": False,
                            "code": "fixed_admin_required",
                            "message": "Apenas o administrador principal pode alterar a localizacao do pivo.",
                        },
                    )
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                pivot_id = str(body.get("pivot_id", "")).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return

                try:
                    updated = telemetry_store.update_pivot_coordinates(
                        pivot_id,
                        latitude=body.get("latitude"),
                        longitude=body.get("longitude"),
                    )
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return

                self._write_json(200, {"ok": True, "updated": updated})
                return

            if path == "/api/pivots/reset-modem":
                if not self._can_delete_pivots(auth_context):
                    self._write_json(
                        403,
                        {
                            "ok": False,
                            "code": "fixed_admin_required",
                            "message": "Apenas o administrador principal pode resetar varios pivos.",
                        },
                    )
                    return
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return
                try:
                    pivot_ids = _normalize_bulk_pivot_ids(body.get("pivot_ids"))
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return

                results = []
                success_count = 0
                for pivot_id in pivot_ids:
                    try:
                        result = telemetry_store.send_modem_reset_command(pivot_id)
                        results.append({"ok": True, "pivot_id": pivot_id, "reset_command": result})
                        success_count += 1
                    except (ValueError, RuntimeError) as exc:
                        results.append({"ok": False, "pivot_id": pivot_id, "error": str(exc)})

                error_count = len(results) - success_count
                self._write_json(
                    200,
                    {
                        "ok": error_count == 0,
                        "partial": success_count > 0 and error_count > 0,
                        "success_count": success_count,
                        "error_count": error_count,
                        "results": results,
                    },
                )
                return

            if path == "/api/pivot-reset-modem":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                pivot_id = str(body.get("pivot_id", "")).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return

                try:
                    result = telemetry_store.send_modem_reset_command(pivot_id)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                except RuntimeError as exc:
                    self._write_json(503, {"error": str(exc)})
                    return

                self._write_json(200, {"ok": True, "reset_command": result})
                return

            if path == "/api/pivot-config-request":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                pivot_id = str(body.get("pivot_id", "")).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return

                idp = str(body.get("idp") or "03").strip()
                try:
                    result = telemetry_store.send_config_request(pivot_id, idp=idp)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                except RuntimeError as exc:
                    self._write_json(503, {"error": str(exc)})
                    return

                self._remember_config_request(auth_context, pivot_id, result.get("idp") or idp, result.get("request_ts"))
                self._write_json(200, {"ok": True, "request": result})
                return

            if path == "/api/pivot-config-send":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"error": "json invalido"})
                    return

                pivot_id = str(body.get("pivot_id", "")).strip()
                if not pivot_id:
                    self._write_json(400, {"error": "pivot_id obrigatorio"})
                    return

                idp = str(body.get("idp") or "03").strip()
                if not self._has_config_response_for_request(auth_context, pivot_id, idp):
                    self._write_json(
                        409,
                        {
                            "ok": False,
                            "code": "config_request_required",
                            "message": "Peca a configuracao e aguarde a resposta antes de configurar.",
                        },
                    )
                    return
                values = body.get("values") if isinstance(body.get("values"), dict) else {}
                try:
                    result = telemetry_store.send_config_update(pivot_id, idp=idp, values=values)
                except ValueError as exc:
                    self._write_json(400, {"error": str(exc)})
                    return
                except RuntimeError as exc:
                    self._write_json(503, {"error": str(exc)})
                    return

                self._write_json(200, {"ok": True, "command": result})
                return

            if path != "/api/probe-config":
                self._write_json(404, {"error": "rota nao encontrada"})
                return

            try:
                body = self._read_json_body()
            except json.JSONDecodeError:
                self._write_json(400, {"error": "json invalido"})
                return

            pivot_id = str(body.get("pivot_id", "")).strip()
            if not pivot_id:
                self._write_json(400, {"error": "pivot_id obrigatorio"})
                return

            enabled = bool(body.get("enabled", False))
            interval_sec = body.get("interval_sec")
            if interval_sec is None:
                interval_sec = telemetry_store.probe_default_interval_sec

            try:
                updated = telemetry_store.update_probe_setting(pivot_id, enabled, interval_sec)
            except ValueError as exc:
                self._write_json(400, {"error": str(exc)})
                return

            self._write_json(200, {"ok": True, "updated": updated})

        def do_PUT(self):
            parsed = urlparse(self.path)
            path = parsed.path

            auth_context = self._enforce_auth(path, "PUT")
            if auth_context is auth_blocked:
                return

            if path == "/api/user/pivot-table-columns":
                try:
                    body = self._read_json_body()
                except json.JSONDecodeError:
                    self._write_json(400, {"ok": False, "error": "json invalido"})
                    return

                result = auth_service.set_pivot_table_columns(
                    (auth_context or {}).get("session_user_id"),
                    body.get("columns"),
                )
                status_code = 200 if result.get("ok") else 400
                if result.get("code") in ("auth_required", "user_not_found"):
                    status_code = 401
                self._write_json(status_code, result)
                return

            self._write_json(404, {"error": "rota nao encontrada"})
            return

        def log_message(self, format_text, *args):
            return

    return DashboardHandler


def start_dashboard_server(port, telemetry_store, reload_token_getter=None, host="127.0.0.1"):
    ensure_dirs()
    handler = _build_handler(telemetry_store, reload_token_getter=reload_token_getter)
    server = ThreadingHTTPServer((str(host), int(port)), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server
