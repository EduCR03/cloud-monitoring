"""Authentication and account helpers for the dashboard server."""

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import smtplib
import threading
import time
import uuid
from email.message import EmailMessage
from urllib.parse import quote

from backend.cloudv2_db import connect_database, resolve_database_settings


def _env_int(name, default, minimum=1):
    raw_value = os.environ.get(name)
    try:
        parsed = int(raw_value) if raw_value is not None else int(default)
    except (TypeError, ValueError):
        parsed = int(default)
    if parsed < int(minimum):
        return int(minimum)
    return parsed


SESSION_COOKIE_NAME = str(os.environ.get("AUTH_SESSION_COOKIE_NAME", "cloudv2_session")).strip() or "cloudv2_session"
SESSION_TTL_SEC = _env_int("AUTH_SESSION_TTL_SEC", 12 * 3600, minimum=300)
VERIFY_TOKEN_TTL_SEC = _env_int("AUTH_VERIFY_TOKEN_TTL_SEC", 24 * 3600, minimum=300)
RESET_TOKEN_TTL_SEC = _env_int("AUTH_RESET_TOKEN_TTL_SEC", 3600, minimum=300)
PASSWORD_MIN_LENGTH = _env_int("AUTH_PASSWORD_MIN_LENGTH", 8, minimum=8)
PRIVACY_POLICY_VERSION = str(os.environ.get("PRIVACY_POLICY_VERSION", "2026-02-10")).strip() or "2026-02-10"
TOKEN_PEPPER = str(os.environ.get("AUTH_TOKEN_PEPPER", "cloudv2-dev-token-pepper")).strip() or "cloudv2-dev-token-pepper"
FIXED_ADMIN_ENABLED = str(os.environ.get("AUTH_FIXED_ADMIN_ENABLED", "1")).strip().lower() in ("1", "true", "yes", "on")
FIXED_ADMIN_EMAIL = (
    str(os.environ.get("AUTH_FIXED_ADMIN_EMAIL", "eduardocostar03@gmail.com")).strip().lower()
    or "eduardocostar03@gmail.com"
)
FIXED_ADMIN_PASSWORD = str(os.environ.get("AUTH_FIXED_ADMIN_PASSWORD", "31380626ESP32")).strip() or "31380626ESP32"
FIXED_ADMIN_NAME = str(os.environ.get("AUTH_FIXED_ADMIN_NAME", "Administrador Global")).strip() or "Administrador Global"
FIXED_ADMIN_FORCE_PASSWORD = str(os.environ.get("AUTH_FIXED_ADMIN_FORCE_PASSWORD", "1")).strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)


def mask_email(email):
    value = str(email or "").strip()
    if "@" not in value:
        return "***"
    local, _, domain = value.partition("@")
    if not local:
        local_mask = "*"
    elif len(local) == 1:
        local_mask = local[0] + "*"
    elif len(local) == 2:
        local_mask = local[0] + "*"
    else:
        local_mask = f"{local[0]}{'*' * (len(local) - 2)}{local[-1]}"

    domain_parts = [part for part in domain.split(".") if part]
    if not domain_parts:
        return f"{local_mask}@***"

    masked_parts = []
    for part in domain_parts:
        if len(part) <= 2:
            masked_parts.append(part[0] + "*")
        else:
            masked_parts.append(f"{part[0]}{'*' * (len(part) - 2)}{part[-1]}")
    return f"{local_mask}@{'.'.join(masked_parts)}"


def _now_ts():
    return float(time.time())


class InMemoryRateLimiter:
    def __init__(self):
        self._lock = threading.Lock()
        self._history = {}

    def allow(self, scope, key, limit, window_sec):
        safe_scope = str(scope or "").strip() or "default"
        safe_key = str(key or "").strip() or "anonymous"
        safe_limit = max(1, int(limit or 1))
        safe_window = max(1, int(window_sec or 1))
        now = _now_ts()
        bucket_key = (safe_scope, safe_key)

        with self._lock:
            recent = [ts for ts in self._history.get(bucket_key, []) if ts >= (now - safe_window)]
            if len(recent) >= safe_limit:
                retry_after = max(1, int((recent[0] + safe_window) - now))
                self._history[bucket_key] = recent
                return False, retry_after

            recent.append(now)
            self._history[bucket_key] = recent

            if len(self._history) > 3000:
                oldest_cutoff = now - (safe_window * 3)
                for existing_key in list(self._history.keys()):
                    filtered = [ts for ts in self._history.get(existing_key, []) if ts >= oldest_cutoff]
                    if filtered:
                        self._history[existing_key] = filtered
                    else:
                        self._history.pop(existing_key, None)

        return True, 0


class AuthEmailService:
    def __init__(self, logger=None):
        self.logger = logger
        self.smtp_host = str(os.environ.get("AUTH_SMTP_HOST", "")).strip()
        self.smtp_port = _env_int("AUTH_SMTP_PORT", 587, minimum=1)
        self.smtp_user = str(os.environ.get("AUTH_SMTP_USER", "")).strip()
        self.smtp_password = str(os.environ.get("AUTH_SMTP_PASSWORD", "")).strip()
        self.smtp_from = str(os.environ.get("AUTH_SMTP_FROM", "")).strip()
        self.smtp_starttls = str(os.environ.get("AUTH_SMTP_STARTTLS", "1")).strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        self.smtp_ssl = str(os.environ.get("AUTH_SMTP_SSL", "0")).strip().lower() in (
            "1",
            "true",
            "yes",
            "on",
        )
        self.smtp_timeout_sec = _env_int("AUTH_SMTP_TIMEOUT_SEC", 20, minimum=5)
        self.smtp_host = self._resolve_smtp_host()
        if not self.smtp_from:
            self.smtp_from = self.smtp_user or "no-reply@cloud-monitor.local"
        self.mode = self._resolve_mode()

    def _resolve_mode(self):
        forced = str(os.environ.get("AUTH_EMAIL_MODE", "")).strip().lower()
        if forced in ("smtp", "console"):
            return forced
        if self.smtp_host:
            return "smtp"
        if self.smtp_user and self.smtp_password:
            return "smtp"
        return "console"

    def _resolve_smtp_host(self):
        explicit_host = str(self.smtp_host or "").strip()
        if explicit_host:
            return explicit_host
        lowered_user = str(self.smtp_user or "").strip().lower()
        if lowered_user.endswith("@gmail.com") or lowered_user.endswith("@googlemail.com"):
            return "smtp.gmail.com"
        if lowered_user.endswith("@outlook.com") or lowered_user.endswith("@hotmail.com") or lowered_user.endswith("@live.com"):
            return "smtp.office365.com"
        if lowered_user.endswith("@yahoo.com") or lowered_user.endswith("@yahoo.com.br"):
            return "smtp.mail.yahoo.com"
        return ""

    def _log_dev_link(self, email, label, link):
        masked = mask_email(email)
        print(f"[auth-email:{label}] to={masked} link={link}")

    def _send_smtp(self, email, subject, body):
        if not self.smtp_host:
            raise RuntimeError("AUTH_SMTP_HOST ausente para envio SMTP")
        if self.smtp_user and not self.smtp_password:
            raise RuntimeError("AUTH_SMTP_PASSWORD ausente para autenticar no SMTP")

        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self.smtp_from
        message["To"] = str(email).strip()
        message.set_content(body)

        if self.smtp_ssl:
            with smtplib.SMTP_SSL(self.smtp_host, self.smtp_port, timeout=self.smtp_timeout_sec) as server:
                if self.smtp_user:
                    server.login(self.smtp_user, self.smtp_password)
                server.send_message(message)
            return

        with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=self.smtp_timeout_sec) as server:
            server.ehlo()
            if self.smtp_starttls:
                server.starttls()
                server.ehlo()
            if self.smtp_user:
                server.login(self.smtp_user, self.smtp_password)
            server.send_message(message)

    def _log_email_error(self, email, label, error_text):
        if self.logger is not None:
            self.logger.warning(
                "Falha no envio de e-mail (%s) para %s: %s",
                str(label or "unknown"),
                mask_email(email),
                str(error_text or "erro desconhecido"),
            )

    def _normalize_error(self, exc):
        message = str(exc or "").strip()
        if not message:
            return "erro inesperado no envio de e-mail"
        if len(message) > 220:
            return message[:220]
        return message

    def send_verification_email(self, email, verification_link):
        subject = "Verificacao de e-mail - Cloud Monitoring"
        body = (
            "Ola,\n\n"
            "Para confirmar seu cadastro, use o link abaixo:\n"
            f"{verification_link}\n\n"
            f"Este link expira em {int(VERIFY_TOKEN_TTL_SEC / 60)} minuto(s).\n"
            "Se voce nao solicitou este cadastro, ignore esta mensagem.\n"
        )
        return self._dispatch_email(email, subject, body, "verify", verification_link)

    def send_reset_password_email(self, email, reset_link):
        subject = "Redefinicao de senha - Cloud Monitoring"
        body = (
            "Ola,\n\n"
            "Para redefinir sua senha, use o link abaixo:\n"
            f"{reset_link}\n\n"
            f"Este link expira em {int(RESET_TOKEN_TTL_SEC / 60)} minuto(s).\n"
            "Se voce nao solicitou redefinicao, ignore esta mensagem.\n"
        )
        return self._dispatch_email(email, subject, body, "reset", reset_link)

    def _dispatch_email(self, email, subject, body, label, fallback_link):
        if self.mode == "smtp":
            try:
                self._send_smtp(email, subject, body)
            except Exception as exc:
                normalized_error = self._normalize_error(exc)
                self._log_email_error(email, label, normalized_error)
                return {
                    "sent": False,
                    "mode": "smtp",
                    "error": normalized_error,
                }
            return {
                "sent": True,
                "mode": "smtp",
                "error": "",
            }
        self._log_dev_link(email, label, fallback_link)
        return {
            "sent": False,
            "mode": "console",
            "error": "AUTH_EMAIL_MODE=console ativo (sem envio real).",
        }


class AuthService:
    def __init__(self, db_path=None, db_backend="sqlite", database_url=None, logger=None, email_service=None):
        self.db_settings = resolve_database_settings(
            {
                "db_backend": db_backend,
                "sqlite_db_path": str(db_path or "").strip(),
                "database_url": database_url,
            }
        )
        self.db_path = self.db_settings.sqlite_db_path
        self.db_backend = self.db_settings.backend
        self.database_url = self.db_settings.database_url
        self.logger = logger
        self.email_service = email_service or AuthEmailService(logger=logger)

    def _connect(self):
        return connect_database(self.db_settings, auth_mode=True)

    def _normalize_email(self, email):
        return str(email or "").strip().lower()

    def _normalize_name(self, name):
        normalized = str(name or "").strip()
        if not normalized:
            return None
        if len(normalized) > 120:
            return normalized[:120]
        return normalized

    def _is_valid_email(self, email):
        if not email or len(email) > 254:
            return False
        return bool(re.match(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$", email))

    def _validate_password_strength(self, password):
        text = str(password or "")
        issues = []
        if len(text) < PASSWORD_MIN_LENGTH:
            issues.append(f"A senha deve ter no minimo {PASSWORD_MIN_LENGTH} caracteres.")
        return issues

    def _hash_password(self, password):
        plain = str(password or "")
        salt = secrets.token_bytes(16)
        n = 2**14
        r = 8
        p = 1
        digest = hashlib.scrypt(plain.encode("utf-8"), salt=salt, n=n, r=r, p=p, dklen=64)
        encoded_salt = base64.urlsafe_b64encode(salt).decode("ascii")
        encoded_digest = base64.urlsafe_b64encode(digest).decode("ascii")
        return f"scrypt${n}${r}${p}${encoded_salt}${encoded_digest}"

    def _verify_password(self, password, stored_hash):
        plain = str(password or "")
        encoded = str(stored_hash or "")
        try:
            algorithm, n_str, r_str, p_str, encoded_salt, encoded_digest = encoded.split("$", 5)
            if algorithm != "scrypt":
                return False
            n = int(n_str)
            r = int(r_str)
            p = int(p_str)
            salt = base64.urlsafe_b64decode(encoded_salt.encode("ascii"))
            expected_digest = base64.urlsafe_b64decode(encoded_digest.encode("ascii"))
            computed_digest = hashlib.scrypt(
                plain.encode("utf-8"),
                salt=salt,
                n=n,
                r=r,
                p=p,
                dklen=len(expected_digest),
            )
            return hmac.compare_digest(computed_digest, expected_digest)
        except Exception:
            return False

    def _hash_secret(self, raw_value, purpose):
        payload = f"{purpose}:{raw_value}:{TOKEN_PEPPER}".encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def _generate_token(self):
        return secrets.token_urlsafe(48)

    def _cleanup_expired_records(self, conn, now_ts):
        cutoff_tokens = float(now_ts) - (30 * 24 * 3600)
        cutoff_sessions = float(now_ts) - (30 * 24 * 3600)
        with conn:
            conn.execute(
                """
                DELETE FROM user_tokens
                WHERE expires_at < ?
                    AND (used_at IS NOT NULL OR expires_at < ?)
                """,
                (float(now_ts), cutoff_tokens),
            )
            conn.execute(
                """
                DELETE FROM user_sessions
                WHERE expires_at < ?
                    AND (revoked_at IS NOT NULL OR expires_at < ?)
                """,
                (float(now_ts), cutoff_sessions),
            )

    def _get_base_url(self, request_base_url):
        env_base = str(os.environ.get("AUTH_BASE_URL", "")).strip()
        if env_base:
            return env_base.rstrip("/")

        render_base = str(os.environ.get("RENDER_EXTERNAL_URL", "")).strip()
        if render_base:
            return render_base.rstrip("/")

        fallback = str(request_base_url or "").strip()
        if fallback:
            return fallback.rstrip("/")
        return "http://localhost:8008"

    def _build_verify_link(self, request_base_url, token):
        base = self._get_base_url(request_base_url)
        return f"{base}/auth/verify?token={quote(str(token or ''), safe='')}"

    def _build_reset_link(self, request_base_url, token):
        base = self._get_base_url(request_base_url)
        return f"{base}/reset-password?token={quote(str(token or ''), safe='')}"

    def _invalidate_token_type(self, conn, user_id, token_type, now_ts):
        conn.execute(
            """
            UPDATE user_tokens
            SET used_at = COALESCE(used_at, ?)
            WHERE user_id = ?
                AND type = ?
                AND used_at IS NULL
            """,
            (float(now_ts), str(user_id), str(token_type)),
        )

    def _insert_token(self, conn, user_id, token_type, ttl_sec, now_ts):
        raw_token = self._generate_token()
        token_hash = self._hash_secret(raw_token, f"token:{token_type}")
        conn.execute(
            """
            INSERT INTO user_tokens (
                id,
                user_id,
                type,
                token_hash,
                expires_at,
                used_at,
                created_at
            ) VALUES (?, ?, ?, ?, ?, NULL, ?)
            """,
            (
                str(uuid.uuid4()),
                str(user_id),
                str(token_type),
                str(token_hash),
                float(now_ts + ttl_sec),
                float(now_ts),
            ),
        )
        return raw_token

    def _insert_session(self, conn, user_id, ip_address, user_agent, now_ts):
        raw_session = self._generate_token()
        session_hash = self._hash_secret(raw_session, "session")
        ip_hash = None
        if str(ip_address or "").strip():
            ip_hash = self._hash_secret(str(ip_address or "").strip(), "ip")

        ua = str(user_agent or "").strip()
        if len(ua) > 255:
            ua = ua[:255]

        conn.execute(
            """
            INSERT INTO user_sessions (
                id,
                user_id,
                session_hash,
                created_at,
                expires_at,
                revoked_at,
                last_seen_at,
                user_agent,
                ip_hash
            ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                str(user_id),
                str(session_hash),
                float(now_ts),
                float(now_ts + SESSION_TTL_SEC),
                float(now_ts),
                ua or None,
                ip_hash,
            ),
        )
        return raw_session

    def _public_user_from_row(self, row):
        if row is None:
            return None
        email = str(row["email"] or "").strip().lower()
        role = "user"
        try:
            role = str(row["role"] or "user").strip().lower() or "user"
        except Exception:
            role = "user"
        return {
            "id": str(row["id"]),
            "email": email,
            "email_masked": mask_email(email),
            "name": str(row["name"] or ""),
            "role": role,
            "status": str(row["status"] or "active"),
            "email_verified": row["email_verified_at"] is not None,
            "email_verified_at": row["email_verified_at"],
            "privacy_policy_version": str(row["privacy_policy_version"] or ""),
            "privacy_policy_accepted_at": row["privacy_policy_accepted_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_login_at": row["last_login_at"],
            "deleted_at": row["deleted_at"],
        }

    def ensure_fixed_admin_account(self):
        if not FIXED_ADMIN_ENABLED:
            return {
                "ok": False,
                "disabled": True,
            }

        admin_email = self._normalize_email(FIXED_ADMIN_EMAIL)
        admin_password = str(FIXED_ADMIN_PASSWORD or "").strip()
        admin_name = self._normalize_name(FIXED_ADMIN_NAME) or "Administrador Global"
        if not self._is_valid_email(admin_email):
            return {
                "ok": False,
                "error": "AUTH_FIXED_ADMIN_EMAIL invalido",
            }
        if not admin_password:
            return {
                "ok": False,
                "error": "AUTH_FIXED_ADMIN_PASSWORD vazio",
            }

        now_ts = _now_ts()
        created = False
        password_hash = self._hash_password(admin_password)
        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            existing = conn.execute(
                """
                SELECT *
                FROM users
                WHERE email = ?
                LIMIT 1
                """,
                (admin_email,),
            ).fetchone()

            with conn:
                if existing is None:
                    created = True
                    user_id = str(uuid.uuid4())
                    conn.execute(
                        """
                        INSERT INTO users (
                            id,
                            email,
                            name,
                            password_hash,
                            email_verified_at,
                            status,
                            role,
                            privacy_policy_version,
                            privacy_policy_accepted_at,
                            created_at,
                            updated_at,
                            last_login_at,
                            deleted_at
                        ) VALUES (?, ?, ?, ?, ?, 'active', 'admin', ?, ?, ?, ?, NULL, NULL)
                        """,
                        (
                            user_id,
                            admin_email,
                            admin_name,
                            password_hash,
                            now_ts,
                            PRIVACY_POLICY_VERSION,
                            now_ts,
                            now_ts,
                            now_ts,
                        ),
                    )
                else:
                    user_id = str(existing["id"])
                    if FIXED_ADMIN_FORCE_PASSWORD:
                        conn.execute(
                            """
                            UPDATE users
                            SET
                                name = ?,
                                password_hash = ?,
                                email_verified_at = ?,
                                status = 'active',
                                role = 'admin',
                                privacy_policy_version = ?,
                                privacy_policy_accepted_at = COALESCE(privacy_policy_accepted_at, ?),
                                updated_at = ?,
                                deleted_at = NULL
                            WHERE id = ?
                            """,
                            (
                                admin_name,
                                password_hash,
                                now_ts,
                                PRIVACY_POLICY_VERSION,
                                now_ts,
                                now_ts,
                                user_id,
                            ),
                        )
                    else:
                        conn.execute(
                            """
                            UPDATE users
                            SET
                                name = ?,
                                email_verified_at = ?,
                                status = 'active',
                                role = 'admin',
                                privacy_policy_version = ?,
                                privacy_policy_accepted_at = COALESCE(privacy_policy_accepted_at, ?),
                                updated_at = ?,
                                deleted_at = NULL
                            WHERE id = ?
                            """,
                            (
                                admin_name,
                                now_ts,
                                PRIVACY_POLICY_VERSION,
                                now_ts,
                                now_ts,
                                user_id,
                            ),
                        )

                self._invalidate_token_type(conn, user_id, "email_verify", now_ts)
                conn.execute(
                    """
                    UPDATE user_tokens
                    SET used_at = COALESCE(used_at, ?)
                    WHERE user_id = ?
                        AND type = 'password_reset'
                        AND used_at IS NULL
                    """,
                    (now_ts, user_id),
                )

        if self.logger is not None:
            self.logger.info("Conta admin fixa garantida para %s", mask_email(admin_email))

        return {
            "ok": True,
            "created": created,
            "email_masked": mask_email(admin_email),
            "role": "admin",
        }

    def register_user(
        self,
        email,
        password,
        password_confirm,
        name=None,
        privacy_policy_accepted=False,
        request_base_url="",
    ):
        normalized_email = self._normalize_email(email)
        normalized_name = self._normalize_name(name)
        plain_password = str(password or "")
        plain_confirm = str(password_confirm or "")

        if not self._is_valid_email(normalized_email):
            return {"ok": False, "code": "invalid_email", "message": "E-mail invalido."}

        if plain_password != plain_confirm:
            return {"ok": False, "code": "password_mismatch", "message": "As senhas nao conferem."}

        password_issues = self._validate_password_strength(plain_password)
        if password_issues:
            return {
                "ok": False,
                "code": "weak_password",
                "message": "Senha fora do padrao minimo de seguranca.",
                "details": password_issues,
            }

        if not bool(privacy_policy_accepted):
            return {
                "ok": False,
                "code": "privacy_policy_required",
                "message": "E necessario aceitar a Politica de Privacidade para continuar.",
            }

        now_ts = _now_ts()
        password_hash = self._hash_password(plain_password)

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            existing = conn.execute(
                """
                SELECT *
                FROM users
                WHERE email = ?
                LIMIT 1
                """,
                (normalized_email,),
            ).fetchone()

            with conn:
                if existing is not None and existing["deleted_at"] is None:
                    return {
                        "ok": False,
                        "code": "email_in_use",
                        "message": "E-mail ja cadastrado.",
                    }

                if existing is not None and existing["deleted_at"] is not None:
                    user_id = str(existing["id"])
                    conn.execute(
                        """
                        UPDATE users
                        SET
                            name = ?,
                            password_hash = ?,
                            email_verified_at = ?,
                            status = 'active',
                            role = 'user',
                            privacy_policy_version = ?,
                            privacy_policy_accepted_at = ?,
                            updated_at = ?,
                            last_login_at = NULL,
                            deleted_at = NULL
                        WHERE id = ?
                        """,
                        (
                            normalized_name,
                            password_hash,
                            now_ts,
                            PRIVACY_POLICY_VERSION,
                            now_ts,
                            now_ts,
                            user_id,
                        ),
                    )
                else:
                    user_id = str(uuid.uuid4())
                    conn.execute(
                        """
                        INSERT INTO users (
                            id,
                            email,
                            name,
                            password_hash,
                            email_verified_at,
                            status,
                            role,
                            privacy_policy_version,
                            privacy_policy_accepted_at,
                            created_at,
                            updated_at,
                            last_login_at,
                            deleted_at
                        ) VALUES (?, ?, ?, ?, ?, 'active', 'user', ?, ?, ?, ?, NULL, NULL)
                        """,
                        (
                            user_id,
                            normalized_email,
                            normalized_name,
                            password_hash,
                            now_ts,
                            PRIVACY_POLICY_VERSION,
                            now_ts,
                            now_ts,
                            now_ts,
                        ),
                    )

                self._invalidate_token_type(conn, user_id, "email_verify", now_ts)
                conn.execute(
                    """
                    UPDATE user_tokens
                    SET used_at = COALESCE(used_at, ?)
                    WHERE user_id = ?
                        AND type = 'password_reset'
                        AND used_at IS NULL
                    """,
                    (now_ts, user_id),
                )

        return {
            "ok": True,
            "code": "registered",
            "message": "Cadastro concluido. Agora voce ja pode fazer login.",
            "email_masked": mask_email(normalized_email),
            "privacy_policy_version": PRIVACY_POLICY_VERSION,
            "email_delivery": "disabled",
            "email_error": "",
        }

    def login_user(self, email, password, ip_address=None, user_agent=None):
        normalized_email = self._normalize_email(email)
        plain_password = str(password or "")
        now_ts = _now_ts()

        if not self._is_valid_email(normalized_email) or not plain_password:
            return {"ok": False, "code": "invalid_credentials", "message": "Credenciais invalidas."}

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            user_row = conn.execute(
                """
                SELECT *
                FROM users
                WHERE email = ?
                LIMIT 1
                """,
                (normalized_email,),
            ).fetchone()

            if user_row is None:
                return {"ok": False, "code": "invalid_credentials", "message": "Credenciais invalidas."}

            if user_row["deleted_at"] is not None or str(user_row["status"] or "active") != "active":
                return {"ok": False, "code": "invalid_credentials", "message": "Credenciais invalidas."}

            if not self._verify_password(plain_password, user_row["password_hash"]):
                return {"ok": False, "code": "invalid_credentials", "message": "Credenciais invalidas."}

            with conn:
                session_token = self._insert_session(
                    conn,
                    user_id=str(user_row["id"]),
                    ip_address=ip_address,
                    user_agent=user_agent,
                    now_ts=now_ts,
                )
                conn.execute(
                    """
                    UPDATE users
                    SET
                        email_verified_at = COALESCE(email_verified_at, ?),
                        last_login_at = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now_ts, now_ts, now_ts, str(user_row["id"])),
                )
                refreshed_row = conn.execute(
                    """
                    SELECT *
                    FROM users
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (str(user_row["id"]),),
                ).fetchone()

        return {
            "ok": True,
            "code": "authenticated",
            "message": "Login realizado com sucesso.",
            "session_token": session_token,
            "session_ttl_sec": SESSION_TTL_SEC,
            "user": self._public_user_from_row(refreshed_row),
        }

    def resolve_session(self, raw_session_token, touch=True):
        token = str(raw_session_token or "").strip()
        if not token:
            return None

        now_ts = _now_ts()
        session_hash = self._hash_secret(token, "session")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    sessions.id AS session_id,
                    sessions.user_id AS session_user_id,
                    sessions.expires_at AS session_expires_at,
                    users.*
                FROM user_sessions AS sessions
                JOIN users
                    ON users.id = sessions.user_id
                WHERE sessions.session_hash = ?
                    AND sessions.revoked_at IS NULL
                    AND sessions.expires_at > ?
                    AND users.deleted_at IS NULL
                    AND users.status = 'active'
                LIMIT 1
                """,
                (session_hash, now_ts),
            ).fetchone()

            if row is None:
                return None

            if touch:
                with conn:
                    conn.execute(
                        """
                        UPDATE user_sessions
                        SET last_seen_at = ?
                        WHERE id = ?
                        """,
                        (now_ts, str(row["session_id"])),
                    )

        user = self._public_user_from_row(row)
        if user is None:
            return None

        return {
            "session_id": str(row["session_id"]),
            "session_user_id": str(row["session_user_id"]),
            "session_expires_at": row["session_expires_at"],
            "user": user,
        }

    def logout_session(self, raw_session_token):
        token = str(raw_session_token or "").strip()
        if not token:
            return
        now_ts = _now_ts()
        session_hash = self._hash_secret(token, "session")
        with self._connect() as conn:
            with conn:
                conn.execute(
                    """
                    UPDATE user_sessions
                    SET revoked_at = COALESCE(revoked_at, ?)
                    WHERE session_hash = ?
                    """,
                    (now_ts, session_hash),
                )

    def verify_email_token(self, raw_token):
        token = str(raw_token or "").strip()
        if not token:
            return {"ok": False, "code": "invalid_token", "message": "Token invalido."}

        now_ts = _now_ts()
        token_hash = self._hash_secret(token, "token:email_verify")

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    user_tokens.id AS token_id,
                    user_tokens.user_id AS token_user_id,
                    user_tokens.expires_at AS token_expires_at,
                    user_tokens.used_at AS token_used_at,
                    users.*
                FROM user_tokens
                JOIN users
                    ON users.id = user_tokens.user_id
                WHERE user_tokens.type = 'email_verify'
                    AND user_tokens.token_hash = ?
                LIMIT 1
                """,
                (token_hash,),
            ).fetchone()

            if row is None:
                return {"ok": False, "code": "invalid_token", "message": "Token invalido."}

            if row["token_used_at"] is not None:
                return {"ok": False, "code": "token_used", "message": "Token ja utilizado."}

            if float(row["token_expires_at"]) < now_ts:
                with conn:
                    conn.execute(
                        """
                        UPDATE user_tokens
                        SET used_at = COALESCE(used_at, ?)
                        WHERE id = ?
                        """,
                        (now_ts, str(row["token_id"])),
                    )
                return {"ok": False, "code": "token_expired", "message": "Token expirado."}

            if row["deleted_at"] is not None or str(row["status"] or "active") != "active":
                return {"ok": False, "code": "invalid_user", "message": "Usuario indisponivel."}

            with conn:
                conn.execute(
                    """
                    UPDATE user_tokens
                    SET used_at = ?
                    WHERE id = ?
                        AND used_at IS NULL
                    """,
                    (now_ts, str(row["token_id"])),
                )
                conn.execute(
                    """
                    UPDATE users
                    SET
                        email_verified_at = COALESCE(email_verified_at, ?),
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (now_ts, now_ts, str(row["token_user_id"])),
                )

        return {"ok": True, "code": "verified", "message": "E-mail verificado com sucesso."}

    def resend_verification(self, email=None, user_id=None, request_base_url=""):
        return {
            "ok": True,
            "code": "verification_not_required",
            "message": "Verificacao de conta desabilitada. Voce ja pode fazer login.",
            "email_delivery": "disabled",
        }

    def forgot_password(self, email, request_base_url=""):
        now_ts = _now_ts()
        normalized_email = self._normalize_email(email)
        target_email = None
        reset_token = None
        channel_mode = str(getattr(self.email_service, "mode", "console")).strip().lower()
        email_delivery = "console_link" if channel_mode == "console" else "smtp_attempted"

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            if self._is_valid_email(normalized_email):
                row = conn.execute(
                    """
                    SELECT *
                    FROM users
                    WHERE email = ?
                    LIMIT 1
                    """,
                    (normalized_email,),
                ).fetchone()
            else:
                row = None

            if (
                row is not None
                and row["deleted_at"] is None
                and str(row["status"] or "active") == "active"
                and row["email_verified_at"] is not None
            ):
                target_email = str(row["email"] or "").strip().lower()
                with conn:
                    self._invalidate_token_type(conn, str(row["id"]), "password_reset", now_ts)
                    reset_token = self._insert_token(conn, str(row["id"]), "password_reset", RESET_TOKEN_TTL_SEC, now_ts)

        if target_email and reset_token:
            reset_link = self._build_reset_link(request_base_url, reset_token)
            delivery = self.email_service.send_reset_password_email(target_email, reset_link) or {}
            if (not bool(delivery.get("sent"))) and (channel_mode == "smtp") and (self.logger is not None):
                self.logger.warning(
                    "Falha ao enviar reset de senha para %s",
                    mask_email(target_email),
                )

        return {
            "ok": True,
            "code": "accepted",
            "message": "Se o e-mail existir, enviaremos um link de redefinicao de senha.",
            "email_delivery": email_delivery,
        }

    def reset_password(self, raw_token, new_password, confirm_password):
        token = str(raw_token or "").strip()
        new_plain = str(new_password or "")
        confirm_plain = str(confirm_password or "")

        if not token:
            return {"ok": False, "code": "invalid_token", "message": "Token invalido."}

        if new_plain != confirm_plain:
            return {"ok": False, "code": "password_mismatch", "message": "As senhas nao conferem."}

        password_issues = self._validate_password_strength(new_plain)
        if password_issues:
            return {
                "ok": False,
                "code": "weak_password",
                "message": "Senha fora do padrao minimo de seguranca.",
                "details": password_issues,
            }

        now_ts = _now_ts()
        token_hash = self._hash_secret(token, "token:password_reset")
        new_password_hash = self._hash_password(new_plain)

        with self._connect() as conn:
            self._cleanup_expired_records(conn, now_ts)
            row = conn.execute(
                """
                SELECT
                    user_tokens.id AS token_id,
                    user_tokens.user_id AS token_user_id,
                    user_tokens.expires_at AS token_expires_at,
                    user_tokens.used_at AS token_used_at,
                    users.*
                FROM user_tokens
                JOIN users
                    ON users.id = user_tokens.user_id
                WHERE user_tokens.type = 'password_reset'
                    AND user_tokens.token_hash = ?
                LIMIT 1
                """,
                (token_hash,),
            ).fetchone()

            if row is None:
                return {"ok": False, "code": "invalid_token", "message": "Token invalido."}

            if row["token_used_at"] is not None:
                return {"ok": False, "code": "token_used", "message": "Token ja utilizado."}

            if float(row["token_expires_at"]) < now_ts:
                with conn:
                    conn.execute(
                        """
                        UPDATE user_tokens
                        SET used_at = COALESCE(used_at, ?)
                        WHERE id = ?
                        """,
                        (now_ts, str(row["token_id"])),
                    )
                return {"ok": False, "code": "token_expired", "message": "Token expirado."}

            if row["deleted_at"] is not None or str(row["status"] or "active") != "active":
                return {"ok": False, "code": "invalid_user", "message": "Usuario indisponivel."}

            user_id = str(row["token_user_id"])
            with conn:
                conn.execute(
                    """
                    UPDATE users
                    SET
                        password_hash = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (new_password_hash, now_ts, user_id),
                )
                conn.execute(
                    """
                    UPDATE user_tokens
                    SET used_at = COALESCE(used_at, ?)
                    WHERE user_id = ?
                        AND type = 'password_reset'
                        AND used_at IS NULL
                    """,
                    (now_ts, user_id),
                )
                conn.execute(
                    """
                    UPDATE user_sessions
                    SET revoked_at = COALESCE(revoked_at, ?)
                    WHERE user_id = ?
                        AND revoked_at IS NULL
                    """,
                    (now_ts, user_id),
                )

        return {"ok": True, "code": "password_reset", "message": "Senha redefinida com sucesso."}

    def export_account_data(self, user_id):
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return None
        now_ts = _now_ts()

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM users
                WHERE id = ?
                LIMIT 1
                """,
                (normalized_user_id,),
            ).fetchone()
            if row is None:
                return None

            token_rows = conn.execute(
                """
                SELECT type, COUNT(*) AS qty
                FROM user_tokens
                WHERE user_id = ?
                    AND used_at IS NULL
                    AND expires_at > ?
                GROUP BY type
                """,
                (normalized_user_id, now_ts),
            ).fetchall()
            active_tokens = {str(item["type"]): int(item["qty"] or 0) for item in token_rows}

            active_sessions_row = conn.execute(
                """
                SELECT COUNT(*) AS qty
                FROM user_sessions
                WHERE user_id = ?
                    AND revoked_at IS NULL
                    AND expires_at > ?
                """,
                (normalized_user_id, now_ts),
            ).fetchone()
            active_sessions = int((active_sessions_row or {"qty": 0})["qty"] or 0)

        user_data = self._public_user_from_row(row)
        if user_data is None:
            return None

        return {
            "exported_at": now_ts,
            "user": user_data,
            "active_sessions": active_sessions,
            "active_tokens": {
                "email_verify": int(active_tokens.get("email_verify", 0)),
                "password_reset": int(active_tokens.get("password_reset", 0)),
            },
            "retention": {
                "verify_token_ttl_sec": VERIFY_TOKEN_TTL_SEC,
                "reset_token_ttl_sec": RESET_TOKEN_TTL_SEC,
                "session_ttl_sec": SESSION_TTL_SEC,
                "logs_policy": "Apenas logs operacionais minimos, sem senha, token bruto ou e-mail completo.",
            },
        }

    def get_pivot_table_columns(self, user_id):
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return []

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT prefs.pivot_table_columns_json
                FROM users
                LEFT JOIN user_ui_preferences AS prefs
                    ON prefs.user_id = users.id
                WHERE users.id = ?
                    AND users.deleted_at IS NULL
                    AND users.status = 'active'
                LIMIT 1
                """,
                (normalized_user_id,),
            ).fetchone()

        if row is None:
            return []

        raw_json = row["pivot_table_columns_json"]
        if raw_json in (None, ""):
            return []

        try:
            parsed = json.loads(raw_json)
        except (TypeError, ValueError):
            return []

        if not isinstance(parsed, list):
            return []

        cleaned = []
        seen = set()
        for item in parsed:
            key = str(item or "").strip()
            if not key:
                continue
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(key)
            if len(cleaned) >= 64:
                break
        return cleaned

    def set_pivot_table_columns(self, user_id, columns):
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return {
                "ok": False,
                "code": "auth_required",
                "message": "Autenticacao obrigatoria.",
            }

        if not isinstance(columns, list):
            return {
                "ok": False,
                "code": "invalid_columns",
                "message": "Lista de colunas invalida.",
            }

        cleaned = []
        seen = set()
        for item in columns:
            key = str(item or "").strip()
            if not key:
                continue
            if len(key) > 80:
                continue
            if key in seen:
                continue
            seen.add(key)
            cleaned.append(key)
            if len(cleaned) >= 64:
                break

        now_ts = _now_ts()
        payload = json.dumps(cleaned, ensure_ascii=False)
        with self._connect() as conn:
            user_row = conn.execute(
                """
                SELECT id
                FROM users
                WHERE id = ?
                    AND deleted_at IS NULL
                    AND status = 'active'
                LIMIT 1
                """,
                (normalized_user_id,),
            ).fetchone()
            if user_row is None:
                return {
                    "ok": False,
                    "code": "user_not_found",
                    "message": "Usuario nao encontrado.",
                }

            with conn:
                conn.execute(
                    """
                    INSERT INTO user_ui_preferences (
                        user_id,
                        pivot_table_columns_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(user_id) DO UPDATE SET
                        pivot_table_columns_json = excluded.pivot_table_columns_json,
                        updated_at = excluded.updated_at
                    """,
                    (normalized_user_id, payload, now_ts, now_ts),
                )

        return {
            "ok": True,
            "code": "ok",
            "columns": cleaned,
        }

    def _is_active_admin_row(self, row):
        if row is None:
            return False
        if row["deleted_at"] is not None:
            return False
        if str(row["status"] or "active") != "active":
            return False
        role = str(row["role"] or "user").strip().lower()
        return role == "admin"

    def list_users_for_admin(self, admin_user_id, limit=200):
        normalized_admin_id = str(admin_user_id or "").strip()
        if not normalized_admin_id:
            return {"ok": False, "code": "auth_required", "message": "Autenticacao obrigatoria."}

        try:
            safe_limit = int(limit or 200)
        except (TypeError, ValueError):
            safe_limit = 200
        safe_limit = max(1, min(500, safe_limit))

        with self._connect() as conn:
            admin_row = conn.execute(
                """
                SELECT id, email, role, status, deleted_at
                FROM users
                WHERE id = ?
                LIMIT 1
                """,
                (normalized_admin_id,),
            ).fetchone()
            if not self._is_active_admin_row(admin_row):
                return {"ok": False, "code": "admin_required", "message": "Acesso restrito ao administrador."}

            user_rows = conn.execute(
                """
                SELECT
                    id,
                    email,
                    name,
                    role,
                    status,
                    created_at,
                    updated_at,
                    last_login_at
                FROM users
                WHERE deleted_at IS NULL
                    AND LOWER(COALESCE(role, 'user')) <> 'admin'
                    AND LOWER(email) <> ?
                ORDER BY COALESCE(updated_at, created_at) DESC
                LIMIT ?
                """,
                (FIXED_ADMIN_EMAIL, safe_limit),
            ).fetchall()

        users = []
        for row in user_rows:
            users.append(
                {
                    "id": str(row["id"]),
                    "email": str(row["email"] or "").strip().lower(),
                    "name": str(row["name"] or "").strip(),
                    "role": str(row["role"] or "user").strip().lower() or "user",
                    "status": str(row["status"] or "active").strip().lower() or "active",
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                    "last_login_at": row["last_login_at"],
                }
            )

        return {"ok": True, "code": "ok", "users": users}

    def admin_delete_user(self, admin_user_id, target_user_id):
        normalized_admin_id = str(admin_user_id or "").strip()
        normalized_target_id = str(target_user_id or "").strip()
        if not normalized_admin_id:
            return {"ok": False, "code": "auth_required", "message": "Autenticacao obrigatoria."}
        if not normalized_target_id:
            return {"ok": False, "code": "invalid_target", "message": "Conta alvo invalida."}

        with self._connect() as conn:
            admin_row = conn.execute(
                """
                SELECT id, email, role, status, deleted_at
                FROM users
                WHERE id = ?
                LIMIT 1
                """,
                (normalized_admin_id,),
            ).fetchone()
            if not self._is_active_admin_row(admin_row):
                return {"ok": False, "code": "admin_required", "message": "Acesso restrito ao administrador."}

            target_row = conn.execute(
                """
                SELECT id, email, role, status, deleted_at
                FROM users
                WHERE id = ?
                LIMIT 1
                """,
                (normalized_target_id,),
            ).fetchone()

        if target_row is None or target_row["deleted_at"] is not None:
            return {"ok": False, "code": "not_found", "message": "Conta nao encontrada."}

        target_role = str(target_row["role"] or "user").strip().lower()
        target_email = str(target_row["email"] or "").strip().lower()
        if (
            normalized_target_id == normalized_admin_id
            or target_role == "admin"
            or target_email == FIXED_ADMIN_EMAIL
        ):
            return {"ok": False, "code": "admin_protected", "message": "Conta de administrador nao pode ser excluida."}

        deleted = self.delete_account(normalized_target_id)
        if not deleted:
            return {"ok": False, "code": "not_found", "message": "Conta nao encontrada."}

        return {"ok": True, "code": "deleted", "message": "Conta excluida com sucesso."}

    def delete_account(self, user_id):
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return False
        now_ts = _now_ts()

        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT id, email, role
                FROM users
                WHERE id = ?
                LIMIT 1
                """,
                (normalized_user_id,),
            ).fetchone()
            if row is None:
                return False
            row_role = str(row["role"] or "user").strip().lower()
            row_email = str(row["email"] or "").strip().lower()
            if row_role == "admin" or row_email == FIXED_ADMIN_EMAIL:
                return False

            anonymized_email = f"deleted-{normalized_user_id[:8]}-{int(now_ts)}-{uuid.uuid4().hex[:8]}@anon.invalid"
            anonymized_password = self._hash_password(secrets.token_urlsafe(24))
            with conn:
                conn.execute(
                    """
                    UPDATE users
                    SET
                        email = ?,
                        name = NULL,
                        password_hash = ?,
                        email_verified_at = NULL,
                        status = 'disabled',
                        role = 'user',
                        privacy_policy_accepted_at = NULL,
                        updated_at = ?,
                        deleted_at = ?
                    WHERE id = ?
                    """,
                    (
                        anonymized_email,
                        anonymized_password,
                        now_ts,
                        now_ts,
                        normalized_user_id,
                    ),
                )
                conn.execute(
                    """
                    UPDATE user_tokens
                    SET used_at = COALESCE(used_at, ?)
                    WHERE user_id = ?
                        AND used_at IS NULL
                    """,
                    (now_ts, normalized_user_id),
                )
                conn.execute(
                    """
                    UPDATE user_sessions
                    SET revoked_at = COALESCE(revoked_at, ?)
                    WHERE user_id = ?
                        AND revoked_at IS NULL
                    """,
                    (now_ts, normalized_user_id),
                )

        return True
