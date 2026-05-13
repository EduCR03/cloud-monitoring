CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    name TEXT,
    password_hash TEXT NOT NULL,
    email_verified_at DOUBLE PRECISION,
    status TEXT NOT NULL DEFAULT 'active',
    privacy_policy_version TEXT NOT NULL,
    privacy_policy_accepted_at DOUBLE PRECISION,
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL,
    last_login_at DOUBLE PRECISION,
    deleted_at DOUBLE PRECISION,
    CHECK (status IN ('active', 'disabled'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_email_unique
    ON users (email);

CREATE INDEX IF NOT EXISTS idx_users_email
    ON users (email);

CREATE TABLE IF NOT EXISTS user_tokens (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    type TEXT NOT NULL,
    token_hash TEXT NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL,
    used_at DOUBLE PRECISION,
    created_at DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    CHECK (type IN ('email_verify', 'password_reset'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_tokens_token_hash
    ON user_tokens (token_hash);

CREATE INDEX IF NOT EXISTS idx_user_tokens_user_id
    ON user_tokens (user_id);

CREATE INDEX IF NOT EXISTS idx_user_tokens_type
    ON user_tokens (type);

CREATE INDEX IF NOT EXISTS idx_user_tokens_expires_at
    ON user_tokens (expires_at);

CREATE TABLE IF NOT EXISTS user_sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    session_hash TEXT NOT NULL,
    created_at DOUBLE PRECISION NOT NULL,
    expires_at DOUBLE PRECISION NOT NULL,
    revoked_at DOUBLE PRECISION,
    last_seen_at DOUBLE PRECISION,
    user_agent TEXT,
    ip_hash TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_user_sessions_session_hash
    ON user_sessions (session_hash);

CREATE INDEX IF NOT EXISTS idx_user_sessions_user_id
    ON user_sessions (user_id);

CREATE INDEX IF NOT EXISTS idx_user_sessions_expires_at
    ON user_sessions (expires_at);
