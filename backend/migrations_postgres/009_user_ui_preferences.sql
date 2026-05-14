CREATE TABLE IF NOT EXISTS user_ui_preferences (
    user_id TEXT PRIMARY KEY,
    pivot_table_columns_json TEXT,
    created_at DOUBLE PRECISION NOT NULL,
    updated_at DOUBLE PRECISION NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
