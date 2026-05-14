from pathlib import Path


SCRIPT = Path("scripts/ec2-postgres-cutover.sh")
RUNBOOK = Path("POSTGRES_MIGRATION_RUNBOOK.md")


def test_ec2_cutover_script_has_safe_guards():
    content = SCRIPT.read_text(encoding="utf-8")

    assert "APPLY_CUTOVER" in content
    assert "ROLLBACK_ONLY" in content
    assert "restore_on_error" in content
    assert "trap 'restore_on_error' ERR" in content
    assert "validate_postgres_migration.py" in content
    assert "--migrate-first" in content
    assert "create_sqlite_backup" in content
    assert "sha256sum" in content
    assert "SQLITE_BACKUP_PATH" in content
    assert "DB_BACKEND=sqlite" in content
    assert 'set_env_value "DB_BACKEND" "postgres"' in content


def test_runbook_documents_ec2_cutover_and_rollback():
    content = RUNBOOK.read_text(encoding="utf-8")

    assert "Cutover EC2/RDS" in content
    assert "APPLY_CUTOVER=1" in content
    assert "ROLLBACK_ONLY=1" in content
    assert "/data/backups/telemetry.<timestamp>.sqlite3" in content
    assert "Restaurar backup SQLite manualmente" in content
    assert "scripts/ec2-postgres-cutover.sh" in content
