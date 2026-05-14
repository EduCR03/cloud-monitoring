from pathlib import Path


SCRIPT = Path("scripts/ec2-postgres-cutover.sh")
RUNBOOK = Path("POSTGRES_MIGRATION_RUNBOOK.md")
DOCKERFILE = Path("Dockerfile")


def test_ec2_cutover_script_has_safe_guards():
    content = SCRIPT.read_text(encoding="utf-8")

    assert "APPLY_CUTOVER" in content
    assert "ROLLBACK_ONLY" in content
    assert "ROLLBACK_SQLITE_BACKUP_PATH" in content
    assert "rollback_sqlite_applied" in content
    assert "sha256sum -c" in content
    assert "restore_on_error" in content
    assert "trap 'restore_on_error' ERR" in content
    assert "validate_postgres_migration.py" in content
    assert "--migrate-first" in content
    assert "create_sqlite_backup" in content
    assert "sha256sum" in content
    assert "SQLITE_BACKUP_PATH" in content
    assert "wait_for_backend_health" in content
    assert "/api/health" in content
    assert "docker compose logs --tail 80 backend" in content
    assert "run_postgres_preflight" in content
    assert "postgres_preflight.py" in content
    assert "REQUIRE_EMPTY_POSTGRES" in content
    assert "run_http_smoke" in content
    assert "http_smoke_check.py" in content
    assert "RUN_HTTP_SMOKE" in content
    assert "write_cutover_manifest" in content
    assert "write_cutover_manifest.py" in content
    assert "MANIFEST_PATH" in content
    assert "validated_without_cutover" in content
    assert "cutover_applied" in content
    assert "failed_restored" in content
    assert "DB_BACKEND=sqlite" in content
    assert 'set_env_value "DB_BACKEND" "postgres"' in content


def test_runbook_documents_ec2_cutover_and_rollback():
    content = RUNBOOK.read_text(encoding="utf-8")

    assert "Cutover EC2/RDS" in content
    assert "APPLY_CUTOVER=1" in content
    assert "ROLLBACK_ONLY=1" in content
    assert "ROLLBACK_SQLITE_BACKUP_PATH=/data/backups/telemetry.TIMESTAMP.sqlite3" in content
    assert "/data/backups/telemetry.<timestamp>.sqlite3" in content
    assert "Restaurar backup SQLite manualmente" in content
    assert "/api/health" in content
    assert "reinicia em SQLite" in content
    assert "Preflight RDS" in content
    assert "postgres_preflight.py" in content
    assert "Smoke HTTP" in content
    assert "http_smoke_check.py" in content
    assert "manifesto auditavel" in content
    assert "URL PostgreSQL mascarada" in content
    assert "scripts/ec2-postgres-cutover.sh" in content


def test_backend_image_contains_cutover_support_scripts():
    content = DOCKERFILE.read_text(encoding="utf-8")

    assert "COPY scripts /app/scripts" in content
