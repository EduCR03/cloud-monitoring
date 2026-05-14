from pathlib import Path


STEPS = Path("POSTGRES_PRODUCTION_STEPS.md")
RUNBOOK = Path("POSTGRES_MIGRATION_RUNBOOK.md")


def test_production_steps_cover_user_actions():
    content = STEPS.read_text(encoding="utf-8")

    assert "Criar RDS" in content
    assert "Criar GitHub secrets" in content
    assert "EC2_HOST" in content
    assert "POSTGRES_DATABASE_URL" in content
    assert "Validar acesso RDS" in content
    assert "Migrar e comparar" in content
    assert "Cutover sem aplicar" in content
    assert "Aplicar cutover" in content
    assert "Rollback" in content
    assert "ROLLBACK_SQLITE_BACKUP_PATH" in content


def test_runbook_points_to_production_steps():
    content = RUNBOOK.read_text(encoding="utf-8")

    assert "POSTGRES_PRODUCTION_STEPS.md" in content
