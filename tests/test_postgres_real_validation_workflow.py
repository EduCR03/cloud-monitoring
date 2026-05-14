from pathlib import Path


WORKFLOW = Path(".github/workflows/postgres-real-validation.yml")
RUNBOOK = Path("POSTGRES_MIGRATION_RUNBOOK.md")


def test_real_validation_workflow_requires_safe_confirmation():
    content = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch" in content
    assert "migrate-and-compare" in content
    assert "confirm_target_truncate" in content
    assert "migrate-and-compare exige confirm_target_truncate=true" in content
    assert "secrets.POSTGRES_DATABASE_URL" in content
    assert "scripts/ec2-postgres-real-validate.sh" in content
    assert "APPLY_CUTOVER" not in content


def test_runbook_documents_real_validation_workflow_secrets():
    content = RUNBOOK.read_text(encoding="utf-8")

    assert "PostgreSQL Real Validation" in content
    assert "EC2_HOST" in content
    assert "POSTGRES_DATABASE_URL" in content
    assert "validation_mode=preflight" in content
    assert "confirm_target_truncate=true" in content
