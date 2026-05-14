from pathlib import Path


SCRIPT = Path("scripts/ec2-postgres-real-validate.sh")
RUNBOOK = Path("POSTGRES_MIGRATION_RUNBOOK.md")


def test_ec2_real_validate_script_is_safe_by_default():
    content = SCRIPT.read_text(encoding="utf-8")

    assert "VALIDATION_MODE" in content
    assert "preflight|compare-existing|migrate-and-compare" in content
    assert "CONFIRM_TARGET_TRUNCATE=1 obrigatorio" in content
    assert "postgres_real_validation.py" in content
    assert "--report-json" in content
    assert "docker compose stop backend" in content
    assert 'if [ "${VALIDATION_MODE}" = "migrate-and-compare" ]' in content
    assert "DB_BACKEND" not in content
    assert "APPLY_CUTOVER" not in content


def test_runbook_documents_ec2_real_validation():
    content = RUNBOOK.read_text(encoding="utf-8")

    assert "ec2-postgres-real-validate.sh" in content
    assert "VALIDATION_MODE=migrate-and-compare" in content
    assert "CONFIRM_TARGET_TRUNCATE=1" in content
    assert "nao aplica cutover" in content
