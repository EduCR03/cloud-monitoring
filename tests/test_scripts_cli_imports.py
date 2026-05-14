import subprocess
import sys


SCRIPT_HELP_TARGETS = [
    "scripts/migrate_sqlite_to_postgres.py",
    "scripts/validate_postgres_migration.py",
    "scripts/postgres_preflight.py",
    "scripts/postgres_real_validation.py",
    "scripts/write_cutover_manifest.py",
    "scripts/http_smoke_check.py",
]


def test_scripts_can_run_by_file_path():
    for script_path in SCRIPT_HELP_TARGETS:
        result = subprocess.run(
            [sys.executable, script_path, "--help"],
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
        assert result.returncode == 0, result.stderr
        assert "ModuleNotFoundError" not in result.stderr
