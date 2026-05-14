import json

from scripts.write_cutover_manifest import parse_args, write_manifest


def test_write_manifest_redacts_database_url_and_includes_backup_hash(tmp_path):
    backup_hash = tmp_path / "telemetry.sqlite3.sha256"
    backup_hash.write_text("abc123  telemetry.sqlite3\n", encoding="utf-8")
    manifest_path = tmp_path / "manifest.json"

    args = parse_args(
        [
            "--path",
            str(manifest_path),
            "--cutover-id",
            "20260101000000",
            "--status",
            "cutover_applied",
            "--branch",
            "dev",
            "--commit",
            "abc1234",
            "--database-url",
            "postgresql://user:secret@example.com:5432/cloudv2",
            "--sqlite-path",
            "/data/telemetry.sqlite3",
            "--sqlite-backup-path",
            "/data/backups/telemetry.sqlite3",
            "--sqlite-backup-sha256-path",
            str(backup_hash),
            "--compare-report-path",
            "/data/postgres-compare-report.json",
            "--smoke-report-path",
            "/data/postgres-http-smoke-report.json",
            "--apply-cutover",
            "--run-http-smoke",
        ]
    )

    manifest = write_manifest(args)
    saved = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert manifest["database_url"] == "postgresql://user:***@example.com:5432/cloudv2"
    assert saved["database_url"] == "postgresql://user:***@example.com:5432/cloudv2"
    assert "secret" not in manifest_path.read_text(encoding="utf-8")
    assert saved["sqlite_backup_sha256"] == "abc123  telemetry.sqlite3"
    assert saved["apply_cutover"] is True
    assert saved["run_http_smoke"] is True
