import argparse
import json
import os
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.postgres_preflight import redact_database_url


def _read_text(path):
    safe_path = str(path or "").strip()
    if not safe_path or not os.path.isfile(safe_path):
        return ""
    with open(safe_path, "r", encoding="utf-8", errors="replace") as handle:
        return handle.read().strip()


def build_manifest(args):
    sha256_text = _read_text(args.sqlite_backup_sha256_path)
    return {
        "cutover_id": str(args.cutover_id or "").strip(),
        "status": str(args.status or "").strip(),
        "created_at_ts": time.time(),
        "created_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "branch": str(args.branch or "").strip(),
        "commit": str(args.commit or "").strip(),
        "apply_cutover": bool(args.apply_cutover),
        "run_http_smoke": bool(args.run_http_smoke),
        "database_url": redact_database_url(args.database_url),
        "sqlite_path": str(args.sqlite_path or "").strip(),
        "sqlite_backup_path": str(args.sqlite_backup_path or "").strip(),
        "sqlite_backup_sha256_path": str(args.sqlite_backup_sha256_path or "").strip(),
        "sqlite_backup_sha256": sha256_text,
        "env_backup_path": str(args.env_backup_path or "").strip(),
        "compare_report_path": str(args.compare_report_path or "").strip(),
        "smoke_report_path": str(args.smoke_report_path or "").strip(),
        "note": str(args.note or "").strip(),
    }


def write_manifest(args):
    manifest = build_manifest(args)
    target_path = os.path.abspath(str(args.path or "").strip())
    if not target_path:
        raise ValueError("path obrigatorio")
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    with open(target_path, "w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    return manifest


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Grava manifesto auditavel de cutover PostgreSQL.")
    parser.add_argument("--path", required=True)
    parser.add_argument("--cutover-id", required=True)
    parser.add_argument("--status", required=True)
    parser.add_argument("--branch", default="")
    parser.add_argument("--commit", default="")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", ""))
    parser.add_argument("--sqlite-path", default="")
    parser.add_argument("--sqlite-backup-path", default="")
    parser.add_argument("--sqlite-backup-sha256-path", default="")
    parser.add_argument("--env-backup-path", default="")
    parser.add_argument("--compare-report-path", default="")
    parser.add_argument("--smoke-report-path", default="")
    parser.add_argument("--apply-cutover", action="store_true")
    parser.add_argument("--run-http-smoke", action="store_true")
    parser.add_argument("--note", default="")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    manifest = write_manifest(args)
    print(f"manifest: {os.path.abspath(args.path)}")
    print(f"status: {manifest['status']}")
    print(f"database: {manifest['database_url']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
