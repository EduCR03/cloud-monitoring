import argparse
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from scripts.http_smoke_check import run_smoke
from scripts.postgres_preflight import redact_database_url, run_preflight
from scripts.validate_postgres_migration import compare_backends


VALIDATION_MODES = ("preflight", "compare-existing", "migrate-and-compare")


def run_real_validation(
    *,
    sqlite_path,
    database_url,
    mode="preflight",
    sample_limit=5,
    require_empty=False,
    smoke_base_url="",
    smoke_email="",
    smoke_password="",
    smoke_timeout=10,
):
    safe_mode = str(mode or "preflight").strip()
    if safe_mode not in VALIDATION_MODES:
        raise ValueError(f"mode invalido: {safe_mode}")

    result = {
        "ok": False,
        "mode": safe_mode,
        "database_url": redact_database_url(database_url),
        "sqlite_path": str(sqlite_path or "").strip(),
        "steps": [],
    }

    preflight = run_preflight(database_url=database_url, require_empty=require_empty)
    result["steps"].append({"label": "preflight", "ok": bool(preflight.get("ok")), "result": preflight})
    if not preflight.get("ok"):
        result["ok"] = False
        return result

    if safe_mode in ("compare-existing", "migrate-and-compare"):
        compare = compare_backends(
            sqlite_path=sqlite_path,
            database_url=database_url,
            migrate_first=(safe_mode == "migrate-and-compare"),
            sample_limit=max(1, int(sample_limit or 5)),
        )
        result["steps"].append({"label": safe_mode, "ok": bool(compare.get("ok")), "result": compare})

    if str(smoke_base_url or "").strip():
        smoke = run_smoke(
            base_url=smoke_base_url,
            email=smoke_email,
            password=smoke_password,
            timeout=max(1.0, float(smoke_timeout or 10)),
        )
        result["steps"].append({"label": "http_smoke", "ok": bool(smoke.get("ok")), "result": smoke})

    result["ok"] = all(bool(step.get("ok")) for step in result["steps"])
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Executa validacao real SQLite -> PostgreSQL/RDS.")
    parser.add_argument("--sqlite-path", required=True, help="Caminho do telemetry.sqlite3")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
        help="URL PostgreSQL. Tambem aceita DATABASE_URL.",
    )
    parser.add_argument(
        "--mode",
        choices=VALIDATION_MODES,
        default="preflight",
        help="preflight nao migra; compare-existing compara DB ja migrado; migrate-and-compare trunca destino.",
    )
    parser.add_argument("--sample-limit", type=int, default=5)
    parser.add_argument("--require-empty", action="store_true")
    parser.add_argument(
        "--confirm-target-truncate",
        action="store_true",
        help="Obrigatorio com mode=migrate-and-compare.",
    )
    parser.add_argument("--smoke-base-url", default=os.environ.get("SMOKE_BASE_URL", ""))
    parser.add_argument("--smoke-email", default=os.environ.get("SMOKE_EMAIL", ""))
    parser.add_argument("--smoke-password", default=os.environ.get("SMOKE_PASSWORD", ""))
    parser.add_argument("--smoke-timeout", type=float, default=float(os.environ.get("SMOKE_TIMEOUT_SEC", "10")))
    parser.add_argument("--report-json", default="")
    return parser.parse_args(argv)


def _write_report(path, result):
    report_path = os.path.abspath(str(path or "").strip())
    if not report_path:
        return
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def main(argv=None):
    args = parse_args(argv)
    if args.mode == "migrate-and-compare" and not args.confirm_target_truncate:
        print("fail: --confirm-target-truncate obrigatorio para migrate-and-compare", file=sys.stderr)
        return 2

    result = run_real_validation(
        sqlite_path=args.sqlite_path,
        database_url=args.database_url,
        mode=args.mode,
        sample_limit=args.sample_limit,
        require_empty=args.require_empty,
        smoke_base_url=args.smoke_base_url,
        smoke_email=args.smoke_email,
        smoke_password=args.smoke_password,
        smoke_timeout=args.smoke_timeout,
    )
    if args.report_json:
        _write_report(args.report_json, result)

    print(f"mode: {result['mode']}")
    print(f"database: {result['database_url']}")
    for step in result["steps"]:
        status = "ok" if step.get("ok") else "fail"
        print(f"{status}: {step['label']}")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
