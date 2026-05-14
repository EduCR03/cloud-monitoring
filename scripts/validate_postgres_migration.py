import argparse
import json
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from backend.cloudv2_auth import AuthService
from backend.cloudv2_persistence import TelemetryPersistence
from scripts.migrate_sqlite_to_postgres import migrate


def _normalize_payload(value):
    if isinstance(value, dict):
        return {str(key): _normalize_payload(val) for key, val in sorted(value.items(), key=lambda item: str(item[0]))}
    if isinstance(value, list):
        return [_normalize_payload(item) for item in value]
    return value


def _compare_json(label, left, right):
    normalized_left = _normalize_payload(left)
    normalized_right = _normalize_payload(right)
    if normalized_left == normalized_right:
        return {"label": label, "match": True}
    return {
        "label": label,
        "match": False,
        "left": normalized_left,
        "right": normalized_right,
    }


def _build_persistence_sqlite(sqlite_path):
    persistence = TelemetryPersistence(db_path=sqlite_path, db_backend="sqlite")
    persistence.start()
    return persistence


def _build_persistence_postgres(database_url):
    persistence = TelemetryPersistence(db_backend="postgres", database_url=database_url)
    persistence.start()
    return persistence


def _sample_panel_targets(state_payload, limit=5):
    targets = []
    for pivot in state_payload.get("pivots", []):
        pivot_id = str(pivot.get("pivot_id") or "").strip()
        session_id = str(pivot.get("session_id") or "").strip()
        if not pivot_id or not session_id:
            continue
        targets.append((pivot_id, session_id))
        if len(targets) >= int(limit or 5):
            break
    return targets


def compare_backends(sqlite_path, database_url, migrate_first=False, sample_limit=5):
    if migrate_first:
        migrate(
            sqlite_path=sqlite_path,
            database_url=database_url,
            backup=True,
            truncate_target=True,
        )

    sqlite_persistence = _build_persistence_sqlite(sqlite_path)
    postgres_persistence = _build_persistence_postgres(database_url)
    sqlite_auth = AuthService(db_path=sqlite_path, db_backend="sqlite")
    postgres_auth = AuthService(db_backend="postgres", database_url=database_url)

    checks = []
    try:
        sqlite_run = sqlite_persistence.resolve_run()
        postgres_run = postgres_persistence.resolve_run()
        checks.append(_compare_json("resolve_run", sqlite_run, postgres_run))

        run_id = str((sqlite_run or {}).get("run_id") or "")
        sqlite_state = sqlite_persistence.get_run_state_payload(run_id=run_id or None)
        postgres_state = postgres_persistence.get_run_state_payload(run_id=run_id or None)
        checks.append(_compare_json("state_payload", sqlite_state, postgres_state))

        sqlite_quality = sqlite_persistence.get_quality_cards_payload(run_id=run_id or None)
        postgres_quality = postgres_persistence.get_quality_cards_payload(run_id=run_id or None)
        checks.append(_compare_json("quality_payload", sqlite_quality, postgres_quality))

        sqlite_filters = sqlite_persistence.get_cloud2_filter_options(run_id=run_id or None)
        postgres_filters = postgres_persistence.get_cloud2_filter_options(run_id=run_id or None)
        checks.append(_compare_json("cloud2_filters", sqlite_filters, postgres_filters))

        sqlite_probe_settings = sqlite_persistence.load_probe_settings()
        postgres_probe_settings = postgres_persistence.load_probe_settings()
        checks.append(_compare_json("probe_settings", sqlite_probe_settings, postgres_probe_settings))

        sqlite_pending = sqlite_persistence.load_expected_pivots_pending()
        postgres_pending = postgres_persistence.load_expected_pivots_pending()
        checks.append(_compare_json("expected_pivots_pending", sqlite_pending, postgres_pending))

        sqlite_cards_history = sqlite_persistence.fetch_summary_cards_hourly_history(limit=24)
        postgres_cards_history = postgres_persistence.fetch_summary_cards_hourly_history(limit=24)
        checks.append(_compare_json("summary_cards_hourly", sqlite_cards_history, postgres_cards_history))

        sqlite_connected_history = sqlite_persistence.fetch_connected_pivots_hourly_history(limit=24)
        postgres_connected_history = postgres_persistence.fetch_connected_pivots_hourly_history(limit=24)
        checks.append(_compare_json("connected_pivots_hourly", sqlite_connected_history, postgres_connected_history))

        for pivot_id, session_id in _sample_panel_targets(sqlite_state or {}, limit=sample_limit):
            sqlite_panel = sqlite_persistence.get_panel_payload(pivot_id, session_id=session_id, run_id=run_id or None)
            postgres_panel = postgres_persistence.get_panel_payload(pivot_id, session_id=session_id, run_id=run_id or None)
            checks.append(_compare_json(f"panel:{pivot_id}", sqlite_panel, postgres_panel))

        sqlite_admin = sqlite_auth.ensure_fixed_admin_account()
        postgres_admin = postgres_auth.ensure_fixed_admin_account()
        checks.append(
            _compare_json(
                "fixed_admin",
                {
                    "ok": sqlite_admin.get("ok"),
                    "created": sqlite_admin.get("created"),
                    "email_masked": sqlite_admin.get("email_masked"),
                    "role": sqlite_admin.get("role"),
                },
                {
                    "ok": postgres_admin.get("ok"),
                    "created": postgres_admin.get("created"),
                    "email_masked": postgres_admin.get("email_masked"),
                    "role": postgres_admin.get("role"),
                },
            )
        )
    finally:
        sqlite_persistence.stop()
        postgres_persistence.stop()

    failed = [item for item in checks if not item.get("match")]
    return {
        "ok": not bool(failed),
        "checks": checks,
        "failed": failed,
    }


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Valida compatibilidade SQLite vs PostgreSQL.")
    parser.add_argument("--sqlite-path", required=True, help="Caminho do telemetry.sqlite3")
    parser.add_argument("--database-url", required=True, help="URL PostgreSQL destino")
    parser.add_argument("--migrate-first", action="store_true", help="Executa migracao antes da comparacao")
    parser.add_argument("--sample-limit", type=int, default=5, help="Quantidade de paineis de pivô para comparar")
    parser.add_argument("--report-json", default="", help="Arquivo opcional para salvar relatorio JSON")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    result = compare_backends(
        sqlite_path=args.sqlite_path,
        database_url=args.database_url,
        migrate_first=bool(args.migrate_first),
        sample_limit=max(1, int(args.sample_limit or 5)),
    )
    if args.report_json:
        report_path = os.path.abspath(str(args.report_json))
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2)
    for item in result["checks"]:
        status = "ok" if item.get("match") else "mismatch"
        print(f"{status}: {item['label']}")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
