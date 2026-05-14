from scripts import postgres_real_validation


def test_migrate_and_compare_requires_explicit_confirmation():
    code = postgres_real_validation.main(
        [
            "--sqlite-path",
            "/data/telemetry.sqlite3",
            "--database-url",
            "postgresql://user:secret@example.com/cloudv2",
            "--mode",
            "migrate-and-compare",
        ]
    )

    assert code == 2


def test_run_real_validation_preflight_only(monkeypatch):
    calls = []

    def fake_preflight(database_url, require_empty=False):
        calls.append(("preflight", database_url, require_empty))
        return {"ok": True, "checks": []}

    def fake_compare(*args, **kwargs):
        calls.append(("compare", args, kwargs))
        return {"ok": True}

    monkeypatch.setattr(postgres_real_validation, "run_preflight", fake_preflight)
    monkeypatch.setattr(postgres_real_validation, "compare_backends", fake_compare)

    result = postgres_real_validation.run_real_validation(
        sqlite_path="/data/telemetry.sqlite3",
        database_url="postgresql://user:secret@example.com/cloudv2",
        mode="preflight",
    )

    assert result["ok"] is True
    assert result["database_url"] == "postgresql://user:***@example.com/cloudv2"
    assert calls == [("preflight", "postgresql://user:secret@example.com/cloudv2", False)]


def test_run_real_validation_migrate_and_compare(monkeypatch):
    calls = []

    monkeypatch.setattr(
        postgres_real_validation,
        "run_preflight",
        lambda database_url, require_empty=False: {"ok": True},
    )

    def fake_compare(sqlite_path, database_url, migrate_first=False, sample_limit=5):
        calls.append(
            {
                "sqlite_path": sqlite_path,
                "database_url": database_url,
                "migrate_first": migrate_first,
                "sample_limit": sample_limit,
            }
        )
        return {"ok": True, "checks": []}

    monkeypatch.setattr(postgres_real_validation, "compare_backends", fake_compare)

    result = postgres_real_validation.run_real_validation(
        sqlite_path="/data/telemetry.sqlite3",
        database_url="postgresql://user:secret@example.com/cloudv2",
        mode="migrate-and-compare",
        sample_limit=8,
    )

    assert result["ok"] is True
    assert calls == [
        {
            "sqlite_path": "/data/telemetry.sqlite3",
            "database_url": "postgresql://user:secret@example.com/cloudv2",
            "migrate_first": True,
            "sample_limit": 8,
        }
    ]
