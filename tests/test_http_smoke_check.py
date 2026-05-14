import pytest

from scripts.http_smoke_check import normalize_base_url, run_smoke


def test_normalize_base_url_trims_trailing_slash():
    assert normalize_base_url("https://example.com/") == "https://example.com"


def test_normalize_base_url_rejects_invalid_url():
    with pytest.raises(ValueError):
        normalize_base_url("example.com")


def test_run_smoke_without_credentials_only_checks_health(monkeypatch):
    def fake_json_request(_opener, _base_url, path, method="GET", body=None, timeout=10):
        assert path == "/api/health"
        return 200, {"ok": True}

    monkeypatch.setattr("scripts.http_smoke_check._json_request", fake_json_request)

    result = run_smoke("http://127.0.0.1:8008")

    assert result["ok"] is True
    assert result["authenticated"] is False
    assert [item["label"] for item in result["checks"]] == ["health"]


def test_run_smoke_with_credentials_checks_read_endpoints(monkeypatch):
    requested = []

    def fake_json_request(_opener, _base_url, path, method="GET", body=None, timeout=10):
        requested.append((method, path))
        if path == "/auth/login":
            assert body == {"email": "admin@example.com", "password": "secret"}
            return 200, {"ok": True}
        if path == "/auth/me":
            return 200, {"authenticated": True, "user": {"role": "admin", "email": "admin@example.com"}}
        if path == "/api/state":
            return 200, {"pivots": [], "counts": {}, "run_id": "run"}
        if path == "/api/monitoring/runs?limit=5":
            return 200, {"runs": []}
        return 200, {"points": []}

    monkeypatch.setattr("scripts.http_smoke_check._json_request", fake_json_request)

    result = run_smoke("http://127.0.0.1:8008", email="admin@example.com", password="secret")

    assert result["ok"] is True
    assert result["authenticated"] is True
    assert ("POST", "/auth/login") in requested
    assert ("GET", "/api/state") in requested
