import argparse
import http.cookiejar
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


READ_ENDPOINTS = [
    ("/auth/me", "auth_me"),
    ("/api/state", "state"),
    ("/api/quality-lite", "quality_lite"),
    ("/api/summary/cards-history", "summary_cards_history"),
    ("/api/summary/connected-history", "summary_connected_history"),
    ("/api/monitoring/runs?limit=5", "monitoring_runs"),
]


def normalize_base_url(base_url):
    safe_url = str(base_url or "").strip().rstrip("/")
    if not safe_url:
        raise ValueError("base_url obrigatorio")
    parsed = urllib.parse.urlsplit(safe_url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError("base_url invalido")
    return safe_url


def _json_request(opener, base_url, path, method="GET", body=None, timeout=10):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        urllib.parse.urljoin(base_url + "/", path.lstrip("/")),
        data=data,
        method=method,
        headers=headers,
    )
    with opener.open(request, timeout=timeout) as response:
        raw_body = response.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw_body) if raw_body else {}
        except json.JSONDecodeError:
            payload = {"raw": raw_body}
        return int(response.status), payload


def _check_json(label, fn):
    try:
        status, payload = fn()
        ok = 200 <= int(status) < 300
        if isinstance(payload, dict) and payload.get("ok") is False:
            ok = False
        return {
            "label": label,
            "ok": ok,
            "status": int(status),
            "details": _summarize_payload(label, payload),
        }
    except urllib.error.HTTPError as exc:
        return {"label": label, "ok": False, "status": int(exc.code), "error": exc.reason}
    except Exception as exc:
        return {"label": label, "ok": False, "error": str(exc)}


def _summarize_payload(label, payload):
    if not isinstance(payload, dict):
        return {"type": type(payload).__name__}
    if label == "state":
        pivots = payload.get("pivots") if isinstance(payload.get("pivots"), list) else []
        return {
            "pivots": len(pivots),
            "run_id": payload.get("run_id"),
            "has_counts": isinstance(payload.get("counts"), dict),
        }
    if label == "quality_lite":
        return {"has_cards": bool(payload)}
    if label in ("summary_cards_history", "summary_connected_history"):
        points = payload.get("points") if isinstance(payload.get("points"), list) else []
        return {"points": len(points)}
    if label == "monitoring_runs":
        runs = payload.get("runs") if isinstance(payload.get("runs"), list) else []
        return {"runs": len(runs)}
    if label == "auth_me":
        user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
        return {
            "authenticated": bool(payload.get("authenticated") or user),
            "role": user.get("role"),
            "email": user.get("email"),
        }
    return {"keys": sorted(str(key) for key in payload.keys())[:10]}


def run_smoke(base_url, email="", password="", timeout=10):
    safe_base_url = normalize_base_url(base_url)
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cookie_jar))
    result = {
        "ok": False,
        "base_url": safe_base_url,
        "authenticated": False,
        "checks": [],
    }

    result["checks"].append(
        _check_json(
            "health",
            lambda: _json_request(opener, safe_base_url, "/api/health", timeout=timeout),
        )
    )

    safe_email = str(email or "").strip()
    safe_password = str(password or "")
    if safe_email or safe_password:
        login_check = _check_json(
            "login",
            lambda: _json_request(
                opener,
                safe_base_url,
                "/auth/login",
                method="POST",
                body={"email": safe_email, "password": safe_password},
                timeout=timeout,
            ),
        )
        result["checks"].append(login_check)
        result["authenticated"] = bool(login_check.get("ok"))

        if login_check.get("ok"):
            for path, label in READ_ENDPOINTS:
                result["checks"].append(
                    _check_json(
                        label,
                        lambda path=path: _json_request(opener, safe_base_url, path, timeout=timeout),
                    )
                )

    result["ok"] = all(bool(item.get("ok")) for item in result["checks"])
    return result


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Smoke test HTTP read-only do dashboard.")
    parser.add_argument("--base-url", default=os.environ.get("SMOKE_BASE_URL", "http://127.0.0.1:8008"))
    parser.add_argument("--email", default=os.environ.get("SMOKE_EMAIL", ""))
    parser.add_argument("--password", default=os.environ.get("SMOKE_PASSWORD", ""))
    parser.add_argument("--timeout", type=float, default=float(os.environ.get("SMOKE_TIMEOUT_SEC", "10")))
    parser.add_argument("--report-json", default=os.environ.get("SMOKE_REPORT_JSON", ""))
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    result = run_smoke(
        base_url=args.base_url,
        email=args.email,
        password=args.password,
        timeout=max(1.0, float(args.timeout or 10)),
    )
    if args.report_json:
        report_path = os.path.abspath(str(args.report_json))
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as handle:
            json.dump(result, handle, ensure_ascii=False, indent=2)

    print(f"base_url: {result['base_url']}")
    for item in result["checks"]:
        status = "ok" if item.get("ok") else "fail"
        print(f"{status}: {item['label']}")
        if item.get("error"):
            print(f"  {item['error']}")
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
