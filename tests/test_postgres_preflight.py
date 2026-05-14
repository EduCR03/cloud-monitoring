from scripts.postgres_preflight import redact_database_url


def test_redact_database_url_hides_password():
    result = redact_database_url("postgresql://user:secret@example.com:5432/cloudv2")

    assert result == "postgresql://user:***@example.com:5432/cloudv2"
    assert "secret" not in result


def test_redact_database_url_without_password_keeps_user():
    result = redact_database_url("postgresql://user@example.com/cloudv2")

    assert result == "postgresql://user@example.com/cloudv2"
