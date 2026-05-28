from pathlib import Path


COMPOSE = Path("docker-compose.yml")
ENV_EXAMPLE = Path(".env.backend.example")
CADDY_EXAMPLE = Path("Caddyfile.cloud-monitoring.example")
REQUIREMENTS = Path("cloud-monitoring-sandbox-proxy.requirements.md")
EC2_DEPLOY = Path("scripts/ec2-deploy-backend.sh")
EC2_ONE_COMMAND = Path("scripts/ec2-one-command.sh")


def _backend_compose_block():
    content = COMPOSE.read_text(encoding="utf-8")
    return content.split("  backend:", 1)[1].split("\nvolumes:", 1)[0]


def test_backend_is_exposed_only_to_sandbox_proxy_network():
    content = COMPOSE.read_text(encoding="utf-8")
    backend_block = _backend_compose_block()

    assert "ports:" not in backend_block
    assert 'expose:\n      - "8008"' in backend_block
    assert "- proxy-net" in backend_block
    assert "proxy-net:" in content
    assert 'name: "${SANDBOX_PROXY_NETWORK:-proxy-net}"' in content


def test_legacy_host_port_variables_are_not_required_for_ec2_backend():
    for path in (ENV_EXAMPLE, EC2_DEPLOY, EC2_ONE_COMMAND):
        content = path.read_text(encoding="utf-8")
        assert "BACKEND_PUBLIC_PORT" not in content
        assert "BACKEND_BIND_ADDRESS" not in content


def test_sandbox_proxy_requirements_and_caddy_target_container_upstream():
    requirements = REQUIREMENTS.read_text(encoding="utf-8")
    caddy_example = CADDY_EXAMPLE.read_text(encoding="utf-8")

    assert "cloud-monitoring-backend:8008" in requirements
    assert "sentinel.soiltech.com.br" in requirements
    assert "back-cloud-monitor.duckdns.org" in requirements
    assert "cloud-monitoring-backend:8008" in caddy_example
