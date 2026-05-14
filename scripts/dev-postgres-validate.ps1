$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$postgresPort = if ($env:POSTGRES_PUBLIC_PORT) { $env:POSTGRES_PUBLIC_PORT } else { "55432" }
$postgresDb = if ($env:POSTGRES_DB) { $env:POSTGRES_DB } else { "cloudv2" }
$postgresUser = if ($env:POSTGRES_USER) { $env:POSTGRES_USER } else { "postgres" }
$postgresPassword = if ($env:POSTGRES_PASSWORD) { $env:POSTGRES_PASSWORD } else { "postgres" }
$databaseUrl = "postgresql://${postgresUser}:${postgresPassword}@127.0.0.1:${postgresPort}/${postgresDb}"

Set-Location $repoRoot

try {
    docker version | Out-Null
} catch {
    throw "Docker nao esta disponivel. Abra o Docker Desktop e rode novamente."
}

Write-Host "Subindo PostgreSQL local..."
$env:POSTGRES_PUBLIC_PORT = $postgresPort
$env:POSTGRES_DB = $postgresDb
$env:POSTGRES_USER = $postgresUser
$env:POSTGRES_PASSWORD = $postgresPassword
docker compose --profile postgres up -d postgres

Write-Host "Aguardando PostgreSQL..."
$deadline = (Get-Date).AddSeconds(90)
while ($true) {
    try {
        $env:TEST_POSTGRES_URL = $databaseUrl
@'
import os
import psycopg
with psycopg.connect(os.environ["TEST_POSTGRES_URL"], autocommit=True) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
'@ | python -
        break
    } catch {
        if ((Get-Date) -gt $deadline) {
            throw
        }
        Start-Sleep -Seconds 2
    }
}

Write-Host "Rodando validacao PostgreSQL..."
$env:TEST_POSTGRES_URL = $databaseUrl
python -m pytest -q `
    tests/test_postgres_smoke_integration.py `
    tests/test_postgres_compare_integration.py `
    tests/test_postgres_migration_inventory.py `
    tests/test_sqlite_to_postgres_migration_script.py `
    tests/test_validate_postgres_migration_script.py `
    tests/test_db_backend_config.py

Write-Host "PostgreSQL local validado."
