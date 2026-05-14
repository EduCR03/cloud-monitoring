# PostgreSQL Migration Runbook

## Objetivo

Trocar SQLite por PostgreSQL sem regressao funcional.

## Estrategia

- manter `DB_BACKEND=sqlite` como fallback
- habilitar `DB_BACKEND=postgres` por configuracao
- migrar dados sem destruir origem
- validar SQLite vs PostgreSQL lado a lado
- so depois trocar producao

## Variaveis

### SQLite atual

```env
DB_BACKEND=sqlite
SQLITE_DB_PATH=/data/telemetry.sqlite3
```

### PostgreSQL

```env
DB_BACKEND=postgres
DATABASE_URL=postgresql://usuario:senha@host:5432/cloudv2
```

## Migracao de dados

1. Fazer backup do SQLite.
2. Garantir banco PostgreSQL vazio ou truncavel.
3. Executar:

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path /data/telemetry.sqlite3 \
  --database-url postgresql://usuario:senha@host:5432/cloudv2
```

4. Verificar contagens tabela a tabela.

## Validacao comparativa

### Local com Docker

No Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\dev-postgres-validate.ps1
```

No Linux/EC2:

```bash
bash scripts/dev-postgres-validate.sh
```

Isso sobe um PostgreSQL local em `127.0.0.1:55432` e roda:

- smoke real de persistencia
- smoke real de autenticacao
- comparacao SQLite vs PostgreSQL
- testes de migrations

### Manual

Executar comparacao lado a lado:

```bash
python scripts/validate_postgres_migration.py \
  --sqlite-path /data/telemetry.sqlite3 \
  --database-url postgresql://usuario:senha@host:5432/cloudv2 \
  --sample-limit 5
```

Com migracao automatica antes da comparacao:

```bash
python scripts/validate_postgres_migration.py \
  --sqlite-path /data/telemetry.sqlite3 \
  --database-url postgresql://usuario:senha@host:5432/cloudv2 \
  --migrate-first \
  --sample-limit 5 \
  --report-json ./postgres-compare-report.json
```

## Checklist funcional

- login admin
- login usuario comum
- dashboard principal
- cards resumo
- hover charts
- filtros cloud2
- painel individual do pivô
- timeline
- probe settings
- fila descoberta
- mapa
- cards de historico horario

## Rollback

Se qualquer validacao falhar:

1. restaurar:

```env
DB_BACKEND=sqlite
SQLITE_DB_PATH=/data/telemetry.sqlite3
```

2. remover ou ignorar `DATABASE_URL`
3. reiniciar backend

Rollback operacional nao depende de migrar dados de volta.
Origem SQLite continua preservada.

## Riscos remanescentes

- smoke real PostgreSQL ainda depende ambiente com servidor ativo
- algumas queries ainda dependem traducao automatica de SQL SQLite
- scripts de deploy ainda assumem SQLite como padrao seguro
- troca final em producao deve acontecer so apos validacao comparativa real
