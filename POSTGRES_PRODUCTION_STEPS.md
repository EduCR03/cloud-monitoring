# PostgreSQL Production Steps

## 1. Criar RDS

No AWS Console:

1. Abra `RDS`.
2. Clique `Create database`.
3. Escolha `PostgreSQL`.
4. Use banco inicial `cloudv2`.
5. Salve usuario e senha.
6. Deixe RDS na mesma VPC da EC2.
7. No security group do RDS, libere entrada `TCP 5432` vinda do security group da EC2.
8. Copie o endpoint do RDS.

Monte a URL:

```text
postgresql://USUARIO:SENHA@ENDPOINT-RDS:5432/cloudv2
```

Se a senha tiver caracteres especiais, use URL encoding.

## 2. Criar GitHub secrets

No GitHub:

1. Abra o repositorio.
2. Va em `Settings`.
3. Abra `Secrets and variables`.
4. Abra `Actions`.
5. Clique `New repository secret`.

Crie:

- `EC2_HOST`: IP publico ou DNS da EC2.
- `EC2_USER`: usuario SSH, geralmente `ubuntu` ou `ec2-user`.
- `EC2_SSH_KEY`: chave privada SSH inteira.
- `POSTGRES_DATABASE_URL`: URL PostgreSQL do RDS.
- `SMOKE_EMAIL`: email admin para teste, opcional.
- `SMOKE_PASSWORD`: senha admin para teste, opcional.

Nao coloque esses valores em arquivo do projeto.

## 3. Validar acesso RDS

No GitHub:

1. Abra `Actions`.
2. Abra workflow `PostgreSQL Real Validation`.
3. Clique `Run workflow`.
4. Use branch `dev`.
5. Use `validation_mode=preflight`.
6. Deixe `confirm_target_truncate=false`.
7. Clique `Run workflow`.

Resultado esperado:

```text
ok: preflight
```

## 4. Migrar e comparar

No mesmo workflow:

1. Clique `Run workflow`.
2. Use branch `dev`.
3. Use `validation_mode=migrate-and-compare`.
4. Marque `confirm_target_truncate=true`.
5. Use `sample_limit=8`.
6. Marque `run_smoke_after=true` se criou secrets de smoke.
7. Clique `Run workflow`.

Esse passo nao troca producao para PostgreSQL.
Ele copia SQLite para RDS e compara os dados.

Relatorio fica na EC2:

```text
/data/postgres-real-validation-*.json
```

## 5. Cutover sem aplicar

Entre na EC2 por SSH:

```bash
cd ~/cloud-monitoring
git fetch origin
git checkout dev
git pull --ff-only origin dev
```

Rode validacao final sem trocar banco:

```bash
DATABASE_URL='postgresql://USUARIO:SENHA@ENDPOINT-RDS:5432/cloudv2' \
bash scripts/ec2-postgres-cutover.sh
```

Resultado esperado:

```text
Validacao concluida. Cutover nao aplicado.
```

## 6. Aplicar cutover

Ainda na EC2:

```bash
APPLY_CUTOVER=1 \
DATABASE_URL='postgresql://USUARIO:SENHA@ENDPOINT-RDS:5432/cloudv2' \
SMOKE_EMAIL='email-admin' \
SMOKE_PASSWORD='senha-admin' \
bash scripts/ec2-postgres-cutover.sh
```

O script faz:

- preflight RDS.
- backup SQLite.
- migra dados.
- compara SQLite vs PostgreSQL.
- troca `.env.backend` para `DB_BACKEND=postgres`.
- reinicia backend.
- roda `/api/health`.
- roda smoke HTTP.
- grava manifesto.

Arquivos importantes:

```text
/data/backups/telemetry.<timestamp>.sqlite3
/data/backups/telemetry.<timestamp>.sqlite3.sha256
/data/postgres-compare-report.json
/data/postgres-http-smoke-report.json
/data/postgres-cutover-<timestamp>.manifest.json
```

## 7. Validar tela

Depois do cutover:

1. Abra dashboard em producao.
2. Login admin.
3. Veja cards resumo.
4. Abra um pivo.
5. Confira timeline grande.
6. Confira timeline pequena.
7. Abra mapa.
8. Confira fila descoberta.
9. Teste salvar localizacao.
10. Teste salvar tecnologia.

## 8. Rollback

Rollback sem restaurar arquivo:

```bash
ROLLBACK_ONLY=1 \
bash scripts/ec2-postgres-cutover.sh
```

Rollback restaurando backup SQLite:

```bash
ROLLBACK_ONLY=1 \
ROLLBACK_SQLITE_BACKUP_PATH=/data/backups/telemetry.TIMESTAMP.sqlite3 \
bash scripts/ec2-postgres-cutover.sh
```

Depois validar:

```bash
curl -fsS http://127.0.0.1:8008/api/health
```

## 9. Depois de aprovado

Quando tudo estiver aprovado na `dev`:

1. Fazer merge para `feat/aws-server`.
2. Rodar deploy normal.
3. Repetir preflight.
4. Repetir cutover se necessario.

