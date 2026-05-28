# Sandbox Proxy Requirements - cloud-monitoring

## Projeto

- Nome do projeto: cloud-monitoring
- Repositorio: Monitoramento-de-Conectividade/cloud-monitoring
- Responsavel: Soil Tecnologia
- Ambiente-alvo: EC2 sandbox
- Tipo de aplicacao: backend/API com dashboard web estatico servido pelo proprio backend

## Dominio publico

| Dominio | Servico destino | Porta | Observacao |
|---|---|---:|---|
| sentinel.soiltech.com.br | cloud-monitoring-backend | 8008 | Dominio principal da API/dashboard |
| back-cloud-monitor.duckdns.org | cloud-monitoring-backend | 8008 | Dominio alternativo legado |

## Servicos publicados

| Servico | Container name | Porta interna | Protocolo | Observacao |
|---|---|---:|---|---|
| backend | cloud-monitoring-backend | 8008 | HTTP | API, autenticacao, dashboard e monitor MQTT |

## Servicos internos

| Servico | Container name | Porta interna | Tipo | Observacao |
|---|---|---:|---|---|
| sqlite | volume `cloud_monitoring_data` | n/a | Banco | Persistido em `/data/telemetry.sqlite3` |
| postgres | cloud-monitoring-postgres | 5432 | Banco | Apenas profile local `postgres`; nao conectar a `proxy-net` |

## Roteamento esperado

- `/` -> `cloud-monitoring-backend:8008`
- `/login` -> `cloud-monitoring-backend:8008`
- `/api/*` -> `cloud-monitoring-backend:8008`
- `/auth/*` -> `cloud-monitoring-backend:8008`

## Bloco Caddy sugerido

```caddyfile
back-cloud-monitor.duckdns.org, sentinel.soiltech.com.br {
    encode zstd gzip

    reverse_proxy cloud-monitoring-backend:8008 {
        header_up Host {host}
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
        header_up X-Forwarded-Host {host}
    }
}
```

## Redes Docker

Servicos que devem estar na `proxy-net`:

- cloud-monitoring-backend

Servicos que devem ficar apenas na rede interna do projeto:

- cloud-monitoring-postgres, quando o profile `postgres` for usado para validacoes locais

Rede externa esperada:

```bash
docker network create proxy-net
```

O nome pode ser sobrescrito por `SANDBOX_PROXY_NETWORK`, mas o padrao da EC2 sandbox e `proxy-net`.

## Healthchecks

| Servico | Endpoint | Resultado esperado |
|---|---|---|
| backend interno | `http://127.0.0.1:8008/login` dentro do container | HTTP 200 |
| backend API | `/api/health` via sandbox-proxy | HTTP 200 |
| backend login | `/login` via sandbox-proxy | HTTP 200 |

## Variaveis obrigatorias

| Variavel | Uso |
|---|---|
| BROKER | Endpoint MQTT/AWS IoT |
| MQTT_PORT | Porta MQTT, normalmente `8883` |
| CORS_ALLOWED_ORIGINS | Origem publica do frontend quando separado do backend |
| AUTH_BASE_URL | URL HTTPS publica exposta pelo sandbox-proxy |
| AUTH_COOKIE_SAMESITE | Usar `None` quando frontend e backend estiverem em dominios diferentes |
| AUTH_COOKIE_SECURE | Usar `1` em HTTPS publico |
| DB_BACKEND | `sqlite` por padrao; `postgres` em cutover validado |
| SQLITE_DB_PATH | Caminho do SQLite, normalmente `/data/telemetry.sqlite3` |
| DATABASE_URL | Obrigatoria somente quando `DB_BACKEND=postgres` |
| AUTH_FIXED_ADMIN_EMAIL | Admin inicial/fixo |
| AUTH_FIXED_ADMIN_PASSWORD | Senha do admin inicial/fixo |

## Arquivos obrigatorios

| Caminho no host | Caminho no container | Uso |
|---|---|---|
| `./cloudv2-config.json` | `/app/cloudv2-config.json` | Configuracao base do monitor |
| `./certs/amazon_ca.pem` | `/app/certs/amazon_ca.pem` | CA AWS IoT |
| `./certs/device.pem.crt` | `/app/certs/device.pem.crt` | Certificado do dispositivo |
| `./certs/private.pem.key` | `/app/certs/private.pem.key` | Chave privada do dispositivo |

## Volumes persistentes

| Volume ou bind mount | Uso |
|---|---|
| `cloud_monitoring_data` | SQLite, relatorios e artefatos persistidos em `/data` |
| `cloud_monitoring_postgres_data` | Dados do PostgreSQL local no profile `postgres` |
| `./logs_mqtt` | Logs MQTT persistidos no host |
| `./certs` | Certificados MQTT montados como somente leitura |

## Migracao inicial

O backend aplica automaticamente as migrations SQLite/PostgreSQL presentes em `backend/migrations` e `backend/migrations_postgres` durante a inicializacao do repositorio de persistencia.

Para cutover PostgreSQL, usar os runbooks e scripts existentes:

```bash
DATABASE_URL=postgresql://usuario:senha@host:5432/cloudv2 bash scripts/ec2-postgres-real-validate.sh
APPLY_CUTOVER=1 DATABASE_URL=postgresql://usuario:senha@host:5432/cloudv2 bash scripts/ec2-postgres-cutover.sh
```

## Seed inicial

Nao ha seed separada obrigatoria para dados de monitoramento. O admin inicial pode ser criado pelas variaveis `AUTH_FIXED_ADMIN_*`.

## Observacoes de seguranca

- O projeto nao deve publicar `80:80` nem `443:443`.
- O backend usa `expose: 8008`, nao `ports`, em operacao na EC2 sandbox.
- TLS publico, certificados HTTPS e roteamento externo sao responsabilidade do `sandbox-proxy`.
- O backend deve permanecer em HTTP interno atras do `sandbox-proxy`.
- SQLite permanece no volume `cloud_monitoring_data`.
- PostgreSQL local existe apenas no profile `postgres`; nao deve entrar na `proxy-net`.
- Certificados MQTT e variaveis sensiveis devem ficar fora do Git.

## Checklist

- [x] Servicos publicados usam `expose`, nao `ports`.
- [x] Servicos publicados estao na `proxy-net`.
- [x] Banco fica fora da `proxy-net`.
- [x] Projeto nao publica `80:80` nem `443:443`.
- [x] Bloco Caddy sugerido foi documentado.
- [x] Healthchecks foram documentados.
- [x] Variaveis obrigatorias foram documentadas.
- [x] Volumes persistentes foram documentados.
