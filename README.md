# cloud-monitoring

Monitoramento robusto de conectividade e saúde de tráfego dos pivots via MQTT com:

- Assinatura fixa e somente leitura dos tópicos: `cloudv2`, `cloudv2-ping`, `cloud2`, `cloudv2-network`, `cloudv2-info`.
- Auto-descoberta de pivots apenas por mensagens em `cloudv2`.
- Parser resiliente para payload `#<IDP>-<PIVOT_ID>-...$` com descarte seguro de malformados.
- Cálculo dinâmico da mediana de intervalo de `cloudv2` por pivot (janela móvel).
- Status consolidado por pivot com tolerância de 50%.
- Probe ativo seletivo com `#11$` publicando apenas no tópico dinâmico `pivot_id`.
- Dashboard web com visão geral e visão por pivot (timeline, métricas e controle de probe).

## Estrutura

- `backend/`: backend (monitor MQTT, telemetria, persistência, dashboard HTTP e utilitários).
- `frontend/`: frontend (assets `index.html`, `dashboard.css`, `dashboard.js`).
- Wrappers legados no root: `cloudv2-ping-monitoring.py`, `cloudv2_fixture_simulator.py`, `cloudv2-config-ui.py`.

## Regras críticas de tópicos

- O sistema **não publica** em: `cloudv2`, `cloudv2-ping`, `cloud2`, `cloudv2-network`, `cloudv2-info`.
- O único publish permitido é no tópico dinâmico `pivot_id` do equipamento, com payload `#11$`.

## Regra de desconexão (offline)

Um pivot é considerado **desconectado** quando:

- o tempo desde a última mensagem recebida em qualquer tópico de conectividade monitorado
  (`cloudv2`, `cloudv2-ping`, `cloudv2-info`, `cloudv2-network`)
- excede `1.25 * max_mediana_esperada_por_topico` do próprio pivot.

Ou seja, o limite usa a maior mediana entre esses tópicos para o pivot e aplica fator de tolerância `1.25`.

## Como executar

1. Ajuste `cloudv2-config.json` (broker, porta e parâmetros de monitoramento).
2. Inicie o monitor:

```bash
python backend/run_monitor.py
```

Por padrao o hot reload de desenvolvimento ja vem ativo (reinicia o processo em mudanca de `.py/.html/.css/.js`).
Se quiser desativar:

```bash
$env:CLOUDV2_DEV_HOT_RELOAD="0"; python backend/run_monitor.py
```

3. Abra:

```text
http://localhost:8008/login
```

### Banco de dados

Modo atual, seguro:

- `DB_BACKEND=sqlite`
- `SQLITE_DB_PATH=/data/telemetry.sqlite3`

Modo PostgreSQL em migracao:

- `DB_BACKEND=postgres`
- `DATABASE_URL=postgresql://usuario:senha@host:5432/cloudv2`

Rollback simples:

- voltar `DB_BACKEND=sqlite`
- manter `SQLITE_DB_PATH`
- reiniciar backend

## Fluxo de desenvolvimento local isolado

Se quiser testar mudancas grandes sem encostar no fluxo atual que esta em producao, use uma branch separada, por exemplo:

```bash
git checkout -b dev
```

O projeto agora aceita um backend local com banco/dados isolados e um frontend local separado:

1. Suba o backend local com banco proprio:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\dev-backend.ps1
```

Isso usa:
- `CLOUDV2_DATA_DIR=.local-dev/data`
- `SQLITE_DB_PATH=.local-dev/data/telemetry.sqlite3`
- cookies/CORS proprios para frontend local
- `AUTH_DISABLE_RATE_LIMIT=1` no ambiente local
- conta admin local da `dev`: `admin-dev@local.test` / `31380626ESP32`

2. Em outro terminal, suba o frontend local:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\dev-frontend.ps1
```

3. Abra:

```text
http://127.0.0.1:4173/index.html
```

Quando o frontend roda em `localhost`/`127.0.0.1`, `frontend/runtime-config.js` passa a apontar automaticamente para `http://127.0.0.1:8008`.

Se quiser que o frontend local bata em outro backend sem editar arquivo versionado:

```javascript
localStorage.setItem("cloudv2.apiBaseUrl", "https://SEU_BACKEND");
location.reload();
```

Para voltar ao backend local:

```javascript
localStorage.removeItem("cloudv2.apiBaseUrl");
location.reload();
```

Esse fluxo deixa o codigo atual intacto e isola os testes locais em `.local-dev/`, sem misturar com a versao que voce mantem em producao.

Documentacao operacional completa da branch `dev`:

- `DEV_BRANCH_WORKFLOW.md`

Protecao adicional:

- mock/dados locais devem ficar fora do Git
- o repositorio possui uma checagem automatica que falha se arquivos de `.local-dev/` ou `frontend/data/` (exceto `.gitkeep`) forem versionados

## Autenticacao e LGPD

- Todas as rotas e APIs do dashboard exigem autenticacao.
- Fluxos disponiveis:
  - `GET /login`
  - `GET /register`
  - `GET /verify-email`
  - `GET /forgot-password`
  - `GET /reset-password?token=...`
- Endpoints principais:
  - `POST /auth/register`
  - `POST /auth/login`
  - `POST /auth/logout`
  - `GET /auth/verify?token=...`
  - `POST /auth/resend-verification`
  - `POST /auth/forgot-password`
  - `POST /auth/reset-password`
  - `GET /auth/me`
- Direitos do titular:
  - `GET /account/export`
  - `POST /account/delete`

### Conta admin fixa (seed automatico)

- Ao iniciar o servidor, uma conta admin global e criada/atualizada automaticamente com e-mail ja verificado.
- No fluxo local da branch `dev`, `scripts/dev-backend.ps1` sobe esta credencial:
  - `admin-dev@local.test`
  - senha: `31380626ESP32`
- Fora do fluxo local, prefira definir explicitamente as variaveis de ambiente do admin fixo em vez de depender de valores padrao.
- Pode customizar por variaveis:
  - `AUTH_FIXED_ADMIN_ENABLED` (default `1`)
  - `AUTH_FIXED_ADMIN_EMAIL`
  - `AUTH_FIXED_ADMIN_PASSWORD`
  - `AUTH_FIXED_ADMIN_NAME`
  - `AUTH_FIXED_ADMIN_FORCE_PASSWORD` (default `1`)

### E-mail de verificacao e reset

- DEV: por padrao, links sao exibidos no console.
- PROD: configure SMTP via variaveis:
  - `AUTH_EMAIL_MODE=smtp`
  - `AUTH_SMTP_HOST`
  - `AUTH_SMTP_PORT`
  - `AUTH_SMTP_USER`
  - `AUTH_SMTP_PASSWORD`
  - `AUTH_SMTP_FROM`
  - opcional: `AUTH_SMTP_STARTTLS=1`, `AUTH_SMTP_SSL=0`

Sem `AUTH_SMTP_*` validos, o sistema permanece em `AUTH_EMAIL_MODE=console` e nao envia e-mail real.
Se apenas `AUTH_SMTP_USER`/`AUTH_SMTP_PASSWORD` forem definidos, o backend tenta inferir host por provedor:
- Gmail: `smtp.gmail.com`
- Outlook/Hotmail/Live: `smtp.office365.com`
- Yahoo: `smtp.mail.yahoo.com`

### Seguranca

- Senha com hash `scrypt` (nunca em texto puro).
- Tokens de verificacao/reset armazenados apenas em hash.
- Sessao por cookie `HttpOnly` com `SameSite=Lax` e `Secure` quando HTTPS.
- Rate limit basico aplicado em login/reenvio/esqueci senha.

## Simulador/fixture

O simulador valida:

- auto-descoberta por `cloudv2`;
- deduplicação;
- mediana móvel;
- estados `gray`, `yellow`, `critical`, `red`;
- evento `cloud2` com tecnologia `wifi`;
- ciclo completo de probe (`sent`/`response`/`timeout`);
- garantia de publicação somente em tópico dinâmico.

Execute:

```bash
python backend/run_fixture_simulator.py
```

O script imprime um JSON com todos os checks e retorna código `0` em sucesso.

## Dashboard

### Visão principal

- Busca por `pivot_id`.
- Filtro de status (`green`, `yellow`, `critical`, `red`, `gray`).
- Ordenação por criticidade e atividade.
- Cards com:
  - `pivot_id`;
  - status;
  - último ping;
  - último pacote `cloudv2`;
  - mediana estimada + número de amostras;
  - último `cloud2` (RSSI/tecnologia/firmware).

### Visão por pivot

- Timeline de conectividade (verde/vermelho) por período:
  - `Total`, `24h`, `48h`, `7d`, `15d`, `30d` ou `Por periodo (De/Ate)`.
  - Verde = conectado, Vermelho = desconectado.
  - Construída com base em eventos `cloudv2-ping`, `cloudv2` e `cloud2`.
- Métricas:
  - quedas em 24h e 7d;
  - última duração de queda;
  - último RSSI/tecnologia (incluindo `wifi`);
  - firmware e datas.
- Controle de probe por pivot:
  - habilitar/desabilitar;
  - ajustar intervalo;
  - salvar via API.

## Configuração relevante

Campos importantes em `cloudv2-config.json`:

- `ping_interval_minutes` (base do cálculo de ping esperado).
- `tolerance_factor` (padrão `3.0`).
- `attention_disconnected_pct_threshold` (padrão `20.0` para status de atenção).
- `critical_disconnected_pct_threshold` (padrão `50.0` para status crítico).
- `cloudv2_median_window` e `cloudv2_min_samples` (padrao: `5`).
- `probe_default_interval_sec`, `probe_min_interval_sec`, `probe_timeout_factor`.
- `history_retention_hours` (mínimo 24h).
- `dedupe_window_sec`.
- `probe_settings`:

```json
{
  "probe_settings": {
    "PioneiraLEM_2": { "enabled": true, "interval_sec": 300 },
    "NovaBahia_6": { "enabled": false, "interval_sec": 300 }
  }
}
```

## Compatibilidade

- Wrappers legados continuam funcionando:
  - `python cloudv2-ping-monitoring.py`
  - `python cloudv2_fixture_simulator.py`
  - `python cloudv2-config-ui.py`
- Variável opcional:
  - `CLOUDV2_WEB_DIR` para forçar o diretório do frontend servido pelo backend.

## Deploy no Vercel (quick start)

1. Crie um projeto no Vercel com:
   - Framework preset: `Other`
   - Root Directory: `frontend`
   - Build Command: vazio
   - Output Directory: `.`
2. Versione `frontend/vercel.json` para rewrites das rotas:
   - `/login`, `/register`, `/verify-email`, `/forgot-password`, `/reset-password`, `/privacy-policy`
3. Configure `frontend/runtime-config.js`:

```js
window.CLOUDV2_API_BASE_URL = "https://SEU_BACKEND_API";
```

Observacao:
- o painel web nao exige mais acao manual de "iniciar novo monitoramento/carregar historico";
- no deploy Docker, o backend sobe em modo continuo com:
  - `REQUIRE_APPLY_TO_START=0`
  - `HISTORY_MODE=merge`

## Split deploy: backend AWS + frontend Vercel

Sim, a estrutura atual permite separar:
- backend (MQTT + SQLite + API/auth) rodando 24/7 em VM AWS;
- frontend estatico no Vercel consumindo API do backend.

### 1) Backend na AWS (VM)

- Rode o monitor como servico (ex.: systemd) com `python backend/run_monitor.py`.
- Publique o HTTP com HTTPS usando Caddy, por exemplo `https://api.seudominio.com`.
- Configure variaveis importantes:
  - `CORS_ALLOWED_ORIGINS=https://seu-frontend.vercel.app`
  - `AUTH_COOKIE_SAMESITE=None`
  - `AUTH_COOKIE_SECURE=1`
  - `AUTH_BASE_URL=https://api.seudominio.com`
  - `CLOUDV2_DEV_HOT_RELOAD=0`

### 2) Frontend no Vercel

- Publique apenas a pasta `frontend/` no projeto Vercel.
- Use `frontend/vercel.json` para rotas amigaveis.
- Configure `frontend/runtime-config.js`:

```js
window.CLOUDV2_API_BASE_URL = "https://api.seudominio.com";
```

### 3) Observacoes de autenticacao

- Em split de dominio (Vercel <> AWS), login usa cookie cross-site:
  - frontend envia `credentials: include`;
  - backend responde com CORS restrito e cookie `SameSite=None; Secure`.
- Se quiser manter tudo em mesma origem (sem CORS/cookie cross-site), use proxy reverso para servir frontend e API no mesmo dominio.

## Deploy mais simples na AWS com Docker (recomendado)

### Proxy oficial: Caddy

Este projeto assume Caddy como proxy HTTPS da EC2.

- O backend continua ouvindo HTTP interno na porta `8008`.
- O Caddy faz TLS e encaminha para o backend.
- O script antigo de HTTPS agora gera e valida bloco Caddyfile.
- Nenhum script deste repo deve sobrescrever o Caddy compartilhado do Fleet.

Bloco Caddyfile minimo:

```caddyfile
api.seudominio.com {
    encode zstd gzip

    reverse_proxy 127.0.0.1:8008 {
        header_up Host {host}
        header_up X-Real-IP {remote_host}
        header_up X-Forwarded-For {remote_host}
        header_up X-Forwarded-Proto {scheme}
        header_up X-Forwarded-Host {host}
    }
}
```

Se o Caddy roda em outro Docker stack, mantenha `BACKEND_BIND_ADDRESS=0.0.0.0` e use no Caddy o upstream que o container Caddy consegue alcançar.

Se quiser conectar o backend direto na rede Docker do Caddy:

```bash
CADDY_DOCKER_NETWORK=proxy_default \
docker compose -f docker-compose.yml -f docker-compose.caddy.yml up -d --build backend
```

Neste modo, o upstream no Caddy pode ser:

```caddyfile
reverse_proxy cloud-monitoring-backend:8008
```

Para gerar e validar o bloco:

```bash
DOMAIN=api.seudominio.com \
FRONTEND_URL=https://SEU_FRONTEND.vercel.app \
BACKEND_UPSTREAM=http://127.0.0.1:8008 \
bash scripts/ec2-setup-https.sh
```

### 1) Setup inicial na EC2 (uma vez)

```bash
git clone https://github.com/Monitoramento-de-Conectividade/cloud-monitoring.git
cd cloud-monitoring
bash scripts/ec2-install-docker.sh
cp .env.backend.example .env.backend
mkdir -p certs logs_mqtt
```

Copie os certificados para `certs/`:
- `certs/amazon_ca.pem`
- `certs/device.pem.crt`
- `certs/private.pem.key`

Edite `.env.backend` com seus dados reais.

### Setup em 1 comando (sem nano)

Se quiser evitar edicao manual de `.env.backend`, use:

```bash
FRONTEND_URL=https://SEU_FRONTEND.vercel.app \
BACKEND_URL=https://SEU_BACKEND_API \
ADMIN_EMAIL=eduardocostar03@gmail.com \
ADMIN_PASSWORD='SUA_SENHA_FORTE' \
bash scripts/ec2-one-command.sh
```

O script:
- faz pull da branch;
- cria `.env.backend`;
- sobe `docker compose up -d --build backend`.

Observacao:
- antes disso, coloque os certificados em `certs/amazon_ca.pem`, `certs/device.pem.crt`, `certs/private.pem.key`.

### 2) Subir backend 24/7

```bash
docker compose up -d --build backend
docker compose ps
```

O container usa `restart: unless-stopped`, então volta sozinho após reboot da VM (com serviço Docker ativo).

### 3) Atualizar backend após novos commits

```bash
BRANCH=main APP_DIR=$HOME/cloud-monitoring bash scripts/ec2-deploy-backend.sh
```

### 4) Variáveis essenciais para frontend no Vercel

- `CORS_ALLOWED_ORIGINS=https://SEU_FRONTEND.vercel.app`
- `AUTH_COOKIE_SAMESITE=None`
- `AUTH_COOKIE_SECURE=1`
- `AUTH_BASE_URL=https://SEU_BACKEND_API`

E no frontend (`frontend/runtime-config.js`):

```js
window.CLOUDV2_API_BASE_URL = "https://SEU_BACKEND_API";
```

Importante: para cookie cross-site funcionar no login, a API da AWS deve estar em HTTPS.

Importante para Caddy:

- mantenha `AUTH_BASE_URL` com a URL HTTPS publica do Caddy;
- mantenha `AUTH_COOKIE_SECURE=1`;
- mantenha `AUTH_COOKIE_SAMESITE=None` quando frontend e API estao em dominios diferentes;
- garanta que o Caddy envie `X-Forwarded-Proto` e `X-Forwarded-Host`.

## Deploy automático por commit (opcional)

Workflow incluído: `.github/workflows/deploy-aws-backend.yml`.

Secrets obrigatórios no GitHub:
- `AWS_EC2_HOST`
- `AWS_EC2_USER`
- `AWS_EC2_SSH_KEY`
- `VERCEL_FRONTEND_DEPLOY_HOOK_URL`

Secrets opcionais no GitHub:
- opcional: `AWS_EC2_PORT` (default `22`)
- opcional: `AWS_EC2_APP_DIR` (default `$HOME/cloud-monitoring`)
- opcional: `AWS_REPO_URL`

### Troubleshooting rapido

- Erro no workflow `Secret obrigatorio ausente: AWS_EC2_HOST`:
  - confirme os secrets no mesmo repositorio onde o GitHub Action esta rodando.
  - preencha `AWS_EC2_HOST`, `AWS_EC2_USER` e `AWS_EC2_SSH_KEY`.
- Frontend abre mas API falha por CORS:
  - em `.env.backend`, use `CORS_ALLOWED_ORIGINS` com o dominio final do Vercel (ex.: `https://cloud-monitoring.vercel.app`).

## Auto-recovery da EC2 sem abrir console

Para evitar reinicio manual da VM quando houver queda, configure alarmes CloudWatch com AWS CLI:

- `StatusCheckFailed_System` -> `ec2:recover` (tentativa automatica de recovery da instancia)
- `StatusCheckFailed_Instance` -> `ec2:reboot` (reboot automatico da instancia)

Com a AWS CLI configurada, rode:

```bash
INSTANCE_ID=i-0123456789abcdef \
AWS_REGION=us-east-1 \
bash scripts/ec2-enable-auto-recovery.sh
```

Opcional:

- `ALARM_PREFIX=meu-projeto` para customizar o nome dos alarmes.

## Perfil recomendado para EC2 pequena (t2.micro + gp2 30GiB)

Se sua instancia for pequena (1 vCPU / 1 GiB RAM), use limites de runtime para reduzir escrita em disco:

- `DASHBOARD_REFRESH_SEC=20`
- `API_STATE_CACHE_TTL_SEC=5`
- `API_QUALITY_CACHE_TTL_SEC=5`
- `MAX_EVENTS_PER_PIVOT=800`
- `MAX_EVENTS_PER_PIVOT_PANEL=800`
- `MAX_EVENTS_PER_PIVOT_LIST=400`
- `PROBE_DEFAULT_INTERVAL_SEC=900`
- `PROBE_MIN_INTERVAL_SEC=180`

Coloque essas variaveis no `.env.backend` antes de subir o container.

Importante:

- `gp2` em 30 GiB tem baseline baixo de IOPS (minimo 100) e pode esgotar burst sob carga continua.
- Para estabilidade, prefira migrar o volume para `gp3` (mesmo tamanho) com IOPS/throughput provisionados.
