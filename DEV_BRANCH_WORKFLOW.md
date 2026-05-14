# Desenvolvimento Seguro na Branch `dev`

Este documento explica como usar a branch `dev` para desenvolver funcionalidades grandes, testar ideias, rodar frontend/backend localmente e trabalhar com liberdade sem afetar a versao estavel.

## Objetivo

A branch `dev` existe para:

- permitir mudancas grandes sem risco para a branch principal de trabalho
- deixar outros programadores testarem ideias novas localmente
- manter banco, runtime e dados locais separados do ambiente principal
- permitir validacao visual no frontend local antes de decidir o que vai para producao

## Papeis das branches

Use as branches assim:

- `main`: historico principal do repositorio
- `feat/aws-server`: branch estavel de trabalho e de deploy
- `dev`: branch de desenvolvimento livre, para testes e mudancas maiores

Regra pratica:

- nada deve ir direto da `dev` para producao sem revisao
- quando uma mudanca ficar pronta e validada, ela deve ser levada da `dev` para `feat/aws-server`
- o merge de `dev` para `feat/aws-server` nao deve ser automatico nem cego; sempre revise o que e local-only

## Fluxo recomendado

### 1. Comecar trabalho novo

Atualize a branch `dev`:

```powershell
git checkout dev
git pull
```

Se for uma mudanca grande ou se varias pessoas estiverem usando `dev`, prefira criar uma branch filha a partir dela:

```powershell
git checkout dev
git pull
git checkout -b dev/nome-da-feature
```

Assim voce continua no fluxo da `dev`, mas sem baguncar o trabalho de outra pessoa.

### 2. Rodar backend local isolado

Suba o backend local:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\dev-backend.ps1
```

Esse script prepara um ambiente isolado para desenvolvimento:

- usa `CLOUDV2_DATA_DIR=.local-dev/data`
- usa SQLite local em `.local-dev/data/telemetry.sqlite3`
- libera CORS para frontend local
- desabilita rate limit de login apenas nesse ambiente local
- sobe uma conta admin propria da branch `dev`
- mantem o ambiente principal intacto

### Credencial admin local da `dev`

Quando voce sobe o backend com `scripts/dev-backend.ps1`, ele forca uma conta admin apenas para o fluxo local de desenvolvimento:

- e-mail: `admin-dev@local.test`
- senha: `31380626ESP32`

Essa e a credencial recomendada para:

- testar login local
- validar telas protegidas
- desenvolver funcionalidades de admin
- onboard de outros programadores na branch `dev`

### 3. Rodar frontend local

Em outro terminal:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\dev-frontend.ps1
```

Abra:

```text
http://127.0.0.1:4173/index.html
```

Quando o frontend roda em `localhost` ou `127.0.0.1`, o arquivo `frontend/runtime-config.js` aponta automaticamente para o backend local `http://127.0.0.1:8008`.

## Dados locais

Tudo que for desenvolvimento local deve ficar fora do Git.

### Local de dados

O ambiente local usa:

```text
.local-dev/
```

Esse diretório esta ignorado no Git e pode conter:

- banco SQLite local
- runtime local
- arquivos temporarios
- dados de teste
- fixtures locais

### O que nunca deve ser commitado

Nunca suba:

- `.local-dev/`
- bancos `.sqlite3` locais
- `runtime_store.json` local
- arquivos de teste temporarios com dados reais
- certificados e segredos locais
- overrides manuais de ambiente feitos so para teste

Regra pratica para mock:

- todo dado mockado/local deve ficar em `.local-dev/`
- nao use `frontend/data/` para guardar mock versionado
- `frontend/data/` deve continuar vazio no Git, exceto `frontend/data/.gitkeep`

Antes de commitar, sempre rode:

```powershell
git status --short --ignored
```

Isso ajuda a confirmar que os dados locais continuam aparecendo como ignorados.

O repositorio tambem possui uma checagem automatica que falha se alguem tentar versionar:

- qualquer arquivo dentro de `.local-dev/`
- qualquer arquivo dentro de `frontend/data/`, exceto `.gitkeep`

## Trocar o frontend local para outro backend

Se quiser usar o frontend local contra outro backend sem editar arquivo versionado, rode no console do navegador:

```javascript
localStorage.setItem("cloudv2.apiBaseUrl", "https://SEU_BACKEND");
location.reload();
```

Para voltar ao backend local:

```javascript
localStorage.removeItem("cloudv2.apiBaseUrl");
location.reload();
```

## Boas praticas de desenvolvimento na `dev`

- faça mudancas grandes aqui antes de pensar em deploy
- use commits pequenos e com objetivo claro
- valide no frontend local sempre que mexer em UI
- valide backend local sempre que mexer em persistencia, auth, timeline ou APIs
- se criar mocks, deixe em `.local-dev/` ou em arquivos claramente temporarios
- se uma ideia ainda estiver incerta, nao misture com codigo pronto para merge

## Testes recomendados antes de subir codigo

Rode pelo menos:

```powershell
node --check frontend/dashboard.js
node --check frontend/mapa.js
node --test tests/connectivity.test.js
python -m pytest -q
```

Se voce mexer em outro arquivo JS isolado, rode `node --check` nele tambem.

## Regras para merge da `dev` para `feat/aws-server`

Antes de levar qualquer mudanca para `feat/aws-server`, revise com cuidado:

### Pode ir para `feat/aws-server`

- melhorias reais de produto
- correcoes de bug
- otimizacoes de backend/frontend
- documentacao util
- suporte a desenvolvimento local que nao altera comportamento padrao de producao

### Precisa revisar antes de ir

- flags de conveniencia para dev
- bypass local de seguranca
- URLs locais ou temporarias
- seeds de usuario apenas para teste
- dados mockados
- scripts que alteram comportamento so para debug

### Pergunta obrigatoria antes do merge

Se esta mudanca for para `feat/aws-server`, o sistema de producao continua com o mesmo comportamento padrao quando nenhuma configuracao local estiver ativa?

Se a resposta nao for um “sim” claro, revise antes de mergear.

## Fluxo de entrega recomendado

Quando a funcionalidade estiver pronta:

1. termine os testes na `dev`
2. revise o diff e remova o que for local-only
3. leve para `feat/aws-server`
4. teste novamente em `feat/aws-server`
5. so depois faca deploy

Se houver duvida, prefira merge seletivo ou cherry-pick de commits, em vez de mergear toda a `dev`.

## Exemplo de rotina segura

```powershell
git checkout dev
git pull

powershell -ExecutionPolicy Bypass -File .\scripts\dev-backend.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\dev-frontend.ps1

node --test tests/connectivity.test.js
python -m pytest -q
```

Depois de validar:

```powershell
git checkout feat/aws-server
git pull
git merge dev
```

Se a `dev` estiver com muito conteudo experimental, use merge seletivo em vez disso.

## Resumo

A branch `dev` e o espaco seguro para:

- testar
- quebrar e refazer
- validar visualmente
- experimentar arquitetura
- evoluir funcionalidades grandes

A branch `feat/aws-server` deve receber apenas o que ja foi revisado, limpo e validado.
