param(
  [int]$Port = 4173,
  [string]$Bind = "127.0.0.1"
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$frontendDir = Join-Path $repoRoot "frontend"

Write-Host ""
Write-Host "Frontend local de desenvolvimento"
Write-Host "  URL:        http://$Bind`:$Port/index.html"
Write-Host "  API local:  http://127.0.0.1:8008"
Write-Host ""
Write-Host "Quando o frontend roda em localhost/127.0.0.1, runtime-config.js usa o backend local automaticamente."
Write-Host "Se quiser apontar para outro backend, defina no navegador:"
Write-Host "  localStorage.setItem('cloudv2.apiBaseUrl', 'https://SEU_BACKEND'); location.reload();"
Write-Host ""

Set-Location $frontendDir
python -m http.server $Port --bind $Bind
