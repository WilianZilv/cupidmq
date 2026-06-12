# 8 producers + 8 rust + 8 python consumers (16 total)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Integ = Join-Path $Root "test-environments\integration"
$EnvFile = Join-Path $Integ ".env"
$ComposeFile = Join-Path $Integ "docker-compose.yml"

if (-not (Test-Path $EnvFile)) {
    Copy-Item (Join-Path $Integ ".env.example") $EnvFile
}

Set-Location $Root
docker compose -f $ComposeFile --env-file $EnvFile up -d --build `
    --scale producer=8 `
    --scale consumer-rust=8 `
    --scale consumer-python=8

Write-Host "Dashboard: http://127.0.0.1:9752/"
