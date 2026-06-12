# Build wheel + .crate into dist/ (local dry-run of release packages job)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Master = Join-Path $Root "master"
$Client = Join-Path $Root "python-client"
$Dist = Join-Path $Root "dist"

New-Item -ItemType Directory -Force -Path $Dist | Out-Null

Push-Location $Master
cargo package
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
Pop-Location

Push-Location $Client
uv build
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
Pop-Location

Copy-Item (Join-Path $Master "target\package\cupidmq-*.crate") $Dist -Force
Copy-Item (Join-Path $Client "dist\*") $Dist -Force

Write-Host "[publish-packages] dist/"
Get-ChildItem $Dist | Format-Table Name, Length
