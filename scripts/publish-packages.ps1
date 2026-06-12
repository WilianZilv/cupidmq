# Build Python wheel into dist/ (local dry-run of release wheel job)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Client = Join-Path $Root "python-client"
$Dist = Join-Path $Root "dist"

New-Item -ItemType Directory -Force -Path $Dist | Out-Null

Push-Location $Client
uv build --wheel
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
Pop-Location

Copy-Item (Join-Path $Client "dist\*.whl") $Dist -Force

Write-Host "[publish-packages] dist/"
Get-ChildItem $Dist -Filter "*.whl" | Format-Table Name, Length
