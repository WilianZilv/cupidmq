# Bundle release binaries into dist/ (platform-native names)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Master = Join-Path $Root "master"
$Dashboard = Join-Path $Root "dashboard"
$Dist = Join-Path $Root "dist"
$Target = Join-Path $Master "target\release"

$env:VITE_CUPIDMQ_METRICS = "/metrics"
Push-Location $Dashboard
npm ci
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
npm run build
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
Pop-Location

Push-Location $Master
cargo build --release --bin cupidmq-headless
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
cargo build --release --bin cupidmq --features embed-dashboard
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
cargo build --release --example cupidmq-producer
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
Pop-Location

New-Item -ItemType Directory -Force -Path $Dist | Out-Null

$Ext = ""
$HeadlessBin = Join-Path $Target "cupidmq-headless.exe"
$DashboardBin = Join-Path $Target "cupidmq.exe"
$ProducerBin = Join-Path $Target "examples\cupidmq-producer.exe"
if (-not (Test-Path $HeadlessBin)) {
    $Ext = ""
    $HeadlessBin = Join-Path $Target "cupidmq-headless"
    $DashboardBin = Join-Path $Target "cupidmq"
    $ProducerBin = Join-Path $Target "examples/cupidmq-producer"
} else {
    $Ext = ".exe"
}

Copy-Item $HeadlessBin (Join-Path $Dist "cupidmq-headless$Ext") -Force
Copy-Item $DashboardBin (Join-Path $Dist "cupidmq$Ext") -Force
Copy-Item $ProducerBin (Join-Path $Dist "cupidmq-producer$Ext") -Force
Copy-Item (Join-Path $Master "cupidmq.conf.example") (Join-Path $Dist "cupidmq.conf.example") -Force

Write-Host "[publish] dist/"
Get-ChildItem $Dist | Format-Table Name, Length
