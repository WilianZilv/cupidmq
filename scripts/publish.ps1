# Bundle release binaries into dist/ (platform-native names)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Master = Join-Path $Root "master"
$Dist = Join-Path $Root "dist"
$Target = Join-Path $Master "target\release"

Push-Location $Master
cargo build --release
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
cargo build --release --example cupidmq-producer
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
Pop-Location

New-Item -ItemType Directory -Force -Path $Dist | Out-Null

$MasterBin = Join-Path $Target "cupidmq.exe"
$ProducerBin = Join-Path $Target "examples\cupidmq-producer.exe"
if (-not (Test-Path $MasterBin)) {
    $MasterBin = Join-Path $Target "cupidmq"
    $ProducerBin = Join-Path $Target "examples/cupidmq-producer"
}

Copy-Item $MasterBin (Join-Path $Dist "cupidmq$([IO.Path]::GetExtension($MasterBin))") -Force
Copy-Item $ProducerBin (Join-Path $Dist "cupidmq-producer$([IO.Path]::GetExtension($ProducerBin))") -Force
Copy-Item (Join-Path $Master "cupidmq.conf.example") (Join-Path $Dist "cupidmq.conf.example") -Force

Write-Host "[publish] dist/"
Get-ChildItem $Dist | Format-Table Name, Length
