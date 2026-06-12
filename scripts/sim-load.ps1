# Simulated load — fixed publish rate per producer (default 100 msg/s), 800KB payload
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Master = Join-Path $Root "master"
$ProducerCount = if ($env:CUPIDMQ_SIM_PRODUCERS) { [int]$env:CUPIDMQ_SIM_PRODUCERS } else { 4 }
$ConsumerCount = if ($env:CUPIDMQ_SIM_CONSUMERS) { [int]$env:CUPIDMQ_SIM_CONSUMERS } else { 4 }
$PublishRatePerProducer = if ($env:CUPIDMQ_SIM_PUBLISH_RATE) { [int]$env:CUPIDMQ_SIM_PUBLISH_RATE } else { 100 }
Write-Host "[sim] build..."
Push-Location $Master
cargo build --release --example cupidmq-producer | Out-Null
Pop-Location

$masterProc = Get-Process cupidmq -ErrorAction SilentlyContinue
if (-not $masterProc) {
    Write-Host "[sim] starting cupidmq..."
    Start-Process -WindowStyle Hidden `
        -FilePath (Join-Path $Master "target\release\cupidmq.exe") `
        -ArgumentList @("--config", "cupidmq.conf") `
        -WorkingDirectory $Master
    Start-Sleep -Seconds 2
}

$Duration = if ($env:CUPIDMQ_SIM_DURATION_SECS) { $env:CUPIDMQ_SIM_DURATION_SECS } else { "600" }
$PayloadKb = 800
$Producer = Join-Path $Master "target\release\examples\cupidmq-producer.exe"
$ConsumerDir = Join-Path $Root "python-client"

Write-Host "[sim] $ConsumerCount consumers (batch=32, process 2-12ms)..."
1..$ConsumerCount | ForEach-Object {
    $tag = "consumer-$_"
    Start-Process -WindowStyle Hidden `
        -FilePath "uv" `
        -ArgumentList @(
            "run", "python", "-m", "harness.consumer_cli",
            "--consumer-tag", $tag,
            "--max-batch-size-count", "32",
            "--process-ms-min", "2",
            "--process-ms-max", "12",
            "--log-every", "25"
        ) `
        -WorkingDirectory $ConsumerDir
}

Start-Sleep -Seconds 1

Write-Host "[sim] $ProducerCount producers (${PublishRatePerProducer}/s each, ${PayloadKb}KB payload)..."
1..$ProducerCount | ForEach-Object {
    $n = $_
    Start-Process -WindowStyle Hidden `
        -FilePath $Producer `
        -ArgumentList @(
            "--label", "producer-$n",
            "--source-id", (100 + $n),
            "--rate-min", "$PublishRatePerProducer",
            "--rate-max", "$PublishRatePerProducer",
            "--payload-bytes", ($PayloadKb * 1024),
            "--batch-mode",
            "--duration-secs", $Duration
        ) `
        -WorkingDirectory $Master
}

Write-Host "[sim] running ${Duration}s — Ctrl+C to stop processes manually"
