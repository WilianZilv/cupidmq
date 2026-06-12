# stress-16p-20c-tcp-mult8 — kill + build + restart
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Master = Join-Path $Root "master"
$Consumer = Join-Path $Root "python-client"
$Dashboard = Join-Path $Root "dashboard"

Write-Host "[stress] stopping old processes..."
Get-Process cupidmq -ErrorAction SilentlyContinue | Stop-Process -Force
Get-Process cupidmq-producer -ErrorAction SilentlyContinue | Stop-Process -Force
Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "harness\.consumer_cli|cupidmq\.consumer_cli" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Get-CimInstance Win32_Process -Filter "Name='node.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "cupidmq-dashboard|dashboard" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-Sleep -Seconds 2

Write-Host "[stress] cargo clean + build --release..."
Push-Location $Master
cargo clean
cargo build --release
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
cargo build --release --example cupidmq-producer
if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
Pop-Location

$Bin = Join-Path $Master "target\release\cupidmq.exe"
$Producer = Join-Path $Master "target\release\examples\cupidmq-producer.exe"

Write-Host "[stress] starting master..."
Start-Process -WindowStyle Hidden `
    -FilePath $Bin `
    -ArgumentList @("--config", "cupidmq.conf") `
    -WorkingDirectory $Master
Start-Sleep -Seconds 2

Write-Host "[stress] 20 consumers (consumer-g0..19, data :9760+)..."
0..19 | ForEach-Object {
    $i = $_
    $tag = "consumer-g$i"
    $port = 9760 + $i
    Start-Process -WindowStyle Hidden `
        -FilePath "uv" `
        -ArgumentList @(
            "run", "python", "-m", "harness.consumer_cli",
            "--consumer-tag", $tag,
            "--data-addr", "127.0.0.1:$port",
            "--max-batch-size-count", "128",
            "--no-simulate-process-ms",
            "--prefetch-batch-count", "4",
            "--batch-timeout-secs", "12",
            "--data-idle-secs", "60",
            "--reconnect-delay-secs", "2",
            "--log-every", "50"
        ) `
        -WorkingDirectory $Consumer
}
Start-Sleep -Seconds 2

Write-Host "[stress] 16 producers (512/s x mult 8, 8KB payload)..."
1..16 | ForEach-Object {
    $n = $_
    Start-Process -WindowStyle Hidden `
        -FilePath $Producer `
        -ArgumentList @(
            "--label", "producer-$n",
            "--source-id", (100 + $n),
            "--rate-min", "512",
            "--rate-max", "512",
            "--publish-mult", "8",
            "--payload-bytes", "8192",
            "--reuse-payload",
            "--batch-mode",
            "--batch-flush-ms", "100",
            "--batch-mb", "64",
            "--outbound-queue-mb", "4096",
            "--delivery-timeout-secs", "8",
            "--reconnect-delay-secs", "2",
            "--duration-secs", "3600"
        ) `
        -WorkingDirectory $Master
}

Write-Host "[stress] dashboard..."
if (Test-Path $Dashboard) {
    Start-Process -WindowStyle Hidden -FilePath "npm" -ArgumentList @("run", "dev") -WorkingDirectory $Dashboard
    Write-Host "[stress] dashboard http://127.0.0.1:5175"
} else {
    Write-Host "[stress] dashboard dir missing - skip"
}

Write-Host "[stress] metrics http://127.0.0.1:9752/metrics"
Write-Host "[stress] done"
