# Start CupidMQ stack from test-environments preset JSON
param(
    [Parameter(Mandatory = $true)]
    [string]$Preset,
    [switch]$SkipBuild,
    [switch]$NoDashboard
)
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Master = Join-Path $Root "master"
$Consumer = Join-Path $Root "python-client"
$Dashboard = Join-Path $Root "dashboard"
$PresetPath = Join-Path $Root "test-environments/presets/$Preset.json"
if (-not (Test-Path $PresetPath)) { throw "preset not found: $PresetPath" }
$cfg = Get-Content $PresetPath -Raw | ConvertFrom-Json

Write-Host "[cupidmq] stopping old processes..."
Get-Process cupidmq, cupidmq-producer -ErrorAction SilentlyContinue | Stop-Process -Force
Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "harness\.consumer_cli|cupidmq\.consumer_cli" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Get-CimInstance Win32_Process -Filter "Name='node.exe'" -ErrorAction SilentlyContinue |
    Where-Object { $_.CommandLine -match "cupidmq-dashboard|dashboard" } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue }
Start-Sleep -Seconds 2

if (-not $SkipBuild) {
    Write-Host "[cupidmq] cargo build --release..."
    Push-Location $Master
    cargo build --release
    if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
    cargo build --release --example cupidmq-producer
    if ($LASTEXITCODE -ne 0) { Pop-Location; exit $LASTEXITCODE }
    Pop-Location
}

$Bin = Join-Path $Master "target/release/cupidmq.exe"
$Producer = Join-Path $Master "target/release/examples/cupidmq-producer.exe"
$UvCmd = Get-Command uv -ErrorAction SilentlyContinue
if (-not $UvCmd) { throw "uv not found in PATH - install uv or add to PATH" }
$Uv = $UvCmd.Source

Write-Host "[cupidmq] starting master..."
Start-Process -WindowStyle Hidden -FilePath $Bin -ArgumentList @("--config", "cupidmq.conf") -WorkingDirectory $Master
Start-Sleep -Seconds 2

$p = $cfg.producers
$c = $cfg.consumers
Write-Host "[cupidmq] $($c.count) consumers (consumer-g0..$($c.count - 1))..."
0..($c.count - 1) | ForEach-Object {
    $i = $_
    $tag = "consumer-g$i"
    $port = 9760 + $i
    $args = @(
        "run", "python", "-m", "harness.consumer_cli",
        "--consumer-tag", $tag,
        "--data-addr", "127.0.0.1:$port",
        "--max-batch-size-count", "$($c.max_batch_size_count)",
        "--prefetch-batch-count", "$($c.prefetch_batch_count)",
        "--batch-timeout-secs", "$($c.batch_timeout_secs)",
        "--data-idle-secs", "$($c.data_idle_secs)",
        "--reconnect-delay-secs", "$($c.reconnect_delay_secs)",
        "--log-every", "$($c.log_every)"
    )
    if ($c.simulate_process_ms) {
        $args += @("--simulate-process-ms", "--process-ms-min", "$($c.process_ms_min)", "--process-ms-max", "$($c.process_ms_max)")
    } else {
        $args += @("--no-simulate-process-ms")
    }
    Start-Process -WindowStyle Hidden -FilePath $Uv -ArgumentList $args -WorkingDirectory $Consumer
}
Start-Sleep -Seconds 2

Write-Host "[cupidmq] $($p.count) producers..."
1..$p.count | ForEach-Object {
    $n = $_
    $prodArgs = @(
        "--label", "producer-$n",
        "--source-id", (100 + $n),
        "--rate-min", "$($p.rate_min)",
        "--rate-max", "$($p.rate_max)",
        "--reconnect-delay-secs", "$($p.reconnect_delay_secs)",
        "--duration-secs", "$($p.duration_secs)"
    )
    if ($p.payload_bytes_min) { $prodArgs += @("--payload-bytes-min", "$($p.payload_bytes_min)") }
    if ($p.payload_bytes_max) { $prodArgs += @("--payload-bytes-max", "$($p.payload_bytes_max)") }
    if ($p.payload_bytes -and -not $p.payload_bytes_min) { $prodArgs += @("--payload-bytes", "$($p.payload_bytes)") }
    if ($p.payload_pattern) { $prodArgs += @("--payload-pattern", "$($p.payload_pattern)") }
    if ($p.reuse_payload) { $prodArgs += "--reuse-payload" }
    if ($p.batch_mode) {
        $prodArgs += @("--batch-mode", "--flush-timeout-ms", "$($p.flush_timeout_ms)")
        if ($p.max_batch_size_mb) { $prodArgs += @("--max-batch-size-mb", "$($p.max_batch_size_mb)") }
        if ($p.outbound_max_mb) { $prodArgs += @("--outbound-max-mb", "$($p.outbound_max_mb)") }
    }
    if ($p.publish_mult) { $prodArgs += @("--publish-mult", "$($p.publish_mult)") }
    if ($p.delivery_timeout_secs) { $prodArgs += @("--delivery-timeout-secs", "$($p.delivery_timeout_secs)") }
    Start-Process -WindowStyle Hidden -FilePath $Producer -ArgumentList $prodArgs -WorkingDirectory $Master
}

if (-not $NoDashboard -and (Test-Path $Dashboard)) {
    $NpmCmd = Get-Command npm.cmd -ErrorAction SilentlyContinue
    if (-not $NpmCmd) { $NpmCmd = Get-Command npm -ErrorAction SilentlyContinue }
    if (-not $NpmCmd) {
        Write-Warning "[cupidmq] npm not found - dashboard skipped"
    } else {
        if (-not (Test-Path (Join-Path $Dashboard "node_modules"))) {
            Write-Host "[cupidmq] dashboard npm install..."
            Push-Location $Dashboard
            & $NpmCmd.Source install
            if ($LASTEXITCODE -ne 0) { Pop-Location; throw "npm install failed" }
            Pop-Location
        }
        Write-Host "[cupidmq] dashboard http://127.0.0.1:5175"
        Start-Process -WindowStyle Hidden -FilePath $NpmCmd.Source -ArgumentList @("run", "dev") -WorkingDirectory $Dashboard
    }
}

Write-Host "[cupidmq] metrics http://127.0.0.1:9752/metrics"
Write-Host "[cupidmq] preset $($cfg.id) up"
