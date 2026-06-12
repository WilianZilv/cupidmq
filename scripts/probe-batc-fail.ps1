# Sample delivery_failures_total and fail ticks from metrics HTTP (30s window)
$ErrorActionPreference = "Stop"
$before = (Invoke-RestMethod http://127.0.0.1:9752/metrics).delivery_failures_total
Start-Sleep -Seconds 2
$ticks = Invoke-RestMethod http://127.0.0.1:9752/metrics/ticks
$after = (Invoke-RestMethod http://127.0.0.1:9752/metrics).delivery_failures_total
"errors +$($after - $before) in 2s"
$fail = $ticks.ticks | Where-Object { $_.ok -eq $false -or $_.phase -eq 'fail' }
"fail ticks drained: $($fail.Count)"
$m = Invoke-RestMethod http://127.0.0.1:9752/metrics
$m.consumers | Where-Object state -eq 'delivery_failed' | Select-Object -First 3 id,consumer_tag,state | Format-Table
