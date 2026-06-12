# Test environments

| Path | Purpose |
|------|---------|
| [`integration/`](integration/) | Docker Compose E2E stack (8p + 16 consumers) |
| [`presets/`](presets/) | JSON harness configs (`producers` + `consumers`) |

Preset = JSON with **`producers`** and **`consumers`**: `count` + harness flags.

```
test-environments/
  integration/          # docker-compose.yml (+ bridge fallback)
  schema.test-environment.json
  presets/
    stress-16p-20c-tcp-mult8.json
```

Runner conventions: labels `producer-{n}`, tags `consumer-g{n-1}`, data TCP `:9760+`.

```powershell
powershell -ExecutionPolicy Bypass -File scripts/start-preset.ps1 -Preset stress-16p-20c-tcp-mult8
make docker-up   # integration compose (WSL/Linux)
```
