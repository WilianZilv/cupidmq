# Docker integration test

Multi-replica Compose stack for end-to-end BATC/TCP tests — **not** production deployment.

| File | Use |
|------|-----|
| `docker-compose.yml` | Linux/WSL — `network_mode: host`, scale 8p + 8 rust + 8 py |
| `docker-compose.bridge.yml` | Docker Desktop fallback — 4 fixed consumers on bridge |
| `.env.example` | Copy to `.env` and set `CUPIDMQ_HOST_IP` |

```bash
cp test-environments/integration/.env.example test-environments/integration/.env
make docker-up
```

Dashboard (baked into master): `http://127.0.0.1:9752/`

## Typical bottleneck (`ready_cycle_ms` high, `consumers_waiting: 0`)

- `state: processing`, `unacked > 0`, `batches_per_sec: 0`
- `producers_ready: N`, backlog rising, `transfer_per_sec → 0`

Consumer received BATC but did not send the next `CRDY` — matcher has no consumers queued.

## BATC port allocation (replicas)

`docker/entrypoint-consumer.sh` + volume `cupidmq-port-alloc`:

- REG advertises `CUPIDMQ_HOST_IP:PORT`
- bind `0.0.0.0:PORT`
- tag `${PREFIX}-${PORT}` — e.g. `rust-9760`, `py-9764`

## Network

`network_mode: host` — no Docker DNS; clients use `CUPIDMQ_HOST_IP` (not `master:9750`).

Build images reuse Dockerfiles under [`docker/`](../../docker/).
