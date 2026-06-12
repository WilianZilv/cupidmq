# Docker integration test

Multi-replica Compose stack for end-to-end BATC/TCP tests — **not** production deployment.

| File | Use |
|------|-----|
| `docker-compose.yml` | Linux/WSL — `network_mode: host`, scale 8p + 8 rust + 8 py |
| `docker-compose.bridge.yml` | Docker Desktop fallback — 4 fixed consumers on bridge |
| `.env.example` | Copy to `.env` and set `CUPIDMQ_HOST_IP` |

```bash
cp test-environments/integration/.env.example test-environments/integration/.env
make docker-up    # from repo root
```

**Stop everything** (must use the same project + compose file as `make docker-up`):

```bash
# repo root — preferred
make docker-down

# or explicit
docker compose -f test-environments/integration/docker-compose.yml \
  --env-file test-environments/integration/.env down -v

# from test-environments/integration/ (project name is cupidmq in compose file)
docker compose --env-file .env down -v
```

`docker compose down` alone in this folder **used to fail** (wrong project name `integration` vs `cupidmq`). The compose file now sets `name: cupidmq`.

Emergency if compose still mismatches:

```bash
docker stop $(docker ps -q --filter name=cupidmq-)
docker rm $(docker ps -aq --filter name=cupidmq-)
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
