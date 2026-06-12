# Docker image assets

Dockerfiles, entrypoints, and master config for CupidMQ images.

| Compose | Path | Use |
|---------|------|-----|
| **Master only** | [`docker-compose.yml`](../docker-compose.yml) (repo root) | `make docker-master-up` — dashboard on `:9752` |
| **Integration E2E** | [`test-environments/integration/`](../test-environments/integration/) | `make docker-up` — scaled producers + consumers |

| File | Role |
|------|------|
| `Dockerfile.client` | Rust producer + `docker-consumer` |
| `Dockerfile.python-client` | Python harness consumer |
| `entrypoint-consumer.sh` | BATC port flock + consumer spawn |
| `run-python-consumer.sh` | Python consumer wrapper |
| `cupidmq.conf` | Master bind (dashboard embedded in image binary) |
