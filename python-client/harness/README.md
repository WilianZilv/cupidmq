# Harness (dev only)

**Not the library.** Load tests and optional processing simulation.

| Component | Path | Role |
|-----------|------|------|
| **CupidMQ client** | `cupidmq/` | `CupidMQ.consumer` / `CupidMQ.producer` |
| **Harness** | `harness/` | stress CLI, optional payload dump |
| **Rust harness** | `master/examples/cupidmq-producer.rs` | synthetic byte payloads |

```bash
cd python-client && uv sync
uv run python -m harness.consumer_cli --master 127.0.0.1:9750 --data-addr 127.0.0.1:9760
```

Harness payload wire format (producer example only):

```
CUPIDMQ\0 | seq u64 LE | source_id u32 LE | body...
```
