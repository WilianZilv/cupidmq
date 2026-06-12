# CupidMQ Python client

Package `cupidmq` — brokerless P2P batch queue. The library moves **bytes**; it does not define an application message format.

See [monorepo README](../README.md). **Install elsewhere**: [Use in your project](../README.md#use-in-your-project) — git `@main` or Release wheel URL after tag `v*`.

```python
async with CupidMQ.consumer("127.0.0.1:9750", data_addr="127.0.0.1:9760") as c:
    async for batch in c.consume():
        ...
```

```bash
cd python-client && uv sync
uv run python -c "from cupidmq import CupidMQ"
```

Harness: `uv run python -m harness.consumer_cli` → [harness/README.md](harness/README.md).
