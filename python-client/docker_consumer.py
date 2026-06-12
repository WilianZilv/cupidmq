"""Docker consumer — simulated per-item process; env from entrypoint-consumer.sh."""

from __future__ import annotations

import asyncio
import os
import random

from cupidmq import CupidMQ


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


async def main() -> None:
    master = _env("CUPIDMQ_MASTER", "127.0.0.1:9750")
    data_addr = _env("CUPIDMQ_DATA_ADDR")
    if not data_addr:
        raise SystemExit("CUPIDMQ_DATA_ADDR required")

    bind_addr = _env("CUPIDMQ_BIND_ADDR") or None
    tag = _env("CUPIDMQ_CONSUMER_TAG")
    process_min_ms = float(_env("CUPIDMQ_PROCESS_MS_MIN", "2"))
    process_max_ms = max(process_min_ms, float(_env("CUPIDMQ_PROCESS_MS_MAX", "16")))

    client = CupidMQ.consumer(
        master,
        data_addr=data_addr,
        bind_addr=bind_addr,
        consumer_tag=tag,
    )
    cc = client._config.consumer  # noqa: SLF001 — startup log

    print(
        f"docker-consumer-py: master={master} advertise={cc.data_addr} "
        f"bind={cc.effective_bind_addr} tag={tag} "
        f"process={process_min_ms:.0f}-{process_max_ms:.0f}ms/item",
        flush=True,
    )

    async with client:
        async for batch in client.consume():
            for item in batch:
                delay_s = random.uniform(process_min_ms, process_max_ms) / 1000.0
                if delay_s > 0:
                    await asyncio.sleep(delay_s)
                _ = len(item)


if __name__ == "__main__":
    asyncio.run(main())
