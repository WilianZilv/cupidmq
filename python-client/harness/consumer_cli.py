"""CLI stress consumer — simulated processing; optional dump-dir (dev only)."""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import time
from pathlib import Path

from cupidmq import CupidMQ, ConsumerConfig, ConsumerOptions

_DEFAULTS = ConsumerConfig()


def batch_process_ms(item_count: int, min_ms: float, max_ms: float) -> float:
    if item_count <= 0:
        return 0.0
    lo = min(min_ms, max_ms)
    hi = max(min_ms, max_ms)
    return sum(random.uniform(lo, hi) for _ in range(item_count))


def maybe_dump_batch(dump_dir: str, batch: list[bytes], counter: list[int]) -> None:
    if not dump_dir:
        return
    from harness.payload import dump_payload

    out = Path(dump_dir).resolve()
    for raw in batch:
        dump_payload(raw, out, counter[0])
        counter[0] += 1


def _default_master() -> str:
    return os.environ.get("CUPIDMQ_MASTER") or os.environ.get(
        "CUPIDMQ_CONSUMER", "127.0.0.1:9750"
    )


def _default_data_addr() -> str:
    host = os.environ.get("CUPIDMQ_DATA_HOST", "127.0.0.1")
    port = os.environ.get("CUPIDMQ_DATA_PORT", "9760")
    return f"{host}:{port}"


def _optional_int(env_key: str, arg: int | None) -> int | None:
    if arg is not None:
        return arg
    raw = os.environ.get(env_key)
    return int(raw) if raw else None


def _optional_float(env_key: str, arg: float | None) -> float | None:
    if arg is not None:
        return arg
    raw = os.environ.get(env_key)
    return float(raw) if raw else None


def _optional_str(env_key: str, arg: str | None) -> str | None:
    if arg is not None:
        return arg
    raw = os.environ.get(env_key)
    return raw if raw else None


def _consumer_overrides(args: argparse.Namespace) -> ConsumerOptions:
    defaults = _DEFAULTS
    out: ConsumerOptions = {}

    tag = _optional_str("CUPIDMQ_CONSUMER_TAG", args.consumer_tag)
    if tag is not None and tag != defaults.consumer_tag:
        out["consumer_tag"] = tag

    max_batch_size_count = _optional_int(
        "CUPIDMQ_MAX_BATCH_SIZE_COUNT", args.max_batch_size_count
    )
    if (
        max_batch_size_count is not None
        and max_batch_size_count != defaults.max_batch_size_count
    ):
        out["max_batch_size_count"] = max_batch_size_count

    prefetch = _optional_int("CUPIDMQ_PREFETCH_BATCH_COUNT", args.prefetch_batch_count)
    if prefetch is not None and prefetch != defaults.prefetch_batch_count:
        out["prefetch_batch_count"] = prefetch

    batch_timeout = _optional_float("CUPIDMQ_BATCH_TIMEOUT_SECS", args.batch_timeout_secs)
    if batch_timeout is not None and batch_timeout != defaults.batch_timeout_secs:
        out["batch_timeout_secs"] = batch_timeout

    data_idle = _optional_float("CUPIDMQ_DATA_IDLE_SECS", args.data_idle_secs)
    if data_idle is not None and data_idle != defaults.data_idle_secs:
        out["data_idle_secs"] = data_idle

    if args.reconnect is not None and args.reconnect != defaults.reconnect:
        out["reconnect"] = args.reconnect

    reconnect_delay = _optional_float("CUPIDMQ_RECONNECT_DELAY", args.reconnect_delay_secs)
    if reconnect_delay is not None and reconnect_delay != defaults.reconnect_delay_secs:
        out["reconnect_delay_secs"] = reconnect_delay

    bind_addr = _optional_str("CUPIDMQ_BIND_ADDR", args.bind_addr)
    if bind_addr is not None:
        out["bind_addr"] = bind_addr

    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CupidMQ stress consumer (harness)")
    p.add_argument(
        "--master",
        default=_default_master(),
        help="TCP control master (default: CUPIDMQ_MASTER / 127.0.0.1:9750)",
    )
    p.add_argument(
        "--data-addr",
        default=_default_data_addr(),
        help="BATC address advertised in REG! (default: CUPIDMQ_DATA_* / 127.0.0.1:9760)",
    )
    p.add_argument(
        "--bind-addr",
        default=os.environ.get("CUPIDMQ_BIND_ADDR"),
        help="Local BATC listen socket (default: 0.0.0.0:<port of data-addr>)",
    )
    p.add_argument("--consumer-tag", default=None, help="CRDY tag (default: lib generates consumer-{id})")
    p.add_argument(
        "--max-batch-size-count",
        type=int,
        default=None,
        help=f"CRDY max_items (default: {_DEFAULTS.max_batch_size_count})",
    )
    p.add_argument(
        "--prefetch-batch-count",
        type=int,
        default=None,
        help=f"batched CRDY prefetch (default: {_DEFAULTS.prefetch_batch_count})",
    )
    p.add_argument("--batch-timeout-secs", type=float, default=None)
    p.add_argument("--data-idle-secs", type=float, default=None)
    p.add_argument("--reconnect", action=argparse.BooleanOptionalAction, default=None)
    p.add_argument("--reconnect-delay-secs", type=float, default=None)
    p.add_argument(
        "--process-ms-min",
        type=float,
        default=float(os.environ.get("CUPIDMQ_PROCESS_MS_MIN", "2")),
    )
    p.add_argument(
        "--process-ms-max",
        type=float,
        default=float(os.environ.get("CUPIDMQ_PROCESS_MS_MAX", "16")),
    )
    p.add_argument(
        "--simulate-process-ms",
        action=argparse.BooleanOptionalAction,
        default=os.environ.get("CUPIDMQ_SIMULATE_PROCESS_MS", "1").lower()
        not in ("0", "false", "no"),
    )
    p.add_argument("--dump-dir", default=os.environ.get("CUPIDMQ_DUMP_DIR", ""))
    p.add_argument("--log-every", type=int, default=20)
    return p.parse_args()


async def run_consumer(args: argparse.Namespace) -> None:
    overrides = _consumer_overrides(args)
    client = CupidMQ.consumer(args.master, data_addr=args.data_addr, **overrides)

    tag = overrides.get("consumer_tag") or args.consumer_tag or "auto"
    cc = client._config.consumer  # noqa: SLF001 — harness startup log
    print(
        f"[{tag}] harness consumer master={args.master} "
        f"advertise={cc.data_addr} bind={cc.effective_bind_addr}",
        flush=True,
    )

    dump_counter = [0]
    async with client:
        async for batch in client.consume():
            t0 = time.monotonic()
            if args.simulate_process_ms:
                proc_ms = batch_process_ms(
                    len(batch), args.process_ms_min, args.process_ms_max
                )
                if proc_ms > 0:
                    await asyncio.sleep(proc_ms / 1000.0)
            maybe_dump_batch(args.dump_dir, batch, dump_counter)
            _ = (time.monotonic() - t0)
            st = client.stats
            if st.batches_in % args.log_every == 0:
                print(
                    f"[{tag}] batches={st.batches_in} msgs={st.msgs_in} "
                    f"reconnects={st.reconnects} "
                    f"drops={st.drops} empty={st.empty_waits} stale={st.stale_batches}",
                    flush=True,
                )


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run_consumer(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
