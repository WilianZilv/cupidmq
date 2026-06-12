"""Runtime producer — PRDY/ASGN + BATC TCP."""

from __future__ import annotations

import asyncio
import contextlib
import struct
import time
from enum import Enum

from cupidmq.accumulator import BatchAccumulator
from cupidmq.config import ProducerConfig
from cupidmq.ring import ByteRing, QueuedMessage
from cupidmq.stats import ClientStats
from cupidmq.transport import read_exact, tune_stream
from cupidmq.wire import (
    AssignRequest,
    DeliveryReport,
    FailureReport,
    ProducerHeartbeat,
    encode_batch_frame,
    encode_batch_payload,
    encode_delivered,
    encode_failed,
    encode_heartbeat,
    encode_producer_ready,
    MAGIC_ASSIGN,
)

class _LinkState(Enum):
    DISCONNECTED = "disconnected"
    IDLE = "idle"
    READY = "ready"
    BUSY = "busy"


def _now_ms() -> int:
    return int(time.time() * 1000)


class ProducerRuntime:
    def __init__(self, config: ProducerConfig, stats: ClientStats) -> None:
        self._config = config
        self._stats = stats
        self._pending = ByteRing(config.outbound_max_bytes)
        self._ring = ByteRing(config.outbound_max_bytes)
        self._accumulator = BatchAccumulator(
            config.max_batch_bytes,
            config.flush_timeout_ms / 1000.0,
        )
        self._link_state = _LinkState.DISCONNECTED
        self._connected = False
        self._last_connect_fail: float | None = None
        self._notify = asyncio.Event()
        self._stop = asyncio.Event()
        self._delivery_pool: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        if self._tasks:
            return
        self._tasks = [
            asyncio.create_task(self._run_accumulator(), name="cupidmq-producer-acc"),
            asyncio.create_task(self._run_control(), name="cupidmq-producer-ctl"),
        ]

    async def close(self) -> None:
        self._stop.set()
        self._notify.set()
        for task in self._tasks:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        for _, writer in self._delivery_pool.values():
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
        self._delivery_pool.clear()

    def enqueue(self, payload: bytes) -> None:
        self._pending.push(payload, _now_ms())
        self._sync_drop_stats()
        self._notify.set()

    def _sync_drop_stats(self) -> None:
        self._stats.queue_dropped_items = (
            self._pending.drops_total + self._ring.drops_total
        )

    def _push_batch_to_ring(self, batch: list[bytes]) -> None:
        now = _now_ms()
        for item in batch:
            self._ring.push(item, now)

    async def _run_accumulator(self) -> None:
        flush_secs = self._config.flush_timeout_ms / 1000.0
        while not self._stop.is_set():
            try:
                await asyncio.wait_for(self._notify.wait(), timeout=flush_secs)
            except asyncio.TimeoutError:
                pass
            self._notify.clear()
            ring_gained = False
            while True:
                msg = self._pending.pop_front()
                if msg is None:
                    break
                for batch in self._accumulator.push(msg.payload):
                    self._push_batch_to_ring(batch)
                    ring_gained = True
            batch = self._accumulator.flush_if_timeout()
            if batch:
                self._push_batch_to_ring(batch)
                ring_gained = True
            self._sync_drop_stats()
            if ring_gained:
                self._notify.set()

    async def _run_control(self) -> None:
        while not self._stop.is_set():
            stream = await self._connect_if_needed()
            if stream is None:
                await asyncio.sleep(0.05)
                continue
            reader, writer = stream
            await self._maybe_send_ready(writer)
            await self._maybe_send_heartbeat(writer)
            heartbeat_secs = self._config.heartbeat_interval_ms / 1000.0
            next_hb = time.monotonic() + heartbeat_secs
            try:
                while not self._stop.is_set():
                    timeout = max(0.01, next_hb - time.monotonic())
                    read_task = asyncio.create_task(read_exact(reader, 4))
                    notify_task = asyncio.create_task(self._notify.wait())
                    done, pending = await asyncio.wait(
                        {read_task, notify_task},
                        timeout=timeout,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for p in pending:
                        p.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await p
                    if read_task in done:
                        magic = read_task.result()
                        if magic != MAGIC_ASSIGN:
                            raise RuntimeError(f"expected ASGN, got {magic!r}")
                        hdr = await read_exact(reader, 4)
                        max_items, tag_len = struct.unpack("<HH", hdr)
                        tag = await read_exact(reader, tag_len)
                        data_len = struct.unpack("<H", await read_exact(reader, 2))[0]
                        data_addr = (await read_exact(reader, data_len)).decode(
                            "utf-8", errors="replace"
                        )
                        assign = AssignRequest(
                            max_items=max_items,
                            consumer_tag=tag.decode("utf-8", errors="replace"),
                            data_addr=data_addr,
                        )
                        self._link_state = _LinkState.BUSY
                        report = await self._deliver_assign(assign)
                        await self._handle_delivery_result(writer, report)
                        self._link_state = _LinkState.IDLE
                        await self._maybe_send_ready(writer)
                        await self._maybe_send_heartbeat(writer)
                    elif notify_task in done:
                        notify_task.result()
                        self._notify.clear()
                        await self._maybe_send_ready(writer)
                    if time.monotonic() >= next_hb:
                        await self._maybe_send_heartbeat(writer)
                        next_hb = time.monotonic() + heartbeat_secs
            except (asyncio.IncompleteReadError, ConnectionResetError, OSError, RuntimeError):
                self._mark_disconnected()
            finally:
                self._mark_disconnected()
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()

    async def _connect_if_needed(
        self,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | None:
        if not self._may_try_connect():
            return None
        try:
            reader, writer = await asyncio.open_connection(
                self._config.master_host,
                self._config.master_port,
            )
            tune_stream(writer)
            self._connected = True
            self._link_state = _LinkState.IDLE
            self._last_connect_fail = None
            return reader, writer
        except (ConnectionRefusedError, ConnectionResetError, OSError):
            self._stats.reconnects += 1
            self._last_connect_fail = time.monotonic()
            return None

    def _may_try_connect(self) -> bool:
        if self._connected:
            return True
        if self._last_connect_fail is None:
            return True
        return (
            time.monotonic() - self._last_connect_fail
            >= self._config.reconnect_delay_secs
        )

    def _mark_disconnected(self) -> None:
        self._connected = False
        self._link_state = _LinkState.DISCONNECTED
        self._last_connect_fail = time.monotonic()

    def _collect_heartbeat(self) -> ProducerHeartbeat:
        return ProducerHeartbeat(
            pending_messages=len(self._pending),
            pending_bytes=self._pending.byte_len(),
            ring_messages=len(self._ring),
            ring_bytes=self._ring.byte_len(),
            drops_total=self._pending.drops_total + self._ring.drops_total,
        )

    async def _maybe_send_heartbeat(self, writer: asyncio.StreamWriter) -> None:
        if not self._connected:
            return
        writer.write(encode_heartbeat(self._collect_heartbeat()))
        try:
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError):
            self._mark_disconnected()

    async def _maybe_send_ready(self, writer: asyncio.StreamWriter) -> None:
        if (
            self._ring.is_empty()
            or self._link_state is not _LinkState.IDLE
            or not self._connected
        ):
            return
        self._link_state = _LinkState.READY
        writer.write(encode_producer_ready())
        try:
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError):
            self._stats.reconnects += 1
            self._mark_disconnected()

    async def _deliver_assign(
        self,
        assign: AssignRequest,
    ) -> DeliveryReport | FailureReport:
        max_items = max(1, assign.max_items)
        items = self._drain_for_assign(max_items, self._config.max_batch_bytes)
        msg_count = len(items)
        payloads = [m.payload for m in items]
        batch_bytes = len(encode_batch_payload(payloads))
        try:
            await asyncio.wait_for(
                self._tcp_deliver_batch(assign.data_addr, payloads),
                timeout=self._config.delivery_timeout_secs,
            )
            return DeliveryReport(msg_count=msg_count, batch_bytes=batch_bytes)
        except Exception as exc:
            if items:
                self._requeue_items(items)
            return FailureReport(
                msg_count=msg_count,
                batch_bytes=batch_bytes,
                code=0,
                message=str(exc),
            )

    def _drain_for_assign(
        self,
        max_items: int,
        max_batch_bytes: int,
    ) -> list[QueuedMessage]:
        out = self._ring.pop_up_to(max_items, _now_ms())
        if not out:
            return out
        total = 0
        keep = len(out)
        for i, msg in enumerate(out):
            nxt = total + len(msg.payload)
            if nxt > max_batch_bytes and i > 0:
                keep = i
                break
            total = nxt
        if keep < len(out):
            rest = out[keep:]
            out = out[:keep]
            for msg in reversed(rest):
                self._ring.push_front(msg)
        return out

    def _requeue_items(self, items: list[QueuedMessage]) -> None:
        for msg in reversed(items):
            self._ring.push_front(msg)

    async def _tcp_deliver_batch(self, addr: str, items: list[bytes]) -> None:
        last_err: Exception | None = None
        for attempt in range(2):
            try:
                reader, writer = await self._get_delivery_stream(addr)
                writer.write(encode_batch_frame(items))
                await writer.drain()
                self._delivery_pool[addr] = (reader, writer)
                return
            except Exception as exc:
                last_err = exc
                await self._invalidate_delivery(addr)
        raise last_err or RuntimeError("delivery failed")

    async def _get_delivery_stream(
        self,
        addr: str,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if addr in self._delivery_pool:
            return self._delivery_pool[addr]
        host, _, port_s = addr.rpartition(":")
        if not host:
            host, port_s = "127.0.0.1", addr
        reader, writer = await asyncio.open_connection(host, int(port_s))
        tune_stream(writer)
        return reader, writer

    async def _invalidate_delivery(self, addr: str) -> None:
        conn = self._delivery_pool.pop(addr, None)
        if conn is None:
            return
        _, writer = conn
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()

    async def _handle_delivery_result(
        self,
        writer: asyncio.StreamWriter,
        report: DeliveryReport | FailureReport,
    ) -> None:
        if isinstance(report, DeliveryReport):
            if report.msg_count > 0:
                self._stats.msgs_out += report.msg_count
                self._stats.bytes_out += report.batch_bytes
                self._stats.batches_out += 1
            writer.write(encode_delivered(report))
        else:
            self._stats.delivery_errors += 1
            self._sync_drop_stats()
            writer.write(encode_failed(report))
        try:
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError):
            self._mark_disconnected()
