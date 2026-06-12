"""Runtime consumer — REG!/CRDY + data plane BATC."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncIterator

from cupidmq.config import ConsumerConfig
from cupidmq.protocol import split_batch_payload
from cupidmq.stats import ClientStats
from cupidmq.transport import read_batch_payload, read_exact, tune_stream
from cupidmq.wire import MAGIC_BATCH, encode_consumer_ready, encode_register

_CONNECT_RETRY_SECS = 0.1


class _DataPlane:
    def __init__(self, idle_secs: float) -> None:
        self.batch_ready = asyncio.Event()
        self.pending: list[bytes] | None = None
        self._accept_lock = asyncio.Lock()
        self._accept_batch = False
        self.stale_batches = 0
        self.idle_secs = idle_secs

    async def set_accept_batch(self, value: bool) -> None:
        async with self._accept_lock:
            self._accept_batch = value

    async def accept_batch(self) -> bool:
        async with self._accept_lock:
            return self._accept_batch

    async def note_stale_batch(self) -> None:
        async with self._accept_lock:
            self.stale_batches += 1


class ConsumerRuntime:
    def __init__(self, config: ConsumerConfig, stats: ClientStats) -> None:
        self._config = config
        self._stats = stats
        self._data = _DataPlane(config.data_idle_secs)
        self._out: asyncio.Queue[list[bytes] | None] = asyncio.Queue(
            maxsize=max(1, config.prefetch_batch_count)
        )
        self._ready_for_next = asyncio.Event()
        self._ready_for_next.set()
        self._stop = asyncio.Event()
        self._data_server: asyncio.AbstractServer | None = None
        self._runner: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._runner is not None:
            return
        self._data.batch_ready = asyncio.Event()
        self._data_server = await self._start_data_server()
        self._runner = asyncio.create_task(self._run(), name="cupidmq-client")

    async def close(self) -> None:
        self._stop.set()
        if self._runner is not None:
            self._runner.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._runner
            self._runner = None
        await self._out.put(None)
        if self._data_server is not None:
            self._data_server.close()
            await self._data_server.wait_closed()
            self._data_server = None

    async def consume(self) -> AsyncIterator[list[bytes]]:
        while True:
            batch = await self._out.get()
            try:
                if batch is None:
                    return
                yield batch
                self._ready_for_next.set()
            finally:
                self._out.task_done()

    async def _start_data_server(self) -> asyncio.AbstractServer:
        try:
            return await asyncio.start_server(
                self._handle_data_client,
                self._config.bind_host,
                self._config.bind_port,
            )
        except OSError as exc:
            raise RuntimeError(
                f"BATC bind {self._config.effective_bind_addr} in use "
                f"(advertise {self._config.data_addr}) — another consumer? ({exc})"
            ) from exc

    async def _handle_data_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        await self._serve_batch_on_connection(reader, writer)

    async def _serve_batch_on_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        tune_stream(writer)
        idle = self._data.idle_secs
        try:
            while not self._stop.is_set():
                try:
                    magic = await asyncio.wait_for(read_exact(reader, 4), timeout=idle)
                except asyncio.TimeoutError:
                    break
                if magic != MAGIC_BATCH:
                    break
                try:
                    payload = await read_batch_payload(reader)
                except (asyncio.IncompleteReadError, ValueError):
                    break
                if not await self._data.accept_batch():
                    await self._data.note_stale_batch()
                    continue
                try:
                    items = split_batch_payload(payload)
                except ValueError:
                    break
                self._data.pending = items
                self._data.batch_ready.set()
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _run(self) -> None:
        retry = min(_CONNECT_RETRY_SECS, max(0.1, self._config.reconnect_delay_secs))
        while not self._stop.is_set():
            try:
                reader, writer = await self._connect_master(retry)
            except asyncio.CancelledError:
                raise
            try:
                await self._session(reader, writer)
            except asyncio.IncompleteReadError:
                self._stats.drops += 1
            except (ConnectionResetError, BrokenPipeError, OSError):
                self._stats.drops += 1
            finally:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
            if not self._config.reconnect or self._stop.is_set():
                break
            self._ready_for_next.set()
            await asyncio.sleep(retry)
            self._stats.reconnects += 1
        if not self._stop.is_set():
            await self._out.put(None)

    async def _connect_master(
        self,
        retry: float,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        while not self._stop.is_set():
            try:
                reader, writer = await asyncio.open_connection(
                    self._config.master_host,
                    self._config.master_port,
                )
                tune_stream(writer)
                writer.write(encode_register(self._config.data_addr))
                await writer.drain()
                return reader, writer
            except (ConnectionRefusedError, ConnectionResetError, OSError):
                self._stats.drops += 1
                await asyncio.sleep(retry)

    async def _session(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        link_closed = asyncio.Event()

        async def read_watchdog() -> None:
            try:
                while not self._stop.is_set():
                    chunk = await reader.read(1)
                    if not chunk:
                        break
            except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError, OSError):
                pass
            finally:
                link_closed.set()

        watchdog = asyncio.create_task(read_watchdog())
        try:
            await self._fetch_loop(writer, link_closed)
        finally:
            watchdog.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watchdog

    async def _fetch_loop(
        self,
        writer: asyncio.StreamWriter,
        link_closed: asyncio.Event,
    ) -> None:
        prefetch_slot = self._ready_for_next
        while not self._stop.is_set():
            prefetch_wait = asyncio.create_task(prefetch_slot.wait())
            link_wait = asyncio.create_task(link_closed.wait())
            done, pending = await asyncio.wait(
                [prefetch_wait, link_wait],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                for task in pending:
                    await task
            if link_closed.is_set() or self._stop.is_set():
                raise ConnectionResetError("master control disconnected")

            prefetch_slot.clear()
            assert self._data.batch_ready is not None
            self._data.batch_ready.clear()
            self._data.pending = None
            writer.write(
                encode_consumer_ready(
                    self._config.max_batch_size_count, self._config.consumer_tag
                )
            )
            await writer.drain()
            await self._data.set_accept_batch(True)
            try:
                batch_wait = asyncio.create_task(self._data.batch_ready.wait())
                link_wait = asyncio.create_task(link_closed.wait())
                done, pending = await asyncio.wait(
                    [batch_wait, link_wait],
                    timeout=self._config.batch_timeout_secs,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    for task in pending:
                        await task
                if link_closed.is_set():
                    raise ConnectionResetError("master control disconnected")
                if batch_wait not in done:
                    self._stats.empty_waits += 1
                    prefetch_slot.set()
                    continue
            finally:
                await self._data.set_accept_batch(False)
            self._stats.stale_batches = self._data.stale_batches
            batch = self._data.pending or []
            if not batch:
                self._stats.empty_waits += 1
                prefetch_slot.set()
                continue
            self._stats.batches_in += 1
            self._stats.msgs_in += len(batch)
            await self._out.put(batch)
