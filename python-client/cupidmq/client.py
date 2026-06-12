"""CupidMQ client — master TCP control plane + BATC data plane."""

from __future__ import annotations

from collections.abc import AsyncIterator
from types import TracebackType

from cupidmq._consumer import ConsumerRuntime
from cupidmq._producer import ProducerRuntime
from cupidmq.config import (
    ClientConfig,
    ClientType,
    ConsumerConfig,
    ProducerConfig,
)
from cupidmq.stats import ClientStats


class CupidMQ:
    """Master CupidMQ client.

    Typical usage (mirrors Rust ``connect_producer`` / Python consumer):

    ```python
    # consumer — construct + start inside the context manager
    async with CupidMQ.consumer("127.0.0.1:9750", data_addr="127.0.0.1:9760") as c:
        async for batch in c.consume():
            ...

    # producer
    async with CupidMQ.producer("127.0.0.1:9750", flush_timeout_ms=100) as c:
        c.enqueue(payload_bytes)

    # or explicit connect_* (already started)
    c = await CupidMQ.connect_producer("127.0.0.1:9750")
    ```
    """

    _config: ClientConfig
    _stats: ClientStats
    _consumer: ConsumerRuntime | None
    _producer: ProducerRuntime | None
    _started: bool

    @classmethod
    def consumer(
        cls,
        master: str,
        *,
        data_addr: str = "127.0.0.1:9760",
        bind_addr: str | None = None,
        consumer_tag: str = "",
        max_batch_size_count: int = 32,
        prefetch_batch_count: int = 4,
        batch_timeout_secs: float = 10.0,
        data_idle_secs: float = 60.0,
        reconnect: bool = True,
        reconnect_delay_secs: float = 2.0,
    ) -> CupidMQ:
        """Build a consumer instance (does not start — use ``async with`` or ``await start()``).

        Registers with the master on the control plane (typically ``:9750``).
        Announces ``data_addr`` in REG!; producers dial that address for BATC.
        Listens locally on ``bind_addr`` or ``0.0.0.0:<port of data_addr>``.

        Args:
            master: Master address — ``host:port`` of the control plane (connect).
            data_addr: Routable BATC address sent in REG! (e.g. ``127.0.0.1:9760``).
            bind_addr: Optional local listen socket (e.g. ``0.0.0.0:9760``).
            consumer_tag: Optional identifier sent in REG!/CRDY (metrics/debug).
            max_batch_size_count: Maximum items per batch requested from the master (CRDY).
            prefetch_batch_count: Batches buffered locally before the next CRDY
                (prefetch measured in batch units).
            batch_timeout_secs: Timeout while waiting for a batch on the control plane.
            data_idle_secs: Maximum BATC connection idle time before closing.
            reconnect: Reconnect control/data planes after a disconnect.
            reconnect_delay_secs: Delay between reconnection attempts.
        """
        cfg = ConsumerConfig(
            master=master,
            data_addr=data_addr,
            bind_addr=bind_addr,
            consumer_tag=consumer_tag,
            max_batch_size_count=max_batch_size_count,
            prefetch_batch_count=prefetch_batch_count,
            batch_timeout_secs=batch_timeout_secs,
            data_idle_secs=data_idle_secs,
            reconnect=reconnect,
            reconnect_delay_secs=reconnect_delay_secs,
        )
        return cls._from_consumer(cfg)

    @classmethod
    def producer(
        cls,
        master: str,
        *,
        max_batch_bytes: int = 64 * 1024 * 1024,
        flush_timeout_ms: int = 100,
        outbound_max_bytes: int = 4 * 1024 * 1024 * 1024,
        reconnect_delay_secs: float = 2.0,
        delivery_timeout_secs: float = 8.0,
        delivery_idle_secs: float = 60.0,
        heartbeat_interval_ms: int = 2000,
    ) -> CupidMQ:
        """Build a producer instance (does not start — use ``async with`` or ``await start()``).

        Connects to the master control plane, accumulates ``enqueue`` payloads into
        batches, and delivers them P2P over BATC when the master assigns a consumer.

        Args:
            master: Master address — ``host:port`` of the control plane.
            max_batch_bytes: Maximum encoded batch size sent per BATC delivery.
            flush_timeout_ms: Max wait before flushing a partial batch from the
                local accumulator (even if ``max_batch_bytes`` is not reached).
            outbound_max_bytes: Total in-memory cap for pending + queued payloads;
                oldest items are dropped when exceeded.
            reconnect_delay_secs: Minimum delay between control-plane reconnect attempts.
            delivery_timeout_secs: Timeout for a single BATC delivery attempt.
            delivery_idle_secs: Maximum idle time for a pooled BATC connection to a
                consumer before closing it.
            heartbeat_interval_ms: Interval for HBRP heartbeats on the control plane
                (backlog snapshot for master recovery).
        """
        cfg = ProducerConfig(
            master=master,
            max_batch_bytes=max_batch_bytes,
            flush_timeout_ms=flush_timeout_ms,
            outbound_max_bytes=outbound_max_bytes,
            reconnect_delay_secs=reconnect_delay_secs,
            delivery_timeout_secs=delivery_timeout_secs,
            delivery_idle_secs=delivery_idle_secs,
            heartbeat_interval_ms=heartbeat_interval_ms,
        )
        return cls._from_producer(cfg)

    @classmethod
    async def connect_consumer(
        cls,
        master: str,
        *,
        data_addr: str = "127.0.0.1:9760",
        bind_addr: str | None = None,
        consumer_tag: str = "",
        max_batch_size_count: int = 32,
        prefetch_batch_count: int = 4,
        batch_timeout_secs: float = 10.0,
        data_idle_secs: float = 60.0,
        reconnect: bool = True,
        reconnect_delay_secs: float = 2.0,
    ) -> CupidMQ:
        """Build a consumer and call ``start()`` immediately."""
        client = cls.consumer(
            master,
            data_addr=data_addr,
            bind_addr=bind_addr,
            consumer_tag=consumer_tag,
            max_batch_size_count=max_batch_size_count,
            prefetch_batch_count=prefetch_batch_count,
            batch_timeout_secs=batch_timeout_secs,
            data_idle_secs=data_idle_secs,
            reconnect=reconnect,
            reconnect_delay_secs=reconnect_delay_secs,
        )
        await client.start()
        return client

    @classmethod
    async def connect_producer(
        cls,
        master: str,
        *,
        max_batch_bytes: int = 64 * 1024 * 1024,
        flush_timeout_ms: int = 100,
        outbound_max_bytes: int = 4 * 1024 * 1024 * 1024,
        reconnect_delay_secs: float = 2.0,
        delivery_timeout_secs: float = 8.0,
        delivery_idle_secs: float = 60.0,
        heartbeat_interval_ms: int = 2000,
    ) -> CupidMQ:
        """Build a producer and call ``start()`` immediately."""
        client = cls.producer(
            master,
            max_batch_bytes=max_batch_bytes,
            flush_timeout_ms=flush_timeout_ms,
            outbound_max_bytes=outbound_max_bytes,
            reconnect_delay_secs=reconnect_delay_secs,
            delivery_timeout_secs=delivery_timeout_secs,
            delivery_idle_secs=delivery_idle_secs,
            heartbeat_interval_ms=heartbeat_interval_ms,
        )
        await client.start()
        return client

    @classmethod
    def _from_consumer(cls, cfg: ConsumerConfig) -> CupidMQ:
        inst = cls.__new__(cls)
        inst._init_from_config(ClientConfig(type=ClientType.CONSUMER, consumer=cfg))
        return inst

    @classmethod
    def _from_producer(cls, cfg: ProducerConfig) -> CupidMQ:
        inst = cls.__new__(cls)
        inst._init_from_config(ClientConfig(type=ClientType.PRODUCER, producer=cfg))
        return inst

    def _init_from_config(self, config: ClientConfig) -> None:
        self._config = config
        self._stats = ClientStats()
        self._consumer = None
        self._producer = None
        self._started = False

        if self._config.type is ClientType.CONSUMER:
            self._consumer = ConsumerRuntime(self._config.consumer, self._stats)
        else:
            self._producer = ProducerRuntime(self._config.producer, self._stats)

    @property
    def type(self) -> ClientType:
        """``CONSUMER`` or ``PRODUCER``."""
        return self._config.type

    @property
    def stats(self) -> ClientStats:
        """Live counters (throughput, drops, reconnects, etc.)."""
        return self._stats

    async def start(self) -> None:
        """Start background tasks (control plane, data plane, accumulator)."""
        if self._started:
            return
        if self._consumer is not None:
            await self._consumer.start()
        elif self._producer is not None:
            await self._producer.start()
        self._started = True

    async def close(self) -> None:
        """Stop background tasks and close open connections."""
        if self._consumer is not None:
            await self._consumer.close()
        if self._producer is not None:
            await self._producer.close()
        self._started = False

    async def __aenter__(self) -> CupidMQ:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.close()

    def enqueue(self, payload: bytes) -> None:
        """Queue bytes — non-blocking; local ring + background tasks deliver."""
        if self._producer is None:
            raise RuntimeError("use CupidMQ.producer(...) or connect_producer(...)")
        self._producer.enqueue(payload)

    async def consume(self) -> AsyncIterator[list[bytes]]:
        """Iterate over received batches (each batch is a ``list[bytes]``)."""
        if self._consumer is None:
            raise RuntimeError("use CupidMQ.consumer(...) or connect_consumer(...)")
        async for batch in self._consumer.consume():
            yield batch
