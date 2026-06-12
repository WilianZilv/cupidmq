"""CupidMQ client configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TypedDict


class ClientType(str, Enum):
    CONSUMER = "consumer"
    PRODUCER = "producer"


def parse_master(master: str, default_port: int) -> tuple[str, int]:
    host, _, port_s = master.rpartition(":")
    if not host:
        host, port_s = "127.0.0.1", master
    return host, int(port_s or default_port)


@dataclass
class ConsumerConfig:
    master: str = "127.0.0.1:9750"
    data_addr: str = "127.0.0.1:9760"
    """Address advertised in REG! (producer connects here). Local bind: `0.0.0.0:port` (port from `data_addr`)."""
    bind_addr: str | None = None
    consumer_tag: str = ""
    max_batch_size_count: int = 32
    prefetch_batch_count: int = 4
    batch_timeout_secs: float = 10.0
    data_idle_secs: float = 60.0
    reconnect: bool = True
    reconnect_delay_secs: float = 2.0

    @property
    def master_host(self) -> str:
        return parse_master(self.master, 9750)[0]

    @property
    def master_port(self) -> int:
        return parse_master(self.master, 9750)[1]

    @property
    def data_host(self) -> str:
        host, _, _ = self.data_addr.rpartition(":")
        return host or "127.0.0.1"

    @property
    def data_port(self) -> int:
        _, _, port_s = self.data_addr.rpartition(":")
        return int(port_s or "9760")

    @property
    def bind_host(self) -> str:
        if self.bind_addr:
            host, _, _ = self.bind_addr.rpartition(":")
            return host or "0.0.0.0"
        return "0.0.0.0"

    @property
    def bind_port(self) -> int:
        if self.bind_addr:
            _, _, port_s = self.bind_addr.rpartition(":")
            return int(port_s or "9760")
        _, _, port_s = self.data_addr.rpartition(":")
        return int(port_s or "9760")

    @property
    def effective_bind_addr(self) -> str:
        return f"{self.bind_host}:{self.bind_port}"


@dataclass
class ProducerConfig:
    master: str = "127.0.0.1:9750"
    max_batch_bytes: int = 64 * 1024 * 1024
    flush_timeout_ms: int = 100
    outbound_max_bytes: int = 4 * 1024 * 1024 * 1024
    reconnect_delay_secs: float = 2.0
    delivery_timeout_secs: float = 8.0
    delivery_idle_secs: float = 60.0
    heartbeat_interval_ms: int = 2000

    @property
    def master_host(self) -> str:
        return parse_master(self.master, 9750)[0]

    @property
    def master_port(self) -> int:
        return parse_master(self.master, 9750)[1]


@dataclass
class ClientConfig:
    type: ClientType = ClientType.CONSUMER
    consumer: ConsumerConfig = field(default_factory=ConsumerConfig)
    producer: ProducerConfig = field(default_factory=ProducerConfig)


class ConsumerOptions(TypedDict, total=False):
    """Campos opcionais de `CupidMQ.consumer` (exceto `master` / `data_addr`)."""

    consumer_tag: str
    bind_addr: str
    max_batch_size_count: int
    prefetch_batch_count: int
    batch_timeout_secs: float
    data_idle_secs: float
    reconnect: bool
    reconnect_delay_secs: float


class ProducerOptions(TypedDict, total=False):
    """Campos opcionais de `CupidMQ.producer` (exceto `master`)."""

    max_batch_bytes: int
    flush_timeout_ms: int
    outbound_max_bytes: int
    reconnect_delay_secs: float
    delivery_timeout_secs: float
    delivery_idle_secs: float
    heartbeat_interval_ms: int


def client_config_from_type(
    master: str,
    client_type: ClientType,
    *,
    data_addr: str = "127.0.0.1:9760",
    consumer: ConsumerOptions | None = None,
    producer: ProducerOptions | None = None,
) -> ClientConfig:
    """Legado — prefira `CupidMQ.consumer` / `CupidMQ.producer`."""
    kind = ClientType(client_type)
    if kind is ClientType.CONSUMER:
        opts = consumer or {}
        return ClientConfig(
            type=kind,
            consumer=ConsumerConfig(master=master, data_addr=data_addr, **opts),
        )
    opts = producer or {}
    return ClientConfig(type=kind, producer=ProducerConfig(master=master, **opts))
