"""Framing cupidmq master — REG!/CRDY/PRDY/ASGN/BATC/DELV/FAIL/HBRP."""

from __future__ import annotations

import struct
from dataclasses import dataclass

MAGIC_REGISTER = b"REG!"
MAGIC_CONSUMER_READY = b"CRDY"
MAGIC_BATCH = b"BATC"
MAGIC_PRODUCER_READY = b"PRDY"
MAGIC_ASSIGN = b"ASGN"
MAGIC_DELIVERED = b"DELV"
MAGIC_FAILED = b"FAIL"
MAGIC_HEARTBEAT = b"HBRP"

MAX_BATCH_BYTES = 64 * 1024 * 1024


@dataclass
class AssignRequest:
    max_items: int
    consumer_tag: str
    data_addr: str


@dataclass
class DeliveryReport:
    msg_count: int
    batch_bytes: int


@dataclass
class FailureReport:
    msg_count: int
    batch_bytes: int
    code: int
    message: str


@dataclass
class ProducerHeartbeat:
    pending_messages: int = 0
    pending_bytes: int = 0
    ring_messages: int = 0
    ring_bytes: int = 0
    drops_total: int = 0


def encode_register(data_addr: str) -> bytes:
    addr = data_addr.encode("utf-8")
    if not addr or len(addr) > 256:
        raise ValueError("data_addr invalid")
    return MAGIC_REGISTER + struct.pack("<H", len(addr)) + addr


def encode_consumer_ready(max_items: int, consumer_tag: str) -> bytes:
    tag = consumer_tag.encode("utf-8")[:256]
    return MAGIC_CONSUMER_READY + struct.pack("<HH", max_items, len(tag)) + tag


def encode_producer_ready() -> bytes:
    return MAGIC_PRODUCER_READY


def encode_heartbeat(report: ProducerHeartbeat) -> bytes:
    return (
        MAGIC_HEARTBEAT
        + struct.pack(
            "<IIIIQ",
            report.pending_messages,
            report.pending_bytes,
            report.ring_messages,
            report.ring_bytes,
            report.drops_total,
        )
    )


def encode_delivered(report: DeliveryReport) -> bytes:
    return MAGIC_DELIVERED + struct.pack("<HI", report.msg_count, report.batch_bytes)


def encode_failed(report: FailureReport) -> bytes:
    msg = report.message.encode("utf-8")[:65535]
    return (
        MAGIC_FAILED
        + struct.pack("<HIH", report.msg_count, report.batch_bytes, report.code)
        + struct.pack("<H", len(msg))
        + msg
    )


def inner_batch_payload_len(items: list[bytes]) -> int:
    return 2 + sum(4 + len(item) for item in items)


def encode_batch_payload(items: list[bytes]) -> bytes:
    payload = struct.pack("<H", len(items))
    for item in items:
        payload += struct.pack("<I", len(item)) + item
    return payload


def encode_batch_frame(items: list[bytes]) -> bytes:
    payload = encode_batch_payload(items)
    return (
        MAGIC_BATCH
        + struct.pack("<HI", len(items), len(payload))
        + payload
    )


def parse_assign_body(body: bytes) -> AssignRequest:
    if len(body) < 4:
        raise ValueError("ASGN header too short")
    max_items, tag_len = struct.unpack_from("<HH", body, 0)
    offset = 4
    tag = body[offset : offset + tag_len].decode("utf-8", errors="replace")
    offset += tag_len
    if offset + 2 > len(body):
        raise ValueError("ASGN missing data_addr len")
    data_len = struct.unpack_from("<H", body, offset)[0]
    offset += 2
    data_addr = body[offset : offset + data_len].decode("utf-8", errors="replace")
    if not data_addr:
        raise ValueError("assign data_addr empty")
    return AssignRequest(max_items=max_items, consumer_tag=tag, data_addr=data_addr)
