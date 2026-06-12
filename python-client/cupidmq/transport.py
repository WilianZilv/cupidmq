"""TCP helpers."""

from __future__ import annotations

import asyncio
import socket
import struct

from cupidmq.wire import MAGIC_BATCH, MAX_BATCH_BYTES


async def read_exact(reader: asyncio.StreamReader, n: int) -> bytes:
    return await reader.readexactly(n)


async def read_batch_payload(reader: asyncio.StreamReader) -> bytes:
    hdr = await read_exact(reader, 6)
    payload_len = struct.unpack_from("<I", hdr, 2)[0]
    if payload_len > MAX_BATCH_BYTES:
        raise ValueError(f"batch payload too large ({payload_len})")
    if payload_len == 0:
        return b""
    return await read_exact(reader, payload_len)


async def read_batch_frame(reader: asyncio.StreamReader) -> bytes:
    magic = await read_exact(reader, 4)
    if magic != MAGIC_BATCH:
        raise ValueError(f"expected BATC, got {magic!r}")
    return await read_batch_payload(reader)


def tune_stream(writer: asyncio.StreamWriter) -> None:
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 2)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
