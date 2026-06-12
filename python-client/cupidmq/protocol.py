"""Split inner BATC batch — bytes only, no domain decode."""

from __future__ import annotations

import struct


def split_batch_payload(payload: bytes) -> list[bytes]:
    """Inner batch `[count u16][len u32][item]…` → lista de items brutos."""
    if len(payload) < 2:
        return []
    count = struct.unpack_from("<H", payload, 0)[0]
    offset = 2
    out: list[bytes] = []
    for _ in range(count):
        if offset + 4 > len(payload):
            break
        item_len = struct.unpack_from("<I", payload, offset)[0]
        offset += 4
        if offset + item_len > len(payload):
            break
        out.append(payload[offset : offset + item_len])
        offset += item_len
    return out
