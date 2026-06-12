"""Optional harness payload helpers — dev only, not part of the library."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from pathlib import Path

MAGIC = b"CUPIDMQ\0"
HEADER_FMT = "<8sQI"
HEADER_SIZE = struct.calcsize(HEADER_FMT)


@dataclass
class HarnessPayload:
    seq: int
    source_id: int
    body: bytes

    @property
    def total_bytes(self) -> int:
        return HEADER_SIZE + len(self.body)


def parse_harness_payload(raw: bytes) -> HarnessPayload | None:
    if len(raw) < HEADER_SIZE or raw[:8] != MAGIC:
        return None
    magic, seq, source_id = struct.unpack_from(HEADER_FMT, raw, 0)
    if magic != MAGIC:
        return None
    return HarnessPayload(seq=seq, source_id=source_id, body=raw[HEADER_SIZE:])


def dump_payload(raw: bytes, out_dir: Path, index: int) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    parsed = parse_harness_payload(raw)
    if parsed is None:
        name = f"raw-{index:06d}-{len(raw)}b.bin"
    else:
        name = f"seq{parsed.seq}-src{parsed.source_id}-{parsed.total_bytes}b.bin"
    path = out_dir / name
    path.write_bytes(raw)
    return path
