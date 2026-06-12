"""Client metrics."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ClientStats:
    batches_in: int = 0
    msgs_in: int = 0
    batches_out: int = 0
    msgs_out: int = 0
    bytes_out: int = 0
    empty_waits: int = 0
    stale_batches: int = 0
    reconnects: int = 0
    drops: int = 0
    delivery_errors: int = 0
    queue_dropped_items: int = 0
