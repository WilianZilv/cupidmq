"""Batch por tamanho + timeout — espelha cupidmq producer accumulator."""

from __future__ import annotations

import time


class BatchAccumulator:
    def __init__(self, max_bytes: int, flush_timeout_secs: float) -> None:
        self._max_bytes = max(1, max_bytes)
        self._flush_timeout = max(0.001, flush_timeout_secs)
        self._items: list[bytes] = []
        self._bytes = 0
        self._opened_at: float | None = None

    def push(self, item: bytes) -> list[list[bytes]]:
        item_len = len(item)
        if item_len > self._max_bytes:
            out: list[list[bytes]] = []
            if batch := self.take_batch():
                out.append(batch)
            out.append([item])
            return out

        out = []
        if self._bytes + item_len > self._max_bytes:
            if batch := self.take_batch():
                out.append(batch)

        if self._opened_at is None:
            self._opened_at = time.monotonic()
        self._bytes += item_len
        self._items.append(item)
        return out

    def flush_if_timeout(self) -> list[bytes] | None:
        if not self._items or self._opened_at is None:
            return None
        if time.monotonic() - self._opened_at < self._flush_timeout:
            return None
        return self.take_batch()

    def take_batch(self) -> list[bytes] | None:
        if not self._items:
            return None
        self._bytes = 0
        self._opened_at = None
        batch = self._items
        self._items = []
        return batch

    @property
    def pending_items(self) -> int:
        return len(self._items)

    @property
    def pending_bytes(self) -> int:
        return self._bytes
