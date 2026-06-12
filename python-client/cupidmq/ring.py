"""Ring local — drop oldest quando cheio (cap só por bytes)."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class QueuedMessage:
    payload: bytes
    enqueued_ms: int


class ByteRing:
    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max(1, max_bytes)
        self._items: deque[QueuedMessage] = deque()
        self._bytes = 0
        self.drops_total = 0

    def __len__(self) -> int:
        return len(self._items)

    def byte_len(self) -> int:
        return self._bytes

    def is_empty(self) -> bool:
        return not self._items

    def push(self, payload: bytes, enqueued_ms: int) -> None:
        if not payload:
            return
        if len(payload) > self._max_bytes:
            self.drops_total += 1
            return
        while self._bytes + len(payload) > self._max_bytes and self._items:
            dropped = self._items.popleft()
            self._bytes -= len(dropped.payload)
            self.drops_total += 1
        self._items.append(QueuedMessage(payload=payload, enqueued_ms=enqueued_ms))
        self._bytes += len(payload)

    def push_front(self, msg: QueuedMessage) -> None:
        self._items.appendleft(msg)
        self._bytes += len(msg.payload)
        while self._bytes > self._max_bytes and self._items:
            dropped = self._items.pop()
            self._bytes -= len(dropped.payload)
            self.drops_total += 1

    def pop_front(self) -> QueuedMessage | None:
        if not self._items:
            return None
        msg = self._items.popleft()
        self._bytes -= len(msg.payload)
        return msg

    def pop_up_to(self, max_items: int, enqueued_ms: int) -> list[QueuedMessage]:
        out: list[QueuedMessage] = []
        while len(out) < max_items and self._items:
            out.append(self._items.popleft())
        self._bytes = sum(len(m.payload) for m in self._items)
        return out
