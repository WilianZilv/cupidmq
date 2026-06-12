"""Consumer bind vs advertise address resolution."""

from __future__ import annotations

import unittest

from cupidmq.config import ConsumerConfig


class ConsumerBindTests(unittest.TestCase):
    def test_effective_bind_defaults_all_interfaces(self) -> None:
        cfg = ConsumerConfig(
            master="127.0.0.1:9750",
            data_addr="192.168.1.10:9760",
        )
        self.assertEqual(cfg.data_addr, "192.168.1.10:9760")
        self.assertEqual(cfg.effective_bind_addr, "0.0.0.0:9760")

    def test_explicit_bind_addr(self) -> None:
        cfg = ConsumerConfig(
            master="127.0.0.1:9750",
            data_addr="127.0.0.1:9760",
            bind_addr="0.0.0.0:9761",
        )
        self.assertEqual(cfg.effective_bind_addr, "0.0.0.0:9761")


if __name__ == "__main__":
    unittest.main()
