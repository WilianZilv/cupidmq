"""Probe TCP BATC — simulates a producer against a consumer data port."""
import asyncio
import struct
import sys

MAGIC_BATCH = b"BATC"


def encode_empty_batc() -> bytes:
    inner = struct.pack("<H", 0)  # count=0
    return MAGIC_BATCH + struct.pack("<HI", 0, len(inner)) + inner


async def probe(port: int) -> None:
    addr = ("127.0.0.1", port)
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(*addr), timeout=2)
    except Exception as e:
        print(f":{port} CONNECT FAIL: {type(e).__name__}: {e}")
        return
    frame = encode_empty_batc()
    writer.write(frame)
    try:
        await asyncio.wait_for(writer.drain(), timeout=3)
        print(f":{port} BATC flush OK (no reply on data plane)")
    except asyncio.TimeoutError:
        print(f":{port} FLUSH TIMEOUT")
    except Exception as e:
        print(f":{port} FLUSH ERR: {type(e).__name__}: {e}")
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        del reader


async def main() -> None:
    ports = [int(p) for p in sys.argv[1:]] if len(sys.argv) > 1 else [9760, 9761, 9764]
    await asyncio.gather(*(probe(p) for p in ports))


if __name__ == "__main__":
    asyncio.run(main())
