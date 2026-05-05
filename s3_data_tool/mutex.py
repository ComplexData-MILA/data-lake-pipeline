import asyncio
import json
import time
import uuid
from os import environ
import logging

from websockets.asyncio.client import connect


class WSSMutex:
    def __init__(self, lock_name: str, base_url: str | None = None):
        if not base_url:
            base_url = environ["WSS_MUTEX_BASE_URL"]
        self.url = f"{base_url.rstrip('/')}/ws/{lock_name}"
        self.client_id = uuid.uuid4()
        self.ws = None
        self._acquired_at = None
        self._ttl_ms = None
        self.logger = logging.getLogger(__name__)

    async def connect(self):
        """Establishes WS connection and performs handshake."""
        self.ws = await connect(self.url)

        # 1. Expect Server Hello
        msg = json.loads(await self.ws.recv())
        if msg.get("type") != "hello_ok":
            raise Exception("Handshake failed")

        # 2. Send Client Hello
        await self.ws.send(
            json.dumps({"type": "hello", "clientId": str(self.client_id)})
        )

    async def acquire(self, ttl_ms: int = 10_000) -> None:
        """Acquires the lock, retrying automatically if busy."""
        if not self.ws:
            await self.connect()

        while True:
            # Request lock
            await self.ws.send(json.dumps({"type": "acquire", "ttlMs": ttl_ms}))

            msg = json.loads(await self.ws.recv())

            if msg["type"] == "granted":
                self._acquired_at = time.monotonic()
                self._ttl_ms = ttl_ms
                return

            elif msg["type"] == "busy":
                # Simplified: Just wait 1 second and retry
                await asyncio.sleep(1.0)

            elif msg["type"] == "error":
                raise Exception(f"Server error: {msg['error']}")

    async def release(self):
        """Releases the lock and closes connection."""
        if not self.ws:
            return

        elapsed = (time.monotonic() - self._acquired_at) * 1000 if self._acquired_at is not None else 0
        if self._ttl_ms is not None and elapsed > self._ttl_ms:
            print(f"Lock was not released in time ({elapsed:.0f}ms > {self._ttl_ms}ms TTL)")

        await self.ws.send(json.dumps({"type": "release"}))
        await self.ws.close()
        self.ws = None

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()
        return False
