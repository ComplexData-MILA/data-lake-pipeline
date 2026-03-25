"""
Distributed lock system via Cloudflare.

Usage:

base_ws_url = "wss://cf-mutex.<your-subdomain>.workers.dev"

async with AsyncCloudflareMutex(base_ws_url, "example-lock") as lock:
    print("acquired", lock.client_id, "fencing_token=", lock.fencing_token)
    await asyncio.sleep(5)
    print("done")
"""

import asyncio
import json
import time
import uuid
from contextlib import suppress

from websockets.asyncio.client import connect


class MutexError(Exception):
    pass


class AsyncCloudflareMutex:
    def __init__(self, base_ws_url: str, lock_name: str, client_id: str | None = None):
        self.base_ws_url = base_ws_url.rstrip("/")
        self.lock_name = lock_name
        self.client_id = client_id or f"py-{uuid.uuid4()}"
        self.ws = None
        self._lease_task = None
        self._held = False
        self._fencing_token = None

    @property
    def fencing_token(self) -> int | None:
        return self._fencing_token

    async def connect(self) -> None:
        url = f"{self.base_ws_url}/ws/{self.lock_name}"
        self.ws = await connect(url)
        hello = json.loads(await self.ws.recv())
        if hello["type"] != "hello_ok":
            raise MutexError(f"unexpected handshake message: {hello}")
        await self._send({"type": "hello", "clientId": self.client_id})

    async def close(self) -> None:
        if self._lease_task:
            self._lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._lease_task
            self._lease_task = None
        if self.ws:
            await self.ws.close()
            self.ws = None

    async def acquire(self, timeout: float = 30.0, ttl_ms: int = 30_000) -> int:
        if not self.ws:
            await self.connect()

        deadline = time.monotonic() + timeout

        while True:
            await self._send({"type": "acquire", "ttlMs": ttl_ms})
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("timed out waiting for mutex")

            while True:
                raw = await asyncio.wait_for(self.ws.recv(), timeout=remaining)
                msg = json.loads(raw)

                if msg["type"] == "granted":
                    self._held = True
                    self._fencing_token = msg["fencingToken"]
                    self._start_lease_renewer(ttl_ms)
                    return self._fencing_token

                if msg["type"] == "busy":
                    # Wait until either:
                    # 1) the server broadcasts "available", or
                    # 2) the current lease likely expires.
                    now_ms = int(time.time() * 1000)
                    sleep_s = max(
                        0.25, min(2.0, (msg["leaseUntilMs"] - now_ms) / 1000.0)
                    )
                    try:
                        raw2 = await asyncio.wait_for(self.ws.recv(), timeout=sleep_s)
                        msg2 = json.loads(raw2)
                        if msg2["type"] == "available":
                            break
                    except asyncio.TimeoutError:
                        break

                elif msg["type"] == "error":
                    raise MutexError(msg["error"])

                elif msg["type"] == "available":
                    break

    async def release(self) -> None:
        if not self.ws or not self._held:
            return

        if self._lease_task:
            self._lease_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._lease_task
            self._lease_task = None

        await self._send({"type": "release"})
        raw = await self.ws.recv()
        msg = json.loads(raw)
        if msg["type"] == "released":
            self._held = False
            self._fencing_token = None
            return
        if msg["type"] == "error":
            raise MutexError(msg["error"])
        raise MutexError(f"unexpected release response: {msg}")

    async def __aenter__(self):
        await self.connect()
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self.release()
        finally:
            await self.close()

    async def _send(self, msg: dict) -> None:
        assert self.ws is not None
        await self.ws.send(json.dumps(msg))

    def _start_lease_renewer(self, ttl_ms: int) -> None:
        async def renew_loop():
            # Renew at half the lease interval.
            period = max(1.0, ttl_ms / 2000.0)
            while True:
                await asyncio.sleep(period)
                if not self._held or not self.ws:
                    return
                await self._send({"type": "renew", "ttlMs": ttl_ms})
                raw = await self.ws.recv()
                msg = json.loads(raw)
                if msg["type"] != "granted":
                    raise MutexError(f"lease renew failed: {msg}")

        self._lease_task = asyncio.create_task(renew_loop())
