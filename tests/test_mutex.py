import asyncio
import uuid
from os import environ

import pytest
from dotenv import load_dotenv

from s3_data_tool.mutex import WSSMutex

load_dotenv()


def get_base_url():
    return environ.get("WSS_MUTEX_BASE_URL")


@pytest.fixture
def lock_name():
    return f"test-lock-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def base_url():
    url = get_base_url()
    if not url:
        pytest.skip("WSS_MUTEX_BASE_URL not set in environment")
    return url


@pytest.fixture
async def server_available(base_url):
    mutex = WSSMutex(f"probe-{uuid.uuid4().hex[:8]}", base_url)
    try:
        await asyncio.wait_for(mutex.connect(), timeout=10.0)
        await mutex.ws.close()
        return True
    except (TimeoutError, OSError, Exception):
        pytest.skip("WSS Mutex server unavailable")


@pytest.fixture
def mutex(lock_name, base_url):
    return WSSMutex(lock_name, base_url)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_connect_handshake(mutex, server_available):
    await mutex.connect()
    assert mutex.ws is not None
    await mutex.ws.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_acquire_release(mutex, server_available):
    await mutex.acquire(ttl_ms=5000)
    assert mutex._acquired_at is not None
    assert mutex._ttl_ms == 5000
    await mutex.release()
    assert mutex.ws is None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_context_manager(lock_name, base_url, server_available):
    async with WSSMutex(lock_name, base_url) as m:
        assert m._acquired_at is not None
        assert m._ttl_ms is not None
    assert m.ws is None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_concurrent_lock_contention(lock_name, base_url, server_available):
    mutex1 = WSSMutex(lock_name, base_url)
    mutex2 = WSSMutex(lock_name, base_url)

    await mutex1.acquire(ttl_ms=5000)
    
    acquired_event = asyncio.Event()
    acquire_task = asyncio.create_task(acquire_and_signal(mutex2, acquired_event))
    
    await asyncio.sleep(0.5)
    assert not acquired_event.is_set(), "Second client should not acquire while first holds lock"
    
    await mutex1.release()
    
    await asyncio.wait_for(acquire_task, timeout=5.0)
    assert acquired_event.is_set(), "Second client should acquire after first releases"
    
    await mutex2.release()


async def acquire_and_signal(mutex: WSSMutex, event: asyncio.Event):
    await mutex.acquire(ttl_ms=5000)
    event.set()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_ttl_expiration_warning(lock_name, base_url, server_available, capsys):
    mutex = WSSMutex(lock_name, base_url)
    await mutex.acquire(ttl_ms=100)
    
    await asyncio.sleep(0.15)
    
    await mutex.release()
    
    captured = capsys.readouterr()
    assert "Lock was not released in time" in captured.out


@pytest.mark.asyncio
async def test_custom_base_url(lock_name):
    custom_url = "wss://custom.example.com"
    mutex = WSSMutex(lock_name, base_url=custom_url)
    assert custom_url.rstrip("/") in mutex.url
    assert lock_name in mutex.url


@pytest.mark.asyncio
async def test_env_base_url_fallback(lock_name, monkeypatch):
    monkeypatch.setenv("WSS_MUTEX_BASE_URL", "wss://env.example.com")
    mutex = WSSMutex(lock_name)
    assert "wss://env.example.com" in mutex.url


@pytest.mark.asyncio
@pytest.mark.integration
async def test_acquire_reconnect_on_missing_websocket(mutex, server_available):
    assert mutex.ws is None
    await mutex.acquire(ttl_ms=5000)
    assert mutex.ws is not None
    assert mutex._acquired_at is not None
    await mutex.release()
