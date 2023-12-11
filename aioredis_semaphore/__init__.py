import time
from typing import Any, Callable, List, Optional

from redis.asyncio import Redis, StrictRedis


class NotAvailable(Exception):
    pass


class Semaphore:
    exists_val = "ok"

    def __init__(
        self,
        client: Redis,
        count: int,
        namespace: str = "SEMAPHORE",
        stale_client_timeout: Optional[int] = None,
        blocking: bool = True,
    ) -> None:
        self.client = client or StrictRedis()
        if count < 1:
            raise ValueError("Parameter 'count' must be larger than 1")
        self.count = count
        self.namespace = namespace
        self.stale_client_timeout = stale_client_timeout
        self.is_use_local_time = False
        self.blocking = blocking
        self._local_tokens: List[bytes] = list()

    async def _exists_or_init(self) -> None:
        old_key = await self.client.getset(self.check_exists_key, self.exists_val)
        if old_key:
            return
        return await self._init()

    async def _init(self) -> None:
        await self.client.expire(self.check_exists_key, 10)
        async with self.client.pipeline() as pipe:
            pipe.multi()
            await pipe.delete(self.grabbed_key, self.available_key)
            await pipe.rpush(self.available_key, *range(self.count))
            await pipe.execute()
        await self.client.persist(self.check_exists_key)

    async def available_count(self) -> int:
        return await self.client.llen(self.available_key)

    async def acquire(
        self, timeout: int = 0, target: Optional[Callable] = None
    ) -> bytes:
        await self._exists_or_init()
        if self.stale_client_timeout is not None:
            await self.release_stale_locks()

        if self.blocking:
            pair = await self.client.blpop(self.available_key, timeout)
            if pair is None:
                raise NotAvailable
            token = pair[1]
        else:
            token = await self.client.lpop(self.available_key)
            if token is None:
                raise NotAvailable

        self._local_tokens.append(token)
        await self.client.hset(self.grabbed_key, token, await self.current_time())
        if target is not None:
            try:
                target(token)
            finally:
                await self.signal(token)
        return token

    async def release_stale_locks(self, expires: int = 10) -> None:
        token = await self.client.getset(self.check_release_locks_key, self.exists_val)
        if token:
            return
        await self.client.expire(self.check_release_locks_key, expires)
        try:
            for token, looked_at in await self.client.hgetall(self.grabbed_key):
                timed_out_at = float(looked_at) + (self.stale_client_timeout or 0)
                if timed_out_at < await self.current_time():
                    await self.signal(token)
        finally:
            await self.client.delete(self.check_release_locks_key)

    async def _is_locked(self, token: bytes) -> bool:
        return await self.client.hexists(self.grabbed_key, token)

    async def has_lock(self) -> bool:
        for t in self._local_tokens:
            if await self._is_locked(t):
                return True
        return False

    async def release(self) -> None:
        if not await self.has_lock():
            return
        await self.signal(self._local_tokens.pop())

    async def reset(self) -> None:
        await self._init()

    async def signal(self, token: bytes) -> None:
        if token is None:
            return None
        async with self.client.pipeline() as pipe:
            pipe.multi()
            await pipe.hdel(self.grabbed_key, token)
            await pipe.lpush(self.available_key, token)
            await pipe.execute()

    def get_namespaced_key(self, suffix: str) -> str:
        return "{0}:{1}".format(self.namespace, suffix)

    @property
    def check_exists_key(self) -> str:
        return self._get_or_set_key("_exists_key", "EXISTS")

    @property
    def available_key(self) -> str:
        return self._get_or_set_key("_available_key", "AVAILABLE")

    @property
    def grabbed_key(self) -> str:
        return self._get_or_set_key("_grabbed_key", "GRABBED")

    @property
    def check_release_locks_key(self) -> str:
        return self._get_or_set_key("_release_locks_ley", "RELEASE_LOCKS")

    def _get_or_set_key(self, key_name: str, namespace_suffix: str) -> str:
        if not hasattr(self, key_name):
            setattr(self, key_name, self.get_namespaced_key(namespace_suffix))
        return getattr(self, key_name)

    async def current_time(self) -> float:
        if self.is_use_local_time:
            return time.time()
        return float(".".join(map(str, await self.client.time())))

    async def __aenter__(self) -> "Semaphore":
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        await self.release()
