from __future__ import annotations

import asyncio
import re
import threading
import time
from collections import OrderedDict

from starlette.types import ASGIApp, Message, Receive, Scope, Send


_TASK_DETAIL_PATH = re.compile(r"^/api/tasks/[0-9]+$")


class WebReadGuardMiddleware:
    """Keep expensive dashboard reads from starving control-plane traffic.

    The scheduler intentionally runs as one process because starting multiple
    web workers would also start duplicate scheduler loops.  Large dashboard
    and task-inventory GETs therefore share the sync endpoint thread pool with
    AEDT lease/session heartbeats.  This middleware:

    * coalesces identical dashboard/inventory reads and keeps a very short
      server-side response cache;
    * bounds distinct bulk reads and legacy per-task detail bursts *before*
      they consume sync worker tokens; and
    * leaves health, heartbeat, command, and every mutating request untouched.

    Cached responses are keyed by the exact query string and accepted content
    encoding.  The cache is deliberately short-lived so it protects bursts
    without turning into application state.
    """

    _CACHEABLE_PATHS = frozenset({"/", "/api/tasks"})

    def __init__(
        self,
        app: ASGIApp,
        *,
        ttl_seconds: float = 2.0,
        max_entries: int = 64,
        max_cache_body_bytes: int = 8 * 1024 * 1024,
        max_bulk_concurrency: int = 4,
        max_task_detail_concurrency: int = 16,
    ) -> None:
        self.app = app
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self.max_entries = max(1, int(max_entries))
        self.max_cache_body_bytes = max(0, int(max_cache_body_bytes))
        self.max_bulk_concurrency = max(1, int(max_bulk_concurrency))
        self.max_task_detail_concurrency = max(
            1, int(max_task_detail_concurrency)
        )
        self._cache: OrderedDict[
            tuple[bytes, bytes, bytes], tuple[float, tuple[Message, ...]]
        ] = OrderedDict()
        self._inflight: dict[
            tuple[int, tuple[bytes, bytes, bytes]], asyncio.Future[tuple[Message, ...] | None]
        ] = {}
        self._semaphores: dict[tuple[int, str], asyncio.Semaphore] = {}
        # Multiple TestClient instances can use different event-loop threads.
        # The lock protects only tiny dictionary operations and is never held
        # across an await or application work.
        self._state_lock = threading.Lock()

    @staticmethod
    def _header(scope: Scope, name: bytes) -> bytes:
        for key, value in scope.get("headers") or []:
            if key.lower() == name:
                return value
        return b""

    def _cache_key(self, scope: Scope) -> tuple[bytes, bytes, bytes]:
        return (
            str(scope.get("path") or "").encode("utf-8", errors="replace"),
            bytes(scope.get("query_string") or b""),
            self._header(scope, b"accept-encoding"),
        )

    @staticmethod
    def _copy_messages(messages: list[Message]) -> tuple[Message, ...]:
        copied: list[Message] = []
        for message in messages:
            item = dict(message)
            if "headers" in item:
                item["headers"] = list(item["headers"])
            if "body" in item:
                item["body"] = bytes(item["body"])
            copied.append(item)
        return tuple(copied)

    @staticmethod
    async def _replay(messages: tuple[Message, ...], send: Send) -> None:
        for message in messages:
            await send(dict(message))

    def _cached(
        self, key: tuple[bytes, bytes, bytes], now: float
    ) -> tuple[Message, ...] | None:
        with self._state_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            expires_at, messages = entry
            if expires_at <= now:
                self._cache.pop(key, None)
                return None
            self._cache.move_to_end(key)
            return messages

    def _store(
        self,
        key: tuple[bytes, bytes, bytes],
        messages: tuple[Message, ...],
        now: float,
    ) -> None:
        body_bytes = sum(
            len(message.get("body") or b"")
            for message in messages
            if message.get("type") == "http.response.body"
        )
        status = next(
            (
                int(message.get("status") or 0)
                for message in messages
                if message.get("type") == "http.response.start"
            ),
            0,
        )
        if (
            status != 200
            or self.ttl_seconds <= 0
            or body_bytes > self.max_cache_body_bytes
        ):
            return
        with self._state_lock:
            self._cache[key] = (now + self.ttl_seconds, messages)
            self._cache.move_to_end(key)
            while len(self._cache) > self.max_entries:
                self._cache.popitem(last=False)

    def _semaphore(self, loop: asyncio.AbstractEventLoop, kind: str) -> asyncio.Semaphore:
        key = (id(loop), kind)
        with self._state_lock:
            semaphore = self._semaphores.get(key)
            if semaphore is None:
                limit = (
                    self.max_bulk_concurrency
                    if kind == "bulk"
                    else self.max_task_detail_concurrency
                )
                semaphore = asyncio.Semaphore(limit)
                self._semaphores[key] = semaphore
            return semaphore

    async def _bounded_call(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
        *,
        kind: str,
    ) -> None:
        semaphore = self._semaphore(asyncio.get_running_loop(), kind)
        async with semaphore:
            await self.app(scope, receive, send)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") != "http" or scope.get("method") != "GET":
            await self.app(scope, receive, send)
            return

        path = str(scope.get("path") or "")
        if _TASK_DETAIL_PATH.fullmatch(path):
            await self._bounded_call(
                scope, receive, send, kind="task-detail"
            )
            return
        if path not in self._CACHEABLE_PATHS:
            await self.app(scope, receive, send)
            return

        key = self._cache_key(scope)
        cached = self._cached(key, time.monotonic())
        if cached is not None:
            await self._replay(cached, send)
            return

        loop = asyncio.get_running_loop()
        inflight_key = (id(loop), key)
        with self._state_lock:
            future = self._inflight.get(inflight_key)
            leader = future is None
            if future is None:
                future = loop.create_future()
                self._inflight[inflight_key] = future

        if not leader:
            messages = await future
            if messages is None:
                # The leader failed before producing a response.  Let the
                # normal exception middleware handle an independent retry,
                # while retaining the bulk bound for all waiting requests.
                await self._bounded_call(
                    scope, receive, send, kind="bulk"
                )
            else:
                await self._replay(messages, send)
            return

        captured: list[Message] = []

        async def capture(message: Message) -> None:
            captured.append(message)

        try:
            await self._bounded_call(
                scope, receive, capture, kind="bulk"
            )
            messages = self._copy_messages(captured)
            self._store(key, messages, time.monotonic())
            with self._state_lock:
                self._inflight.pop(inflight_key, None)
                if not future.done():
                    future.set_result(messages)
            await self._replay(messages, send)
        except BaseException:
            with self._state_lock:
                self._inflight.pop(inflight_key, None)
                if not future.done():
                    future.set_result(None)
            raise
