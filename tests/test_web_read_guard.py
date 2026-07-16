from __future__ import annotations

import asyncio
import gzip
import time
import unittest
from dataclasses import dataclass
from urllib.parse import urlsplit

from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from starlette.middleware.gzip import GZipMiddleware

from slurm_scheduler.web_read_guard import WebReadGuardMiddleware


@dataclass(frozen=True)
class CapturedResponse:
    status_code: int
    headers: dict[str, str]
    content: bytes

    @property
    def text(self) -> str:
        content = self.content
        if self.headers.get("content-encoding") == "gzip":
            content = gzip.decompress(content)
        return content.decode("utf-8")


class WebReadGuardTests(unittest.TestCase):
    @staticmethod
    async def _request(
        app,
        target: str,
        *,
        method: str = "GET",
        headers: dict[str, str] | None = None,
    ) -> CapturedResponse:
        parsed = urlsplit(target)
        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": method,
            "scheme": "http",
            "path": parsed.path,
            "raw_path": parsed.path.encode("ascii"),
            "query_string": parsed.query.encode("ascii"),
            "headers": [
                (key.lower().encode("ascii"), value.encode("ascii"))
                for key, value in (headers or {}).items()
            ],
            "client": ("testclient", 50000),
            "server": ("testserver", 80),
            "root_path": "",
        }
        request_sent = False

        async def receive():
            nonlocal request_sent
            if not request_sent:
                request_sent = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.disconnect"}

        messages = []

        async def send(message):
            messages.append(dict(message))

        await app(scope, receive, send)
        start = next(
            message for message in messages if message["type"] == "http.response.start"
        )
        response_headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in start.get("headers") or []
        }
        content = b"".join(
            message.get("body") or b""
            for message in messages
            if message["type"] == "http.response.body"
        )
        return CapturedResponse(
            int(start["status"]), response_headers, content
        )

    def test_identical_bulk_reads_are_coalesced_and_short_lived(self) -> None:
        async def run() -> None:
            inner = FastAPI()
            calls = 0

            @inner.get("/")
            async def dashboard() -> PlainTextResponse:
                nonlocal calls
                calls += 1
                await asyncio.sleep(0.05)
                return PlainTextResponse("dashboard")

            app = WebReadGuardMiddleware(inner, ttl_seconds=0.08)
            responses = await asyncio.gather(
                *(self._request(app, "/") for _ in range(30))
            )
            self.assertEqual({response.status_code for response in responses}, {200})
            self.assertEqual({response.text for response in responses}, {"dashboard"})
            self.assertEqual(calls, 1)
            await asyncio.sleep(0.1)
            self.assertEqual((await self._request(app, "/")).status_code, 200)
            self.assertEqual(calls, 2)

        asyncio.run(run())

    def test_distinct_bulk_reads_are_bounded_before_entering_application(self) -> None:
        async def run() -> None:
            inner = FastAPI()
            active = 0
            peak = 0

            @inner.get("/api/tasks")
            async def tasks() -> dict[str, int]:
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                try:
                    await asyncio.sleep(0.04)
                    return {"active": active}
                finally:
                    active -= 1

            app = WebReadGuardMiddleware(
                inner, ttl_seconds=0, max_bulk_concurrency=2
            )
            responses = await asyncio.gather(
                *(
                    self._request(app, f"/api/tasks?page={index}")
                    for index in range(8)
                )
            )

            self.assertEqual({response.status_code for response in responses}, {200})
            self.assertLessEqual(peak, 2)

        asyncio.run(run())

    def test_task_detail_burst_is_bounded_but_heartbeat_post_bypasses_guard(self) -> None:
        async def run() -> None:
            inner = FastAPI()
            active = 0
            peak = 0

            @inner.get("/api/tasks/{task_id}")
            async def task_detail(task_id: int) -> dict[str, int]:
                nonlocal active, peak
                active += 1
                peak = max(peak, active)
                try:
                    await asyncio.sleep(0.08)
                    return {"id": task_id}
                finally:
                    active -= 1

            @inner.post("/api/aedt-pool/leases/{lease_id}/heartbeat")
            async def heartbeat(lease_id: int) -> dict[str, int]:
                return {"lease_id": lease_id}

            app = WebReadGuardMiddleware(
                inner, max_task_detail_concurrency=2
            )
            details = [
                asyncio.create_task(self._request(app, f"/api/tasks/{index}"))
                for index in range(12)
            ]
            await asyncio.sleep(0.01)
            started = time.perf_counter()
            heartbeat_response = await self._request(
                app,
                "/api/aedt-pool/leases/7/heartbeat",
                method="POST",
            )
            heartbeat_elapsed = time.perf_counter() - started
            detail_responses = await asyncio.gather(*details)

            self.assertEqual(heartbeat_response.status_code, 200)
            self.assertLess(heartbeat_elapsed, 0.05)
            self.assertEqual({response.status_code for response in detail_responses}, {200})
            self.assertLessEqual(peak, 2)

        asyncio.run(run())

    def test_mutation_bypasses_cache_and_read_staleness_is_bounded_by_ttl(self) -> None:
        async def run() -> None:
            inner = FastAPI()
            value = 1

            @inner.get("/")
            async def dashboard() -> dict[str, int]:
                return {"value": value}

            @inner.post("/mutate")
            async def mutate() -> dict[str, int]:
                nonlocal value
                value += 1
                return {"value": value}

            app = WebReadGuardMiddleware(inner, ttl_seconds=0.08)
            first = await self._request(app, "/")
            mutation = await self._request(app, "/mutate", method="POST")
            briefly_stale = await self._request(app, "/")
            await asyncio.sleep(0.1)
            refreshed = await self._request(app, "/")

            self.assertEqual(first.text, '{"value":1}')
            self.assertEqual(mutation.text, '{"value":2}')
            self.assertEqual(briefly_stale.text, '{"value":1}')
            self.assertEqual(refreshed.text, '{"value":2}')

        asyncio.run(run())

    def test_guard_cache_keeps_gzip_variants_separate(self) -> None:
        async def run() -> None:
            inner = FastAPI()
            calls = 0

            @inner.get("/")
            async def dashboard() -> PlainTextResponse:
                nonlocal calls
                calls += 1
                return PlainTextResponse("x" * 5000)

            compressed = GZipMiddleware(inner, minimum_size=1000, compresslevel=5)
            app = WebReadGuardMiddleware(compressed, ttl_seconds=1)
            gzip_response = await self._request(
                app, "/", headers={"Accept-Encoding": "gzip"}
            )
            identity_response = await self._request(
                app, "/", headers={"Accept-Encoding": "identity"}
            )
            cached_gzip_response = await self._request(
                app, "/", headers={"Accept-Encoding": "gzip"}
            )

            self.assertEqual(gzip_response.headers.get("content-encoding"), "gzip")
            self.assertIsNone(identity_response.headers.get("content-encoding"))
            self.assertEqual(gzip_response.text, "x" * 5000)
            self.assertEqual(cached_gzip_response.text, "x" * 5000)
            self.assertEqual(calls, 2)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
