from __future__ import annotations

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class ObserverGuardMiddleware:
    """Reject writes before route matching, body parsing, or endpoint execution."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("type") == "http" and scope.get("method") not in {"GET", "HEAD", "OPTIONS"}:
            response = JSONResponse(
                {"detail": "observer mode rejects mutating requests"}, status_code=403
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
