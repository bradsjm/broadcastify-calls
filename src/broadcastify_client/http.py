"""Async HTTP client abstraction tailored for Broadcastify interactions."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any, Protocol

import httpx

from .config import HttpClientConfig
from .errors import TransportError


class AsyncHttpClientProtocol(Protocol):
    """Protocol describing the async HTTP operations required by the client."""

    async def post_form(
        self,
        url: str,
        data: Mapping[str, str],
        *,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:  # pragma: no cover - protocol signature
        """Send a form-encoded POST request and return the HTTP response."""

        ...

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> httpx.Response:  # pragma: no cover - protocol signature
        """Send a GET request and return the HTTP response."""

        ...

    async def close(self) -> None:  # pragma: no cover - protocol signature
        """Release HTTP resources and close underlying connections."""

        ...


class BroadcastifyHttpClient(AsyncHttpClientProtocol):
    """httpx-based client that injects required headers and manages connection pooling."""

    def __init__(self, config: HttpClientConfig | None = None) -> None:
        """Initialise the HTTP client with optional *config*."""

        self._config = config or HttpClientConfig()
        limits = httpx.Limits(max_connections=self._config.max_connections or None)
        self._client = httpx.AsyncClient(
            base_url=str(self._config.base_url),
            http2=self._config.enable_http2,
            limits=limits,
            headers=self._build_default_headers(),
        )

    async def post_form(
        self,
        url: str,
        data: Mapping[str, str],
        *,
        headers: Mapping[str, str] | None = None,
    ) -> httpx.Response:
        """Send a form-encoded POST request with browser-mimicking headers."""

        try:
            response = await self._client.post(url, data=data, headers=self._merge_headers(headers))
        except httpx.HTTPError as exc:  # pragma: no cover - network failure path
            raise TransportError(str(exc)) from exc
        response.raise_for_status()
        return response

    async def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        params: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        """Send a GET request respecting connection pooling and compression settings."""

        try:
            response = await self._client.get(
                url,
                params=params,
                headers=self._merge_headers(headers),
            )
        except httpx.HTTPError as exc:  # pragma: no cover - network failure path
            raise TransportError(str(exc)) from exc
        response.raise_for_status()
        return response

    async def close(self) -> None:
        """Close the underlying httpx.AsyncClient instance."""

        await self._client.aclose()

    def _merge_headers(self, headers: Mapping[str, str] | None) -> MutableMapping[str, str]:
        """Merge default headers with user-provided *headers*."""

        merged: MutableMapping[str, str] = dict(self._client.headers)
        if headers:
            merged.update(headers)
        return merged

    def _build_default_headers(self) -> MutableMapping[str, str]:
        """Return the default header set applied to every request."""

        return {
            "User-Agent": self._config.user_agent,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,*/*;q=0.8"
            ),
            "Accept-Language": self._config.accept_language,
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
        }
