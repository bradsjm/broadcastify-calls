"""Authentication primitives for the Broadcastify client."""

from __future__ import annotations

import logging
from typing import Protocol

import httpx

from .config import Credentials, HttpClientConfig
from .errors import AuthenticationError
from .http import AsyncHttpClientProtocol
from .models import SessionToken

logger = logging.getLogger(__name__)


class AuthenticationBackend(Protocol):
    """Protocol implemented by authentication backends."""

    async def login(self, credentials: Credentials) -> SessionToken:  # pragma: no cover - protocol
        """Authenticate with Broadcastify and return a session token."""

        ...

    async def logout(self, token: SessionToken) -> None:  # pragma: no cover - protocol
        """Invalidate the provided session token (best effort)."""

        ...


class HttpAuthenticationBackend(AuthenticationBackend):
    """Authentication backend using the Broadcastify login form."""

    def __init__(
        self,
        http_client: AsyncHttpClientProtocol,
        config: HttpClientConfig | None = None,
    ) -> None:
        """Create an HTTP authentication backend using *http_client*."""

        self._http_client = http_client
        self._config = config or HttpClientConfig()

    async def login(self, credentials: Credentials) -> SessionToken:
        """Submit the login form and extract the session cookie."""

        logger.debug("Submitting Broadcastify login request for user %s", credentials.username)
        response = await self._http_client.post_form(
            url="/login/",
            data={
                "username": credentials.username,
                "password": credentials.password,
                "action": "auth",
                "redirect": "https://www.broadcastify.com",
            },
        )
        token_value = _extract_session_cookie(response.cookies)
        if token_value is None:
            logger.error("Login response missing session cookie for user %s", credentials.username)
            raise AuthenticationError("Missing session cookie in login response")
        return SessionToken(token=token_value)

    async def logout(self, token: SessionToken) -> None:
        """Try to inform Broadcastify that the session should be terminated."""

        try:
            await self._http_client.get("/account/?action=logout")
        except httpx.HTTPError as exc:  # pragma: no cover - best effort path
            logger.warning("Broadcastify logout request failed", exc_info=exc)
            return


def _extract_session_cookie(cookies: httpx.Cookies) -> str | None:
    """Return the known session cookie value if present."""

    cookie = cookies.get("bcfyuser1", None)
    return cookie


CredentialsOrToken = Credentials | SessionToken


class Authenticator:
    """Stateful helper that manages session tokens via an authentication backend."""

    def __init__(self, backend: AuthenticationBackend) -> None:
        """Initialise the authenticator with an authentication *backend*."""

        self._backend = backend
        self._token: SessionToken | None = None

    async def authenticate(self, credentials_or_token: CredentialsOrToken) -> SessionToken:
        """Return a valid session token, refreshing it if required."""

        if isinstance(credentials_or_token, SessionToken):
            if credentials_or_token.is_expired():
                raise AuthenticationError("Provided session token is expired")
            self._token = credentials_or_token
            return credentials_or_token
        if self._token and not self._token.is_expired():
            return self._token
        self._token = await self._backend.login(credentials_or_token)
        return self._token

    async def logout(self) -> None:
        """Invalidate the active session token if one exists."""

        if not self._token:
            return
        await self._backend.logout(self._token)
        self._token = None
