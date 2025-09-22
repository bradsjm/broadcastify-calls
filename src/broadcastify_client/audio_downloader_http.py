"""HTTP-based audio downloader for Broadcastify live call audio.

Builds the canonical audio URL from live call metadata and downloads the
corresponding audio asset for downstream consumers (transcription, storage).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Final

from .audio_consumer import AudioDownloader
from .auth import Authenticator
from .errors import AudioDownloadError
from .http import AsyncHttpClientProtocol
from .models import AudioChunkEvent, LiveCallEnvelope

logger = logging.getLogger(__name__)


class HttpAudioDownloader(AudioDownloader):
    """Downloader that retrieves audio bytes via the Broadcastify calls CDN.

    The URL shape mirrors the web client:
        https://calls.broadcastify.com/{hash?}/{systemId}/{filename}.{enc}
    where ``hash`` is optional.
    """

    _BASE_HOST: Final[str] = "https://calls.broadcastify.com"

    def __init__(
        self,
        http: AsyncHttpClientProtocol,
        authenticator: Authenticator | None = None,
    ) -> None:
        """Create a downloader bound to the shared async HTTP client.

        Args:
            http: Shared HTTP client instance for requests.
            authenticator: Optional authenticator to retrieve the current
                session cookie value for cross-subdomain requests.
        
        """
        self._http = http
        self._auth = authenticator

    async def fetch_audio(self, call: LiveCallEnvelope) -> AsyncIterator[AudioChunkEvent]:
        """Return an async iterator yielding the audio payload as a single chunk.

        Current implementation downloads the entire asset in one request, then
        returns an async generator that yields the resulting chunk. This matches the
        ``AudioDownloader`` protocol, which is awaited by the consumer to obtain an
        ``AsyncIterator``.
        """
        raw = call.raw_payload
        filename_obj = raw.get("filename")
        enc_obj = raw.get("enc")
        hash_obj = raw.get("hash")

        if not isinstance(filename_obj, str) or not filename_obj:
            raise AudioDownloadError("Live call payload missing 'filename' for audio URL")
        if not isinstance(enc_obj, str) or not enc_obj:
            raise AudioDownloadError("Live call payload missing 'enc' (extension) for audio URL")
        system_id = call.call.system_id

        if isinstance(hash_obj, str) and hash_obj:
            url = f"{self._BASE_HOST}/{hash_obj}/{system_id}/{filename_obj}.{enc_obj}"
        else:
            url = f"{self._BASE_HOST}/{system_id}/{filename_obj}.{enc_obj}"

        logger.debug("Downloading audio for call %s from %s", call.call.call_id, url)
        try:
            # Include Referer and session cookie when available to satisfy CDN auth.
            headers = {"Accept": "*/*", "Referer": "https://www.broadcastify.com/calls/"}
            token = self._auth.session_cookie_value() if self._auth is not None else None
            if token:
                headers["Cookie"] = f"bcfyuser1={token}"
            response = await self._http.get(url, headers=headers)
        except Exception as exc:  # pragma: no cover - network failure path
            logger.warning("HTTP error retrieving audio for call %s: %s", call.call.call_id, exc)
            raise AudioDownloadError(str(exc)) from exc

        content_type = response.headers.get("content-type", "audio/mpeg")
        data = response.content

        async def _one() -> AsyncIterator[AudioChunkEvent]:
            # Emit a single chunk; offsets are 0 for lack of precise duration metadata.
            yield AudioChunkEvent(
                call_id=call.call.call_id,
                sequence=0,
                start_offset=0.0,
                end_offset=0.0,
                payload=data,
                content_type=content_type,
                finished=True,
            )

        return _one()
