"""In-memory audio pre-processing (band-limit and tail trim) using PyAV.

This module provides a small pre-processing step that decodes an input audio
payload, applies a voice-band filter (high-pass + low-pass) and trims trailing
silence, then re-encodes to a container suitable for downstream STT.

No external executables are invoked and no temporary files are written.
"""

from __future__ import annotations

import io
import logging
from typing import Any, Final, cast

from .config import AudioPreprocessConfig
from .errors import BroadcastifyError
from .models import AudioChunkEvent

logger = logging.getLogger(__name__)


class AudioPreprocessError(BroadcastifyError):
    """Raised when audio pre-processing fails."""


class AudioPreprocessor:
    """Applies band-limiting and tail-silence trimming to audio bytes.

    The processor operates entirely in-memory using PyAV (libav). It decodes the
    provided payload, pushes frames through a small filter graph, and re-encodes
    to a portable format for STT. By default, output uses WAV/PCM to avoid
    lossy re-encoding artifacts and codec availability constraints.
    """

    _OUT_MIME: Final[str] = "audio/wav"
    _OUT_FORMAT: Final[str] = "wav"
    _OUT_CODEC: Final[str] = "pcm_s16le"

    def __init__(self, config: AudioPreprocessConfig) -> None:
        """Create a preprocessor configured by ``AudioPreprocessConfig``."""
        self._config = config

    def process_chunk(self, chunk: AudioChunkEvent) -> AudioChunkEvent:
        """Return a new chunk with processed audio and updated content type.

        The call preserves metadata from the original chunk and updates only the
        payload and ``content_type``. Offsets and sequence are unmodified.
        """
        try:
            processed, out_mime = self.process_bytes(chunk.payload, chunk.content_type)
        except AudioPreprocessError:
            raise
        except Exception as exc:  # pragma: no cover - defensive path
            logger.exception("Audio pre-processing failed for call %s", chunk.call_id)
            raise AudioPreprocessError(str(exc)) from exc

        return AudioChunkEvent(
            call_id=chunk.call_id,
            sequence=chunk.sequence,
            start_offset=chunk.start_offset,
            end_offset=chunk.end_offset,
            payload=processed,
            content_type=out_mime,
            finished=chunk.finished,
        )

    def process_bytes(self, data: bytes, content_type: str | None = None) -> tuple[bytes, str]:
        """Decode, filter, and encode ``data``; return bytes and MIME type.

        - Band-limit: high-pass at ``highpass_hz`` and low-pass at ``lowpass_hz``.
        - Tail trim: remove trailing silence using the configured dB threshold and
          minimum duration.
        - Output: WAV/PCM 16-bit mono at ``sample_rate`` by default.
        """
        if not data:
            return data, self._OUT_MIME

        # Decode and filter to frames
        out_frames = self._decode_and_filter(data)
        # Encode to target container
        out_bytes = self._encode_wav(out_frames)
        return out_bytes, self._OUT_MIME

    def _decode_and_filter(self, data: bytes) -> list[object]:  # noqa: PLR0912, PLR0915
        """Return filtered audio frames using a PyAV filter graph."""
        try:
            import av  # type: ignore[import-not-found]  # noqa: PLC0415
            from av.filter import Graph as FilterGraph  # noqa: PLC0415
        except Exception as exc:  # pragma: no cover - optional dependency
            raise AudioPreprocessError(
                "PyAV is required for audio pre-processing; please install 'av'."
            ) from exc

        input_io = io.BytesIO(data)
        out_frames: list[Any] = []
        try:
            with av.open(input_io, mode="r") as in_container:
                audio_stream = next((s for s in in_container.streams if s.type == "audio"), None)
                if audio_stream is None:
                    raise AudioPreprocessError("No audio stream found in input payload")

                # Graph and sink initialised on first frame
                graph: Any | None = None
                sink: Any | None = None

                def _ensure_graph(frm: Any) -> tuple[Any, Any]:
                    nonlocal graph, sink
                    if graph is not None and sink is not None:
                        return graph, sink
                    g: Any = FilterGraph()
                    abuffer = g.add(
                        "abuffer",
                        time_base=str(frm.time_base or audio_stream.time_base or "1/1000"),
                        sample_rate=str(frm.sample_rate),
                        sample_fmt=frm.format.name,
                        channel_layout=frm.layout.name,
                    )
                    hp = g.add("highpass", f=str(self._config.highpass_hz))
                    lp = g.add("lowpass", f=str(self._config.lowpass_hz))
                    aformat = g.add(
                        "aformat",
                        sample_fmts="s16",
                        sample_rates=str(self._config.sample_rate),
                        channel_layouts="mono" if self._config.mono else frm.layout.name,
                    )
                    stop_duration = max(0.0, float(self._config.tail_silence_min_ms) / 1000.0)
                    sil_rm = g.add(
                        "silenceremove",
                        start_periods="0",
                        start_duration="0",
                        start_threshold="0dB",
                        stop_periods="1",
                        stop_duration=str(stop_duration),
                        stop_threshold=f"{self._config.tail_silence_threshold_db}dB",
                    )
                    sink_node = g.add("abuffersink")
                    abuffer.link_to(hp)
                    hp.link_to(lp)
                    lp.link_to(aformat)
                    aformat.link_to(sil_rm)
                    sil_rm.link_to(sink_node)
                    g.configure()
                    graph = g
                    sink = sink_node
                    return g, sink_node

                out_frames = []
                for packet in in_container.demux(audio_stream):
                    for frame in packet.decode():
                        g, sk = _ensure_graph(frame)
                        g.push(frame)
                        while True:
                            try:
                                f = sk.pull()
                            except getattr(av, "AVError", Exception):
                                break
                            if f is None:
                                break
                            out_frames.append(f)

                if graph is not None:
                    try:
                        graph.push(None)  # type: ignore[arg-type]
                        while True:
                            try:
                                f = sink.pull()  # type: ignore[call-arg]
                            except getattr(av, "AVError", Exception):
                                break
                            if f is None:
                                break
                            out_frames.append(f)
                    except getattr(av, "AVError", Exception) as exc:
                        logger.debug("Preprocessor flush ignored av.AVError: %s", exc)

        except AudioPreprocessError:
            raise
        except Exception as exc:  # pragma: no cover - defensive demux/graph path
            raise AudioPreprocessError(f"Decoding/filtering failed: {exc}") from exc
        return list(out_frames)

    def _encode_wav(self, out_frames: list[object]) -> bytes:
        """Encode frames to WAV/PCM 16-bit in-memory and return bytes."""
        try:
            import av  # type: ignore[import-not-found]  # noqa: PLC0415
        except Exception as exc:  # pragma: no cover - optional dependency
            raise AudioPreprocessError(
                "PyAV is required for audio pre-processing; please install 'av'."
            ) from exc

        output_io = io.BytesIO()
        try:
            with av.open(output_io, mode="w", format=self._OUT_FORMAT) as out_container:
                container_any = cast(Any, out_container)
                stream = container_any.add_stream(self._OUT_CODEC, rate=self._config.sample_rate)
                # Do not set channels/layout on the encoder; frames carry correct params
                for frm in out_frames:
                    for packet in stream.encode(frm):
                        out_container.mux(packet)
                for packet in stream.encode(None):
                    out_container.mux(packet)
        except Exception as exc:  # pragma: no cover - defensive encode path
            raise AudioPreprocessError(f"Encoding failed: {exc}") from exc
        return output_io.getvalue()
