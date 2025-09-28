"""In-memory audio pre-processing (band-limit and tail trim) using PyAV.

This module provides a small pre-processing step that decodes an input audio
payload, applies a voice-band filter (high-pass + low-pass) and trims trailing
silence, then re-encodes to a container suitable for downstream STT.

No external executables are invoked and no temporary files are written.
"""

from __future__ import annotations

import io
import logging
import math
import wave
from dataclasses import dataclass
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

    @dataclass(frozen=True)
    class Segment:
        """In-memory audio segment encoded as WAV with timing metadata."""

        payload: bytes
        start_time: float
        end_time: float
        content_type: str

    def segment_call(self, data: bytes) -> list[Segment]:  # noqa: PLR0912, PLR0915
        """Split a call into segments by silence and return WAV segments.

        Heuristic: compute short-time energy over the band-limited PCM stream and
        split where energy stays below threshold for a minimum duration. Adds small
        padding to avoid clipping phonemes.
        """
        frames = self._decode_and_filter(data)
        # Render PCM mono s16 for analysis
        pcm_bytes, sample_rate = self._render_pcm(frames)
        if not pcm_bytes:
            return []
        # Parameters
        threshold_db = float(self._config.tail_silence_threshold_db)
        window_ms = 20.0
        hop_ms = 20.0
        min_sil_ms = max(150.0, float(self._config.tail_silence_min_ms))
        min_seg_ms = 800.0
        max_seg_ms = 30_000.0
        prepad_ms = 100.0
        postpad_ms = 100.0

        # Energy profile
        samples = self._bytes_to_samples(pcm_bytes)
        win = max(1, int(sample_rate * (window_ms / 1000.0)))
        hop = max(1, int(sample_rate * (hop_ms / 1000.0)))
        energies_db: list[float] = []
        for start in range(0, len(samples) - win + 1, hop):
            s = samples[start : start + win]
            rms = math.sqrt(sum(int(x) * int(x) for x in s) / float(win))
            # Avoid log of zero; full scale for int16 is 32768
            db = -120.0 if rms <= 1.0 else 20.0 * math.log10(min(rms / 32768.0, 1.0))
            energies_db.append(db)

        # Find silence runs
        def frames_to_ms(n: int) -> float:
            return (n * hop) / sample_rate * 1000.0

        cut_points: list[int] = []
        run_start: int | None = None
        for i, db in enumerate(energies_db):
            if db < threshold_db:
                if run_start is None:
                    run_start = i
            elif run_start is not None:
                dur_ms = frames_to_ms(i - run_start + 1)
                if dur_ms >= min_sil_ms:
                    cut_points.append(i)
                run_start = None
        # Build segments from cut_points, enforce min/max and padding
        segments: list[tuple[int, int]] = []  # (start_sample, end_sample)
        total_samples = len(samples)
        # Convert frame indices to sample indices (approximate via hop)
        candidate_samples = [cp * hop for cp in cut_points]
        last_start = 0
        for cp in candidate_samples:
            seg_len = cp - last_start
            if (seg_len / sample_rate * 1000.0) >= min_seg_ms:
                segments.append((last_start, cp))
                last_start = cp
        # Tail segment
        if last_start < total_samples:
            segments.append((last_start, total_samples))
        # Enforce max length by splitting long tails
        max_len_samples = int(sample_rate * (max_seg_ms / 1000.0))
        split_segments: list[tuple[int, int]] = []
        for a, b in segments:
            cur = a
            while (b - cur) > max_len_samples:
                split_segments.append((cur, cur + max_len_samples))
                cur += max_len_samples
            split_segments.append((cur, b))
        segments = split_segments

        # Apply padding and clamp
        prepad = int(sample_rate * (prepad_ms / 1000.0))
        postpad = int(sample_rate * (postpad_ms / 1000.0))
        padded: list[tuple[int, int]] = []
        for _idx, (a, b) in enumerate(segments):
            pa = max(0, a - prepad)
            pb = min(total_samples, b + postpad)
            if pb > pa:
                padded.append((pa, pb))

        # Emit WAV segments
        results: list[AudioPreprocessor.Segment] = []
        for (a, b) in padded:
            seg_bytes = self._wav_from_samples(samples[a:b], sample_rate)
            start_time = a / float(sample_rate)
            end_time = b / float(sample_rate)
            results.append(
                AudioPreprocessor.Segment(
                    seg_bytes, start_time, end_time, self._OUT_MIME
                )
            )
        return results

    def _render_pcm(self, out_frames: list[Any]) -> tuple[bytes, int]:
        """Return raw PCM s16 mono bytes and sample rate from frames."""
        if not out_frames:
            return b"", self._config.sample_rate
        # Concatenate planes bytes; frames are already mono s16 from filter
        chunks: list[bytes] = []
        sr = self._config.sample_rate
        for frm in out_frames:
            sr = getattr(frm, "sample_rate", sr)
            plane = frm.planes[0]
            to_bytes = getattr(plane, "to_bytes", None)
            if callable(to_bytes):
                data = cast(bytes, to_bytes())
            else:
                # Fallback for PyAV versions where planes implement buffer protocol
                data = bytes(plane)
            chunks.append(data)
        return b"".join(chunks), int(sr)

    def _bytes_to_samples(self, payload: bytes) -> list[int]:
        """Interpret PCM16 mono bytes as a list of int samples."""
        # Memoryview to avoid copies
        mv = memoryview(payload)
        # Convert little-endian 16-bit signed
        samples: list[int] = []
        for i in range(0, len(mv), 2):
            val = int.from_bytes(mv[i : i + 2], byteorder="little", signed=True)
            samples.append(val)
        return samples

    def _wav_from_samples(self, samples: list[int], sample_rate: int) -> bytes:
        """Write a mono 16-bit WAV from int samples."""
        buf = io.BytesIO()
        with wave.open(buf, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(sample_rate)
            # Pack samples as little-endian 16-bit signed
            out = bytearray()
            for s in samples:
                out.extend(int(s).to_bytes(2, byteorder="little", signed=True))
            wf.writeframes(out)
        return buf.getvalue()

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
