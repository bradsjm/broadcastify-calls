# PyAV Silence Trimming Capability

## Context
- Broadcastify live downloads arrive as AAC in an MP4/M4A container with duplicated mono channels.
- Goal: optional, opt-in PyAV pipeline that trims leading/trailing silence and downmixes to a single channel while preserving AAC output.
- Must keep async architecture responsive, surface telemetry, and avoid regressions for users who do not enable processing.

## Architecture Decisions
- **Processing Toggle**: Introduce `AudioProcessingConfig` (default disabled) wired through `BroadcastifyClientDependencies` and CLI/env plumbing.
- **Processor Abstraction**: Define `AudioProcessor` protocol with async `process(AudioPayloadEvent)`; provide no-op implementation when processing disabled.
- **PyAV Trimmer**: Implement `PyAvSilenceTrimmer` that:
  - Runs heavy work inside `asyncio.to_thread` to avoid blocking.
  - Assumes AAC/MP4 input; raise `AudioProcessingError` if payload deviates.
  - Decodes via PyAV, downmixes to mono, detects leading/trailing silence using RMS threshold & windowing, re-encodes to AAC original bitrate, updates offsets.
  - Emits telemetry (duration, trimmed ms/percentage, failures) and logs with deferred interpolation.
- **Dependency Handling**: Create optional `audio-processing` dependency group adding `av` to `pyproject.toml`. Import PyAV lazily to keep base install lean.

## Implementation Steps
1. **Configuration Layer**
   - Add `AudioProcessingConfig` to `config.py` (fields: `enabled`, `silence_threshold_db`, `min_silence_duration_ms`, `analysis_window_ms`).
   - Thread config through `BroadcastifyClientDependencies` and `BroadcastifyClient` constructor.
   - Extend CLI/env readers to expose toggle and thresholds.

2. **Processing Interfaces**
   - Add new module `audio_processing.py` defining `AudioProcessingError`, `AudioProcessor` protocol, and `NullAudioProcessor` implementation.
   - Update `audio_consumer.py` to accept optional processor; wrap processing call with graceful fallback & telemetry.

3. **PyAV Implementation**
   - Add `audio_processing_pyav.py` implementing `PyAvSilenceTrimmer`.
   - Handle decode→analysis→encode pipeline, leveraging PyAV filter graph or manual frame iteration for silence detection.
   - Downmix to single channel before encoding; preserve sample rate and metadata.
   - Ensure deterministic trimming by configuring FFmpeg resampler/encoder options.

4. **Client Wiring**
   - Build processor factory in `BroadcastifyClient` that instantiates PyAV trimmer when config enabled.
   - Ensure lifecycle cleanup and error propagation consistent with existing telemetry/events.

5. **Testing**
   - Add fixture AAC clips (with leading/trailing silence, stereo mono duplication) under `tests/fixtures`.
   - Write unit tests for trimmer behaviour (threshold, zero-trim path, unsupported format error).
   - Add async integration test to verify processed events from `AudioConsumer` are trimmed and mono.
   - Include regression test ensuring disabled config bypasses PyAV import.

6. **Documentation & Tooling**
   - Update `README.md` and `API.md` describing toggle, dependency group, and behaviour.
   - Add release note entry / comment in CHANGELOG if available.
   - Document telemetry events and config defaults.

## Non-Goals
- Partial/chunked processing (only final payloads handled).
- Generic multi-format transcoding; pipeline limited to AAC input/output.
- Real-time streaming silence suppression.

## Open Questions
- Confirm desired default thresholds (e.g., -50 dB for ≥200 ms). Adjust after empirical testing.
- Decide whether to expose additional metrics (e.g., trimming ratio) on event bus.

## Validation Checklist
- `uv run ruff check .`
- `uv run pyright`
- `uv run pytest`
- Manual listen spot-check of trimmed output to confirm silence removal quality.

## Phase 2: Band-Pass Filtering (Post-Validation)
- **Objective**: Once silence trimming ships successfully, introduce an optional PyAV filter stage that attenuates P25 TDMA artifacts and tones outside the intelligible voice band (approx. 300–3400 Hz) to boost Whisper transcription accuracy.
- **Design Considerations**:
  - Reuse the `AudioProcessingConfig` with an additional toggle (`bandpass_enabled`) and passband bounds.
  - Leverage FFmpeg biquad or `firequalizer` filters via PyAV filter graphs, ensuring processing still executes off the event loop.
  - Preserve AAC output and single-channel audio, cascading after silence trimming to avoid redundant decode cycles.
  - Capture telemetry on filter application (cutoff frequencies, attenuation applied) and surface fallback behaviour when FFmpeg filter graph creation fails.
- **Planned Steps**:
  1. Extend configuration/CLI wiring with band-pass options and sane defaults (e.g., low-cut 250 Hz, high-cut 3800 Hz) with documentation of trade-offs.
  2. Implement a composable filter chain within `PyAvSilenceTrimmer` or a dedicated `PyAvBandPassFilter` that can be orchestrated by the processor factory.
  3. Add targeted tests using synthetic clips with TDMA tones verifying attenuation and transcription improvements (mocking Whisper for deterministic assertions).
  4. Update docs/releases post-validation with guidance on when to enable the filter and expected transcription gains.

## Phase 3: Half-Duplex Segmentation for Whisper (Post-Filtering)
- **Motivation**: Whisper exhibits higher accuracy and diarization quality when each input clip contains a single speaker. Broadcastify calls are half-duplex; long silence gaps typically mark floor changes between participants. Segmenting on these gaps allows feeding Whisper isolated voices and emitting per-segment transcriptions.
- **Approach Overview**:
  - After trimming and optional band-pass filtering, scan the mono PCM stream for silence spans exceeding a configurable `segment_break_ms` threshold.
  - Split the payload into sequential segments, each annotated with channel metadata and relative timing for downstream correlation.
  - Submit each segment independently to the transcription backend, emitting `transcription.complete` events per segment (e.g., `call_id` + `segment_index`). Provide an aggregate helper to reconstruct full-call transcripts in order when needed.
- **Design Considerations**:
  - Extend `AudioProcessingConfig` with segmentation toggles (`segment_on_silence`, `segment_break_ms`, `min_segment_ms`). Ensure defaults keep feature disabled until validated.
  - Reuse the PyAV decode path to operate on PCM buffers, avoiding redundant decoding when multiple stages are enabled.
  - Preserve AAC output for archival by stitching segment outputs back into a trimmed+filtered master payload, while simultaneously producing PCM snippets for transcription.
  - Emit telemetry for segmentation decisions (number of segments, detected silence durations) to tune thresholds in production.
- **Planned Steps**:
  1. Introduce segment-aware audio processor (either extending the existing class or adding a coordinator) that yields both the primary payload and derived PCM segments.
  2. Update the transcription dispatch logic to iterate segments, calling Whisper per segment and including speaker-turn metadata in the published events.
  3. Add stateful tests using synthetic half-duplex conversations verifying correct split points, ordering, and transcription fan-out.
  4. Document the segmentation workflow, configuration knobs, and API changes for consumers relying on single `transcription.complete` events.

## Future Enhancements (Pending Validation of Earlier Phases)
- **Dynamic Loudness Normalization**
  - Investigate LUFS-based normalization to stabilize perceived loudness around –20 LUFS without aggressive compression.
  - Integrate as an optional stage with safeguards against clipping and over-processing.
- **Artifact Frame Rejection**
  - Detect and discard encrypted bursts or data-only frames before transcription to reduce hallucinated output.
  - Leverage spectral heuristics or metadata flags from Broadcastify payloads.
- **Domain-Adaptive Prompting**
  - Extend transcription backends with EMS/LEO prompt templates and adjustable decoding parameters (temperature, silence penalty).
  - Allow per-agency wordlists loaded from configuration.
- **Clock Drift Correction**
  - Analyze sample-rate drift on ingest; resample streams with noticeable deviation to the nominal rate before processing.
  - Emit telemetry when corrections exceed thresholds for operational awareness.
- **Metadata-Aware Post-Processing**
  - Propagate dispatcher/unit identifiers or talkgroup context into transcription events for richer downstream labelling.
  - Optionally merge segmented transcripts with structured speaker annotations.
