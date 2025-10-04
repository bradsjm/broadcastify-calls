## Phase 2: Band-Pass Filtering (Complete)
- Extended CLI, environment configuration, and telemetry to support the band-pass stage (defaults 250–3800 Hz) with PyAV-backed fallback/safety checks.
- `PyAvSilenceTrimmer` now executes the band-pass filter off the event loop after trimming, preserving AAC output while logging attenuation metrics.
- Added targeted unit coverage and documentation updates describing the new knobs and behaviour.

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
