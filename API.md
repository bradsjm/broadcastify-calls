# Broadcastify Integration Specification

**Target Audience:** Engineers implementing the next-generation asynchronous, event-driven `broadcastify` client library.

**Document Date:** 2025-09-21

---

## 1. Scope & Goals

- Deliver a modern, asyncio-native Python package that authenticates with Broadcastify, consumes live call feeds as an event stream, retrieves archived metadata, and optionally performs near real-time transcription through a remote Whisper-compatible service.
- Structure the library around producer/consumer patterns with resilient back-pressure handling, allowing downstream subscribers (analytics, storage, alerting) to react to events in near real time.
- Encapsulate Broadcastify HTTP traffic behind strongly typed boundaries with browser-mimicking headers, deterministic error semantics, and comprehensive observability hooks.
- Support synchronous compatibility only via thin adapters; the primary focus is an async event-driven core optimized for long-running services and microservices.

### Non-Goals

- Building UX layers (CLI, GUI) or opinionated storage engines; consumers wire their own interfaces.
- Managing credential persistence or secret rotation; the library surfaces hooks to integrate external secret stores.
- Implementing proprietary streaming transports (e.g., WebSockets) beyond the documented HTTP polling contract.

## 2. High-Level Architecture

```
broadcastify_client/
├─ config.py            # Typed configuration schemas (credentials, headers, transcription, runtime tuning)
├─ http.py              # Async HTTP client abstractions with browser header injection
├─ auth.py              # Async authentication/session lifecycle using http.py
├─ models.py            # Dataclasses / TypedDicts (Call, Events, Errors, Transcription artifacts)
├─ archives.py          # Async archive retrieval with pluggable cache provider
├─ live_producer.py     # Async LiveCall producer (poll -> event queue)
├─ audio_consumer.py    # Async consumer downloading audio and emitting payload events
├─ transcription.py     # Async Whisper pipeline consuming audio payload events
├─ eventbus.py          # Async event dispatcher (Queues, Topics, back-pressure controls)
├─ cache.py             # Cache provider contracts + default async filesystem implementation
├─ telemetry.py         # Structured logging and metrics instrumentation
└─ errors.py            # Exception hierarchy with retry hints
```

- **Event Flow:**
  1. `LiveCallProducer` polls Broadcastify on an interval, pushes `CallEvent` objects into an `asyncio.Queue`.
  2. `AudioConsumer` listens on the queue, downloads call audio assets, and emits `AudioPayloadEvent` instances for downstream consumers.
  3. Optional `TranscriptionPipeline` performs final-only transcription. The client concatenates each call’s raw audio, uploads it once to the provider, and publishes a single final transcript.
  4. Applications register additional consumers (analytics, persistence) via `EventBus.subscribe(topic, callback)`.
- Dependency injection allows swapping HTTP client (e.g., `aiohttp`, `httpx.AsyncClient`), cache backend, and transcription provider.

## 3. External API Interactions

### 3.0 HTTP Efficiency Controls

- Maintain a single shared async HTTP client (`http.py`) with connection pooling, reuse of TLS handshakes, and TCP keep-alive enabled.
- Enable HTTP/2 when the underlying library and Broadcastify endpoints support it; otherwise continue with HTTP/1.1 while reusing connections.
- Apply `Accept-Encoding: gzip, deflate, br` to all GET/POST requests and allow automatic decompression to minimize bandwidth.
- Impose global concurrency guards (semaphores) to limit the number of simultaneous Broadcastify requests, preventing server overload while smoothing client CPU usage.
- Respect rate limits using the `rate_limit_per_minute` field in `LiveProducerConfig`; coordinate shard-level budgets via a shared token bucket.
- Cache Archive responses aggressively and honor future `ETag`/`Last-Modified` headers when Broadcastify provides them, falling back to current caching heuristics otherwise.
- Propagate retry-after semantics: when receiving HTTP 429 or 503 with `Retry-After`, back off all producers before resuming.
- For audio downloads, stream with buffered reads (default 8 KiB) while reusing the pooled client; pause downloads when downstream consumers lag to reduce unnecessary IO.

### 3.1 Authentication Lifecycle

#### Login Request

- **Endpoint:** `POST https://www.broadcastify.com/login/`
- **Content-Type:** `application/x-www-form-urlencoded`
- **Mandatory Headers:**
  - `User-Agent`: `Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36`
  - `Accept`: `text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8`
  - `Accept-Language`: `en-US,en;q=0.9` (configurable)
  - `Connection`: `keep-alive`
- **Form Fields:**
  | Field | Type | Required | Default | Notes |
  |-------------|--------|----------|---------|-------|
  | `username` | string | Yes | N/A | Broadcastify account username. |
  | `password` | string | Yes | N/A | Broadcastify account password. |
  | `action` | string | Yes | `auth` | Required by Broadcastify. |
  | `redirect` | string | Yes | `https://www.broadcastify.com` | Mirrors site flow. |
- **Async Behavior:** Authentication is executed via the shared async HTTP client. Requests disable redirects to inspect the raw 302 response.
- **Success Criteria:**
  1. HTTP status `< 400` (observed `302 Found`).
  2. `Set-Cookie` header exists with leading `bcfyuser1=` cookie. Token becomes the session credential.
- **Failure Modes:** `failed=1` redirect → `AuthenticationError(kind="invalid_credentials")`; missing cookie → `AuthenticationError(kind="missing_cookie")`; HTTP errors → `TransportError`.
- **Output:** `AuthResult` containing `SessionToken`, headers snapshot, and issued timestamp.

#### Session Persistence & Refresh

- `SessionToken` dataclass: `{ value: str, issued_at: datetime, expires_at: Optional[datetime] }`.
- Provide async `CredentialStore` protocol for retrieving/storing tokens.
- Expose `AuthManager.ensure_session()` coroutine that re-authenticates when headers fail or explicit invalidation is detected.

#### Logout

- **Endpoint:** `GET https://www.broadcastify.com/account/?action=logout`
- **Headers/Cookies:** Same browser header template plus `bcfyuser1=<token>` cookie.
- Non-2xx responses raise `TransportError`. Logout is optional but exposed via async `logout()`.

### 3.2 Archived Call Retrieval

#### Request

- **Endpoint:** `GET https://www.broadcastify.com/calls/apis/archivecall.php`
- **Query Parameters:**
  | Parameter | Type | Required | Notes |
  |-----------|------|----------|-------|
  | `group` | string | Yes | `{systemId}-{talkgroupId}`. |
  | `s` | int | Yes | Epoch seconds floored to 30-minute boundary. |
- **Headers:** Browser spoof template + `bcfyuser1` cookie.
- **Async Flow:** `ArchiveClient.fetch(time_block)` first checks the async cache provider; on miss performs HTTP GET.
- **Response Schema:** Expects `start`, `end`, `calls` array; missing `calls` triggers `ResponseParsingError`.
- **Return Type:** `ArchiveResult(calls: Sequence[Call], window: TimeWindow, fetched_at: datetime, cache_hit: bool, raw: Mapping[str, Any])`.

### 3.3 Live Call Streaming (Producer)

#### Poll Contract

- **Endpoint:** `POST https://www.broadcastify.com/calls/apis/live-calls`
- **Headers:** Browser spoof template + `Content-Type: application/x-www-form-urlencoded`.
- **Form Fields:**
  | Field | Type | Required | Notes |
  |--------------|--------|----------|-------|
  | `groups[]` | string | Yes | `{systemId}-{talkgroupId}`. |
  | `pos` | float | Yes | Cursor seconds; maintained by producer. |
  | `doInit` | int | Yes | `1` for first poll, `0` thereafter. |
  | `systemId` | int | Yes | Observed constant `0` (retain). |
  | `sid` | int | Yes | Observed constant `0` (retain). |
  | `sessionKey` | string | Yes | Random token per session. |
- **Cursor Management:** Producer maintains `position = max(last_call.start_time + 1, response.lastPos or position)`.
- **Polling Loop:**
  - Implemented as `async def run(self, queue: Queue[LiveCallEnvelope])`.
  - Supports configurable poll interval and jitter; on failure, uses exponential backoff with retry budgets.

#### Producer Configuration

```python
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class LiveProducerConfig:
    poll_interval: float = 2.0           # seconds between polls when healthy
    jitter_ratio: float = 0.1            # fraction of poll_interval applied as +/- jitter
    queue_maxsize: int = 256             # max items waiting in queue before back-pressure
    initial_backoff: float = 1.0         # seconds for first retry after failure
    max_backoff: float = 30.0            # ceiling for exponential backoff
    max_retry_attempts: Optional[int] = None  # None => infinite retries within service lifetime
    rate_limit_per_minute: Optional[int] = None  # throttle outbound polls if configured
    metrics_interval: float = 60.0       # seconds between producer health emissions
    initial_position: Optional[float] = None  # resume cursor in seconds (None => start fresh)
```

- Defaults target a balance between responsiveness and Broadcastify friendliness; adjust `poll_interval` upward when monitoring calm talkgroups or to reduce API usage.
- `queue_maxsize` drives back-pressure. When the queue is full the producer pauses polling until consumers acknowledge events.
- `rate_limit_per_minute` allows operators to enforce an upper bound on API hits across sharded producers.
- Runtime injection occurs via `LiveCallProducer(config: LiveProducerConfig, ...)` with configuration sourced from `config.py` schemas (ENV/YAML adaptor recommended).

#### Producer Output

```python
@dataclass(frozen=True)
class LiveCallEnvelope:
    call: Call
    cursor: Optional[float]
    received_at: datetime
    shard_key: tuple[int, int]  # (systemId, talkgroupId)
    raw_payload: Mapping[str, object]
```

- Events are enqueued onto `asyncio.Queue` (size configurable). Back-pressure is applied via queue size; when full, producer pauses until consumers drain items.
- `BroadcastifyClient` forwards each dequeued event to the event bus on two topics: the shared channel `"calls.live"` and a shard-specific channel `f"calls.live.{systemId}.{talkgroupId}"`, enabling coarse and fine-grained subscribers without additional wiring.
- Telemetry metrics exposed from the producer loop include `live_producer.dispatched` (per poll) and a periodic `live_producer.queue_depth` gauge controlled by `metrics_interval`.

### 3.4 Audio Pipeline (Consumer)

- `AudioConsumer` subscribes to the `LiveCallEnvelope` queue and downloads each call audio asset once from Broadcastify’s calls CDN. The provider typically serves AAC in MP4/M4A (extension `m4a`).
- An `AudioPayloadEvent` (usually a single final payload) is emitted with the response body and the server’s `Content-Type` header.
- Chunks are published on `calls.audio.raw` and per-call channels `calls.audio.raw.{callId}` for downstream consumers (transcription, storage, analytics). No additional preprocessing or segmentation is performed in the current pipeline.

#### Event Topics & Ordering

Each call produces a deterministic sequence of bus publications so downstream services can reason about lifecycle stages. The table below lists the high-level order; per-call scoped topics (`*.{callId}`) fire immediately after their corresponding global topic.

| Order | Topic | Payload Type | Description |
| ----- | ----- | ------------ | ----------- |
| 1 | `calls.live` | `LiveCallEnvelope` | Dispatches every live call as soon as metadata is polled. A shard-specific topic `calls.live.{systemId}.{talkgroupId}` is emitted in the same tick. |
| 2 | `calls.audio.raw` | `AudioPayloadEvent` | Raw bytes downloaded from Broadcastify (typically AAC/M4A). Per-call scoped topic: `calls.audio.raw.{callId}`. |
| 3 | `transcription.complete` | `TranscriptionResult` | Optional final transcript emitted when transcription is enabled. |

When transcription is disabled, only the `calls.live` and `calls.audio.raw` topics fire. All events respect per-topic ordering guarantees enforced by the async event bus.

### 3.5 Transcription (final-only)

#### Configuration (high level)

```python
@dataclass(frozen=True)
class TranscriptionConfig:
    enabled: bool
    provider: WhisperProvider
    model: str = "whisper-1"
    endpoint_url: Optional[str] = None
    api_key: Optional[str] = None
    language: Optional[str] = "en"
    max_concurrency: int = 2
    # Note: streaming partials are removed.
```

Raw AAC payloads are uploaded without intermediate preprocessing, silence trimming, or segmentation.

#### Workflow

1. On call completion, the client concatenates raw AAC bytes emitted by the audio consumer.
2. The entire call audio is uploaded as an `.m4a` (`audio/mp4`) file to the configured OpenAI-compatible endpoint.
3. The client reads the provider response from the `.text` field and publishes a final transcript.
4. Any provider failure (missing text, empty transcript, HTTP error) raises `TranscriptionError` and is logged without suppression.

#### Topics

- `transcription.complete` — final transcript for the call (emitted once when enabled).

#### Models

```python
@dataclass(frozen=True)
class AudioPayloadEvent:
    call_id: str
    sequence: int
    start_offset: float
    end_offset: float
    payload: bytes
    content_type: str  # e.g., "audio/mp4" (m4a)

@dataclass(frozen=True)
class TranscriptionResult:
    call_id: str
    text: str
    language: str
    average_logprob: Optional[float]
    segments: Sequence[str]
```
The current OpenAI backend does not provide segment-level metadata; the
``segments`` field remains for forward compatibility and defaults to an empty
tuple.

#### Error Handling

- Provider failures (missing text, empty transcript, HTTP error) raise `TranscriptionError`; callers can decide whether to retry or log the failure. No fallback transcript is emitted when the provider rejects the audio.
- Network errors bubble as `TransportError`; catastrophic failures trigger backoff to avoid hammering the provider.

## 4. Data Models (Summary)

- `Call`: typed mapping of Broadcastify call payload including extended fields (`call_freq`, `call-ttl`, `metadata`, etc.), storing unknown keys in immutable `raw` mapping.
- `SessionToken`, `TimeWindow`, `ArchiveResult`, `LiveCallEnvelope`, `AudioPayloadEvent`, `TranscriptionResult` as defined above.
- Enumerations for error kinds and provider identifiers.

## 5. Public Facade (Async-First Interface)

```python
class AsyncBroadcastifyClient(Protocol):
    async def authenticate(self, credentials: Credentials | SessionToken) -> SessionToken: ...
    async def logout(self) -> None: ...
    async def get_archived_calls(self, system_id: int, talkgroup_id: int, time_block: int) -> ArchiveResult: ...
    async def create_live_producer(self, system_id: int, talkgroup_id: int, *, position: Optional[int] = None) -> LiveCallProducer: ...
    async def register_consumer(self, topic: str, callback: ConsumerCallback) -> None: ...
    async def start(self) -> None: ...  # kicks off producers/consumers
    async def shutdown(self) -> None: ...
```

- Provide optional synchronous adapters (e.g., `BroadcastifyClient.from_async(client)`), but they internally run an event loop and are secondary.
- `LiveCallProducer` exposes `async def run(queue)` and `async def stop()`.

## 6. Error Taxonomy

- `BroadcastifyError`
  - `AuthenticationError`
  - `TransportError`
  - `ResponseParsingError`
  - `CacheError`
  - `LiveSessionError`
  - `AudioDownloadError`
  - `TranscriptionError`
- Exceptions include retry hints, telemetry tags, and sanitized payload snippets.

## 7. Caching Requirements

- Default cache layer persists structured archive responses keyed by `(systemId, talkgroupId, time_block)` with configurable TTL and optional metadata retention.
- Cache backend must present async interface (`async def get(...)`, `async def set(...)`), support eviction policies, and allow instrumentation for hit/miss metrics.

## 8. Security & Compliance

- Mask tokens in logs; never emit API keys.
- Support dependency injection for secret providers (e.g., AWS Secrets Manager) to refresh Broadcastify/Whisper credentials.
- Honor Broadcastify ToS: maintain respectful polling cadence, avoid parallel login storms, and mimic legitimate browser headers without malicious intent.
- Provide encryption hooks for persisted audio/transcript data when stored by consumers.

## 9. Telemetry & Observability

- Structured logging with correlation IDs per call event.
- Metrics: authentication failures, poll latency, queue depth, consumer lag, transcription throughput, retry counts.
- Optional OpenTelemetry integration for spans around Broadcastify requests and transcription calls.
- Health endpoints/hooks enabling liveness & readiness checks (e.g., queue backlog thresholds).

## 10. Testing Strategy

- Async unit tests using `pytest-asyncio` covering authentication flows, live producer cursor updates, queue back-pressure, and transcription scheduling.
- Contract tests for header injection verifying spoofed headers on every outbound HTTP call.
- Integration tests with mocked HTTP/Whisper servers to validate end-to-end event propagation.
- Load tests measuring queue behavior under high call volume and back-pressure scenarios.

## 11. Recommendations

To accelerate implementation while preserving the specification's type-safety and async guarantees, adopt the following typed libraries:

- **httpx** (`httpx[http2]`): async client shipping `py.typed`, with connection pooling, HTTP/2, and header controls aligning with `http.py` requirements.
- **anyio**: structured-concurrency primitives (`TaskGroup`, `CapacityLimiter`) that underpin resilient producer/consumer orchestration and graceful cancellation.
- **aiolimiter**: typed token-bucket rate limiter suitable for enforcing `LiveProducerConfig.rate_limit_per_minute` budgets across shards.
- **openai**: official async Whisper client supporting typed request/response models, easing `transcription.py` implementation.
- **pydantic** (>=2.0): strict configuration schemas for `config.py`, ensuring credentials, headers, and runtime tuning remain validated.
- **opentelemetry-instrumentation-httpx**: typed instrumentation integrating with `telemetry.py` to emit spans and metrics around outbound HTTP calls.

## 12. Future Enhancements

- Multiple shard support: spawn producers per talkgroup with unified event routing.
- Pluggable speech-to-text providers (Deepgram, AssemblyAI) sharing the transcription interface.
- Real-time alerting framework (rules engine) built atop the event bus.
- WebSocket/SSE bridge exposing live call events directly to frontend clients.

---

**Appendix A: Field Coverage Checklist**

- Browser-mimicking headers for all Broadcastify requests ✅
- Archived call parameters and cache strategy ✅
- Live call poll schema & async producer behavior ✅
- Audio streaming consumer ✅
- Remote Whisper transcription pipeline ✅
- Async interface definitions & event bus ✅
- Telemetry, testing, security considerations ✅
